//! [`ClusterAdapter`]: the worker-side `Adapter` the percore [`crate::transport::worker`]
//! drives via `block_on(ctx.dispatch(..))` when clustering is active.
//!
//! It does the LOCAL half synchronously on an injected [`LocalAdapter`] (which never
//! awaits real I/O) and fires the matching FIRE-AND-FORGET [`ClusterCmd`] at the
//! [`ClusterBridge`] over a [`ClusterHandle`]. It NEVER awaits Redis — that is the whole
//! point of the bridge: the sync mio loop must not block on the network.
//!
//! Division of labour for the membership/broadcast edges:
//! - `subscribe` / `unsubscribe`: the worker keeps the node-LOCAL outcome (count, presence
//!   roster). The bridge, on the node's single `RedisAdapter`, computes the authoritative
//!   cluster count and fires the single cluster-wide `subscription_count` /
//!   `channel_occupied` / `channel_vacated` — which the connection handler suppresses in
//!   cluster mode (`ConnectionContext::clustered`). For PRESENCE channels the worker still
//!   does the node-local join (so the connection is indexed for delivery), but the bridge
//!   owns the cluster-wide outputs: it sends the cluster ROSTER back as
//!   `subscription_succeeded` and fires the single cluster-wide `member_added` /
//!   `member_removed` (`PresenceSubscribe` / `PresenceLeave`).
//! - `broadcast`: local delivery happens here on the worker; the bridge's `Publish` does
//!   ONLY the Redis publish, so there is no double local delivery and self-dedup stops the
//!   origin re-receiving its own frame.
//!
//! Signin/watchlist follow the same split: `signin_user` / `signout_user` / `watch` /
//! `unwatch` do the node-LOCAL half synchronously and fire the cluster edge at the bridge
//! (`Signin` / `Signout` / `Watch` / `Unwatch`), which owns the cluster-wide online
//! refcount, the `WatchOnline` / `WatchOffline` publish (REMOTE watchers) + the LOCAL
//! watcher notify, and the cluster-wide initial online snapshot. `send_to_user` /
//! `terminate_user` are NEVER called on the worker path (they are REST/admin ops driven by
//! the node's `RedisAdapter` via `bridge.adapter()`); the worker methods delegate to
//! `local` only as a non-cluster fallback. Presence CAPACITY enforcement and cache stay
//! node-local per the per-method notes.

use crate::adapter::local::LocalAdapter;
use crate::adapter::Adapter;
use crate::channel::cache::CachedEvent;
use crate::channel::outcome::{ChannelSummary, SubscribeOutcome, UnsubscribeOutcome};
use crate::cluster::bridge::ClusterHandle;
use crate::connection::handle::ConnectionHandle;
use crate::presence::member::PresenceMember;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use crate::user::{UserJoinOutcome, UserLeaveOutcome};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

/// Worker-side clustering adapter: node-local state on `local`, cross-node coordination
/// fired (never awaited) at the bridge via `handle`.
pub struct ClusterAdapter {
    local: Arc<LocalAdapter>,
    handle: ClusterHandle,
}

impl ClusterAdapter {
    /// Build a `ClusterAdapter` over the worker's shared `local` and a `handle` to the
    /// node's bridge. `local` MUST be the same `LocalAdapter` the bridge's `RedisAdapter`
    /// shares (so cross-node frames the recv loop re-delivers land on the workers' sink).
    pub fn new(local: Arc<LocalAdapter>, handle: ClusterHandle) -> Self {
        Self { local, handle }
    }
}

#[async_trait]
impl Adapter for ClusterAdapter {
    async fn subscribe(
        &self,
        app: &str,
        channel: &str,
        handle: ConnectionHandle,
        member: Option<PresenceMember>,
    ) -> SubscribeOutcome {
        // Capture the socket id + mailbox BEFORE `handle` is moved into the local adapter.
        // The mailbox lets the bridge send the CLUSTER-wide `subscription_succeeded` roster
        // straight to this connection on the presence path.
        let socket_id = handle.socket_id.clone();
        let mailbox = handle.mailbox.clone();
        // Node-local subscribe (synchronous) — the returned outcome is node-local truth.
        // For presence this also indexes the connection for delivery on this worker (so it
        // receives member_added/removed and broadcasts); the cluster roster + member_added
        // come from the bridge, not this node-local outcome.
        let out = self
            .local
            .subscribe(app, channel, handle, member.clone())
            .await;
        // The node-local 0→1 edge drives the bridge's Redis msg-channel subscribe-on-first.
        let node_first = out.subscription_count == 1;
        // Fire-and-forget at the bridge. Presence routes to PresenceSubscribe (cluster
        // roster + member_added + channel_occupied); non-presence routes to Subscribe
        // (cluster subscription_count + channel_occupied + the cache replay / cache_miss
        // for cache channels, delivered to this connection's `mailbox`).
        match &member {
            Some(m) => self.handle.presence_subscribe(
                Arc::from(app),
                Arc::from(channel),
                m.clone(),
                socket_id,
                mailbox,
                node_first,
            ),
            None => self.handle.subscribe(
                Arc::from(app),
                Arc::from(channel),
                socket_id,
                mailbox,
                node_first,
            ),
        }
        out
    }

    async fn unsubscribe(
        &self,
        app: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> UnsubscribeOutcome {
        let out = self.local.unsubscribe(app, channel, socket_id).await;
        let node_last = out.subscription_count == 0;
        // Presence routes to PresenceLeave (cluster member_removed + channel_vacated);
        // non-presence routes to Unsubscribe (cluster subscription_count + channel_vacated).
        match &out.presence {
            Some(leave) => self.handle.presence_leave(
                Arc::from(app),
                Arc::from(channel),
                leave.user_id.clone(),
                socket_id.clone(),
                node_last,
            ),
            None => self.handle.unsubscribe(
                Arc::from(app),
                Arc::from(channel),
                socket_id.clone(),
                node_last,
            ),
        }
        out
    }

    async fn broadcast(
        &self,
        app: &str,
        channel: &str,
        event: ServerEvent,
        except: Option<SocketId>,
    ) {
        // Local delivery on THIS worker (typed event, honouring `except`).
        self.local
            .broadcast(app, channel, event.clone(), except.clone())
            .await;
        // Pre-encode the v7 frame ONCE and fire it at the bridge, which does ONLY the
        // Redis publish (no double local delivery; self-dedup on the origin node).
        let frame = match &event {
            ServerEvent::Raw(f) => f.to_string(),
            other => crate::protocol::v7::frames::encode(other),
        };
        self.handle
            .publish(Arc::from(app), Arc::from(channel), frame, except);
    }

    async fn channels(&self, app: &str, prefix: Option<&str>) -> Vec<ChannelSummary> {
        // Cluster-correct channel listing is the REST plane's job (it queries the node's
        // `RedisAdapter` directly). Node-local here; reached only as a non-cluster fallback.
        self.local.channels(app, prefix).await
    }

    async fn channel(&self, app: &str, channel: &str) -> ChannelSummary {
        // Cluster-correct channel read is the REST plane's job; delegate to local here.
        self.local.channel(app, channel).await
    }

    async fn presence_members(&self, app: &str, channel: &str) -> Vec<PresenceMember> {
        // Node-local roster. In cluster mode the bridge owns the cluster-wide presence
        // roster + capacity; this worker method is reached only on the non-cluster path
        // (the clustered subscribe is `!clustered`-guarded in `ws::subscribe`).
        self.local.presence_members(app, channel).await
    }

    async fn cache_set(&self, app: &str, channel: &str, event: CachedEvent, ttl: Duration) {
        // Cache WRITES on the percore worker path stay node-local: the cluster (Redis)
        // cache is populated by the REST publish path on each node (which drives the
        // node's `RedisAdapter::cache_set`). The worker never writes the cache here.
        self.local.cache_set(app, channel, event, ttl).await
    }

    async fn cache_get(&self, app: &str, channel: &str) -> Option<CachedEvent> {
        // Node-local read only. The CLUSTER (Redis) cache replay for a subscribing
        // connection is done by the bridge's `ClusterCmd::Subscribe` arm (it reads the
        // node's `RedisAdapter` and sends the replay to the connection's mailbox), so the
        // worker's own inline cache replay in `ws::subscribe` is suppressed in cluster
        // mode. This node-local read remains for any non-cluster fallback caller.
        self.local.cache_get(app, channel).await
    }

    async fn signin_user(
        &self,
        app: &str,
        user_id: &str,
        handle: ConnectionHandle,
    ) -> UserJoinOutcome {
        // Capture the socket id BEFORE `handle` is moved into the local adapter — the
        // bridge needs it for the cluster USER_SIGNIN binding token.
        let socket_id = handle.socket_id.clone();
        // Node-local signin (synchronous) — `first_for_user` here is the NODE-local 0→1
        // edge, which drives the bridge's usermsg subscribe-on-first. The cluster-wide
        // online edge (WatchOnline publish + local-watcher notify) is computed on the
        // bridge, NOT from this node-local outcome.
        let out = self.local.signin_user(app, user_id, handle).await;
        let node_first = out.first_for_user;
        self.handle.signin(
            Arc::from(app),
            user_id.to_string(),
            socket_id,
            node_first,
        );
        out
    }

    async fn signout_user(
        &self,
        app: &str,
        user_id: &str,
        socket_id: &SocketId,
    ) -> UserLeaveOutcome {
        // Node-local signout (synchronous) — `last_for_user` here is the NODE-local 1→0
        // edge (drives the bridge's usermsg unsubscribe-on-last). The cluster-wide offline
        // edge is computed on the bridge.
        let out = self.local.signout_user(app, user_id, socket_id).await;
        let node_last = out.last_for_user;
        self.handle.signout(
            Arc::from(app),
            user_id.to_string(),
            socket_id.clone(),
            node_last,
        );
        out
    }

    async fn is_user_online(&self, app: &str, user_id: &str) -> bool {
        // Node-local check. Cluster-wide online status is served by the REST plane via the
        // node's `RedisAdapter`; not reached on the worker path in cluster mode.
        self.local.is_user_online(app, user_id).await
    }

    async fn send_to_user(&self, app: &str, user_id: &str, event: ServerEvent) {
        // Node-local delivery. Cross-node user delivery is a REST/admin op on the node's
        // `RedisAdapter` (`bridge.adapter()`); never called on the worker path in cluster mode.
        self.local.send_to_user(app, user_id, event).await
    }

    async fn terminate_user(&self, app: &str, user_id: &str) -> Vec<SocketId> {
        // Node-local terminate. Cross-node terminate is a REST/admin op on the node's
        // `RedisAdapter`; not called on the worker path in cluster mode.
        self.local.terminate_user(app, user_id).await
    }

    async fn watch(
        &self,
        app: &str,
        handle: ConnectionHandle,
        watched: Vec<String>,
    ) -> Vec<String> {
        // Capture the socket id + mailbox BEFORE `handle` is moved into the local adapter.
        // The mailbox lets the bridge send the CLUSTER-wide initial online snapshot
        // straight to this connection.
        let socket_id = handle.socket_id.clone();
        let mailbox = handle.mailbox.clone();
        // Record watchers node-locally + learn which users this node now NEWLY watches
        // (the 0→1 watcher edges that drive the bridge's per-user watch Redis SUBSCRIBE).
        let (online, newly) = self.local.watch_edges(app, handle, watched.clone());
        self.handle.watch(
            Arc::from(app),
            socket_id,
            watched,
            newly,
            mailbox,
        );
        // Return the NODE-LOCAL online set. The handler ignores it in cluster mode — the
        // authoritative CLUSTER online snapshot is sent by the bridge via the mailbox.
        online
    }

    async fn unwatch(&self, app: &str, socket_id: &SocketId) {
        // Drop this connection's watch state node-locally + learn the users whose LOCAL
        // watcher set dropped to empty here (1→0), which the bridge UNSUBSCRIBEs.
        let gone = self.local.unwatch_edges(app, socket_id);
        self.handle
            .unwatch(Arc::from(app), socket_id.clone(), gone);
    }

    async fn watchers_of(&self, app: &str, user_id: &str) -> Vec<ConnectionHandle> {
        // Node-local watchers. In cluster mode `notify_watchers` is `!clustered`-guarded and
        // the bridge does the local-watcher notify (the cluster-wide watch edge is published
        // by `watch`/`unwatch` above); reached only on the non-cluster path.
        self.local.watchers_of(app, user_id).await
    }
}
