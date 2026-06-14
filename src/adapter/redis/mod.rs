//! Redis scaling adapter — key schema, broadcast envelope, fred client wiring,
//! and the `RedisAdapter` itself.
//!
//! A3 ships a *skeleton*: every [`Adapter`] method delegates to a private
//! [`LocalAdapter`] so a `redis`-configured node behaves exactly like a `local`
//! node. Real cross-node behavior (PUBLISH/SUBSCRIBE broadcast, Redis-backed
//! presence/cache/users) is layered on in later phases (B–E) without changing
//! handler code.

pub mod client;
pub mod envelope;
pub mod keys;
pub mod pubsub;

use super::Adapter;
use crate::adapter::local::LocalAdapter;
use crate::channel::cache::CachedEvent;
use crate::channel::outcome::{ChannelSummary, SubscribeOutcome, UnsubscribeOutcome};
use crate::channel::registry::Registry;
use crate::connection::handle::ConnectionHandle;
use crate::presence::member::PresenceMember;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use crate::server::config::ServerConfig;
use crate::user::{UserJoinOutcome, UserLeaveOutcome};
use async_trait::async_trait;
use fred::clients::Pool;
use fred::interfaces::{
    EventInterface, HashesInterface, KeysInterface, PubsubInterface, SetsInterface,
};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

/// Current wall-clock time as milliseconds since the Unix epoch. Used to stamp the
/// per-member `expireAt` in the occupancy hash (the sweeper reaps stale members).
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Membership TTL heartbeat loop. Every `interval_secs`, re-stamp each LOCAL
/// member's `expireAt` in its channel's occupancy hash and bump that hash's
/// whole-key TTL, so a live node never lets its members expire. A dead node simply
/// stops ticking — its entries go stale and the per-key `EXPIRE` reaps them.
///
/// One Redis error refreshes one member; it is logged and skipped, never fatal —
/// the loop runs for the adapter's lifetime.
async fn heartbeat_loop(
    local: Arc<LocalAdapter>,
    pool: Pool,
    keys: keys::Keys,
    node_id: String,
    ttl_secs: u64,
    interval_secs: u64,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
    loop {
        ticker.tick().await;
        let expire_at = (now_ms() + ttl_secs * 1000).to_string();
        for (app, channel, socket_id) in local.local_members() {
            let occ = keys.occ(&app, &channel);
            let token = keys::member_token(&node_id, socket_id.as_str());
            // Pipeline: HSET occ token expire_at ; EXPIRE occ ttl_secs. Refreshes the
            // per-member stamp and the whole-key TTL backstop in one round-trip.
            let pipe = pool.next().pipeline();
            if let Err(e) = async {
                pipe.hset::<(), _, _>(&occ, (token.clone(), expire_at.clone()))
                    .await?;
                pipe.expire::<(), _>(&occ, ttl_secs as i64, None).await?;
                pipe.all::<()>().await
            }
            .await
            {
                tracing::warn!(
                    error = %e,
                    app, channel,
                    "redis membership heartbeat refresh failed; skipping this member"
                );
            }
        }
    }
}

/// The few `ServerConfig` knobs the Redis adapter needs to keep around for the
/// later phases (TTLs, heartbeat cadence, grace window). Cheap `Copy` struct so
/// it can be read on any task without locking.
#[derive(Clone, Copy, Debug)]
pub struct RedisConfig {
    pub membership_ttl_secs: u64,
    pub presence_heartbeat_secs: u64,
    pub node_heartbeat_secs: u64,
    pub sweep_interval_secs: u64,
    pub webhook_vacated_grace_ms: u64,
    pub sharded_pubsub: bool,
}

impl RedisConfig {
    fn from_server_config(cfg: &ServerConfig) -> Self {
        Self {
            membership_ttl_secs: cfg.redis_membership_ttl_secs,
            presence_heartbeat_secs: cfg.redis_presence_heartbeat_secs,
            node_heartbeat_secs: cfg.redis_node_heartbeat_secs,
            sweep_interval_secs: cfg.redis_sweep_interval_secs,
            webhook_vacated_grace_ms: cfg.webhook_vacated_grace_ms,
            sharded_pubsub: cfg.redis_sharded_pubsub,
        }
    }
}

/// Cross-node adapter backed by Redis. Broadcasts deliver locally and fan out over
/// Redis pub/sub; a spawned receive loop re-delivers remote broadcasts to this
/// node's local sockets. Everything else still delegates to the local adapter.
pub struct RedisAdapter {
    /// Shared with the receive loop so it can deliver remote broadcasts locally.
    local: Arc<LocalAdapter>,
    clients: client::RedisClients,
    keys: keys::Keys,
    node_id: String,
    cfg: RedisConfig,
    /// Pre-compiled (SHA-1 hashed) membership Lua scripts. Loaded into Redis lazily
    /// on first use via `evalsha_with_reload`'s NOSCRIPT fallback.
    scripts: client::Scripts,
    /// The pub/sub receive loop. Kept alive for the adapter's lifetime — dropping
    /// it would abort cross-node delivery on this node.
    #[allow(dead_code)]
    recv_handle: JoinHandle<()>,
    /// The membership TTL heartbeat. Re-stamps every local member's `expireAt` and
    /// bumps the occ-hash TTL on each tick. Kept alive for the adapter's lifetime —
    /// dropping it stops the refresh and this node's members would expire.
    #[allow(dead_code)]
    heartbeat_handle: JoinHandle<()>,
}

impl RedisAdapter {
    /// Connect to Redis (per `cfg.redis_url` / `cfg.redis_pool_size`) and build
    /// the adapter. Fails loud if Redis is unreachable.
    pub async fn new(cfg: &ServerConfig) -> anyhow::Result<Self> {
        let node_id = uuid::Uuid::new_v4().to_string();
        let keys = keys::Keys::new(&cfg.redis_prefix);
        let clients = client::RedisClients::connect(&cfg.redis_url, cfg.redis_pool_size).await?;
        let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));

        // Spawn the pub/sub receive loop. It shares the local adapter so remote
        // broadcasts land on this node's sockets. The handle is stored on the
        // struct so the task is not dropped (which would stop cross-node delivery).
        let rx = clients.sub.message_rx();
        let recv_local = local.clone();
        let recv_node = node_id.clone();
        let recv_handle =
            tokio::spawn(async move { pubsub::receive_loop(rx, recv_local, recv_node).await });

        let redis_cfg = RedisConfig::from_server_config(cfg);

        // Spawn the membership TTL heartbeat. It re-stamps every local member's
        // `expireAt` and bumps the occ-hash TTL every `presence_heartbeat_secs`, so a
        // live node never lets its members expire. fred clients are cheap clones; the
        // handle is stored so the task is not dropped (which would stop the refresh).
        let hb_local = local.clone();
        let hb_pool = clients.pool.clone();
        let hb_keys = keys.clone();
        let hb_node = node_id.clone();
        let hb_ttl = redis_cfg.membership_ttl_secs;
        let hb_interval = redis_cfg.presence_heartbeat_secs;
        let heartbeat_handle = tokio::spawn(async move {
            heartbeat_loop(hb_local, hb_pool, hb_keys, hb_node, hb_ttl, hb_interval).await
        });

        Ok(Self {
            local,
            clients,
            keys,
            node_id,
            cfg: redis_cfg,
            // `from_lua` is local (SHA-1 only) — no Redis round-trip here.
            scripts: client::Scripts::new(),
            recv_handle,
            heartbeat_handle,
        })
    }

    /// Test-support accessor: the set of Redis pub/sub channels this node's
    /// SubscriberClient is currently tracking. Used by the cluster integration
    /// tests to assert the per-(app,channel) subscription lifecycle.
    #[doc(hidden)]
    pub fn tracked_redis_channels(&self) -> Vec<String> {
        self.clients
            .sub
            .tracked_channels()
            .into_iter()
            .map(|c| c.to_string())
            .collect()
    }
}

#[async_trait]
impl Adapter for RedisAdapter {
    async fn subscribe(
        &self,
        app: &str,
        channel: &str,
        handle: ConnectionHandle,
        member: Option<PresenceMember>,
    ) -> SubscribeOutcome {
        // Capture the socket id BEFORE `handle` is moved into the local adapter —
        // we need it below to form this connection's member token for Redis.
        let socket_id = handle.socket_id.clone();

        let mut out = self.local.subscribe(app, channel, handle, member).await;

        // The Redis-subscription lifecycle is keyed on the node-LOCAL subscriber
        // edge: subscribe to the msg channel when this node goes 0 → 1. We capture
        // the local count now because the cluster count (below) overwrites
        // `out.subscription_count` — the lifecycle must stay on the local edge.
        let local_count = out.subscription_count;
        if local_count == 1 {
            let msg_key = self.keys.msg(app, channel);
            if let Err(e) = self.clients.sub.subscribe(msg_key.clone()).await {
                // The local subscription already succeeded; a Redis SUBSCRIBE
                // failure only costs cross-node delivery for this channel on this
                // node. Log loudly but never panic the connection task.
                tracing::warn!(
                    error = %e,
                    channel = %msg_key,
                    "failed to SUBSCRIBE to Redis msg channel on 0→1 edge"
                );
            }
        }

        // Record cluster-wide membership and read back the AUTHORITATIVE count.
        // Atomic Lua: HSET member, refresh whole-key TTL, HLEN, index on the 0→1
        // cluster edge. On any Redis error, keep the node-local outcome (graceful
        // degradation — a membership write failure must never fail the subscribe).
        let ttl_secs = self.cfg.membership_ttl_secs;
        let occ = self.keys.occ(app, channel);
        let chans = self.keys.chans(app);
        let token = keys::member_token(&self.node_id, socket_id.as_str());
        let argv = vec![
            token,
            (now_ms() + ttl_secs * 1000).to_string(),
            ttl_secs.to_string(),
            channel.to_string(),
        ];
        match self
            .scripts
            .subscribe
            .evalsha_with_reload::<i64, _, _>(self.clients.pool.next(), vec![occ, chans], argv)
            .await
        {
            Ok(count) => {
                out.subscription_count = count as usize;
                out.occupied = count == 1;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    app, channel,
                    "redis SUBSCRIBE membership script failed; keeping node-local count"
                );
            }
        }

        out
    }

    async fn unsubscribe(
        &self,
        app: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> UnsubscribeOutcome {
        let mut out = self.local.unsubscribe(app, channel, socket_id).await;

        // Mirror of `subscribe`: tear down the Redis subscription on the node-LOCAL
        // 1 → 0 edge. Keyed on the local count (see note in `subscribe`): the cluster
        // count below overwrites `out.subscription_count`, so the lifecycle decision
        // must read the node-local count captured here.
        let local_count = out.subscription_count;
        if local_count == 0 {
            let msg_key = self.keys.msg(app, channel);
            if let Err(e) = self.clients.sub.unsubscribe(msg_key.clone()).await {
                tracing::warn!(
                    error = %e,
                    channel = %msg_key,
                    "failed to UNSUBSCRIBE from Redis msg channel on 1→0 edge"
                );
            }
        }

        // Remove cluster-wide membership and read back the AUTHORITATIVE remaining
        // count. Atomic Lua: HDEL member, HLEN, and on the 1→0 cluster edge DEL the
        // now-empty hash + de-index. On Redis error, keep the node-local outcome.
        let occ = self.keys.occ(app, channel);
        let chans = self.keys.chans(app);
        let token = keys::member_token(&self.node_id, socket_id.as_str());
        let argv = vec![token, channel.to_string()];
        match self
            .scripts
            .unsubscribe
            .evalsha_with_reload::<i64, _, _>(self.clients.pool.next(), vec![occ, chans], argv)
            .await
        {
            Ok(count) => {
                out.subscription_count = count as usize;
                out.vacated = count == 0;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    app, channel,
                    "redis UNSUBSCRIBE membership script failed; keeping node-local count"
                );
            }
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
        // 1. Local delivery on THIS node — typed event, honouring `except`.
        self.local
            .broadcast(app, channel, event.clone(), except.clone())
            .await;

        // 2. Fan out to the rest of the cluster. Publish the *pre-encoded* v7 frame
        //    so remote nodes deliver it verbatim (no re-encoding). Always publish —
        //    even with no local subscribers — because a REST trigger may land on a
        //    node where the channel is only subscribed elsewhere.
        let frame = crate::protocol::v7::frames::encode(&event);
        let env = envelope::Envelope {
            node_id: self.node_id.clone(),
            app: app.to_string(),
            channel: channel.to_string(),
            event: serde_json::Value::String(frame),
            except: except.as_ref().map(|s| s.as_str().to_string()),
        };
        // Publish as a UTF-8 string (the envelope JSON is valid UTF-8); the receive
        // loop reads it back with `Value::into_string()` — a proven round-trip.
        let payload = match String::from_utf8(env.encode()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, app, channel, "envelope was not valid UTF-8");
                return;
            }
        };
        let key = self.keys.msg(app, channel);
        if let Err(e) = self
            .clients
            .pool
            .next()
            .publish::<(), _, _>(key, payload)
            .await
        {
            tracing::warn!(error = %e, app, channel, "redis publish failed");
        }
    }

    async fn channels(&self, app: &str, prefix: Option<&str>) -> Vec<ChannelSummary> {
        // Cluster-wide view: the app's active-channels set is the source of truth
        // for which channels are occupied; `HLEN occ` is each one's cluster count.
        let client = self.clients.pool.next();
        let members: Result<Vec<String>, _> = client.smembers(self.keys.chans(app)).await;
        let members = match members {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, app, "redis SMEMBERS chans failed; falling back to local channels");
                return self.local.channels(app, prefix).await;
            }
        };

        let mut out = Vec::new();
        for name in members {
            if let Some(p) = prefix {
                if !name.starts_with(p) {
                    continue;
                }
            }
            let count: Result<i64, _> = client.hlen(self.keys.occ(app, &name)).await;
            let count = match count {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(error = %e, app, channel = %name, "redis HLEN occ failed; falling back to local channels");
                    return self.local.channels(app, prefix).await;
                }
            };
            // A channel indexed in the set but with HLEN 0 is mid-vacate; skip it so
            // callers never see a phantom occupied channel.
            if count <= 0 {
                continue;
            }
            // `user_count` (the presence roster) stays node-local in SP7a.
            let user_count = self.local.channel(app, &name).await.user_count;
            out.push(ChannelSummary {
                name,
                occupied: true,
                subscription_count: count as usize,
                user_count,
            });
        }
        out
    }

    async fn channel(&self, app: &str, channel: &str) -> ChannelSummary {
        // `HLEN occ` is the authoritative cluster-wide subscription count; the
        // presence roster (`user_count`) stays node-local in SP7a.
        let count: Result<i64, _> = self
            .clients
            .pool
            .next()
            .hlen(self.keys.occ(app, channel))
            .await;
        match count {
            Ok(count) => {
                let user_count = self.local.channel(app, channel).await.user_count;
                ChannelSummary {
                    name: channel.to_string(),
                    occupied: count > 0,
                    subscription_count: count as usize,
                    user_count,
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, app, channel, "redis HLEN occ failed; falling back to local channel");
                self.local.channel(app, channel).await
            }
        }
    }

    async fn presence_members(&self, app: &str, channel: &str) -> Vec<PresenceMember> {
        self.local.presence_members(app, channel).await
    }

    async fn cache_set(&self, app: &str, channel: &str, event: CachedEvent, ttl: Duration) {
        self.local.cache_set(app, channel, event, ttl).await
    }

    async fn cache_get(&self, app: &str, channel: &str) -> Option<CachedEvent> {
        self.local.cache_get(app, channel).await
    }

    async fn signin_user(
        &self,
        app: &str,
        user_id: &str,
        handle: ConnectionHandle,
    ) -> UserJoinOutcome {
        self.local.signin_user(app, user_id, handle).await
    }

    async fn signout_user(
        &self,
        app: &str,
        user_id: &str,
        socket_id: &SocketId,
    ) -> UserLeaveOutcome {
        self.local.signout_user(app, user_id, socket_id).await
    }

    async fn is_user_online(&self, app: &str, user_id: &str) -> bool {
        self.local.is_user_online(app, user_id).await
    }

    async fn send_to_user(&self, app: &str, user_id: &str, event: ServerEvent) {
        self.local.send_to_user(app, user_id, event).await
    }

    async fn terminate_user(&self, app: &str, user_id: &str) -> Vec<SocketId> {
        self.local.terminate_user(app, user_id).await
    }

    async fn watch(
        &self,
        app: &str,
        handle: ConnectionHandle,
        watched: Vec<String>,
    ) -> Vec<String> {
        self.local.watch(app, handle, watched).await
    }

    async fn unwatch(&self, app: &str, socket_id: &SocketId) {
        self.local.unwatch(app, socket_id).await
    }

    async fn watchers_of(&self, app: &str, user_id: &str) -> Vec<ConnectionHandle> {
        self.local.watchers_of(app, user_id).await
    }
}
