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
pub mod presence;
pub mod pubsub;
pub mod sweeper;
pub mod user;

use super::Adapter;
use crate::adapter::local::LocalAdapter;
use crate::channel::cache::CachedEvent;
use crate::channel::outcome::{ChannelSummary, SubscribeOutcome, UnsubscribeOutcome};
use crate::channel::registry::Registry;
use crate::connection::handle::ConnectionHandle;
use crate::presence::member::PresenceMember;
use crate::protocol::event::{PresencePayload, ServerEvent};
use crate::protocol::socket_id::SocketId;
use crate::server::config::ServerConfig;
use crate::user::{UserJoinOutcome, UserLeaveOutcome};
use async_trait::async_trait;
use fred::clients::Pool;
use fred::interfaces::{
    EventInterface, HashesInterface, KeysInterface, PubsubInterface, SetsInterface,
};
use fred::types::Expiration;
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

        // Re-stamp this node's own user bindings (the `usr(app,user)` HASH), exactly as
        // for channel members above: a live node keeps its bindings' `expireAt` in the
        // future so the sweeper never reaps them; a crashed node stops ticking and its
        // bindings go stale, firing the cluster offline edge once the user's last
        // cluster connection (on the dead node) is reaped.
        for (app, user_id, socket_id) in local.local_user_bindings() {
            let usr = keys.usr(&app, &user_id);
            let token = keys::member_token(&node_id, socket_id.as_str());
            let pipe = pool.next().pipeline();
            if let Err(e) = async {
                pipe.hset::<(), _, _>(&usr, (token.clone(), expire_at.clone()))
                    .await?;
                pipe.expire::<(), _>(&usr, ttl_secs as i64, None).await?;
                pipe.all::<()>().await
            }
            .await
            {
                tracing::warn!(error = %e, app, user_id, "redis user-binding heartbeat refresh failed; skipping");
            }
        }
    }
}

/// Node-liveness heartbeat loop. Every `interval_secs`, advertise this node as alive:
/// `SET node(node_id) "1" EX (3 * interval_secs)` (so a missed beat still leaves slack)
/// and `SADD nodes node_id`. A dead node simply stops ticking — its `node` key TTL-
/// expires, and the sweeper's dead-node prune removes it from the `nodes` set.
///
/// One Redis error is logged and skipped, never fatal — the loop runs for the
/// adapter's lifetime.
async fn node_heartbeat_loop(pool: Pool, keys: keys::Keys, node_id: String, interval_secs: u64) {
    let interval = interval_secs.max(1);
    let ttl_secs = (3 * interval) as i64;
    let mut ticker = tokio::time::interval(Duration::from_secs(interval));
    loop {
        ticker.tick().await;
        let node_key = keys.node(&node_id);
        if let Err(e) = pool
            .next()
            .set::<(), _, _>(
                &node_key,
                "1",
                Some(fred::types::Expiration::EX(ttl_secs)),
                None,
                false,
            )
            .await
        {
            tracing::warn!(error = %e, node_id, "redis node heartbeat SET failed; skipping this tick");
        }
        if let Err(e) = pool
            .next()
            .sadd::<i64, _, _>(keys.nodes(), node_id.clone())
            .await
        {
            tracing::warn!(error = %e, node_id, "redis node heartbeat SADD nodes failed; skipping this tick");
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
    /// The node-liveness heartbeat. Re-stamps `node(node_id)` (with a TTL) and SADDs
    /// `node_id` to the `nodes` set each tick. Kept alive for the adapter's lifetime —
    /// dropping it stops the heartbeat and this node's `node` key TTL-expires.
    #[allow(dead_code)]
    node_heartbeat_handle: JoinHandle<()>,
    /// The lease-locked occupancy sweeper. Started LATER via [`RedisAdapter::start_sweeper`]
    /// once the `WebhookHandle` exists (it can't start in `new()` because the webhook
    /// dispatcher needs the adapter-backed occupancy source — a construction cycle the
    /// deferred start breaks). Stored so the task is not dropped.
    sweeper_handle: std::sync::Mutex<Option<JoinHandle<()>>>,
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

        // Spawn the node-liveness heartbeat. It advertises this node as alive every
        // `node_heartbeat_secs` (re-stamping the `node` key with a TTL and SADDing to
        // the `nodes` set), so a dead node's `node` key simply TTL-expires.
        let nh_pool = clients.pool.clone();
        let nh_keys = keys.clone();
        let nh_node = node_id.clone();
        let nh_interval = redis_cfg.node_heartbeat_secs;
        let node_heartbeat_handle = tokio::spawn(async move {
            node_heartbeat_loop(nh_pool, nh_keys, nh_node, nh_interval).await
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
            node_heartbeat_handle,
            // The sweeper is started later via `start_sweeper` once the webhook
            // handle exists (see the doc on the field).
            sweeper_handle: std::sync::Mutex::new(None),
        })
    }

    /// Start the lease-locked occupancy sweeper. Called from `main.rs` AFTER the
    /// webhook dispatcher is spawned (the sweeper needs the `WebhookHandle`, and the
    /// dispatcher needs the adapter-backed occupancy source — starting the sweeper
    /// here, rather than in `new()`, breaks that construction cycle).
    ///
    /// The sweep interval comes from config; the lease is sized to outlive a tick
    /// (`max(interval*3s, 5s)`) so the holder keeps the lease across ticks but it
    /// auto-frees (PX expiry) if the holder dies. The spawned handle is stored so the
    /// task is not dropped.
    pub fn start_sweeper(&self, webhooks: crate::webhook::WebhookHandle) {
        let interval_secs = self.cfg.sweep_interval_secs.max(1);
        let lease_ms = (interval_secs * 1000 * 3).max(5000);
        let pool = self.clients.pool.clone();
        let keys = self.keys.clone();
        let node_id = self.node_id.clone();
        let handle = tokio::spawn(async move {
            sweeper::sweeper_loop(pool, keys, node_id, lease_ms, interval_secs, webhooks).await
        });
        if let Ok(mut guard) = self.sweeper_handle.lock() {
            *guard = Some(handle);
        }
    }

    /// Test-support hook: run one deterministic sweep pass with the adapter's own
    /// pool/keys/node_id and the given `now` millis, returning `(acquired, reaped,
    /// vacated)`. The integration tests live in an external crate and cannot see the
    /// `pub(crate)` `sweep_once`, so this thin `#[doc(hidden)] pub` seam exposes it.
    #[doc(hidden)]
    pub async fn sweep_now(
        &self,
        webhooks: &crate::webhook::WebhookHandle,
        now_ms: u64,
    ) -> (bool, usize, Vec<(String, String)>) {
        let lease_ms = (self.cfg.sweep_interval_secs.max(1) * 1000 * 3).max(5000);
        let report = sweeper::sweep_once(
            &self.clients.pool,
            &self.keys,
            &self.node_id,
            lease_ms,
            webhooks,
            now_ms,
        )
        .await;
        (report.acquired, report.reaped, report.vacated)
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

/// Cluster-only coordination ops, factored out of the `Adapter` trait methods.
///
/// Each of these does ONLY the Redis/cluster half of an operation — the SUBSCRIBE_LUA /
/// presence / user / pub-sub work — and performs NO `self.local.*` call. The local half
/// (and the node-first / node-last edge it computes) is the caller's responsibility: the
/// `Adapter` impl below threads it in from its own `LocalAdapter`, and the percore
/// `ClusterBridge` will thread it in from its worker-local `LocalAdapter`. Behavior is
/// identical to the inline code these were extracted from — they are the single source of
/// truth that both callers now share.
impl RedisAdapter {
    /// Cluster half of `subscribe`: record cluster-wide membership (SUBSCRIBE_LUA), index
    /// the app, and drive the node-local Redis `msg`-channel subscribe-on-first lifecycle.
    ///
    /// `node_first` is the node-local 0→1 subscriber edge (the caller computes it from its
    /// own `LocalAdapter` — `out.subscription_count == 1`). Returns the AUTHORITATIVE
    /// `(cluster_count, occupied)`. On any Redis error every step degrades gracefully
    /// (logged, never fatal); the returned count then stays `0` and the caller keeps its
    /// node-local outcome, exactly as the inline path did.
    #[doc(hidden)]
    pub async fn cluster_subscribe(
        &self,
        app: &str,
        channel: &str,
        socket_id: &SocketId,
        node_first: bool,
    ) -> (usize, bool) {
        // Subscribe to the msg channel when this NODE goes 0 → 1 for the channel.
        if node_first {
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
        // cluster edge. On any Redis error, report a zero count so the caller keeps
        // its node-local outcome (graceful degradation — a membership write failure
        // must never fail the subscribe).
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
        let mut count = 0usize;
        let mut occupied = false;
        match self
            .scripts
            .subscribe
            .evalsha_with_reload::<i64, _, _>(self.clients.pool.next(), vec![occ, chans], argv)
            .await
        {
            Ok(c) => {
                count = c as usize;
                occupied = c == 1;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    app, channel,
                    "redis SUBSCRIBE membership script failed; keeping node-local count"
                );
            }
        }

        // Index the app so the sweeper can enumerate it (SMEMBERS apps → SMEMBERS
        // chans(app)). Idempotent and cheap; the apps set is bounded by configured
        // apps so it needs no cleanup. Log + ignore errors — this is best-effort.
        if let Err(e) = self
            .clients
            .pool
            .next()
            .sadd::<i64, _, _>(self.keys.apps(), app.to_string())
            .await
        {
            tracing::warn!(error = %e, app, "redis SADD apps failed; sweeper may miss this app");
        }

        (count, occupied)
    }

    /// Cluster half of `unsubscribe`: remove cluster-wide membership (UNSUBSCRIBE_LUA) and
    /// tear down the node-local Redis `msg`-channel subscription on the node-local 1 → 0
    /// edge (`node_last`, computed by the caller as `out.subscription_count == 0`). Returns
    /// the AUTHORITATIVE `(cluster_count, vacated)`; on Redis error returns `(0, false)` so
    /// the caller keeps its node-local outcome.
    #[doc(hidden)]
    pub async fn cluster_unsubscribe(
        &self,
        app: &str,
        channel: &str,
        socket_id: &SocketId,
        node_last: bool,
    ) -> (usize, bool) {
        // Tear down the Redis subscription on the node-LOCAL 1 → 0 edge.
        if node_last {
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
        // now-empty hash + de-index. On Redis error, report a zero count so the caller
        // keeps its node-local outcome.
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
            Ok(count) => (count as usize, count == 0),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    app, channel,
                    "redis UNSUBSCRIBE membership script failed; keeping node-local count"
                );
                (0, false)
            }
        }
    }

    /// Cluster half of a presence join: PRESENCE_JOIN refcount + cluster roster read.
    /// Returns `(first_for_user, cluster_roster)`. Propagates the Redis error (the caller
    /// keeps its node-local join on `Err`, as the inline path did).
    #[doc(hidden)]
    pub async fn cluster_presence_join(
        &self,
        app: &str,
        channel: &str,
        member: &PresenceMember,
        socket_id: &SocketId,
    ) -> anyhow::Result<(bool, PresencePayload)> {
        presence::join(
            &self.scripts,
            &self.clients.pool,
            &self.keys,
            &self.node_id,
            app,
            channel,
            member,
            socket_id,
        )
        .await
    }

    /// Cluster half of a presence leave: PRESENCE_LEAVE refcount. Returns `last_for_user`.
    /// Propagates the Redis error (the caller keeps its node-local leave on `Err`).
    #[doc(hidden)]
    pub async fn cluster_presence_leave(
        &self,
        app: &str,
        channel: &str,
        user_id: &str,
        socket_id: &SocketId,
    ) -> anyhow::Result<bool> {
        presence::leave(
            &self.scripts,
            &self.clients.pool,
            &self.keys,
            &self.node_id,
            app,
            channel,
            user_id,
            socket_id,
        )
        .await
    }

    /// Cluster presence capacity probe for the presence-subscribe admission check: the
    /// cluster distinct-user count (`HLEN presusers`) and whether `user_id` is already in
    /// the cluster roster (`HEXISTS presusers user_id`). Both reads are best-effort: a
    /// Redis error degrades to `(0, false)` so the capacity gate fails open rather than
    /// rejecting a join on a transient blip.
    #[doc(hidden)]
    pub async fn cluster_presence_capacity(
        &self,
        app: &str,
        channel: &str,
        user_id: &str,
    ) -> (usize, bool) {
        let count = match presence::user_count(&self.clients.pool, &self.keys, app, channel).await {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(error = %e, app, channel, "redis presence user_count failed; capacity check degrades to 0");
                0
            }
        };
        let already_member: bool = match self
            .clients
            .pool
            .next()
            .hexists(self.keys.presusers(app, channel), user_id)
            .await
        {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, app, channel, user_id, "redis HEXISTS presusers failed; treating as not-yet-member");
                false
            }
        };
        (count, already_member)
    }

    /// Cluster half of `signin_user`: USER_SIGNIN refcount, the node-local `usermsg`
    /// subscribe-on-first lifecycle, the `apps` index, and the WatchOnline publish on the
    /// cluster 0→1 edge. `node_first` is the node-local first-connection edge (the caller
    /// computes it as `out.first_for_user`). Returns the cluster `first_for_user`; on Redis
    /// error returns `node_first` so the caller keeps its node-local outcome.
    #[doc(hidden)]
    pub async fn cluster_signin(
        &self,
        app: &str,
        user_id: &str,
        socket_id: &SocketId,
        node_first: bool,
    ) -> bool {
        // usermsg sub lifecycle on the node-LOCAL first-connection edge: when this node
        // gains its first connection for the user (0→1), SUBSCRIBE the per-user `usermsg`
        // channel so cross-node send/terminate reach this node.
        if node_first {
            if let Err(e) = self
                .clients
                .sub
                .subscribe(self.keys.usermsg(app, user_id))
                .await
            {
                tracing::warn!(error = %e, app, user_id, "failed to SUBSCRIBE usermsg on local 0→1");
            }
        }

        // Index the app so the sweeper can enumerate it (SMEMBERS apps → SMEMBERS
        // users(app)) for the user-binding sweep — exactly as `subscribe` does for the
        // channel sweep. Without this, a user that only ever SIGNED IN (never subscribed
        // a channel) would leave `apps` empty and the sweeper could not reap its stale
        // bindings on a crash. Idempotent + cheap; log + ignore errors (best-effort).
        if let Err(e) = self
            .clients
            .pool
            .next()
            .sadd::<i64, _, _>(self.keys.apps(), app.to_string())
            .await
        {
            tracing::warn!(error = %e, app, "redis SADD apps (signin) failed; sweeper may miss this app");
        }

        // Cluster online edge: USER_SIGNIN returns the cluster `first_for_user`
        // (HLEN == 1). On any Redis error, report false so the caller keeps node-local.
        match user::signin(
            &self.scripts,
            &self.clients.pool,
            &self.keys,
            &self.node_id,
            app,
            user_id,
            socket_id,
            self.cfg.membership_ttl_secs,
        )
        .await
        {
            Ok(first) => {
                if first {
                    // Notify the cluster the user came online. Remote nodes deliver
                    // it to their local watchers; the origin's local watchers are
                    // notified directly (self-dedup'd by the receive loop).
                    user::publish(
                        &self.clients.pool,
                        &self.keys.watch(app, user_id),
                        &self.node_id,
                        app,
                        user_id,
                        envelope::EnvelopeKind::WatchOnline,
                        serde_json::Value::Null,
                    )
                    .await;
                }
                first
            }
            Err(e) => {
                tracing::warn!(error = %e, app, user_id, "redis user signin failed; keeping node-local");
                node_first
            }
        }
    }

    /// Cluster half of `signout_user`: USER_SIGNOUT refcount, the node-local `usermsg`
    /// unsubscribe-on-last lifecycle, and the WatchOffline publish on the cluster 1→0
    /// edge. `node_last` is the node-local last-connection edge (the caller computes it as
    /// `out.last_for_user`). Returns the cluster `last_for_user`; on Redis error returns
    /// `node_last` so the caller keeps its node-local outcome.
    #[doc(hidden)]
    pub async fn cluster_signout(
        &self,
        app: &str,
        user_id: &str,
        socket_id: &SocketId,
        node_last: bool,
    ) -> bool {
        // usermsg sub teardown on the node-LOCAL last-connection edge (1→0).
        if node_last {
            if let Err(e) = self
                .clients
                .sub
                .unsubscribe(self.keys.usermsg(app, user_id))
                .await
            {
                tracing::warn!(error = %e, app, user_id, "failed to UNSUBSCRIBE usermsg on local 1→0");
            }
        }

        // Cluster offline edge: USER_SIGNOUT returns the cluster `last_for_user`
        // (HLEN == 0). On any Redis error, report false so the caller keeps node-local.
        match user::signout(
            &self.scripts,
            &self.clients.pool,
            &self.keys,
            &self.node_id,
            app,
            user_id,
            socket_id,
        )
        .await
        {
            Ok(last) => {
                if last {
                    user::publish(
                        &self.clients.pool,
                        &self.keys.watch(app, user_id),
                        &self.node_id,
                        app,
                        user_id,
                        envelope::EnvelopeKind::WatchOffline,
                        serde_json::Value::Null,
                    )
                    .await;
                }
                last
            }
            Err(e) => {
                tracing::warn!(error = %e, app, user_id, "redis user signout failed; keeping node-local");
                node_last
            }
        }
    }

    /// Cluster half of `watch`: SUBSCRIBE the per-user `watch` Redis channel for every
    /// `newly_watched` user (the users whose node-LOCAL watcher set just went 0→1, which
    /// the caller computes from `LocalAdapter::watch_edges`) so this node receives their
    /// cluster online/offline transitions, then return the cluster-wide online subset of
    /// `watched`. Per-user `is_online` errors degrade to the node-local check for that
    /// user's snapshot entry, exactly as the inline path did.
    #[doc(hidden)]
    pub async fn cluster_watch(
        &self,
        app: &str,
        watched: &[String],
        newly_watched: &[String],
    ) -> Vec<String> {
        // Subscribe to each newly-watched user's watch channel so this node receives
        // their cluster online/offline transitions.
        for u in newly_watched {
            if let Err(e) = self.clients.sub.subscribe(self.keys.watch(app, u)).await {
                tracing::warn!(error = %e, app, user = %u, "failed to SUBSCRIBE watch channel");
            }
        }
        // Cluster-wide initial online snapshot: is_user_online per watched user.
        let mut online = Vec::new();
        for u in watched {
            match user::is_online(&self.clients.pool, &self.keys, app, u).await {
                Ok(true) => online.push(u.clone()),
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(error = %e, app, user = %u, "redis is_online failed; using local for snapshot");
                    if self.local.is_user_online(app, u).await {
                        online.push(u.clone());
                    }
                }
            }
        }
        online
    }

    /// Cluster half of `unwatch`: UNSUBSCRIBE the per-user `watch` Redis channels for the
    /// users whose node-LOCAL watcher set just went 1→0 (`no_longer_watched`, computed by
    /// the caller from `LocalAdapter::unwatch_edges`).
    #[doc(hidden)]
    pub async fn cluster_unwatch(&self, app: &str, no_longer_watched: &[String]) {
        for u in no_longer_watched {
            if let Err(e) = self.clients.sub.unsubscribe(self.keys.watch(app, u)).await {
                tracing::warn!(error = %e, app, user = %u, "failed to UNSUBSCRIBE watch channel");
            }
        }
    }

    /// Cluster half of `broadcast`: PUBLISH the Broadcast envelope (the pre-encoded v7
    /// `frame`) on the channel's `msg` pub/sub key so every other node delivers it. The
    /// local delivery is done separately by the caller. Always publishes — even with no
    /// local subscribers — because a REST trigger may land on a node where the channel is
    /// only subscribed elsewhere. Best-effort: logs + returns on any Redis error.
    #[doc(hidden)]
    pub async fn cluster_publish_broadcast(
        &self,
        app: &str,
        channel: &str,
        frame: String,
        except: Option<&SocketId>,
    ) {
        let env = envelope::Envelope {
            node_id: self.node_id.clone(),
            app: app.to_string(),
            kind: envelope::EnvelopeKind::Broadcast,
            channel: channel.to_string(),
            event: serde_json::Value::String(frame),
            except: except.map(|s| s.as_str().to_string()),
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
}

impl Drop for RedisAdapter {
    /// Dropping the adapter "crashes" this node: abort every background task so it
    /// stops re-stamping its members' `expireAt` (membership heartbeat) and stops
    /// advertising liveness (node heartbeat). A `tokio::JoinHandle` detaches on drop
    /// rather than aborting, so without this the heartbeats would outlive the adapter
    /// and the node's members would never go stale — defeating the sweeper. Aborting
    /// here makes a dropped adapter behave exactly like a crashed node.
    fn drop(&mut self) {
        self.recv_handle.abort();
        self.heartbeat_handle.abort();
        self.node_heartbeat_handle.abort();
        if let Ok(guard) = self.sweeper_handle.lock() {
            if let Some(h) = guard.as_ref() {
                h.abort();
            }
        }
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
        let node_first = out.subscription_count == 1;

        // Cluster half: SUBSCRIBE_LUA authoritative count + occupied edge + the node-local
        // msg-channel subscribe-on-first + the app index. On any Redis error this returns a
        // zero count and we KEEP the node-local outcome (graceful degradation — a membership
        // write failure must never fail the subscribe).
        let (count, occupied) = self
            .cluster_subscribe(app, channel, &socket_id, node_first)
            .await;
        if count > 0 {
            out.subscription_count = count;
            out.occupied = occupied;
        }

        // Presence: overwrite the node-local PresenceJoin with cluster truth — the
        // first_for_user edge (HINCRBY refcount) and the cluster-wide roster. On any
        // Redis error keep the node-local join (graceful degradation).
        if let Some(join) = out.presence.as_mut() {
            match self
                .cluster_presence_join(app, channel, &join.member, &socket_id)
                .await
            {
                Ok((first_for_user, roster)) => {
                    join.first_for_user = first_for_user;
                    join.roster = roster;
                }
                Err(e) => {
                    tracing::warn!(error = %e, app, channel, "redis presence join failed; keeping node-local roster");
                }
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
        let node_last = out.subscription_count == 0;

        // Cluster half: the node-local msg-channel unsubscribe-on-last + UNSUBSCRIBE_LUA
        // authoritative remaining count + vacated edge. On Redis error this returns the
        // `(0, false)` sentinel — which a real success never produces (a real 1→0 success is
        // `(0, true)`) — so we keep the node-local outcome only in that exact case.
        let (count, vacated) = self
            .cluster_unsubscribe(app, channel, socket_id, node_last)
            .await;
        if count > 0 || vacated {
            out.subscription_count = count;
            out.vacated = vacated;
        }

        // Presence: overwrite last_for_user with the cluster refcount edge.
        if let Some(leave) = out.presence.as_mut() {
            match self
                .cluster_presence_leave(app, channel, &leave.user_id, socket_id)
                .await
            {
                Ok(last_for_user) => leave.last_for_user = last_for_user,
                Err(e) => {
                    tracing::warn!(error = %e, app, channel, "redis presence leave failed; keeping node-local last_for_user");
                }
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
        self.cluster_publish_broadcast(app, channel, frame, except.as_ref())
            .await;
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
            // Presence `user_count` (distinct-user roster) is cluster-wide via Redis;
            // non-presence channels have none.
            let user_count = if presence::is_presence(&name) {
                presence::user_count(&self.clients.pool, &self.keys, app, &name)
                    .await
                    .ok()
            } else {
                None
            };
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
        // `HLEN occ` is the authoritative cluster-wide subscription count; for
        // presence channels `user_count` is the cluster `HLEN presusers` (SP7b).
        let count: Result<i64, _> = self
            .clients
            .pool
            .next()
            .hlen(self.keys.occ(app, channel))
            .await;
        match count {
            Ok(count) => {
                let user_count = if presence::is_presence(channel) {
                    match presence::user_count(&self.clients.pool, &self.keys, app, channel).await {
                        Ok(n) => Some(n),
                        Err(e) => {
                            tracing::warn!(error = %e, app, channel, "redis presence user_count failed; using local");
                            self.local.channel(app, channel).await.user_count
                        }
                    }
                } else {
                    None
                };
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
        match presence::members(&self.clients.pool, &self.keys, app, channel).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, app, channel, "redis presence_members failed; falling back to local");
                self.local.presence_members(app, channel).await
            }
        }
    }

    async fn cache_set(&self, app: &str, channel: &str, event: CachedEvent, ttl: Duration) {
        let key = self.keys.cache(app, channel);
        let json = match serde_json::to_string(&event) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, app, channel, "redis cache_set serialize failed");
                return;
            }
        };
        let ttl_ms = ttl.as_millis() as u64;
        // Redis `PX 0` (or negative) is an error. A ttl of 0 means "immediately
        // expired", so we SKIP the SET entirely — a subsequent `cache_get` then sees
        // no key and returns None. This mirrors the `LocalAdapter`'s `<` expiry
        // semantics (a ttl-0 entry is treated as already expired) without writing a
        // doomed key. The production cache ttl (`cache_ttl_secs`, default 1800s) is
        // always non-zero, so this only guards the degenerate case.
        if ttl_ms == 0 {
            return;
        }
        if let Err(e) = self
            .clients
            .pool
            .next()
            .set::<(), _, _>(key, json, Some(Expiration::PX(ttl_ms as i64)), None, false)
            .await
        {
            tracing::warn!(error = %e, app, channel, "redis cache_set failed");
        }
    }

    async fn cache_get(&self, app: &str, channel: &str) -> Option<CachedEvent> {
        let key = self.keys.cache(app, channel);
        // GET returns nil → `None` after the PX TTL elapses; Redis handles expiry
        // natively so there is NO manual expiry check here (unlike `LocalAdapter`).
        let raw: Option<String> = match self.clients.pool.next().get(key).await {
            Ok(v) => v,
            Err(e) => {
                // Degrade to a benign cache_miss. Do NOT fall back to the node-local
                // cache — that would be cross-node-inconsistent.
                tracing::warn!(error = %e, app, channel, "redis cache_get failed");
                return None;
            }
        };
        raw.and_then(|s| serde_json::from_str::<CachedEvent>(&s).ok())
    }

    async fn signin_user(
        &self,
        app: &str,
        user_id: &str,
        handle: ConnectionHandle,
    ) -> UserJoinOutcome {
        // Capture the socket id BEFORE `handle` is moved into the local adapter —
        // we need it to form this connection's binding token for Redis.
        let socket_id = handle.socket_id.clone();
        let mut out = self.local.signin_user(app, user_id, handle).await;

        // The node-LOCAL first-connection edge, read BEFORE the cluster half overwrites
        // `out.first_for_user`: when this node gains its first connection for the user
        // (0→1), the cluster half SUBSCRIBEs the per-user `usermsg` channel.
        let node_first = out.first_for_user;

        // Cluster half: USER_SIGNIN refcount (the cluster `first_for_user`) + usermsg
        // subscribe-on-first + the app index + the WatchOnline publish on the cluster 0→1
        // edge. The cluster `first_for_user` overwrites the node-local one; on any Redis
        // error this returns false, keeping the node-local outcome.
        out.first_for_user = self
            .cluster_signin(app, user_id, &socket_id, node_first)
            .await;
        out
    }

    async fn signout_user(
        &self,
        app: &str,
        user_id: &str,
        socket_id: &SocketId,
    ) -> UserLeaveOutcome {
        let mut out = self.local.signout_user(app, user_id, socket_id).await;

        // The node-LOCAL last-connection edge (1→0), read BEFORE the cluster half
        // overwrites `out.last_for_user`: on it the cluster half UNSUBSCRIBEs the per-user
        // `usermsg` channel.
        let node_last = out.last_for_user;

        // Cluster half: USER_SIGNOUT refcount (the cluster `last_for_user`) + usermsg
        // unsubscribe-on-last + the WatchOffline publish on the cluster 1→0 edge. The
        // cluster `last_for_user` overwrites the node-local one; on any Redis error this
        // returns `node_last`, keeping the node-local outcome.
        out.last_for_user = self
            .cluster_signout(app, user_id, socket_id, node_last)
            .await;
        out
    }

    async fn is_user_online(&self, app: &str, user_id: &str) -> bool {
        match user::is_online(&self.clients.pool, &self.keys, app, user_id).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, app, user_id, "redis is_user_online failed; falling back to local");
                self.local.is_user_online(app, user_id).await
            }
        }
    }

    async fn send_to_user(&self, app: &str, user_id: &str, event: ServerEvent) {
        // Deliver to this node's local connections of the user, then fan the
        // pre-encoded frame out to every other node holding a connection of the user.
        self.local.send_to_user(app, user_id, event.clone()).await;
        let frame = crate::protocol::v7::frames::encode(&event);
        user::publish(
            &self.clients.pool,
            &self.keys.usermsg(app, user_id),
            &self.node_id,
            app,
            user_id,
            envelope::EnvelopeKind::UserSend,
            serde_json::Value::String(frame),
        )
        .await;
    }

    async fn terminate_user(&self, app: &str, user_id: &str) -> Vec<SocketId> {
        // Terminate this node's local connections (returns their ids), then fan a
        // terminate control out to every other node holding a connection of the user.
        let ids = self.local.terminate_user(app, user_id).await;
        user::publish(
            &self.clients.pool,
            &self.keys.usermsg(app, user_id),
            &self.node_id,
            app,
            user_id,
            envelope::EnvelopeKind::UserTerminate,
            serde_json::Value::Null,
        )
        .await;
        ids
    }

    async fn watch(
        &self,
        app: &str,
        handle: ConnectionHandle,
        watched: Vec<String>,
    ) -> Vec<String> {
        // Record watchers locally + learn which users this node now newly watches.
        let (_local_online, newly_watched) = self.local.watch_edges(app, handle, watched.clone());
        // Cluster half: SUBSCRIBE each newly-watched user's watch channel + the cluster-wide
        // initial online snapshot.
        self.cluster_watch(app, &watched, &newly_watched).await
    }

    async fn unwatch(&self, app: &str, socket_id: &SocketId) {
        let no_longer_watched = self.local.unwatch_edges(app, socket_id);
        self.cluster_unwatch(app, &no_longer_watched).await;
    }

    async fn watchers_of(&self, app: &str, user_id: &str) -> Vec<ConnectionHandle> {
        self.local.watchers_of(app, user_id).await
    }
}
