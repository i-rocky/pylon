//! The [`ClusterBridge`]: a dedicated tokio runtime that hosts a [`RedisAdapter`] so the
//! SYNC percore workers can drive cross-node coordination without ever blocking on Redis.
//!
//! A worker holds a cheap-clone [`ClusterHandle`] and fires fire-and-forget [`ClusterCmd`]s
//! over a bounded `mpsc` channel. The bridge's runtime thread drains that channel and runs
//! each command against the adapter. The control-plane channel is BOUNDED: a full channel
//! means the bridge is momentarily behind, and the worker drops the command rather than
//! block (at-most-once cross-node delivery — acceptable for a best-effort fan-out).
//!
//! The bridge's `RedisAdapter` is built with [`RedisAdapter::with_local`] so it SHARES the
//! workers' [`LocalAdapter`]: the adapter's pub/sub receive loop re-delivers remote frames
//! by calling `local.broadcast(Raw(..))`, which then shards through the workers' broadcast
//! sink straight to the right cores — no extra delivery code on this side.

use crate::adapter::local::LocalAdapter;
use crate::adapter::redis::RedisAdapter;
use crate::adapter::Adapter;
use crate::app::AppManager;
use crate::channel::kind::{AuthKind, ChannelInfo};
use crate::connection::handle::Mailbox;
use crate::presence::member::PresenceMember;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use crate::server::config::ServerConfig;
use crate::webhook::event::WebhookEvent;
use crate::webhook::WebhookHandle;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use tokio::sync::mpsc;

/// Shared counters for the cluster bridge, exposed via `/metrics`.
pub struct ClusterMetrics {
    /// Total `ClusterCmd`s dropped because the bridge channel was full or closed.
    pub cmd_dropped: AtomicU64,
    /// Whether the Redis connection is currently healthy. Set `true` by the
    /// node-heartbeat loop after a successful tick; `false` on error.
    /// Held as `Arc<AtomicBool>` so it can be cloned into the heartbeat loop.
    pub redis_connected: Arc<AtomicBool>,
}

impl ClusterMetrics {
    pub fn new() -> Self {
        Self {
            cmd_dropped: AtomicU64::new(0),
            redis_connected: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Default for ClusterMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Bound on the worker→bridge control-plane channel. A full channel drops the command (see
/// [`ClusterHandle::publish`]); sized generously so only a sustained bridge stall — never a
/// normal burst — ever overflows it.
const CMD_CHANNEL_CAPACITY: usize = 8192;

/// A cross-node coordination command a percore worker fires at the bridge.
///
/// Today: [`Publish`](ClusterCmd::Publish) (the broadcast fan-out),
/// [`Subscribe`](ClusterCmd::Subscribe) / [`Unsubscribe`](ClusterCmd::Unsubscribe) — the
/// non-presence channel membership edges (cluster `subscription_count` + the single
/// cluster-wide `channel_occupied` / `channel_vacated`) — and
/// [`PresenceSubscribe`](ClusterCmd::PresenceSubscribe) /
/// [`PresenceLeave`](ClusterCmd::PresenceLeave): the presence membership edges (the
/// cluster-wide roster in `subscription_succeeded` + the single cluster-wide
/// `member_added` / `member_removed`, plus the presence channel's
/// `channel_occupied` / `channel_vacated`). Later SP11 tasks GROW this enum (Signin /
/// Watch …) as those paths move onto the bridge; each variant is added with its full
/// drain-loop handling, never a stub.
pub enum ClusterCmd {
    /// Fan a pre-encoded v7 broadcast `frame` out to the rest of the cluster on
    /// `(app, channel)`, excluding `except`. Maps to [`RedisAdapter::cluster_publish_broadcast`].
    Publish {
        app: Arc<str>,
        channel: Arc<str>,
        frame: String,
        except: Option<SocketId>,
    },
    /// Record cluster-wide membership for `(app, channel, socket_id)` and, on the
    /// resulting cluster edges, broadcast the cluster `subscription_count` and fire
    /// `channel_occupied`. `node_first` is the worker's node-local 0→1 subscriber edge
    /// (computed from its `LocalAdapter` before this is fired). For a CACHE channel,
    /// also replay the cluster-wide last event (or send `pusher:cache_miss`) to the
    /// joining connection's `mailbox` — the worker's `ClusterAdapter::cache_get` is
    /// node-local, so the cluster (Redis) cache replay MUST happen here on the bridge.
    /// Maps to [`RedisAdapter::cluster_subscribe`] (+ [`RedisAdapter::cache_get`] for
    /// cache channels).
    Subscribe {
        app: Arc<str>,
        channel: Arc<str>,
        socket_id: SocketId,
        mailbox: Mailbox,
        node_first: bool,
    },
    /// Remove cluster-wide membership for `(app, channel, socket_id)` and, on the
    /// resulting cluster edges, broadcast the cluster `subscription_count` and fire
    /// `channel_vacated`. `node_last` is the worker's node-local 1→0 subscriber edge.
    /// Maps to [`RedisAdapter::cluster_unsubscribe`].
    Unsubscribe {
        app: Arc<str>,
        channel: Arc<str>,
        socket_id: SocketId,
        node_last: bool,
    },
    /// Presence-channel subscribe: record cluster-wide membership + the presence
    /// refcount, then send the CLUSTER-wide roster back to the joining connection's
    /// `mailbox` as `subscription_succeeded`, and — on the cluster-wide first connection
    /// for this user (`first_for_user`) — broadcast the single cluster-wide `member_added`
    /// (excluding the joiner) and fire the `member_added` webhook. Also fires the single
    /// cluster-wide `channel_occupied` on the cluster 0→1 edge. `node_first` is the
    /// worker's node-local 0→1 subscriber edge (drives the Redis msg-channel subscribe).
    /// Maps to [`RedisAdapter::cluster_subscribe`] + [`RedisAdapter::cluster_presence_join`].
    PresenceSubscribe {
        app: Arc<str>,
        channel: Arc<str>,
        member: PresenceMember,
        socket_id: SocketId,
        mailbox: Mailbox,
        node_first: bool,
    },
    /// Presence-channel leave: remove cluster-wide membership + the presence refcount,
    /// and — on the cluster-wide last connection for this user (`last_for_user`) —
    /// broadcast the single cluster-wide `member_removed` and fire the `member_removed`
    /// webhook. Also fires the single cluster-wide `channel_vacated` on the cluster 1→0
    /// edge. `node_last` is the worker's node-local 1→0 subscriber edge. Maps to
    /// [`RedisAdapter::cluster_unsubscribe`] + [`RedisAdapter::cluster_presence_leave`].
    PresenceLeave {
        app: Arc<str>,
        channel: Arc<str>,
        user_id: String,
        socket_id: SocketId,
        node_last: bool,
    },
    /// User signin: record the cluster-wide USER_SIGNIN refcount + the node-local
    /// `usermsg` subscribe-on-first, and — on the cluster-wide first connection for this
    /// user — publish `WatchOnline` (REMOTE watchers) AND notify THIS node's LOCAL
    /// watchers directly (the publish self-dedups on the origin, so its own local
    /// watchers must be notified here). `node_first` is the worker's node-local 0→1
    /// edge (from `LocalAdapter::signin_user`). Maps to [`RedisAdapter::cluster_signin`].
    Signin {
        app: Arc<str>,
        user_id: String,
        socket_id: SocketId,
        node_first: bool,
    },
    /// User signout: record the cluster-wide USER_SIGNOUT refcount + the node-local
    /// `usermsg` unsubscribe-on-last, and — on the cluster-wide last connection for this
    /// user — publish `WatchOffline` (REMOTE watchers) AND notify THIS node's LOCAL
    /// watchers directly. `node_last` is the worker's node-local 1→0 edge (from
    /// `LocalAdapter::signout_user`). Maps to [`RedisAdapter::cluster_signout`].
    Signout {
        app: Arc<str>,
        user_id: String,
        socket_id: SocketId,
        node_last: bool,
    },
    /// Register this connection's watchlist cluster-wide: SUBSCRIBE the per-user `watch`
    /// Redis channel for every `newly_watched` user (node-local 0→1 watcher edges) and
    /// send the CLUSTER-wide initial online snapshot back to the joining connection's
    /// `mailbox` as `watchlist_events { online }`. The worker's `ClusterAdapter::watch`
    /// returns the NODE-LOCAL online set, which the handler ignores in cluster mode — the
    /// authoritative cluster snapshot is sent here. Maps to [`RedisAdapter::cluster_watch`].
    Watch {
        app: Arc<str>,
        socket_id: SocketId,
        watched: Vec<String>,
        newly_watched: Vec<String>,
        mailbox: Mailbox,
    },
    /// Drop this connection's watchlist cluster-wide: UNSUBSCRIBE the per-user `watch`
    /// Redis channel for every `no_longer_watched` user (node-local 1→0 watcher edges).
    /// Maps to [`RedisAdapter::cluster_unwatch`].
    Unwatch {
        app: Arc<str>,
        socket_id: SocketId,
        no_longer_watched: Vec<String>,
    },
}

/// Cheap-clone handle a percore worker uses to fire [`ClusterCmd`]s at the bridge. `Send +
/// Sync`, so each worker can hold its own clone.
#[derive(Clone)]
pub struct ClusterHandle {
    tx: mpsc::Sender<ClusterCmd>,
    node_id: Arc<str>,
    /// Shared bridge metrics. The handle only writes `cmd_dropped`; `redis_connected`
    /// is written by the heartbeat loop that runs inside the bridge's runtime.
    metrics: Arc<ClusterMetrics>,
}

impl ClusterHandle {
    /// This node's cluster id (minted by the bridge's `RedisAdapter`). Stable for the
    /// bridge's lifetime.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Fire a cross-node broadcast at the bridge. NON-BLOCKING: a `try_send` that drops the
    /// command if the channel is full or closed — the worker must NEVER block on the bridge.
    /// A full channel means the bridge is momentarily behind; a dropped publish is
    /// at-most-once cross-node delivery, which is acceptable for this best-effort fan-out.
    pub fn publish(
        &self,
        app: Arc<str>,
        channel: Arc<str>,
        frame: String,
        except: Option<SocketId>,
    ) {
        let cmd = ClusterCmd::Publish {
            app,
            channel,
            frame,
            except,
        };
        match self.tx.try_send(cmd) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge channel full; dropping cross-node publish");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge gone; dropping cross-node publish");
            }
        }
    }

    /// Fire a cluster Subscribe at the bridge. NON-BLOCKING, drop-on-full/closed exactly
    /// like [`publish`](ClusterHandle::publish) — the worker must NEVER block on the
    /// bridge. A dropped Subscribe at most costs this node a missed cluster count/occupied
    /// edge for one connection (and, for a cache channel, the cluster cache replay) — the
    /// node-local subscribe already succeeded on the worker. `mailbox` is the joining
    /// connection's frame channel, used to deliver the cache replay / `cache_miss`.
    pub fn subscribe(
        &self,
        app: Arc<str>,
        channel: Arc<str>,
        socket_id: SocketId,
        mailbox: Mailbox,
        node_first: bool,
    ) {
        let cmd = ClusterCmd::Subscribe {
            app,
            channel,
            socket_id,
            mailbox,
            node_first,
        };
        match self.tx.try_send(cmd) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge channel full; dropping cross-node subscribe");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge gone; dropping cross-node subscribe");
            }
        }
    }

    /// Fire a cluster Unsubscribe at the bridge. NON-BLOCKING, drop-on-full/closed exactly
    /// like [`publish`](ClusterHandle::publish).
    pub fn unsubscribe(
        &self,
        app: Arc<str>,
        channel: Arc<str>,
        socket_id: SocketId,
        node_last: bool,
    ) {
        let cmd = ClusterCmd::Unsubscribe {
            app,
            channel,
            socket_id,
            node_last,
        };
        match self.tx.try_send(cmd) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge channel full; dropping cross-node unsubscribe");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge gone; dropping cross-node unsubscribe");
            }
        }
    }

    /// Fire a cluster PresenceSubscribe at the bridge. NON-BLOCKING, drop-on-full/closed
    /// exactly like [`publish`](ClusterHandle::publish). A dropped PresenceSubscribe at
    /// most costs this connection its cluster roster / member_added edge; the node-local
    /// presence join already succeeded on the worker (so it still receives deliveries).
    #[allow(clippy::too_many_arguments)]
    pub fn presence_subscribe(
        &self,
        app: Arc<str>,
        channel: Arc<str>,
        member: PresenceMember,
        socket_id: SocketId,
        mailbox: Mailbox,
        node_first: bool,
    ) {
        let cmd = ClusterCmd::PresenceSubscribe {
            app,
            channel,
            member,
            socket_id,
            mailbox,
            node_first,
        };
        match self.tx.try_send(cmd) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    "cluster bridge channel full; dropping cross-node presence subscribe"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge gone; dropping cross-node presence subscribe");
            }
        }
    }

    /// Fire a cluster PresenceLeave at the bridge. NON-BLOCKING, drop-on-full/closed
    /// exactly like [`publish`](ClusterHandle::publish).
    pub fn presence_leave(
        &self,
        app: Arc<str>,
        channel: Arc<str>,
        user_id: String,
        socket_id: SocketId,
        node_last: bool,
    ) {
        let cmd = ClusterCmd::PresenceLeave {
            app,
            channel,
            user_id,
            socket_id,
            node_last,
        };
        match self.tx.try_send(cmd) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge channel full; dropping cross-node presence leave");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge gone; dropping cross-node presence leave");
            }
        }
    }

    /// Fire a cluster Signin at the bridge. NON-BLOCKING, drop-on-full/closed exactly
    /// like [`publish`](ClusterHandle::publish). A dropped Signin at most costs this
    /// connection its cluster online edge (the WatchOnline publish + the usermsg
    /// subscribe); the node-local signin already succeeded on the worker.
    pub fn signin(&self, app: Arc<str>, user_id: String, socket_id: SocketId, node_first: bool) {
        let cmd = ClusterCmd::Signin {
            app,
            user_id,
            socket_id,
            node_first,
        };
        match self.tx.try_send(cmd) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge channel full; dropping cross-node signin");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge gone; dropping cross-node signin");
            }
        }
    }

    /// Fire a cluster Signout at the bridge. NON-BLOCKING, drop-on-full/closed exactly
    /// like [`publish`](ClusterHandle::publish).
    pub fn signout(&self, app: Arc<str>, user_id: String, socket_id: SocketId, node_last: bool) {
        let cmd = ClusterCmd::Signout {
            app,
            user_id,
            socket_id,
            node_last,
        };
        match self.tx.try_send(cmd) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge channel full; dropping cross-node signout");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge gone; dropping cross-node signout");
            }
        }
    }

    /// Fire a cluster Watch at the bridge. NON-BLOCKING, drop-on-full/closed exactly
    /// like [`publish`](ClusterHandle::publish). A dropped Watch at most costs this
    /// connection its cross-node online/offline transitions + its cluster initial
    /// online snapshot; the node-local watch already succeeded on the worker.
    pub fn watch(
        &self,
        app: Arc<str>,
        socket_id: SocketId,
        watched: Vec<String>,
        newly_watched: Vec<String>,
        mailbox: Mailbox,
    ) {
        let cmd = ClusterCmd::Watch {
            app,
            socket_id,
            watched,
            newly_watched,
            mailbox,
        };
        match self.tx.try_send(cmd) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge channel full; dropping cross-node watch");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge gone; dropping cross-node watch");
            }
        }
    }

    /// Fire a cluster Unwatch at the bridge. NON-BLOCKING, drop-on-full/closed exactly
    /// like [`publish`](ClusterHandle::publish).
    pub fn unwatch(&self, app: Arc<str>, socket_id: SocketId, no_longer_watched: Vec<String>) {
        let cmd = ClusterCmd::Unwatch {
            app,
            socket_id,
            no_longer_watched,
        };
        match self.tx.try_send(cmd) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge channel full; dropping cross-node unwatch");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.cmd_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("cluster bridge gone; dropping cross-node unwatch");
            }
        }
    }
}

/// Owns the dedicated tokio runtime (on its own OS thread) that hosts the bridge's
/// `RedisAdapter`, plus the shutdown signal and a [`ClusterHandle`] for the workers.
///
/// Dropping the bridge signals shutdown and JOINS the runtime thread, so a test or
/// `run_percore` can tear it down cleanly. Inside the thread, dropping the `RedisAdapter`
/// at the end of the drain loop aborts its background tasks via the adapter's own `Drop`.
pub struct ClusterBridge {
    handle: ClusterHandle,
    /// Shared metrics for this bridge (cmd_dropped + redis_connected).
    metrics: Arc<ClusterMetrics>,
    /// The node's single `RedisAdapter`, sent out of the runtime thread on the startup
    /// handshake and held here so the REST plane can drive cluster-wide reads/writes
    /// through it. There is EXACTLY ONE `RedisAdapter` per node: its one pub/sub recv loop
    /// and one `node_id` are what make cross-node self-dedup correct, so the bridge and the
    /// REST plane MUST share this same instance — never a second adapter.
    adapter: Arc<RedisAdapter>,
    /// The deferred `WebhookHandle` the drain loop fires occupied/vacated/member_added/
    /// cache_miss through. Set EXACTLY ONCE by [`ClusterBridge::attach_webhooks`], which
    /// runs after the webhook dispatcher exists (breaking the
    /// webhooks→adapter→bridge→webhooks startup cycle). The drain loop reads it via
    /// `get()`: before `attach_webhooks` no WS connections exist, so no `ClusterCmd`s
    /// arrive and a `None` read is impossible in practice — but the loop handles `None`
    /// as a clean no-op (never a panic). Shared (`Arc`) so the runtime thread's drain
    /// loop and `attach_webhooks` see the same cell.
    webhooks: Arc<OnceLock<WebhookHandle>>,
    /// Set on `Drop` to break the drain loop; the loop also exits if the command channel
    /// closes (all `ClusterHandle`s dropped).
    shutdown: Arc<AtomicBool>,
    /// The runtime thread. `Option` so `Drop` can `take()` and `join()` it exactly once.
    thread: Option<JoinHandle<()>>,
}

impl ClusterBridge {
    /// A cheap clone of the handle for a worker (or a test).
    pub fn handle(&self) -> ClusterHandle {
        self.handle.clone()
    }

    /// The shared cluster metrics (cmd_dropped counter + redis_connected gauge).
    pub fn metrics(&self) -> Arc<ClusterMetrics> {
        self.metrics.clone()
    }

    /// The node's single `RedisAdapter`, as an `Arc<dyn Adapter>` for the REST plane to
    /// drive cluster-wide channel reads + REST broadcast publishes through. Cloning the
    /// `Arc` shares the ONE adapter (and its single recv loop / node_id) — essential for
    /// correct self-dedup; this never creates a second adapter.
    pub fn adapter(&self) -> Arc<dyn Adapter> {
        self.adapter.clone()
    }

    /// Attach the webhook dispatcher AFTER it has been built. This breaks the
    /// webhooks→adapter→bridge→webhooks startup cycle: `start` builds the node's
    /// `RedisAdapter` (which the occupancy source the dispatcher needs reads through),
    /// and only once the dispatcher exists does the caller hand its [`WebhookHandle`]
    /// back here. It (1) sets the deferred `OnceLock` the drain loop fires
    /// occupied/vacated/member_added/cache_miss through, and (2) starts the Redis sweeper
    /// with the SAME handle (the sweeper deferral `main.rs` did inline before SP11), so
    /// sweep-driven and command-driven vacated webhooks share one dispatcher.
    ///
    /// Idempotent on the `OnceLock` (a second call is ignored); call it exactly once per
    /// bridge, right after spawning the dispatcher.
    pub fn attach_webhooks(&self, webhooks: WebhookHandle) {
        // Start the sweeper on the node's single `RedisAdapter` with this handle.
        self.adapter.start_sweeper(webhooks.clone());
        // Publish the handle to the drain loop. `set` returns `Err` only on a second
        // call; ignore it (the first handle stays authoritative).
        let _ = self.webhooks.set(webhooks);
    }
}

impl Drop for ClusterBridge {
    /// Signal the drain loop to stop, then join the runtime thread so the bridge (and its
    /// `RedisAdapter`'s background tasks) are fully torn down before this returns.
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        // Closing every sender also wakes the loop's `recv()`; we drop our handle clone so a
        // bridge whose only remaining sender is its own `ClusterHandle` lets the loop end.
        if let Some(thread) = self.thread.take() {
            if let Err(e) = thread.join() {
                tracing::error!(?e, "cluster bridge runtime thread panicked on shutdown");
            }
        }
    }
}

/// Start the bridge: spawn its dedicated multi-thread tokio runtime on a fresh OS thread,
/// connect a `RedisAdapter` (sharing `local`) inside it, start the sweeper, and run the
/// command drain loop. Returns once the runtime thread reports the adapter connected — or
/// `Err` if the connect failed (surfaced across the startup handshake, never a silent hang).
///
/// `local` MUST be the same `LocalAdapter` the percore workers broadcast through, so the
/// adapter's pub/sub receive loop's `local.broadcast(Raw(..))` shards remote frames to the
/// workers' sink.
pub fn start(
    cfg: &ServerConfig,
    local: Arc<LocalAdapter>,
    apps: Arc<dyn AppManager>,
) -> anyhow::Result<ClusterBridge> {
    let (tx, mut rx) = mpsc::channel::<ClusterCmd>(CMD_CHANNEL_CAPACITY);
    let shutdown = Arc::new(AtomicBool::new(false));
    let metrics = Arc::new(ClusterMetrics::new());
    // Clone the redis_connected Arc so the runtime thread can pass it into with_local,
    // where the node-heartbeat loop will update it after each tick.
    let thread_redis_connected = metrics.redis_connected.clone();

    // The deferred webhook cell: empty until `attach_webhooks` runs (after the dispatcher
    // is built). Shared with the runtime thread's drain loop so it sees the handle once set.
    let webhooks: Arc<OnceLock<WebhookHandle>> = Arc::new(OnceLock::new());

    // Owned copy moved into the runtime thread (the caller keeps its borrow).
    let cfg = cfg.clone();
    let thread_shutdown = shutdown.clone();
    let thread_webhooks = webhooks.clone();
    // The cluster-wide presence member cap the drain loop enforces in
    // `ClusterCmd::PresenceSubscribe` (the inline node-local check in `ws::subscribe` is
    // guarded off in cluster mode). Captured as the single `usize` the loop needs.
    let max_presence_members = cfg.limits().max_presence_members;

    // Startup handshake: the runtime thread connects fred asynchronously, but `start` is
    // sync and must return a usable handle or the connect error. A one-shot std channel
    // carries `Result<Arc<RedisAdapter>, anyhow::Error>` from the thread back here — the
    // node's SINGLE adapter, which the `ClusterBridge` then holds for the REST plane and
    // whose `node_id()` mints the handle.
    let (ready_tx, ready_rx) =
        std::sync::mpsc::sync_channel::<anyhow::Result<Arc<RedisAdapter>>>(1);

    let thread = std::thread::Builder::new()
        .name("pylon-cluster-bridge".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    // Report the build failure to `start` and exit the thread.
                    let _ = ready_tx.send(Err(anyhow::Error::from(e)));
                    return;
                }
            };

            runtime.block_on(async move {
                // Keep our own clone of the workers' `LocalAdapter` BEFORE it is moved into
                // the adapter: the drain loop needs it to notify THIS node's local watchers
                // on the cluster online/offline edges (the `Signin` / `Signout` arms).
                let local_for_loop = local.clone();
                // Connect the adapter sharing the workers' `LocalAdapter`. On failure, hand
                // the error back so `start` returns `Err` instead of hanging.
                // Pass `thread_redis_connected` so the node-heartbeat loop inside the adapter
                // updates it after each tick (true = ok, false = error).
                let adapter =
                    match RedisAdapter::with_local(&cfg, local, Some(thread_redis_connected)).await
                    {
                        Ok(a) => Arc::new(a),
                        Err(e) => {
                            let _ = ready_tx.send(Err(e));
                            return;
                        }
                    };

                // The sweeper + the drain loop's occupied/vacated webhooks both need the
                // `WebhookHandle`, but it may not exist yet (the dispatcher's occupancy
                // source reads through THIS adapter, so the dispatcher is built AFTER this
                // returns ready). Both are deferred to `ClusterBridge::attach_webhooks`,
                // which starts the sweeper and publishes the handle to `thread_webhooks`.

                // Report ready with the node's single adapter so `start` can build the live
                // handle (from its `node_id`) and the bridge can expose it to the REST plane.
                if ready_tx.send(Ok(adapter.clone())).is_err() {
                    // `start` was dropped before we became ready (no handle will ever be
                    // used); nothing to do but tear down — `adapter` drops at end of scope.
                    return;
                }
                drop(ready_tx);

                // Command drain loop. Runs until the channel closes (every `ClusterHandle`
                // dropped) OR the shutdown flag is set on bridge `Drop`. `adapter` is kept
                // alive for the whole loop; its `Drop` aborts the background tasks at the end.
                loop {
                    if thread_shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                    // A short timeout makes the loop re-check the shutdown flag promptly even
                    // when idle, without a busy spin.
                    let next =
                        tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
                            .await;
                    match next {
                        // Channel closed: all handles dropped — nothing more can arrive.
                        Ok(None) => break,
                        // Idle tick: loop back and re-check the shutdown flag.
                        Err(_) => continue,
                        Ok(Some(cmd)) => {
                            handle_cmd(
                                &adapter,
                                &local_for_loop,
                                &apps,
                                &thread_webhooks,
                                max_presence_members,
                                cmd,
                            )
                            .await;
                        }
                    }
                }
                // `adapter` drops here → its background tasks (recv/heartbeats/sweeper) abort.
            });
        })?;

    // Block until the thread reports ready (or fails to connect). A dropped `ready_tx`
    // without a value (e.g. the thread panicked before sending) surfaces as a recv error.
    let adapter = match ready_rx.recv() {
        Ok(Ok(adapter)) => adapter,
        Ok(Err(e)) => {
            // The runtime thread reported a connect/build error; join it and propagate.
            let _ = thread.join();
            return Err(e);
        }
        Err(_) => {
            let _ = thread.join();
            return Err(anyhow::anyhow!(
                "cluster bridge runtime thread exited before reporting ready"
            ));
        }
    };

    let node_id: Arc<str> = Arc::from(adapter.node_id());
    let handle = ClusterHandle {
        tx,
        node_id,
        metrics: metrics.clone(),
    };
    Ok(ClusterBridge {
        handle,
        metrics,
        adapter,
        webhooks,
        shutdown,
        thread: Some(thread),
    })
}

/// Drain-loop dispatch for one [`ClusterCmd`]. Runs on the bridge's runtime, on the
/// node's single `RedisAdapter` (so cross-node self-dedup stays correct). Each arm does
/// ONLY the Redis/cluster half plus the cluster-wide edge emissions the connection
/// handler suppresses in cluster mode (the clustered `subscription_count` broadcast and
/// the single cluster-wide `channel_occupied` / `channel_vacated`).
async fn handle_cmd(
    adapter: &RedisAdapter,
    local: &Arc<LocalAdapter>,
    apps: &Arc<dyn AppManager>,
    webhooks: &OnceLock<WebhookHandle>,
    max_presence_members: usize,
    cmd: ClusterCmd,
) {
    match cmd {
        ClusterCmd::Publish {
            app,
            channel,
            frame,
            except,
        } => {
            // ONLY the Redis publish — the worker already delivered locally. Self-dedup on
            // the adapter's `node_id` stops the origin re-receiving its own frame.
            adapter
                .cluster_publish_broadcast(&app, &channel, frame, except.as_ref())
                .await;
        }
        ClusterCmd::Subscribe {
            app,
            channel,
            socket_id,
            mailbox,
            node_first,
        } => {
            // Cluster half: authoritative cluster `(count, occupied)` + the node-local
            // msg-channel subscribe-on-first + the app index.
            let (count, occupied) = adapter
                .cluster_subscribe(&app, &channel, &socket_id, node_first)
                .await;
            // Resolve the per-app flags ourselves (the worker's handler is guarded off in
            // cluster mode). An app that vanished mid-flight just drops the edge.
            let a = match apps.by_id(&app).await {
                Ok(Some(a)) => a,
                Ok(None) => return,
                Err(e) => { tracing::warn!(error = %e, "cluster-edge app lookup failed; skipping"); return }
            };
            // Clustered subscription_count: a single cluster-wide emit (local-via-sink +
            // cluster publish through the adapter's `broadcast`). Mirror the handler's
            // `maybe_emit_count` gating — enabled AND non-presence — and the trait
            // method's `count > 0` guard so a Redis-error zero count never broadcasts a
            // bogus value.
            if count > 0
                && a.subscription_count_enabled
                && ChannelInfo::of(&channel).auth != AuthKind::Presence
            {
                adapter
                    .broadcast(
                        &app,
                        &channel,
                        ServerEvent::SubscriptionCount {
                            channel: channel.to_string(),
                            count,
                        },
                        None,
                    )
                    .await;
            }
            // Single cluster-wide channel_occupied on the cluster 0→1 edge.
            if occupied && a.has_channel_occupied_webhooks {
                if let Some(wh) = webhooks.get() {
                    wh.enqueue(WebhookEvent::ChannelOccupied {
                        app: app.to_string(),
                        channel: channel.to_string(),
                    });
                }
            }
            // Cache channels: replay the CLUSTER-wide last event (Redis-backed) straight
            // to the joining connection's mailbox — or signal a miss. The worker's handler
            // suppresses its node-local replay in cluster mode (`ConnectionContext::
            // clustered`), so a subscriber on this node still sees an event published on
            // ANY node. Ordering is preserved: the handler sends `subscription_succeeded`
            // INLINE (non-presence, no cluster data) before this async mailbox frame.
            // A closed mailbox `send` returns `Err` — a safe no-op (the connection is gone).
            // Match the handler's frame shape + webhook semantics in `ws::subscribe`.
            if ChannelInfo::of(&channel).cache {
                match adapter.cache_get(&app, &channel).await {
                    Some(cached) => {
                        let _ = mailbox.send(ServerEvent::ChannelEvent {
                            channel: channel.to_string(),
                            event: cached.event,
                            data: serde_json::Value::String(cached.data),
                            user_id: None,
                        });
                    }
                    None => {
                        if a.has_cache_miss_webhooks {
                            if let Some(wh) = webhooks.get() {
                                wh.enqueue(WebhookEvent::CacheMiss {
                                    app: app.to_string(),
                                    channel: channel.to_string(),
                                });
                            }
                        }
                        let _ = mailbox.send(ServerEvent::CacheMiss {
                            channel: channel.to_string(),
                        });
                    }
                }
            }
        }
        ClusterCmd::Unsubscribe {
            app,
            channel,
            socket_id,
            node_last,
        } => {
            // Cluster half: authoritative remaining cluster `(count, vacated)` + the
            // node-local msg-channel unsubscribe-on-last.
            let (count, vacated) = adapter
                .cluster_unsubscribe(&app, &channel, &socket_id, node_last)
                .await;
            let a = match apps.by_id(&app).await {
                Ok(Some(a)) => a,
                Ok(None) => return,
                Err(e) => { tracing::warn!(error = %e, "cluster-edge app lookup failed; skipping"); return }
            };
            // Clustered subscription_count — same gating as Subscribe (enabled +
            // non-presence + `count > 0`).
            if count > 0
                && a.subscription_count_enabled
                && ChannelInfo::of(&channel).auth != AuthKind::Presence
            {
                adapter
                    .broadcast(
                        &app,
                        &channel,
                        ServerEvent::SubscriptionCount {
                            channel: channel.to_string(),
                            count,
                        },
                        None,
                    )
                    .await;
            }
            // Single cluster-wide channel_vacated on the cluster 1→0 edge.
            if vacated && a.has_channel_vacated_webhooks {
                if let Some(wh) = webhooks.get() {
                    wh.enqueue(WebhookEvent::ChannelVacated {
                        app: app.to_string(),
                        channel: channel.to_string(),
                    });
                }
            }
        }
        ClusterCmd::PresenceSubscribe {
            app,
            channel,
            member,
            socket_id,
            mailbox,
            node_first,
        } => {
            // Cluster-wide presence capacity gate (Soketi parity:
            // `presence-channel-manager.getChannelMembersCount` is cluster-wide). The
            // count of record is in REDIS — and Redis is only written by
            // `cluster_presence_join` BELOW, never by the worker's inline LOCAL join — so
            // we can authoritatively check the cluster count HERE, before committing the
            // join, and reject cleanly without ever having corrupted the count. The inline
            // node-local capacity check in `ws::subscribe` is GUARDED OFF in cluster mode
            // (it only sees this node's members), so this is the SOLE cap enforcement on
            // the cluster path. A distinct new user that would exceed the cap is rejected:
            //   1) send the SAME 4004 `subscription_error` the inline path sends
            //      (`send_subscription_error(channel,"LimitReached","Presence channel is
            //      full",4004)`) straight to the joining connection's mailbox,
            //   2) undo the inline LOCAL join the worker already performed (the bridge
            //      holds the shared `local`), so the connection is not left a node-local
            //      member, and
            //   3) return WITHOUT running `cluster_subscribe`/`cluster_presence_join`/
            //      roster/`member_added` — Redis was never written for this member, so the
            //      cluster count stays exactly correct.
            // An `already_member` user (a second connection for a user already in the
            // cluster roster) is NOT a new distinct user and is admitted as normal.
            let (cluster_user_count, already_member) = adapter
                .cluster_presence_capacity(&app, &channel, &member.user_id)
                .await;
            if !already_member && cluster_user_count >= max_presence_members {
                let _ = mailbox.send(ServerEvent::SubscriptionError {
                    channel: channel.to_string(),
                    error_type: "LimitReached".to_string(),
                    error: "Presence channel is full".to_string(),
                    status: 4004,
                });
                // Undo the worker's inline node-local join (in `ctx.subscribed` +
                // `presence_membership` on the worker side, and `L.subscribe` here). The
                // worker deindexes its delivery index when it drains the
                // `SubscriptionError`; this removes the matching node-local membership so
                // the rejected connection is fully cleaned up. Redis was never written for
                // this member, so the cluster count is unaffected.
                local.unsubscribe(&app, &channel, &socket_id).await;
                return;
            }
            // Membership half: authoritative cluster `(count, occupied)` + the node-local
            // msg-channel subscribe-on-first + the app index. Presence channels do NOT emit
            // `subscription_count` (P4), so we ignore the count here — only the `occupied`
            // edge (and the presence join below) matter for presence.
            let (_count, occupied) = adapter
                .cluster_subscribe(&app, &channel, &socket_id, node_first)
                .await;
            // Presence half: the cluster-wide `first_for_user` refcount edge + the
            // cluster-wide roster. On a Redis error we keep the join best-effort: read the
            // cluster roster directly (mirrors the trait method KEEPING its node-local
            // roster on error — here the bridge has no node-local roster, so a best-effort
            // cluster read is the closest equivalent; an empty payload only if that fails
            // too). `first_for_user` degrades to `false` so a transient blip never emits a
            // spurious cross-node `member_added`.
            let (first_for_user, roster) = match adapter
                .cluster_presence_join(&app, &channel, &member, &socket_id)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        app = %app, channel = %channel,
                        "redis presence join failed on bridge; sending best-effort roster"
                    );
                    let roster = adapter.presence_members(&app, &channel).await;
                    (false, roster_payload(roster))
                }
            };
            // Send the CLUSTER roster back to the joining connection as
            // `subscription_succeeded`. A closed connection's mailbox returns `Err` here —
            // a safe no-op that doubles as the generation guard (the connection is gone).
            let _ = mailbox.send(ServerEvent::SubscriptionSucceeded {
                channel: channel.to_string(),
                presence: Some(roster),
            });
            // Resolve the per-app flags ourselves (the worker's handler is guarded off in
            // cluster mode). An app that vanished mid-flight just drops the edges.
            let a = match apps.by_id(&app).await {
                Ok(Some(a)) => a,
                Ok(None) => return,
                Err(e) => { tracing::warn!(error = %e, "cluster-edge app lookup failed; skipping"); return }
            };
            // Single cluster-wide `member_added` on the cluster-wide first connection for
            // this user (local-via-sink + cluster publish through `broadcast`, excluding
            // the joiner), plus the `member_added` webhook.
            if first_for_user {
                adapter
                    .broadcast(
                        &app,
                        &channel,
                        ServerEvent::MemberAdded {
                            channel: channel.to_string(),
                            user_id: member.user_id.clone(),
                            user_info: member.user_info.clone(),
                        },
                        Some(socket_id),
                    )
                    .await;
                if a.has_member_added_webhooks {
                    if let Some(wh) = webhooks.get() {
                        wh.enqueue(WebhookEvent::MemberAdded {
                            app: app.to_string(),
                            channel: channel.to_string(),
                            user_id: member.user_id.clone(),
                        });
                    }
                }
            }
            // Single cluster-wide channel_occupied on the cluster 0→1 edge.
            if occupied && a.has_channel_occupied_webhooks {
                if let Some(wh) = webhooks.get() {
                    wh.enqueue(WebhookEvent::ChannelOccupied {
                        app: app.to_string(),
                        channel: channel.to_string(),
                    });
                }
            }
        }
        ClusterCmd::PresenceLeave {
            app,
            channel,
            user_id,
            socket_id,
            node_last,
        } => {
            // Membership half: authoritative remaining cluster `(count, vacated)` + the
            // node-local msg-channel unsubscribe-on-last. Presence channels do NOT emit
            // `subscription_count` (P4), so the count is ignored — only `vacated` matters.
            let (_count, vacated) = adapter
                .cluster_unsubscribe(&app, &channel, &socket_id, node_last)
                .await;
            // Presence half: the cluster-wide `last_for_user` refcount edge. On a Redis
            // error degrade to `false` (log) so a blip never emits a spurious
            // cross-node `member_removed`.
            let last_for_user = match adapter
                .cluster_presence_leave(&app, &channel, &user_id, &socket_id)
                .await
            {
                Ok(last) => last,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        app = %app, channel = %channel,
                        "redis presence leave failed on bridge; treating as NOT last_for_user"
                    );
                    false
                }
            };
            let a = match apps.by_id(&app).await {
                Ok(Some(a)) => a,
                Ok(None) => return,
                Err(e) => { tracing::warn!(error = %e, "cluster-edge app lookup failed; skipping"); return }
            };
            // Single cluster-wide `member_removed` on the cluster-wide last connection for
            // this user, plus the `member_removed` webhook.
            if last_for_user {
                adapter
                    .broadcast(
                        &app,
                        &channel,
                        ServerEvent::MemberRemoved {
                            channel: channel.to_string(),
                            user_id: user_id.clone(),
                        },
                        None,
                    )
                    .await;
                if a.has_member_removed_webhooks {
                    if let Some(wh) = webhooks.get() {
                        wh.enqueue(WebhookEvent::MemberRemoved {
                            app: app.to_string(),
                            channel: channel.to_string(),
                            user_id: user_id.clone(),
                        });
                    }
                }
            }
            // Single cluster-wide channel_vacated on the cluster 1→0 edge.
            if vacated && a.has_channel_vacated_webhooks {
                if let Some(wh) = webhooks.get() {
                    wh.enqueue(WebhookEvent::ChannelVacated {
                        app: app.to_string(),
                        channel: channel.to_string(),
                    });
                }
            }
        }
        ClusterCmd::Signin {
            app,
            user_id,
            socket_id,
            node_first,
        } => {
            // Cluster half: USER_SIGNIN refcount + usermsg subscribe-on-first + the app
            // index + the WatchOnline publish on the cluster 0→1 edge (REMOTE watchers).
            // Returns the cluster-wide `first_for_user`.
            let first = adapter
                .cluster_signin(&app, &user_id, &socket_id, node_first)
                .await;
            // On the cluster-wide first connection for this user, notify THIS node's
            // LOCAL watchers directly — the WatchOnline publish self-dedups on the origin
            // node, so the recv loop will NOT re-deliver it here.
            if first {
                notify_local_watchers(local, &app, &user_id, "online").await;
            }
        }
        ClusterCmd::Signout {
            app,
            user_id,
            socket_id,
            node_last,
        } => {
            // Cluster half: USER_SIGNOUT refcount + usermsg unsubscribe-on-last + the
            // WatchOffline publish on the cluster 1→0 edge (REMOTE watchers). Returns the
            // cluster-wide `last_for_user`.
            let last = adapter
                .cluster_signout(&app, &user_id, &socket_id, node_last)
                .await;
            if last {
                notify_local_watchers(local, &app, &user_id, "offline").await;
            }
        }
        ClusterCmd::Watch {
            app,
            socket_id: _,
            watched,
            newly_watched,
            mailbox,
        } => {
            // SUBSCRIBE each newly-watched user's watch Redis channel (so this node sees
            // their cluster online/offline transitions) + compute the CLUSTER-wide initial
            // online snapshot. Send the snapshot back to the joining connection's mailbox.
            // A closed mailbox `send` returns `Err` — a safe no-op (the connection is gone).
            let online = adapter.cluster_watch(&app, &watched, &newly_watched).await;
            if !online.is_empty() {
                let _ = mailbox.send(ServerEvent::WatchlistEvents {
                    events: vec![crate::protocol::event::WatchlistChange {
                        name: "online".to_string(),
                        user_ids: online,
                    }],
                });
            }
        }
        ClusterCmd::Unwatch {
            app,
            socket_id: _,
            no_longer_watched,
        } => {
            // UNSUBSCRIBE the per-user watch Redis channels for the users whose node-local
            // watcher set just went 1→0 here.
            adapter.cluster_unwatch(&app, &no_longer_watched).await;
        }
    }
}

/// Push a single-change `watchlist_events` frame to every LOCAL connection on this node
/// that is watching `user_id`. Mirrors the pub/sub receive loop's `WatchOnline` /
/// `WatchOffline` arm EXACTLY: this is the origin node's side of a cluster online/offline
/// transition (the cross-node `WatchOnline` publish self-dedups on the origin, so its own
/// local watchers must be notified here). `name` is `"online"` or `"offline"`.
async fn notify_local_watchers(local: &LocalAdapter, app: &str, user_id: &str, name: &str) {
    let ev = ServerEvent::WatchlistEvents {
        events: vec![crate::protocol::event::WatchlistChange {
            name: name.to_string(),
            user_ids: vec![user_id.to_string()],
        }],
    };
    for h in local.watchers_of(app, user_id).await {
        let _ = h.mailbox.send(ev.clone());
    }
}

/// Build a [`PresencePayload`] from a list of cluster presence members, mirroring the
/// roster shape `cluster_presence_join` produces: ids SORTED, the hash keyed by `user_id`
/// → `user_info`, and the distinct-user `count`. Used only on the best-effort error path
/// of [`ClusterCmd::PresenceSubscribe`], where the authoritative roster read failed.
fn roster_payload(members: Vec<PresenceMember>) -> crate::protocol::event::PresencePayload {
    let mut ids: Vec<String> = members.iter().map(|m| m.user_id.clone()).collect();
    ids.sort();
    ids.dedup();
    let mut hash = serde_json::Map::new();
    for m in &members {
        hash.insert(m.user_id.clone(), m.user_info.clone());
    }
    let count = ids.len();
    crate::protocol::event::PresencePayload { ids, hash, count }
}
