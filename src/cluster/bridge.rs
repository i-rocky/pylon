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
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use crate::server::config::ServerConfig;
use crate::webhook::event::WebhookEvent;
use crate::webhook::WebhookHandle;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::sync::mpsc;

/// Bound on the worker→bridge control-plane channel. A full channel drops the command (see
/// [`ClusterHandle::publish`]); sized generously so only a sustained bridge stall — never a
/// normal burst — ever overflows it.
const CMD_CHANNEL_CAPACITY: usize = 8192;

/// A cross-node coordination command a percore worker fires at the bridge.
///
/// Today: [`Publish`](ClusterCmd::Publish) (the broadcast fan-out) plus
/// [`Subscribe`](ClusterCmd::Subscribe) / [`Unsubscribe`](ClusterCmd::Unsubscribe) — the
/// non-presence channel membership edges (cluster `subscription_count` + the single
/// cluster-wide `channel_occupied` / `channel_vacated`). Later SP11 tasks GROW this enum
/// (PresenceSubscribe / Signin / Watch …) as those paths move onto the bridge; each
/// variant is added with its full drain-loop handling, never a stub.
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
    /// (computed from its `LocalAdapter` before this is fired). Maps to
    /// [`RedisAdapter::cluster_subscribe`].
    Subscribe {
        app: Arc<str>,
        channel: Arc<str>,
        socket_id: SocketId,
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
}

/// Cheap-clone handle a percore worker uses to fire [`ClusterCmd`]s at the bridge. `Send +
/// Sync`, so each worker can hold its own clone.
#[derive(Clone)]
pub struct ClusterHandle {
    tx: mpsc::Sender<ClusterCmd>,
    node_id: Arc<str>,
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
                tracing::debug!("cluster bridge channel full; dropping cross-node publish");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::debug!("cluster bridge gone; dropping cross-node publish");
            }
        }
    }

    /// Fire a cluster Subscribe at the bridge. NON-BLOCKING, drop-on-full/closed exactly
    /// like [`publish`](ClusterHandle::publish) — the worker must NEVER block on the
    /// bridge. A dropped Subscribe at most costs this node a missed cluster count/occupied
    /// edge for one connection; the node-local subscribe already succeeded on the worker.
    pub fn subscribe(
        &self,
        app: Arc<str>,
        channel: Arc<str>,
        socket_id: SocketId,
        node_first: bool,
    ) {
        let cmd = ClusterCmd::Subscribe {
            app,
            channel,
            socket_id,
            node_first,
        };
        match self.tx.try_send(cmd) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::debug!("cluster bridge channel full; dropping cross-node subscribe");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
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
                tracing::debug!("cluster bridge channel full; dropping cross-node unsubscribe");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::debug!("cluster bridge gone; dropping cross-node unsubscribe");
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
    /// The node's single `RedisAdapter`, sent out of the runtime thread on the startup
    /// handshake and held here so the REST plane can drive cluster-wide reads/writes
    /// through it. There is EXACTLY ONE `RedisAdapter` per node: its one pub/sub recv loop
    /// and one `node_id` are what make cross-node self-dedup correct, so the bridge and the
    /// REST plane MUST share this same instance — never a second adapter.
    adapter: Arc<RedisAdapter>,
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

    /// The node's single `RedisAdapter`, as an `Arc<dyn Adapter>` for the REST plane to
    /// drive cluster-wide channel reads + REST broadcast publishes through. Cloning the
    /// `Arc` shares the ONE adapter (and its single recv loop / node_id) — essential for
    /// correct self-dedup; this never creates a second adapter.
    pub fn adapter(&self) -> Arc<dyn Adapter> {
        self.adapter.clone()
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
    webhooks: WebhookHandle,
    apps: Arc<dyn AppManager>,
) -> anyhow::Result<ClusterBridge> {
    let (tx, mut rx) = mpsc::channel::<ClusterCmd>(CMD_CHANNEL_CAPACITY);
    let shutdown = Arc::new(AtomicBool::new(false));

    // Owned copy moved into the runtime thread (the caller keeps its borrow).
    let cfg = cfg.clone();
    let thread_shutdown = shutdown.clone();

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
                // Connect the adapter sharing the workers' `LocalAdapter`. On failure, hand
                // the error back so `start` returns `Err` instead of hanging.
                let adapter = match RedisAdapter::with_local(&cfg, local).await {
                    Ok(a) => Arc::new(a),
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };

                // The sweeper needs the `WebhookHandle`; start it now that the adapter is
                // up. We keep our OWN clone of `webhooks` to fire occupied/vacated from the
                // drain loop, and hand a second clone to the sweeper.
                adapter.start_sweeper(webhooks.clone());

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
                    let next = tokio::time::timeout(
                        std::time::Duration::from_millis(100),
                        rx.recv(),
                    )
                    .await;
                    match next {
                        // Channel closed: all handles dropped — nothing more can arrive.
                        Ok(None) => break,
                        // Idle tick: loop back and re-check the shutdown flag.
                        Err(_) => continue,
                        Ok(Some(cmd)) => {
                            handle_cmd(&adapter, &apps, &webhooks, cmd).await;
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
    let handle = ClusterHandle { tx, node_id };
    Ok(ClusterBridge {
        handle,
        adapter,
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
    apps: &Arc<dyn AppManager>,
    webhooks: &WebhookHandle,
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
            node_first,
        } => {
            // Cluster half: authoritative cluster `(count, occupied)` + the node-local
            // msg-channel subscribe-on-first + the app index.
            let (count, occupied) = adapter
                .cluster_subscribe(&app, &channel, &socket_id, node_first)
                .await;
            // Resolve the per-app flags ourselves (the worker's handler is guarded off in
            // cluster mode). An app that vanished mid-flight just drops the edge.
            let Some(a) = apps.by_id(&app).await else {
                return;
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
                webhooks.enqueue(WebhookEvent::ChannelOccupied {
                    app: app.to_string(),
                    channel: channel.to_string(),
                });
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
            let Some(a) = apps.by_id(&app).await else {
                return;
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
                webhooks.enqueue(WebhookEvent::ChannelVacated {
                    app: app.to_string(),
                    channel: channel.to_string(),
                });
            }
        }
    }
}
