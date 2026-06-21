//! Consolidated test harness for the WS-driving suites (`integration`, `signin`,
//! `watchlist`, `webhooks`).
//!
//! Each `tests/*.rs` is its own crate, so every WS suite once carried its OWN copy
//! of `spawn`/`connect`/`next_json`/`established_socket_id`/`auth_token`. This
//! module hoists those into one place and drives them all on the percore transport
//! — a real per-core `mio` worker fleet ([`pylon::transport::run_percore`]) with
//! the REST handoff plane wired, bound to an ephemeral port (the only transport;
//! the legacy axum WS path was removed in SP11).
//!
//! A test file builds a [`SpawnSpec`] (mirroring the constructible `AppState`
//! fields + the concrete `LocalAdapter` the percore sharded fan-out installs on)
//! and calls [`spawn`]. The common case — the standard capacity-2 `APPS` app with
//! a null webhook sink — is the [`spawn_default`] one-liner.

#![allow(dead_code)] // each test crate uses a different subset of these helpers

use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::auth::signature::channel_signature;
use pylon::channel::registry::Registry;
use pylon::cluster::adapter::ClusterAdapter;
use pylon::cluster::bridge::{self, ClusterBridge};
use pylon::server::config::ServerConfig;
use pylon::server::router::{build_router, AppState};
use pylon::webhook::WebhookHandle;
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

pub const SECRET: &str = "app-secret";
pub const KEY: &str = "app-key";

/// The standard single-app config the `integration`/`signin` suites use:
/// capacity 2, client messages + subscription_count enabled.
pub const APPS: &str = r#"[
    {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
     "capacity":2,"client_messages_enabled":true,"subscription_count_enabled":true}
]"#;

pub type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// The constructible pieces of an `AppState` plus the concrete `LocalAdapter` the
/// percore sharded broadcast sink installs on. A test file assembles one of these
/// (usually via [`SpawnSpec::with_apps`]) and hands it to [`spawn`]; the harness
/// then builds either an axum server or a percore worker fleet from the SAME
/// pieces, so the only thing that varies across transports is the I/O plane.
pub struct SpawnSpec {
    pub config: ServerConfig,
    pub apps: Arc<dyn AppManager>,
    /// The concrete local adapter. Held as the concrete type (not `dyn Adapter`)
    /// so [`spawn_percore`] can install the SP9/SP10 sharded broadcast sink on it.
    pub local: Arc<LocalAdapter>,
    pub conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
    pub webhooks: WebhookHandle,
}

impl SpawnSpec {
    /// Build a spec from an apps-JSON string and a config, with a fresh
    /// `LocalAdapter`, empty connection counters, and a null webhook sink.
    pub fn with_apps(config: ServerConfig, apps_json: &str) -> Self {
        let apps: Arc<dyn AppManager> =
            Arc::new(StaticFileAppManager::from_json(apps_json).unwrap());
        let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new()), Arc::new(pylon::adapter::app_registry::AppRegistry::new())));
        Self {
            config,
            apps,
            local,
            conn_counts: Arc::new(Default::default()),
            webhooks: WebhookHandle::null(),
        }
    }

    /// `dyn Adapter` view of the concrete local adapter (what `AppState` holds).
    fn adapter(&self) -> Arc<dyn Adapter> {
        self.local.clone()
    }
}

/// Spawn the server for `spec` on the percore transport and return its bound
/// `127.0.0.1` address — a real per-core worker fleet + REST plane.
pub async fn spawn(spec: SpawnSpec) -> SocketAddr {
    spawn_percore(spec).await
}

/// Convenience for the common case: the standard capacity-2 [`APPS`] app, a null
/// webhook sink, and the given `config`.
pub async fn spawn_default(config: ServerConfig) -> SocketAddr {
    spawn(SpawnSpec::with_apps(config, APPS)).await
}

/// The percore transport: a real per-core `mio` worker fleet bound to an
/// ephemeral `127.0.0.1` port, with the REST handoff plane wired so REST-driven
/// behaviors (server-to-user triggers, terminate_connections, webhooks-occupied
/// publishes) work end-to-end.
///
/// Mirrors `main.rs`'s single-node percore wiring: build the REST `AppState` plus
/// a handoff channel, spawn `rest::serve` on the tokio runtime, then run
/// [`pylon::transport::run_percore`] on a dedicated blocking thread. The worker
/// installs the sharded broadcast sink on the concrete `LocalAdapter` and serves
/// the full v7 protocol; plain-HTTP connections are handed off to the axum REST
/// router. The worker thread + shutdown flag are leaked (the OS reclaims the
/// listener + threads at process exit) — test processes are short-lived.
pub async fn spawn_percore(spec: SpawnSpec) -> SocketAddr {
    let SpawnSpec {
        mut config,
        apps,
        local,
        conn_counts,
        webhooks,
    } = spec;

    // Force the percore worker onto an ephemeral 127.0.0.1 port. A throwaway std
    // listener reserves a free port, then is dropped before the worker re-binds
    // it with SO_REUSEPORT (race-free in practice — the OS won't immediately
    // recycle it to another process; mirrors tests/percore.rs::free_port).
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    config.bind = "127.0.0.1".into();
    config.port = port;
    // A single worker keeps the test deterministic: one accept queue, one slab, so
    // subscribe/broadcast ordering is a single sequential stream. (The multi-worker
    // sharded fan-out is proven separately by percore_multiworker.)
    config.workers = 1;

    let adapter: Arc<dyn Adapter> = local.clone();

    // REST handoff plane: the worker hands plain-HTTP connections to this axum
    // router via `rest_tx`; `rest::serve` drives them on the tokio runtime.
    let (rest_tx, rest_rx) = tokio::sync::mpsc::unbounded_channel::<pylon::transport::RestConn>();
    let rest_state = AppState {
        config: config.clone(),
        apps: apps.clone(),
        adapter: adapter.clone(),
        conn_counts: conn_counts.clone(),
        webhooks: webhooks.clone(),
        saturated: Some(local.saturation_flag()),
        draining: Arc::new(AtomicBool::new(false)),
        cluster_metrics: None,
        invalidator: None,
    };
    let rest_router = build_router(rest_state);
    tokio::spawn(pylon::transport::rest::serve(rest_rx, rest_router));

    // Run the blocking `mio` worker fleet on a dedicated thread. The shutdown flag
    // is leaked alongside the join handle: the test process exits long before any
    // graceful-shutdown is needed, and leaking avoids a Drop-ordering race between
    // the worker thread and the tokio runtime tearing down.
    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = shutdown.clone();
    let worker_config = config.clone();
    let local_for_sink = Some(local.clone());
    // Phase 7: capture the runtime handle here (async context) before spawning the
    // raw worker thread (where `Handle::try_current()` would return `Err`).
    let worker_runtime = tokio::runtime::Handle::current();
    let handle = std::thread::spawn(move || {
        let _ = pylon::transport::run_percore(
            worker_config,
            apps,
            adapter,
            conn_counts,
            Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            webhooks,
            Some(rest_tx),
            worker_shutdown,
            local_for_sink,
            // Single-node parity harness: not clustered (the cluster harness is
            // `spawn_percore_cluster`, which passes `true`).
            false,
            None,
            worker_runtime,
        );
    });
    // Keep the worker alive for the whole test process.
    std::mem::forget((shutdown, handle));

    // Give the worker a moment to bind its SO_REUSEPORT listener before the first
    // client connects (mirrors tests/percore.rs).
    tokio::time::sleep(Duration::from_millis(200)).await;

    format!("127.0.0.1:{port}").parse().unwrap()
}

/// Test Redis URL for the clustered harness: `PYLON_TEST_REDIS_URL` or the
/// documented test default (port 6390 — NOT the 6379 production default, so a
/// real Redis never gets clobbered by a stray run).
fn cluster_test_redis_url() -> String {
    std::env::var("PYLON_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6390".to_string())
}

/// A guard the test holds for the lifetime of a clustered percore node. It owns
/// the node's [`ClusterBridge`] (whose `Drop` joins its dedicated Redis runtime
/// thread) plus the worker thread + its shutdown flag. The node MUST stay alive
/// for the whole test — dropping the bridge tears down Redis, so a test keeps the
/// guard in scope until its assertions are done.
///
/// On `Drop` it signals the worker thread to stop (so a test that finishes early
/// doesn't leak a spinning worker), then drops the bridge (which joins its
/// runtime). The worker thread itself is detached after the shutdown signal —
/// joining it would block on its 50ms poll cadence and serialize teardown; the OS
/// reclaims it at process exit, matching how `spawn_percore` leaks its worker.
pub struct ClusterNodeGuard {
    bridge: Option<ClusterBridge>,
    shutdown: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl Drop for ClusterNodeGuard {
    fn drop(&mut self) {
        // Stop the worker loop, then drop the bridge (its `Drop` joins the Redis
        // runtime thread). Order matters only in that the worker no longer fires
        // commands at a torn-down bridge.
        self.shutdown.store(true, Ordering::SeqCst);
        self.bridge.take();
        // Detach the worker: its loop exits within ~50ms of the shutdown flag, but
        // we don't block teardown on that — the OS reaps it at process exit.
        let _ = self.worker.take();
    }
}

/// Spawn ONE clustered percore node on `prefix` and return its bound
/// `127.0.0.1` address plus a [`ClusterNodeGuard`] the test must keep alive.
///
/// Mirrors [`spawn_percore`] but for the SP11 clustered path: a single
/// `LocalAdapter` is shared by (a) the node's [`ClusterBridge`] (which owns the
/// node's single `RedisAdapter`, sharing this `local`, on its own runtime), (b)
/// the REST plane's [`AppState`] (driving the `RedisAdapter` directly for
/// cluster-wide reads/publishes), and (c) the worker fleet's sharded broadcast
/// sink (installed by `run_percore` when `local` is `Some`). The worker drives a
/// [`ClusterAdapter`] = `{ local, bridge.handle() }`, so a node-local subscribe
/// is synchronous and the cross-node edges are fired (never awaited) at the
/// bridge. `run_percore` is called with `clustered = true`, so each connection's
/// handler defers the single-emit cluster edges to the bridge.
///
/// Two nodes spawned on the SAME `prefix` form a 2-node cluster over one Redis.
pub async fn spawn_percore_cluster(prefix: &str) -> (SocketAddr, ClusterNodeGuard) {
    // The default cluster node uses the standard config (no override applied).
    spawn_percore_cluster_with(prefix, |_| {}).await
}

/// As [`spawn_percore_cluster`] but lets the caller mutate the node's
/// [`ServerConfig`] before the bridge/worker are built — e.g. to inject a small
/// `max_presence_members` so a test can hit the cluster-wide presence capacity
/// cap. The `adapter`/`redis_url`/`redis_prefix`/`bind`/`port`/`workers` fields
/// are set by the harness AFTER `with` runs, so an override can't clobber the
/// cluster wiring; everything else (the limits) is the caller's to tune.
pub async fn spawn_percore_cluster_with(
    prefix: &str,
    with: impl FnOnce(&mut ServerConfig),
) -> (SocketAddr, ClusterNodeGuard) {
    // The single shared LocalAdapter: the bridge's RedisAdapter shares it (so the
    // pub/sub recv loop's `local.broadcast(Raw)` shards remote frames to this
    // node's workers), the REST plane reads the saturation flag off it, and the
    // worker's ClusterAdapter + the sharded sink install on it.
    let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new()), Arc::new(pylon::adapter::app_registry::AppRegistry::new())));

    // A free ephemeral port, reserved then released (mirrors `spawn_percore`).
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };

    // Start from the default config and let the caller tune the limits BEFORE the
    // harness stamps the cluster wiring on top (so an override can't break it).
    let mut config = ServerConfig::default();
    with(&mut config);
    // Redis adapter config forced onto the percore single-worker transport on the
    // free port, sharing `prefix` so sibling nodes see the same keys.
    config.adapter = "redis".into();
    config.redis_url = cluster_test_redis_url();
    config.redis_prefix = prefix.into();
    config.bind = "127.0.0.1".into();
    config.port = port;
    config.workers = 1;

    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());
    let webhooks = WebhookHandle::null();

    // Start the bridge: builds the node's single `RedisAdapter` sharing `local`,
    // on its own runtime. `start` is sync (it owns its runtime thread) and returns
    // once Redis is connected, or panics here with a clear message if it isn't.
    // Webhooks are attached AFTER start (deferred, mirroring `main.rs`): this sets the
    // drain loop's handle AND starts the Redis sweeper with it.
    let bridge = bridge::start(&config, local.clone(), apps.clone())
        .expect("ClusterBridge::start must connect to the test Redis and report ready");
    bridge.attach_webhooks(webhooks.clone());

    // REST plane: drives the node's `RedisAdapter` (full async; blocking on Redis
    // is fine on the tokio runtime) for cluster-wide channel reads + REST publishes.
    let (rest_tx, rest_rx) = tokio::sync::mpsc::unbounded_channel::<pylon::transport::RestConn>();
    let rest_state = AppState {
        config: config.clone(),
        apps: apps.clone(),
        adapter: bridge.adapter(),
        conn_counts: conn_counts.clone(),
        webhooks: webhooks.clone(),
        saturated: Some(local.saturation_flag()),
        draining: Arc::new(AtomicBool::new(false)),
        cluster_metrics: None,
        invalidator: None,
    };
    tokio::spawn(pylon::transport::rest::serve(
        rest_rx,
        build_router(rest_state),
    ));

    // Worker: a `ClusterAdapter` over the shared `local` + the bridge handle. With
    // `Some(local)` the sharded sink installs on the SAME `local` the bridge's
    // RedisAdapter holds, so cross-node received frames shard to this worker.
    let worker_adapter: Arc<dyn Adapter> =
        Arc::new(ClusterAdapter::new(local.clone(), bridge.handle()));
    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = shutdown.clone();
    let worker_config = config.clone();
    let worker_apps = apps.clone();
    let worker_webhooks = webhooks.clone();
    let worker_local = local.clone();
    // Phase 7: capture the runtime handle here (async context) before spawning.
    let worker_runtime = tokio::runtime::Handle::current();
    let worker = std::thread::spawn(move || {
        let _ = pylon::transport::run_percore(
            worker_config,
            worker_apps,
            worker_adapter,
            conn_counts,
            Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            worker_webhooks,
            Some(rest_tx),
            worker_shutdown,
            Some(worker_local),
            // This IS a clustered node: defer the single-emit cluster edges.
            true,
            None,
            worker_runtime,
        );
    });

    // Give the worker a moment to bind its SO_REUSEPORT listener before any client
    // connects (mirrors `spawn_percore`).
    tokio::time::sleep(Duration::from_millis(200)).await;

    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let guard = ClusterNodeGuard {
        bridge: Some(bridge),
        shutdown,
        worker: Some(worker),
    };
    (addr, guard)
}

// ── Shared WS client helpers (identical across every WS suite) ──────────────

pub async fn connect(addr: SocketAddr, query: &str) -> Ws {
    let url = format!("ws://{addr}/app/app-key{query}");
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .expect("connect within 5s")
    .expect("ws handshake");
    ws
}

/// Connect to an arbitrary app key (some suites use multiple keys / no query).
pub async fn connect_key(addr: SocketAddr, key: &str, query: &str) -> Ws {
    let url = format!("ws://{addr}/app/{key}{query}");
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .expect("connect within 5s")
    .expect("ws handshake");
    ws
}

/// Read the next text frame as JSON, failing fast on a hang or unexpected close.
pub async fn next_json(ws: &mut Ws) -> Value {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return serde_json::from_str(&t).unwrap(),
            Ok(Some(Ok(Message::Close(_)))) => panic!("unexpected close while awaiting a frame"),
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => panic!("ws error while awaiting a frame: {e}"),
            Ok(None) => panic!("stream ended while awaiting a frame"),
            Err(_) => panic!("timed out awaiting a frame"),
        }
    }
}

/// Read frames until one with the given event name arrives, skipping others
/// (e.g. interleaved `pusher_internal:subscription_count` frames).
pub async fn next_event_named(ws: &mut Ws, event: &str) -> Value {
    loop {
        let f = next_json(ws).await;
        if f["event"] == event {
            return f;
        }
    }
}

/// Try to read a frame within a short window; `None` if none arrived.
pub async fn try_next_json_short(ws: &mut Ws) -> Option<Value> {
    match tokio::time::timeout(Duration::from_millis(300), ws.next()).await {
        Ok(Some(Ok(Message::Text(t)))) => serde_json::from_str(&t).ok(),
        _ => None,
    }
}

pub async fn send_json(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string())).await.unwrap();
}

/// `connection_established`'s `data` is a JSON-encoded STRING; extract socket_id.
pub async fn established_socket_id(ws: &mut Ws) -> String {
    let frame = next_json(ws).await;
    assert_eq!(frame["event"], "pusher:connection_established");
    let data: Value = serde_json::from_str(frame["data"].as_str().unwrap()).unwrap();
    data["socket_id"].as_str().unwrap().to_string()
}

/// Build a channel-subscribe auth token for the standard app key/secret.
pub fn auth_token(socket_id: &str, channel: &str, channel_data: Option<&str>) -> String {
    format!(
        "{KEY}:{}",
        channel_signature(SECRET, socket_id, channel, channel_data)
    )
}
