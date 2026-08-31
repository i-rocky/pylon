//! Protocol parity tests for the SP9 per-core transport's [`Mode::Dispatch`].
//!
//! These drive a real `tokio-tungstenite` client against the per-core `mio`
//! worker (run on a dedicated `std::thread`) wired to a `LocalAdapter`-backed
//! `AppState` — the SAME app config the `integration.rs` axum suite uses. The
//! worker reuses the production `ConnectionContext::dispatch`, so any divergence
//! from the legacy transport surfaces here as a failed assertion.
//!
//! Every socket-driving step is wrapped in a hard `tokio::time::timeout` wall so
//! a hang fails fast instead of blocking the suite.

use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use pylon::adapter::app_registry::AppRegistry;
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::auth::signature::channel_signature;
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::transport::worker::{run, DispatchEnv, Mode, WorkerConfig};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

const SECRET: &str = "app-secret";
const KEY: &str = "app-key";

/// Same app JSON as `tests/integration.rs`: capacity 2, client + count enabled.
const APPS: &str = r#"[
    {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
     "capacity":2,"client_messages_enabled":true,"subscription_count_enabled":true}
]"#;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A running per-core worker plus the shared adapter (so tests can assert
/// adapter-level state directly) and its shutdown flag / join handle.
struct Harness {
    port: u16,
    adapter: Arc<dyn Adapter>,
    conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
    /// The node-level live-connection counter handed to the worker's env —
    /// exposed so tests can assert it nets to zero across reap paths.
    node_conns: Arc<AtomicUsize>,
    app_registry: Arc<AppRegistry>,
    shutdown: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Reserve a free port via a throwaway std listener, then drop it.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Spawn a dispatch worker on its own OS thread with a `LocalAdapter`-backed
/// environment mirroring `AppState`. Waits briefly for the listener to bind.
async fn spawn(config: ServerConfig) -> Harness {
    spawn_with_apps(config, APPS).await
}

/// The R1 fixture: one enabled app plus one DISABLED app (`"enabled": false`).
const APPS_WITH_DISABLED: &str = r#"[
    {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
     "capacity":2,"client_messages_enabled":true,"subscription_count_enabled":true},
    {"name":"Off","id":"off-app","key":"off-key","secret":"off-secret","enabled":false}
]"#;

/// [`spawn`] with a custom apps.json fixture (exercises the SYNCHRONOUS
/// `by_key_cached` probe path of the static-file manager).
async fn spawn_with_apps(config: ServerConfig, apps_json: &str) -> Harness {
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(apps_json).unwrap());
    let registry = Arc::new(Registry::new());
    let app_registry = Arc::new(AppRegistry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry, app_registry.clone()));
    let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());
    let node_conns = Arc::new(AtomicUsize::new(0));
    let env = Arc::new(DispatchEnv {
        apps,
        adapter: adapter.clone(),
        limits: config.limits(),
        activity_timeout: config.activity_timeout,
        pong_timeout: config.pong_timeout,
        max_conn_lifetime_secs: config.max_conn_lifetime_secs,
        strict_protocol: config.strict_protocol,
        conn_counts: conn_counts.clone(),
        node_conns: node_conns.clone(),
        webhooks: pylon::webhook::WebhookHandle::null(),
        saturated: None,
        clustered: false,
        cluster: None,
        max_connections: 0,
        mailbox_capacity: 256,
        app_registry: app_registry.clone(),
        runtime: tokio::runtime::Handle::current(),
    });

    let port = free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    let sd = shutdown.clone();
    let handle = std::thread::spawn(move || {
        run(
            WorkerConfig {
                addr,
                max_payload: 1 << 20,
                max_message_bytes: 1 << 20,
                // G3 (slowloris) knobs: from the config so the env-var tests
                // (`PYLON_MAX_HEAD_BYTES` / `PYLON_HANDSHAKE_TIMEOUT_MS`)
                // reach the worker.
                max_head_bytes: config.max_head_bytes,
                handshake_timeout_ms: config.handshake_timeout_ms,
                high_water: 1 << 20,
                mode: Mode::Dispatch(env),
                rest_handoff: None,
                worker_id: 0,
                broadcast: None,
                per_worker_budget: 0,
                inflight_slot: None,
                accepted_slot: None,
                codel_dropped_slot: None,
                drophead_dropped_slot: None,
                mailbox_dropped_slot: None,
                codel: pylon::transport::conn::CodelParams::DEFAULT,
                budget_factor: None,
                shutdown_grace_ms: 0,
                tls: None,
            },
            sd,
        )
        .expect("worker run failed");
    });

    // Give the worker a moment to bind before the first client connects.
    tokio::time::sleep(Duration::from_millis(150)).await;

    Harness {
        port,
        adapter,
        conn_counts,
        node_conns,
        app_registry,
        shutdown,
        handle: Some(handle),
    }
}

async fn connect(port: u16, query: &str) -> Ws {
    let url = format!("ws://127.0.0.1:{port}/app/app-key{query}");
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .expect("connect within 5s")
    .expect("ws handshake");
    ws
}

/// Read the next Text frame as JSON (skipping non-text frames), with a 5s wall.
async fn next_json(ws: &mut Ws) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(t) => return serde_json::from_str(&t).unwrap(),
                Message::Close(_) => panic!("unexpected close while awaiting a frame"),
                _ => continue,
            }
        }
    })
    .await
    .expect("frame within 5s")
}

/// Read frames until one with the given event name arrives, skipping others
/// (e.g. interleaved subscription_count frames).
async fn next_event_named(ws: &mut Ws, event: &str) -> Value {
    loop {
        let f = next_json(ws).await;
        if f["event"] == event {
            return f;
        }
    }
}

async fn send_json(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string())).await.unwrap();
}

async fn established_socket_id(ws: &mut Ws) -> String {
    let frame = next_json(ws).await; // connection_established
    let data: Value = serde_json::from_str(frame["data"].as_str().unwrap()).unwrap();
    data["socket_id"].as_str().unwrap().to_string()
}

fn auth_token(socket_id: &str, channel: &str, channel_data: Option<&str>) -> String {
    format!(
        "{KEY}:{}",
        channel_signature(SECRET, socket_id, channel, channel_data)
    )
}

// ── Scenario 1: connection_established ──────────────────────────────────────

#[tokio::test]
async fn connection_established_on_connect() {
    let h = spawn(ServerConfig::default()).await;
    let mut ws = connect(h.port, "?protocol=7").await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "pusher:connection_established");
    let data: Value = serde_json::from_str(frame["data"].as_str().unwrap()).unwrap();
    assert!(
        data["socket_id"].as_str().unwrap().contains('.'),
        "socket_id should look like `<n>.<n>`"
    );
    assert_eq!(data["activity_timeout"], 120);
}

// ── Scenario 2: public subscribe (+ subscription_count) ─────────────────────

#[tokio::test]
async fn public_subscribe_succeeds_and_emits_count() {
    let h = spawn(ServerConfig::default()).await;
    let mut ws = connect(h.port, "?protocol=7").await;
    let _ = established_socket_id(&mut ws).await;
    send_json(
        &mut ws,
        json!({ "event": "pusher:subscribe", "data": { "channel": "my-channel" } }),
    )
    .await;

    let succ = next_json(&mut ws).await;
    assert_eq!(succ["event"], "pusher_internal:subscription_succeeded");
    assert_eq!(succ["channel"], "my-channel");
    assert_eq!(succ["data"], "{}"); // "{}" string for non-presence (P12)

    // subscription_count_enabled = true → a count frame follows.
    let count = next_json(&mut ws).await;
    assert_eq!(count["event"], "pusher_internal:subscription_count");
    let cd: Value = serde_json::from_str(count["data"].as_str().unwrap()).unwrap();
    assert_eq!(cd["subscription_count"], 1);
}

// ── Scenario 3: broadcast delivery (client-event fan-out excludes sender) ────

#[tokio::test]
async fn client_event_delivered_to_peer_not_sender() {
    let h = spawn(ServerConfig::default()).await;
    let mut a = connect(h.port, "?protocol=7").await;
    let sid_a = established_socket_id(&mut a).await;
    let mut b = connect(h.port, "?protocol=7").await;
    let sid_b = established_socket_id(&mut b).await;

    // Both join the same private channel (client events require a non-public chan).
    for (ws, sid) in [(&mut a, &sid_a), (&mut b, &sid_b)] {
        send_json(
            ws,
            json!({
                "event": "pusher:subscribe",
                "data": { "channel": "private-x", "auth": auth_token(sid, "private-x", None) }
            }),
        )
        .await;
        let _ = next_event_named(ws, "pusher_internal:subscription_succeeded").await;
    }

    // a emits a client event.
    send_json(
        &mut a,
        json!({ "event": "client-foo", "channel": "private-x", "data": { "hi": true } }),
    )
    .await;

    // b receives it...
    let got = next_event_named(&mut b, "client-foo").await;
    assert_eq!(got["event"], "client-foo");
    assert_eq!(got["channel"], "private-x");
    assert_eq!(got["data"]["hi"], true);

    // ...and a (the sender) does NOT — a ping round-trips instead, proving no
    // echo of its own client event arrived first.
    send_json(&mut a, json!({ "event": "pusher:ping", "data": {} })).await;
    assert_eq!(
        next_event_named(&mut a, "pusher:pong").await["event"],
        "pusher:pong"
    );
}

// ── Scenario 3b: node connection ceiling (4100) ──────────────────────────────

/// Spawn a dispatch worker with a node-level connection ceiling of `max_node`.
/// The app's own per-app `capacity` is set to 0 (unlimited) so only the node
/// ceiling fires.
async fn spawn_with_node_ceiling(max_connections: usize) -> Harness {
    /// App with capacity=0 (unlimited per-app) so only the node ceiling fires.
    const APPS_UNLIMITED: &str = r#"[
        {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
         "capacity":0,"client_messages_enabled":true,"subscription_count_enabled":false}
    ]"#;
    let apps: Arc<dyn AppManager> =
        Arc::new(StaticFileAppManager::from_json(APPS_UNLIMITED).unwrap());
    let registry = Arc::new(Registry::new());
    let app_registry = Arc::new(AppRegistry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry, app_registry.clone()));
    let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());
    let node_conns = Arc::new(AtomicUsize::new(0));
    let env = Arc::new(DispatchEnv {
        apps,
        adapter: adapter.clone(),
        limits: ServerConfig::default().limits(),
        activity_timeout: 120,
        pong_timeout: 30,
        // These harnesses predate the lifetime close; keep it disabled so their
        // behaviour is unchanged (the default config wires 86400).
        max_conn_lifetime_secs: 0,
        strict_protocol: false,
        conn_counts: conn_counts.clone(),
        node_conns: node_conns.clone(),
        webhooks: pylon::webhook::WebhookHandle::null(),
        saturated: None,
        clustered: false,
        cluster: None,
        max_connections,
        mailbox_capacity: 256,
        app_registry: app_registry.clone(),
        runtime: tokio::runtime::Handle::current(),
    });

    let port = free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    let sd = shutdown.clone();
    let handle = std::thread::spawn(move || {
        run(
            WorkerConfig {
                addr,
                max_payload: 1 << 20,
                max_message_bytes: 1 << 20,
                max_head_bytes: 16_384,
                handshake_timeout_ms: 10_000,
                high_water: 1 << 20,
                mode: Mode::Dispatch(env),
                rest_handoff: None,
                worker_id: 0,
                broadcast: None,
                per_worker_budget: 0,
                inflight_slot: None,
                accepted_slot: None,
                codel_dropped_slot: None,
                drophead_dropped_slot: None,
                mailbox_dropped_slot: None,
                codel: pylon::transport::conn::CodelParams::DEFAULT,
                budget_factor: None,
                shutdown_grace_ms: 0,
                tls: None,
            },
            sd,
        )
        .expect("worker run failed");
    });

    tokio::time::sleep(Duration::from_millis(150)).await;

    Harness {
        port,
        adapter,
        conn_counts,
        node_conns,
        app_registry,
        shutdown,
        handle: Some(handle),
    }
}

/// Read frames until a Close frame arrives; return its code (or None).
async fn wait_close_code(ws: &mut Ws) -> Option<u16> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(Some(cf)))) => return Some(u16::from(cf.code)),
                Some(Ok(Message::Close(None))) | None => return None,
                Some(Ok(_)) => {} // skip text/ping/binary
                Some(Err(_)) => return None,
            }
        }
    })
    .await
    .expect("close frame within 5s")
}

/// Connect without waiting for the established frame (we may receive a reject).
async fn try_connect(port: u16) -> Ws {
    let url = format!("ws://127.0.0.1:{port}/app/app-key?protocol=7");
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .expect("connect within 5s")
    .expect("ws handshake")
    .0
}

/// With `max_connections = 2`, the 3rd simultaneous connection is rejected
/// and its close frame carries code 4100.  After the held connections close,
/// the counter returns to 0 so a new connection succeeds (no counter leak).
#[tokio::test]
async fn node_ceiling_rejects_at_4100_and_counter_released() {
    let h = spawn_with_node_ceiling(2).await;

    // Open 2 connections — both should succeed (get connection_established).
    let mut ws1 = try_connect(h.port).await;
    let _ = established_socket_id(&mut ws1).await;
    let mut ws2 = try_connect(h.port).await;
    let _ = established_socket_id(&mut ws2).await;

    // 3rd connection: ceiling is 2, so this must be rejected with 4100.
    let mut ws3 = try_connect(h.port).await;
    let close_code = wait_close_code(&mut ws3).await;
    assert_eq!(
        close_code,
        Some(4100),
        "3rd connection should be rejected with close code 4100, got {close_code:?}"
    );

    // Drop the 2 held connections; the node counter must return to 0.
    drop(ws1);
    drop(ws2);

    // Give the worker a moment to process the close events.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Now a fresh connection should succeed — counter was properly released.
    let mut ws4 = try_connect(h.port).await;
    let sid = established_socket_id(&mut ws4).await;
    assert!(
        sid.contains('.'),
        "new connection after counter release should succeed, got sid {sid:?}"
    );
    drop(ws4);
}

// ── Scenario 4: disconnect cleanup ──────────────────────────────────────────

#[tokio::test]
async fn disconnect_cleans_up_subscription() {
    let h = spawn(ServerConfig::default()).await;

    // a subscribes to my-channel.
    let mut a = connect(h.port, "?protocol=7").await;
    let _ = established_socket_id(&mut a).await;
    send_json(
        &mut a,
        json!({ "event": "pusher:subscribe", "data": { "channel": "my-channel" } }),
    )
    .await;
    let _ = next_event_named(&mut a, "pusher_internal:subscription_succeeded").await;
    // Drain a's own count=1 frame so the next count frame a reads is b's join.
    let count1 = next_event_named(&mut a, "pusher_internal:subscription_count").await;
    let c1: Value = serde_json::from_str(count1["data"].as_str().unwrap()).unwrap();
    assert_eq!(c1["subscription_count"], 1);

    // b joins; a sees the count climb to 2.
    let mut b = connect(h.port, "?protocol=7").await;
    let _ = established_socket_id(&mut b).await;
    send_json(
        &mut b,
        json!({ "event": "pusher:subscribe", "data": { "channel": "my-channel" } }),
    )
    .await;
    let count2 = next_event_named(&mut a, "pusher_internal:subscription_count").await;
    let c2: Value = serde_json::from_str(count2["data"].as_str().unwrap()).unwrap();
    assert_eq!(c2["subscription_count"], 2);

    // The adapter agrees there are 2 subscribers.
    assert_eq!(
        h.adapter
            .channel("app", "my-channel")
            .await
            .subscription_count,
        2
    );

    // b disconnects → its subscription is cleaned up.
    drop(b);

    // a receives an updated subscription_count of 1 (proves on_close ran the
    // unsubscribe + broadcast through the percore mailbox drain).
    let count_after = next_event_named(&mut a, "pusher_internal:subscription_count").await;
    let ca: Value = serde_json::from_str(count_after["data"].as_str().unwrap()).unwrap();
    assert_eq!(ca["subscription_count"], 1);

    // And the adapter's count has dropped to 1.
    assert_eq!(
        h.adapter
            .channel("app", "my-channel")
            .await
            .subscription_count,
        1
    );
}

// ── Scenario 5: Task 3 — memory-pressure accept gate (4100) ──────────────────

/// Spawn a dispatch worker with a manually-controlled `saturated` flag. Returns
/// the harness AND the flag so the test can flip it.
async fn spawn_with_saturation_flag() -> (Harness, Arc<AtomicBool>) {
    const APPS_UNLIMITED: &str = r#"[
        {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
         "capacity":0,"client_messages_enabled":true,"subscription_count_enabled":false}
    ]"#;
    let apps: Arc<dyn AppManager> =
        Arc::new(StaticFileAppManager::from_json(APPS_UNLIMITED).unwrap());
    let registry = Arc::new(pylon::channel::registry::Registry::new());
    let app_registry = Arc::new(AppRegistry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(pylon::adapter::local::LocalAdapter::new(
        registry,
        app_registry.clone(),
    ));
    let sat_flag = Arc::new(AtomicBool::new(false));
    let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());
    let node_conns = Arc::new(AtomicUsize::new(0));
    let env = Arc::new(DispatchEnv {
        apps,
        adapter: adapter.clone(),
        limits: ServerConfig::default().limits(),
        activity_timeout: 120,
        pong_timeout: 30,
        // These harnesses predate the lifetime close; keep it disabled so their
        // behaviour is unchanged (the default config wires 86400).
        max_conn_lifetime_secs: 0,
        strict_protocol: false,
        conn_counts: conn_counts.clone(),
        node_conns: node_conns.clone(),
        webhooks: pylon::webhook::WebhookHandle::null(),
        saturated: Some(sat_flag.clone()),
        clustered: false,
        cluster: None,
        max_connections: 0,
        mailbox_capacity: 256,
        app_registry: app_registry.clone(),
        runtime: tokio::runtime::Handle::current(),
    });

    let port = free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    let sd = shutdown.clone();
    let handle = std::thread::spawn(move || {
        run(
            WorkerConfig {
                addr,
                max_payload: 1 << 20,
                max_message_bytes: 1 << 20,
                max_head_bytes: 16_384,
                handshake_timeout_ms: 10_000,
                high_water: 1 << 20,
                mode: Mode::Dispatch(env),
                rest_handoff: None,
                worker_id: 0,
                broadcast: None,
                per_worker_budget: 0,
                inflight_slot: None,
                accepted_slot: None,
                codel_dropped_slot: None,
                drophead_dropped_slot: None,
                mailbox_dropped_slot: None,
                codel: pylon::transport::conn::CodelParams::DEFAULT,
                budget_factor: None,
                shutdown_grace_ms: 0,
                tls: None,
            },
            sd,
        )
        .expect("worker run failed");
    });

    tokio::time::sleep(Duration::from_millis(150)).await;

    let h = Harness {
        port,
        adapter,
        conn_counts,
        node_conns,
        app_registry,
        shutdown,
        handle: Some(handle),
    };
    (h, sat_flag)
}

/// With the saturation flag forced `true`, a new connection attempt is rejected
/// with close code 4100. With the flag cleared, a subsequent connection succeeds.
/// This verifies both that (a) the accept gate fires and (b) the node-connection
/// counter is correctly decremented on the reject path (no counter leak).
#[tokio::test]
async fn saturated_accept_gate_rejects_4100_and_releases_counter() {
    let (h, sat_flag) = spawn_with_saturation_flag().await;

    // ── Saturated: new connection must be rejected with 4100. ──────────────
    sat_flag.store(true, Ordering::SeqCst);
    let mut ws1 = try_connect(h.port).await;
    let close_code = wait_close_code(&mut ws1).await;
    assert_eq!(
        close_code,
        Some(4100),
        "new connection while saturated must be rejected with close code 4100, got {close_code:?}"
    );

    // ── Not saturated: clear the flag — a new connection must succeed. ──────
    sat_flag.store(false, Ordering::SeqCst);
    // Give the worker a moment to process the previous close so the node counter
    // is back to 0 before the next connect (the reject path should have already
    // decremented it, but a small sleep confirms).
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut ws2 = try_connect(h.port).await;
    let sid = established_socket_id(&mut ws2).await;
    assert!(
        sid.contains('.'),
        "connection must succeed after saturation clears, got sid {sid:?}"
    );
    drop(ws2);
}

/// The `conn_counts` entry for an app must be REMOVED once its last connection
/// closes (pre-existing leak fix), and the `AppRegistry` entry must clear too.
#[tokio::test]
async fn conn_counts_and_registry_self_clean_on_last_disconnect() {
    let h = spawn(ServerConfig::default()).await;
    let mut ws = try_connect(h.port).await;
    let _ = established_socket_id(&mut ws).await;
    // While connected: both shared maps carry an entry for "app".
    assert!(
        h.conn_counts.contains_key("app"),
        "counter entry must exist while connected"
    );
    assert_eq!(h.app_registry.connected_app_ids(), vec!["app".to_string()]);

    drop(ws);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // After the last disconnect: BOTH entries are gone (no zombie).
    assert!(
        !h.conn_counts.contains_key("app"),
        "conn_counts entry must be removed when the app's last connection closes"
    );
    assert!(
        h.app_registry.connected_app_ids().is_empty(),
        "AppRegistry entry must be removed when the app's last connection closes"
    );
}

// ── Scenario 7: connection-path rejection (4005 / 4001) ──────────────────────

/// Connect to an arbitrary request path (not just `/app/app-key`). A malformed
/// path must still complete the 101 handshake so the server can deliver its
/// `pusher:error` + Close rejection frames over the WebSocket (Pusher parity).
async fn connect_path(port: u16, path: &str) -> Ws {
    let url = format!("ws://127.0.0.1:{port}{path}");
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .expect("connect within 5s")
    .expect("ws handshake")
    .0
}

/// Assert the connection is rejected: exactly one `pusher:error` Text frame
/// carrying `code` + `message`, followed by a WS Close with the same code.
async fn assert_rejected_with(ws: &mut Ws, code: u16, message: &str) {
    let frame = next_json(ws).await;
    assert_eq!(frame["event"], "pusher:error", "frame: {frame}");
    assert_eq!(frame["data"]["code"], code, "frame: {frame}");
    assert_eq!(frame["data"]["message"], message, "frame: {frame}");
    let close_code = wait_close_code(ws).await;
    assert_eq!(
        close_code,
        Some(code),
        "WS Close after pusher:error {code} must carry the same code"
    );
}

/// A WS connection to a path that is not `/app/{key}` must be rejected with
/// 4005 "Path not found" (NOT 4001 — that is reserved for a well-formed path
/// with an unknown app key).
#[tokio::test]
async fn non_app_path_errors_4005_path_not_found() {
    let h = spawn(ServerConfig::default()).await;
    let mut ws = connect_path(h.port, "/nope/?protocol=7").await;
    assert_rejected_with(&mut ws, 4005, "Path not found").await;
}

/// `/app/` with an empty key does not match the `/app/{key}` shape → 4005.
#[tokio::test]
async fn empty_app_key_errors_4005_path_not_found() {
    let h = spawn(ServerConfig::default()).await;
    let mut ws = connect_path(h.port, "/app/?protocol=7").await;
    assert_rejected_with(&mut ws, 4005, "Path not found").await;
}

/// Regression pin: a WELL-FORMED path whose key is unknown keeps 4001 — the
/// 4005 fix must not swallow the unknown-app case.
#[tokio::test]
async fn unknown_app_key_still_errors_4001() {
    let h = spawn(ServerConfig::default()).await;
    let mut ws = connect_path(h.port, "/app/no-such-key?protocol=7").await;
    assert_rejected_with(&mut ws, 4001, "Could not find app by key").await;
}

/// R1: a DISABLED app's key keeps the single WS answer for an unusable key —
/// 4001 "Could not find app by key" (the audit's chosen behavior; the REST
/// side distinguishes disabled via 403, the WS side does not).
#[tokio::test]
async fn disabled_app_key_still_errors_4001() {
    let h = spawn_with_apps(ServerConfig::default(), APPS_WITH_DISABLED).await;
    let mut ws = connect_path(h.port, "/app/off-key?protocol=7").await;
    assert_rejected_with(&mut ws, 4001, "Could not find app by key").await;
}

// ── Scenario 8: max-connection-lifetime close 4202 ──────────────────────────

/// RAII guard removing an env var on drop (even on panic), so a failing test
/// can't leak `PYLON_MAX_CONN_LIFETIME_SECS` into later tests.
struct EnvVarGuard(&'static str);

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        std::env::remove_var(self.0);
    }
}

/// With `PYLON_MAX_CONN_LIFETIME_SECS=1`, an ESTABLISHED connection is closed
/// with code **4202** ("Closed after inactivity", Pusher's reconnect-
/// immediately band) within the lifetime deadline: first a `pusher:error` Text
/// frame carrying the code, then a WS Close with the same code.
///
/// The client stays demonstrably ACTIVE (a `pusher:ping` every 200 ms) for the
/// whole window: the lifetime deadline is ABSOLUTE from establishment and must
/// NOT be pushed out by activity (unlike the idle-ping deadline, which every
/// inbound frame re-arms).
#[tokio::test]
async fn max_conn_lifetime_closes_4202_even_when_active() {
    std::env::set_var("PYLON_MAX_CONN_LIFETIME_SECS", "1");
    let _guard = EnvVarGuard("PYLON_MAX_CONN_LIFETIME_SECS");
    let h = spawn(ServerConfig::from_env()).await;

    let mut ws = connect(h.port, "?protocol=7").await;
    let _sid = established_socket_id(&mut ws).await;

    // Keep the connection busy while the lifetime runs out.
    let (mut sink, mut stream) = ws.split();
    let pinger = tokio::spawn(async move {
        for _ in 0..25 {
            // 25 × 200ms = 5s of activity; breaks early once the server closes.
            if sink
                .send(Message::Text(
                    r#"{"event":"pusher:ping","data":{}}"#.to_string(),
                ))
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    // Within 5s of establish: a pusher:error Text frame with code 4202 (pong
    // replies to our pings may interleave — skip them).
    let err = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match stream.next().await {
                Some(Ok(Message::Text(t))) => {
                    let v: Value = serde_json::from_str(&t).unwrap();
                    if v["event"] == "pusher:error" {
                        return v;
                    }
                }
                Some(Ok(Message::Close(_))) => panic!("WS Close arrived before pusher:error 4202"),
                Some(Ok(_)) => {}
                Some(Err(e)) => panic!("ws error before pusher:error 4202: {e}"),
                None => panic!("stream ended before pusher:error 4202"),
            }
        }
    })
    .await
    .expect("pusher:error 4202 within 5s of establish");
    assert_eq!(err["event"], "pusher:error", "frame: {err}");
    assert_eq!(err["data"]["code"], 4202, "frame: {err}");
    assert_eq!(
        err["data"]["message"], "Closed after inactivity",
        "frame: {err}"
    );

    // Then the WS Close frame carries the same 4202 code.
    let close_code = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match stream.next().await {
                Some(Ok(Message::Close(Some(cf)))) => return Some(u16::from(cf.code)),
                Some(Ok(Message::Close(None))) | None | Some(Err(_)) => return None,
                Some(Ok(_)) => {}
            }
        }
    })
    .await
    .expect("close frame within 5s");
    assert_eq!(close_code, Some(4202), "WS Close must carry code 4202");

    let _ = pinger.await;
}

/// With `PYLON_MAX_CONN_LIFETIME_SECS=0` the lifetime close is DISABLED: an
/// otherwise idle connection stays open well past the 1s lifetime used in the
/// sibling test (here ≥3s) and remains responsive (a ping round-trips, and no
/// 4202 close may arrive).
#[tokio::test]
async fn max_conn_lifetime_zero_disables_the_close() {
    std::env::set_var("PYLON_MAX_CONN_LIFETIME_SECS", "0");
    let _guard = EnvVarGuard("PYLON_MAX_CONN_LIFETIME_SECS");
    let h = spawn(ServerConfig::from_env()).await;

    let mut ws = connect(h.port, "?protocol=7").await;
    let _sid = established_socket_id(&mut ws).await;

    // Outlive the sibling test's 1s deadline 3× over with NO traffic at all.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Still open and responsive: the ping round-trips (next_json panics if a
    // Close arrives instead — proving no 4202 was sent).
    send_json(&mut ws, json!({ "event": "pusher:ping", "data": {} })).await;
    assert_eq!(
        next_event_named(&mut ws, "pusher:pong").await["event"],
        "pusher:pong"
    );
}

// ── Scenario 9: G3 slowloris hardening (head cap + handshake deadline) ───────

/// Open a RAW TCP connection to the worker (no WS handshake bytes sent).
async fn raw_tcp(port: u16) -> tokio::net::TcpStream {
    tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .expect("raw tcp connect")
}

/// Wait until the server closes its side (read EOF or error), bounded by
/// `budget`. Returns `Some(elapsed)` when the close was observed, `None` when
/// the budget expired with the connection still open (the pre-fix behaviour).
async fn wait_server_close(
    rd: &mut tokio::net::tcp::OwnedReadHalf,
    budget: Duration,
) -> Option<std::time::Duration> {
    use tokio::io::AsyncReadExt;
    let start = std::time::Instant::now();
    let mut buf = [0u8; 64];
    let closed = tokio::time::timeout(budget, async {
        loop {
            match rd.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    })
    .await;
    closed.ok().map(|_| start.elapsed())
}

/// Pre-session connections never take any counter (both `node_conns` and the
/// per-app `conn_counts` increment only in `finish_establish`); a reap that
/// decremented what it never took would drift them. Pin the net-zero invariant
/// on both reap paths.
fn assert_counters_net_zero(h: &Harness) {
    assert_eq!(
        h.node_conns.load(Ordering::SeqCst),
        0,
        "node_conns must net to zero after the pre-session reap"
    );
    assert!(
        h.conn_counts.is_empty(),
        "conn_counts must hold no entries after the pre-session reap"
    );
}

/// (a) A client dribbling HEADERLESS bytes slowly grows `inbuf` forever
/// pre-fix. With the default 16 KiB head cap, the connection must be CLOSED
/// once the accumulated head exceeds the cap — well within the bounded wait —
/// and the counters must net to zero.
#[tokio::test]
async fn head_cap_closes_a_dribbling_slowloris() {
    let h = spawn(ServerConfig::default()).await;
    let (mut rd, mut wr) = raw_tcp(h.port).await.into_split();

    // Dribble 32 KiB of headerless bytes in 1 KiB chunks (~650 ms total): the
    // cap trips after ~17 chunks, long before the stream ends. If every chunk
    // lands (pre-fix), HOLD the write side open — the only close the read side
    // may then observe is the server's, never our own EOF.
    let writer = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let chunk = [b'x'; 1024];
        for _ in 0..32 {
            if wr.write_all(&chunk).await.is_err() {
                return; // server already closed — exactly what we want
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        futures_util::future::pending::<()>().await;
    });

    let closed = wait_server_close(&mut rd, Duration::from_secs(3)).await;
    assert!(
        closed.is_some(),
        "server must close the connection once the head cap trips (pre-fix: unbounded buffer, close never comes)"
    );
    writer.abort();
    let _ = writer.await;
    tokio::time::sleep(Duration::from_millis(100)).await; // reap settles
    assert_counters_net_zero(&h);
}

/// (b) A TCP connection that sends NOTHING never completes its handshake and
/// (pre-fix) leaks its fd + slab slot forever. With
/// `PYLON_HANDSHAKE_TIMEOUT_MS=500` the server must close it within ~1.5s.
#[tokio::test]
async fn handshake_timeout_reaps_a_silent_connection() {
    std::env::set_var("PYLON_HANDSHAKE_TIMEOUT_MS", "500");
    let _guard = EnvVarGuard("PYLON_HANDSHAKE_TIMEOUT_MS");
    let h = spawn(ServerConfig::from_env()).await;

    let (mut rd, _wr) = raw_tcp(h.port).await.into_split();
    let start = std::time::Instant::now();
    let closed = wait_server_close(&mut rd, Duration::from_millis(1500)).await;
    assert!(
        closed.is_some(),
        "silent pre-session connection must be reaped within ~1.5s (pre-fix: leaked forever)"
    );
    assert!(
        start.elapsed() >= Duration::from_millis(300),
        "the reap must respect the 500ms deadline, not close instantly"
    );
    assert_counters_net_zero(&h);
}

/// (c) The slowloris limits are generous for real handshakes: under the same
/// `PYLON_HANDSHAKE_TIMEOUT_MS=500` + `PYLON_MAX_HEAD_BYTES=1024` config, a
/// normal WS connect still establishes, and stays established + responsive.
#[tokio::test]
async fn normal_connect_survives_the_slowloris_limits() {
    std::env::set_var("PYLON_HANDSHAKE_TIMEOUT_MS", "500");
    let _g_timeout = EnvVarGuard("PYLON_HANDSHAKE_TIMEOUT_MS");
    std::env::set_var("PYLON_MAX_HEAD_BYTES", "1024");
    let _g_head = EnvVarGuard("PYLON_MAX_HEAD_BYTES");
    let h = spawn(ServerConfig::from_env()).await;

    let mut ws = connect(h.port, "?protocol=7").await;
    let sid = established_socket_id(&mut ws).await;
    assert!(
        sid.contains('.'),
        "handshake must complete under the cap+deadline"
    );

    // Still established and responsive a second later (the deadline is
    // pre-session only — it never touches a live session).
    tokio::time::sleep(Duration::from_secs(1)).await;
    send_json(&mut ws, json!({ "event": "pusher:ping", "data": {} })).await;
    assert_eq!(
        next_event_named(&mut ws, "pusher:pong").await["event"],
        "pusher:pong"
    );
}

/// (d) The handshake deadline is ABSOLUTE from accept: dribbling one byte every
/// 100ms for 1s (continuous activity) must NOT postpone the 500ms deadline —
/// the connection closes at ~500ms anyway, unlike the idle timer.
#[tokio::test]
async fn handshake_deadline_is_not_postponed_by_activity() {
    std::env::set_var("PYLON_HANDSHAKE_TIMEOUT_MS", "500");
    let _guard = EnvVarGuard("PYLON_HANDSHAKE_TIMEOUT_MS");
    let h = spawn(ServerConfig::from_env()).await;

    let (mut rd, mut wr) = raw_tcp(h.port).await.into_split();
    let start = std::time::Instant::now();
    let writer = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        for _ in 0..10 {
            if wr.write_all(b"x").await.is_err() {
                return; // server closed mid-dribble — the expected outcome
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // All 1s of dribble landed (pre-fix): hold the write side open so the
        // only close the read side can observe is the server's reap.
        futures_util::future::pending::<()>().await;
    });

    let closed = wait_server_close(&mut rd, Duration::from_millis(1200)).await;
    assert!(
        closed.is_some(),
        "connection must be reaped at the deadline despite constant dribble"
    );
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(1),
        "closed at {elapsed:?}; the deadline was postponed by activity (expected ~500ms)"
    );
    writer.abort();
    let _ = writer.await;
    assert_counters_net_zero(&h);
}

// ── Scenario 10: G5 same-burst subscribe + close deindexing ─────────────────

/// Build one masked client→server WS frame (RFC 6455 §5.3): FIN=1, `opcode`,
/// payload XOR-masked with the fixed 4-byte `mask`. Client frames MUST be
/// masked (the worker rejects unmasked ones as a protocol error).
fn masked_client_frame(opcode: u8, payload: &[u8], mask: [u8; 4]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 14);
    out.push(0x80 | opcode); // FIN=1 + opcode
    let len = payload.len();
    if len < 126 {
        out.push(0x80 | len as u8); // MASK=1 + 7-bit length
    } else {
        assert!(
            len <= u16::MAX as usize,
            "test helper: extended-64 lengths unneeded"
        );
        out.push(0x80 | 126); // MASK=1 + 16-bit extended length
        out.extend_from_slice(&(len as u16).to_be_bytes());
    }
    out.extend_from_slice(&mask);
    for (i, b) in payload.iter().enumerate() {
        out.push(b ^ mask[i % 4]);
    }
    out
}

/// Complete a raw WS upgrade BY HAND (no tungstenite) so the test owns every
/// byte on the wire after the 101 — tungstenite would frame and flush each
/// message separately, letting the worker process the subscribe and the Close
/// in different readable batches. Returns the socket halves with the server's
/// HTTP head consumed (any bytes already past it, e.g. the
/// `connection_established` frame, are server→client noise we may drop).
async fn raw_ws_upgrade(
    port: u16,
) -> (
    tokio::net::tcp::OwnedReadHalf,
    tokio::net::tcp::OwnedWriteHalf,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let stream = raw_tcp(port).await;
    let (mut rd, mut wr) = stream.into_split();
    let req = "GET /app/app-key?protocol=7 HTTP/1.1\r\n\
               Host: 127.0.0.1\r\n\
               Upgrade: websocket\r\n\
               Connection: Upgrade\r\n\
               Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
               Sec-WebSocket-Version: 13\r\n\r\n";
    wr.write_all(req.as_bytes()).await.expect("write upgrade");
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 1024];
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let n = rd.read(&mut chunk).await.expect("read upgrade response");
            assert!(n > 0, "EOF before the 101");
            buf.extend_from_slice(&chunk[..n]);
            // The FIRST CRLFCRLF in the byte stream is the head terminator.
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf);
                assert!(
                    head.starts_with("HTTP/1.1 101"),
                    "unexpected upgrade response: {head}"
                );
                return;
            }
        }
    })
    .await
    .expect("101 within 5s");
    (rd, wr)
}

/// G5: a readable batch containing [subscribe, Close] — written as ONE TCP
/// burst — makes `dispatch_frames` return `Action::Close` for the Close frame
/// right after the subscribe took, so the worker's post-dispatch
/// `reconcile_membership` (which runs only on the `Action::Keep` path) never
/// saw the new channel. The close path must still fully deindex the
/// freshly-subscribed channel: the adapter's channel view (what REST
/// `GET /apps/{id}/channels` serves) must not report `leak-ch` as occupied or
/// list it, and the worker-local `local_subs` delivery index must be empty —
/// otherwise the index entry lingers forever (dead socket ids accumulate; the
/// channel's subscriber set never empties; monotonic growth with churn).
///
/// A witness subscriber (`a`) proves the burst is not vacuous: it observes the
/// subscription_count 1→2 edge from the burst connection's subscribe (so the
/// subscribe really was dispatched in the same readable batch as the Close)
/// and the 2→1 edge from its on-close unsubscribe. Repeating the burst three
/// times guards the accumulation claim directly.
#[tokio::test]
async fn same_burst_subscribe_then_close_deindexes_membership() {
    use tokio::io::AsyncWriteExt;
    let h = spawn(ServerConfig::default()).await;

    // Witness: a subscribes to leak-ch and drains its own count=1 frame.
    let mut a = connect(h.port, "?protocol=7").await;
    let _ = established_socket_id(&mut a).await;
    send_json(
        &mut a,
        json!({ "event": "pusher:subscribe", "data": { "channel": "leak-ch" } }),
    )
    .await;
    let _ = next_event_named(&mut a, "pusher_internal:subscription_succeeded").await;
    let count1 = next_event_named(&mut a, "pusher_internal:subscription_count").await;
    let c1: Value = serde_json::from_str(count1["data"].as_str().unwrap()).unwrap();
    assert_eq!(c1["subscription_count"], 1);

    for _ in 0..3 {
        let (mut rd, mut wr) = raw_ws_upgrade(h.port).await;

        // ONE write: the subscribe Text frame immediately followed by the
        // Close frame (code 1000). The worker parses both into the same
        // readable batch.
        const MASK: [u8; 4] = [0x37, 0xfa, 0x21, 0x3d];
        let subscribe = br#"{"event":"pusher:subscribe","data":{"channel":"leak-ch"}}"#;
        let mut burst = masked_client_frame(0x1, subscribe, MASK);
        burst.extend(masked_client_frame(0x8, &[0x03, 0xe8], MASK)); // Close, code 1000
        wr.write_all(&burst).await.expect("burst write");

        // The server completes the closing handshake (echoed Close + TCP
        // EOF), bounded — the same-batch Close takes the `Action::Close` path
        // before any reconcile ran for the subscribe.
        let closed = wait_server_close(&mut rd, Duration::from_secs(5)).await;
        assert!(
            closed.is_some(),
            "server must close after the same-burst Close frame"
        );

        // The witness saw the burst connection's membership come AND go: the
        // 1→2 edge proves the subscribe dispatched inside the same batch as
        // the Close (not dropped as post-handshake noise); the 2→1 edge
        // proves the close path's on-close unsubscribe ran.
        let count2 = next_event_named(&mut a, "pusher_internal:subscription_count").await;
        let c2: Value = serde_json::from_str(count2["data"].as_str().unwrap()).unwrap();
        assert_eq!(
            c2["subscription_count"], 2,
            "the burst subscribe must take effect"
        );
        let count_back = next_event_named(&mut a, "pusher_internal:subscription_count").await;
        let cb: Value = serde_json::from_str(count_back["data"].as_str().unwrap()).unwrap();
        assert_eq!(
            cb["subscription_count"], 1,
            "the same-batch close must unsubscribe"
        );
    }

    // While a stays subscribed the worker index holds exactly its ONE slot —
    // none of the three burst connections leaked a dead socket id into it.
    let slots = pylon::transport::worker::percore_local_subs_len();
    assert_eq!(slots, 1, "only the witness's member slot may remain");

    drop(a);

    // Bounded settle: channel-occupied state propagates through the worker
    // immediately on Close; poll so the budget is generous but finite. After
    // a's own close the channel must be gone from the adapter view (what GET
    // /apps/{id}/channels serves) and the index fully empty.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let summary = h.adapter.channel("app", "leak-ch").await;
        let listed = h
            .adapter
            .channels("app", None)
            .await
            .iter()
            .any(|s| s.name == "leak-ch");
        let slots = pylon::transport::worker::percore_local_subs_len();
        if !summary.occupied && !listed && slots == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "leak-ch must be fully deindexed after the same-burst close \
             (occupied={}, listed={}, local_subs slots={})",
            summary.occupied,
            listed,
            slots
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Counters net zero (establish → close): no leaked per-app entries either.
    assert_counters_net_zero(&h);
}

/// G5 control: the NORMAL close path (subscribe settled, THEN a separate
/// Close) keeps deindexing exactly as before — the union deindex must not
/// double-remove or miss on the reconciled path. A second connection on the
/// same channel proves the surviving subscriber is untouched.
#[tokio::test]
async fn normal_close_still_deindexes_and_is_idempotent() {
    let h = spawn(ServerConfig::default()).await;

    // a subscribes and receives subscription_succeeded (reconcile ran).
    let mut a = connect(h.port, "?protocol=7").await;
    let _ = established_socket_id(&mut a).await;
    send_json(
        &mut a,
        json!({ "event": "pusher:subscribe", "data": { "channel": "my-channel" } }),
    )
    .await;
    let _ = next_event_named(&mut a, "pusher_internal:subscription_succeeded").await;
    // Drain a's own count=1 frame.
    let _ = next_event_named(&mut a, "pusher_internal:subscription_count").await;

    // b subscribes to the same channel, then closes NORMALLY (its own TCP
    // close after the subscribe settled).
    let mut b = connect(h.port, "?protocol=7").await;
    let _ = established_socket_id(&mut b).await;
    send_json(
        &mut b,
        json!({ "event": "pusher:subscribe", "data": { "channel": "my-channel" } }),
    )
    .await;
    let _ = next_event_named(&mut b, "pusher_internal:subscription_succeeded").await;
    drop(b);

    // a sees the count fall back to 1 — b's membership was deindexed once.
    // (First consume b's join count=2, exactly like
    // `disconnect_cleans_up_subscription` above.)
    let count2 = next_event_named(&mut a, "pusher_internal:subscription_count").await;
    let c2: Value = serde_json::from_str(count2["data"].as_str().unwrap()).unwrap();
    assert_eq!(c2["subscription_count"], 2);
    let count_after = next_event_named(&mut a, "pusher_internal:subscription_count").await;
    let ca: Value = serde_json::from_str(count_after["data"].as_str().unwrap()).unwrap();
    assert_eq!(ca["subscription_count"], 1);

    // The local index holds exactly a's slot (not zero, not two, not negative:
    // a double-remove would have eaten a's entry, a missed one b's).
    let slots = pylon::transport::worker::percore_local_subs_len();
    assert_eq!(slots, 1, "exactly one member slot must remain (a's)");
    drop(a);

    // …and drains to zero on a's normal close too.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while pylon::transport::worker::percore_local_subs_len() != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "local_subs must empty after both normal closes"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_counters_net_zero(&h);
}
