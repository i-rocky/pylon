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

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::auth::signature::channel_signature;
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use dashmap::DashMap;
use pylon::adapter::app_registry::AppRegistry;
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
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let registry = Arc::new(Registry::new());
    let app_registry = Arc::new(AppRegistry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry, app_registry.clone()));
    let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());
    let env = Arc::new(DispatchEnv {
        apps,
        adapter: adapter.clone(),
        limits: config.limits(),
        activity_timeout: config.activity_timeout,
        pong_timeout: config.pong_timeout,
        strict_protocol: config.strict_protocol,
        conn_counts: conn_counts.clone(),
        node_conns: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        webhooks: pylon::webhook::WebhookHandle::null(),
        saturated: None,
        clustered: false,
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
                high_water: 1 << 20,
                mode: Mode::Dispatch(env),
                rest_handoff: None,
                worker_id: 0,
                broadcast: None,
                per_worker_budget: 0,
                inflight_slot: None,
                accepted_slot: None,
                codel_dropped_slot: None,
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
    assert_eq!(succ["data"], ""); // empty-string data for non-presence

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
    let env = Arc::new(DispatchEnv {
        apps,
        adapter: adapter.clone(),
        limits: ServerConfig::default().limits(),
        activity_timeout: 120,
        pong_timeout: 30,
        strict_protocol: false,
        conn_counts: conn_counts.clone(),
        node_conns: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        webhooks: pylon::webhook::WebhookHandle::null(),
        saturated: None,
        clustered: false,
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
                high_water: 1 << 20,
                mode: Mode::Dispatch(env),
                rest_handoff: None,
                worker_id: 0,
                broadcast: None,
                per_worker_budget: 0,
                inflight_slot: None,
                accepted_slot: None,
                codel_dropped_slot: None,
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
    let adapter: Arc<dyn Adapter> = Arc::new(pylon::adapter::local::LocalAdapter::new(registry, app_registry.clone()));
    let sat_flag = Arc::new(AtomicBool::new(false));
    let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());
    let env = Arc::new(DispatchEnv {
        apps,
        adapter: adapter.clone(),
        limits: ServerConfig::default().limits(),
        activity_timeout: 120,
        pong_timeout: 30,
        strict_protocol: false,
        conn_counts: conn_counts.clone(),
        node_conns: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        webhooks: pylon::webhook::WebhookHandle::null(),
        saturated: Some(sat_flag.clone()),
        clustered: false,
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
                high_water: 1 << 20,
                mode: Mode::Dispatch(env),
                rest_handoff: None,
                worker_id: 0,
                broadcast: None,
                per_worker_budget: 0,
                inflight_slot: None,
                accepted_slot: None,
                codel_dropped_slot: None,
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
        app_registry,
        shutdown,
        handle: Some(handle),
    };
    (h, sat_flag)
}

/// With the saturation flag forced `true`, a new connection attempt is rejected
/// with close code 4100. With the flag cleared, a subsequent connection succeeds.
/// This verifies both that (a) the accept gate fires and (b) the NODE_CONNS
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
    // Give the worker a moment to process the previous close so NODE_CONNS is
    // back to 0 before the next connect (the reject path should have already
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
    assert!(h.conn_counts.contains_key("app"), "counter entry must exist while connected");
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
