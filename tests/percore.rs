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
use pylon::transport::worker::{run, DispatchEnv, Mode, WorkerConfig};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
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
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry));
    let env = Arc::new(DispatchEnv {
        apps,
        adapter: adapter.clone(),
        limits: config.limits(),
        activity_timeout: config.activity_timeout,
        strict_protocol: config.strict_protocol,
        conn_counts: Arc::new(Default::default()),
        webhooks: pylon::webhook::WebhookHandle::null(),
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
        h.adapter.channel("app", "my-channel").await.subscription_count,
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
        h.adapter.channel("app", "my-channel").await.subscription_count,
        1
    );
}
