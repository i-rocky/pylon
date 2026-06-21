//! Cross-worker SHARDED broadcast delivery proof for the SP9 per-core transport.
//!
//! Spins up a real multi-worker per-core server via [`run_percore`] (4 workers,
//! one `SO_REUSEPORT` listener each, on an ephemeral port) backed by a single
//! concrete `LocalAdapter` with the sharded broadcast sink installed. 40
//! `tokio-tungstenite` subscribers connect to ONE channel; the kernel spreads
//! them across the 4 workers' accept queues, so subscribers for the same channel
//! end up owned by different worker threads.
//!
//! This proves the two delivery guarantees the new fan-out path must hold:
//!  1. A REST publish reaches ALL 40 subscribers EXACTLY ONCE — even those owned
//!     by a worker other than the one that accepted the publish — i.e. the
//!     per-worker broadcast inbox + local-subscriber fan-out works across cores.
//!  2. A `client-foo` event from ONE subscriber reaches the OTHER 39 and is NOT
//!     echoed to the sender — i.e. sender exclusion (`except`) survives sharding.
//!
//! Every socket-driving step is walled by a hard 5s `tokio::time::timeout`.

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::auth::signature::{channel_signature, hmac_sha256_hex, md5_hex};
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::server::router::{build_router, AppState};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

const SECRET: &str = "app-secret";
const KEY: &str = "app-key";
const N_SUBS: usize = 40;
const N_WORKERS: usize = 4;

const APPS: &str = r#"[
    {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
     "capacity":0,"client_messages_enabled":true,"subscription_count_enabled":false}
]"#;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A running multi-worker percore server: its bound port plus the shutdown flag
/// and worker join handle (joined on drop so no thread leaks between tests).
struct Harness {
    port: u16,
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

/// Reserve a free port via a throwaway std listener, then drop it. The OS won't
/// immediately reuse the port, so the workers re-binding it moments later is
/// race-free in practice.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Start a 4-worker percore server on an ephemeral port with a concrete
/// `LocalAdapter` (sharded sink installed by `run_percore`) and a REST plane
/// served on the test's tokio runtime. Waits for the listeners to bind.
async fn spawn() -> Harness {
    let port = free_port();
    let config = ServerConfig {
        bind: "127.0.0.1".to_string(),
        port,
        workers: N_WORKERS,
        ..Default::default()
    };

    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new()), Arc::new(pylon::adapter::app_registry::AppRegistry::new())));
    let adapter: Arc<dyn Adapter> = local.clone();
    let conn_counts = Arc::new(Default::default());
    let webhooks = pylon::webhook::WebhookHandle::null();

    // REST plane: the worker hands plain-HTTP (publish) connections to this task,
    // which serves them with the same axum router the legacy transport uses. The
    // router shares the SAME adapter, so a REST publish routes through the sink.
    let (rest_tx, rest_rx) = tokio::sync::mpsc::unbounded_channel::<pylon::transport::RestConn>();
    let rest_state = AppState {
        config: config.clone(),
        apps: apps.clone(),
        adapter: adapter.clone(),
        conn_counts: Arc::clone(&conn_counts),
        webhooks: webhooks.clone(),
        saturated: Some(local.saturation_flag()),
        draining: Arc::new(AtomicBool::new(false)),
        cluster_metrics: None,
        invalidator: None,
    };
    let rest_router = build_router(rest_state);
    tokio::spawn(pylon::transport::rest::serve(rest_rx, rest_router));

    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = shutdown.clone();
    // Phase 7: capture the runtime handle here (async context) before spawning.
    let worker_runtime = tokio::runtime::Handle::current();
    let handle = std::thread::spawn(move || {
        pylon::transport::run_percore(
            config,
            apps,
            adapter,
            conn_counts,
            Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            webhooks,
            Some(rest_tx),
            worker_shutdown,
            Some(local),
            false, // not clustered
            None,
            worker_runtime,
        )
        .expect("run_percore failed");
    });

    // Give all four workers a moment to bind their SO_REUSEPORT listeners.
    tokio::time::sleep(Duration::from_millis(300)).await;

    Harness {
        port,
        shutdown,
        handle: Some(handle),
    }
}

async fn connect(port: u16) -> Ws {
    let url = format!("ws://127.0.0.1:{port}/app/{KEY}?protocol=7");
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .expect("connect within 5s")
    .expect("ws handshake");
    ws
}

/// Read the next Text frame as JSON (skipping non-text frames), 5s wall.
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

async fn established_socket_id(ws: &mut Ws) -> String {
    let frame = next_json(ws).await;
    let data: Value = serde_json::from_str(frame["data"].as_str().unwrap()).unwrap();
    data["socket_id"].as_str().unwrap().to_string()
}

fn auth_token(socket_id: &str, channel: &str) -> String {
    format!(
        "{KEY}:{}",
        channel_signature(SECRET, socket_id, channel, None)
    )
}

/// Sign a REST request the Pusher way (matches `tests/rest.rs`).
fn signed_query(method: &str, path: &str, body: &[u8]) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut p: BTreeMap<String, String> = BTreeMap::new();
    p.insert("auth_key".into(), KEY.into());
    p.insert("auth_timestamp".into(), now.to_string());
    p.insert("auth_version".into(), "1.0".into());
    if !body.is_empty() {
        p.insert("body_md5".into(), md5_hex(body));
    }
    let canon = p
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let signed = format!("{}\n{}\n{}", method.to_uppercase(), path, canon);
    let sig = hmac_sha256_hex(SECRET, &signed);
    format!("{canon}&auth_signature={sig}")
}

#[tokio::test]
async fn sharded_broadcast_reaches_all_workers_and_excludes_sender() {
    let h = spawn().await;

    // Connect 40 subscribers (private channel so client events are allowed). The
    // kernel spreads them across the 4 workers' SO_REUSEPORT accept queues.
    let channel = "private-shard";
    let mut subs: Vec<Ws> = Vec::with_capacity(N_SUBS);
    let mut sids: Vec<String> = Vec::with_capacity(N_SUBS);
    for _ in 0..N_SUBS {
        let mut ws = connect(h.port).await;
        let sid = established_socket_id(&mut ws).await;
        ws.send(Message::Text(
            json!({
                "event": "pusher:subscribe",
                "data": { "channel": channel, "auth": auth_token(&sid, channel) }
            })
            .to_string(),
        ))
        .await
        .unwrap();
        // Drain the subscription_succeeded frame.
        let succ = next_json(&mut ws).await;
        assert_eq!(succ["event"], "pusher_internal:subscription_succeeded");
        subs.push(ws);
        sids.push(sid);
    }

    // ── Part 1: REST publish reaches ALL 40 subscribers exactly once ──────────
    let body = json!({
        "name": "my-event",
        "data": "{\"hi\":1}",
        "channels": [channel],
    })
    .to_string();
    let q = signed_query("POST", "/apps/app/events", body.as_bytes());
    let resp = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/apps/app/events?{q}", h.port))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "REST publish must be accepted");

    for (i, ws) in subs.iter_mut().enumerate() {
        let frame = next_json(ws).await;
        assert_eq!(frame["event"], "my-event", "subscriber {i} got wrong event");
        assert_eq!(frame["channel"], channel, "subscriber {i} wrong channel");
        assert_eq!(frame["data"], "{\"hi\":1}", "subscriber {i} wrong data");
        // Exactly once: no second copy should be queued. A ping round-trip proves
        // the next frame is the pong, not a duplicate delivery.
        ws.send(Message::Text(
            json!({"event":"pusher:ping","data":{}}).to_string(),
        ))
        .await
        .unwrap();
        let next = next_json(ws).await;
        assert_eq!(
            next["event"], "pusher:pong",
            "subscriber {i} received a DUPLICATE broadcast (saw {next:?} before pong)"
        );
    }

    // ── Part 2: a client event excludes the sender, reaches the other 39 ──────
    // subs[0] emits; subs[1..] must receive; subs[0] must NOT (its next frame is
    // a pong, proving no self-echo).
    subs[0]
        .send(Message::Text(
            json!({
                "event": "client-foo",
                "channel": channel,
                "data": { "x": 7 }
            })
            .to_string(),
        ))
        .await
        .unwrap();

    for (i, ws) in subs.iter_mut().enumerate().skip(1) {
        let frame = next_json(ws).await;
        assert_eq!(
            frame["event"], "client-foo",
            "subscriber {i} missed the client event"
        );
        assert_eq!(frame["channel"], channel);
        assert_eq!(frame["data"]["x"], 7);
    }

    // The sender gets a pong, never its own client-foo echo.
    subs[0]
        .send(Message::Text(
            json!({"event":"pusher:ping","data":{}}).to_string(),
        ))
        .await
        .unwrap();
    let next = next_json(&mut subs[0]).await;
    assert_eq!(
        next["event"], "pusher:pong",
        "sender received its OWN client event (expected pong, got {next:?})"
    );

    drop(subs);
    drop(h);
}
