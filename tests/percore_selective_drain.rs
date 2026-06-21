//! Waker-driven SELECTIVE mailbox drain (no O(N) idle scan).
//!
//! These drive a real per-core `mio` worker (on a dedicated `std::thread`) wired
//! to a `LocalAdapter`-backed `AppState`, exactly like `tests/percore.rs`. They
//! prove the worker delivers a CROSS-connection mailbox send (here a
//! `send_to_user`, the same kind of direct mailbox delivery the cluster bridge,
//! `notify_watchers`, and the registry mailbox path all use) by visiting ONLY the
//! connections that actually received a send — never the idle ones:
//!
//! * `selective_drain_skips_idle_connections` — with many idle connections and a
//!   couple active, a `send_to_user` reaches its targets PROMPTLY (well under the
//!   50ms idle poll — proving the `Waker` fast-path) and the per-process
//!   `percore_selective_drain_visits()` instrumentation counter rises by only a
//!   tiny amount (≈ the number of ACTIVE connections), NOT by the idle count.
//! * `no_loss_under_many_cross_connection_sends` — many `send_to_user`s to many
//!   distinct connections all arrive; no dirty-push is ever missed.
//!
//! Every socket-driving step is wrapped in a hard `tokio::time::timeout` so a hang
//! fails fast instead of blocking the suite.

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::auth::signature::user_signature;
use pylon::channel::registry::Registry;
use pylon::protocol::event::ServerEvent;
use pylon::server::config::ServerConfig;
use pylon::transport::worker::{
    percore_selective_drain_visits, run, DispatchEnv, Mode, WorkerConfig,
};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

const SECRET: &str = "app-secret";
const KEY: &str = "app-key";

/// A high per-app capacity so a test can open hundreds of connections.
const APPS: &str = r#"[
    {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
     "capacity":100000,"client_messages_enabled":true,"subscription_count_enabled":true}
]"#;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

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

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Spawn a single dispatch worker on its own OS thread with a `LocalAdapter`-backed
/// environment. `broadcast: None`, so channel broadcasts AND direct mailbox sends
/// both route through the registry `Mailbox` path — every cross-connection send
/// goes through `Mailbox::send` and is delivered by the selective drain.
async fn spawn() -> Harness {
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let registry = Arc::new(Registry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry, Arc::new(pylon::adapter::app_registry::AppRegistry::new())));
    let config = ServerConfig::default();
    let env = Arc::new(DispatchEnv {
        apps,
        adapter: adapter.clone(),
        limits: config.limits(),
        activity_timeout: config.activity_timeout,
        pong_timeout: config.pong_timeout,
        strict_protocol: config.strict_protocol,
        conn_counts: Arc::new(Default::default()),
        node_conns: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        webhooks: pylon::webhook::WebhookHandle::null(),
        saturated: None,
        clustered: false,
        max_connections: 0,
        mailbox_capacity: 256,
        app_registry: Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
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
        shutdown,
        handle: Some(handle),
    }
}

async fn connect(port: u16) -> Ws {
    let url = format!("ws://127.0.0.1:{port}/app/app-key?protocol=7");
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .expect("connect within 5s")
    .expect("ws handshake");
    ws
}

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

async fn send_json(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string())).await.unwrap();
}

/// Read the `connection_established` frame and return this connection's socket_id.
async fn established_socket_id(ws: &mut Ws) -> String {
    let frame = next_json(ws).await;
    assert_eq!(frame["event"], "pusher:connection_established");
    let data: Value = serde_json::from_str(frame["data"].as_str().unwrap()).unwrap();
    data["socket_id"].as_str().unwrap().to_string()
}

/// Sign in `ws` as `user_data`, asserting the `pusher:signin_success` ack.
async fn signin(ws: &mut Ws, socket_id: &str, user_data: &str) {
    let auth = format!("{KEY}:{}", user_signature(SECRET, socket_id, user_data));
    send_json(
        ws,
        json!({ "event": "pusher:signin", "data": { "auth": auth, "user_data": user_data } }),
    )
    .await;
    let ack = next_json(ws).await;
    assert_eq!(ack["event"], "pusher:signin_success");
}

/// With many IDLE connections and two ACTIVE (signed-in) ones, a `send_to_user`
/// reaches both active connections PROMPTLY (well under the 50ms idle poll) while
/// the selective drain visits only a tiny number of mailboxes — never the idle
/// connections. Proves the `Waker` fast-path AND that idle connections are skipped.
#[tokio::test]
async fn selective_drain_skips_idle_connections() {
    let h = spawn().await;

    // A pile of IDLE connections that never speak after the handshake. If the
    // worker scanned every connection each loop (the old O(N) drain), the visit
    // counter below would balloon by ~IDLE per active delivery.
    const IDLE: usize = 300;
    let mut idle = Vec::with_capacity(IDLE);
    for _ in 0..IDLE {
        let mut ws = connect(h.port).await;
        let _ = established_socket_id(&mut ws).await;
        idle.push(ws);
    }

    // Two ACTIVE connections, signed in as the SAME user so a single
    // `send_to_user` fans out to both — two cross-connection mailbox deliveries.
    let mut a = connect(h.port).await;
    let mut b = connect(h.port).await;
    let sid_a = established_socket_id(&mut a).await;
    let sid_b = established_socket_id(&mut b).await;
    signin(&mut a, &sid_a, r#"{"id":"U"}"#).await;
    signin(&mut b, &sid_b, r#"{"id":"U"}"#).await;

    // Let the worker settle to the idle (50ms-poll) state, then snapshot the
    // selective-drain visit counter.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = percore_selective_drain_visits();

    // Fire the cross-connection delivery and time how long it takes to arrive.
    let started = Instant::now();
    h.adapter.send_to_user("app", "U", ServerEvent::Pong).await;

    // Both active connections must receive the Pong PROMPTLY. We require it within
    // a window comfortably under the 50ms idle poll — only the `MAILBOX_WAKER`
    // fast-path can deliver this quickly when the worker is otherwise idle.
    let fa = next_json(&mut a).await;
    let fb = next_json(&mut b).await;
    let elapsed = started.elapsed();
    assert_eq!(fa["event"], "pusher:pong");
    assert_eq!(fb["event"], "pusher:pong");
    assert!(
        elapsed < Duration::from_millis(40),
        "send_to_user took {elapsed:?}; the Waker fast-path should deliver in <40ms, \
         not wait on the 50ms idle poll"
    );

    // The selective drain must have visited only a HANDFUL of mailboxes for this
    // delivery — the two active connections (plus possibly an extra empty pass).
    // It must NOT have visited the 300 idle connections.
    let after = percore_selective_drain_visits();
    let visited = after - before;
    assert!(
        visited <= 8,
        "selective drain visited {visited} mailboxes for a 2-target delivery among \
         {IDLE} idle connections; idle connections must never be scanned"
    );

    drop(idle);
}

/// Many cross-connection sends to many distinct connections: every one is
/// delivered. Guards against a missed dirty-push dropping a delivery.
#[tokio::test]
async fn no_loss_under_many_cross_connection_sends() {
    let h = spawn().await;

    // Open N connections, each signed in as its OWN user, so each `send_to_user`
    // targets exactly one connection. Interleave a few idle connections too.
    const N: usize = 60;
    let mut active = Vec::with_capacity(N);
    for i in 0..N {
        // Sprinkle idle (never-signed-in) connections between the active ones.
        let mut idle = connect(h.port).await;
        let _ = established_socket_id(&mut idle).await;
        std::mem::forget(idle); // keep the socket open; never drained

        let mut ws = connect(h.port).await;
        let sid = established_socket_id(&mut ws).await;
        signin(&mut ws, &sid, &format!(r#"{{"id":"U{i}"}}"#)).await;
        active.push(ws);
    }

    // Fire one send_to_user per user, all back-to-back.
    for i in 0..N {
        h.adapter
            .send_to_user("app", &format!("U{i}"), ServerEvent::Pong)
            .await;
    }

    // Every active connection must receive its Pong. A missed dirty-push would
    // strand one of these and the `next_json` timeout would fail the test.
    for (i, ws) in active.iter_mut().enumerate() {
        let f = next_json(ws).await;
        assert_eq!(
            f["event"], "pusher:pong",
            "connection {i} lost its delivery"
        );
    }
}
