//! SP11 §4 liveness parity on the per-core transport.
//!
//! Mirrors the legacy `connection/task.rs` idle-ping / 4201 pong-timeout
//! semantics, now driven by the per-worker [`TimerWheel`]. Two scenarios:
//!
//! 1. A silent client (one that never sends a `pusher:pong`) must receive a
//!    `pusher:ping` and then a WebSocket **Close with code 4201** within the
//!    deadline — the server reaped a dead connection.
//! 2. A live client that *does* answer each `pusher:ping` with a `pusher:pong`
//!    stays connected past the same deadline — activity keeps it alive.
//!
//! The worker is spawned with SHORT timeouts (`activity_timeout=1s`,
//! `pong_timeout=1s`) so the whole exchange completes in ~2s; every socket step
//! is wrapped in a hard `tokio::time::timeout` wall so a hang fails fast.
//!
//! Note: `tokio-tungstenite` auto-answers *protocol-level* WS Pings with WS
//! Pongs, but `pusher:ping` is an application Text frame — the client only sends
//! a `pusher:pong` if we explicitly do so. The silent client therefore triggers
//! the pong-timeout path.

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::transport::worker::{run, DispatchEnv, Mode, WorkerConfig};
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

const APPS: &str = r#"[
    {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
     "capacity":0,"client_messages_enabled":true,"subscription_count_enabled":true}
]"#;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

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

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Spawn a percore dispatch worker with the given (short) liveness timeouts.
async fn spawn(activity_timeout: u32, pong_timeout: u32) -> Harness {
    let config = ServerConfig {
        activity_timeout,
        pong_timeout,
        ..ServerConfig::default()
    };
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let registry = Arc::new(Registry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry, Arc::new(pylon::adapter::app_registry::AppRegistry::new())));
    let env = Arc::new(DispatchEnv {
        apps,
        adapter,
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

/// A silent client (never answers `pusher:ping`) gets pinged, then closed 4201.
#[tokio::test]
async fn idle_connection_pinged_then_closed_4201() {
    let h = spawn(1, 1).await;
    let mut ws = connect(h.port).await;

    // The whole idle→ping→4201 exchange must complete well within this wall.
    let outcome = tokio::time::timeout(Duration::from_secs(6), async {
        let mut saw_ping = false;
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(t))) => {
                    let v: Value = serde_json::from_str(&t).unwrap();
                    if v["event"] == "pusher:ping" {
                        saw_ping = true;
                    }
                    // The server must NOT signal the timeout as an in-band
                    // pusher:error text frame — it must use a WS Close.
                    if v["event"] == "pusher:error" {
                        panic!("server sent pusher:error instead of a WS Close 4201");
                    }
                    // Deliberately send NO pusher:pong — stay dead.
                }
                Some(Ok(Message::Close(Some(cf)))) => {
                    return (saw_ping, Some(u16::from(cf.code)));
                }
                Some(Ok(Message::Close(None))) | None => return (saw_ping, None),
                Some(Ok(_)) => {} // ignore WS-level ping/pong, binary, etc.
                Some(Err(_)) => return (saw_ping, None),
            }
        }
    })
    .await
    .expect("idle connection should be pinged and closed within 6s");

    let (saw_ping, close_code) = outcome;
    assert!(saw_ping, "server should have sent a pusher:ping");
    assert_eq!(
        close_code,
        Some(4201),
        "server should have closed with WS code 4201"
    );

    drop(h);
}

/// A client that answers each `pusher:ping` with `pusher:pong` stays connected
/// past the (short) deadline — its activity keeps resetting the idle timer.
#[tokio::test]
async fn responsive_connection_stays_alive() {
    let h = spawn(1, 1).await;
    let mut ws = connect(h.port).await;

    // Drive the connection for ~4s (4× the activity_timeout): every pusher:ping
    // is answered with a pusher:pong, so the wheel never reaches a pong-timeout.
    // If the server wrongly closed us, we'd observe a Close frame here.
    let result = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(t))) => {
                    let v: Value = serde_json::from_str(&t).unwrap();
                    if v["event"] == "pusher:ping" {
                        // Answer like a real client → inbound activity → stay alive.
                        ws.send(Message::Text(
                            r#"{"event":"pusher:pong","data":{}}"#.to_string(),
                        ))
                        .await
                        .unwrap();
                    }
                }
                Some(Ok(Message::Close(_))) | None => {
                    return Err("server closed a live connection")
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(Box::leak(format!("ws error: {e}").into_boxed_str())),
            }
        }
    })
    .await;

    // The inner future never returns Ok (it loops until the outer timeout); a
    // timeout (Err) is the SUCCESS case — the connection survived. An inner
    // `Err(reason)` means the server closed us, which is the failure.
    match result {
        Err(_elapsed) => { /* survived the full window — correct */ }
        Ok(Err(reason)) => panic!("{reason}"),
        Ok(Ok(())) => unreachable!(),
    }

    // Confirm the socket is still usable: a pusher:ping we send is answered with
    // a pusher:pong, proving the connection is live (not half-closed).
    ws.send(Message::Text(
        r#"{"event":"pusher:ping","data":{}}"#.to_string(),
    ))
    .await
    .unwrap();
    let pong = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(t) => {
                    let v: Value = serde_json::from_str(&t).unwrap();
                    if v["event"] == "pusher:pong" {
                        return true;
                    }
                }
                Message::Close(_) => return false,
                _ => {}
            }
        }
    })
    .await
    .expect("pong within 3s");
    assert!(
        pong,
        "live connection should answer a pusher:ping with pusher:pong"
    );

    drop(h);
}
