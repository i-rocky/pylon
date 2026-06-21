//! C2a — graceful connection drain integration test.
//!
//! Verifies that when the shutdown flag is set on a percore worker:
//!
//!   (a) Every connected WS client receives a `pusher:error` Text frame with
//!       code 4200 followed by a WS **Close** frame with close code 4200
//!       ("reconnect immediately" — Pusher's rolling-restart signal).
//!   (b) The app's per-app `conn_counts` entry returns to 0 (all `on_close` /
//!       `remove` paths ran and decremented the counter).
//!
//! The harness directly constructs a single-worker `DispatchEnv` with a
//! `shutdown_grace_ms` large enough to flush a tiny frame but not so long
//! that the test hangs, connects 3 clients and subscribes them, then sets
//! `shutdown=true` and waits for each client to receive its Close frame within
//! a tight wall-clock budget.

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::protocol::event::ServerEvent;
use pylon::server::config::ServerConfig;
use pylon::transport::worker::{run, DispatchEnv, Mode, WorkerConfig};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

const APPS: &str = r#"[
    {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
     "capacity":0,"client_messages_enabled":true,"subscription_count_enabled":true}
]"#;
const APP_ID: &str = "app";

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct Harness {
    port: u16,
    shutdown: Arc<AtomicBool>,
    conn_counts: Arc<dashmap::DashMap<String, Arc<AtomicUsize>>>,
    /// Shared adapter — retained so tests can call `adapter.broadcast()` to push
    /// events to subscribed connections and exercise the inflight-bytes path.
    adapter: Arc<dyn Adapter>,
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

/// Serializes the drain tests that share the same worker config. Not strictly
/// required for single-worker tests, but prevents flaky port contention if tests
/// run multi-threaded.
static HARNESS_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Spawn a single dispatch worker with `shutdown_grace_ms` so the drain flushes
/// the Close frame to connected clients before exiting.
async fn spawn_with_grace(grace_ms: u64) -> Harness {
    let config = ServerConfig::default();
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let registry = Arc::new(Registry::new());
    // Keep a clone of the adapter before it is moved into DispatchEnv so tests
    // can call adapter.broadcast() to push events to subscribed connections.
    let adapter_arc: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry, Arc::new(pylon::adapter::app_registry::AppRegistry::new())));
    let adapter_for_harness = adapter_arc.clone();
    let conn_counts: Arc<dashmap::DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());
    let env = Arc::new(DispatchEnv {
        apps,
        adapter: adapter_arc,
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
                shutdown_grace_ms: grace_ms,
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
        conn_counts,
        adapter: adapter_for_harness,
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

/// Read messages from `ws` until we receive the `pusher:connection_established`
/// Text frame, then return the socket_id. Panics if the stream ends first.
async fn wait_established(ws: &mut Ws) -> String {
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(t))) => {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["event"] == "pusher:connection_established" {
                    let data: Value = serde_json::from_str(v["data"].as_str().unwrap()).unwrap();
                    return data["socket_id"].as_str().unwrap().to_string();
                }
            }
            Some(Ok(_)) => {}
            msg => panic!("unexpected message before connection_established: {msg:?}"),
        }
    }
}

/// Send a subscribe command and drain until we see the `pusher_internal:subscription_succeeded`.
async fn subscribe(ws: &mut Ws, channel: &str) {
    ws.send(Message::Text(
        json!({"event": "pusher:subscribe", "data": {"channel": channel}}).to_string(),
    ))
    .await
    .unwrap();
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(t))) => {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["event"] == "pusher_internal:subscription_succeeded" {
                    return;
                }
            }
            Some(Ok(_)) => {}
            msg => panic!("unexpected message waiting for subscription_succeeded: {msg:?}"),
        }
    }
}

/// The result of waiting for the graceful-drain sequence on a client:
/// the `pusher:error` event (if any) that arrived before the Close, and the
/// WS close code.
struct DrainClose {
    /// The close code from the WS Close frame (None if the stream ended
    /// without a Close frame).
    code: Option<u16>,
    /// True if a `pusher:error` Text frame with `data.code == 4200` arrived
    /// immediately before the Close frame (belt-and-suspenders Soketi convention).
    had_pusher_error_4200: bool,
}

/// Wait for the graceful-drain sequence on `ws`: an optional `pusher:error`
/// 4200 Text frame followed by the WS Close frame. Returns a [`DrainClose`]
/// with the close code and whether the `pusher:error` 4200 event was seen.
async fn wait_close(ws: &mut Ws) -> DrainClose {
    let mut had_pusher_error_4200 = false;
    loop {
        match ws.next().await {
            Some(Ok(Message::Close(Some(cf)))) => {
                return DrainClose {
                    code: Some(u16::from(cf.code)),
                    had_pusher_error_4200,
                }
            }
            Some(Ok(Message::Close(None))) | None => {
                return DrainClose {
                    code: None,
                    had_pusher_error_4200,
                }
            }
            Some(Ok(Message::Text(t))) => {
                // Check if this is the pusher:error 4200 frame that precedes
                // the Close on a graceful drain.
                if let Ok(v) = serde_json::from_str::<Value>(&t) {
                    if v["event"] == "pusher:error" {
                        let code = v["data"]["code"].as_u64().unwrap_or(0);
                        if code == 4200 {
                            had_pusher_error_4200 = true;
                        }
                    }
                }
            }
            Some(Ok(_)) => {} // ignore other frames while draining
            Some(Err(_)) => {
                return DrainClose {
                    code: None,
                    had_pusher_error_4200,
                }
            }
        }
    }
}

/// Main drain test: connect 3 clients, subscribe them, trigger graceful
/// shutdown, assert each receives a `pusher:error` 4200 event followed by
/// Close(4200), and that conn_counts returns to 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_drain_sends_pusher_error_4200_and_close_4200() {
    let _lock = HARNESS_LOCK.lock().await;
    const N: usize = 3;
    // A 2-second grace window: plenty to flush a tiny Close frame, tight enough
    // that the test completes in reasonable time even under load.
    let h = spawn_with_grace(2000).await;

    // Connect N clients and bring them to the established + subscribed state.
    let mut clients: Vec<Ws> = Vec::with_capacity(N);
    for _ in 0..N {
        let mut ws = connect(h.port).await;
        wait_established(&mut ws).await;
        subscribe(&mut ws, "test-channel").await;
        clients.push(ws);
    }

    // Confirm the counters are live before triggering shutdown.
    {
        let count = h
            .conn_counts
            .get(APP_ID)
            .map(|v| v.load(Ordering::SeqCst))
            .unwrap_or(0);
        assert_eq!(
            count, N,
            "all {N} connections should be counted before drain"
        );
    }

    // Trigger the graceful shutdown. Workers will: deregister the listener, queue
    // pusher:error(4200) + Close(4200) on every open connection, flush, then call
    // remove() on each.
    h.shutdown.store(true, Ordering::SeqCst);

    // Each client must receive the drain sequence within the grace window + slack.
    let wall = Duration::from_secs(5);
    let results = tokio::time::timeout(wall, async {
        let mut results = Vec::with_capacity(N);
        for mut ws in clients {
            results.push(wait_close(&mut ws).await);
        }
        results
    })
    .await
    .expect("all clients should receive Close frames within 5s");

    // (a) Every client got a Close frame with code 4200 (Pusher "reconnect
    // immediately"), preceded by a pusher:error 4200 text event.
    for (i, result) in results.iter().enumerate() {
        assert_eq!(
            result.code,
            Some(4200),
            "client {i} should have received Close(4200), got {:?}",
            result.code
        );
        assert!(
            result.had_pusher_error_4200,
            "client {i} should have received pusher:error(4200) before the Close frame"
        );
    }

    // (b) The worker thread should exit (cleanup ran) — join it with a timeout.
    // We take the handle out of h to join without triggering the Drop path.
    // Drop sets shutdown again (no-op) then joins — both are safe here.
    // Give the worker up to 3s to finish its drain loop after the Close frames
    // are delivered (it still needs to call remove() on each connection).
    let join_deadline = std::time::Instant::now() + Duration::from_secs(3);
    // The worker was already signalled; just wait for it to exit by polling its
    // conn_counts: they should all reach 0 within the grace window.
    loop {
        let count = h
            .conn_counts
            .get(APP_ID)
            .map(|v| v.load(Ordering::SeqCst))
            .unwrap_or(0);
        if count == 0 {
            break;
        }
        if std::time::Instant::now() >= join_deadline {
            panic!(
                "conn_counts[{APP_ID}] still {} after drain — remove() did not run for all connections",
                count
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Backpressure drain test: ensure that when a connection has queued-but-not-yet-
/// flushed bytes at the moment of shutdown (`inflight_bytes > 0`), the drain does
/// NOT fire the `inflight_bytes == 0` exit prematurely and the `debug_assert_eq!`
/// invariant holds (no panic in debug builds).
///
/// Without Fix 1 (missing `fold_delta` after the drain-phase `send_close` loop):
///   * `inflight_bytes` stays 0 even though the Close frame is queued on the
///     backpressured connection.
///   * The drain's `inflight_bytes == 0` guard exits immediately, dropping the
///     frame without flushing it.
///   * In debug mode the `debug_assert_eq!(inflight_bytes, sum(out_bytes))` fires
///     and panics — that is exactly what this test catches.
///
/// With Fix 1 in place:
///   * `fold_delta` folds the Close frame's queued bytes into `inflight_bytes`.
///   * The drain loop iterates until those bytes flush or the grace window expires.
///   * `debug_assert_eq` holds throughout; the test passes without panic.
///
/// # How backpressure is produced
///
/// The harness connects one WS client and subscribes it to a channel. It then
/// STOPS reading from the client socket (never calls `.next()` on the stream
/// after subscribe) and floods the channel with `N_FLOOD` broadcast events, each
/// carrying a ~1 KB payload. Once the TCP socket's receive buffer is saturated,
/// `conn.flush()` returns `WouldBlock` and the frames accumulate in `out_bytes`,
/// making `inflight_bytes > 0`. Shutdown is then triggered.
///
/// Determinism note: we flood enough data (≥ 1 MB) that the kernel's per-socket
/// receive buffer (typically 64–128 KB) fills up and backpressure is virtually
/// guaranteed. A small delay between subscribe and flood ensures the subscription
/// frame is drained first, so only the broadcast frames cause the WouldBlock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_drain_with_backpressured_client() {
    let _lock = HARNESS_LOCK.lock().await;

    // 5 s grace window: enough for the worker to flush the queued Close frame even
    // through a slow-draining TCP send buffer; tight enough that the test doesn't
    // hang if something goes wrong.
    let h = spawn_with_grace(5000).await;

    // Connect one client and subscribe it so the channel is registered.
    let mut ws = connect(h.port).await;
    wait_established(&mut ws).await;
    subscribe(&mut ws, "flood-channel").await;

    // Give the worker a moment to drain the subscription_succeeded frame so it is
    // not counted as inflight when we flood — we want only broadcast frames to
    // contribute to inflight_bytes.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // NOW stop reading from the socket: we hold `ws` but never call `.next()` on
    // it again. The TCP receive buffer on the client side will fill up once we
    // start flooding, causing the server's `conn.flush()` to return WouldBlock
    // and leaving frames in `out_bytes` (=> inflight_bytes > 0).

    // Flood the channel with N_FLOOD large events through the adapter.
    // ~1 KB payload × 1 000 iterations = ~1 MB of broadcast data, enough to
    // saturate the ~64–128 KB kernel receive buffer and guarantee WouldBlock.
    const N_FLOOD: usize = 1_000;
    let pad = "x".repeat(1024); // ~1 KB per event
    let adapter = h.adapter.clone();
    let pad_clone = pad.clone();
    tokio::spawn(async move {
        for i in 0..N_FLOOD {
            adapter
                .broadcast(
                    APP_ID,
                    "flood-channel",
                    ServerEvent::ChannelEvent {
                        channel: "flood-channel".to_string(),
                        event: "flood".to_string(),
                        data: serde_json::json!({ "i": i, "pad": pad_clone }),
                        user_id: None,
                    },
                    None,
                )
                .await;
        }
    });

    // Give the flood task a moment to queue frames into the worker. The non-reading
    // client will saturate quickly; we don't need to wait for all N_FLOOD events —
    // just enough that the worker's out-queue has bytes when we signal shutdown.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Trigger graceful shutdown while inflight_bytes is (very likely) > 0.
    // The regression guard is: if Fix 1 is absent, the worker panics here in
    // debug mode on the debug_assert_eq!. With Fix 1, it drains cleanly.
    h.shutdown.store(true, Ordering::SeqCst);

    // The worker must exit cleanly (no panic from the worker thread) and the
    // conn_counts must return to 0 within grace_ms + some slack.
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        let count = h
            .conn_counts
            .get(APP_ID)
            .map(|v| v.load(Ordering::SeqCst))
            .unwrap_or(0);
        if count == 0 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "conn_counts[{APP_ID}] still {count} after drain — \
                 remove() did not run (or worker panicked on debug_assert)"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // `ws` is intentionally kept alive (not read) until here so the TCP receive
    // buffer stays full during the drain, keeping the backpressure scenario live.
    drop(ws);
}
