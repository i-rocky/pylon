//! Phase 7 proof: an L1-miss app lookup is OFFLOADED to tokio and the connection
//! is PARKED — the synchronous per-core worker never blocks on the I/O, so other
//! connections keep being served while one lookup is in flight. Also proves a
//! parked connection that disconnects mid-lookup leaks no counter and its late
//! resolution is discarded.

use dashmap::DashMap;
use futures_util::StreamExt;
use pylon::adapter::app_registry::AppRegistry;
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::{App, AppLookupError, AppManager};
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::transport::worker::{run, DispatchEnv, Mode, WorkerConfig};
use serde_json::Value;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// An AppManager whose L1 probe always MISSES (forcing offload) and whose async
/// `by_key` sleeps `delay` before resolving — modelling a slow DB round-trip.
/// `calls` counts how many offloaded lookups actually ran.
struct SlowAppManager {
    app: Arc<App>,
    delay: Duration,
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl AppManager for SlowAppManager {
    async fn by_key(&self, key: &str) -> Result<Option<Arc<App>>, AppLookupError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        if key == self.app.key {
            Ok(Some(self.app.clone()))
        } else {
            Ok(None)
        }
    }
    async fn by_id(&self, id: &str) -> Result<Option<Arc<App>>, AppLookupError> {
        tokio::time::sleep(self.delay).await;
        if id == self.app.id {
            Ok(Some(self.app.clone()))
        } else {
            Ok(None)
        }
    }
    // No `by_key_cached` override → defaults to `None` → always offloads.
}

fn slow_app() -> Arc<App> {
    let mut a: App = serde_json::from_value(serde_json::json!({
        "name": "Slow", "id": "app", "key": "app-key", "secret": "app-secret",
        "capacity": 0, "client_messages_enabled": true
    }))
    .unwrap();
    a.recompute_has_flags();
    Arc::new(a)
}

struct Harness {
    port: u16,
    conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
    calls: Arc<AtomicUsize>,
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

async fn spawn_slow(delay: Duration) -> Harness {
    let calls = Arc::new(AtomicUsize::new(0));
    let apps: Arc<dyn AppManager> = Arc::new(SlowAppManager {
        app: slow_app(),
        delay,
        calls: calls.clone(),
    });
    let registry = Arc::new(Registry::new());
    let app_registry = Arc::new(AppRegistry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry, app_registry.clone()));
    let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());
    let config = ServerConfig::default();
    let env = Arc::new(DispatchEnv {
        apps,
        adapter: adapter.clone(),
        limits: config.limits(),
        activity_timeout: config.activity_timeout,
        pong_timeout: config.pong_timeout,
        strict_protocol: config.strict_protocol,
        conn_counts: conn_counts.clone(),
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
        conn_counts,
        calls,
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

// ── Test (a): parked lookup doesn't block the worker ──────────────────────────

/// Connection A hits the slow (150ms) offloaded lookup and PARKS. While it is
/// parked, connection B connects and ALSO parks on its own offloaded lookup.
/// If the worker blocked on A's lookup, B's offload would only start AFTER A's
/// resolved — giving a total wall-time of ~300ms (two sequential sleeps). If
/// the worker is correctly non-blocking, both lookups are IN FLIGHT concurrently
/// and both A and B establish in ≈150ms total — well under 2× the sleep.
///
/// Proof: measure the time from "both connections opened" to "both have
/// established", assert it is well under 2× the delay (i.e. ≈150ms not ≈300ms).
#[tokio::test]
async fn parked_connection_does_not_block_the_worker() {
    let h = spawn_slow(Duration::from_millis(150)).await;

    // Open A and B in rapid succession; A's lookup is immediately offloaded and
    // its connection parked. B's accept + offload must also fire while A is parked.
    let started = Instant::now();
    let mut a = connect(h.port).await;
    let mut b = connect(h.port).await;

    // Collect both established frames. Both lookups were offloaded concurrently so
    // both should resolve in ~one sleep duration from when the connections opened.
    let fa = next_json(&mut a).await;
    let fb = next_json(&mut b).await;
    let total_elapsed = started.elapsed();

    assert_eq!(fa["event"], "pusher:connection_established");
    assert_eq!(fb["event"], "pusher:connection_established");

    // Serial execution would take ≥300ms (two consecutive 150ms sleeps).
    // Concurrent offloading takes ≈150ms. Allow up to 280ms to be safe on a
    // loaded CI/test machine while still ruling out the blocking (serial) case.
    assert!(
        total_elapsed < Duration::from_millis(280),
        "both connections established in {total_elapsed:?}; expected ~150ms (concurrent offloads) \
         not ~300ms (serial/blocking) — worker may be blocking on A's parked lookup"
    );

    // Both lookups were offloaded (L1 always misses for SlowAppManager).
    assert!(
        h.calls.load(Ordering::SeqCst) >= 2,
        "both lookups must offload"
    );

    // Both connections are counted exactly once.
    let counter = h.conn_counts.get("app").expect("app counter exists");
    assert_eq!(
        counter.load(Ordering::SeqCst),
        2,
        "two live connections counted"
    );
}

// ── Test (b): slow connection eventually establishes ──────────────────────────

/// Even with a 150ms lookup delay the connection receives
/// `pusher:connection_established` within generous tolerance, confirming the
/// park → offload → resume path completes end-to-end.
#[tokio::test]
async fn slow_connection_eventually_establishes() {
    let h = spawn_slow(Duration::from_millis(150)).await;

    let start = Instant::now();
    let mut ws = connect(h.port).await;
    let frame = next_json(&mut ws).await;
    let elapsed = start.elapsed();

    assert_eq!(frame["event"], "pusher:connection_established");
    let data: Value =
        serde_json::from_str(frame["data"].as_str().unwrap()).unwrap();
    assert!(
        data["socket_id"].as_str().unwrap().contains('.'),
        "socket_id should look like `<n>.<n>`"
    );
    // Must take at least the delay (lookup ran), but well under 5s (no hang).
    assert!(
        elapsed >= Duration::from_millis(100),
        "establish completed suspiciously fast ({elapsed:?}); did the slow lookup actually run?"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "establish took too long ({elapsed:?})"
    );
    assert_eq!(
        h.calls.load(Ordering::SeqCst),
        1,
        "exactly one offloaded lookup for one connection"
    );
}

// ── Test (c): parked disconnect leaks nothing + late resolution discarded ─────

/// A connection that disconnects WHILE parked (before its slow lookup resolves)
/// must leave the per-app counter untouched (no counter was taken at park time),
/// and the late `ResolvedApp` for its freed slab token must be safely discarded
/// (no panic, no phantom counter). We drop A mid-lookup, then bring a fresh
/// connection up and confirm the counter reflects exactly one live connection.
#[tokio::test]
async fn parked_disconnect_leaks_no_counter_and_discards_late_resolution() {
    let h = spawn_slow(Duration::from_millis(150)).await;

    // Open A and drop it almost immediately — while its lookup is still sleeping.
    {
        let _a = connect(h.port).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        // `_a` drops here → client closes the socket while A is parked.
    }

    // Wait past the lookup's completion so the late ResolvedApp is drained.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The parked-then-dropped connection took no counter and the late resolution
    // was discarded: either the entry is absent, or it reads 0.
    match h.conn_counts.get("app") {
        None => {}
        Some(c) => assert_eq!(
            c.load(Ordering::SeqCst),
            0,
            "a parked-then-dropped connection must leak no per-app counter"
        ),
    }

    // The worker is still healthy: a new connection establishes and is counted once.
    let mut b = connect(h.port).await;
    let fb = next_json(&mut b).await;
    assert_eq!(fb["event"], "pusher:connection_established");
    let counter = h.conn_counts.get("app").expect("app counter exists after B");
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "exactly one live connection after the parked drop"
    );
}
