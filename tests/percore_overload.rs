//! SP10 Phase 1 — the headline overload regression test for the per-core
//! transport: under a publish flood that outruns delivery, the server must
//! **not hang or OOM** and must **stay alive**.
//!
//! Background (SP9, measured): the per-connection out-queue and the
//! publish→workers broadcast hand-off were both unbounded, so a publish spike
//! grew memory to 9.2 GB and the server wedged at 0% CPU. Phase 1 bounds both
//! buffers with drop-head (per-connection, freshest-wins) and a bounded
//! `sync_channel` hand-off (drop-on-full). WebSocket delivery is at-most-once,
//! so dropping is the correct overload response.
//!
//! This test mirrors `tests/percore_multiworker.rs`'s `run_percore` harness:
//! a real multi-worker percore server on an ephemeral port, a REST plane served
//! on the test runtime, and a single concrete `LocalAdapter` with the sharded
//! broadcast sink installed. It then:
//!
//!   * connects ~200 subscribers to one channel,
//!   * floods REST publishes **as fast as possible** for ~5 s (the SP9 scenario),
//!   * asserts — under a hard 30 s `tokio::time::timeout` wall — that the whole
//!     run COMPLETES (no hang),
//!   * then connects a FRESH probe subscriber and publishes one more frame,
//!     asserting it arrives within 2 s (the server is alive, not wedged).
//!
//! The budget-bound assertion (total inflight ≤ budget) uses the per-worker
//! byte-budget accounting + the `percore_total_inflight_bytes()` debug hook
//! (wired in SP10 Phase 2); see `overload_total_inflight_stays_within_budget`.

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::auth::signature::{hmac_sha256_hex, md5_hex};
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::server::router::{build_router, AppState};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

const SECRET: &str = "app-secret";
const KEY: &str = "app-key";
const N_SUBS: usize = 200;
const N_WORKERS: usize = 4;
/// Hard wall: the whole flood + drain must finish well inside this. A hang would
/// blow past it; the `tokio::time::timeout` then fails the test deterministically
/// instead of wedging CI.
const WALL: Duration = Duration::from_secs(30);
/// How long to flood publishes as fast as possible.
const FLOOD: Duration = Duration::from_secs(5);

const APPS: &str = r#"[
    {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
     "capacity":0,"client_messages_enabled":true,"subscription_count_enabled":false}
]"#;

/// Serializes the overload tests. The `percore_total_inflight_bytes()` debug
/// hook sums a PROCESS-GLOBAL slot vector that each `run_percore` replaces on
/// spawn, so two concurrent percore harnesses would clobber each other's slots.
/// Every test here holds this lock for its duration so only one harness lives at
/// a time. `tokio::sync::Mutex` is await-safe (held across the test body).
static HARNESS_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Base percore config for a harness on `port` — overload knobs left at their
/// auto defaults. Individual tests clone this and set the budget/cap fields
/// directly (no process-global `PYLON_*` env, so tests stay parallel-safe).
fn base_config(port: u16) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1".to_string(),
        port,
        workers: N_WORKERS,
        ..Default::default()
    }
}

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

/// Reserve a free port via a throwaway std listener, then drop it.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Start a 4-worker percore server on an ephemeral port with a concrete
/// `LocalAdapter` (sharded sink installed by `run_percore`) and a REST plane
/// served on the test's tokio runtime. Waits for the listeners to bind.
async fn spawn() -> Harness {
    spawn_with(base_config(free_port())).await
}

/// Start a percore harness from an explicit `config` (so a test can set the SP10
/// budget/cap knobs directly without touching process-global env).
async fn spawn_with(config: ServerConfig) -> Harness {
    let port = config.port;

    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let local = Arc::new(LocalAdapter::new(
        Arc::new(Registry::new()),
        Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
    ));
    let adapter: Arc<dyn Adapter> = local.clone();
    let conn_counts = Arc::new(Default::default());
    let webhooks = pylon::webhook::WebhookHandle::null();

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
            None,  // no cluster bridge handle
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

/// Subscribe `ws` to a PUBLIC channel (no auth) and drain its
/// `subscription_succeeded`.
async fn subscribe_public(ws: &mut Ws, channel: &str) {
    ws.send(Message::Text(
        json!({
            "event": "pusher:subscribe",
            "data": { "channel": channel }
        })
        .to_string(),
    ))
    .await
    .unwrap();
    let succ = next_json(ws).await;
    assert_eq!(succ["event"], "pusher_internal:subscription_succeeded");
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

/// Fire one REST publish to `channel` with `data`. Returns the HTTP status (the
/// flood ignores it; the probe checks it is 200).
async fn publish(port: u16, client: &reqwest::Client, channel: &str, data: &str) -> u16 {
    let body = json!({
        "name": "flood",
        "data": data,
        "channels": [channel],
    })
    .to_string();
    let q = signed_query("POST", "/apps/app/events", body.as_bytes());
    match client
        .post(format!("http://127.0.0.1:{port}/apps/app/events?{q}"))
        .body(body)
        .send()
        .await
    {
        Ok(r) => r.status().as_u16(),
        // A transient connection error during the all-out flood is fine — the
        // point is the server doesn't hang; we don't require every publish to
        // succeed.
        Err(_) => 0,
    }
}

/// THE GATE: flood publishes past delivery capacity and prove the server neither
/// hangs nor wedges. The bounded per-connection drop-head queue + bounded
/// broadcast hand-off keep memory bounded, so the whole run completes inside the
/// hard wall and a fresh subscriber is still served afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overload_flood_does_not_hang_and_server_stays_alive() {
    let _guard = HARNESS_LOCK.lock().await;
    // The entire test body runs under a hard wall: a hang fails deterministically
    // instead of blocking CI forever.
    tokio::time::timeout(WALL, async {
        let h = spawn().await;
        let channel = "flood-chan";

        // ── Connect ~200 subscribers to the one flooded channel. ──────────────
        // (Public channel → no per-sub auth needed.) The kernel spreads them
        // across the 4 workers' SO_REUSEPORT accept queues.
        let mut subs: Vec<Ws> = Vec::with_capacity(N_SUBS);
        for _ in 0..N_SUBS {
            let mut ws = connect(h.port).await;
            // Drain pusher:connection_established.
            let est = next_json(&mut ws).await;
            assert_eq!(est["event"], "pusher:connection_established");
            subscribe_public(&mut ws, channel).await;
            subs.push(ws);
        }

        // The subscribers are intentionally NOT read during the flood: their
        // out-queues back up, exercising the per-connection drop-head path (a
        // slow consumer must not grow memory without bound).

        // ── Flood REST publishes as fast as possible for ~5 s. ────────────────
        // Several concurrent publisher tasks keep the pipeline saturated. Each
        // fires back-to-back with no rate limit — the SP9 hang scenario.
        let client = reqwest::Client::new();
        let port = h.port;
        let mut publishers = Vec::new();
        for _ in 0..8 {
            let client = client.clone();
            publishers.push(tokio::spawn(async move {
                let start = Instant::now();
                let mut sent: u64 = 0;
                while start.elapsed() < FLOOD {
                    let _ = publish(port, &client, channel, "{\"n\":1}").await;
                    sent += 1;
                }
                sent
            }));
        }
        let mut total_sent: u64 = 0;
        for p in publishers {
            total_sent += p.await.expect("publisher task panicked");
        }
        // Sanity: the flood actually pushed a meaningful number of publishes
        // through (so we really exercised the overload path, not a no-op).
        assert!(
            total_sent > 0,
            "flood sent no publishes; harness/REST plane broken"
        );

        // Reaching here means the flood + the server's drain did not hang: the
        // bounded queues capped memory and the publishers all returned.

        // ── Server still alive: a FRESH probe subscriber gets a new frame. ────
        // Subscribe a brand-new connection to a SEPARATE channel (no flood
        // backlog), publish once, and require delivery within 2 s.
        let probe_chan = "probe-chan";
        let mut probe = connect(h.port).await;
        let est = next_json(&mut probe).await;
        assert_eq!(est["event"], "pusher:connection_established");
        subscribe_public(&mut probe, probe_chan).await;

        let status = publish(h.port, &client, probe_chan, "{\"alive\":true}").await;
        assert_eq!(status, 200, "probe publish must be accepted (server alive)");

        let frame = tokio::time::timeout(Duration::from_secs(2), next_json(&mut probe))
            .await
            .expect("probe frame within 2s — server is wedged");
        assert_eq!(frame["event"], "flood", "probe got wrong event");
        assert_eq!(frame["channel"], probe_chan, "probe wrong channel");
        assert_eq!(frame["data"], "{\"alive\":true}", "probe wrong data");

        drop(subs);
        drop(h);
    })
    .await
    .expect("OVERLOAD HANG: the flood + drain did not complete within the wall");
}

/// Publish a sequence-numbered frame to `channel`: the data is `"<seq>:<pad>"`
/// so each frame is large enough (≈ `pad` bytes) that an un-drained slow consumer
/// backs up past its out-queue cap and drop-head evicts. The leading `<seq>:` is
/// recovered to verify ordering / freshest-wins.
async fn publish_seq(
    port: u16,
    client: &reqwest::Client,
    channel: &str,
    seq: u64,
    pad: &str,
) -> u16 {
    publish(port, client, channel, &format!("{seq}:{pad}")).await
}

/// Recover the sequence number from a `"<seq>:<pad>"` flood payload.
fn seq_of(data: &str) -> u64 {
    data.split_once(':').unwrap().0.parse().unwrap()
}

/// G8 (drop-head observability): scrape `/metrics` and return the value of
/// `pylon_drophead_dropped_total{worker="0"}` (None when the series is absent).
/// The harness serves REST on the same port the WS clients connect to, so the
/// percore registry backing this flood is the one being read.
async fn drophead_dropped_total(port: u16, client: &reqwest::Client) -> Option<u64> {
    let body = client
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;
    body.lines()
        .find(|l| l.starts_with(r#"pylon_drophead_dropped_total{worker="0"}"#))
        .and_then(|l| l.split_whitespace().last())
        .and_then(|v| v.parse().ok())
}

/// Task 2.3 — TARGETED SHED + FRESHEST-WINS. On a single-worker percore server
/// with a tiny budget, flood a channel with sequence-numbered frames. A FAST
/// subscriber that drains continuously keeps its out-queue empty, so the
/// graduated shed never skips it — it receives EVERY published frame, in order.
/// A SLOW subscriber that never reads backs up, so it is shed / drop-headed: it
/// loses frames, and the frames it DOES end up with are the NEWEST (freshest-wins
/// drop-head), never a stale prefix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overload_targeted_shed_fast_gets_all_slow_gets_freshest() {
    let _guard = HARNESS_LOCK.lock().await;
    // One worker so the fast + slow subscriber share the same per-worker budget
    // (SO_REUSEPORT can't otherwise guarantee colocation). Tiny budget + small
    // per-conn cap so the slow consumer saturates quickly.
    let config = ServerConfig {
        workers: 1,
        memory_budget_bytes: 1 << 20, // 1 MiB worker budget
        expected_conns_per_worker: 8,
        perconn_queue_min_bytes: 16 << 10,
        perconn_queue_max_bytes: 64 << 10,
        ..base_config(free_port())
    };
    const N_PUB: u64 = 4000;
    // ≈ 2 KiB per frame so an un-drained slow consumer overflows its 64 KiB cap
    // (and socket buffers) after a few dozen frames → drop-head evicts the oldest.
    let pad = "p".repeat(2048);

    let result = tokio::time::timeout(WALL, async {
        let h = spawn_with(config).await;
        let channel = "shed-chan";

        // FAST subscriber: subscribed, then drained continuously by a task that
        // records every sequence number it receives, in arrival order.
        let mut fast = connect(h.port).await;
        let est = next_json(&mut fast).await;
        assert_eq!(est["event"], "pusher:connection_established");
        subscribe_public(&mut fast, channel).await;

        // SLOW subscriber: subscribed, then NEVER read until after the flood.
        let mut slow = connect(h.port).await;
        let est = next_json(&mut slow).await;
        assert_eq!(est["event"], "pusher:connection_established");
        subscribe_public(&mut slow, channel).await;

        // Drain the FAST subscriber in the background, collecting sequence numbers.
        let fast_seqs = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
        let fast_seqs_bg = fast_seqs.clone();
        let drain_fast = tokio::spawn(async move {
            loop {
                match tokio::time::timeout(Duration::from_millis(500), fast.next()).await {
                    Ok(Some(Ok(Message::Text(t)))) => {
                        let v: Value = serde_json::from_str(&t).unwrap();
                        if v["event"] == "flood" {
                            fast_seqs_bg
                                .lock()
                                .unwrap()
                                .push(seq_of(v["data"].as_str().unwrap()));
                        }
                    }
                    Ok(Some(Ok(_))) => {}
                    // Idle gap after the flood ended → done draining.
                    Err(_) => break,
                    Ok(Some(Err(_))) | Ok(None) => break,
                }
            }
        });

        // Flood the channel with N_PUB sequence-numbered publishes, paced just
        // enough that the slow consumer backs up but the fast one keeps up.
        let client = reqwest::Client::new();
        for seq in 1..=N_PUB {
            let _ = publish_seq(h.port, &client, channel, seq, &pad).await;
        }

        // Let the fast drain settle, then collect its received sequence list.
        tokio::time::sleep(Duration::from_millis(800)).await;
        let _ = drain_fast.await;
        let fast_got = fast_seqs.lock().unwrap().clone();

        // FAST: received EVERY published frame, in order, exactly once.
        assert_eq!(
            fast_got.len() as u64,
            N_PUB,
            "fast subscriber must receive ALL {N_PUB} frames (got {})",
            fast_got.len()
        );
        let expected: Vec<u64> = (1..=N_PUB).collect();
        assert_eq!(
            fast_got, expected,
            "fast subscriber frames out of order / missing"
        );

        // SLOW: drain whatever it managed to queue (drop-head kept the newest).
        let mut slow_got: Vec<u64> = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_millis(500), slow.next()).await {
                Ok(Some(Ok(Message::Text(t)))) => {
                    let v: Value = serde_json::from_str(&t).unwrap();
                    if v["event"] == "flood" {
                        slow_got.push(seq_of(v["data"].as_str().unwrap()));
                    }
                }
                Ok(Some(Ok(_))) => {}
                _ => break,
            }
        }

        // TARGETED: the slow subscriber lost frames (was shed) while the fast one
        // got them all.
        assert!(
            (slow_got.len() as u64) < N_PUB,
            "slow subscriber should have LOST frames (got {} of {N_PUB})",
            slow_got.len()
        );
        assert!(!slow_got.is_empty(), "slow subscriber got nothing at all");
        // FRESHEST-WINS: the newest frame (N_PUB) survived. drop-head evicts the
        // OLDEST queued frame for a slow consumer, so the latest data always wins
        // — the freshest frame must be among those the slow subscriber received.
        let slow_max = *slow_got.iter().max().unwrap();
        assert_eq!(
            slow_max, N_PUB,
            "slow subscriber missed the NEWEST frame (max seq {slow_max} != {N_PUB}) — \
             drop-head must keep the freshest"
        );

        // ── G8: the drop-head evictions are OBSERVABLE. The slow subscriber's
        // loss above proves drop-head evictions occurred (freshest-wins is only
        // reachable through `Connection::queue` evicting the oldest frames);
        // that loss must be reflected in `pylon_drophead_dropped_total` — silent
        // frame loss is the finding this metric exists to close. Poll briefly:
        // the worker mirrors its local total into the shared slot at loop top,
        // so the value is visible within a few iterations after the flood.
        let mut drophead: Option<u64> = None;
        for _ in 0..10 {
            drophead = drophead_dropped_total(h.port, &client).await;
            if drophead.unwrap_or(0) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let drophead = drophead.unwrap_or(0);
        assert!(
            drophead > 0,
            "pylon_drophead_dropped_total{{worker=\"0\"}} must be > 0 after a flood that \
             evicted {}+ frames via drop-head (got {drophead}); silent frame loss",
            N_PUB - slow_got.len() as u64
        );

        drop(slow);
        drop(h);
    })
    .await;
    result.expect("targeted-shed test did not complete within the wall");
}

/// Task 2.3 — 503 ADMISSION. Under a sustained flood that saturates the broadcast
/// hand-off / per-worker budget, `POST /events` returns 503 (admission control);
/// once the flood stops and delivery drains, a publish returns 200 again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overload_publish_returns_503_then_200_after() {
    let _guard = HARNESS_LOCK.lock().await;
    // Tiny budget + small hand-off cap so saturation is easy to provoke.
    let config = ServerConfig {
        memory_budget_bytes: 2 << 20,
        expected_conns_per_worker: 50,
        perconn_queue_min_bytes: 16 << 10,
        perconn_queue_max_bytes: 64 << 10,
        broadcast_handoff_cap: 8,
        ..base_config(free_port())
    };

    let result = tokio::time::timeout(WALL, async {
        let h = spawn_with(config).await;
        let channel = "admit-chan";

        // Many never-reading subscribers so delivery backs up and the pipeline
        // saturates under the flood.
        let mut subs: Vec<Ws> = Vec::with_capacity(N_SUBS);
        for _ in 0..N_SUBS {
            let mut ws = connect(h.port).await;
            let est = next_json(&mut ws).await;
            assert_eq!(est["event"], "pusher:connection_established");
            subscribe_public(&mut ws, channel).await;
            subs.push(ws);
        }

        let client = reqwest::Client::new();
        let port = h.port;
        // Flood with a big payload from several publishers; concurrently sample
        // the publish status. Under saturation at least one must be a 503.
        let big = "y".repeat(8192);
        let saw_503 = Arc::new(AtomicBool::new(false));
        let mut publishers = Vec::new();
        for _ in 0..8 {
            let client = client.clone();
            let payload = big.clone();
            let saw = saw_503.clone();
            publishers.push(tokio::spawn(async move {
                let start = Instant::now();
                while start.elapsed() < FLOOD {
                    let status = publish(port, &client, channel, &payload).await;
                    if status == 503 {
                        saw.store(true, Ordering::SeqCst);
                    }
                }
            }));
        }
        for p in publishers {
            let _ = p.await;
        }

        assert!(
            saw_503.load(Ordering::SeqCst),
            "under sustained flood, POST /events must return 503 at least once"
        );

        // After the flood: drain the subscribers so the workers clear saturation,
        // then a publish to a fresh channel must be accepted (200).
        for ws in subs.iter_mut() {
            // Drain a few frames each so out-queues empty and saturation clears.
            for _ in 0..50 {
                if tokio::time::timeout(Duration::from_millis(50), ws.next())
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
        // Give the workers a moment to drain inboxes and clear the saturated flag.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Retry briefly: the flag clears on the next worker drain cycle.
        let mut got_200 = false;
        for _ in 0..40 {
            let status = publish(h.port, &client, "post-flood-chan", "{\"ok\":1}").await;
            if status == 200 {
                got_200 = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            got_200,
            "after the flood, POST /events must return 200 again"
        );

        drop(subs);
        drop(h);
    })
    .await;
    result.expect("503-admission test did not complete within the wall");
}

/// Phase 2 gate: under the flood, the total bytes queued across all workers must
/// never exceed the configured memory budget. With a small explicit budget
/// (`PYLON_MEMORY_BUDGET_BYTES`), flood publishes to many never-reading
/// subscribers and sample `percore_total_inflight_bytes()` throughout — it must
/// stay within the budget (a small per-frame slack for the in-flight enqueue
/// before the next loop-top recompute / shed kicks in).
/// Phase 2 gate: under the flood, the total bytes queued across all workers must
/// never exceed the configured memory budget. With a small explicit budget,
/// flood publishes to many never-reading subscribers and sample
/// `percore_total_inflight_bytes()` throughout — it must stay within the budget
/// (a small per-frame slack for the in-flight enqueue before the next loop-top
/// recompute / shed kicks in).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overload_total_inflight_stays_within_budget() {
    let _guard = HARNESS_LOCK.lock().await;
    // A small, explicit budget so the bound is tight and the test is fast.
    const BUDGET: u64 = 16 << 20; // 16 MiB across all workers (4 MiB/worker)
                                  // Size the per-conn cap so the per-worker drop-head caps SUM to the per-worker
                                  // budget: 4 MiB/worker ÷ 50 expected ≈ 84 KiB; with ~50 subs/worker that's
                                  // 50 × 84 KiB ≈ 4 MiB = per-worker budget, so total inflight is bounded by
                                  // both the graduated shed (new enqueues) AND the per-conn drop-head (already
                                  // queued) — the two together hold the total at/under the budget.
    let config = ServerConfig {
        memory_budget_bytes: BUDGET,
        expected_conns_per_worker: 50,
        perconn_queue_min_bytes: 16 << 10,
        perconn_queue_max_bytes: 128 << 10,
        ..base_config(free_port())
    };

    let result = tokio::time::timeout(WALL, async {
        let h = spawn_with(config).await;
        let channel = "budget-chan";

        // Connect subscribers that NEVER read — their out-queues back up, so the
        // per-worker budget + drop-head must cap total queued bytes.
        let mut subs: Vec<Ws> = Vec::with_capacity(N_SUBS);
        for _ in 0..N_SUBS {
            let mut ws = connect(h.port).await;
            let est = next_json(&mut ws).await;
            assert_eq!(est["event"], "pusher:connection_established");
            subscribe_public(&mut ws, channel).await;
            subs.push(ws);
        }

        let client = reqwest::Client::new();
        let port = h.port;
        // A larger payload so queued bytes accumulate fast against the small budget.
        let big = "x".repeat(4096);
        let big2 = big.clone();
        let mut publishers = Vec::new();
        for _ in 0..8 {
            let client = client.clone();
            let payload = big2.clone();
            publishers.push(tokio::spawn(async move {
                let start = Instant::now();
                while start.elapsed() < FLOOD {
                    let _ = publish(port, &client, channel, &payload).await;
                }
            }));
        }

        // Sample the inflight total while the flood runs; it must never exceed
        // the budget plus a small slack (a handful of in-flight per-conn caps per
        // worker — the most a single drain can transiently add before the next
        // recompute / shed). per_conn_cap here is ≤ 128 KiB.
        let slack: u64 = (N_WORKERS as u64) * 4 * (128 << 10);
        let mut max_seen: u64 = 0;
        let sample_until = Instant::now() + FLOOD;
        while Instant::now() < sample_until {
            let inflight = pylon::transport::percore_total_inflight_bytes();
            max_seen = max_seen.max(inflight);
            assert!(
                inflight <= BUDGET + slack,
                "inflight {inflight} exceeded budget {BUDGET} (+slack {slack})"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        for p in publishers {
            let _ = p.await;
        }
        // We must have actually accumulated SOME queued bytes (proving the path
        // was exercised, not a no-op where everything drained instantly).
        assert!(
            max_seen > 0,
            "no inflight bytes ever observed; flood was a no-op"
        );

        drop(subs);
        drop(h);
    })
    .await;

    result.expect("budget flood did not complete within the wall");
}

/// Task 3.1 (finding G1) — NO BUSY-SPIN ON A BACKPRESSURED CONNECTION, AND NO
/// LOSS. One subscriber on a plain connection subscribes to a channel and then
/// STOPS READING. The publisher floods ADAPTIVELY — publishing until the
/// worker actually holds queued bytes for that connection
/// (`percore_total_inflight_bytes() > 0`, self-sizing to whatever this
/// runner's kernel buffers absorb) — so the connection is backpressured with a
/// non-empty out-queue. Then all work stops.
///
/// `mio` is level-triggered: a connection with a full send buffer produces NO
/// writable event, so a loop that polls 0 ms whenever `inflight_bytes > 0` —
/// even when the previous iteration did no work — spins the whole worker core
/// at 100% CPU. The loop must instead park in the 50 ms poll, which is sound
/// only because every connection with queued bytes holds WRITABLE interest
/// (armed by `flush_and_arm` on `WouldBlock`): the kernel wakes the loop the
/// moment the socket drains.
///
/// Asserts BOTH halves:
///
///   1. NO SPIN: during a quiet window with the backlog still queued, the
///      `percore_poll_zero_timeouts()` counter stops growing (bounded by a
///      handful of straggler iterations after the last enqueue).
///   2. NO LOSS: the queued bytes REMAIN queued while parked (inflight stays
///      > 0 — nothing was dropped to escape the poll), and once the client
///      finally drains, EVERY published frame is delivered in order via the
///      wake-on-writable path, with inflight reaching exactly 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backpressured_connection_does_not_spin_and_backlog_survives() {
    let _guard = HARNESS_LOCK.lock().await;
    // ONE worker so the measured poll loop is the only one in the process, and
    // no shed/CoDel/drop-head interference with the backlog: big budget, big
    // per-conn cap, CoDel off. Whatever backs up must STAY queued.
    let config = ServerConfig {
        workers: 1,
        memory_budget_bytes: 64 << 20,
        expected_conns_per_worker: 8,
        perconn_queue_min_bytes: 8 << 20,
        perconn_queue_max_bytes: 8 << 20,
        codel_target_ms: 0,
        ..base_config(free_port())
    };
    // ≈ 8.3 KiB per frame. The flood VOLUME is adaptive (stop at the first
    // observed backpressure — see the loop below), so no fixed byte budget
    // has to guess the runner's kernel buffer sizes. The only hard bound:
    // MAX_PUB × 8.3 KiB ≈ 6.6 MiB stays under the 8 MiB per-conn cap, so
    // drop-head can never fire and every published frame must survive to be
    // delivered — while still covering kernel absorptions up to ~6 MiB
    // (server send autotune ceiling is 4 MiB on stock Linux).
    const MAX_PUB: u64 = 800;
    let pad = "z".repeat(8192);
    // This test's own wall (wider than the file-wide `WALL`): on a slow
    // 2-core runner the adaptive flood alone can take ~10 s, and the bounded
    // drain deadline below is 30 s — the wall must never be what fires (that
    // would mask the real, better-diagnosed assertions).
    const SPIN_WALL: Duration = Duration::from_secs(90);

    let result = tokio::time::timeout(SPIN_WALL, async {
        let h = spawn_with(config).await;
        let channel = "spin-chan";

        // ── The backpressured subscriber: a plain connection that simply
        //    never reads after the subscription ack. Backpressure is provoked
        //    by flood volume, not by socket surgery. ────────────────────────
        let mut slow = connect_plain(h.port).await;
        let est = next_json_raw(&mut slow).await;
        assert_eq!(est["event"], "pusher:connection_established");
        slow.send(Message::Text(
            json!({
                "event": "pusher:subscribe",
                "data": { "channel": channel }
            })
            .to_string(),
        ))
        .await
        .unwrap();
        let succ = next_json_raw(&mut slow).await;
        assert_eq!(succ["event"], "pusher_internal:subscription_succeeded");

        // ── Flood ADAPTIVELY: publish until the worker actually holds queued
        //    bytes for this connection, then STOP IMMEDIATELY. The flood
        //    volume self-sizes to the runner's kernel buffers instead of
        //    guessing them (a fixed multi-MiB flood wastes minutes draining
        //    back through the pinned window on Linux — CI run 33334731440
        //    failed exactly that way at 400 frames — while a small fixed one
        //    may never back up a big-buffered runner at all).
        let client = reqwest::Client::new();
        let mut published: u64 = 0;
        loop {
            published += 1;
            let status = publish_seq(h.port, &client, channel, published, &pad).await;
            assert_eq!(status, 200, "flood publish must be accepted (no shed here)");
            // Give the worker a loop iteration to drain its broadcast inbox,
            // flush, and mirror the POST-flush queue depth into the hook. A
            // positive sample therefore means bytes SURVIVED a flush attempt
            // (a true WouldBlock residue), not a mid-iteration transient —
            // and with the client never reading again, that residue is
            // sticky for the rest of the test.
            tokio::time::sleep(Duration::from_millis(15)).await;
            if pylon::transport::percore_total_inflight_bytes() > 0 {
                break;
            }
            assert!(
                published < MAX_PUB,
                "published {published} frames without ever backing the subscriber \
                 up; backpressure path not exercised on this runner"
            );
        }
        assert!(
            published >= 8,
            "backed up after only {published} frames; the flood was too small to \
             exercise the backpressure path meaningfully"
        );

        // ── QUIET WINDOW: no more publishes; the only live connection is the
        //    silent, backpressured subscriber. Let stragglers settle, then
        //    measure two 0ms-poll-counter snapshots separated by far longer
        //    than a spinning loop would need to run away. ────────────────────
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            pylon::transport::percore_total_inflight_bytes() > 0,
            "backlog must still be queued after the flood"
        );
        let polls_before = pylon::transport::worker::percore_poll_zero_timeouts();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let polls_after = pylon::transport::worker::percore_poll_zero_timeouts();
        let spun = polls_after - polls_before;
        assert!(
            spun <= 8,
            "worker made {spun} zero-timeout polls in a 400ms quiet window with a \
             backpressured connection; the loop is busy-spinning on queued bytes"
        );
        assert!(
            pylon::transport::percore_total_inflight_bytes() > 0,
            "queued bytes vanished while parked; the backlog was silently dropped"
        );

        // ── NO LOSS: drain the client. Writable events wake the loop and the
        //    whole backlog flushes — every frame, in order, inflight → 0. ────
        //
        // RATE NOTE (why this test never pins the client's receive window):
        // an earlier revision shrank `SO_RCVBUF` to a few KiB so backpressure
        // needed only a small flood — but on Linux that also throttles the
        // drain to ~8.3 KiB per writable edge, each edge floor-priced by
        // delayed-ACK / zero-window-probe timers (~205 ms/frame,
        // curve-measured in a 2-CPU Linux container: linear, CONTINUOUS; CI
        // run 33334731440 failed exactly that way — 75 of 400 frames in the
        // 15 s deadline, ~200 ms/frame, no stalls), and the window cannot be
        // reliably reopened afterwards (explicit `SO_RCVBUF` disables receive
        // autotuning; the scale is fixed at handshake time). That is a rate
        // bound of the harness's tiny window, NOT a worker bug: broken
        // WRITABLE arming would deliver NOTHING further (no other path
        // flushes a backpressured connection), yet frames kept arriving via
        // edges in both environments. With a full-size window the drain runs
        // at kernel speed and only rides the wake-on-writable path under
        // test.
        //
        // Delivery is CURVE-INSTRUMENTED (elapsed-ms vs cumulative frames, plus
        // the worst inter-frame gap) so a slow/failed drain reports its shape:
        // continuous-but-slow vs stalled/stepwise.
        let drain_started = Instant::now();
        let mut got: Vec<u64> = Vec::new();
        let mut curve: Vec<(u128, usize)> = Vec::new();
        let mut max_gap = Duration::ZERO;
        let mut last_frame_at = drain_started;
        // Downsampled curve view (every 10th sample + the last) keeps the
        // shape readable in logs.
        let curve_view = |curve: &[(u128, usize)]| -> Vec<(u128, usize)> {
            curve
                .iter()
                .enumerate()
                .filter(|(i, _)| i % 10 == 0)
                .map(|(_, s)| *s)
                .chain(curve.last().copied())
                .collect()
        };
        // Generous but bounded: with a full-size window the drain measures
        // tens of ms unstarved and well under ~2 s on a slow runner — 30 s
        // only ever pays out on a real failure, and the 3 s per-read floor
        // absorbs a stalled scheduler slice without mistaking it for a
        // finished drain.
        let deadline = drain_started + Duration::from_secs(30);
        loop {
            match tokio::time::timeout(Duration::from_millis(3_000), slow.next()).await {
                Ok(Some(Ok(Message::Text(t)))) => {
                    let v: Value = serde_json::from_str(&t).unwrap();
                    if v["event"] == "flood" {
                        let now = Instant::now();
                        max_gap = max_gap.max(now - last_frame_at);
                        last_frame_at = now;
                        got.push(seq_of(v["data"].as_str().unwrap()));
                        curve.push(((now - drain_started).as_millis(), got.len()));
                    }
                }
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(e))) => panic!("backlog drain read failed: {e}"),
                Ok(None) => break, // server closed the stream
                Err(_) => break,   // 3s of silence: backlog fully drained
            }
            if got.len() == published as usize {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "backlog did not fully drain in time; got {} of {published} frames in {:?} \
                 (max inter-frame gap {:?}); delivery curve (elapsed_ms, frames): {:?}",
                got.len(),
                drain_started.elapsed(),
                max_gap,
                curve_view(&curve),
            );
        }
        println!(
            "drain curve (backed up after {published} published frames): {} frames in {:?} \
             (max inter-frame gap {:?}); samples (elapsed_ms, frames): {:?}",
            got.len(),
            drain_started.elapsed(),
            max_gap,
            curve_view(&curve)
        );
        let expected: Vec<u64> = (1..=published).collect();
        assert_eq!(
            got, expected,
            "the whole backlog must be delivered in order after the drain"
        );
        // And nothing remains queued anywhere on the worker.
        let mut inflight_zero = false;
        for _ in 0..200 {
            if pylon::transport::percore_total_inflight_bytes() == 0 {
                inflight_zero = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            inflight_zero,
            "inflight bytes never reached 0 after draining the backlog"
        );

        drop(slow);
        drop(h);
    })
    .await;
    result.expect("busy-spin test did not complete within the wall");
}

/// Connect a plain WebSocket client. The receive buffer is deliberately left
/// at kernel defaults: the flood phase provokes backpressure by VOLUME (the
/// adaptive loop publishes until the worker queue is non-empty), not by
/// pinning the window, and the drain phase then runs at full window speed on
/// every kernel. (An earlier revision shrank `SO_RCVBUF` to force backpressure
/// cheaply — but on Linux that also throttles the drain to ~8.3 KiB per
/// writable edge at ~200 ms each, and the window cannot be reliably reopened
/// afterwards: explicit `SO_RCVBUF` disables receive autotuning, and the
/// window scale is fixed at handshake time. See the drain-phase note.)
async fn connect_plain(port: u16) -> tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> {
    let std_stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    std_stream.set_nonblocking(true).unwrap();
    let stream = tokio::net::TcpStream::from_std(std_stream).unwrap();
    let url = format!("ws://127.0.0.1:{port}/app/{KEY}?protocol=7");
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::client_async(url, stream),
    )
    .await
    .expect("connect within 5s")
    .expect("ws handshake");
    ws
}

/// `next_json` for the raw (non-`MaybeTlsStream`) client returned by
/// [`connect_big_rcvbuf`]. Reads the next Text frame as JSON, 5s wall.
async fn next_json_raw<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
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
