//! Lean per-core WebSocket transport (SP9).
//!
//! This module owns the raw RFC 6455 frame layer for the new per-connection
//! transport. Unlike `tokio-tungstenite`, it does **not** eagerly allocate a
//! large (128 KiB) read buffer per connection: framing operates over a
//! caller-owned [`bytes::BytesMut`] that grows lazily, and parsed payloads are
//! cheap `Bytes` slices into that buffer.
//!
//! [`frame`] is the RFC 6455 codec; [`conn`] is the per-connection state +
//! non-blocking read/write that the worker event loop drives. The event loop
//! itself is built in later SP9 tasks.

pub mod conn;
pub mod fanout;
pub mod frame;
pub mod handshake;
pub mod rest;
pub mod timer;
pub mod tls;
pub mod worker;

use crate::adapter::app_registry::AppRegistry;
use crate::adapter::local::LocalAdapter;
use crate::adapter::Adapter;
use crate::app::AppManager;
use crate::server::config::ServerConfig;
use crate::server::resources;
use crate::webhook::WebhookHandle;
use dashmap::DashMap;
use fanout::{BroadcastSink, WorkerSlot};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use worker::{BroadcastWiring, DispatchEnv, Mode, WorkerConfig};

pub use rest::RestConn;

/// Process-global registry of live per-core worker metrics. Filled unconditionally
/// by [`run_percore`] at startup so the `/metrics` handler can read it in production.
static PERCORE_REGISTRY: std::sync::OnceLock<std::sync::Mutex<PercoreRegistry>> =
    std::sync::OnceLock::new();

/// Internal registry holding shared atomics for every per-core worker.
struct PercoreRegistry {
    inflight_slots: Vec<Arc<AtomicU64>>,
    /// Worker slot arcs retained to read `.dropped` (a plain `AtomicU64` embedded
    /// in the slot struct, owned by the `Arc`). `Empty` when `local` is `None`.
    worker_slots: Vec<Arc<fanout::WorkerSlot>>,
    /// Per-worker cumulative accepted-connections counter (B1).
    accepted_slots: Vec<Arc<AtomicU64>>,
    /// Per-worker cumulative CoDel-dropped-frames counter (B1).
    codel_dropped_slots: Vec<Arc<AtomicU64>>,
    /// Task 4: per-worker cumulative mailbox-full-drop counter.
    mailbox_dropped_slots: Vec<Arc<AtomicU64>>,
    budget_factor: Arc<AtomicU32>,
    worker_budget_bytes: u64,
}

/// Snapshot of per-core worker metrics for the `/metrics` handler.
pub struct PercoreMetricsSnapshot {
    /// Per-worker inflight bytes (one entry per worker, in order).
    pub inflight: Vec<u64>,
    /// Per-worker broadcast drop count (cumulative counter, one per worker).
    pub dropped: Vec<u64>,
    /// Per-worker cumulative accepted-connections count (B1).
    pub accepted: Vec<u64>,
    /// Per-worker cumulative CoDel-dropped-frames count (B1).
    pub codel_dropped: Vec<u64>,
    /// Task 4: per-worker cumulative mailbox-full-drop count.
    pub mailbox_dropped: Vec<u64>,
    /// Sum of all workers' inflight bytes.
    pub inflight_total: u64,
    /// Budget factor as a fraction (×1000 fixed-point → 0.0–1.0).
    pub budget_factor: f64,
    /// Per-worker memory budget in bytes.
    pub worker_budget_bytes: u64,
}

/// Snapshot the current per-core worker metrics. Returns `None` if no percore
/// fleet has been started (e.g. REST-only test environments).
pub fn percore_metrics_snapshot() -> Option<PercoreMetricsSnapshot> {
    use std::sync::atomic::Ordering;
    // Recover from a poisoned lock instead of panicking: the `/metrics` handler must
    // never fail, and the registry is only ever held for trivial atomic reads.
    let guard = PERCORE_REGISTRY
        .get()?
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let inflight: Vec<u64> = guard
        .inflight_slots
        .iter()
        .map(|s| s.load(Ordering::Relaxed))
        .collect();
    let dropped: Vec<u64> = guard
        .worker_slots
        .iter()
        .map(|s| s.dropped.load(Ordering::Relaxed))
        .collect();
    let accepted: Vec<u64> = guard
        .accepted_slots
        .iter()
        .map(|s| s.load(Ordering::Relaxed))
        .collect();
    let codel_dropped: Vec<u64> = guard
        .codel_dropped_slots
        .iter()
        .map(|s| s.load(Ordering::Relaxed))
        .collect();
    let mailbox_dropped: Vec<u64> = guard
        .mailbox_dropped_slots
        .iter()
        .map(|s| s.load(Ordering::Relaxed))
        .collect();
    let inflight_total = inflight.iter().sum();
    let budget_factor = guard.budget_factor.load(Ordering::Relaxed) as f64 / 1000.0;
    let worker_budget_bytes = guard.worker_budget_bytes;
    Some(PercoreMetricsSnapshot {
        inflight,
        dropped,
        accepted,
        codel_dropped,
        mailbox_dropped,
        inflight_total,
        budget_factor,
        worker_budget_bytes,
    })
}

/// Test hook (SP10): total bytes queued across ALL per-core workers. Returns 0
/// before any percore server has installed its slots.
#[cfg(any(test, feature = "test-hooks"))]
pub fn percore_total_inflight_bytes() -> u64 {
    percore_metrics_snapshot()
        .map(|s| s.inflight_total)
        .unwrap_or(0)
}

/// Run the per-core transport as the actual server.
///
/// Takes the already-built shared pieces (the same ones `main`/`AppState`
/// assemble), builds ONE shared [`DispatchEnv`], then spawns
/// `config.worker_count()` worker threads — each pinned to a CPU and each with
/// its OWN `SO_REUSEPORT` listener on `config.bind:config.port`. The kernel
/// load-balances incoming connections across the workers' accept queues, so
/// fan-out parallelizes across cores. The `Arc`'d adapter/env (the `LocalAdapter`
/// registry is `DashMap`-concurrent) and a clone of the REST handoff `Sender`
/// are shared by all workers; cross-worker delivery already works because each
/// per-conn mailbox is `Send + Sync` and every worker drains its own
/// connections. Blocks until `shutdown` is observed by all workers (or a fatal
/// bind/poll error occurs), joining every worker thread before returning.
///
/// REST handling (SP9 §3.4): a worker no longer closes non-WS connections. On a
/// `HeadResult::Rest` head it transfers the accepted fd (plus the head bytes
/// already read) over `rest_handoff` to the tokio/axum REST plane spawned by the
/// caller via [`rest::serve`]. WS connections + the full v7 protocol run on the
/// worker threads. `rest_handoff` is `None` only in tests that exercise a worker
/// without a REST plane.
#[allow(clippy::too_many_arguments)]
pub fn run_percore(
    config: ServerConfig,
    apps: Arc<dyn AppManager>,
    adapter: Arc<dyn Adapter>,
    conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
    // Per-app connection index shared with `LocalAdapter::purge_app`. Created once
    // in `main.rs` (like `conn_counts`) and stamped into the `DispatchEnv`.
    app_registry: Arc<AppRegistry>,
    // Node-level live connection counter, shared across all of this node's workers
    // for the connection-ceiling check. Created once in `main.rs` alongside
    // `conn_counts` and injected here so tests can supply an independent counter.
    node_conns: Arc<AtomicUsize>,
    webhooks: WebhookHandle,
    rest_handoff: Option<UnboundedSender<RestConn>>,
    shutdown: Arc<AtomicBool>,
    // The CONCRETE local adapter (the same instance wrapped in `adapter` above)
    // when the per-core sharded fan-out applies. `Some` ⇒ install the broadcast
    // sink and give each worker a broadcast inbox so channel deliveries shard
    // across workers; `None` (e.g. the deferred redis+percore combo) ⇒ no sink,
    // broadcasts fall back to the legacy registry mailbox path.
    local: Option<Arc<LocalAdapter>>,
    // SP11 §3.6: clustering toggle for this node. `true` ⇒ a clustered percore
    // node whose workers defer the single-emit cluster edges to the bridge (the
    // connection handler suppresses its node-local `subscription_count` /
    // `channel_occupied` / `channel_vacated`); `false` ⇒ the standalone percore
    // node keeps the node-local handler emits. Stamped onto every connection's
    // `ConnectionContext` via the shared `DispatchEnv`.
    clustered: bool,
    // Optional rustls TLS server config. `Some` ⇒ every accepted TCP connection
    // is wrapped with a TLS server-side handshake before the WS upgrade;
    // `None` ⇒ plain TCP (no TLS).
    tls: Option<Arc<rustls::ServerConfig>>,
    // Phase 7: tokio runtime handle for offloading L1-miss `by_key` lookups.
    // The caller MUST capture `tokio::runtime::Handle::current()` BEFORE spawning
    // the worker thread (or `spawn_blocking` body), where the runtime context is
    // present. `run_percore` itself may run on a plain `std::thread` where
    // `Handle::try_current()` returns `Err`.
    runtime: tokio::runtime::Handle,
) -> std::io::Result<()> {
    let addr: std::net::SocketAddr = format!("{}:{}", config.bind, config.port)
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    // SP10: the shared saturation flag the WS client-event ingress drop reads. It
    // lives on the `LocalAdapter` (so the REST `AppState` and the sink share the
    // SAME bit); `None` when there's no concrete local adapter (redis+percore
    // fallback), so the WS drop never fires there.
    let saturated_flag = local.as_ref().map(|l| l.saturation_flag());

    // SP10 self-sizing: worker count (explicit or `available_parallelism`), the
    // total memory budget (explicit/fraction override or the `max(1.5 GiB, 7%)`
    // reserve formula over the effective — cgroup-aware — envelope), each
    // worker's budget slice, and the per-connection out-queue cap clamped to the
    // configured [min, max] window. `per_conn_cap` becomes each `Connection`'s
    // `high_water`, so a slow consumer's drop-head queue is sized to the host.
    // Computed before `env` so `resolved_max_connections` can consume the budget.
    let worker_count = config.worker_count();
    let effective_mem = resources::detect_effective_mem();
    let budget = config.resolved_memory_budget(effective_mem);

    let env = Arc::new(DispatchEnv {
        apps,
        adapter,
        limits: config.limits(),
        activity_timeout: config.activity_timeout,
        pong_timeout: config.pong_timeout,
        strict_protocol: config.strict_protocol,
        conn_counts,
        node_conns,
        app_registry,
        webhooks,
        saturated: saturated_flag,
        clustered,
        // Node ceiling: explicit override or auto-derived from memory budget.
        // `0` = unlimited (no ceiling check).
        max_connections: config.resolved_max_connections(budget),
        // Task 4: bounded mailbox capacity from config (default 256).
        mailbox_capacity: config.mailbox_capacity,
        runtime,
    });
    // WS frame cap: bound a single inbound frame's payload. The configured
    // event-payload limit is small (KiB), so use a 1 MiB frame ceiling that
    // comfortably covers any legitimate Pusher frame while bounding abuse.
    let max_payload = config.max_event_payload_bytes.max(1 << 20);

    let per_worker_budget = budget / worker_count.max(1) as u64;
    let per_conn_cap = resources::per_conn_cap(per_worker_budget, config.expected_conns_per_worker)
        .clamp(
            config.perconn_queue_min_bytes,
            config.perconn_queue_max_bytes,
        );
    let high_water = per_conn_cap as usize;

    // CPU ids to pin to. May be empty if the OS won't report them — workers then
    // run unpinned (still fully functional, just not affinity-bound).
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();

    tracing::info!(
        %addr,
        workers = worker_count,
        budget_mib = budget >> 20,
        per_conn_cap_kib = per_conn_cap >> 10,
        "pylon percore: {worker_count} workers, budget {} MiB, per-conn cap {} KiB",
        budget >> 20,
        per_conn_cap >> 10,
    );

    // Per-worker inflight-byte counters (one shared `AtomicU64` per worker). Each
    // worker mirrors its local `inflight_bytes` into its slot every loop; the
    // off-hot-path `percore_total_inflight_bytes()` test hook sums them.
    let inflight_slots: Vec<Arc<AtomicU64>> = (0..worker_count)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    // B1: per-worker accepted-connections and CoDel-dropped-frames counters.
    let accepted_slots: Vec<Arc<AtomicU64>> = (0..worker_count)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();
    let codel_dropped_slots: Vec<Arc<AtomicU64>> = (0..worker_count)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();
    // Task 4: per-worker mailbox-full-drop counters. Each worker's counter Arc is
    // cloned into every `Mailbox` created on that worker, so the counter tracks
    // all mailbox drops attributable to that worker.
    let mailbox_dropped_slots: Vec<Arc<AtomicU64>> = (0..worker_count)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    // Build the per-core sharded broadcast plumbing: one `(Sender, Receiver)`
    // pair + `WorkerSlot` per worker. The `Sender`s live in the sink (installed
    // on the concrete adapter BEFORE any worker spawns, so the first broadcast
    // already routes through it); each `Receiver` + slot is handed to its worker,
    // which fills the slot's `Waker` `OnceLock` at startup. When `local` is
    // `None` (no concrete adapter to install on), broadcasts use the legacy
    // mailbox path and workers get no inbox.
    // Bounded broadcast hand-off capacity (frames) per worker (SP10): the
    // publish→workers channel is bounded so a publish flood that outruns delivery
    // is dropped at the hand-off rather than buffered unbounded (the SP9 hang).
    let handoff_cap = config.broadcast_handoff_cap;

    // Worker slot arcs retained for the metrics registry (to read `.dropped`).
    let mut worker_slots_for_metrics: Vec<Arc<WorkerSlot>> = Vec::new();

    let mut wirings: Vec<Option<BroadcastWiring>> = Vec::with_capacity(worker_count);
    if let Some(local) = &local {
        // One shared `Arc<WorkerSlot>` per worker: the sink and the worker both
        // hold it, so the `Waker` the worker publishes into the slot at startup
        // is immediately visible to the publisher.
        let mut slots: Vec<Arc<WorkerSlot>> = Vec::with_capacity(worker_count);
        let mut receivers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            // Bounded hand-off: `sync_channel(cap)` → publisher `try_send` drops
            // on full instead of growing memory without limit.
            let (tx, rx) = std::sync::mpsc::sync_channel(handoff_cap);
            slots.push(Arc::new(WorkerSlot {
                tx,
                waker: std::sync::OnceLock::new(),
                dropped: AtomicU64::new(0),
            }));
            receivers.push(rx);
        }
        // Retain slot arcs for the metrics registry before moving them into wirings.
        worker_slots_for_metrics.extend(slots.iter().cloned());
        // Sink-shared saturation flag: set by the publisher on a full hand-off
        // OR by a worker that hit ≥100% of its byte budget, cleared by each
        // worker after it drains its inbox to empty. Sourced from the
        // `LocalAdapter` so the REST `AppState`'s 503 admission check (which holds
        // a clone via `saturation_flag()`) observes the SAME bit.
        let saturated = local.saturation_flag();
        let sink = BroadcastSink {
            workers: Arc::new(slots.clone()),
            saturated: saturated.clone(),
        };
        // Install BEFORE spawning workers so the very first broadcast routes here.
        local.set_broadcast_sink(sink);
        for (slot, rx) in slots.into_iter().zip(receivers) {
            wirings.push(Some(BroadcastWiring {
                rx,
                slot,
                saturated: saturated.clone(),
            }));
        }
    } else {
        for _ in 0..worker_count {
            wirings.push(None);
        }
    }

    // SP10 §7: CoDel freshness parameters, shared by every connection on every
    // worker (config-derived; `target_ms == 0` disables → pure drop-head).
    let codel = config.codel_params();

    // SP10 §8: PSI memory-pressure backstop. A single control-plane task (NOT a
    // worker) polls the kernel pressure file ~1 Hz and shrinks a shared
    // `budget_factor` (×1000 fixed-point, 1000 = full) when the machine is
    // genuinely thrashing; the workers read this factor when sizing their shed
    // budget — the hot path NEVER reads PSI inline. Enabled when the config gate
    // is on (default: on iff the pressure file is readable at startup) AND a tokio
    // runtime handle is available to host the task; otherwise the factor stays
    // pinned at 1000 (a no-op backstop).
    let budget_factor = Arc::new(AtomicU32::new(1000));
    let psi_path = resources::psi_pressure_path();
    let psi_enabled = config.psi_backstop.unwrap_or(psi_path.is_some());
    if psi_enabled {
        if let (Some(path), Ok(handle)) = (psi_path, tokio::runtime::Handle::try_current()) {
            handle.spawn(psi_backstop_loop(
                path,
                config.psi_threshold,
                budget_factor.clone(),
                shutdown.clone(),
            ));
            tracing::info!(
                psi_path = path,
                threshold = config.psi_threshold,
                "pylon percore: PSI memory-pressure backstop enabled"
            );
        } else {
            tracing::debug!("PSI backstop requested but unavailable; budget factor pinned");
        }
    }

    // Fill the global percore metrics registry. Overwrites any prior entry (safe:
    // `get_or_init` initialises the Mutex once; subsequent runs replace the inner
    // value so re-runs in tests see fresh slots).
    {
        let mut g = PERCORE_REGISTRY
            .get_or_init(|| {
                std::sync::Mutex::new(PercoreRegistry {
                    inflight_slots: Vec::new(),
                    worker_slots: Vec::new(),
                    accepted_slots: Vec::new(),
                    codel_dropped_slots: Vec::new(),
                    mailbox_dropped_slots: Vec::new(),
                    budget_factor: budget_factor.clone(),
                    worker_budget_bytes: per_worker_budget,
                })
            })
            .lock()
            .unwrap();
        g.inflight_slots.clear();
        g.inflight_slots.extend(inflight_slots.iter().cloned());
        g.worker_slots.clear();
        g.worker_slots
            .extend(worker_slots_for_metrics.iter().cloned());
        g.accepted_slots.clear();
        g.accepted_slots.extend(accepted_slots.iter().cloned());
        g.codel_dropped_slots.clear();
        g.codel_dropped_slots
            .extend(codel_dropped_slots.iter().cloned());
        g.mailbox_dropped_slots.clear();
        g.mailbox_dropped_slots
            .extend(mailbox_dropped_slots.iter().cloned());
        g.budget_factor = budget_factor.clone();
        g.worker_budget_bytes = per_worker_budget;
    }

    let mut handles = Vec::with_capacity(worker_count);
    for (i, wiring) in wirings.into_iter().enumerate() {
        let cfg = WorkerConfig {
            addr,
            max_payload,
            high_water,
            mode: Mode::Dispatch(env.clone()),
            rest_handoff: rest_handoff.clone(),
            worker_id: i,
            broadcast: wiring,
            per_worker_budget,
            inflight_slot: Some(inflight_slots[i].clone()),
            accepted_slot: Some(accepted_slots[i].clone()),
            codel_dropped_slot: Some(codel_dropped_slots[i].clone()),
            mailbox_dropped_slot: Some(mailbox_dropped_slots[i].clone()),
            codel,
            budget_factor: Some(budget_factor.clone()),
            shutdown_grace_ms: config.shutdown_grace_ms,
            tls: tls.clone(),
        };
        let shutdown = shutdown.clone();
        let core = core_ids.get(i % core_ids.len().max(1)).copied();
        let handle = std::thread::Builder::new()
            .name(format!("pylon-worker-{i}"))
            .spawn(move || {
                if let Some(core) = core {
                    if core_affinity::set_for_current(core) {
                        tracing::debug!(worker = i, core = ?core, "pinned percore worker to core");
                    } else {
                        tracing::debug!(worker = i, core = ?core, "core pinning unsupported; running unpinned");
                    }
                }
                worker::run(cfg, shutdown)
            })?;
        handles.push(handle);
    }

    // Join all workers. The first fatal worker error is propagated; remaining
    // workers are still joined so we don't leak threads on shutdown.
    let mut first_err = None;
    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(_) => {
                if first_err.is_none() {
                    first_err = Some(std::io::Error::other("percore worker thread panicked"));
                }
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// SP10 §8 PSI memory-pressure backstop control loop.
///
/// Polls `path` (a kernel PSI pressure file) once a second and adjusts the shared
/// `budget_factor` (×1000 fixed-point; `1000` = full per-worker budget). When the
/// `full avg10` pressure exceeds `threshold` the machine is genuinely thrashing —
/// our byte estimate was too optimistic for this host — so the factor is
/// multiplied down toward a `0.8×` floor (`800`); when pressure clears (drops
/// below `threshold / 2`) the factor ramps back up toward `1000`. The workers
/// read the factor (relaxed) when sizing their shed budget; this loop is the only
/// place PSI is ever read, keeping it entirely off the hot path. Exits when
/// `shutdown` is set.
///
/// `compute_factor` (below) is the pure step function so the policy is unit-tested
/// without touching `/proc`.
async fn psi_backstop_loop(
    path: &'static str,
    threshold: f64,
    budget_factor: Arc<AtomicU32>,
    shutdown: Arc<AtomicBool>,
) {
    use std::sync::atomic::Ordering;
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tick.tick().await;
        if shutdown.load(Ordering::SeqCst) {
            return;
        }
        // Read the pressure file off the hot path; a transient read failure leaves
        // the factor unchanged (treated as "no new pressure signal").
        let pressure = std::fs::read_to_string(path)
            .ok()
            .and_then(|s| resources::psi_full_avg10(&s));
        if let Some(p) = pressure {
            let cur = budget_factor.load(Ordering::Relaxed);
            let next = compute_factor(cur, p, threshold);
            if next != cur {
                budget_factor.store(next, Ordering::Relaxed);
                tracing::debug!(
                    full_avg10 = p,
                    factor = next,
                    "PSI backstop adjusted budget factor"
                );
            }
        }
    }
}

/// Pure step for the PSI budget factor (×1000 fixed-point). Above `threshold`,
/// multiply down toward the `0.8×` (800) floor; below `threshold / 2`, ramp back
/// up toward full (1000); in the hysteresis band, hold. Each step moves ~10%.
fn compute_factor(current: u32, full_avg10: f64, threshold: f64) -> u32 {
    const FLOOR: u32 = 800; // 0.8×
    const CEIL: u32 = 1000; // 1.0×
    if full_avg10 > threshold {
        // Thrashing: shrink toward the floor (factor * 9/10, clamped).
        (current * 9 / 10).max(FLOOR)
    } else if full_avg10 < threshold / 2.0 {
        // Recovered: grow back toward full (+~10% of full, clamped).
        (current + CEIL / 10).min(CEIL)
    } else {
        // Hysteresis band: hold steady to avoid oscillation.
        current.clamp(FLOOR, CEIL)
    }
}

#[cfg(test)]
mod psi_tests {
    use super::compute_factor;

    #[test]
    fn factor_shrinks_under_pressure_and_recovers() {
        let threshold = 15.0;
        // Under heavy pressure the factor steps down toward the 0.8 floor.
        let f1 = compute_factor(1000, 40.0, threshold);
        assert!(
            (800..1000).contains(&f1),
            "stepped down but not below floor: {f1}"
        );
        // Repeated pressure keeps shrinking but never past 800.
        let mut f = 1000;
        for _ in 0..20 {
            f = compute_factor(f, 40.0, threshold);
        }
        assert_eq!(f, 800, "clamps at the 0.8x floor");
        // In the hysteresis band (between threshold/2 and threshold) it holds.
        assert_eq!(compute_factor(900, 10.0, threshold), 900);
        // Once pressure clears (< threshold/2) it ramps back toward full.
        let up = compute_factor(800, 1.0, threshold);
        assert!(up > 800 && up <= 1000, "ramped up: {up}");
        let mut g = 800;
        for _ in 0..20 {
            g = compute_factor(g, 0.0, threshold);
        }
        assert_eq!(g, 1000, "recovers to full budget");
    }
}
