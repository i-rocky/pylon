//! SHIPPING fan-out path benchmark (percore `BroadcastSink` → worker drain) —
//! the F18/X4 audit gap: every prior bench measured either the LEGACY registry
//! path (`benches/fanout.rs`) or a micro-slice; nothing drove the production
//! loop end to end.
//!
//! What is measured, per scenario, is exactly the production sequence:
//!
//! 1. **publish half** — the same statements `LocalAdapter::broadcast` runs
//!    when the sink is installed: encode the v7 JSON once (`Raw` events skip
//!    this — one `Arc<str>` clone), WS-frame it once (`frame::encode_text` +
//!    `freeze`), hand the shared `Bytes` to every worker via
//!    `BroadcastSink::broadcast` (one bounded `try_send` per worker slot);
//! 2. **drain half** — `drain_broadcasts`' real inbox pump (factored into
//!    `pylon::transport::worker::drain_broadcast_inbox` for exactly this
//!    bench): the `(app, channel) → local_subs` lookup, then per subscriber
//!    the shed-band reclassify, sender exclusion, `sid_to_token` resolve, the
//!    graduated skip, and the `Connection::queue(frame.clone(), ..)`
//!    refcount-bump enqueue with live inflight/drop-head accounting.
//!
//! No drain logic is reimplemented here: the bench calls the production
//! function through the production `ConnIndex` shape (its impl adds the same
//! `ConnState::Open` check the worker's impl performs).
//!
//! Scenarios:
//!
//! * `typed/{n}` — a fat `ChannelEvent` (real JSON encode per broadcast).
//! * `raw/{n}` — `ServerEvent::Raw(Arc<str>)` carrying the SAME event's encoded
//!   JSON (what the redis relay re-frames), so the Raw no-copy path (encode
//!   skipped, one shared buffer) is actually measured.
//! * scales 1k / 10k / 100k subscribers on one channel; `Throughput::Elements`
//!   reports subscriber-enqueues/s.
//!
//! ## Mailbox strategy (documented choice)
//!
//! Production per-connection out-queues are byte-bounded drop-head, so the
//! drain never blocks — but a healthy worker FLUSHES between drains, and the
//! steady state the fan-out path actually experiences is a near-empty queue.
//! This bench keeps that state honestly WITHOUT timing socket I/O (the flush's
//! writev is the F4 transport, covered by the percore suites, not F18): after
//! each timed iteration every connection is reset to a fresh `Connection` over
//! the SAME socket (slab `remove` → `into_stream` → `insert_at`; no fd
//! duplication, token-stable), which drops the queued frame refcounts exactly
//! as a fully-caught-up flush would leave things. The reset is OUTSIDE the
//! timed window (`iter_custom`). `inflight_bytes` is zeroed alongside (all
//! queues empty), and the per-worker byte budget stays in the `Normal` band —
//! the shed/skip paths are covered by the percore overload suites.
//!
//! ## Harness notes
//!
//! * One worker slot (this bench is one worker's view of one channel). The
//!   W-box of the fan-out is the sink's per-worker `try_send`, not the
//!   per-subscriber cost — multi-worker scaling is exercised by
//!   `percore_multiworker`.
//! * `Connection` requires a real `mio::net::TcpStream`, so setup
//!   connect/accepts `n` loopback sockets and drops the client ends with
//!   `SO_LINGER=0` (RST) — otherwise macOS pins each client's ephemeral port in
//!   FIN_WAIT_2 for as long as the accepted side lives (~16k-port ceiling, the
//!   100k scale would exhaust the pool). The accepted ends are never written
//!   to; the RST is inert for a queue-only workload.
//! * The concurrent-churn case (below) is the committed replacement for the
//!   6.5 throwaway contention probe.

use std::collections::{HashMap, HashSet};
use std::io::Write as _;
use std::net::TcpListener;
use std::os::fd::OwnedFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mio::net::TcpStream as MioTcpStream;
use slab::Slab;

use pylon::channel::registry::Registry;
use pylon::connection::handle::{ConnectionHandle, Mailbox};
use pylon::protocol::event::ServerEvent;
use pylon::protocol::socket_id::SocketId;
use pylon::protocol::v7::frames;
use pylon::transport::conn::{ConnState, Connection};
use pylon::transport::fanout::{
    BroadcastMsg, BroadcastSink, WorkerSlot, DEFAULT_BROADCAST_HANDOFF_CAP,
};
use pylon::transport::frame;
use pylon::transport::worker::{drain_broadcast_inbox, ConnIndex};

/// Subscriber scales mandated by the plan.
const SCALES: [usize; 3] = [1_000, 10_000, 100_000];

/// Per-connection out-queue cap: the floor of the production formula
/// `per_conn_cap` (`clamp(per_worker_budget / expected_conns, 256 KiB, 8 MiB)`)
/// — never reached here because queues are reset between iterations.
const HIGH_WATER: usize = 256 << 10;

/// Effective per-worker byte budget: a mid-box slice (`memory_budget` split
/// across 8 workers). Only its Normal band is exercised (queues are caught up
/// every iteration); 0 would disable enforcement, a real value keeps the
/// per-subscriber `shed_band` reclassification in the measured path.
const EFFECTIVE_BUDGET: u64 = 64 << 20;

/// The bench's connection table: slab-keyed `Connection`s, all `Open` (the
/// drain only ever delivers to Open dispatch connections).
struct BenchConns {
    slab: Slab<Connection>,
}

impl ConnIndex for BenchConns {
    fn open_conn(&mut self, token: usize) -> Option<&mut Connection> {
        let conn = self.slab.get_mut(token)?;
        (conn.state == ConnState::Open).then_some(conn)
    }
}

/// One worker's world: the sink (one slot), its inbox receiver, and the
/// worker-local indexes the drain walks — `local_subs` in the exact
/// single-map `(app, channel) → {SocketId}` shape the worker keeps, plus the
/// `socket_id → slab token` reverse map.
struct SinkWorld {
    conns: BenchConns,
    /// The rotation spare (see `reset_queues`): one extra never-subscribed
    /// socket that lets every connection be swapped for a fresh one over its
    /// own socket without fd duplication. `Option` so the reset can `take()`
    /// an owned socket out through `&mut` (the rotation threads it by value).
    spare: Option<MioTcpStream>,
    rx: std::sync::mpsc::Receiver<BroadcastMsg>,
    sink: BroadcastSink,
    local_subs: HashMap<(Arc<str>, Arc<str>), HashSet<SocketId>>,
    sid_to_token: HashMap<SocketId, usize>,
    app: Arc<str>,
    channel: Arc<str>,
}

/// One accepted, never-written loopback socket: connect with `SO_LINGER=0`
/// on the client (its drop sends RST, freeing the ephemeral port immediately —
/// a plain FIN close parks the port in FIN_WAIT_2 for the accepted side's
/// lifetime and caps the process at ~16k live sockets on macOS), accept,
/// drop the client, wrap for mio.
fn accepted_stream(listener: &TcpListener) -> MioTcpStream {
    let addr = listener.local_addr().expect("listener addr");
    let client = std::net::TcpStream::connect(addr).expect("loopback connect");
    socket2::SockRef::from(&client)
        .set_linger(Some(Duration::ZERO))
        .expect("SO_LINGER=0");
    let (accepted, _) = listener.accept().expect("accept");
    drop(client); // RST, not FIN: see the function doc
    MioTcpStream::from(OwnedFd::from(accepted))
}

/// Build `n` real `Connection`s over real (never-written) loopback sockets,
/// all subscribed to one channel on this worker. The client ends are closed
/// RST (`SO_LINGER=0`) so their ephemeral ports free immediately — a plain
/// close would park each port in FIN_WAIT_2 for the lifetime of the accepted
/// side and cap the process at ~16k live sockets on macOS.
fn build_world(n: usize) -> SinkWorld {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");

    // Setup ends with one spare socket (its subscriber never joins local_subs)
    // so the reset below has a spare to rotate through.
    let mut slab: Slab<Connection> = Slab::with_capacity(n);
    let mut sid_to_token: HashMap<SocketId, usize> = HashMap::with_capacity(n);
    let mut subs: HashSet<SocketId> = HashSet::with_capacity(n);

    for _ in 0..n {
        let mut conn = Connection::new(accepted_stream(&listener), HIGH_WATER);
        conn.state = ConnState::Open; // bench conns model established sessions
        let token = slab.insert(conn);
        let sid = SocketId::generate();
        sid_to_token.insert(sid, token);
        subs.insert(sid);
    }
    let spare_sock = accepted_stream(&listener);

    let app: Arc<str> = Arc::from("app");
    let channel: Arc<str> = Arc::from("presence-room-42");
    let mut local_subs = HashMap::with_capacity(1);
    local_subs.insert((app.clone(), channel.clone()), subs);

    // One worker slot over the real bounded hand-off, exactly as `run_percore`
    // wires it. The Waker stays unset: an awake worker needs no wake nudge.
    let (tx, rx) = std::sync::mpsc::sync_channel(DEFAULT_BROADCAST_HANDOFF_CAP);
    let slot = WorkerSlot {
        tx,
        waker: std::sync::OnceLock::new(),
        dropped: AtomicU64::new(0),
    };
    let sink = BroadcastSink {
        workers: Arc::new(vec![Arc::new(slot)]),
        saturated: Arc::new(AtomicBool::new(false)),
    };

    SinkWorld {
        conns: BenchConns { slab },
        spare: Some(spare_sock),
        rx,
        sink,
        local_subs,
        sid_to_token,
        app,
        channel,
    }
}

/// The fat typed event (same shape as `benches/mailbox.rs`'s): a realistic
/// client-event broadcast whose encode is real work per broadcast.
fn fat_event() -> ServerEvent {
    ServerEvent::ChannelEvent {
        channel: "presence-room-42".to_string(),
        event: "client-message".to_string(),
        data: serde_json::json!({"msg": "hello world payload body"}),
        user_id: None,
    }
}

/// Reset every connection to a fresh `Connection` over the SAME socket
/// (token-stable, no fd duplication, no syscalls): one spare socket rotates
/// through the table — parked inside a connection while its real socket is
/// taken out to build the replacement, then recovered from the dirty
/// connection it was parked in. Dropping each dirty connection releases its
/// queued frame refcounts and accounting, leaving exactly the state a
/// fully-caught-up flush leaves — a near-empty out-queue for the next timed
/// iteration.
fn reset_queues(world: &mut SinkWorld) {
    let slab = &mut world.conns.slab;
    let mut spare = world.spare.take().expect("rotation spare present");
    for token in 0..slab.len() {
        let slot = slab.get_mut(token).expect("dense slab keys");
        // Park the spare inside the connection, taking its real socket out.
        let real = std::mem::replace(slot.stream_mut(), spare);
        // Fresh connection over the real socket; the dirty one (holding the
        // parked spare) comes back out.
        let dirty = std::mem::replace(slot, Connection::new(real, HIGH_WATER));
        slot.state = ConnState::Open;
        // Recover the parked spare for the next slot.
        spare = dirty.into_stream();
    }
    world.spare = Some(spare);
}

/// Encode + hand off + drain: the timed window. Statement-for-statement the
/// production publish half (`LocalAdapter::broadcast` with the sink installed)
/// followed by the production inbox pump.
fn broadcast_and_drain(world: &mut SinkWorld, event: &ServerEvent, now_ns: u64) {
    let json: Arc<str> = match event {
        ServerEvent::Raw(f) => f.clone(),
        other => Arc::from(frames::encode(other).as_str()),
    };
    let mut buf = BytesMut::new();
    frame::encode_text(&mut buf, json.as_bytes());
    world
        .sink
        .broadcast(world.app.clone(), world.channel.clone(), buf.freeze(), None);

    let mut touched = HashSet::new();
    let mut inflight: u64 = 0;
    let mut drophead: u64 = 0;
    drain_broadcast_inbox(
        &world.rx,
        &world.local_subs,
        &world.sid_to_token,
        &mut world.conns,
        EFFECTIVE_BUDGET,
        &mut inflight,
        &mut drophead,
        None,
        now_ns,
        &mut touched,
        &HashSet::new(),
    );
    // Self-check (O(1), not a per-subscriber cost): no `except`, every conn
    // Open, band Normal ⇒ every subscriber was queued onto this iteration —
    // each distinct SocketId owns a distinct token, so `touched` must cover
    // the whole reverse map. A silent skip-path regression would otherwise
    // make the bench "faster" while delivering nothing.
    assert_eq!(
        touched.len(),
        world.sid_to_token.len(),
        "every subscriber must be delivered"
    );
    black_box(drophead);
}

/// The sink-path scenarios: typed and `Raw` events at 1k/10k/100k subscribers.
/// The timed window is one broadcast + one full inbox drain; the queue reset
/// runs untimed between iterations (see the module doc's mailbox strategy).
fn bench_sink(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout_sink");
    for n in SCALES {
        // The 100k scale's timed iterations are ~ms and its untimed resets
        // ~10ms; fewer, longer samples keep the full run quick without
        // starving the estimate.
        if n == 100_000 {
            group.sample_size(10);
            group.measurement_time(Duration::from_secs(3));
        } else {
            group.sample_size(20);
            group.measurement_time(Duration::from_secs(2));
        }
        group.warm_up_time(Duration::from_millis(300));
        group.throughput(Throughput::Elements(n as u64));

        let mut world = build_world(n);
        for (case, event) in [("typed", fat_event()), ("raw", raw_event())] {
            group.bench_with_input(BenchmarkId::new(case, n), &n, |b, _| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    let epoch = Instant::now();
                    for _ in 0..iters {
                        // Invariant: every out-queue is empty (reset below),
                        // inflight == 0 — the caught-up steady state.
                        let start = Instant::now();
                        broadcast_and_drain(&mut world, &event, elapsed_ns(epoch));
                        total += start.elapsed();
                        // Untimed: model the post-drain flush having fully
                        // caught up before the next broadcast arrives.
                        reset_queues(&mut world);
                    }
                    total
                });
            });
        }
    }
    group.finish();
}

/// `ServerEvent::Raw` carrying the fat event's encoded JSON — byte-for-byte
/// what a redis-relayed broadcast re-frames, so this case measures the Raw
/// no-copy path (no JSON encode; one shared `Arc<str>` → one shared `Bytes`).
fn raw_event() -> ServerEvent {
    ServerEvent::Raw(Arc::from(frames::encode(&fat_event()).as_str()))
}

/// Monotonic ns since the bench epoch, the same stamp the worker's loop
/// passes the drain (CoDel is disabled on fresh connections, so the stamp is
/// bookkeeping only).
fn elapsed_ns(epoch: Instant) -> u64 {
    epoch.elapsed().as_nanos() as u64
}

/// Concurrent-churn benchmark — the committed replacement for the 6.5
/// throwaway probe (its reference shape: n=1000, drained mailboxes, concurrent
/// churn). A storm thread hammers `Registry::broadcast` on the 1000-subscriber
/// channel while drainer threads keep every mailbox empty (real enqueue cost,
/// not drop-on-full — the degenerate probe variant that hid the F7 pathology),
/// and the MEASURED routine is the churn side: full subscribe/unsubscribe
/// cycles across 64 other registry channels.
///
/// This is the number the F7 fix moved 115 → ~1.8M ops/s: if the fan-out
/// snapshot is ever rebuilt under the shard read guard (or the guard is held
/// across the send loop again), write-starved churn collapses by orders of
/// magnitude and this bench shows it. The sink path itself has no cross-thread
/// registry reads per broadcast — that decoupling IS the architecture this
/// pins from the legacy side. The storm's broadcast rate is printed after the
/// run for context (it is load, not the measured quantity).
const CHURN_SUBS: usize = 1_000;
const CHURN_CHANNELS: usize = 64;
const CHURN_CYCLES: u64 = 256;

fn bench_registry_churn(c: &mut Criterion) {
    let reg = Arc::new(Registry::new());
    let event = fat_event();

    // n=1000 broadcast subscribers with real mailboxes; receivers split
    // between two drainer threads (mpsc Receivers are !Clone — each thread
    // owns its half outright).
    let mut drain_a = Vec::new();
    let mut drain_b = Vec::new();
    for i in 0..CHURN_SUBS {
        let (tx, rx) = tokio::sync::mpsc::channel::<Box<ServerEvent>>(1024);
        (if i % 2 == 0 {
            &mut drain_a
        } else {
            &mut drain_b
        })
        .push(rx);
        reg.subscribe(
            "app",
            "presence-room-42",
            ConnectionHandle {
                socket_id: SocketId::generate(),
                mailbox: Mailbox::new(tx, None, None),
            },
            None,
        );
    }

    // The churner: one connection rapidly subscribing/unsubscribing across 64
    // channels (the pattern production churn generates against the registry).
    let sid = SocketId::generate();
    let (tx, _rx) = tokio::sync::mpsc::channel::<Box<ServerEvent>>(16);
    let churn_handle = ConnectionHandle {
        socket_id: sid,
        mailbox: Mailbox::new(tx, None, None),
    };
    let channels: Vec<String> = (0..CHURN_CHANNELS)
        .map(|i| format!("churn-{i:03}"))
        .collect();

    let stop = Arc::new(AtomicBool::new(false));

    // Storm: registry broadcasts on the hot channel, as fast as possible.
    let storm_stop = stop.clone();
    let storm_reg = reg.clone();
    let storm = std::thread::spawn(move || {
        let started = Instant::now();
        let mut storms = 0u64;
        while !storm_stop.load(Ordering::Relaxed) {
            for _ in 0..64 {
                storm_reg.broadcast("app", "presence-room-42", &event, None);
            }
            storms += 64;
        }
        (storms, started.elapsed())
    });

    // Drainers: keep every mailbox empty so each storm broadcast pays a real
    // enqueue (a full mailbox would turn sends into cheap drop-on-full and
    // hide the contention the bench exists to expose).
    let drainer = |stop: Arc<AtomicBool>,
                   rxs: &mut Vec<tokio::sync::mpsc::Receiver<Box<ServerEvent>>>| {
        let mut drained = 0u64;
        while !stop.load(Ordering::Relaxed) {
            let mut got = 0;
            for rx in rxs.iter_mut() {
                while rx.try_recv().is_ok() {
                    got += 1;
                }
            }
            if got == 0 {
                std::thread::yield_now();
            }
            drained += got;
        }
        drained
    };
    let d_stop = stop.clone();
    let drainer_a = std::thread::spawn(move || drainer(d_stop, &mut drain_a));
    let d2_stop = stop.clone();
    let drainer_b = std::thread::spawn(move || drainer(d2_stop, &mut drain_b));

    let mut group = c.benchmark_group("registry_churn_under_broadcast");
    group.sample_size(50);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(CHURN_CYCLES));
    group.bench_function("churn_cycles", |b| {
        b.iter(|| {
            for i in 0..CHURN_CYCLES as usize {
                let ch = channels[i % CHURN_CHANNELS].as_str();
                reg.subscribe("app", ch, churn_handle.clone(), None);
                reg.unsubscribe("app", ch, &sid);
            }
        });
    });
    group.finish();

    stop.store(true, Ordering::Relaxed);
    let (storms, storm_elapsed) = storm.join().expect("storm thread");
    let drained = drainer_a.join().expect("drainer a") + drainer_b.join().expect("drainer b");
    eprintln!(
        "registry_churn_under_broadcast context: storm broadcast {:.0}/s, mailboxes drained {} total",
        storms as f64 / storm_elapsed.as_secs_f64(),
        drained
    );
    let _ = std::io::stderr().flush();
}

criterion_group!(benches, bench_sink, bench_registry_churn);
criterion_main!(benches);
