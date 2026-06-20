//! Single-worker `mio` event loop for the per-core transport (SP9).
//!
//! [`run`] binds a listener, then drives a non-blocking accept → handshake →
//! frame loop entirely on the calling thread with one [`mio::Poll`]. A
//! [`slab::Slab`] is the connection table: the slab key *is* the connection's
//! [`mio::Token`] value, so a readiness event maps to its [`Connection`] in
//! O(1). The listener uses a reserved token ([`LISTENER`]) that no slab key can
//! collide with.
//!
//! Readiness is managed edge-friendly: a connection is registered
//! `READABLE`-only and only gains `WRITABLE` interest when a [`flush`] returns
//! [`WriteStatus::WouldBlock`]; the interest is dropped back to `READABLE` once
//! the queue drains. This keeps the loop from spinning on a writable socket with
//! nothing to send.
//!
//! Two behaviours are supported:
//!
//! * [`Mode::Echo`] — every inbound data frame is re-encoded and queued straight
//!   back, pings are answered with pongs, a close tears the connection down.
//!   Used by the transport's own unit tests.
//! * [`Mode::Dispatch`] — the real Pusher v7 protocol. On handshake completion
//!   the worker resolves the `/app/{key}` tenant, builds a
//!   [`ConnectionContext`], emits
//!   `pusher:connection_established`, and from then on decodes each inbound Text
//!   frame to a [`ClientCommand`] and drives `ctx.dispatch(..)` via
//!   `block_on`. After every dispatch (and once per loop iteration) every Open
//!   connection's mailbox is drained: queued [`ServerEvent`]s are encoded and
//!   written, so broadcast fan-out reaches its subscribers. This REUSES all
//!   subscribe/presence/client-event/signin logic — it does not reimplement the
//!   protocol.
//!
//! `block_on` is safe here because the [`LocalAdapter`](crate::adapter::local)
//! async methods never await real I/O; they complete synchronously.
//!
//! Safe Rust — the crate root sets `#![deny(unsafe_code)]`; this module adds no
//! `unsafe`.
//!
//! Multiple of these worker loops run in the percore transport (one per CPU),
//! each with its own `SO_REUSEPORT` listener on the same `bind:port`, so the
//! kernel spreads accepts across workers; see [`crate::transport::run_percore`].

use crate::adapter::app_registry::AppRegistry;
use crate::adapter::Adapter;
use crate::app::{App, AppManager};
use crate::connection::handle::MailboxNotify;
use crate::protocol::command::ClientCommand;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use crate::protocol::{codec::Codec, negotiate};
use crate::transport::conn::{ConnError, ConnState, Connection, WriteStatus};
use crate::transport::frame::{self, OpCode};
use crate::transport::handshake::{self, HeadResult};
use crate::transport::timer::{Due, TimerWheel};
use crate::ws::handler::ConnectionContext;
use bytes::BytesMut;
use dashmap::DashMap;
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Reserved token for the listener. Slab keys grow from 0, so the maximum
/// `usize` is guaranteed never to collide with a connection token.
const LISTENER: Token = Token(usize::MAX);

/// Test-hooks instrumentation: a monotonic count of how many connection
/// mailboxes the SELECTIVE drain has visited across this process's workers.
/// Bumped once per session the selective drain touches. A test asserts this stays
/// tiny (≈ the number of ACTIVE connections) even with many idle connections,
/// proving idle connections are never scanned. Behind `test-hooks` so it is free
/// in release builds.
#[cfg(any(test, feature = "test-hooks"))]
pub static SELECTIVE_DRAIN_VISITS: AtomicU64 = AtomicU64::new(0);

/// Test-hooks accessor: the cumulative number of connection mailboxes the
/// Waker-driven selective drain has visited (see [`SELECTIVE_DRAIN_VISITS`]).
#[cfg(any(test, feature = "test-hooks"))]
pub fn percore_selective_drain_visits() -> u64 {
    SELECTIVE_DRAIN_VISITS.load(Ordering::Relaxed)
}

/// Reserved token for this worker's single [`mio::Waker`]. One below [`LISTENER`];
/// slab keys grow from 0 so neither reserved value can collide with a connection
/// token. mio allows exactly ONE active `Waker` per `Poll`, so this single waker
/// serves BOTH wake sources — the broadcast sink nudging a drain, and a
/// cross-connection [`Mailbox::send`](crate::connection::handle::Mailbox) marking a
/// connection dirty (cluster follow-up, `send_to_user`, `notify_watchers`, …). A
/// wake on this token only unblocks the poll; the post-loop broadcast + selective
/// mailbox drains then run and figure out what actually needs delivering.
const WORKER_WAKER: Token = Token(usize::MAX - 1);

/// Process-global live connection counter for the node-level ceiling check.
///
/// Incremented at accept (before the per-app capacity check); decremented either
/// immediately if the node ceiling rejects the connection (before returning
/// `Err(Reject)`) or in [`remove`] on every normal/timeout/drain close path.
/// This ensures the count is released exactly once per accepted connection.
static NODE_CONNS: AtomicUsize = AtomicUsize::new(0);

/// Shared, `Arc`-cloneable bundle of the `AppState` pieces a [`Mode::Dispatch`]
/// worker needs to build a [`ConnectionContext`] per connection.
pub struct DispatchEnv {
    pub apps: Arc<dyn AppManager>,
    pub adapter: Arc<dyn Adapter>,
    pub limits: crate::server::config::Limits,
    pub activity_timeout: u32,
    /// SP11 §4: seconds to wait for a `pusher:pong` after an idle `pusher:ping`
    /// before closing the connection with code `4201`. Drives this worker's
    /// [`TimerWheel`].
    pub pong_timeout: u32,
    pub strict_protocol: bool,
    /// Per-app live connection counters (shared with the rest of the server),
    /// mirroring `AppState::conn_counts`.
    pub conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
    pub webhooks: crate::webhook::WebhookHandle,
    /// SP10 admission control: the shared percore saturation flag. Stamped onto
    /// each connection's [`ConnectionContext`] at session establish so a WS
    /// `client-*` event is dropped at ingress under saturation. `None` when no
    /// sink is wired (e.g. the redis+percore fallback), so the drop never fires.
    pub saturated: Option<Arc<AtomicBool>>,
    /// SP11 §3.6: clustering toggle stamped onto every connection's
    /// [`ConnectionContext`] at session establish. `true` ⇒ this is a clustered
    /// percore node: the single-emit cluster edges (`subscription_count`,
    /// `channel_occupied` / `channel_vacated`) are deferred to the bridge, so the
    /// connection handler suppresses its node-local emits. `false` ⇒ the
    /// not-yet-clustered percore path keeps the node-local handler emits.
    pub clustered: bool,
    /// Node-level connection ceiling enforced before the per-app `capacity` check.
    /// `0` = unlimited (no ceiling check). When non-zero, the 3rd+ connection that
    /// pushes the process-global [`NODE_CONNS`] counter to or above this limit is
    /// rejected with code **4100** (`server_over_capacity`). The per-app `capacity`
    /// (code 4004) check continues to apply independently.
    pub max_connections: usize,
    /// Task 4: capacity (frames) of each per-connection mailbox bounded channel.
    /// Default 256; configurable via `PYLON_MAILBOX_CAPACITY`. Set once at startup
    /// and shared across all connections (all workers use the same cap since this
    /// is in the shared `DispatchEnv`).
    pub mailbox_capacity: usize,
    /// Per-app live CONNECTION index (shared with `LocalAdapter::purge_app`),
    /// maintained beside `conn_counts`: inserted at establish, removed at close.
    /// Mirrors how `conn_counts` is shared between `DispatchEnv` and `AppState`.
    pub app_registry: Arc<AppRegistry>,
    /// Phase 7: the tokio runtime handle captured in [`run_percore`] BEFORE the
    /// worker OS-threads spawn. The worker is a raw `std::thread` where
    /// `Handle::try_current()` is `Err`, so it cannot reach tokio on its own; this
    /// handle lets the L1-MISS establish path `spawn` an offloaded `by_key` lookup
    /// (parking the one connection) instead of blocking the whole worker on I/O.
    pub runtime: tokio::runtime::Handle,
}

/// Configuration for a single worker event loop.
pub struct WorkerConfig {
    /// Address to bind the listener to.
    pub addr: std::net::SocketAddr,
    /// Maximum accepted WebSocket payload size (bytes) per frame.
    pub max_payload: usize,
    /// Per-connection outbound high-water mark (bytes) before backpressure-close.
    pub high_water: usize,
    /// Behaviour applied to inbound frames.
    pub mode: Mode,
    /// Sink for plain-HTTP (REST) connections accepted here but served on the
    /// tokio/axum plane (SP9 §3.4). `None` ⇒ no REST plane (the worker's own
    /// tests); a `Rest` head is then closed as before.
    pub rest_handoff: Option<mpsc::UnboundedSender<crate::transport::rest::RestConn>>,
    /// This worker's index among the spawned per-core workers, used only for
    /// accept-distribution logging (so an operator can confirm `SO_REUSEPORT` is
    /// spreading connections across cores). `0` for a lone/test worker.
    pub worker_id: usize,
    /// SP10: this worker's slice of the global memory budget (bytes). Each worker
    /// owns its slice (Seastar shared-nothing model); the graduated shed (§6)
    /// compares this worker's `inflight_bytes` against it. `0` ⇒ no budget
    /// enforcement (echo workers / tests that don't size a budget).
    pub per_worker_budget: u64,
    /// SP10: this worker's slot in the shared inflight-bytes vector. The worker
    /// stores its local `inflight_bytes` here every iteration so the off-hot-path
    /// `percore_total_inflight_bytes()` test hook can sum across workers. `None`
    /// for echo/test workers without budget accounting.
    pub inflight_slot: Option<Arc<AtomicU64>>,
    /// B1: cumulative accepted-connections counter for this worker. Bumped once
    /// per successful `accept()` call; `None` for echo/test workers.
    pub accepted_slot: Option<Arc<AtomicU64>>,
    /// B1: cumulative CoDel-dropped-frames counter for this worker. Updated after
    /// each flush by folding `conn.take_codel_dropped()`; `None` for test workers.
    pub codel_dropped_slot: Option<Arc<AtomicU64>>,
    /// Task 4: cumulative mailbox-full-drop counter for this worker. Incremented
    /// inside each connection's [`Mailbox::send`] on a `try_send` full-error.
    /// Shared (via `Arc` clone) with every `Mailbox` created by this worker's
    /// connections. `None` for echo/test workers without mailbox drop tracking.
    pub mailbox_dropped_slot: Option<Arc<AtomicU64>>,
    /// SP10 §7: CoDel time-in-queue freshness parameters, stamped onto every
    /// connection at accept. `target_ns == 0` disables CoDel (pure drop-head).
    pub codel: crate::transport::conn::CodelParams,
    /// SP10 §8: shared PSI budget factor (fixed-point ×1000, 1000 = full budget).
    /// A control-plane loop shrinks it under real memory pressure; the worker reads
    /// it (relaxed) when computing its effective shed budget — never reads PSI
    /// inline. `None` ⇒ no backstop (factor pinned at 1.0).
    pub budget_factor: Option<Arc<AtomicU32>>,
    /// Per-core SHARDED broadcast wiring (SP9). `Some` for percore dispatch
    /// workers: the inbound side of this worker's broadcast inbox (paired with
    /// the `Sender` held in the sink) plus the slot whose `waker` `OnceLock` this
    /// worker fills at startup so the sink can nudge it. `None` for echo workers
    /// and the single-worker `tests/percore.rs` parity harness, which fall back
    /// to draining nothing here (those tests use no sink, so broadcasts route via
    /// the legacy registry mailbox path instead).
    pub broadcast: Option<BroadcastWiring>,
    /// C2a: maximum milliseconds the drain phase may run before the worker
    /// force-closes remaining connections and exits. `0` ⇒ no grace (immediate
    /// close on shutdown, matching the old behaviour). Echo/test workers can set
    /// this to `0`; production workers get it from `ServerConfig::shutdown_grace_ms`.
    pub shutdown_grace_ms: u64,
    /// Optional rustls server config. `Some` ⇒ every accepted connection is
    /// wrapped with a TLS server-side handshake; `None` ⇒ plain TCP (legacy).
    pub tls: Option<Arc<rustls::ServerConfig>>,
}

/// The per-worker half of the sharded broadcast plumbing handed to [`run`].
pub struct BroadcastWiring {
    /// Inbound broadcast hand-offs from the sink (the matching `SyncSender` lives
    /// in `slot.tx`). Drained on the [`BCAST_WAKER`] event and once per loop.
    pub rx: std::sync::mpsc::Receiver<crate::transport::fanout::BroadcastMsg>,
    /// This worker's sink slot; its `waker` `OnceLock` is filled at startup with
    /// a `Waker` built from this worker's own `Poll` registry.
    pub slot: Arc<crate::transport::fanout::WorkerSlot>,
    /// The sink-shared saturation flag. After this worker fully drains its
    /// broadcast inbox to empty (so the bounded hand-off has headroom again), it
    /// clears this flag, letting the publish-admission path resume accepting.
    pub saturated: Arc<std::sync::atomic::AtomicBool>,
}

/// Worker behaviour for inbound frames.
pub enum Mode {
    /// Echo every data frame back to the sender; answer pings with pongs.
    Echo,
    /// Drive the real Pusher v7 protocol via [`ConnectionContext::dispatch`].
    Dispatch(Arc<DispatchEnv>),
}

/// Per-connection v7 protocol state, present once the WS handshake completes on
/// a [`Mode::Dispatch`] worker.
struct Session {
    ctx: ConnectionContext,
    /// Inbound side of the connection mailbox; the matching sender lives in
    /// `ctx.self_tx` (and is handed to other connections via `ctx.handle()`).
    rx: mpsc::Receiver<Box<ServerEvent>>,
    codec: Box<dyn Codec>,
    /// The app id + its connection counter, so disconnect can decrement.
    conn_count: Arc<AtomicUsize>,
    /// The channel set this connection was in as of the last `local_subs`
    /// reconcile. Diffed against `ctx.subscribed` after each dispatch to compute
    /// the worker-local subscription-index deltas (added/removed channels).
    subs: HashSet<String>,
}

/// Per-connection slab entry: the [`Connection`] plus its read remainder and,
/// for dispatch workers, the v7 [`Session`] built at handshake completion.
///
/// `inbuf` is empty or tiny when the connection is idle (it only holds bytes
/// that arrived mid-frame). During [`ConnState::Handshaking`] it doubles as the
/// head-accumulation buffer until [`handshake::read_head`] returns something
/// other than [`HeadResult::NeedMore`].
struct Entry {
    conn: Connection,
    inbuf: BytesMut,
    /// The [`Token`] this connection is registered under (== `Token(slab_key)`).
    token: Token,
    /// v7 protocol state; `None` for echo workers and pre-handshake connections.
    session: Option<Session>,
    /// Phase 7: set while this connection is PARKED waiting on an offloaded app
    /// lookup (L1 miss). `Open` + `session: None` + `pending_establish: Some(..)`
    /// is the park state. Cleared (`take`) when its `ResolvedApp` arrives, or
    /// dropped wholesale when the connection closes mid-park (leaking nothing —
    /// no counter was taken). A park holds the slab slot but no app resources.
    pending_establish: Option<PendingEstablish>,
}

/// Phase 7: the establish state captured at park time, replayed by
/// [`drain_resolved`] when the offloaded `by_key` lookup completes. Holds the
/// negotiated `codec` + the worker's mailbox notifier so resume can run
/// [`finish_establish`] exactly as the synchronous path would have. `gen` is the
/// monotonic park generation: it must match the `ResolvedApp.gen` or the
/// resolution is for a since-recycled slab token and is discarded.
struct PendingEstablish {
    key: String,
    codec: Box<dyn Codec>,
    notify: MailboxNotify,
    mailbox_dropped: Option<Arc<AtomicU64>>,
    gen: u64,
}

/// Phase 7: the result of an offloaded `by_key` lookup, sent from the tokio task
/// back to the worker over an unbounded channel. `token` is the parked
/// connection's slab key; `gen` guards against slab-token recycling (a mismatch
/// means the slot now holds a different connection → discard).
struct ResolvedApp {
    token: usize,
    gen: u64,
    result: Result<Option<Arc<App>>, crate::app::AppLookupError>,
}

/// Build a `mio` listener bound to `addr` with `SO_REUSEADDR` + `SO_REUSEPORT`
/// set before bind. `SO_REUSEPORT` lets every per-core worker bind the SAME
/// `bind:port` independently; the kernel then load-balances incoming connections
/// across the workers' listener sockets (one accept queue per worker).
fn reuseport_listener(addr: std::net::SocketAddr) -> std::io::Result<TcpListener> {
    let sock = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    sock.set_reuse_address(true)?;
    sock.set_reuse_port(true)?; // SO_REUSEPORT — kernel load-balances accepts across workers
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    sock.listen(1024)?;
    Ok(TcpListener::from_std(std::net::TcpListener::from(sock)))
}

/// Run the worker event loop until `shutdown` is set. Blocks the calling thread.
///
/// Builds its OWN `SO_REUSEPORT` listener on `cfg.addr` — every worker calls this
/// with the same address, and the kernel spreads accepts across them. Returns
/// once `shutdown` is observed `true` (clean stop) or a fatal I/O error occurs
/// while binding/polling.
pub fn run(mut cfg: WorkerConfig, shutdown: Arc<AtomicBool>) -> std::io::Result<()> {
    let mut poll = Poll::new()?;
    let mut listener = reuseport_listener(cfg.addr)?;
    poll.registry()
        .register(&mut listener, LISTENER, Interest::READABLE)?;

    // This worker's SINGLE `mio::Waker` (mio allows exactly one active per `Poll`).
    // Shared by both wake sources: the broadcast sink and the selective mailbox
    // drain. A wake only unblocks the poll; the post-loop drains then run.
    let worker_waker = Arc::new(mio::Waker::new(poll.registry(), WORKER_WAKER)?);

    // Per-core sharded broadcast plumbing (SP9). Take the wiring out of `cfg`
    // (the `Receiver` is not `Sync`, so it can't stay borrowed); publish the shared
    // worker `Waker` into the sink slot so the publisher can nudge us to drain.
    // `None` ⇒ no broadcast inbox (echo workers / single-worker parity harness):
    // broadcasts route via the legacy registry mailbox path, which now also wakes
    // through `Mailbox::send` and is drained by `drain_dirty_sessions`.
    let broadcast = cfg.broadcast.take();
    let broadcast_rx = match &broadcast {
        Some(w) => {
            // The slot is created with an empty `OnceLock`; this is its only set.
            let _ = w.slot.waker.set(worker_waker.clone());
            Some(&w.rx)
        }
        None => None,
    };
    // The sink-shared saturation flag, cleared after each full broadcast drain.
    let saturated = broadcast.as_ref().map(|w| w.saturated.clone());

    // Waker-driven SELECTIVE mailbox drain: a per-worker dirty-token channel.
    // Every CROSS-connection delivery routes through `Mailbox::send`, which pushes
    // the target connection's slab token onto `dirty_tx` and wakes `worker_waker`.
    // We then drain ONLY those tokens' sessions — idle connections are never
    // visited (O(dirty), not O(N)). On a dispatch worker the shared waker `Arc` +
    // `dirty_tx` are cloned into each session's `ctx.mailbox_notify` in
    // `handle_handshake` (and carried through `finish_establish`); echo workers
    // never stamp one, so `dirty_rx` stays empty
    // and the selective drain is a no-op `try_recv` each iteration.
    let (dirty_tx, dirty_rx) = std::sync::mpsc::channel::<usize>();
    let mailbox_waker = worker_waker;
    // Reused dirty-token set: drained from `dirty_rx` each iteration and deduped
    // (a connection may be marked dirty several times before we drain it).
    let mut dirty_set: HashSet<usize> = HashSet::new();

    // Phase 7: the resume channel for parked (L1-miss) establishes. An offloaded
    // tokio task pushes a `ResolvedApp` here and wakes `worker_waker` (the SAME
    // WORKER_WAKER that backs the dirty drain), so the loop drains it next pass.
    // Unbounded + lives for the worker's whole lifetime ⇒ `tx.send` never fails;
    // a discarded (recycled-token) result is a harmless no-op.
    let (resolved_tx, resolved_rx) = std::sync::mpsc::channel::<ResolvedApp>();
    // Monotonic park generation, bumped once per park. Guards slab-token reuse so
    // a late resolution for a freed/recycled token is detected and dropped.
    let mut next_gen: u64 = 0;

    // SP10 per-worker byte budget + inflight accounting. `inflight_bytes` is this
    // worker's local (non-atomic) view of how many bytes are queued across all of
    // its connections' out-queues — maintained INCREMENTALLY: every site that
    // touches a connection's out-queue folds in that connection's exact signed
    // `take_inflight_delta()` (queue/flush/drop-head/CoDel), and `remove` subtracts
    // a closing connection's still-queued bytes. So the byte-accounting invariant
    // ("a byte enqueued is decremented exactly once, on send XOR drop") holds by
    // construction and the hot loop is O(work), not O(connections). It is mirrored
    // into the shared `inflight_slot` for the `percore_total_inflight_bytes()` test
    // hook, and drives the graduated shed on the broadcast drain.
    let per_worker_budget = cfg.per_worker_budget;
    let inflight_slot = cfg.inflight_slot.clone();
    let accepted_slot = cfg.accepted_slot.clone();
    let codel_dropped_slot = cfg.codel_dropped_slot.clone();
    // SP10 §8: shared PSI budget factor (×1000 fixed-point). `None` ⇒ no backstop.
    let budget_factor = cfg.budget_factor.clone();
    // SP10 §7: CoDel parameters stamped onto every accepted connection.
    let codel = cfg.codel;
    // Running total of queued bytes across all of this worker's connections,
    // maintained incrementally (see above). Starts at 0 — the slab is empty — and
    // every connection begins with a 0 out-queue, so the counter is exact from the
    // first iteration without an initial O(N) sum.
    let mut inflight_bytes: u64 = 0;
    // B1: worker-local accumulator for CoDel drops; mirrored into `codel_dropped_slot`.
    let mut codel_dropped_total: u64 = 0;

    // Worker-local subscription index: which of THIS worker's connections are in
    // each `(app, channel)`. Populated by reconciling `ctx.subscribed` after each
    // dispatch; consulted when a `BroadcastMsg` arrives to fan the frame out to
    // exactly this worker's local subscribers.
    let mut local_subs: HashMap<(Arc<str>, Arc<str>), HashSet<SocketId>> = HashMap::new();
    // Reverse lookup: a subscriber's `socket_id` → its slab token, so a broadcast
    // delivery can find the connection in O(1) without scanning the slab.
    let mut sid_to_token: HashMap<SocketId, usize> = HashMap::new();

    // SP11 §4: per-worker liveness timer wheel. Idle-pings a silent connection
    // after `activity_timeout` and closes it `4201` if a pong doesn't follow
    // within `pong_timeout` — the Pusher v7 liveness contract without a
    // per-connection tokio timer. Keyed by the slab token. Only meaningful for
    // dispatch workers (echo workers / pre-handshake conns never enter it); the
    // timeouts come from the dispatch env (config-derived).
    let (mut wheel, liveness) = match &cfg.mode {
        Mode::Dispatch(env) => (
            TimerWheel::with_timeouts(env.activity_timeout, env.pong_timeout),
            true,
        ),
        Mode::Echo => (TimerWheel::with_timeouts(0, 0), false),
    };

    // Per-app shared maps the close path reclaims. Pulled into run-loop scope from
    // the dispatch env once (Echo workers have no env → no per-app reclaim).
    let (conn_counts, app_registry): (
        Arc<DashMap<String, Arc<AtomicUsize>>>,
        Arc<AppRegistry>,
    ) = match &cfg.mode {
        Mode::Dispatch(env) => (env.conn_counts.clone(), env.app_registry.clone()),
        Mode::Echo => (Arc::new(DashMap::new()), Arc::new(AppRegistry::new())),
    };

    let mut events = Events::with_capacity(1024);
    let mut conns: slab::Slab<Entry> = slab::Slab::new();

    // Adaptive poll timeout: when the previous iteration did real work (or any
    // connection still has buffered writes), poll non-blocking so cross-worker
    // mailbox deliveries drain promptly under load; when idle, block up to 50ms
    // (which also bounds how long `shutdown` goes unchecked) to avoid spinning.
    // A cross-connection mailbox send no longer waits for this idle poll: it wakes
    // the `MAILBOX_WAKER` and the selective drain delivers it on the next pass.
    let mut did_work = true;
    let dispatch = matches!(cfg.mode, Mode::Dispatch(_));
    // Total connections this worker has accepted — logged at shutdown so an
    // operator can confirm SO_REUSEPORT spread accepts across cores.
    let mut accepted_total: u64 = 0;

    // SP10 §7: monotonic epoch for CoDel per-frame enqueue timestamps. A single
    // `now_ns` is computed at the top of each loop iteration and threaded into
    // every `queue`/`flush`, so a frame's sojourn is the real wall-clock time it
    // spent queued ACROSS iterations (enqueue in iter N, flush in iter N+k).
    let worker_epoch = Instant::now();

    // C2a: graceful-drain state. `drain_started` gates the one-time setup (deregister
    // listener, queue Close 1001 on all open connections). `drain_deadline` is the
    // absolute Instant after which we force-close regardless of inflight bytes.
    let mut drain_started = false;
    let mut drain_deadline: Option<Instant> = None;

    loop {
        if shutdown.load(Ordering::SeqCst) {
            // C2a drain phase — runs only on the shutdown path, zero cost otherwise.
            let now_ns = worker_epoch.elapsed().as_nanos() as u64;
            if !drain_started {
                drain_started = true;
                drain_deadline = if cfg.shutdown_grace_ms > 0 {
                    Some(Instant::now() + Duration::from_millis(cfg.shutdown_grace_ms))
                } else {
                    None // grace_ms == 0 ⇒ immediate exit (old behaviour)
                };
                // 1. Stop accepting new connections: deregister this worker's
                //    SO_REUSEPORT listener from the poll. The broadcast/mailbox
                //    waker registration stays so we keep flushing.
                let _ = poll.registry().deregister(&mut listener);
                // 2. Queue a `pusher:error` 4200 text frame + WS Close(4200) on
                //    every open connection. pusher-js reads code 4200 as "reconnect
                //    immediately" → the LB routes the client to a surviving node on
                //    a rolling restart (vs code 1001 which triggers backoff).
                //    Collect keys first to avoid aliasing `conns` while iterating.
                let keys: Vec<usize> = conns.iter().map(|(k, _)| k).collect();
                for k in keys {
                    queue_shutdown_error(&mut conns, k, now_ns);
                    send_close_4200(&poll, &mut conns, k, now_ns);
                    // INCREMENTAL INFLIGHT: mirror the 4201 path (lines ~668-674).
                    // The Close frame may not flush synchronously (backpressured
                    // client). Without this fold, `inflight_bytes` stays 0 and the
                    // drain's `inflight_bytes == 0` exit fires immediately, dropping
                    // the still-queued Close frame. After this fold, inflight_bytes
                    // is exact: the exit only fires when all Close frames are truly
                    // flushed, and the debug_assert_eq holds on a non-idle drain.
                    fold_delta(&mut conns, k, &mut inflight_bytes);
                }
                tracing::info!(
                    worker = cfg.worker_id,
                    conns = conns.len(),
                    "percore worker draining"
                );
            }
            // Decide whether the drain is complete: all bytes flushed, deadline
            // expired, or grace_ms == 0 (immediate mode).
            let expired = drain_deadline.is_none_or(|d| Instant::now() >= d);
            if inflight_bytes == 0 || expired {
                // 3. Final cleanup: run on_close hooks, decrement conn_counts,
                //    deindex channels, deregister sockets — so per-app counters and
                //    presence/channel state return to 0.
                let keys: Vec<usize> = conns.iter().map(|(k, _)| k).collect();
                for k in keys {
                    remove(
                        &poll,
                        &mut conns,
                        k,
                        &mut local_subs,
                        &mut sid_to_token,
                        &mut wheel,
                        &mut inflight_bytes,
                        &conn_counts,
                        &app_registry,
                    );
                }
                tracing::debug!(
                    worker = cfg.worker_id,
                    accepted = accepted_total,
                    "percore worker drained, stopping"
                );
                return Ok(());
            }
            // else: fall through — the rest of the loop polls writable events and
            // flushes the queued Close frames + any pending out-bytes. The poll
            // timeout of 0ms (pending_writes is true while inflight_bytes > 0)
            // keeps this tight. We re-check inflight_bytes/deadline each iteration.
        }

        // Debug-only cross-check: the incrementally-maintained `inflight_bytes`
        // must equal the true sum of every connection's queued bytes. Any missed
        // delta site (a `queue`/`flush`/drop that didn't fold, or a `remove` that
        // didn't subtract) makes this panic in tests — the SP10 overload flood
        // (queue + drop-head + CoDel + send all firing) is the hardest case. Free
        // in release (compiles out under `#[cfg(debug_assertions)]`).
        debug_assert_eq!(
            inflight_bytes,
            conns
                .iter()
                .map(|(_, e)| e.conn.out_bytes() as u64)
                .sum::<u64>(),
            "incremental inflight_bytes drifted from the true out_bytes sum",
        );

        // Mirror the incrementally-maintained total into the shared slot for the
        // off-hot-path `percore_total_inflight_bytes()` test hook. O(1).
        if let Some(slot) = &inflight_slot {
            slot.store(inflight_bytes, Ordering::Relaxed);
        }
        // B1: mirror CoDel drop total into the shared slot (O(1), only on actual drops).
        if codel_dropped_total > 0 {
            if let Some(slot) = &codel_dropped_slot {
                slot.store(codel_dropped_total, Ordering::Relaxed);
            }
        }
        // `pending_writes` is just "any bytes still queued", now read off the
        // incremental counter instead of a fresh O(N) sum.
        let pending_writes = inflight_bytes > 0;
        let timeout = if did_work || pending_writes {
            Some(Duration::from_millis(0))
        } else {
            Some(Duration::from_millis(50))
        };

        if let Err(e) = poll.poll(&mut events, timeout) {
            // A signal can interrupt the poll syscall; just retry.
            if e.kind() == ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }

        // SP10 §7: this iteration's monotonic timestamp (ns since the worker
        // epoch), threaded into every `queue`/`flush` so CoDel measures real
        // time-in-queue across iterations. Computed once per iteration (cheap;
        // off the per-frame inner loop).
        let now_ns = worker_epoch.elapsed().as_nanos() as u64;
        // SP11 §4: same monotonic clock in milliseconds for the liveness wheel.
        let now_ms = now_ns / 1_000_000;

        // SP10 §8: this worker's effective byte budget = per_worker_budget scaled
        // by the shared PSI factor (×1000 fixed-point; 1000 = full). Read once per
        // iteration (relaxed); the hot path never reads PSI itself.
        let effective_budget = match &budget_factor {
            Some(f) => {
                let factor = f.load(Ordering::Relaxed) as u64;
                per_worker_budget.saturating_mul(factor) / 1000
            }
            None => per_worker_budget,
        };

        // Track whether this iteration accomplished anything worth a tight
        // re-poll: any readiness event, or a non-empty cross-worker drain below.
        let mut work = !events.is_empty();

        for event in events.iter() {
            match event.token() {
                LISTENER => {
                    let n = accept_ready(&poll, &mut listener, &mut conns, &cfg, codel);
                    accepted_total += n;
                    if n > 0 {
                        if let Some(slot) = &accepted_slot {
                            slot.fetch_add(n, Ordering::Relaxed);
                        }
                    }
                }
                // The single worker `Waker` only exists to unblock the poll so the
                // post-loop drains (broadcast + selective mailbox) run promptly; the
                // dirty tokens / broadcast messages were already queued by the waker
                // source (`Mailbox::send` / the sink). No per-event work here.
                WORKER_WAKER => {}
                token => {
                    let key = token.0;
                    // The connection may have been removed earlier in this same
                    // event batch (e.g. a read closed it before its writable
                    // event is processed); skip stale tokens.
                    if !conns.contains(key) {
                        continue;
                    }

                    // A peer hangup / error: tear down regardless of r/w intent.
                    if event.is_error() || event.is_read_closed() || event.is_write_closed() {
                        remove(
                            &poll,
                            &mut conns,
                            key,
                            &mut local_subs,
                            &mut sid_to_token,
                            &mut wheel,
                            &mut inflight_bytes,
                            &conn_counts,
                            &app_registry,
                        );
                        continue;
                    }

                    if event.is_readable() {
                        // SP11 §4: inbound bytes are activity — reset this
                        // connection's idle deadline (and cancel any pending
                        // pong-timeout close: a `pusher:pong`, like any other
                        // inbound frame, is just activity). Only dispatch
                        // (`liveness`) workers run the wheel.
                        if liveness {
                            wheel.touch(key, now_ms);
                        }
                        match handle_readable(
                            &poll,
                            &mut conns,
                            key,
                            &cfg,
                            now_ns,
                            &dirty_tx,
                            &mailbox_waker,
                            &resolved_tx,
                            &mut next_gen,
                        ) {
                            Action::Close => {
                                remove(
                                    &poll,
                                    &mut conns,
                                    key,
                                    &mut local_subs,
                                    &mut sid_to_token,
                                    &mut wheel,
                                    &mut inflight_bytes,
                                    &conn_counts,
                                    &app_registry,
                                );
                                continue;
                            }
                            Action::Handoff(prefix) => {
                                // A REST handoff is not a WS session; drop its
                                // (spurious) wheel entry so it can't fire later. A
                                // REST head queued nothing, so any folded delta is
                                // 0; fold anyway so a removed conn never leaks.
                                fold_delta(&mut conns, key, &mut inflight_bytes);
                                wheel.remove(key);
                                handoff_rest(&poll, &mut conns, key, &cfg, prefix);
                                continue;
                            }
                            Action::Keep => {
                                // INCREMENTAL INFLIGHT: the readable path queued
                                // replies (handshake 101 / established / dispatched
                                // frames / pong) and flushed; fold this connection's
                                // net delta into the running total.
                                fold_delta(&mut conns, key, &mut inflight_bytes);
                                fold_codel(&mut conns, key, &mut codel_dropped_total);
                                // A subscribe/unsubscribe in this readable batch
                                // may have changed channel membership; reconcile
                                // this connection's worker-local subscription
                                // index so later broadcasts route correctly.
                                if let Some(entry) = conns.get_mut(key) {
                                    if let Some(session) = entry.session.as_mut() {
                                        reconcile_membership(
                                            session,
                                            key,
                                            &mut local_subs,
                                            &mut sid_to_token,
                                        );
                                    }
                                }
                            }
                        }
                    }

                    if event.is_writable() && conns.contains(key) {
                        let action = handle_writable(&poll, &mut conns, key, now_ns);
                        // INCREMENTAL INFLIGHT: the flush sent bytes out; fold the
                        // (negative) delta before any close so the count is exact.
                        fold_delta(&mut conns, key, &mut inflight_bytes);
                        fold_codel(&mut conns, key, &mut codel_dropped_total);
                        if action == Action::Close {
                            remove(
                                &poll,
                                &mut conns,
                                key,
                                &mut local_subs,
                                &mut sid_to_token,
                                &mut wheel,
                                &mut inflight_bytes,
                                &conn_counts,
                                &app_registry,
                            );
                        }
                    }
                }
            }
        }

        // Per-core SHARDED fan-out: drain this worker's broadcast inbox and
        // deliver each already-WS-framed payload to its LOCAL subscribers by
        // direct slab-enqueue (no per-conn mpsc, no per-conn wake). Run every
        // iteration (the Waker wakes an idle worker; the unconditional drain is a
        // safety net under load when no Waker event fires). Drains are no-ops
        // when the inbox is empty.
        if let Some(rx) = broadcast_rx {
            if drain_broadcasts(
                &poll,
                &mut conns,
                rx,
                &mut local_subs,
                &mut sid_to_token,
                &mut wheel,
                effective_budget,
                &mut inflight_bytes,
                &mut codel_dropped_total,
                saturated.as_ref(),
                now_ns,
                &conn_counts,
                &app_registry,
            ) {
                work = true;
            }
            // `inflight_bytes` is maintained incrementally THROUGH the drain (each
            // enqueue folds its net delta; each post-drain flush folds its sent
            // bytes; internal closes subtract their queued bytes), so no O(N)
            // re-sum is needed. Mirror the up-to-date total into the test hook.
            if let Some(slot) = &inflight_slot {
                slot.store(inflight_bytes, Ordering::Relaxed);
            }
            // `drain_broadcasts` empties the bounded hand-off inbox (its
            // `while rx.try_recv()` loop runs to `Empty`), so the channel now has
            // headroom: clear the sink's saturation flag. The publish-admission
            // path (Phase 2) thereby resumes accepting once delivery catches up.
            if let Some(sat) = &saturated {
                sat.store(false, Ordering::Relaxed);
            }
        }

        // SELECTIVE mailbox drain. A DIRECT send queued onto a connection's mailbox
        // (subscription_succeeded, member rosters, send_to_user, terminate,
        // notify_watchers, cluster follow-ups) had no readiness event of its own, so
        // `Mailbox::send` pushed that connection's slab token onto `dirty_rx` and woke
        // `MAILBOX_WAKER`. Drain `dirty_rx` into the reused (deduped) `dirty_set` and
        // drain ONLY those connections' mailboxes; idle connections are never visited
        // (O(dirty), not O(N)). When truly idle `dirty_rx` is empty, so this is O(1).
        // (Channel broadcasts go through `drain_broadcasts` above when a sink is wired;
        // the legacy registry mailbox path also routes through `Mailbox::send`, so its
        // sends mark their targets dirty and are drained here too.) Returns whether it
        // wrote anything so the adaptive poll stays tight under load.
        if dispatch
            && drain_dirty_sessions(
                &poll,
                &mut conns,
                &dirty_rx,
                &mut dirty_set,
                &mut local_subs,
                &mut sid_to_token,
                &mut wheel,
                &mut inflight_bytes,
                &mut codel_dropped_total,
                now_ns,
                &conn_counts,
                &app_registry,
            )
        {
            work = true;
        }

        // Phase 7: drain completed offloaded app lookups and resume (or reject)
        // each parked connection. Same WORKER_WAKER nudges this; no-op (O(1)
        // `try_recv` → Empty) when nothing is parked. Only meaningful on dispatch
        // workers (echo workers never park).
        if dispatch {
            if let Mode::Dispatch(env) = &cfg.mode {
                if drain_resolved(
                    &poll,
                    &mut conns,
                    &resolved_rx,
                    env,
                    &mut local_subs,
                    &mut sid_to_token,
                    &mut wheel,
                    &mut inflight_bytes,
                    &mut codel_dropped_total,
                    now_ns,
                    &conn_counts,
                    &app_registry,
                ) {
                    work = true;
                }
            }
        }

        // SP11 §4: fire any liveness timers that have come due this iteration.
        // For an idle-expired connection queue a `pusher:ping` and arm its pong
        // deadline; for a pong-timed-out connection send the `4201` close and
        // tear it down (running the normal `remove` close path: on_close hook,
        // counter decrement, deregister). The wheel only visits expired tokens,
        // so this is O(due-count), not O(N-connections). The adaptive poll may
        // sleep up to 50ms, so a timer fires within ~50ms of its deadline —
        // negligible against the 120s/30s timeouts.
        if liveness {
            for due in wheel.due(now_ms) {
                match due {
                    Due::Ping(key) => {
                        if queue_ping(&poll, &mut conns, key, now_ns) {
                            // INCREMENTAL INFLIGHT: the ping was queued + flushed;
                            // fold this connection's net delta into the total.
                            fold_delta(&mut conns, key, &mut inflight_bytes);
                            // Ping queued: arm the pong-timeout close deadline.
                            wheel.mark_ping_sent(key, now_ms);
                            work = true;
                        } else {
                            // The connection vanished (or had no session): drop it
                            // from the wheel so the entry doesn't linger.
                            wheel.remove(key);
                        }
                    }
                    Due::Close4201(key) => {
                        send_close_4201(&poll, &mut conns, key, now_ns);
                        // INCREMENTAL INFLIGHT: the 4201 close frame was queued +
                        // flushed; fold the net delta before `remove` subtracts any
                        // bytes still queued on the connection being torn down.
                        fold_delta(&mut conns, key, &mut inflight_bytes);
                        fold_codel(&mut conns, key, &mut codel_dropped_total);
                        remove(
                            &poll,
                            &mut conns,
                            key,
                            &mut local_subs,
                            &mut sid_to_token,
                            &mut wheel,
                            &mut inflight_bytes,
                            &conn_counts,
                            &app_registry,
                        );
                        work = true;
                    }
                }
            }
        }

        did_work = work;
    }
}

/// SP11 §4: queue a `pusher:ping` (v7 `{"event":"pusher:ping","data":{}}`) onto
/// `key`'s out-queue and flush, the same way [`drain_session`] emits a server
/// frame. Returns `true` if the frame was queued
/// (the connection exists, is Open, and has a session); `false` otherwise (caller
/// drops the wheel entry). A flush that backpressures arms writable interest; a
/// flush that fails closes the connection on the next event.
fn queue_ping(poll: &Poll, conns: &mut slab::Slab<Entry>, key: usize, now_ns: u64) -> bool {
    let Some(entry) = conns.get_mut(key) else {
        return false;
    };
    let Some(session) = entry.session.as_mut() else {
        return false;
    };
    if entry.conn.state != ConnState::Open {
        return false;
    }
    let text = session.codec.encode(&ServerEvent::Ping);
    let mut out = BytesMut::new();
    frame::encode_text(&mut out, text.as_bytes());
    let _ = entry
        .conn
        .queue(Arc::from(out.to_vec().into_boxed_slice()), now_ns);
    // Best-effort flush; interest re-arming / close is handled by the caller's
    // subsequent `remove` only on a hard write failure (rare for a tiny frame).
    let _ = flush_and_arm(poll, entry, now_ns);
    true
}

/// Send a WebSocket Close frame with the given `code` and `reason` text, then
/// let the caller handle the connection (either `remove` it immediately or wait
/// for flush). Mirrors the `ServerEvent::Close` arm of [`drain_session`].
fn send_close(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    key: usize,
    now_ns: u64,
    code: u16,
    reason: &str,
) {
    let Some(entry) = conns.get_mut(key) else {
        return;
    };
    let mut frame_body = Vec::with_capacity(2 + reason.len());
    frame_body.extend_from_slice(&code.to_be_bytes());
    frame_body.extend_from_slice(reason.as_bytes());
    let mut out = BytesMut::new();
    frame::encode(&mut out, true, OpCode::Close, &frame_body);
    let _ = entry
        .conn
        .queue(Arc::from(out.to_vec().into_boxed_slice()), now_ns);
    // Flush so the Close frame actually reaches the peer before we deregister.
    let _ = flush_and_arm(poll, entry, now_ns);
}

/// SP11 §4: send a WebSocket Close frame with code `4201` (pong-timeout) with the
/// canonical Pusher v7 reason text, then let the caller `remove` the connection.
fn send_close_4201(poll: &Poll, conns: &mut slab::Slab<Entry>, key: usize, now_ns: u64) {
    send_close(
        poll,
        conns,
        key,
        now_ns,
        4201,
        "Pong reply not received: ping was sent to the client, but no reply was received",
    );
}

/// C2a drain: send a WebSocket Close frame with code `4200` (Pusher
/// "reconnect immediately") with the canonical shutdown reason text.
/// pusher-js reads 4200 and reconnects to the LB immediately — this minimises
/// client disruption on a rolling restart compared to the 1001 generic-gone-away.
/// The caller is responsible for the subsequent `fold_delta` + eventual `remove`.
fn send_close_4200(poll: &Poll, conns: &mut slab::Slab<Entry>, key: usize, now_ns: u64) {
    send_close(
        poll,
        conns,
        key,
        now_ns,
        4200,
        "Server is shutting down; please reconnect",
    );
}

/// C2a drain: queue a `pusher:error` 4200 text frame onto `key`'s out-queue
/// BEFORE the Close frame. This is the belt-and-suspenders convention from
/// Soketi (which does `ws.end(4200)` after sending the `pusher:error` event).
/// The frame payload matches the Pusher v7 wire format for connection-level
/// errors: `{"event":"pusher:error","data":{"code":4200,"message":"…"}}`.
///
/// If the connection no longer exists or has no session this is a no-op: the
/// Close frame alone (code 4200 in the WS Close payload) is sufficient for
/// pusher-js to recognise the reconnect-immediately signal.
fn queue_shutdown_error(conns: &mut slab::Slab<Entry>, key: usize, now_ns: u64) {
    let Some(entry) = conns.get_mut(key) else {
        return;
    };
    // Build the pusher:error 4200 text frame. Use the session codec when
    // present (so encoding is consistent with every other server event),
    // fall back to the same raw-JSON form used by `queue_reject` when no
    // codec has been negotiated yet (connection still handshaking).
    let text = if let Some(session) = entry.session.as_ref() {
        session.codec.encode(&ServerEvent::Error(
            crate::protocol::error::PusherError::new(
                4200,
                "Server is shutting down; please reconnect",
            ),
        ))
    } else {
        // No codec yet (connection in handshaking state): hand-build the
        // raw JSON that the v7 codec would have produced. The data field is
        // a plain JSON object (not double-encoded) — matching `queue_reject`.
        serde_json::json!({
            "event": "pusher:error",
            "data": { "code": 4200_u16, "message": "Server is shutting down; please reconnect" }
        })
        .to_string()
    };
    let mut out = BytesMut::new();
    frame::encode_text(&mut out, text.as_bytes());
    let _ = entry
        .conn
        .queue(Arc::from(out.to_vec().into_boxed_slice()), now_ns);
    // No explicit flush here: the caller queues the Close frame next, and
    // `send_close` calls `flush_and_arm` which flushes both frames together.
}

/// Outcome of handling a connection event: keep it, close it, or hand it off to
/// the tokio/axum REST plane.
#[derive(Debug, PartialEq, Eq)]
enum Action {
    Keep,
    Close,
    /// A plain-HTTP request head was detected: transfer the connection (and the
    /// `Vec<u8>` of bytes already read off the socket, to be replayed) to the
    /// REST handoff channel. Carries the bytes to replay.
    Handoff(Vec<u8>),
}

/// Drain the listener's accept backlog, registering every accepted socket.
/// Returns the number of connections accepted this call (for accept-distribution
/// accounting).
fn accept_ready(
    poll: &Poll,
    listener: &mut TcpListener,
    conns: &mut slab::Slab<Entry>,
    cfg: &WorkerConfig,
    codel: crate::transport::conn::CodelParams,
) -> u64 {
    let mut accepted = 0;
    loop {
        match listener.accept() {
            Ok((mut stream, _peer)) => {
                let entry = conns.vacant_entry();
                let key = entry.key();
                if let Err(e) =
                    poll.registry()
                        .register(&mut stream, Token(key), Interest::READABLE)
                {
                    // Registration failed: drop the socket, leave the slab slot
                    // unused (vacant_entry didn't consume it).
                    tracing::debug!(error = %e, "failed to register accepted socket");
                    continue;
                }
                let mut conn = if let Some(tls_cfg) = &cfg.tls {
                    match rustls::server::ServerConnection::new(tls_cfg.clone()) {
                        Ok(sc) => Connection::new_tls(stream, Box::new(sc), cfg.high_water),
                        Err(e) => {
                            tracing::debug!(error = %e, "failed to create TLS ServerConnection; dropping");
                            continue;
                        }
                    }
                } else {
                    Connection::new(stream, cfg.high_water)
                };
                conn.set_codel(codel);
                entry.insert(Entry {
                    conn,
                    inbuf: BytesMut::new(),
                    token: Token(key),
                    session: None,
                    pending_establish: None,
                });
                accepted += 1;
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
            Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => {
                tracing::debug!(error = %e, "listener accept error");
                break;
            }
        }
    }
    accepted
}

/// Handle a readable event: either advance the handshake or process frames.
///
/// `dirty_tx` + `mailbox_waker` are this worker's selective-drain notifier inputs;
/// on handshake completion they are stamped (with this connection's slab `key` as
/// the token) into the new session's `ctx.mailbox_notify`, so a later
/// cross-connection `Mailbox::send` marks this connection dirty and wakes the worker.
#[allow(clippy::too_many_arguments)]
fn handle_readable(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    key: usize,
    cfg: &WorkerConfig,
    now_ns: u64,
    dirty_tx: &std::sync::mpsc::Sender<usize>,
    mailbox_waker: &Arc<mio::Waker>,
    resolved_tx: &std::sync::mpsc::Sender<ResolvedApp>,
    next_gen: &mut u64,
) -> Action {
    let entry = &mut conns[key];
    match entry.conn.state {
        ConnState::Handshaking => handle_handshake(
            poll, entry, key, cfg, now_ns, dirty_tx, mailbox_waker, resolved_tx, next_gen,
        ),
        ConnState::Open | ConnState::Closing => handle_frames(poll, entry, cfg, now_ns),
    }
}

/// Accumulate request-head bytes and, once complete, complete the WS upgrade.
///
/// `key` is this connection's slab token; `dirty_tx` + `mailbox_waker` are the
/// worker's selective-drain notifier inputs, stamped (with `key`) into the new
/// session's `ctx.mailbox_notify` so cross-connection sends wake the worker.
#[allow(clippy::too_many_arguments)]
fn handle_handshake(
    poll: &Poll,
    entry: &mut Entry,
    key: usize,
    cfg: &WorkerConfig,
    now_ns: u64,
    dirty_tx: &std::sync::mpsc::Sender<usize>,
    mailbox_waker: &Arc<mio::Waker>,
    resolved_tx: &std::sync::mpsc::Sender<ResolvedApp>,
    next_gen: &mut u64,
) -> Action {
    // Pull all available bytes into the head-accumulation buffer (`inbuf`).
    if drain_into(&mut entry.conn, &mut entry.inbuf) == ReadOutcome::Closed {
        return Action::Close;
    }

    match handshake::read_head(&entry.inbuf) {
        HeadResult::NeedMore => Action::Keep,
        HeadResult::WsUpgrade { key: ws_key, path } => {
            let response = handshake::accept_response(&ws_key).into_boxed_slice();
            // Drop-head queue never rejects; the 101 response always enqueues.
            let _ = entry.conn.queue(Arc::from(response), now_ns);
            // A browser never sends data frames before the 101, so any bytes
            // after the head would be a protocol error anyway; clearing is safe.
            entry.inbuf.clear();
            entry.conn.state = ConnState::Open;

            // For a dispatch worker, build the v7 session now: resolve the app,
            // check capacity, create the mailbox + ConnectionContext, and queue
            // the connection_established frame. On a rejection (unknown app 4001,
            // unsupported protocol 4007, over-capacity 4004) emit the `pusher:error`
            // frame + a WS Close carrying the error code.
            if let Mode::Dispatch(env) = &cfg.mode {
                // Stamp this connection's slab token + the worker's notifier inputs
                // into the session so a cross-connection `Mailbox::send` marks this
                // connection dirty and wakes the worker's selective drain.
                let notify = MailboxNotify {
                    token: key,
                    dirty: dirty_tx.clone(),
                    waker: mailbox_waker.clone(),
                };
                use crate::protocol::error::PusherError;
                let (app_key, protocol) = parse_app_path(&path);
                let codec = match negotiate(protocol.as_deref(), env.strict_protocol) {
                    Ok(c) => c,
                    Err(error) => {
                        queue_reject(entry, &Reject { error, codec: None }, now_ns);
                        let _ = flush_and_arm(poll, entry, now_ns);
                        return Action::Close;
                    }
                };
                let resolved = match env.apps.by_key_cached(&app_key) {
                    Some(Ok(Some(app))) => finish_establish(
                        env,
                        app,
                        codec,
                        notify,
                        cfg.mailbox_dropped_slot.clone(),
                    ),
                    Some(Ok(None)) => Err(Reject {
                        error: PusherError::app_not_found(),
                        codec: Some(codec),
                    }),
                    Some(Err(e)) => {
                        tracing::warn!(key = %app_key, error = %e, "app probe failed");
                        Err(Reject {
                            error: PusherError::backend_unavailable(),
                            codec: Some(codec),
                        })
                    }
                    // L1 MISS / raw driver: PARK this one connection and offload the
                    // `by_key` lookup to tokio. NO counter is taken here (counters
                    // are only incremented in `finish_establish` at resume), so a
                    // drop while parked leaks nothing. The connection stays in the
                    // slab as `Open` + `session: None` + `pending_establish: Some`.
                    None => {
                        let gen = *next_gen;
                        *next_gen = next_gen.wrapping_add(1);
                        entry.pending_establish = Some(PendingEstablish {
                            key: app_key.clone(),
                            codec,
                            notify,
                            mailbox_dropped: cfg.mailbox_dropped_slot.clone(),
                            gen,
                        });
                        // Offload the lookup. The spawned task echoes `token` + `gen`
                        // back in the `ResolvedApp` and wakes the worker; an
                        // unbounded send to a worker-lifetime channel never fails.
                        let apps = env.apps.clone();
                        let tx = resolved_tx.clone();
                        let waker = mailbox_waker.clone();
                        let token = key;
                        let lookup_key = app_key;
                        env.runtime.spawn(async move {
                            let result = apps.by_key(&lookup_key).await;
                            let _ = tx.send(ResolvedApp { token, gen, result });
                            let _ = waker.wake();
                        });
                        // No establish frame yet — flush whatever is queued (the 101)
                        // and keep the connection. It resumes in `drain_resolved`.
                        return flush_and_arm(poll, entry, now_ns);
                    }
                };
                match resolved {
                    Ok(session) => {
                        let established = ServerEvent::ConnectionEstablished {
                            socket_id: session.ctx.socket_id,
                            activity_timeout: env.activity_timeout,
                        };
                        let text = session.codec.encode(&established);
                        let mut out = BytesMut::new();
                        frame::encode_text(&mut out, text.as_bytes());
                        let _ = entry
                            .conn
                            .queue(Arc::from(out.to_vec().into_boxed_slice()), now_ns);
                        entry.session = Some(session);
                    }
                    Err(reject) => {
                        queue_reject(entry, &reject, now_ns);
                        // Flush so the error + Close reach the peer, then tear down.
                        let _ = flush_and_arm(poll, entry, now_ns);
                        return Action::Close;
                    }
                }
            }

            flush_and_arm(poll, entry, now_ns)
        }
        // A plain-HTTP request (a Pusher REST publish): hand the connection off
        // to the tokio/axum plane. We have read *all* currently-available bytes
        // into `inbuf` (head + any body that arrived with it); the whole buffer
        // is the prefix to replay to the HTTP parser. With no REST plane wired
        // (`rest_handoff == None`, e.g. the worker's own echo tests) we close.
        HeadResult::Rest { .. } => {
            if cfg.rest_handoff.is_some() {
                Action::Handoff(entry.inbuf.to_vec())
            } else {
                Action::Close
            }
        }
        HeadResult::Bad(_) => Action::Close,
    }
}

/// A pre-session rejection: the `pusher:error` to emit plus the codec that should
/// encode it. `codec` is `None` only when protocol negotiation itself failed (no
/// codec exists yet) — then the error frame is a hand-built raw JSON object (the
/// no-codec fallback).
struct Reject {
    error: crate::protocol::error::PusherError,
    codec: Option<Box<dyn Codec>>,
}

/// The deferred establish TAIL, reusable by both the synchronous L1-hit path and
/// the park-resume path (Task 3). Given a resolved `app` + a negotiated `codec`,
/// enforce the node ceiling, the saturation gate, and the per-app capacity, then
/// build the [`ConnectionContext`] and register the connection. Counters
/// (`NODE_CONNS`, `conn_counts`) are incremented HERE and rolled back on every
/// reject — never before the app is resolved — so a connection that drops before
/// this runs (e.g. while parked) leaks nothing.
fn finish_establish(
    env: &Arc<DispatchEnv>,
    app: Arc<App>,
    codec: Box<dyn Codec>,
    notify: MailboxNotify,
    mailbox_dropped: Option<Arc<AtomicU64>>,
) -> Result<Session, Reject> {
    use crate::protocol::error::PusherError;

    // Node-level ceiling check: enforced before the per-app capacity check.
    // `max_connections == 0` means unlimited (no ceiling).
    let node_n = NODE_CONNS.fetch_add(1, Ordering::SeqCst);
    if env.max_connections != 0 && node_n >= env.max_connections {
        NODE_CONNS.fetch_sub(1, Ordering::SeqCst);
        return Err(Reject {
            error: PusherError::server_over_capacity(),
            codec: Some(codec),
        });
    }

    // Task 3: memory-pressure admission gate — reject new connections when the
    // broadcast pipeline is saturated. Runs after the node-ceiling fetch_add, so
    // we MUST release the count we just took (same accounting discipline as the
    // node-ceiling reject above). `None` ⇒ flag is not wired (echo workers /
    // tests) → never saturated → never rejects.
    if env
        .saturated
        .as_ref()
        .is_some_and(|s| s.load(Ordering::Relaxed))
    {
        NODE_CONNS.fetch_sub(1, Ordering::SeqCst);
        return Err(Reject {
            error: PusherError::server_over_capacity(),
            codec: Some(codec),
        });
    }

    let counter = env
        .conn_counts
        .entry(app.id.clone())
        .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
        .clone();
    let current = counter.fetch_add(1, Ordering::SeqCst);
    if app.capacity != 0 && current >= app.capacity as usize {
        counter.fetch_sub(1, Ordering::SeqCst);
        NODE_CONNS.fetch_sub(1, Ordering::SeqCst);
        return Err(Reject {
            error: PusherError::over_capacity(),
            codec: Some(codec),
        });
    }

    let socket_id = SocketId::generate();
    // Task 4: bounded mailbox — capacity from config (default 256). Under extreme
    // overload, `Mailbox::send` uses `try_send` and drops on full, bumping the
    // per-worker `mailbox_dropped` counter. Under normal (non-full) load delivery
    // is unchanged: `try_send` on a non-full channel is non-blocking and succeeds.
    // `.max(1)`: `mpsc::channel(0)` panics. `from_env` already rejects 0, but a
    // direct `WorkerEnv` struct literal could pass it — clamp here so the single
    // point where the capacity reaches tokio is panic-proof regardless of source.
    // Mailbox carries `Box<ServerEvent>` (8 B), not `ServerEvent` (104 B): tokio mpsc
    // eagerly allocates a 32-slot block per channel at creation, so a bare ServerEvent
    // makes every connection pay 32*104 ≈ 3.3 KB up front even while idle (profiled as
    // the single largest per-conn allocation). Boxing shrinks that block ~6x; the heap
    // event is allocated only when a direct send actually happens (off the broadcast
    // hot path, which uses the encode-once Arc<[u8]> sink).
    let (tx, rx) = mpsc::channel::<Box<ServerEvent>>(env.mailbox_capacity.max(1));
    let ctx = ConnectionContext {
        app,
        socket_id,
        self_tx: tx,
        adapter: env.adapter.clone(),
        client_event_rate: crate::ws::rate::RateWindow::new(
            env.limits.max_client_events_per_second,
        ),
        limits: env.limits,
        subscribed: HashSet::new(),
        user: None,
        webhooks: env.webhooks.clone(),
        presence_membership: HashMap::new(),
        saturated: env.saturated.clone(),
        // SP11 §3.6: the clustered percore node defers the single-emit cluster
        // edges to the bridge (so the handler suppresses its node-local emits);
        // the not-yet-clustered percore path keeps the node-local handler emits.
        clustered: env.clustered,
        // The worker's selective-drain notifier (this connection's slab token +
        // the dirty queue + the MAILBOX_WAKER). `ctx.handle()` builds a WAKING
        // `Mailbox` from it, so cross-connection sends wake the worker.
        mailbox_notify: Some(notify),
        // Task 4: shared per-worker drop counter — cloned into every `Mailbox`
        // this connection hands out, so any full-mailbox drop is attributed to
        // this worker's `pylon_mailbox_dropped_total` metric.
        mailbox_dropped,
    };

    // Register this live connection under its app so a cluster-wide `purge_app`
    // can force-close it. `ctx.handle()` builds a WAKING mailbox (a cross-thread
    // purge send marks this connection dirty + wakes the worker). All rejection
    // paths above returned BEFORE `ctx` was built, so a rejected connection never
    // registers — exactly as `conn_counts` is rolled back on reject.
    env.app_registry.insert(&ctx.app.id, ctx.handle());

    Ok(Session {
        ctx,
        rx,
        codec,
        conn_count: counter,
        subs: HashSet::new(),
    })
}

/// Queue the pre-session rejection frames onto `entry`'s out-queue: first the
/// `pusher:error` Text frame (codec-encoded when a codec exists, else the raw
/// JSON fallback), then a WebSocket Close frame carrying the error `code` +
/// `message`. The caller flushes and closes.
fn queue_reject(entry: &mut Entry, reject: &Reject, now_ns: u64) {
    // 1) the pusher:error Text frame.
    let text = match &reject.codec {
        Some(c) => c.encode(&ServerEvent::Error(reject.error.clone())),
        None => serde_json::json!({
            "event": "pusher:error",
            "data": { "code": reject.error.code, "message": reject.error.message }
        })
        .to_string(),
    };
    let mut out = BytesMut::new();
    frame::encode_text(&mut out, text.as_bytes());
    let _ = entry
        .conn
        .queue(Arc::from(out.to_vec().into_boxed_slice()), now_ns);

    // 2) the WS Close frame: code = the pusher error code, reason = its message.
    let reason = &reject.error.message;
    let mut frame_body = Vec::with_capacity(2 + reason.len());
    frame_body.extend_from_slice(&reject.error.code.to_be_bytes());
    frame_body.extend_from_slice(reason.as_bytes());
    let mut close_out = BytesMut::new();
    frame::encode(&mut close_out, true, OpCode::Close, &frame_body);
    let _ = entry
        .conn
        .queue(Arc::from(close_out.to_vec().into_boxed_slice()), now_ns);
}

/// Split a `/app/{key}` path (with an optional `?protocol=N&...` query) into the
/// app key and the `protocol` query value.
fn parse_app_path(path: &str) -> (String, Option<String>) {
    let (raw_path, query) = match path.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (path, None),
    };
    let key = raw_path.strip_prefix("/app/").unwrap_or("").to_string();
    let protocol = query.and_then(|q| {
        q.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            (k == "protocol").then(|| v.to_string())
        })
    });
    (key, protocol)
}

/// Read and process every complete frame currently buffered, per [`Mode`].
fn handle_frames(poll: &Poll, entry: &mut Entry, cfg: &WorkerConfig, now_ns: u64) -> Action {
    let frames = {
        // Split the borrow so `inbuf` (the read remainder) and `conn` can be
        // borrowed at once via a temporary swap-out of the buffer.
        let mut scratch = std::mem::take(&mut entry.inbuf);
        let result = entry.conn.read_frames(&mut scratch, cfg.max_payload);
        entry.inbuf = scratch;
        match result {
            Ok(frames) => frames,
            // EOF or a fatal protocol violation: close.
            Err(ConnError::Closed) | Err(ConnError::Protocol(_)) => return Action::Close,
            Err(ConnError::Backpressure) => return Action::Close,
        }
    };

    match &cfg.mode {
        Mode::Echo => echo_frames(poll, entry, frames, now_ns),
        Mode::Dispatch(_) => dispatch_frames(poll, entry, frames, now_ns),
    }
}

/// [`Mode::Echo`]: re-encode every data frame back, answer pings with pongs.
fn echo_frames(poll: &Poll, entry: &mut Entry, frames: Vec<frame::Frame>, now_ns: u64) -> Action {
    let mut wrote = false;
    for f in frames {
        match f.opcode {
            OpCode::Text | OpCode::Binary | OpCode::Continuation => {
                let mut out = BytesMut::new();
                frame::encode(&mut out, f.fin, f.opcode, &f.payload);
                let _ = entry
                    .conn
                    .queue(Arc::from(out.to_vec().into_boxed_slice()), now_ns);
                wrote = true;
            }
            OpCode::Ping => {
                let mut out = BytesMut::new();
                frame::encode(&mut out, true, OpCode::Pong, &f.payload);
                let _ = entry
                    .conn
                    .queue(Arc::from(out.to_vec().into_boxed_slice()), now_ns);
                wrote = true;
            }
            // A peer pong is unsolicited noise here; ignore it.
            OpCode::Pong => {}
            OpCode::Close => return Action::Close,
        }
    }

    if wrote {
        flush_and_arm(poll, entry, now_ns)
    } else {
        Action::Keep
    }
}

/// [`Mode::Dispatch`]: decode each Text frame to a [`ClientCommand`] and drive
/// `ctx.dispatch`, answer pings with pongs, close on a Close frame, then drain
/// this connection's mailbox so any self-directed replies go out.
fn dispatch_frames(
    poll: &Poll,
    entry: &mut Entry,
    frames: Vec<frame::Frame>,
    now_ns: u64,
) -> Action {
    for f in frames {
        match f.opcode {
            OpCode::Text => {
                // The session always exists once Open on a dispatch worker.
                let Some(session) = entry.session.as_mut() else {
                    return Action::Close;
                };
                let text = match std::str::from_utf8(&f.payload) {
                    Ok(t) => t,
                    // A non-UTF-8 text frame is malformed; drop it.
                    Err(_) => continue,
                };
                match session.codec.decode(text) {
                    Ok(cmd) => dispatch_command(session, cmd),
                    Err(e) => {
                        // Unparseable frames are silently dropped; 4200 is a
                        // close/reconnect code and must not be sent in-band.
                        tracing::trace!("dropping malformed client frame: {e}");
                    }
                }
            }
            OpCode::Ping => {
                let mut out = BytesMut::new();
                frame::encode(&mut out, true, OpCode::Pong, &f.payload);
                let _ = entry
                    .conn
                    .queue(Arc::from(out.to_vec().into_boxed_slice()), now_ns);
            }
            OpCode::Pong => {}
            // Binary/Continuation are not part of the Pusher protocol; ignore.
            OpCode::Binary | OpCode::Continuation => {}
            OpCode::Close => return Action::Close,
        }
    }

    // Drain this connection's mailbox: dispatch may have enqueued self-directed
    // replies (subscription_succeeded, pong, errors) plus the adapter may have
    // fanned a broadcast onto it. The readable path reconciles this connection's
    // membership after a `Keep` (see the `Action::Keep` arm in `run`), so any
    // `subscribed` change a drained `SubscriptionError` made here is picked up there.
    drain_session(poll, entry, now_ns).action
}

/// Run one command through the (async) protocol handler synchronously.
fn dispatch_command(session: &mut Session, cmd: ClientCommand) {
    futures_executor::block_on(session.ctx.dispatch(cmd));
}

/// Outcome of a [`drain_session`] call: the resulting [`Action`], whether any frame
/// was written (keeps the adaptive poll tight), and whether `ctx.subscribed` changed
/// during the drain (a `SubscriptionError` removed a channel) so the caller can
/// reconcile this connection's worker-local subscription index.
struct DrainResult {
    action: Action,
    wrote: bool,
    subs_changed: bool,
}

/// Drain every queued [`ServerEvent`] from this connection's mailbox: encode and
/// queue each as a Text frame, except [`ServerEvent::Close`] which becomes a
/// WebSocket Close frame and ends the connection. Returns a [`DrainResult`]: the
/// resulting [`Action`] (`Close` if a close was requested or a write failed),
/// whether anything was actually written (so the loop's adaptive poll stays tight),
/// and whether `ctx.subscribed` changed during the drain.
///
/// A [`ServerEvent::SubscriptionError`] means the subscription did NOT take (the
/// cluster-wide presence-capacity reject fired on the bridge, or any auth/validation
/// failure): the channel must NOT remain in `ctx.subscribed` / `presence_membership`.
/// So before encoding the frame (which is still sent to the client unchanged) the
/// channel is removed from both. This is safe for ALL subscription errors: the
/// auth-failure cases (non-cluster) never inserted the channel (the handler returns
/// early), so the remove is a harmless no-op; the cluster-capacity reject DID
/// inline-join the channel, so the remove reverses it — paired with the caller's
/// post-drain `reconcile_membership` (run when `subs_changed`), the connection is
/// fully deindexed from delivery.
fn drain_session(poll: &Poll, entry: &mut Entry, now_ns: u64) -> DrainResult {
    let Some(session) = entry.session.as_mut() else {
        return DrainResult {
            action: Action::Keep,
            wrote: false,
            subs_changed: false,
        };
    };

    let mut close_after = false;
    let mut wrote = false;
    let mut subs_changed = false;
    while let Ok(ev) = session.rx.try_recv() {
        match *ev {
            ServerEvent::Close { code, reason } => {
                let mut out = BytesMut::new();
                let mut frame_body = Vec::with_capacity(2 + reason.len());
                frame_body.extend_from_slice(&code.to_be_bytes());
                frame_body.extend_from_slice(reason.as_bytes());
                frame::encode(&mut out, true, OpCode::Close, &frame_body);
                let _ = entry
                    .conn
                    .queue(Arc::from(out.to_vec().into_boxed_slice()), now_ns);
                wrote = true;
                close_after = true;
                break;
            }
            other => {
                // A subscription error means the subscription did not take: drop the
                // channel from this connection's protocol state BEFORE encoding the
                // (unchanged) frame, so it is not left a member. No-op when the channel
                // was never inserted (the non-cluster auth-failure cases return early in
                // the handler before any insert).
                if let ServerEvent::SubscriptionError { channel, .. } = &other {
                    if session.ctx.subscribed.remove(channel) {
                        subs_changed = true;
                    }
                    session.ctx.presence_membership.remove(channel);
                }
                let text = session.codec.encode(&other);
                let mut out = BytesMut::new();
                frame::encode_text(&mut out, text.as_bytes());
                let _ = entry
                    .conn
                    .queue(Arc::from(out.to_vec().into_boxed_slice()), now_ns);
                wrote = true;
            }
        }
    }

    let action = if (wrote && flush_and_arm(poll, entry, now_ns) == Action::Close) || close_after {
        Action::Close
    } else {
        Action::Keep
    };
    DrainResult {
        action,
        wrote,
        subs_changed,
    }
}

/// Waker-driven SELECTIVE mailbox drain: visit ONLY the connections whose mailbox
/// actually received a cross-connection send this round, instead of scanning every
/// Open connection. `dirty_rx` carries the slab tokens that `Mailbox::send` pushed
/// (one per cross-connection delivery); they are drained into the reused, deduped
/// `dirty_set` (a connection marked dirty several times is drained once) and only
/// those connections' mailboxes are drained. Idle connections are never visited —
/// O(dirty), not O(N); when no dirty tokens are pending this is an O(1) empty
/// `try_recv`.
///
/// A token whose slab entry is gone, closed, or not yet a session is skipped (a
/// reused slab slot is harmless: `drain_session` only delivers that connection's
/// own queued events and is idempotent, so no generation guard is needed).
/// Connections that request a close (or whose write fails) are torn down. A
/// `subscribed` change during the drain (a `SubscriptionError` — e.g. the bridge's
/// cluster-wide presence-capacity reject) is reconciled into the worker-local
/// delivery index, exactly as the old per-iteration scan did, but only for the
/// dirty connection. Returns `true` if any connection actually wrote a queued
/// event (keeps the adaptive poll tight).
#[allow(clippy::too_many_arguments)]
fn drain_dirty_sessions(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    dirty_rx: &std::sync::mpsc::Receiver<usize>,
    dirty_set: &mut HashSet<usize>,
    local_subs: &mut HashMap<(Arc<str>, Arc<str>), HashSet<SocketId>>,
    sid_to_token: &mut HashMap<SocketId, usize>,
    wheel: &mut TimerWheel,
    inflight_bytes: &mut u64,
    codel_total: &mut u64,
    now_ns: u64,
    conn_counts: &Arc<DashMap<String, Arc<AtomicUsize>>>,
    app_registry: &Arc<AppRegistry>,
) -> bool {
    // Drain the dirty-token queue into the reused set (dedup). Cheap + O(1) when
    // empty (the idle case). The set is cleared at the end so it never grows
    // unbounded across iterations.
    dirty_set.clear();
    while let Ok(tok) = dirty_rx.try_recv() {
        dirty_set.insert(tok);
    }
    if dirty_set.is_empty() {
        return false;
    }

    let mut wrote_any = false;
    for &key in dirty_set.iter() {
        // The token may be stale: the connection closed since it was marked dirty,
        // or its slab slot is vacant/recycled. Skip anything that isn't an Open
        // session — draining a recycled slot would be a no-op anyway, but skipping
        // avoids touching an unrelated connection.
        match conns.get(key) {
            Some(e) if e.session.is_some() && e.conn.state == ConnState::Open => {}
            _ => continue,
        }
        #[cfg(any(test, feature = "test-hooks"))]
        SELECTIVE_DRAIN_VISITS.fetch_add(1, Ordering::Relaxed);
        let result = drain_session(poll, &mut conns[key], now_ns);
        wrote_any |= result.wrote;
        // INCREMENTAL INFLIGHT: `drain_session` queued mailbox events and flushed;
        // fold this connection's net delta (queued minus sent/dropped) into the
        // running total whether or not it closes (a closing conn's REMAINING
        // queued bytes are then subtracted by `remove`).
        fold_delta(conns, key, inflight_bytes);
        fold_codel(conns, key, codel_total);
        if result.action == Action::Close {
            remove(
                poll,
                conns,
                key,
                local_subs,
                sid_to_token,
                wheel,
                inflight_bytes,
                conn_counts,
                app_registry,
            );
            continue;
        }
        // A `subscribed` change made during this mailbox drain (a `SubscriptionError`
        // removed a channel — e.g. the bridge's cluster-wide presence-capacity reject)
        // must propagate to the worker-local delivery index so the rejected connection
        // stops receiving that channel's broadcasts. Gated on an actual change so the
        // path stays O(visited), not O(N-channels) per connection: only the rare
        // rejected connection pays the two-set-diff reconcile.
        if result.subs_changed {
            if let Some(entry) = conns.get_mut(key) {
                if let Some(session) = entry.session.as_mut() {
                    reconcile_membership(session, key, local_subs, sid_to_token);
                }
            }
        }
    }
    dirty_set.clear();
    wrote_any
}

/// Phase 7: drain completed offloaded app lookups and resume each parked
/// connection. For each `ResolvedApp { token, gen, result }`:
///
/// * absent slab entry → the parked connection closed; discard.
/// * `pending_establish.gen != gen` (or `pending_establish` is `None`) → the slab
///   token was recycled to a different connection; discard (no use-after-park).
/// * else take the `PendingEstablish` and apply:
///   * `Ok(Some(app))` → `finish_establish`; on `Ok(session)` queue
///     `connection_established` + store the session + flush; on `Err(reject)`
///     queue the reject + flush + `remove`.
///   * `Ok(None)`  → reject `app_not_found` (4001) + flush + `remove`.
///   * `Err(_)`    → reject `backend_unavailable` (4103) + flush + `remove`.
///
/// Returns whether it wrote anything (so the adaptive poll stays tight). O(1) when
/// the channel is empty.
#[allow(clippy::too_many_arguments)]
fn drain_resolved(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    resolved_rx: &std::sync::mpsc::Receiver<ResolvedApp>,
    env: &Arc<DispatchEnv>,
    local_subs: &mut HashMap<(Arc<str>, Arc<str>), HashSet<SocketId>>,
    sid_to_token: &mut HashMap<SocketId, usize>,
    wheel: &mut TimerWheel,
    inflight_bytes: &mut u64,
    codel_total: &mut u64,
    now_ns: u64,
    conn_counts: &Arc<DashMap<String, Arc<AtomicUsize>>>,
    app_registry: &Arc<AppRegistry>,
) -> bool {
    use crate::protocol::error::PusherError;
    let mut wrote_any = false;
    while let Ok(ResolvedApp { token, gen, result }) = resolved_rx.try_recv() {
        // The parked connection may have closed; its slab slot is gone or recycled.
        // Slab-token recycling guard: only resume if THIS park is still pending and
        // its generation matches. A mismatch means a new connection took the slot.
        // `take` the PendingEstablish in this tight borrow scope so the borrow of
        // `conns` is released before the `fold_delta`/`remove` calls below re-borrow
        // it (mirrors `drain_dirty_sessions`, which reborrows `&mut conns[key]`
        // inline rather than holding an `entry` across the helper calls).
        let pe = {
            let Some(entry) = conns.get_mut(token) else {
                continue;
            };
            match &entry.pending_establish {
                Some(p) if p.gen == gen => {}
                _ => continue,
            }
            entry.pending_establish.take().expect("pending_establish present")
        };

        let outcome = match result {
            Ok(Some(app)) => finish_establish(env, app, pe.codec, pe.notify, pe.mailbox_dropped),
            Ok(None) => Err(Reject {
                error: PusherError::app_not_found(),
                codec: Some(pe.codec),
            }),
            Err(e) => {
                tracing::warn!(key = %pe.key, error = %e, "offloaded app lookup failed (transient)");
                Err(Reject {
                    error: PusherError::backend_unavailable(),
                    codec: Some(pe.codec),
                })
            }
        };

        match outcome {
            Ok(session) => {
                // Liveness: the wheel was already touched when this connection's
                // WS-upgrade read event arrived (the `wheel.touch` in the run loop's
                // readable arm, BEFORE `handle_handshake` parked it), so a resumed
                // connection carries the same idle deadline as a synchronously-
                // established one — no re-touch is needed here. The park itself is
                // bounded by the driver's own lookup timeout (sqlx acquire / mongo
                // serverSelection / fred command), which is ≪ `activity_timeout`, so
                // the wheel never evicts a still-parked connection in any sane config.
                // Reborrow inline so the `entry` borrow drops before `fold_delta`.
                let action = {
                    let entry = &mut conns[token];
                    let established = ServerEvent::ConnectionEstablished {
                        socket_id: session.ctx.socket_id,
                        activity_timeout: env.activity_timeout,
                    };
                    let text = session.codec.encode(&established);
                    let mut out = BytesMut::new();
                    frame::encode_text(&mut out, text.as_bytes());
                    let _ = entry
                        .conn
                        .queue(Arc::from(out.to_vec().into_boxed_slice()), now_ns);
                    entry.session = Some(session);
                    flush_and_arm(poll, entry, now_ns)
                };
                // INCREMENTAL INFLIGHT: the established frame was queued + flushed.
                fold_delta(conns, token, inflight_bytes);
                fold_codel(conns, token, codel_total);
                wrote_any = true;
                if action == Action::Close {
                    remove(
                        poll, conns, token, local_subs, sid_to_token, wheel,
                        inflight_bytes, conn_counts, app_registry,
                    );
                }
            }
            Err(reject) => {
                {
                    let entry = &mut conns[token];
                    queue_reject(entry, &reject, now_ns);
                    let _ = flush_and_arm(poll, entry, now_ns);
                }
                // INCREMENTAL INFLIGHT: fold the reject frames before `remove`
                // subtracts the connection's still-queued bytes.
                fold_delta(conns, token, inflight_bytes);
                fold_codel(conns, token, codel_total);
                wrote_any = true;
                remove(
                    poll, conns, token, local_subs, sid_to_token, wheel,
                    inflight_bytes, conn_counts, app_registry,
                );
            }
        }
    }
    wrote_any
}

/// Handle a writable event: flush and, when drained, drop writable interest.
fn handle_writable(poll: &Poll, conns: &mut slab::Slab<Entry>, key: usize, now_ns: u64) -> Action {
    let entry = &mut conns[key];
    flush_and_arm(poll, entry, now_ns)
}

/// Flush the outbound queue and reconcile writable interest with what remains.
///
/// * [`WriteStatus::Drained`] → re-arm `READABLE`-only (drop `WRITABLE`).
/// * [`WriteStatus::WouldBlock`] → add `WRITABLE` so we get a writable event.
/// * [`WriteStatus::Closed`] → close.
fn flush_and_arm(poll: &Poll, entry: &mut Entry, now_ns: u64) -> Action {
    // Read the token before the mutable stream borrow below.
    let token = entry.token;
    match entry.conn.flush(now_ns) {
        WriteStatus::Drained => {
            if poll
                .registry()
                .reregister(entry.conn.stream_mut(), token, Interest::READABLE)
                .is_err()
            {
                return Action::Close;
            }
            Action::Keep
        }
        WriteStatus::WouldBlock => {
            if poll
                .registry()
                .reregister(
                    entry.conn.stream_mut(),
                    token,
                    Interest::READABLE | Interest::WRITABLE,
                )
                .is_err()
            {
                return Action::Close;
            }
            Action::Keep
        }
        WriteStatus::Closed => Action::Close,
    }
}

/// Read all currently-available bytes off the socket into `buf`, stopping on
/// `WouldBlock` (socket drained) or EOF. Used only during the handshake, where
/// we accumulate the raw head before any framing.
#[derive(PartialEq, Eq)]
enum ReadOutcome {
    Ok,
    Closed,
}

fn drain_into(conn: &mut Connection, buf: &mut BytesMut) -> ReadOutcome {
    if conn.drain_head_bytes(buf) {
        ReadOutcome::Ok
    } else {
        ReadOutcome::Closed
    }
}

/// Transfer a plain-HTTP connection to the tokio/axum REST plane (SP9 §3.4).
///
/// Order matters: deregister the stream from this `Poll` and remove the slab
/// entry BEFORE moving the fd out of mio, so mio's registry/slab no longer
/// reference it. Then [`rest::mio_to_std`] transfers fd ownership into a
/// `std::net::TcpStream` (the single audited `unsafe` site), and the connection
/// plus its already-read `prefix` bytes are sent to the handoff channel. The
/// stream stays non-blocking (inherited from mio), which is what tokio wants.
///
/// On a missing handoff sender, or a closed channel, the connection is simply
/// dropped (closed). A pre-handshake REST connection never has a [`Session`], so
/// no on-close hook / counter decrement is needed.
fn handoff_rest(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    key: usize,
    cfg: &WorkerConfig,
    prefix: Vec<u8>,
) {
    let Some(mut entry) = conns.try_remove(key) else {
        return;
    };
    let _ = poll.registry().deregister(entry.conn.stream_mut());

    let Some(tx) = cfg.rest_handoff.as_ref() else {
        // No REST plane; dropping `entry` closes the socket.
        return;
    };

    // Use `into_io_handoff` to carry the rustls session for TLS connections.
    // For plain connections this is equivalent to the old `into_stream()` call.
    let handoff = entry.conn.into_io_handoff();
    let (std_stream, tls) = match handoff {
        crate::transport::conn::IoHandoff::Plain(mio_stream) => {
            (crate::transport::rest::mio_to_std(mio_stream), None)
        }
        crate::transport::conn::IoHandoff::Tls(mio_stream, tls_conn) => (
            crate::transport::rest::mio_to_std(mio_stream),
            Some(tls_conn),
        ),
    };
    if let Err(e) = tx.send(crate::transport::rest::RestConn {
        fd_stream: std_stream,
        prefix,
        tls,
    }) {
        // Receiver gone (REST task ended): dropping the RestConn closes the fd.
        tracing::debug!(error = %e, "REST handoff channel closed; dropping connection");
    }
}

/// INCREMENTAL INFLIGHT accounting: fold connection `key`'s accumulated
/// `out_bytes` delta into the worker's running `inflight_bytes`, bringing the
/// counter back in step with that connection's queued bytes after a touch
/// (`handle_readable`/`handle_writable`/`queue_ping`/…). A no-op (delta 0) when
/// the connection didn't change or no longer exists. `wrapping_add` because the
/// delta is signed: a net send/drop folds a negative delta. O(1) — this is what
/// replaces the per-iteration O(connections) re-sum.
fn fold_delta(conns: &mut slab::Slab<Entry>, key: usize, inflight_bytes: &mut u64) {
    if let Some(entry) = conns.get_mut(key) {
        let delta = entry.conn.take_inflight_delta();
        if delta != 0 {
            *inflight_bytes = inflight_bytes.wrapping_add(delta as u64);
        }
    }
}

/// B1: drain this connection's per-frame CoDel drop accumulator into the
/// worker-level total. Called alongside `fold_delta` at every flush site so
/// the shared `codel_dropped_slot` reflects actual drops with zero per-frame
/// cost on the normal (no-drop) path.
fn fold_codel(conns: &mut slab::Slab<Entry>, key: usize, codel_total: &mut u64) {
    if let Some(entry) = conns.get_mut(key) {
        let dropped = entry.conn.take_codel_dropped();
        if dropped > 0 {
            *codel_total = codel_total.wrapping_add(dropped);
        }
    }
}

/// Remove a connection: drop it from the worker's sharded-broadcast indexes,
/// run the protocol on-close hook (dispatch only), decrement the app's
/// connection counter, deregister its socket, and drop the slab entry.
///
/// The index cleanup happens BEFORE `on_close()` so that the unsubscribe-driven
/// broadcasts `on_close` fans out (member_removed / subscription_count) can no
/// longer route back to this very connection, and so a concurrent broadcast
/// drain never targets a slab slot that is about to vanish.
#[allow(clippy::too_many_arguments)]
fn remove(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    key: usize,
    local_subs: &mut HashMap<(Arc<str>, Arc<str>), HashSet<SocketId>>,
    sid_to_token: &mut HashMap<SocketId, usize>,
    wheel: &mut TimerWheel,
    inflight_bytes: &mut u64,
    conn_counts: &Arc<DashMap<String, Arc<AtomicUsize>>>,
    app_registry: &Arc<AppRegistry>,
) {
    // SP11 §4: drop the connection from the liveness wheel BEFORE the slab slot
    // (and thus its token) can be recycled by a future accept, so a new
    // connection on the same token never inherits a stale timer.
    wheel.remove(key);
    if let Some(mut entry) = conns.try_remove(key) {
        // INCREMENTAL INFLIGHT: a removed connection's still-queued bytes leave
        // the worker total. Fold its outstanding delta up to date, then subtract
        // its current `out_bytes` so the running counter doesn't leak upward over
        // the worker's lifetime. (After the fold the connection's contribution to
        // `inflight_bytes` is exactly `out_bytes`, so subtracting it zeroes it.)
        *inflight_bytes = inflight_bytes
            .wrapping_add(entry.conn.take_inflight_delta() as u64)
            .wrapping_sub(entry.conn.out_bytes() as u64);
        entry.conn.send_close_notify();
        if let Some(mut session) = entry.session.take() {
            deindex_connection(&session, local_subs, sid_to_token);
            futures_executor::block_on(session.ctx.on_close());
            let app_id = &session.ctx.app.id;
            // Drop this connection from the per-app registry (remove_if-empty inside).
            app_registry.remove(app_id, &session.ctx.socket_id);
            session.conn_count.fetch_sub(1, Ordering::SeqCst);
            // Pre-existing leak fix: the per-app counter entry is created at
            // establish (`entry().or_insert_with`) but was NEVER removed. Drop it
            // atomically once it reaches 0 (a concurrent establish that bumped it
            // back above 0 is re-checked under the shard lock), matching the other
            // registries so even an idle app that is never deleted leaves no zombie.
            conn_counts.remove_if(app_id, |_, c| c.load(Ordering::SeqCst) == 0);
            NODE_CONNS.fetch_sub(1, Ordering::SeqCst);
        }
        let _ = poll.registry().deregister(entry.conn.stream_mut());
    }
}

/// Drop a closing connection's `socket_id` from every `(app, channel)` it was
/// indexed under, and from the reverse `socket_id → token` map. Uses the
/// session's last-reconciled `subs` set (the channels recorded in `local_subs`),
/// so it removes exactly the entries `reconcile_membership` inserted.
fn deindex_connection(
    session: &Session,
    local_subs: &mut HashMap<(Arc<str>, Arc<str>), HashSet<SocketId>>,
    sid_to_token: &mut HashMap<SocketId, usize>,
) {
    let app: Arc<str> = Arc::from(session.ctx.app.id.as_str());
    let sid = &session.ctx.socket_id;
    for channel in &session.subs {
        let k = (Arc::clone(&app), Arc::<str>::from(channel.as_str()));
        if let Some(set) = local_subs.get_mut(&k) {
            set.remove(sid);
            if set.is_empty() {
                local_subs.remove(&k);
            }
        }
    }
    sid_to_token.remove(sid);
}

/// Reconcile a connection's worker-local subscription index against the protocol
/// state after a dispatch. Diffs the session's previously-recorded channel set
/// (`session.subs`) against `ctx.subscribed`: channels newly joined are inserted
/// into `local_subs` (and the `socket_id → token` reverse map is (re)stamped),
/// channels left are removed. Cheap when nothing changed (two set diffs over the
/// usually-tiny per-connection channel set). `token` is this connection's slab
/// key. No-op for a connection in no channels with no change.
fn reconcile_membership(
    session: &mut Session,
    token: usize,
    local_subs: &mut HashMap<(Arc<str>, Arc<str>), HashSet<SocketId>>,
    sid_to_token: &mut HashMap<SocketId, usize>,
) {
    if session.subs == session.ctx.subscribed {
        return;
    }
    let app: Arc<str> = Arc::from(session.ctx.app.id.as_str());
    let sid = &session.ctx.socket_id;

    // Added channels: present in ctx.subscribed, absent from the recorded set.
    for channel in session.ctx.subscribed.difference(&session.subs) {
        local_subs
            .entry((Arc::clone(&app), Arc::<str>::from(channel.as_str())))
            .or_default()
            .insert(*sid);
    }
    // Removed channels: were recorded, no longer subscribed.
    for channel in session.subs.difference(&session.ctx.subscribed) {
        let k = (Arc::clone(&app), Arc::<str>::from(channel.as_str()));
        if let Some(set) = local_subs.get_mut(&k) {
            set.remove(sid);
            if set.is_empty() {
                local_subs.remove(&k);
            }
        }
    }
    // Keep the reverse map current (stamp on first subscribe; harmless re-stamp).
    sid_to_token.insert(*sid, token);
    // Record the new set as the reconcile baseline.
    session.subs = session.ctx.subscribed.clone();
}

/// SP10 graduated-shed band, derived from this worker's `inflight_bytes` as a
/// fraction of its `per_worker_budget` (Envoy Overload-Manager thresholds). A
/// `per_worker_budget` of 0 disables enforcement (always `Normal`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ShedBand {
    /// < 80%: enqueue every broadcast (per-conn drop-head still applies locally).
    Normal,
    /// 80–95%: skip subscribers whose own out-queue is already > 50% of its cap.
    Pressure,
    /// 95–100%: skip any subscriber whose out-queue is non-trivially backed up.
    Severe,
    /// ≥ 100%: drop the broadcast for this worker entirely; set saturated.
    Saturated,
}

fn shed_band(inflight: u64, budget: u64) -> ShedBand {
    if budget == 0 {
        return ShedBand::Normal;
    }
    // Compare with integer math: inflight*100 vs budget*{80,95,100}.
    let scaled = inflight.saturating_mul(100);
    if scaled < budget.saturating_mul(80) {
        ShedBand::Normal
    } else if scaled < budget.saturating_mul(95) {
        ShedBand::Pressure
    } else if scaled < budget.saturating_mul(100) {
        ShedBand::Severe
    } else {
        ShedBand::Saturated
    }
}

/// Whether, in the current band, a frame should be skipped for a subscriber
/// whose out-queue currently holds `out_bytes` against its `high_water` cap.
/// `Normal` never skips; `Pressure` skips the > 50%-full (slow consumers);
/// `Severe` skips any backed-up (non-trivially non-empty) queue; `Saturated` is
/// handled by the caller (whole broadcast dropped).
fn should_skip(band: ShedBand, out_bytes: usize, high_water: usize) -> bool {
    match band {
        ShedBand::Normal => false,
        ShedBand::Pressure => out_bytes * 2 > high_water, // > 50% full
        // > 1/16 of the cap ⇒ "non-trivially backed up". A caught-up subscriber
        // (queue drained to ~0 between iterations) sails through; one that hasn't
        // drained its last delivery is shed.
        ShedBand::Severe => out_bytes * 16 > high_water,
        ShedBand::Saturated => true,
    }
}

/// Deliver every queued [`BroadcastMsg`] to this worker's local subscribers,
/// applying the SP10 graduated shed (§6) against this worker's byte budget.
///
/// For each message: classify the current [`ShedBand`] from `inflight_bytes /
/// effective_budget`; in `Saturated` (≥100%) the whole broadcast is dropped and
/// the sink flagged; otherwise, for each subscriber (skipping `except`), the
/// already-WS-framed `frame` is `queue`d (an `Arc` bump — never re-encoded)
/// UNLESS the band says to skip a backed-up subscriber. `inflight_bytes` is kept
/// live across the drain (each enqueue adds the net byte delta, accounting for
/// any drop-head eviction) so the band tightens as the worker fills within a
/// single drain. Connections that backpressure-close are torn down. Returns
/// `true` if any frame was queued.
///
/// `effective_budget` is the per-worker budget already scaled by the PSI factor
/// (§8); `now_ns` is this iteration's monotonic timestamp, stamped onto every
/// enqueued frame for the CoDel sojourn check (§7).
#[allow(clippy::too_many_arguments)]
fn drain_broadcasts(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    rx: &std::sync::mpsc::Receiver<crate::transport::fanout::BroadcastMsg>,
    local_subs: &mut HashMap<(Arc<str>, Arc<str>), HashSet<SocketId>>,
    sid_to_token: &mut HashMap<SocketId, usize>,
    wheel: &mut TimerWheel,
    effective_budget: u64,
    inflight_bytes: &mut u64,
    codel_total: &mut u64,
    saturated: Option<&Arc<AtomicBool>>,
    now_ns: u64,
    conn_counts: &Arc<DashMap<String, Arc<AtomicUsize>>>,
    app_registry: &Arc<AppRegistry>,
) -> bool {
    let mut touched: HashSet<usize> = HashSet::new();
    // Connections that backpressured during delivery; closed after the drain so
    // we don't mutate the slab mid-lookup.
    let mut to_close: Vec<usize> = Vec::new();

    while let Ok(msg) = rx.try_recv() {
        let key = (Arc::clone(&msg.app), Arc::clone(&msg.channel));
        let Some(subs) = local_subs.get(&key) else {
            continue; // no local subscribers for this channel on this worker
        };
        for sid in subs.iter() {
            // Reclassify PER SUBSCRIBER: the band tightens as `inflight_bytes`
            // grows within this drain, so once the worker crosses 100% mid-fan-out
            // it stops enqueueing for the remaining subscribers of this very
            // broadcast — the budget is never blown past by a single large channel.
            let band = shed_band(*inflight_bytes, effective_budget);
            if band == ShedBand::Saturated {
                // ≥100%: never enqueue past the budget. Flag saturation so the
                // publish-admission path 503s; skip enqueueing this subscriber.
                if let Some(sat) = saturated {
                    sat.store(true, Ordering::Relaxed);
                }
                continue;
            }
            if msg.except.as_ref() == Some(sid) {
                continue; // sender exclusion
            }
            let Some(&token) = sid_to_token.get(sid) else {
                continue; // stale index entry; connection gone
            };
            if to_close.contains(&token) {
                continue;
            }
            let Some(entry) = conns.get_mut(token) else {
                continue;
            };
            // Only deliver to Open dispatch connections.
            if entry.session.is_none() || entry.conn.state != ConnState::Open {
                continue;
            }
            // Graduated shed: under pressure, skip backed-up subscribers so the
            // fast (caught-up) ones still get every frame — targeted drop.
            if should_skip(band, entry.conn.out_bytes(), entry.conn.high_water()) {
                continue;
            }
            // SP10: the per-connection queue is byte-bounded drop-head — it never
            // rejects. A slow consumer simply loses its OLDEST queued frame(s)
            // (freshest-wins, at-most-once), keeping memory bounded without
            // closing the connection or stalling the fast path. Fold the net byte
            // delta (enqueue minus any drop-head eviction) into the live inflight
            // counter via the `take_inflight_delta` choke point so the band stays
            // accurate within this drain — and so the post-drain flush's send delta
            // (taken below) composes correctly without double-counting.
            let _dropped = entry.conn.queue(msg.frame.clone(), now_ns);
            *inflight_bytes = inflight_bytes.wrapping_add(entry.conn.take_inflight_delta() as u64);
            touched.insert(token);
        }
    }

    let wrote = !touched.is_empty();
    // Flush every connection we queued onto. A flush that backpressures arms
    // writable interest (handled in flush_and_arm); a failed flush closes.
    for token in touched {
        if to_close.contains(&token) {
            continue;
        }
        if let Some(entry) = conns.get_mut(token) {
            let action = flush_and_arm(poll, entry, now_ns);
            // INCREMENTAL INFLIGHT: the flush sent bytes out (negative delta); fold
            // it into the running total so it reflects the post-send queue depth.
            *inflight_bytes = inflight_bytes.wrapping_add(entry.conn.take_inflight_delta() as u64);
            // B1: fold any CoDel drops that happened during this flush.
            let cd = entry.conn.take_codel_dropped();
            if cd > 0 {
                *codel_total = codel_total.wrapping_add(cd);
            }
            if action == Action::Close {
                to_close.push(token);
            }
        }
    }
    // Closing connections subtract their still-queued bytes inside `remove`, so the
    // running total never leaks upward when a backpressured peer is torn down.
    for token in to_close {
        remove(
            poll,
            conns,
            token,
            local_subs,
            sid_to_token,
            wheel,
            inflight_bytes,
            conn_counts,
            app_registry,
        );
    }
    wrote
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use std::sync::atomic::AtomicBool;
    use tokio_tungstenite::tungstenite::Message;

    /// Reserve a free port via a throwaway std listener, then drop it. The OS
    /// won't immediately hand the same port to a different process, so the
    /// worker re-binding it moments later is race-free in practice.
    fn free_port() -> u16 {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    }

    /// Spawn the worker on its own OS thread bound to `addr` in [`Mode::Echo`],
    /// returning the shutdown flag and the join handle.
    fn spawn_worker(addr: std::net::SocketAddr) -> (Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        let shutdown = Arc::new(AtomicBool::new(false));
        let sd = shutdown.clone();
        let handle = std::thread::spawn(move || {
            run(
                WorkerConfig {
                    addr,
                    max_payload: 1 << 20,
                    high_water: 1 << 20,
                    mode: Mode::Echo,
                    rest_handoff: None,
                    worker_id: 0,
                    broadcast: None,
                    per_worker_budget: 0,
                    inflight_slot: None,
                    accepted_slot: None,
                    codel_dropped_slot: None,
                    mailbox_dropped_slot: None,
                    codel: crate::transport::conn::CodelParams::DISABLED,
                    budget_factor: None,
                    shutdown_grace_ms: 0,
                    tls: None,
                },
                sd,
            )
            .expect("worker run failed");
        });
        (shutdown, handle)
    }

    /// THE GATE: a real `tokio-tungstenite` client completes the RFC 6455
    /// handshake against the worker and gets its text frame echoed back.
    #[tokio::test]
    async fn worker_handshakes_and_echoes() {
        let port = free_port();
        let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let (shutdown, handle) = spawn_worker(addr);

        // Give the worker a moment to bind before the client connects.
        tokio::time::sleep(Duration::from_millis(150)).await;

        let url = format!("ws://127.0.0.1:{port}/app/app-key");
        let (mut ws, _resp) = tokio_tungstenite::connect_async(url)
            .await
            .expect("ws connect/handshake");

        ws.send(Message::Text("hello".into()))
            .await
            .expect("send text");

        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("echo within 5s")
            .expect("stream not ended")
            .expect("frame ok");
        assert_eq!(msg.into_text().unwrap(), "hello");

        // A ping must be answered with a pong carrying the same payload.
        ws.send(Message::Ping(b"ping-payload".to_vec()))
            .await
            .expect("send ping");
        // tungstenite auto-responds to pongs at the protocol layer, so drive the
        // stream until we observe the pong (or our own buffered messages).
        let pong = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match ws.next().await {
                    Some(Ok(Message::Pong(p))) => return Some(p),
                    Some(Ok(_)) => continue,
                    _ => return None,
                }
            }
        })
        .await
        .expect("pong within 5s");
        assert_eq!(pong.as_deref(), Some(&b"ping-payload"[..]));

        shutdown.store(true, Ordering::SeqCst);
        handle.join().unwrap();
    }

    /// A second connection on the same worker also handshakes and echoes,
    /// exercising the slab's multi-connection path (distinct tokens).
    #[tokio::test]
    async fn worker_handles_multiple_connections() {
        let port = free_port();
        let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let (shutdown, handle) = spawn_worker(addr);
        tokio::time::sleep(Duration::from_millis(150)).await;

        let url = format!("ws://127.0.0.1:{port}/app/app-key");
        let (mut a, _) = tokio_tungstenite::connect_async(url.clone())
            .await
            .expect("connect a");
        let (mut b, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("connect b");

        a.send(Message::Text("aaa".into())).await.unwrap();
        b.send(Message::Text("bbb".into())).await.unwrap();

        let ma = tokio::time::timeout(Duration::from_secs(5), a.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let mb = tokio::time::timeout(Duration::from_secs(5), b.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(ma.into_text().unwrap(), "aaa");
        assert_eq!(mb.into_text().unwrap(), "bbb");

        shutdown.store(true, Ordering::SeqCst);
        handle.join().unwrap();
    }

    #[test]
    fn parse_app_path_extracts_key_and_protocol() {
        assert_eq!(
            parse_app_path("/app/app-key?protocol=7"),
            ("app-key".to_string(), Some("7".to_string()))
        );
        assert_eq!(
            parse_app_path("/app/app-key"),
            ("app-key".to_string(), None)
        );
        assert_eq!(
            parse_app_path("/app/k?foo=1&protocol=7&bar=2"),
            ("k".to_string(), Some("7".to_string()))
        );
    }
}
