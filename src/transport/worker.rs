//! Single-worker `mio` event loop for the per-core transport (SP9).
//!
//! [`run`] binds a listener, then drives a non-blocking accept → handshake →
//! frame loop entirely on the calling thread with one [`mio::Poll`]. A
//! [`slab::Slab`] is the connection table: the slab key *is* the connection's
//! [`mio::Token`] value, so a readiness event maps to its [`Connection`] in
//! O(1). The listener uses a reserved token (`LISTENER`) that no slab key can
//! collide with.
//!
//! Readiness is managed edge-friendly: a connection is registered
//! `READABLE`-only and only gains `WRITABLE` interest when a [`Connection::flush`] returns
//! [`WriteStatus::WouldBlock`] or (G2) a TLS handshake flight write blocks
//! mid-handshake (see [`crate::transport::conn::DrainStatus::NeedsWrite`]);
//! the interest is dropped back to `READABLE` once
//! the queue drains. This keeps the loop from spinning on a writable socket with
//! nothing to send. It is also what lets the loop poll with a real (50ms)
//! timeout whenever the previous iteration did no work: a backpressured
//! connection's queued bytes are guaranteed a wake-up on socket-drain, so the
//! loop never needs to busy-poll them.
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
use crate::transport::conn::{ConnError, ConnState, Connection, DrainStatus, WriteStatus};
use crate::transport::frame::{self, OpCode};
use crate::transport::handshake::{self, HeadResult};
use crate::transport::timer::{Due, TimerWheel};
use crate::ws::handler::ConnectionContext;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::atomic::AtomicI64;
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

/// Test-hooks instrumentation (G1): a monotonic count of how many times this
/// process's worker loops polled with a 0 ms timeout. The loop only polls 0 ms
/// when the PREVIOUS iteration did real work; a backpressured connection
/// (queued bytes, full send buffer) produces no readiness event, so a test can
/// assert this counter STOPS growing once the flood stops — proving the loop
/// parks in the 50 ms poll instead of busy-spinning on queued bytes. Behind
/// `test-hooks` so it is free in release builds.
#[cfg(any(test, feature = "test-hooks"))]
pub static POLL_ZERO_TIMEOUTS: AtomicU64 = AtomicU64::new(0);

/// Test-hooks accessor: the cumulative number of 0 ms (non-blocking) worker
/// polls across this process (see [`POLL_ZERO_TIMEOUTS`]).
#[cfg(any(test, feature = "test-hooks"))]
pub fn percore_poll_zero_timeouts() -> u64 {
    POLL_ZERO_TIMEOUTS.load(Ordering::Relaxed)
}

/// Test-hooks instrumentation (G5): a live gauge of the `(app, channel) →
/// socket_id` membership slots held across this process's `local_subs`
/// indexes, so a test can assert the index fully empties when a connection
/// closes. Signed, so a bookkeeping bug surfaces as a negative rather than a
/// near-`u64::MAX` positive.
#[cfg(any(test, feature = "test-hooks"))]
pub static LOCAL_SUBS_SLOTS: AtomicI64 = AtomicI64::new(0);

/// Test-hooks accessor: the current number of `(app, channel) → socket_id`
/// membership slots across this process's workers' `local_subs` indexes (see
/// [`LOCAL_SUBS_SLOTS`]). 0 when every connection has been deindexed.
#[cfg(any(test, feature = "test-hooks"))]
pub fn percore_local_subs_len() -> i64 {
    LOCAL_SUBS_SLOTS.load(Ordering::Relaxed)
}

/// Which of THIS worker's connections are in each `(app, channel)`, each entry
/// carrying the subscriber's `(slab token, negotiated protocol version)`.
/// Holding the token and version in the subscriber map itself is what lets the
/// broadcast drain resolve both from its iteration, with no probe lookup.
///
/// `#[doc(hidden)]`: exposed only so `benches/fanout_sink.rs` can build the
/// index in the production shape.
#[doc(hidden)]
pub type LocalSubs = HashMap<(Arc<str>, Arc<str>), HashMap<SocketId, (usize, u8)>>;

/// Reserved token for this worker's single [`mio::Waker`]. One below [`LISTENER`];
/// slab keys grow from 0 so neither reserved value can collide with a connection
/// token. mio allows exactly ONE active `Waker` per `Poll`, so this single waker
/// serves BOTH wake sources — the broadcast sink nudging a drain, and a
/// cross-connection [`Mailbox::send`](crate::connection::handle::Mailbox) marking a
/// connection dirty (cluster follow-up, `send_to_user`, `notify_watchers`, …). A
/// wake on this token only unblocks the poll; the post-loop broadcast + selective
/// mailbox drains then run and figure out what actually needs delivering.
const WORKER_WAKER: Token = Token(usize::MAX - 1);

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
    /// Maximum connection age in seconds: once a session has been established
    /// for this long the worker closes the connection with code **4202**
    /// ("Closed after inactivity", Pusher's 24h maximum connection lifetime —
    /// reconnect-immediately band). Armed ONCE per session (absolute deadline in
    /// the [`TimerWheel`]); activity never pushes it out. `0` disables.
    pub max_conn_lifetime_secs: u64,
    pub strict_protocol: bool,
    /// Per-app live connection counters (shared with the rest of the server),
    /// mirroring `AppState::conn_counts`.
    pub conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
    /// Node-level live connection counter, shared across this node's workers for
    /// the connection-ceiling check; injectable so tests get an independent counter
    /// per harness (avoiding cross-test pollution from the old process-global static).
    pub node_conns: Arc<AtomicUsize>,
    pub webhooks: crate::webhook::WebhookHandle,
    /// SP10 admission control: the shared percore saturation flag. Stamped onto
    /// each connection's [`ConnectionContext`] at session establish so a WS
    /// `client-*` event is dropped at ingress under saturation. `None` when no
    /// sink is wired (e.g. the redis+percore fallback), so the drop never fires.
    pub saturated: Option<crate::transport::fanout::SaturationFlag>,
    /// SP11 §3.6: clustering toggle stamped onto every connection's
    /// [`ConnectionContext`] at session establish. `true` ⇒ this is a clustered
    /// percore node: the single-emit cluster edges (`subscription_count`,
    /// `channel_occupied` / `channel_vacated`) are deferred to the bridge, so the
    /// connection handler suppresses its node-local emits. `false` ⇒ the
    /// not-yet-clustered percore path keeps the node-local handler emits.
    pub clustered: bool,
    /// The node's cluster bridge handle, for CLUSTER-WIDE
    /// per-app capacity admission. `Some` only on a clustered node (the same
    /// wiring that sets [`clustered`](DispatchEnv::clustered) = `true`); `None` ⇒
    /// no cluster admission — the per-app `capacity` check below stays purely
    /// node-local (single-node percore and the test workers).
    pub cluster: Option<crate::cluster::bridge::ClusterHandle>,
    /// Node-level connection ceiling enforced before the per-app `capacity` check.
    /// `0` = unlimited (no ceiling check). When non-zero, the connection that
    /// pushes [`node_conns`](DispatchEnv::node_conns) to or above this limit is
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
    /// Phase 7: the tokio runtime handle captured in [`crate::transport::run_percore`] BEFORE the
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
    /// Maximum accepted size (bytes) of one REASSEMBLED inbound text message
    /// (the sum of its RFC 6455 §5.4 fragments). Plumbed from
    /// `max_event_payload_bytes` — the same per-message budget the protocol
    /// layer holds unfragmented events to. It bounds the accumulator from the
    /// FIRST fragment on: a message over the cap is dropped WITHOUT closing the
    /// connection, and its remaining fragments are swallowed without being
    /// buffered.
    pub max_message_bytes: usize,
    /// G3 (slowloris): maximum accepted HTTP request-head size (bytes) — the
    /// window `inbuf` may grow to while the head is incomplete. A head larger
    /// than this is [`HeadResult::Bad`] and the connection closes, so a client
    /// dribbling headerless bytes cannot grow the buffer (and the slab slot)
    /// without bound. `0` disables the cap. Plumbed from
    /// `ServerConfig::max_head_bytes` (`PYLON_MAX_HEAD_BYTES`, default 16384).
    pub max_head_bytes: usize,
    /// G3 (slowloris): absolute deadline (ms) from ACCEPT for a connection to
    /// complete its handshake (head + TLS + WS upgrade + session establish).
    /// On expiry the pre-session connection is closed and its slot reclaimed;
    /// inbound dribble does NOT postpone it. `0` disables the deadline. Plumbed
    /// from `ServerConfig::handshake_timeout_ms`
    /// (`PYLON_HANDSHAKE_TIMEOUT_MS`, default 10000).
    pub handshake_timeout_ms: u64,
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
    /// G8: cumulative drop-head-evicted-frames counter for this worker. Updated
    /// at the same fold sites as `codel_dropped_slot` by folding
    /// `conn.take_drophead_dropped()`; `None` for test workers. Under overload
    /// drop-head is a PRIMARY frame-loss mechanism, so it must be observable.
    pub drophead_dropped_slot: Option<Arc<AtomicU64>>,
    /// Task 4: cumulative mailbox-full-drop counter for this worker. Incremented
    /// inside each connection's [`crate::connection::handle::Mailbox::send`] on a `try_send` full-error.
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
    /// in `slot.tx`). Drained on the `WORKER_WAKER` event and once per loop.
    pub rx: std::sync::mpsc::Receiver<crate::transport::fanout::BroadcastMsg>,
    /// This worker's sink slot; its `waker` `OnceLock` is filled at startup with
    /// a `Waker` built from this worker's own `Poll` registry.
    pub slot: Arc<crate::transport::fanout::WorkerSlot>,
    /// The sink-shared saturation flag. After this worker fully drains its
    /// broadcast inbox to empty (so the bounded hand-off has headroom again), it
    /// releases the inbox-full half, letting the publish-admission path resume
    /// accepting. The budget half lives in `slot`, owned by this worker.
    pub saturated: crate::transport::fanout::SaturationFlag,
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
    /// Whether the cluster-wide per-app capacity gate actually took a Redis unit
    /// for THIS connection, so close releases exactly what establish took. An
    /// admission that failed open (`admit_app` → `None`) took none, and the
    /// release script's node guard cannot tell that apart on a node holding other
    /// units for the app — it would give back a sibling connection's unit.
    cluster_admitted: bool,
}

/// RFC 6455 §5.4: the in-progress fragmented message on this connection.
///
/// `Text` accumulates a fragmented TEXT message (the only data kind the Pusher
/// protocol carries) for dispatch on its FIN=1 Continuation. `Binary` and
/// `Oversize` are ignore-modes: their remaining frames are swallowed until
/// FIN=1 completes the message. Every variant keeps the message OPEN, so its
/// Continuations are never mistaken for strays (§5.4 fails the connection on a
/// stray Continuation).
enum Fragment {
    Text(Vec<u8>),
    /// Binary is outside the Pusher protocol; a lone Binary frame is ignored,
    /// so a fragmented one is ignored frame by frame.
    Binary,
    /// A TEXT message already over `max_message_bytes` — dropped without
    /// closing the connection, and without holding the bytes seen so far.
    Oversize,
}

/// Per-connection slab entry: the [`Connection`] plus its read remainder and,
/// for dispatch workers, the v7 [`Session`] built at handshake completion.
///
/// `inbuf` holds only bytes that arrived mid-frame, except during
/// [`ConnState::Handshaking`], where it doubles as the head-accumulation
/// buffer. That phase is bounded by `WorkerConfig::max_head_bytes` and the
/// handshake deadline and is released at upgrade, so it goes unbilled; once
/// open, its only bound is `WorkerConfig::max_payload` and [`handle_frames`]
/// bills what it holds through `set_frame_buffer_bytes`.
struct Entry {
    conn: Connection,
    inbuf: BytesMut,
    /// The [`Token`] this connection is registered under (== `Token(slab_key)`).
    token: Token,
    /// v7 protocol state; `None` for echo workers and pre-handshake connections.
    session: Option<Session>,
    /// RFC 6455 §5.4: in-progress fragmented message, `Some` once a FIN=0 Text
    /// or Binary frame has opened one (see [`Fragment`]). Mutate it only through
    /// [`Entry::set_fragment`] / [`Entry::take_fragment`], which keep the bytes
    /// it holds billed to `conn`'s inflight accounting.
    fragment: Option<Fragment>,
    /// Phase 7: set while this connection is PARKED waiting on an offloaded app
    /// lookup (L1 miss). `Open` + `session: None` + `pending_establish: Some(..)`
    /// is the park state. Cleared (`take`) when its `ResolvedApp` arrives, or
    /// dropped wholesale when the connection closes mid-park (leaking nothing —
    /// no counter was taken). A park holds the slab slot but no app resources.
    pending_establish: Option<PendingEstablish>,
}

impl Entry {
    /// Install the connection's fragmentation state, re-billing whatever the new
    /// state holds to the connection's inflight accounting.
    fn set_fragment(&mut self, fragment: Option<Fragment>) {
        self.conn.set_reassembly_bytes(match &fragment {
            Some(Fragment::Text(buf)) => buf.len(),
            _ => 0,
        });
        self.fragment = fragment;
    }

    /// Take the fragmentation state, releasing its bytes from the accounting.
    fn take_fragment(&mut self) -> Option<Fragment> {
        self.conn.set_reassembly_bytes(0);
        self.fragment.take()
    }
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
    result: Result<crate::app::AppLookup, crate::app::AppLookupError>,
}

/// Build a `mio` listener bound to `addr` with `SO_REUSEADDR` + `SO_REUSEPORT`
/// set before bind. `SO_REUSEPORT` lets every per-core worker bind the SAME
/// `bind:port` independently; the kernel then load-balances incoming connections
/// across the workers' listener sockets (one accept queue per worker).
fn reuseport_listener(addr: std::net::SocketAddr) -> std::io::Result<TcpListener> {
    // `SO_REUSEADDR` covers TIME_WAIT, but a fast restart racing the previous
    // instance's teardown can still hold the port for a moment, so bind gets a
    // bounded retry. A socket whose bind failed cannot rebind: build a fresh
    // one per attempt.
    let mut last_err: Option<std::io::Error> = None;
    for _ in 0..10 {
        let sock = socket2::Socket::new(
            socket2::Domain::for_address(addr),
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )?;
        sock.set_reuse_address(true)?;
        sock.set_reuse_port(true)?; // SO_REUSEPORT — kernel load-balances accepts across workers
        sock.set_nonblocking(true)?;
        match sock.bind(&addr.into()) {
            Ok(()) => {
                sock.listen(1024)?;
                return Ok(TcpListener::from_std(std::net::TcpListener::from(sock)));
            }
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
                last_err = Some(e);
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            "address in use after retries",
        )
    }))
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

    // mio allows exactly one active `Waker` per `Poll`, so the broadcast sink
    // and the selective mailbox drain share this one.
    let worker_waker = Arc::new(mio::Waker::new(poll.registry(), WORKER_WAKER)?);

    // The `Receiver` is not `Sync`, so the broadcast wiring moves out of `cfg`.
    // `None` ⇒ no broadcast inbox: broadcasts route via the registry mailbox
    // path and are drained by `drain_dirty_sessions`.
    let broadcast = cfg.broadcast.take();
    if let Some(w) = &broadcast {
        let _ = w.slot.waker.set(worker_waker.clone());
    }

    // Every cross-connection delivery routes through `Mailbox::send`, which
    // pushes the target's slab token here and wakes the worker, so the drain
    // is O(dirty) rather than O(N). Echo workers never stamp a notifier, so
    // their `dirty_rx` stays empty.
    let (dirty_tx, dirty_rx) = std::sync::mpsc::channel::<usize>();
    let mailbox_waker = worker_waker;
    // Reused dirty-token set: drained from `dirty_rx` each iteration and deduped
    // (a connection may be marked dirty several times before we drain it).
    let mut dirty_set: HashSet<usize> = HashSet::new();

    // Parked (L1-miss) establishes resume here: an offloaded tokio task pushes
    // a `ResolvedApp` and wakes the worker. Unbounded and worker-lifetime, so
    // `send` never fails.
    let (resolved_tx, resolved_rx) = std::sync::mpsc::channel::<ResolvedApp>();
    // Monotonic park generation, bumped once per park. Guards slab-token reuse so
    // a late resolution for a freed/recycled token is detected and dropped.
    let mut next_gen: u64 = 0;

    // SP10 per-worker byte budget: `Connection::accounted_bytes` summed over
    // this worker's connections, maintained incrementally — every site folds in
    // that connection's signed `take_inflight_delta()` and every teardown
    // subtracts what it still held, so the hot loop stays O(work).
    let per_worker_budget = cfg.per_worker_budget;
    let inflight_slot = cfg.inflight_slot.clone();
    let accepted_slot = cfg.accepted_slot.clone();
    let codel_dropped_slot = cfg.codel_dropped_slot.clone();
    let drophead_dropped_slot = cfg.drophead_dropped_slot.clone();
    // SP10 §8: shared PSI budget factor (×1000 fixed-point). `None` ⇒ no backstop.
    let budget_factor = cfg.budget_factor.clone();
    // SP10 §7: CoDel parameters stamped onto every accepted connection.
    let codel = cfg.codel;
    let mut inflight_bytes: u64 = 0;
    // Queued out-frames plus unflushed TLS plaintext only. The shutdown drain
    // reads this rather than `inflight_bytes`, whose inbound buffers never
    // reach zero once a peer stops sending.
    let mut outbound_bytes: u64 = 0;
    let mut codel_dropped_total: u64 = 0;
    let mut drophead_dropped_total: u64 = 0;

    let mut local_subs: LocalSubs = HashMap::new();

    // SP11 §4: idle-ping a silent connection after `activity_timeout` and close
    // it 4201 if no pong follows within `pong_timeout`. Only dispatch workers
    // enter connections into it.
    let (mut wheel, liveness) = match &cfg.mode {
        Mode::Dispatch(env) => (
            TimerWheel::with_timeouts(env.activity_timeout, env.pong_timeout),
            true,
        ),
        Mode::Echo => (TimerWheel::with_timeouts(0, 0), false),
    };

    // Per-app shared maps the close path reclaims; echo workers have no env,
    // so they get throwaways.
    #[allow(clippy::type_complexity)]
    let (conn_counts, app_registry, node_conns): (
        Arc<DashMap<String, Arc<AtomicUsize>>>,
        Arc<AppRegistry>,
        Arc<AtomicUsize>,
    ) = match &cfg.mode {
        Mode::Dispatch(env) => (
            env.conn_counts.clone(),
            env.app_registry.clone(),
            env.node_conns.clone(),
        ),
        Mode::Echo => (
            Arc::new(DashMap::new()),
            Arc::new(AppRegistry::new()),
            Arc::new(AtomicUsize::new(0)),
        ),
    };

    // The close-side capacity release; `None` off a clustered dispatch worker.
    let cluster: Option<crate::cluster::bridge::ClusterHandle> = match &cfg.mode {
        Mode::Dispatch(env) => env.cluster.clone(),
        Mode::Echo => None,
    };

    let mut events = Events::with_capacity(1024);
    let mut conns: slab::Slab<Entry> = slab::Slab::new();

    // Adaptive poll timeout (G1) — see the in-loop comment at the poll call.
    let mut did_work = true;
    let dispatch = matches!(cfg.mode, Mode::Dispatch(_));
    // Logged at shutdown so an operator can confirm `SO_REUSEPORT` spread
    // accepts across cores.
    let mut accepted_total: u64 = 0;

    // Monotonic epoch for CoDel enqueue timestamps: one `now_ns` per loop
    // iteration, threaded into every `queue`/`flush`, so a frame's sojourn is
    // the real time it spent queued across iterations.
    let worker_epoch = Instant::now();

    // C2a graceful-drain state: `drain_started` gates the one-time setup,
    // `drain_deadline` is when the drain force-closes regardless.
    let mut drain_started = false;
    let mut drain_deadline: Option<Instant> = None;

    loop {
        // Relaxed is sound: the flag carries no payload, and everything after
        // the run loop exits is synchronized by the supervisor's thread join.
        // The 50ms poll bound below re-reads it at least once per idle cycle.
        if shutdown.load(Ordering::Relaxed) {
            // C2a drain phase — runs only on the shutdown path, zero cost otherwise.
            let now_ns = worker_epoch.elapsed().as_nanos() as u64;
            if !drain_started {
                drain_started = true;
                drain_deadline = if cfg.shutdown_grace_ms > 0 {
                    Some(Instant::now() + Duration::from_millis(cfg.shutdown_grace_ms))
                } else {
                    None // grace_ms == 0 ⇒ immediate exit (old behaviour)
                };
                // Stop accepting; the waker registration stays so the loop
                // keeps flushing.
                let _ = poll.registry().deregister(&mut listener);
                // Keys first, to avoid aliasing `conns` while iterating.
                let keys: Vec<usize> = conns.iter().map(|(k, _)| k).collect();
                for k in keys {
                    drain_close_connection(
                        &poll,
                        &mut conns,
                        k,
                        now_ns,
                        &mut local_subs,
                        &mut wheel,
                        &mut inflight_bytes,
                        &mut outbound_bytes,
                        &mut codel_dropped_total,
                        &mut drophead_dropped_total,
                        &conn_counts,
                        &app_registry,
                        &node_conns,
                        &cluster,
                    );
                }
                tracing::info!(
                    worker = cfg.worker_id,
                    conns = conns.len(),
                    "percore worker draining"
                );
            }
            // Deliberately `outbound_bytes`, not `inflight_bytes`: a
            // connection mid-frame pins the latter above zero forever once the
            // peer stops sending, burning the whole grace window every time.
            let expired = drain_deadline.is_none_or(|d| Instant::now() >= d);
            if outbound_bytes == 0 || expired {
                // on_close hooks, counter decrements, channel deindex and
                // socket deregistration, so per-app state returns to 0.
                let keys: Vec<usize> = conns.iter().map(|(k, _)| k).collect();
                for k in keys {
                    remove(
                        &poll,
                        &mut conns,
                        k,
                        &mut local_subs,
                        &mut wheel,
                        &mut inflight_bytes,
                        &mut outbound_bytes,
                        &conn_counts,
                        &app_registry,
                        &node_conns,
                        &cluster,
                    );
                }
                tracing::debug!(
                    worker = cfg.worker_id,
                    accepted = accepted_total,
                    "percore worker drained, stopping"
                );
                return Ok(());
            }
            // Otherwise fall through and let the loop flush the queued Close
            // frames: every still-backpressured connection has WRITABLE armed,
            // so the poll wakes as soon as one drains.
        }

        // A missed delta site shows up here rather than as a slow drift in the
        // overload signal.
        debug_assert_eq!(
            inflight_bytes,
            conns
                .iter()
                .map(|(_, e)| e.conn.accounted_bytes() as u64)
                .sum::<u64>(),
            "incremental inflight_bytes drifted from the true accounted_bytes sum",
        );
        debug_assert_eq!(
            outbound_bytes,
            conns
                .iter()
                .map(|(_, e)| e.conn.outbound_bytes() as u64)
                .sum::<u64>(),
            "incremental outbound_bytes drifted from the true outbound_bytes sum",
        );

        // G1 invariant: owed outbound bytes — queued frames or ciphertext
        // rustls has not put on the wire — MUST come with WRITABLE interest,
        // or the idle 50ms poll can sleep on a backlog nothing will wake it
        // for. Every write site goes through `flush_and_arm` before control
        // returns here, so a violation means one let an unarmed connection
        // live.
        debug_assert!(
            conns
                .iter()
                .all(|(_, e)| !e.conn.has_pending_writes() || e.conn.writable_armed()),
            "connection owes outbound bytes but has no WRITABLE interest armed; \
             the idle poll could strand them"
        );

        if let Some(slot) = &inflight_slot {
            slot.store(inflight_bytes, Ordering::Relaxed);
        }
        if codel_dropped_total > 0 {
            if let Some(slot) = &codel_dropped_slot {
                slot.store(codel_dropped_total, Ordering::Relaxed);
            }
        }
        if drophead_dropped_total > 0 {
            if let Some(slot) = &drophead_dropped_slot {
                slot.store(drophead_dropped_total, Ordering::Relaxed);
            }
        }
        // Poll non-blocking only when the previous iteration did real work;
        // otherwise block up to 50ms. Queued out-bytes deliberately do NOT
        // force a 0ms poll: a backpressured connection produces no readiness
        // event, so polling on `inflight_bytes > 0` would busy-spin the core.
        // Safe because such a connection holds WRITABLE interest (asserted at
        // the loop top) and the kernel wakes the loop when the socket drains.
        let timeout = if did_work {
            #[cfg(any(test, feature = "test-hooks"))]
            POLL_ZERO_TIMEOUTS.fetch_add(1, Ordering::Relaxed);
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

        let now_ns = worker_epoch.elapsed().as_nanos() as u64;
        let now_ms = now_ns / 1_000_000;

        // `per_worker_budget` scaled by the shared PSI factor (×1000
        // fixed-point; 1000 = full), read once per iteration.
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
                    // G3 (slowloris): absolute from accept, on the wheel's
                    // separate handshake side table so inbound dribble cannot
                    // postpone it.
                    let handshake_deadline = if liveness && cfg.handshake_timeout_ms > 0 {
                        Some(now_ms.saturating_add(cfg.handshake_timeout_ms))
                    } else {
                        None
                    };
                    let n = accept_ready(
                        &poll,
                        &mut listener,
                        &mut conns,
                        &cfg,
                        codel,
                        &mut wheel,
                        handshake_deadline,
                    );
                    accepted_total += n;
                    if n > 0 {
                        if let Some(slot) = &accepted_slot {
                            slot.fetch_add(n, Ordering::Relaxed);
                        }
                    }
                }
                // The waker only unblocks the poll; the work it announces was
                // already queued by its source, and the post-loop drains do it.
                WORKER_WAKER => {}
                token => {
                    let key = token.0;
                    // The connection may have been removed earlier in this same
                    // event batch; skip stale tokens.
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
                            &mut wheel,
                            &mut inflight_bytes,
                            &mut outbound_bytes,
                            &conn_counts,
                            &app_registry,
                            &node_conns,
                            &cluster,
                        );
                        continue;
                    }

                    if event.is_readable() {
                        // Inbound bytes are activity: reset the idle deadline
                        // and cancel any pending pong-timeout close.
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
                            &mut wheel,
                        ) {
                            Action::Close => {
                                // Fold the drop counters before teardown, or
                                // they die with the slab entry.
                                fold_codel(&mut conns, key, &mut codel_dropped_total);
                                fold_drophead(&mut conns, key, &mut drophead_dropped_total);
                                remove(
                                    &poll,
                                    &mut conns,
                                    key,
                                    &mut local_subs,
                                    &mut wheel,
                                    &mut inflight_bytes,
                                    &mut outbound_bytes,
                                    &conn_counts,
                                    &app_registry,
                                    &node_conns,
                                    &cluster,
                                );
                                continue;
                            }
                            Action::Handoff(prefix) => {
                                // A REST handoff is not a WS session; drop its
                                // (spurious) wheel entry so it can't fire later.
                                fold_codel(&mut conns, key, &mut codel_dropped_total);
                                fold_drophead(&mut conns, key, &mut drophead_dropped_total);
                                wheel.remove(key);
                                handoff_rest(
                                    &poll,
                                    &mut conns,
                                    key,
                                    &cfg,
                                    prefix,
                                    &mut inflight_bytes,
                                    &mut outbound_bytes,
                                );
                                continue;
                            }
                            Action::Keep => {
                                fold_delta(
                                    &mut conns,
                                    key,
                                    &mut inflight_bytes,
                                    &mut outbound_bytes,
                                );
                                fold_codel(&mut conns, key, &mut codel_dropped_total);
                                fold_drophead(&mut conns, key, &mut drophead_dropped_total);
                                // A subscribe/unsubscribe in this batch may have
                                // changed channel membership.
                                if let Some(entry) = conns.get_mut(key) {
                                    if let Some(session) = entry.session.as_mut() {
                                        reconcile_membership(session, key, &mut local_subs);
                                    }
                                }
                            }
                        }
                    }

                    if event.is_writable() && conns.contains(key) {
                        let action = handle_writable(
                            &poll,
                            &mut conns,
                            key,
                            &cfg,
                            now_ns,
                            &dirty_tx,
                            &mailbox_waker,
                            &resolved_tx,
                            &mut next_gen,
                            &mut wheel,
                        );
                        // Fold the flush's delta before any close or handoff.
                        fold_delta(&mut conns, key, &mut inflight_bytes, &mut outbound_bytes);
                        fold_codel(&mut conns, key, &mut codel_dropped_total);
                        fold_drophead(&mut conns, key, &mut drophead_dropped_total);
                        match action {
                            Action::Close => {
                                remove(
                                    &poll,
                                    &mut conns,
                                    key,
                                    &mut local_subs,
                                    &mut wheel,
                                    &mut inflight_bytes,
                                    &mut outbound_bytes,
                                    &conn_counts,
                                    &app_registry,
                                    &node_conns,
                                    &cluster,
                                );
                            }
                            // A TLS handshake completing on the writable path
                            // can yield a REST head just as the readable path
                            // does: its plaintext waited behind the flight.
                            Action::Handoff(prefix) => {
                                wheel.remove(key);
                                handoff_rest(
                                    &poll,
                                    &mut conns,
                                    key,
                                    &cfg,
                                    prefix,
                                    &mut inflight_bytes,
                                    &mut outbound_bytes,
                                );
                            }
                            Action::Keep => {
                                if let Some(entry) = conns.get_mut(key) {
                                    if let Some(session) = entry.session.as_mut() {
                                        reconcile_membership(session, key, &mut local_subs);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Deliver each already-framed payload to its local subscribers by
        // direct slab-enqueue. Run unconditionally as a safety net for the
        // loaded case where no waker event fires; a no-op on an empty inbox.
        if let Some(wiring) = &broadcast {
            if drain_broadcasts(
                &poll,
                &mut conns,
                &wiring.rx,
                &mut local_subs,
                &mut wheel,
                effective_budget,
                &mut inflight_bytes,
                &mut outbound_bytes,
                &mut codel_dropped_total,
                &mut drophead_dropped_total,
                Some(&wiring.slot.budget_saturated),
                now_ns,
                &conn_counts,
                &app_registry,
                &node_conns,
                &cluster,
            ) {
                work = true;
            }
            if let Some(slot) = &inflight_slot {
                slot.store(inflight_bytes, Ordering::Relaxed);
            }
            // `drain_broadcasts` ran the inbox to `Empty`, so it has headroom.
            wiring.saturated.clear_inbox_full();
            update_budget_pressure(
                &wiring.slot.budget_saturated,
                inflight_bytes,
                effective_budget,
            );
        }

        // A direct mailbox send has no readiness event of its own, so
        // `Mailbox::send` marked its target's token dirty. Drain only those
        // connections; idle ones are never visited.
        if dispatch
            && drain_dirty_sessions(
                &poll,
                &mut conns,
                &dirty_rx,
                &mut dirty_set,
                &mut local_subs,
                &mut wheel,
                &mut inflight_bytes,
                &mut outbound_bytes,
                &mut codel_dropped_total,
                &mut drophead_dropped_total,
                now_ns,
                &conn_counts,
                &app_registry,
                &node_conns,
                &cluster,
            )
        {
            work = true;
        }

        // Resume (or reject) each parked connection whose offloaded app lookup
        // completed; a no-op when nothing is parked.
        if dispatch {
            if let Mode::Dispatch(env) = &cfg.mode {
                if drain_resolved(
                    &poll,
                    &mut conns,
                    &resolved_rx,
                    env,
                    &mut local_subs,
                    &mut wheel,
                    &mut inflight_bytes,
                    &mut outbound_bytes,
                    &mut codel_dropped_total,
                    &mut drophead_dropped_total,
                    now_ns,
                    &conn_counts,
                    &app_registry,
                    &node_conns,
                    &cluster,
                ) {
                    work = true;
                }
            }
        }

        // Liveness timers due this iteration. The wheel only visits expired
        // tokens, and the adaptive poll's 50ms sleep is negligible against the
        // 120s/30s timeouts it schedules.
        if liveness {
            for due in wheel.due(now_ms) {
                match due {
                    Due::Ping(key) => {
                        match queue_ping(&poll, &mut conns, key, now_ns) {
                            Some(action) => {
                                fold_delta(
                                    &mut conns,
                                    key,
                                    &mut inflight_bytes,
                                    &mut outbound_bytes,
                                );
                                if action == Action::Close {
                                    // The queued ping would otherwise sit
                                    // behind a poll interest that never fires
                                    // for a dead socket.
                                    fold_codel(&mut conns, key, &mut codel_dropped_total);
                                    fold_drophead(&mut conns, key, &mut drophead_dropped_total);
                                    remove(
                                        &poll,
                                        &mut conns,
                                        key,
                                        &mut local_subs,
                                        &mut wheel,
                                        &mut inflight_bytes,
                                        &mut outbound_bytes,
                                        &conn_counts,
                                        &app_registry,
                                        &node_conns,
                                        &cluster,
                                    );
                                } else {
                                    wheel.mark_ping_sent(key, now_ms);
                                }
                                work = true;
                            }
                            None => {
                                // Drop the liveness timer but keep any armed
                                // absolute deadline: a pre-session connection
                                // whose spurious idle timer fired here must
                                // still be reaped by the handshake deadline.
                                wheel.clear_liveness(key);
                            }
                        }
                    }
                    Due::Close4201(key) => {
                        send_close_4201(&poll, &mut conns, key, now_ns);
                        fold_delta(&mut conns, key, &mut inflight_bytes, &mut outbound_bytes);
                        fold_codel(&mut conns, key, &mut codel_dropped_total);
                        fold_drophead(&mut conns, key, &mut drophead_dropped_total);
                        remove(
                            &poll,
                            &mut conns,
                            key,
                            &mut local_subs,
                            &mut wheel,
                            &mut inflight_bytes,
                            &mut outbound_bytes,
                            &conn_counts,
                            &app_registry,
                            &node_conns,
                            &cluster,
                        );
                        work = true;
                    }
                    Due::Close4202(key) => {
                        // Max lifetime reached: in-band `pusher:error` 4202
                        // first, then the WS Close 4202.
                        queue_lifetime_error(&mut conns, key, now_ns);
                        send_close_4202(&poll, &mut conns, key, now_ns);
                        fold_delta(&mut conns, key, &mut inflight_bytes, &mut outbound_bytes);
                        fold_codel(&mut conns, key, &mut codel_dropped_total);
                        fold_drophead(&mut conns, key, &mut drophead_dropped_total);
                        remove(
                            &poll,
                            &mut conns,
                            key,
                            &mut local_subs,
                            &mut wheel,
                            &mut inflight_bytes,
                            &mut outbound_bytes,
                            &conn_counts,
                            &app_registry,
                            &node_conns,
                            &cluster,
                        );
                        work = true;
                    }
                    Due::HandshakeTimeout(key) => {
                        // G3 (slowloris) reap. No session exists, so there is
                        // no protocol close to emit and no counter to unwind:
                        // both `node_conns` and `conn_counts` are taken in
                        // `finish_establish`, paired with `session = Some(..)`.
                        let pre_session =
                            conns.get(key).is_some_and(|entry| entry.session.is_none());
                        if pre_session {
                            remove(
                                &poll,
                                &mut conns,
                                key,
                                &mut local_subs,
                                &mut wheel,
                                &mut inflight_bytes,
                                &mut outbound_bytes,
                                &conn_counts,
                                &app_registry,
                                &node_conns,
                                &cluster,
                            );
                        } else {
                            // The wheel's eager scrub means nothing should be
                            // armed here; the clear is a no-op if so.
                            wheel.clear_handshake(key);
                        }
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
/// frame. Returns `Some(action)` when the ping was queued (the connection
/// exists, is Open, and has a session) — `action` is the flush outcome, so a
/// dead-peer write failure is reported as [`Action::Close`] and the caller
/// reaps the connection instead of stranding the queued ping behind a poll
/// interest that will never fire for a dead socket. Returns `None` otherwise
/// (caller drops the wheel entry). A flush that backpressures arms writable
/// interest, so the ping rides the next writable event.
fn queue_ping(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    key: usize,
    now_ns: u64,
) -> Option<Action> {
    let entry = conns.get_mut(key)?;
    let session = entry.session.as_mut()?;
    if entry.conn.state != ConnState::Open {
        return None;
    }
    // F6/6.4: encode through the append seam — no per-frame String clone for
    // pre-encoded (`Raw`) payloads; the text then feeds the WS frame build.
    let mut text = String::new();
    session.codec.encode_into(&ServerEvent::Ping, &mut text);
    let mut out = BytesMut::new();
    frame::encode_text(&mut out, text.as_bytes());
    let _ = entry.conn.queue(out.freeze(), now_ns);
    Some(flush_and_arm(poll, entry, now_ns))
}

/// Queue a single WebSocket Close frame (`code` + `reason`) onto `entry`'s
/// out-queue (no flush). Shared core of every outbound Close frame: the
/// server-initiated closes ([`send_close_reply`], [`close_fragment_violation`],
/// [`close_invalid_utf8`], the drain path) and the RFC 6455 §5.5.1 echo of a
/// client-initiated Close.
fn queue_close_frame(entry: &mut Entry, code: u16, reason: &str, now_ns: u64) {
    let mut frame_body = Vec::with_capacity(2 + reason.len());
    frame_body.extend_from_slice(&code.to_be_bytes());
    frame_body.extend_from_slice(reason.as_bytes());
    let mut out = BytesMut::new();
    frame::encode(&mut out, true, OpCode::Close, &frame_body);
    let _ = entry.conn.queue(out.freeze(), now_ns);
}

/// Send a WebSocket Close frame with the given `code` and `reason` text —
/// queue it and flush so it actually reaches the peer — and report what the
/// flush wants done with the connection. The single generalized Close-reply
/// helper; [`send_close_4200`] and [`send_close_4201`] are thin callers.
///
/// A caller that is not already tearing the connection down MUST honour an
/// [`Action::Close`]: `flush_and_arm` returns it without arming WRITABLE, so
/// leaving the connection in the slab strands its queued bytes behind a poll
/// that will never wake for them.
fn send_close_reply(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    key: usize,
    code: u16,
    reason: &str,
    now_ns: u64,
) -> Action {
    let Some(entry) = conns.get_mut(key) else {
        return Action::Close;
    };
    queue_close_frame(entry, code, reason, now_ns);
    flush_and_arm(poll, entry, now_ns)
}

/// SP11 §4: send a WebSocket Close frame with code `4201` (pong-timeout) with the
/// canonical Pusher v7 reason text, then let the caller `remove` the connection.
fn send_close_4201(poll: &Poll, conns: &mut slab::Slab<Entry>, key: usize, now_ns: u64) {
    send_close_reply(
        poll,
        conns,
        key,
        4201,
        "Pong reply not received: ping was sent to the client, but no reply was received",
        now_ns,
    );
}

/// C2a drain: send a WebSocket Close frame with code `4200` (Pusher
/// "reconnect immediately") with the canonical shutdown reason text.
/// pusher-js reads 4200 and reconnects to the LB immediately — this minimises
/// client disruption on a rolling restart compared to the 1001 generic-gone-away.
fn send_close_4200(poll: &Poll, conns: &mut slab::Slab<Entry>, key: usize, now_ns: u64) -> Action {
    send_close_reply(
        poll,
        conns,
        key,
        4200,
        "Server is shutting down; please reconnect",
        now_ns,
    )
}

/// Max connection lifetime (Pusher parity, default 24h): send a WebSocket Close
/// frame with code `4202` — "Closed after inactivity", the reconnect-immediately
/// band. Message text is the exact close-code name from the Pusher protocol
/// page (see [`crate::protocol::error::PusherError::max_lifetime`]). The caller
/// is responsible for the subsequent `fold_delta` + `remove` (the `Due::Close4202`
/// arm queues the in-band `pusher:error` 4202 first via [`queue_lifetime_error`]).
fn send_close_4202(poll: &Poll, conns: &mut slab::Slab<Entry>, key: usize, now_ns: u64) {
    send_close_reply(
        poll,
        conns,
        key,
        4202,
        crate::protocol::error::PusherError::max_lifetime()
            .message
            .as_str(),
        now_ns,
    );
}

/// Max connection lifetime: queue a `pusher:error` 4202 text frame onto `key`'s
/// out-queue BEFORE the Close frame (the same belt-and-suspenders convention as
/// [`queue_shutdown_error`]): `{"event":"pusher:error","data":{"code":4202,
/// "message":"Closed after inactivity"}}`. No-op if the connection no longer
/// exists — the Close frame alone still carries the code for pusher-js.
fn queue_lifetime_error(conns: &mut slab::Slab<Entry>, key: usize, now_ns: u64) {
    let Some(entry) = conns.get_mut(key) else {
        return;
    };
    let error = crate::protocol::error::PusherError::max_lifetime();
    // A lifetime close only fires on an established session, but fall back to
    // the raw-JSON form for a conn whose session vanished between arming and
    // firing.
    let mut text = String::new();
    match entry.session.as_ref() {
        Some(session) => session
            .codec
            .encode_into(&ServerEvent::Error(error), &mut text),
        None => {
            text.push_str(
                &serde_json::json!({
                    "event": "pusher:error",
                    "data": { "code": error.code, "message": error.message }
                })
                .to_string(),
            );
        }
    }
    let mut out = BytesMut::new();
    frame::encode_text(&mut out, text.as_bytes());
    let _ = entry.conn.queue(out.freeze(), now_ns);
    // The caller queues the Close frame next; its `flush_and_arm` sends both.
}

/// C2a drain: queue a `pusher:error` 4200 text frame onto `key`'s out-queue
/// BEFORE the Close frame — Soketi's belt-and-suspenders convention (it does
/// `ws.end(4200)` after sending the `pusher:error` event). A no-op for a
/// connection that no longer exists; the Close frame's own 4200 is already
/// enough for pusher-js to reconnect immediately.
fn queue_shutdown_error(conns: &mut slab::Slab<Entry>, key: usize, now_ns: u64) {
    let Some(entry) = conns.get_mut(key) else {
        return;
    };
    let mut text = String::new();
    match entry.session.as_ref() {
        Some(session) => session.codec.encode_into(
            &ServerEvent::Error(crate::protocol::error::PusherError::new(
                4200,
                "Server is shutting down; please reconnect",
            )),
            &mut text,
        ),
        None => {
            // No codec negotiated yet: hand-build what the v7 one would have
            // produced, matching `queue_reject`.
            text.push_str(
                &serde_json::json!({
                    "event": "pusher:error",
                    "data": { "code": 4200_u16, "message": "Server is shutting down; please reconnect" }
                })
                .to_string(),
            );
        }
    }
    let mut out = BytesMut::new();
    frame::encode_text(&mut out, text.as_bytes());
    let _ = entry.conn.queue(out.freeze(), now_ns);
    // The caller queues the Close frame next; its `flush_and_arm` sends both.
}

/// C2a drain, one connection: queue the `pusher:error` 4200 + WS Close(4200),
/// flush them, and fold this connection's counters into the worker totals. The
/// Close frame often will not flush synchronously (a backpressured client), so
/// the folded bytes are what makes the drain's `outbound_bytes == 0` exit wait
/// for them.
///
/// A peer that is already write-dead — very common on a rolling restart, where
/// the LB drains clients while the node is stopping — can never receive those
/// frames: the flush reports [`Action::Close`] and arms no WRITABLE interest.
/// Such a connection is removed here rather than left holding queued bytes that
/// nothing will ever send, which would pin `outbound_bytes` above zero for the
/// whole grace window (and trip the loop-top interest invariant in debug builds).
#[allow(clippy::too_many_arguments)]
fn drain_close_connection(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    key: usize,
    now_ns: u64,
    local_subs: &mut LocalSubs,
    wheel: &mut TimerWheel,
    inflight_bytes: &mut u64,
    outbound_bytes: &mut u64,
    codel_total: &mut u64,
    drophead_total: &mut u64,
    conn_counts: &Arc<DashMap<String, Arc<AtomicUsize>>>,
    app_registry: &Arc<AppRegistry>,
    node_conns: &Arc<AtomicUsize>,
    cluster: &Option<crate::cluster::bridge::ClusterHandle>,
) {
    queue_shutdown_error(conns, key, now_ns);
    let action = send_close_4200(poll, conns, key, now_ns);
    fold_delta(conns, key, inflight_bytes, outbound_bytes);
    fold_codel(conns, key, codel_total);
    fold_drophead(conns, key, drophead_total);
    if action == Action::Close {
        remove(
            poll,
            conns,
            key,
            local_subs,
            wheel,
            inflight_bytes,
            outbound_bytes,
            conn_counts,
            app_registry,
            node_conns,
            cluster,
        );
    }
}

/// Outcome of handling a connection event: keep it, close it, or hand it off to
/// the tokio/axum REST plane.
#[derive(Debug, PartialEq, Eq)]
enum Action {
    Keep,
    Close,
    /// A plain-HTTP request head was detected: transfer the connection to the
    /// REST handoff channel, carrying the bytes already read off the socket for
    /// the HTTP parser to replay.
    Handoff(Vec<u8>),
}

/// Drain the listener's accept backlog, registering every accepted socket.
/// Returns the number of connections accepted this call (for accept-distribution
/// accounting).
///
/// G3 (slowloris): every accepted connection is born with an ABSOLUTE
/// handshake deadline (`handshake_deadline`, computed by the caller from
/// accept time + `handshake_timeout_ms`), armed on `wheel`'s handshake side
/// table. A connection that never completes its head/TLS/WS handshake by then
/// is reaped by the `Due::HandshakeTimeout` arm of the liveness loop — the fd
/// and slab slot no longer leak. Inbound dribble arms only the liveness timer
/// (`touch`), never this deadline. `None` disables the arming.
fn accept_ready(
    poll: &Poll,
    listener: &mut TcpListener,
    conns: &mut slab::Slab<Entry>,
    cfg: &WorkerConfig,
    codel: crate::transport::conn::CodelParams,
    wheel: &mut TimerWheel,
    handshake_deadline: Option<u64>,
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
                    tracing::debug!(error = %e, "failed to register accepted socket");
                    continue;
                }
                // Without this a small frame queued right after a partial
                // write waits on the peer's delayed ACK — a 40ms Nagle stall.
                // Best-effort: a failed socket option must not break accept.
                let _ = stream.set_nodelay(true);
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
                    fragment: None,
                    pending_establish: None,
                });
                // G3: the slab key is the wheel's ConnId, so the deadline can
                // only be armed once the slot exists.
                if let Some(deadline_ms) = handshake_deadline {
                    wheel.arm_handshake(key, deadline_ms);
                }
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

/// Arm `key`'s max-connection-lifetime deadline in the wheel: ABSOLUTE at
/// `now_ms + max_conn_lifetime_secs` (Pusher closes connections at a maximum
/// age with code 4202). No-op when the lifetime is disabled (`0`). Called once
/// per session, at establish; [`TimerWheel::touch`] on later activity re-arms
/// only the liveness timer, never this deadline.
fn arm_lifetime(wheel: &mut TimerWheel, env: &DispatchEnv, key: usize, now_ms: u64) {
    if env.max_conn_lifetime_secs > 0 {
        let deadline_ms = now_ms.saturating_add(env.max_conn_lifetime_secs.saturating_mul(1000));
        wheel.arm_lifetime(key, deadline_ms);
    }
}

/// Handle a readable event: either advance the handshake or process frames.
///
/// `dirty_tx` + `mailbox_waker` are this worker's selective-drain notifier inputs;
/// on handshake completion they are stamped (with this connection's slab `key` as
/// the token) into the new session's `ctx.mailbox_notify`, so a later
/// cross-connection `Mailbox::send` marks this connection dirty and wakes the worker.
///
/// `wheel` is the worker's liveness wheel: at handshake completion the new
/// session's absolute max-connection-lifetime deadline is armed on it (the
/// idle deadline is armed by the caller's `touch` on every readable).
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
    wheel: &mut TimerWheel,
) -> Action {
    let entry = &mut conns[key];
    match entry.conn.state {
        ConnState::Handshaking => handle_handshake(
            poll,
            entry,
            key,
            cfg,
            now_ns,
            dirty_tx,
            mailbox_waker,
            resolved_tx,
            next_gen,
            wheel,
        ),
        ConnState::Open => handle_frames(poll, entry, cfg, now_ns),
    }
}

/// Accumulate request-head bytes and, once complete, complete the WS upgrade.
///
/// `key` is this connection's slab token; `dirty_tx` + `mailbox_waker` are the
/// worker's selective-drain notifier inputs, stamped (with `key`) into the new
/// session's `ctx.mailbox_notify` so cross-connection sends wake the worker.
/// `wheel` receives the new session's absolute max-connection-lifetime deadline
/// at establish (see [`arm_lifetime`]).
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
    wheel: &mut TimerWheel,
) -> Action {
    // G2: `NeedsWrite` means a TLS handshake flight blocked mid-write and no
    // plaintext was pulled, so it falls through to `NeedMore ⇒
    // arm_handshake_interest` below, which adds WRITABLE to finish the flight.
    match entry.conn.drain_head_bytes(&mut entry.inbuf) {
        DrainStatus::Closed => return Action::Close,
        DrainStatus::Ok | DrainStatus::NeedsWrite => {}
    }

    match handshake::read_head(&entry.inbuf, cfg.max_head_bytes) {
        // No complete head yet. Reconcile poll interest with the TLS flight
        // state (G2): arms WRITABLE when a flight write blocked, clears it
        // once the flight is done.
        HeadResult::NeedMore => arm_handshake_interest(poll, entry, now_ns),
        HeadResult::WsUpgrade { key: ws_key, path } => {
            let response = handshake::accept_response(&ws_key).into_boxed_slice();
            // Drop-head queue never rejects; the 101 response always enqueues.
            // (`Bytes::from(Box<[u8]>)` takes ownership — no copy.)
            let _ = entry.conn.queue(Bytes::from(response), now_ns);
            // A browser never sends data frames before the 101, so any bytes
            // after the head would be a protocol error anyway; clearing is safe.
            entry.inbuf.clear();
            entry.conn.state = ConnState::Open;

            if let Mode::Dispatch(env) = &cfg.mode {
                // So a cross-connection `Mailbox::send` marks this connection
                // dirty and wakes the worker's selective drain.
                let notify = MailboxNotify {
                    token: key,
                    dirty: dirty_tx.clone(),
                    waker: mailbox_waker.clone(),
                };
                use crate::protocol::error::PusherError;
                let (app_key, protocol, version) = parse_app_path(&path);
                let codec =
                    match negotiate(protocol.as_deref(), version.as_deref(), env.strict_protocol) {
                        Ok(c) => c,
                        Err(error) => {
                            queue_reject(entry, &Reject { error, codec: None }, now_ns);
                            let _ = flush_and_arm(poll, entry, now_ns);
                            return Action::Close;
                        }
                    };
                // 4005 "Path not found": the path must match the `/app/{key}`
                // shape (non-empty single-segment key) BEFORE any app lookup —
                // Pusher reserves 4001 for a well-formed path with an UNKNOWN
                // key (see the protocol's error-code table).
                let app_key = match app_key {
                    Some(key) => key,
                    None => {
                        queue_reject(
                            entry,
                            &Reject {
                                error: PusherError::path_not_found(),
                                codec: Some(codec),
                            },
                            now_ns,
                        );
                        let _ = flush_and_arm(poll, entry, now_ns);
                        return Action::Close;
                    }
                };
                let resolved = match env.apps.by_key_cached(&app_key) {
                    Some(Ok(crate::app::AppLookup::Found(app))) => {
                        finish_establish(env, app, codec, notify, cfg.mailbox_dropped_slot.clone())
                    }
                    // P13: a DISABLED key gets its dedicated doc close code —
                    // 4003 "Application disabled" (the protocol's close-code
                    // table); only an UNKNOWN key keeps 4001.
                    Some(Ok(crate::app::AppLookup::Disabled)) => Err(Reject {
                        error: PusherError::app_disabled(),
                        codec: Some(codec),
                    }),
                    Some(Ok(crate::app::AppLookup::NotFound)) => Err(Reject {
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
                    // L1 miss: park this one connection and offload the `by_key`
                    // lookup to tokio. No counter is taken until
                    // `finish_establish` at resume, so a drop while parked leaks
                    // nothing.
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
                        // The spawned task echoes `token` + `gen` back and wakes
                        // the worker; the send cannot fail.
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
                        // No establish frame yet: flush the 101 and keep the
                        // connection. It resumes in `drain_resolved`.
                        return flush_and_arm(poll, entry, now_ns);
                    }
                };
                match resolved {
                    Ok(session) => {
                        let established = ServerEvent::ConnectionEstablished {
                            socket_id: session.ctx.socket_id,
                            activity_timeout: env.activity_timeout,
                        };
                        let mut text = String::new();
                        session.codec.encode_into(&established, &mut text);
                        let mut out = BytesMut::new();
                        frame::encode_text(&mut out, text.as_bytes());
                        let _ = entry.conn.queue(out.freeze(), now_ns);
                        entry.session = Some(session);
                        // Clear the G3 handshake deadline so the
                        // activity-immune reap can never fire on a live session.
                        wheel.clear_handshake(key);
                        // The max-lifetime deadline is absolute from establish;
                        // activity never pushes it out.
                        arm_lifetime(wheel, env, key, now_ns / 1_000_000);
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
        // A plain-HTTP request: hand it to the tokio/axum plane. `inbuf` holds
        // every byte read so far — head plus any body that arrived with it — so
        // the whole buffer is the prefix to replay to the HTTP parser.
        HeadResult::Rest { .. } => {
            if cfg.rest_handoff.is_some() {
                Action::Handoff(entry.inbuf.to_vec())
            } else {
                Action::Close
            }
        }
        // More header fields than the parser can address. The head is
        // well-formed, so answer it rather than dropping it: RFC 6585 §5's 431
        // in the Pusher JSON error shape. A client that gets no status line at
        // all cannot tell this apart from a network fault.
        HeadResult::TooManyHeaders => {
            tracing::debug!(
                limit = handshake::MAX_HEADERS,
                head_bytes = entry.inbuf.len(),
                "rejecting request head with too many header fields (431)"
            );
            let response = handshake::header_fields_too_large_response().into_boxed_slice();
            // Drop-head queue never rejects; the 431 always enqueues.
            let _ = entry.conn.queue(Bytes::from(response), now_ns);
            let _ = flush_and_arm(poll, entry, now_ns);
            Action::Close
        }
        // Nothing to answer with, but say why so an operator can attribute the
        // close rather than reading it as a transport fault.
        HeadResult::Bad(reason) => {
            tracing::debug!(reason, "closing connection with a bad request head");
            Action::Close
        }
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
/// (`node_conns`, `conn_counts`) are incremented HERE and rolled back on every
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
    let node_n = env.node_conns.fetch_add(1, Ordering::SeqCst);
    if env.max_connections != 0 && node_n >= env.max_connections {
        env.node_conns.fetch_sub(1, Ordering::SeqCst);
        return Err(Reject {
            error: PusherError::server_over_capacity(),
            codec: Some(codec),
        });
    }

    // Memory-pressure admission gate. It runs after the node-ceiling
    // `fetch_add`, so it must release the count just taken.
    if env.saturated.as_ref().is_some_and(|s| s.is_saturated()) {
        env.node_conns.fetch_sub(1, Ordering::SeqCst);
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
        env.node_conns.fetch_sub(1, Ordering::SeqCst);
        return Err(Reject {
            error: PusherError::over_capacity(),
            codec: Some(codec),
        });
    }

    // The local check above only sees this node, so N nodes would each admit up
    // to `capacity`. This is the one place a worker deliberately blocks on the
    // bridge: the establish cannot proceed before the cluster-wide decision.
    let mut cluster_admitted = false;
    if app.capacity != 0 {
        if let Some(bridge) = env.cluster.as_ref() {
            match bridge.admit_app(&app.id, app.capacity) {
                Some(true) => cluster_admitted = true,
                Some(false) => {
                    // The same 4004 the local check sends. The Redis unit was
                    // not taken — the script rejects without incrementing.
                    counter.fetch_sub(1, Ordering::SeqCst);
                    env.node_conns.fetch_sub(1, Ordering::SeqCst);
                    return Err(Reject {
                        error: PusherError::over_capacity(),
                        codec: Some(codec),
                    });
                }
                // Fail open: a degraded bridge must not lock clients out of a
                // node whose local checks already passed. No unit was taken, so
                // this connection owes no release.
                None => {}
            }
        }
    }

    let socket_id = SocketId::generate();
    // `.max(1)` because `mpsc::channel(0)` panics and a direct `WorkerEnv`
    // struct literal could pass 0. The mailbox carries `Box<ServerEvent>`, not
    // `ServerEvent`: tokio eagerly allocates a 32-slot block per channel, so
    // boxing takes each idle connection's up-front cost from ~3.3 KB to ~0.5 KB.
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
        clustered: env.clustered,
        mailbox_notify: Some(notify),
        mailbox_dropped,
        // The single place the codec's capabilities reach the handler layer;
        // every version-feature gate reads this.
        capabilities: codec.capabilities(),
    };

    // Register the live connection under its app so a cluster-wide `purge_app`
    // can force-close it. Every rejection path above returned before `ctx` was
    // built, so a rejected connection never registers.
    env.app_registry.insert(&ctx.app.id, ctx.handle());

    Ok(Session {
        ctx,
        rx,
        codec,
        conn_count: counter,
        subs: HashSet::new(),
        cluster_admitted,
    })
}

/// Queue the pre-session rejection frames onto `entry`'s out-queue: first the
/// `pusher:error` Text frame (codec-encoded when a codec exists, else the raw
/// JSON fallback), then a WebSocket Close frame carrying the error `code` +
/// `message`. The caller flushes and closes.
fn queue_reject(entry: &mut Entry, reject: &Reject, now_ns: u64) {
    // The pusher:error Text frame.
    let mut text = String::new();
    match &reject.codec {
        Some(c) => c.encode_into(&ServerEvent::Error(reject.error.clone()), &mut text),
        None => {
            text.push_str(
                &serde_json::json!({
                    "event": "pusher:error",
                    "data": { "code": reject.error.code, "message": reject.error.message }
                })
                .to_string(),
            );
        }
    }
    let mut out = BytesMut::new();
    frame::encode_text(&mut out, text.as_bytes());
    let _ = entry.conn.queue(out.freeze(), now_ns);

    // The WS Close frame: code = the pusher error code, reason = its message.
    let reason = &reject.error.message;
    let mut frame_body = Vec::with_capacity(2 + reason.len());
    frame_body.extend_from_slice(&reject.error.code.to_be_bytes());
    frame_body.extend_from_slice(reason.as_bytes());
    let mut close_out = BytesMut::new();
    frame::encode(&mut close_out, true, OpCode::Close, &frame_body);
    let _ = entry.conn.queue(close_out.freeze(), now_ns);
}

/// Split a `/app/{key}` path (with an optional `?protocol=N&version=X&...`
/// query) into the app key, the `protocol` query value, and the `version`
/// query value.
///
/// The key is `None` when the path does not match the single-segment
/// `/app/{key}` shape — no `/app/` prefix, an empty key, or a multi-segment
/// key. Callers reject those with 4005 "Path not found" (Pusher parity);
/// 4001 stays reserved for a well-formed path with an unknown key.
fn parse_app_path(path: &str) -> (Option<String>, Option<String>, Option<String>) {
    let (raw_path, query) = match path.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (path, None),
    };
    let key = raw_path
        .strip_prefix("/app/")
        .filter(|k| !k.is_empty() && !k.contains('/'))
        .map(str::to_string);
    let protocol = query_param(query, "protocol");
    let version = query_param(query, "version");
    (key, protocol, version)
}

/// First value of `name` in a raw query string (`None` when absent).
fn query_param(query: Option<&str>, name: &str) -> Option<String> {
    query.and_then(|q| {
        q.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            (k == name).then(|| v.to_string())
        })
    })
}

/// Read and process every complete frame currently buffered, per [`Mode`].
///
/// The remainder `read_frames` leaves in `inbuf` is a partially-received frame,
/// which a peer can hold at `max_payload` by trickling a large one it never
/// completes, so it is billed to the connection before anything else happens.
fn handle_frames(poll: &Poll, entry: &mut Entry, cfg: &WorkerConfig, now_ns: u64) -> Action {
    let result = entry.conn.read_frames(&mut entry.inbuf, cfg.max_payload);
    entry.conn.set_frame_buffer_bytes(entry.inbuf.len());
    let frames = match result {
        Ok(frames) => frames,
        // EOF or a fatal protocol violation: close.
        Err(ConnError::Closed) | Err(ConnError::Protocol(_)) => return Action::Close,
    };

    match &cfg.mode {
        Mode::Echo => echo_frames(poll, entry, frames, now_ns),
        Mode::Dispatch(_) => dispatch_frames(poll, entry, frames, cfg.max_message_bytes, now_ns),
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
                let _ = entry.conn.queue(out.freeze(), now_ns);
                wrote = true;
            }
            OpCode::Ping => {
                let mut out = BytesMut::new();
                frame::encode(&mut out, true, OpCode::Pong, &f.payload);
                let _ = entry.conn.queue(out.freeze(), now_ns);
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

/// The fragmentation state a FIN=0 Text frame opens: an accumulator seeded with
/// the payload, or the ignore-mode when that first fragment already exceeds the
/// per-message cap — nothing a peer sends is buffered before it is checked.
fn open_text_fragment(payload: &[u8], max_message_bytes: usize) -> Fragment {
    if payload.len() > max_message_bytes {
        tracing::trace!(
            assembled = payload.len(),
            max_message_bytes,
            "dropping oversize fragmented message"
        );
        return Fragment::Oversize;
    }
    Fragment::Text(payload.to_vec())
}

/// [`Mode::Dispatch`]: decode each complete Text message to a [`ClientCommand`]
/// and drive `ctx.dispatch`, answer pings with pongs, echo a client-initiated
/// Close per RFC 6455 §5.5.1, then drain this connection's mailbox so any
/// self-directed replies go out.
///
/// Fragmented text messages (RFC 6455 §5.4) are reassembled in
/// [`Entry::fragment`] and dispatched on their FIN=1 Continuation, through the
/// same path as an unfragmented Text frame. Every fragment — the first included
/// — is checked against `max_message_bytes`, and a message that breaches it
/// switches to [`Fragment::Oversize`]: dropped, unbuffered, connection intact.
/// Control frames interleaved mid-fragment are handled in frame order, so a
/// Ping between fragments is answered before the message completes (§5.5.2).
fn dispatch_frames(
    poll: &Poll,
    entry: &mut Entry,
    frames: Vec<frame::Frame>,
    max_message_bytes: usize,
    now_ns: u64,
) -> Action {
    for f in frames {
        match f.opcode {
            // RFC 6455 §5.4: a fragmented message consists of one FIN=0 data
            // frame followed by Continuation frames ONLY, so a data frame
            // interleaved with an open TEXT message fails the connection. A
            // Binary frame while a BINARY message is open is ignored instead:
            // binary is outside the protocol and never fatal on its own.
            OpCode::Text if entry.fragment.is_some() => {
                return close_fragment_violation(
                    poll,
                    entry,
                    now_ns,
                    "data frame interleaved with a fragmented message",
                );
            }
            OpCode::Binary
                if matches!(entry.fragment, Some(Fragment::Text(_) | Fragment::Oversize)) =>
            {
                return close_fragment_violation(
                    poll,
                    entry,
                    now_ns,
                    "data frame interleaved with a fragmented message",
                );
            }
            OpCode::Text if !f.fin => {
                entry.set_fragment(Some(open_text_fragment(&f.payload, max_message_bytes)));
            }
            OpCode::Text => {
                if dispatch_text_message(poll, entry, &f.payload, now_ns) == Action::Close {
                    return Action::Close;
                }
            }
            // Binary is not part of the Pusher protocol; ignore, never fatal.
            OpCode::Binary => {
                if !f.fin && entry.fragment.is_none() {
                    entry.set_fragment(Some(Fragment::Binary));
                }
            }
            OpCode::Continuation => {
                // RFC 6455 §5.4: a Continuation must follow an open fragmented
                // message on this connection; a stray one is a protocol
                // violation — fail the connection with Close 1002.
                let Some(fragment) = entry.take_fragment() else {
                    return close_fragment_violation(
                        poll,
                        entry,
                        now_ns,
                        "continuation frame without a fragmented message",
                    );
                };
                match fragment {
                    ignored @ (Fragment::Binary | Fragment::Oversize) => {
                        if !f.fin {
                            entry.set_fragment(Some(ignored));
                        }
                    }
                    Fragment::Text(mut buf) => {
                        let assembled = buf.len().saturating_add(f.payload.len());
                        if assembled > max_message_bytes {
                            tracing::trace!(
                                assembled,
                                max_message_bytes,
                                "dropping oversize fragmented message"
                            );
                            if !f.fin {
                                entry.set_fragment(Some(Fragment::Oversize));
                            }
                            continue;
                        }
                        buf.extend_from_slice(&f.payload);
                        if f.fin {
                            if dispatch_text_message(poll, entry, &buf, now_ns) == Action::Close {
                                return Action::Close;
                            }
                        } else {
                            entry.set_fragment(Some(Fragment::Text(buf)));
                        }
                    }
                }
            }
            OpCode::Ping => {
                let mut out = BytesMut::new();
                frame::encode(&mut out, true, OpCode::Pong, &f.payload);
                let _ = entry.conn.queue(out.freeze(), now_ns);
                // RFC 6455 §5.5.2: answer a Ping "as soon as is practical" —
                // flush the Pong NOW. The end-of-batch drain below only
                // flushes when mailbox events were written, so a lone Ping's
                // reply (e.g. interleaved mid-fragment) would otherwise sit
                // queued until unrelated activity.
                if flush_and_arm(poll, entry, now_ns) == Action::Close {
                    return Action::Close;
                }
            }
            OpCode::Pong => {}
            // RFC 6455 §5.5.1: a client-initiated Close completes the closing
            // handshake — echo a Close frame (flushed before teardown, which
            // `remove()` does not do) carrying the client's code when present.
            OpCode::Close => {
                return close_handshake_reply(poll, entry, &f.payload, now_ns);
            }
        }
    }

    // Dispatch may have enqueued self-directed replies, and the adapter may
    // have fanned a broadcast onto this connection.
    drain_session(poll, entry, now_ns).action
}

/// Dispatch one complete (reassembled or unfragmented) text payload through
/// the v7 codec. Returns `Action::Close` when the session is gone or the
/// payload is not valid UTF-8 (RFC 6455 §8.1 → Close 1007 via
/// [`close_invalid_utf8`]); payloads that are valid UTF-8 but undecodable by
/// the codec are dropped silently, matching the pre-fragmentation behavior.
fn dispatch_text_message(poll: &Poll, entry: &mut Entry, payload: &[u8], now_ns: u64) -> Action {
    // RFC 6455 §8.1: a Text message (unfragmented frame or assembled
    // fragments) that is not valid UTF-8 is a fatal framing error — fail the
    // connection, do not silently skip the message. Checked before the
    // session borrow so the close path can take `entry` mutably.
    let text = match std::str::from_utf8(payload) {
        Ok(t) => t,
        Err(_) => return close_invalid_utf8(poll, entry, now_ns),
    };
    // The session always exists once Open on a dispatch worker.
    let Some(session) = entry.session.as_mut() else {
        return Action::Close;
    };
    match session.codec.decode(text) {
        Ok(cmd) => dispatch_command(session, cmd),
        Err(e) => {
            // Unparseable frames are silently dropped; 4200 is a
            // close/reconnect code and must not be sent in-band.
            tracing::trace!("dropping malformed client frame: {e}");
        }
    }
    Action::Keep
}

/// Fail the connection for an RFC 6455 §5.4 fragmentation violation: queue a
/// WebSocket Close frame with status code 1002 (protocol error) + `reason`,
/// flush it so it reaches the peer before teardown (`remove()` does not
/// flush), and report [`Action::Close`]. The fragment accumulator is reset —
/// explicit here even though the entry is torn down immediately after.
fn close_fragment_violation(poll: &Poll, entry: &mut Entry, now_ns: u64, reason: &str) -> Action {
    entry.set_fragment(None);
    queue_close_frame(entry, 1002, reason, now_ns);
    let _ = flush_and_arm(poll, entry, now_ns);
    Action::Close
}

/// Fail the connection for a non-UTF-8 Text payload (RFC 6455 §8.1): queue a
/// WebSocket Close frame with status code 1007 (invalid frame payload data) +
/// reason, flush it so it reaches the peer before teardown (`remove()` does
/// not flush — same mechanics as [`close_fragment_violation`]), and report
/// [`Action::Close`]. This is a WebSocket-level failure, NOT a Pusher protocol
/// error: no `pusher:error` frame is sent (those carry 4xxx Pusher codes).
fn close_invalid_utf8(poll: &Poll, entry: &mut Entry, now_ns: u64) -> Action {
    entry.set_fragment(None);
    queue_close_frame(entry, 1007, "invalid UTF-8 in a text message", now_ns);
    let _ = flush_and_arm(poll, entry, now_ns);
    Action::Close
}

/// RFC 6455 §5.5.1: complete the closing handshake on a client-initiated
/// Close. The server MUST send a Close frame in response before closing the
/// connection, so queue the echo and flush it (teardown via `remove()` does
/// not flush — same mechanics as [`close_fragment_violation`]). The echo
/// carries the client's status code when its Close payload has one, else code
/// 1000 (normal closure), per [`echo_close_code`].
fn close_handshake_reply(poll: &Poll, entry: &mut Entry, payload: &[u8], now_ns: u64) -> Action {
    queue_close_frame(entry, echo_close_code(payload), "", now_ns);
    let _ = flush_and_arm(poll, entry, now_ns);
    Action::Close
}

/// The status code for the §5.5.1 echo of a client Close: the client's own
/// code when its payload carries one, else 1000 (normal closure). Codes an
/// endpoint must never put on the wire (RFC 6455 §7.4: the 1005/1006/1015
/// sentinels, codes below 1000 or above 4999) fall back to 1000 so the echo
/// is always a well-formed Close frame.
fn echo_close_code(payload: &[u8]) -> u16 {
    if payload.len() >= 2 {
        let code = u16::from_be_bytes([payload[0], payload[1]]);
        if (1000..=4999).contains(&code) && !matches!(code, 1005 | 1006 | 1015) {
            return code;
        }
    }
    1000
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
/// A [`ServerEvent::SubscriptionError`] means the subscription did not take, so
/// the channel is removed from `ctx.subscribed` and `presence_membership`
/// before the (unchanged) frame is encoded. The auth-failure cases never
/// inserted the channel, making the remove a no-op there; the cluster-capacity
/// reject did inline-join it, and the remove reverses that.
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
    // One encode scratch for the whole drain, reused across every queued event.
    let mut text = String::with_capacity(256);
    while let Ok(ev) = session.rx.try_recv() {
        match *ev {
            ServerEvent::Close { code, reason } => {
                let mut out = BytesMut::new();
                let mut frame_body = Vec::with_capacity(2 + reason.len());
                frame_body.extend_from_slice(&code.to_be_bytes());
                frame_body.extend_from_slice(reason.as_bytes());
                frame::encode(&mut out, true, OpCode::Close, &frame_body);
                let _ = entry.conn.queue(out.freeze(), now_ns);
                wrote = true;
                close_after = true;
                break;
            }
            other => {
                // The subscription did not take, so drop the channel from this
                // connection's protocol state before encoding the (unchanged)
                // frame. A no-op when the channel was never inserted.
                if let ServerEvent::SubscriptionError { channel, .. } = &other {
                    if session.ctx.subscribed.remove(channel) {
                        subs_changed = true;
                    }
                    session.ctx.presence_membership.remove(channel);
                }
                text.clear();
                session.codec.encode_into(&other, &mut text);
                let mut out = BytesMut::new();
                frame::encode_text(&mut out, text.as_bytes());
                let _ = entry.conn.queue(out.freeze(), now_ns);
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

/// Waker-driven SELECTIVE mailbox drain: visit ONLY the connections `dirty_rx`
/// names — the slab tokens `Mailbox::send` pushed, deduped through the reused
/// `dirty_set` — so the cost is O(dirty), not O(N), and an idle round is one
/// empty `try_recv`.
///
/// A token whose slab entry is gone, closed, or not yet a session is skipped; a
/// reused slab slot needs no generation guard, since `drain_session` only
/// delivers that connection's own queued events and is idempotent. Connections
/// that request a close (or whose write fails) are torn down, and a `subscribed`
/// change during the drain is reconciled into the worker-local delivery index.
/// Returns `true` if any connection wrote a queued event (keeps the adaptive
/// poll tight).
#[allow(clippy::too_many_arguments)]
fn drain_dirty_sessions(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    dirty_rx: &std::sync::mpsc::Receiver<usize>,
    dirty_set: &mut HashSet<usize>,
    local_subs: &mut LocalSubs,
    wheel: &mut TimerWheel,
    inflight_bytes: &mut u64,
    outbound_bytes: &mut u64,
    codel_total: &mut u64,
    drophead_total: &mut u64,
    now_ns: u64,
    conn_counts: &Arc<DashMap<String, Arc<AtomicUsize>>>,
    app_registry: &Arc<AppRegistry>,
    node_conns: &Arc<AtomicUsize>,
    cluster: &Option<crate::cluster::bridge::ClusterHandle>,
) -> bool {
    // Drain the dirty-token queue into the reused set, which dedups it.
    dirty_set.clear();
    while let Ok(tok) = dirty_rx.try_recv() {
        dirty_set.insert(tok);
    }
    if dirty_set.is_empty() {
        return false;
    }

    let mut wrote_any = false;
    for &key in dirty_set.iter() {
        // The token may be stale: the connection closed since it was marked
        // dirty, or its slab slot was recycled by an unrelated connection.
        match conns.get(key) {
            Some(e) if e.session.is_some() && e.conn.state == ConnState::Open => {}
            _ => continue,
        }
        #[cfg(any(test, feature = "test-hooks"))]
        SELECTIVE_DRAIN_VISITS.fetch_add(1, Ordering::Relaxed);
        let result = drain_session(poll, &mut conns[key], now_ns);
        wrote_any |= result.wrote;
        // Fold the drain's net delta whether or not the connection closes; a
        // closing one's remaining queued bytes are subtracted by `remove`.
        fold_delta(conns, key, inflight_bytes, outbound_bytes);
        fold_codel(conns, key, codel_total);
        fold_drophead(conns, key, drophead_total);
        if result.action == Action::Close {
            remove(
                poll,
                conns,
                key,
                local_subs,
                wheel,
                inflight_bytes,
                outbound_bytes,
                conn_counts,
                app_registry,
                node_conns,
                cluster,
            );
            continue;
        }
        // A `SubscriptionError` drained here removed a channel, which must
        // reach the delivery index or the rejected connection keeps receiving
        // that channel's broadcasts. Gated so only that rare connection pays
        // the two-set diff.
        if result.subs_changed {
            if let Some(entry) = conns.get_mut(key) {
                if let Some(session) = entry.session.as_mut() {
                    reconcile_membership(session, key, local_subs);
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
///   * `Ok(Found(app))` → `finish_establish`; on `Ok(session)` queue
///     `connection_established` + store the session + flush; on `Err(reject)`
///     queue the reject + flush + `remove`.
///   * `Ok(Disabled)`  → reject `app_disabled` (4003) + flush + `remove`
///     (P13: the doc's close-code table gives disabled its own code).
///   * `Ok(NotFound)`  → reject `app_not_found` (4001) + flush + `remove`.
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
    local_subs: &mut LocalSubs,
    wheel: &mut TimerWheel,
    inflight_bytes: &mut u64,
    outbound_bytes: &mut u64,
    codel_total: &mut u64,
    drophead_total: &mut u64,
    now_ns: u64,
    conn_counts: &Arc<DashMap<String, Arc<AtomicUsize>>>,
    app_registry: &Arc<AppRegistry>,
    node_conns: &Arc<AtomicUsize>,
    cluster: &Option<crate::cluster::bridge::ClusterHandle>,
) -> bool {
    use crate::protocol::error::PusherError;
    let mut wrote_any = false;
    while let Ok(ResolvedApp { token, gen, result }) = resolved_rx.try_recv() {
        // Resume only if THIS park is still pending and its generation matches;
        // a mismatch means a new connection took the recycled slot. `take` runs
        // in a tight scope so the `conns` borrow is released before the
        // `fold_delta`/`remove` calls below re-borrow it.
        let pe = {
            let Some(entry) = conns.get_mut(token) else {
                continue;
            };
            match &entry.pending_establish {
                Some(p) if p.gen == gen => {}
                _ => continue,
            }
            entry
                .pending_establish
                .take()
                .expect("pending_establish present")
        };

        let outcome = match result {
            Ok(crate::app::AppLookup::Found(app)) => {
                finish_establish(env, app, pe.codec, pe.notify, pe.mailbox_dropped)
            }
            // P13: disabled vs unknown split on the parked path too — 4003
            // "Application disabled" for a disabled key, 4001 for unknown.
            Ok(crate::app::AppLookup::Disabled) => Err(Reject {
                error: PusherError::app_disabled(),
                codec: Some(pe.codec),
            }),
            Ok(crate::app::AppLookup::NotFound) => Err(Reject {
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
                // Reborrow inline so the `entry` borrow drops before `fold_delta`.
                let action = {
                    let entry = &mut conns[token];
                    let established = ServerEvent::ConnectionEstablished {
                        socket_id: session.ctx.socket_id,
                        activity_timeout: env.activity_timeout,
                    };
                    let mut text = String::new();
                    session.codec.encode_into(&established, &mut text);
                    let mut out = BytesMut::new();
                    frame::encode_text(&mut out, text.as_bytes());
                    let _ = entry.conn.queue(out.freeze(), now_ns);
                    entry.session = Some(session);
                    flush_and_arm(poll, entry, now_ns)
                };
                fold_delta(conns, token, inflight_bytes, outbound_bytes);
                fold_codel(conns, token, codel_total);
                fold_drophead(conns, token, drophead_total);
                wrote_any = true;
                if action == Action::Close {
                    remove(
                        poll,
                        conns,
                        token,
                        local_subs,
                        wheel,
                        inflight_bytes,
                        outbound_bytes,
                        conn_counts,
                        app_registry,
                        node_conns,
                        cluster,
                    );
                } else {
                    // Re-arm unconditionally: a park that outlived
                    // `activity_timeout` would have had its idle timer fire on
                    // the `session: None` entry and drop it from the wheel, and
                    // a resumed connection must always be liveness-monitored.
                    wheel.touch(token, now_ns / 1_000_000);
                    // The max-lifetime clock starts at this establish moment;
                    // the park is not part of the connection's life.
                    arm_lifetime(wheel, env, token, now_ns / 1_000_000);
                    wheel.clear_handshake(token);
                }
            }
            Err(reject) => {
                {
                    let entry = &mut conns[token];
                    queue_reject(entry, &reject, now_ns);
                    let _ = flush_and_arm(poll, entry, now_ns);
                }
                // Fold the reject frames before `remove` subtracts what is
                // still queued.
                fold_delta(conns, token, inflight_bytes, outbound_bytes);
                fold_codel(conns, token, codel_total);
                fold_drophead(conns, token, drophead_total);
                wrote_any = true;
                remove(
                    poll,
                    conns,
                    token,
                    local_subs,
                    wheel,
                    inflight_bytes,
                    outbound_bytes,
                    conn_counts,
                    app_registry,
                    node_conns,
                    cluster,
                );
            }
        }
    }
    wrote_any
}

/// Handle a writable event: flush and, when drained, drop writable interest.
/// In the `Handshaking` state (G2) the writable event instead re-drives the
/// handshake itself.
///
/// A Handshaking TLS connection only holds WRITABLE interest because its
/// handshake-flight write blocked, so the writable event means the send buffer
/// drained: re-running [`handle_handshake`] completes the flight, pulls the
/// plaintext that was waiting behind it, and clears WRITABLE once rustls has
/// nothing left to write.
#[allow(clippy::too_many_arguments)]
fn handle_writable(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    key: usize,
    cfg: &WorkerConfig,
    now_ns: u64,
    dirty_tx: &std::sync::mpsc::Sender<usize>,
    mailbox_waker: &Arc<mio::Waker>,
    resolved_tx: &std::sync::mpsc::Sender<ResolvedApp>,
    next_gen: &mut u64,
    wheel: &mut TimerWheel,
) -> Action {
    let entry = &mut conns[key];
    match entry.conn.state {
        ConnState::Handshaking => handle_handshake(
            poll,
            entry,
            key,
            cfg,
            now_ns,
            dirty_tx,
            mailbox_waker,
            resolved_tx,
            next_gen,
            wheel,
        ),
        ConnState::Open => flush_and_arm(poll, entry, now_ns),
    }
}

/// Flush the outbound queue and reconcile writable interest with what remains.
///
/// * [`WriteStatus::Drained`] → re-arm `READABLE`-only (drop `WRITABLE`).
/// * [`WriteStatus::WouldBlock`] → add `WRITABLE` so we get a writable event.
/// * [`WriteStatus::Closed`] → close, with no interest re-registered: the
///   caller must tear the connection down, not leave it holding a backlog.
///
/// The `writable_armed` mirror is updated after every successful
/// re-registration, so the loop-top invariant can verify the arm really
/// happened.
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
            entry.conn.set_writable_armed(false);
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
            entry.conn.set_writable_armed(true);
            Action::Keep
        }
        WriteStatus::Closed => Action::Close,
    }
}

/// G2: reconcile a `Handshaking` connection's poll interest with whatever it
/// still has to write — a TLS flight rustls could not finish writing, or the
/// error/Close frames the shutdown drain queues even mid-handshake.
///
/// mio is level-triggered, so arming WRITABLE means the kernel wakes the loop
/// the moment the buffer drains and [`handle_writable`] re-drives the
/// handshake. With nothing pending, a previously-armed WRITABLE drops back to
/// READABLE-only so the connection never spins on an always-ready writable
/// socket; a plain-TCP handshake keeps its accept-time registration untouched.
///
/// The G3 slowloris deadline is ABSOLUTE from accept, so a connection parked
/// here on a stalled flight is still reaped on time.
fn arm_handshake_interest(poll: &Poll, entry: &mut Entry, now_ns: u64) -> Action {
    let token = entry.token;
    if entry.conn.has_pending_writes() {
        return flush_and_arm(poll, entry, now_ns);
    }
    if entry.conn.writable_armed() {
        if poll
            .registry()
            .reregister(entry.conn.stream_mut(), token, Interest::READABLE)
            .is_err()
        {
            return Action::Close;
        }
        entry.conn.set_writable_armed(false);
    }
    Action::Keep
}

/// Transfer a plain-HTTP connection to the tokio/axum REST plane (SP9 §3.4).
///
/// Order matters: deregister the stream and remove the slab entry BEFORE
/// [`crate::transport::rest::mio_to_std`] moves the fd out of mio, so nothing
/// still references it. A pre-handshake REST connection has no [`Session`], so
/// there is no on-close hook or counter to unwind.
///
/// Like [`remove`], this subtracts whatever the connection still holds from the
/// worker totals: the shutdown drain queues 4200 frames onto `Handshaking`
/// entries too, so a REST head arriving mid-drain gets here with a non-empty
/// queue, and bytes left unsubtracted are a permanent phantom floor.
fn handoff_rest(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    key: usize,
    cfg: &WorkerConfig,
    prefix: Vec<u8>,
    inflight_bytes: &mut u64,
    outbound_bytes: &mut u64,
) {
    let Some(mut entry) = conns.try_remove(key) else {
        return;
    };
    *inflight_bytes = inflight_bytes
        .wrapping_add(entry.conn.take_inflight_delta() as u64)
        .wrapping_sub(entry.conn.accounted_bytes() as u64);
    *outbound_bytes = outbound_bytes
        .wrapping_add(entry.conn.take_outbound_delta() as u64)
        .wrapping_sub(entry.conn.outbound_bytes() as u64);
    let _ = poll.registry().deregister(entry.conn.stream_mut());

    let Some(tx) = cfg.rest_handoff.as_ref() else {
        // No REST plane; dropping `entry` closes the socket.
        return;
    };

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

/// Fold connection `key`'s accumulated accounting deltas into the worker's
/// running `inflight_bytes` and `outbound_bytes`, bringing both counters back
/// in step after a touch. `wrapping_add` because the deltas are signed: a net
/// send or drop folds a negative one. O(1) — this is what replaces a
/// per-iteration re-sum.
fn fold_delta(
    conns: &mut slab::Slab<Entry>,
    key: usize,
    inflight_bytes: &mut u64,
    outbound_bytes: &mut u64,
) {
    if let Some(entry) = conns.get_mut(key) {
        let delta = entry.conn.take_inflight_delta();
        if delta != 0 {
            *inflight_bytes = inflight_bytes.wrapping_add(delta as u64);
        }
        let out_delta = entry.conn.take_outbound_delta();
        if out_delta != 0 {
            *outbound_bytes = outbound_bytes.wrapping_add(out_delta as u64);
        }
    }
}

/// B1: drain this connection's CoDel drop accumulator into the worker-level
/// total, alongside [`fold_delta`] at every flush site.
fn fold_codel(conns: &mut slab::Slab<Entry>, key: usize, codel_total: &mut u64) {
    if let Some(entry) = conns.get_mut(key) {
        let dropped = entry.conn.take_codel_dropped();
        if dropped > 0 {
            *codel_total = codel_total.wrapping_add(dropped);
        }
    }
}

/// G8: drain this connection's drop-head eviction accumulator into the
/// worker-level total, at exactly the same sites as [`fold_codel`] —
/// pre-teardown ones included, or an eviction right before a close would die
/// with the slab entry and lose the count.
fn fold_drophead(conns: &mut slab::Slab<Entry>, key: usize, drophead_total: &mut u64) {
    if let Some(entry) = conns.get_mut(key) {
        let dropped = entry.conn.take_drophead_dropped();
        if dropped > 0 {
            *drophead_total = drophead_total.wrapping_add(dropped);
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
    local_subs: &mut LocalSubs,
    wheel: &mut TimerWheel,
    inflight_bytes: &mut u64,
    outbound_bytes: &mut u64,
    conn_counts: &Arc<DashMap<String, Arc<AtomicUsize>>>,
    app_registry: &Arc<AppRegistry>,
    node_conns: &Arc<AtomicUsize>,
    cluster: &Option<crate::cluster::bridge::ClusterHandle>,
) {
    // Before the slab slot (and its token) can be recycled by a future accept,
    // so a new connection never inherits a stale timer.
    wheel.remove(key);
    if let Some(mut entry) = conns.try_remove(key) {
        // After the fold each contribution is exactly its own accounted total,
        // so folding then subtracting zeroes it exactly.
        *inflight_bytes = inflight_bytes
            .wrapping_add(entry.conn.take_inflight_delta() as u64)
            .wrapping_sub(entry.conn.accounted_bytes() as u64);
        *outbound_bytes = outbound_bytes
            .wrapping_add(entry.conn.take_outbound_delta() as u64)
            .wrapping_sub(entry.conn.outbound_bytes() as u64);
        entry.conn.send_close_notify();
        if let Some(mut session) = entry.session.take() {
            deindex_connection(&session, local_subs);
            futures_executor::block_on(session.ctx.on_close());
            let app_id = &session.ctx.app.id;
            // Drop this connection from the per-app registry (remove_if-empty inside).
            app_registry.remove(app_id, &session.ctx.socket_id);
            session.conn_count.fetch_sub(1, Ordering::SeqCst);
            // Drop the per-app counter entry once it reaches 0, so an idle app
            // that is never deleted leaves no zombie. A concurrent establish
            // that bumped it back above 0 is re-checked under the shard lock.
            conn_counts.remove_if(app_id, |_, c| c.load(Ordering::SeqCst) == 0);
            node_conns.fetch_sub(1, Ordering::SeqCst);
            // Gated on this connection's own admission verdict, so it gives
            // back only a unit that was really taken.
            if session.cluster_admitted {
                if let Some(bridge) = cluster {
                    bridge.release_app(app_id);
                }
            }
        }
        let _ = poll.registry().deregister(entry.conn.stream_mut());
    }
}

/// Drop a closing connection's `socket_id` — and its `(token, version)` entry
/// value, absorbed into the subscriber map — from every `(app, channel)` it was
/// indexed under.
///
/// G5: walks the UNION of the session's last-reconciled baseline (`subs`) and
/// the live protocol set (`ctx.subscribed`), because a readable batch of
/// [subscribe, Close] returns `Action::Close` before the `Action::Keep` arm's
/// post-dispatch reconcile ever runs. Deindexing the union makes the close path
/// self-sufficient instead of trusting that bookkeeping, and costs nothing:
/// removing an absent `(key, socket_id)` is a no-op.
fn deindex_connection(session: &Session, local_subs: &mut LocalSubs) {
    let app: Arc<str> = Arc::from(session.ctx.app.id.as_str());
    let sid = &session.ctx.socket_id;
    // `difference` yields the baseline-only channels, so the chain enumerates
    // exactly the union — no duplicate keys, no temporary set allocation.
    let union = session
        .ctx
        .subscribed
        .iter()
        .chain(session.subs.difference(&session.ctx.subscribed));
    for channel in union {
        let k = (Arc::clone(&app), Arc::<str>::from(channel.as_str()));
        if let Some(subs) = local_subs.get_mut(&k) {
            if subs.remove(sid).is_some() {
                #[cfg(any(test, feature = "test-hooks"))]
                LOCAL_SUBS_SLOTS.fetch_sub(1, Ordering::Relaxed);
            }
            if subs.is_empty() {
                local_subs.remove(&k);
            }
        }
    }
}

/// Reconcile a connection's worker-local subscription index against the protocol
/// state after a dispatch. Diffs the session's previously-recorded channel set
/// (`session.subs`) against `ctx.subscribed`: channels newly joined are inserted
/// into `local_subs` — each entry stamped with the connection's `(slab token,
/// negotiated version)` (U3), so the broadcast drain resolves both from the
/// subscriber iteration with no second lookup — channels left are removed.
/// Cheap when nothing changed (two set diffs over the usually-tiny
/// per-connection channel set). `token` is this connection's slab key. No-op for
/// a connection in no channels with no change.
fn reconcile_membership(session: &mut Session, token: usize, local_subs: &mut LocalSubs) {
    if session.subs == session.ctx.subscribed {
        return;
    }
    let app: Arc<str> = Arc::from(session.ctx.app.id.as_str());
    let sid = &session.ctx.socket_id;
    let entry = (token, session.codec.version());

    // The `difference` iterator borrows `session.subs`, so the added names are
    // collected before the baseline is updated. The diff is almost always one
    // channel; the live set is every channel the connection is in.
    let added: Vec<String> = session
        .ctx
        .subscribed
        .difference(&session.subs)
        .cloned()
        .collect();
    for channel in &added {
        let inserted = local_subs
            .entry((Arc::clone(&app), Arc::<str>::from(channel.as_str())))
            .or_default()
            .insert(*sid, entry)
            .is_none();
        if inserted {
            #[cfg(any(test, feature = "test-hooks"))]
            LOCAL_SUBS_SLOTS.fetch_add(1, Ordering::Relaxed);
        }
    }
    for channel in added {
        session.subs.insert(channel);
    }
    // Removed channels: recorded, no longer subscribed.
    let live = &session.ctx.subscribed;
    session.subs.retain(|channel| {
        if live.contains(channel) {
            return true;
        }
        let k = (Arc::clone(&app), Arc::<str>::from(channel.as_str()));
        if let Some(subs) = local_subs.get_mut(&k) {
            if subs.remove(sid).is_some() {
                #[cfg(any(test, feature = "test-hooks"))]
                LOCAL_SUBS_SLOTS.fetch_sub(1, Ordering::Relaxed);
            }
            if subs.is_empty() {
                local_subs.remove(&k);
            }
        }
        false
    });
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
    /// ≥ 100%: drop the broadcast for this worker entirely.
    Saturated,
}

/// The band this worker's `inflight_bytes` must fall back into before its
/// budget-pressure bit is released — deliberately BELOW the band that sets it,
/// so the node's saturation signal cannot flap at the 100% boundary.
const BUDGET_PRESSURE_RELEASE_BAND: ShedBand = ShedBand::Normal;

/// Raise this worker's budget-pressure bit while it is over budget; drop it only
/// once it has fallen back to [`BUDGET_PRESSURE_RELEASE_BAND`]. Only the owning
/// worker calls this, so no other worker can release a pressure this one is
/// still under.
fn update_budget_pressure(bit: &AtomicBool, inflight: u64, budget: u64) {
    match shed_band(inflight, budget) {
        ShedBand::Saturated => bit.store(true, Ordering::Relaxed),
        BUDGET_PRESSURE_RELEASE_BAND => bit.store(false, Ordering::Relaxed),
        _ => {}
    }
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
        // Above 1/16 of the cap is "non-trivially backed up": a subscriber that
        // drained between iterations sails through, one that did not is shed.
        ShedBand::Severe => out_bytes * 16 > high_water,
        ShedBand::Saturated => true,
    }
}

/// Minimal seam over the worker's connection table: resolve a subscriber's
/// slab token to its OPEN dispatch connection. `None` covers everything the
/// inline drain loop has always skipped — a missing slot, a session-less
/// entry, or a non-`Open` connection. Factored out of `drain_broadcasts` so
/// the inbox pump can run against any connection container; the impl below is
/// the production one (`slab::Slab<Entry>`).
///
/// `#[doc(hidden)]`: an internal seam exposed ONLY so `benches/fanout_sink.rs`
/// can drive the REAL inbox pump against a bench-built connection set (the
/// same hidden-seam pattern as the redis adapter's bench accessors). Not part
/// of the supported public API.
#[doc(hidden)]
pub trait ConnIndex {
    /// The OPEN dispatch connection at `token`, if there is one to deliver to.
    fn open_conn(&mut self, token: usize) -> Option<&mut Connection>;
}

impl ConnIndex for slab::Slab<Entry> {
    fn open_conn(&mut self, token: usize) -> Option<&mut Connection> {
        let entry = self.get_mut(token)?;
        // Only deliver to Open dispatch connections.
        if entry.session.is_none() || entry.conn.state != ConnState::Open {
            return None;
        }
        Some(&mut entry.conn)
    }
}

/// Pump this worker's broadcast inbox to empty: deliver every queued
/// [`crate::transport::fanout::BroadcastMsg`] to this worker's local
/// subscribers, applying the SP10 graduated shed (§6) against this worker's
/// byte budget.
///
/// Each message classifies a [`ShedBand`] from `inflight_bytes /
/// effective_budget`: `Saturated` (≥100%) drops the whole broadcast and raises
/// the caller's own `budget_saturated` bit; otherwise every subscriber but
/// `except` is queued the frame already encoded for its negotiated version,
/// unless the band says to skip a backed-up one. `inflight_bytes` stays live
/// across the drain, so the band tightens as the worker fills within one.
/// `touched` collects the connections queued onto, `to_close` the ones an
/// earlier phase of this drain marked for teardown.
///
/// This is the fan-out half of [`drain_broadcasts`], split out so
/// `benches/fanout_sink.rs` can benchmark the real production loop.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub fn drain_broadcast_inbox<C: ConnIndex>(
    rx: &std::sync::mpsc::Receiver<crate::transport::fanout::BroadcastMsg>,
    local_subs: &LocalSubs,
    conns: &mut C,
    effective_budget: u64,
    inflight_bytes: &mut u64,
    outbound_bytes: &mut u64,
    drophead_total: &mut u64,
    budget_saturated: Option<&AtomicBool>,
    now_ns: u64,
    touched: &mut HashSet<usize>,
    to_close: &HashSet<usize>,
) {
    while let Ok(msg) = rx.try_recv() {
        // Destructuring lets the lookup key MOVE `app`/`channel` instead of
        // bumping their refcounts per message.
        let crate::transport::fanout::BroadcastMsg {
            app,
            channel,
            frames,
            except,
        } = msg;
        let Some(subs) = local_subs.get(&(app, channel)) else {
            continue; // no local subscribers for this channel on this worker
        };
        // `frames` carries one finished WS frame per active protocol version,
        // keyed by version byte. Resolving the single-version shape once per
        // message keeps the per-subscriber cost at one `Option` check; the
        // scan below only starts once a second version goes live.
        let Some((_, first_frame)) = frames.first() else {
            continue; // nothing encoded for this message
        };
        let fast: Option<&Bytes> = (frames.len() == 1).then_some(first_frame);
        for (sid, &(token, version)) in subs.iter() {
            // Reclassify per subscriber: the band tightens as `inflight_bytes`
            // grows within this drain, so a single large channel cannot blow
            // past the budget mid-fan-out.
            let band = shed_band(*inflight_bytes, effective_budget);
            if band == ShedBand::Saturated {
                if let Some(bit) = budget_saturated {
                    bit.store(true, Ordering::Relaxed);
                }
                continue;
            }
            if except.as_ref() == Some(sid) {
                continue; // sender exclusion
            }
            if to_close.contains(&token) {
                continue;
            }
            let Some(conn) = conns.open_conn(token) else {
                continue;
            };
            // Graduated shed: under pressure, skip backed-up subscribers so the
            // fast (caught-up) ones still get every frame — targeted drop.
            if should_skip(band, conn.out_bytes(), conn.high_water()) {
                continue;
            }
            // An unrecognised version byte falls back to the first frame
            // rather than dropping the broadcast.
            let frame: &Bytes = match fast {
                Some(f) => f,
                None => frames
                    .iter()
                    .find(|(v, _)| *v == version)
                    .map(|(_, b)| b)
                    .unwrap_or(first_frame),
            };
            // The queue is byte-bounded drop-head and never rejects: a slow
            // consumer loses its oldest queued frames instead. Folding the net
            // delta here keeps the band accurate within this drain.
            let _dropped = conn.queue(frame.clone(), now_ns);
            *inflight_bytes = inflight_bytes.wrapping_add(conn.take_inflight_delta() as u64);
            *outbound_bytes = outbound_bytes.wrapping_add(conn.take_outbound_delta() as u64);
            // Fold evictions now rather than at the post-drain flush, so the
            // count survives a flush that closes the connection.
            let dh = conn.take_drophead_dropped();
            if dh > 0 {
                *drophead_total = drophead_total.wrapping_add(dh);
            }
            touched.insert(token);
        }
    }
}

/// Deliver every queued [`crate::transport::fanout::BroadcastMsg`] to this worker's local subscribers,
/// applying the SP10 graduated shed (§6) against this worker's byte budget.
///
/// The per-message fan-out is [`drain_broadcast_inbox`]. This wrapper adds the
/// two phases the pump must not do mid-lookup: flushing every connection queued
/// onto, then tearing down the ones that failed. Returns `true` if any frame
/// was queued.
///
/// `effective_budget` is the per-worker budget already scaled by the PSI factor
/// (§8); `now_ns` is this iteration's monotonic timestamp, stamped onto every
/// enqueued frame for the CoDel sojourn check (§7).
#[allow(clippy::too_many_arguments)]
fn drain_broadcasts(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    rx: &std::sync::mpsc::Receiver<crate::transport::fanout::BroadcastMsg>,
    local_subs: &mut LocalSubs,
    wheel: &mut TimerWheel,
    effective_budget: u64,
    inflight_bytes: &mut u64,
    outbound_bytes: &mut u64,
    codel_total: &mut u64,
    drophead_total: &mut u64,
    budget_saturated: Option<&AtomicBool>,
    now_ns: u64,
    conn_counts: &Arc<DashMap<String, Arc<AtomicUsize>>>,
    app_registry: &Arc<AppRegistry>,
    node_conns: &Arc<AtomicUsize>,
    cluster: &Option<crate::cluster::bridge::ClusterHandle>,
) -> bool {
    let mut touched: HashSet<usize> = HashSet::new();
    // Connections that backpressured during delivery, closed after the drain so
    // the slab is not mutated mid-lookup. A set because both loops below probe
    // it once per remaining subscriber.
    let mut to_close: HashSet<usize> = HashSet::new();

    drain_broadcast_inbox(
        rx,
        local_subs,
        conns,
        effective_budget,
        inflight_bytes,
        outbound_bytes,
        drophead_total,
        budget_saturated,
        now_ns,
        &mut touched,
        &to_close,
    );

    let wrote = !touched.is_empty();
    // Flush every connection we queued onto. A flush that backpressures arms
    // writable interest (handled in flush_and_arm); a failed flush closes.
    for token in touched {
        if to_close.contains(&token) {
            continue;
        }
        if let Some(entry) = conns.get_mut(token) {
            let action = flush_and_arm(poll, entry, now_ns);
            *inflight_bytes = inflight_bytes.wrapping_add(entry.conn.take_inflight_delta() as u64);
            *outbound_bytes = outbound_bytes.wrapping_add(entry.conn.take_outbound_delta() as u64);
            let cd = entry.conn.take_codel_dropped();
            if cd > 0 {
                *codel_total = codel_total.wrapping_add(cd);
            }
            let dh = entry.conn.take_drophead_dropped();
            if dh > 0 {
                *drophead_total = drophead_total.wrapping_add(dh);
            }
            if action == Action::Close {
                to_close.insert(token);
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
            wheel,
            inflight_bytes,
            outbound_bytes,
            conn_counts,
            app_registry,
            node_conns,
            cluster,
        );
    }
    wrote
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::fanout::{BroadcastMsg, SaturationFlag, WorkerSlot};
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

    /// A plain-TCP `Mode::Echo` worker config for tests.
    fn echo_worker_config(addr: std::net::SocketAddr) -> WorkerConfig {
        WorkerConfig {
            addr,
            max_payload: 1 << 20,
            max_message_bytes: 1 << 20,
            max_head_bytes: 16_384,
            handshake_timeout_ms: 10_000,
            high_water: 1 << 20,
            mode: Mode::Echo,
            rest_handoff: None,
            worker_id: 0,
            broadcast: None,
            per_worker_budget: 0,
            inflight_slot: None,
            accepted_slot: None,
            codel_dropped_slot: None,
            drophead_dropped_slot: None,
            mailbox_dropped_slot: None,
            codel: crate::transport::conn::CodelParams::DISABLED,
            budget_factor: None,
            shutdown_grace_ms: 0,
            tls: None,
        }
    }

    /// Spawn the worker on its own OS thread bound to `addr` in [`Mode::Echo`],
    /// returning the shutdown flag and the join handle.
    fn spawn_worker(addr: std::net::SocketAddr) -> (Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        let shutdown = Arc::new(AtomicBool::new(false));
        let sd = shutdown.clone();
        let handle = std::thread::spawn(move || {
            run(echo_worker_config(addr), sd).expect("worker run failed");
        });
        (shutdown, handle)
    }

    /// Block until the listener signals the readiness `run` only ever calls
    /// [`accept_ready`] under: a peer sitting in the accept queue, rather than
    /// a `connect` that has merely returned on the client's side.
    fn await_listener_readable(poll: &mut Poll) {
        let mut events = Events::with_capacity(4);
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            poll.poll(&mut events, Some(Duration::from_millis(50)))
                .unwrap();
            if events.iter().any(|e| e.token() == LISTENER) {
                return;
            }
        }
        panic!("listener never reported a pending connection");
    }

    /// F3 (Nagle): `accept_ready` must disable Nagle on every accepted socket.
    /// The server-side socket isn't reachable through a client connection (the
    /// client only sees its own half), and a loopback ping/pong latency
    /// assertion is probabilistic, so this asserts the option observably on the
    /// accepted stream itself, driven through the real accept path.
    #[test]
    fn accept_ready_sets_nodelay_on_accepted_socket() {
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        // `mio::net::TcpListener::from_std` expects a listener already in
        // nonblocking mode (as `reuseport_listener` arms it) — otherwise the
        // drain loop's trailing `accept()` would block forever.
        std_listener.set_nonblocking(true).unwrap();
        let addr = std_listener.local_addr().unwrap();
        let mut listener = TcpListener::from_std(std_listener);

        let mut poll = Poll::new().unwrap();
        poll.registry()
            .register(&mut listener, LISTENER, Interest::READABLE)
            .unwrap();
        let _client = std::net::TcpStream::connect(addr).unwrap();
        await_listener_readable(&mut poll);

        let mut conns = slab::Slab::new();
        let mut wheel = TimerWheel::new();
        let cfg = echo_worker_config(addr);

        let accepted = accept_ready(
            &poll,
            &mut listener,
            &mut conns,
            &cfg,
            crate::transport::conn::CodelParams::DISABLED,
            &mut wheel,
            None,
        );
        assert_eq!(accepted, 1);
        assert_eq!(conns.len(), 1);
        let entry = conns.iter_mut().next().unwrap().1;
        assert!(
            entry.conn.stream_mut().nodelay().unwrap(),
            "accepted socket must have TCP_NODELAY set (F3)"
        );
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
        ws.send(Message::Ping(b"ping-payload".as_slice().into()))
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
    fn parse_app_path_extracts_key_protocol_and_version() {
        assert_eq!(
            parse_app_path("/app/app-key?protocol=7&version=7.4.1"),
            (
                Some("app-key".to_string()),
                Some("7".to_string()),
                Some("7.4.1".to_string())
            )
        );
        assert_eq!(
            parse_app_path("/app/app-key"),
            (Some("app-key".to_string()), None, None)
        );
        assert_eq!(
            parse_app_path("/app/k?foo=1&protocol=7&version=8.2.0&bar=2"),
            (
                Some("k".to_string()),
                Some("7".to_string()),
                Some("8.2.0".to_string())
            )
        );
        // `version` without `protocol` (the inference path's input shape).
        assert_eq!(
            parse_app_path("/app/k?version=7.4.1"),
            (Some("k".to_string()), None, Some("7.4.1".to_string()))
        );
    }

    #[test]
    fn parse_app_path_rejects_non_app_shapes_with_none() {
        // No `/app/` prefix → 4005, not an empty-key app lookup (4001).
        assert_eq!(
            parse_app_path("/nope/?protocol=7"),
            (None, Some("7".to_string()), None)
        );
        // Empty key → 4005.
        assert_eq!(
            parse_app_path("/app/?protocol=7"),
            (None, Some("7".to_string()), None)
        );
        // Multi-segment key (a `/` inside the key) → 4005.
        assert_eq!(
            parse_app_path("/app/a/b?protocol=7"),
            (None, Some("7".to_string()), None)
        );
        // No path at all → 4005.
        assert_eq!(parse_app_path("/"), (None, None, None));
    }

    // ---- G2: TLS handshake flight blocked on a full send buffer ---------------

    /// The WS upgrade head the raw TLS client sends once its TLS handshake is
    /// complete (RFC 6455 §4.2.1 sample key).
    const G2_UPGRADE_HEAD: &str = "GET /app/g2-key?protocol=7 HTTP/1.1\r\n\
        Host: localhost\r\n\
        Upgrade: websocket\r\n\
        Connection: Upgrade\r\n\
        Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
        Sec-WebSocket-Version: 13\r\n\
        \r\n";

    /// THE G2 SCENARIO, driven through the REAL worker handlers: a TLS client
    /// with a tiny receive window lets the server's ServerHello flight (a
    /// deliberately bloated certificate) fill every socket buffer
    /// mid-handshake, so `drain_head_bytes` exits with `tls.wants_write()`
    /// still true. Pre-fix nothing re-armed `WRITABLE` — connections register
    /// READABLE-only at accept — so the flight was never completed and the
    /// handshake hung forever (a zero-window client pins it). This test runs
    /// the worker's own event dispatch (`handle_readable` / `handle_writable`)
    /// over a real `mio::Poll` and asserts the handshake — and the WS 101
    /// upgrade behind it — completes within a bounded wait once the client
    /// starts draining.
    ///
    /// Forced at this level rather than in `tests/tls.rs` because a real
    /// server's accepted socket keeps the kernel-default `SO_SNDBUF` (128 KiB
    /// on this macOS loopback, auto-growing), which a TLS flight can never
    /// exceed: rustls caps inbound handshake messages at 64 KiB, so even a
    /// maximally bloated flight always fits and the write never blocks.
    /// Shrinking the accepted socket's `SO_SNDBUF` from the test process is
    /// impossible (the accept happens inside the worker), so the deterministic
    /// reproduction needs a socket pair the test owns — driven here through the
    /// production handlers.
    #[test]
    fn tls_handshake_completes_when_flight_write_blocks() {
        use crate::transport::conn::tls_test_support as tlsup;
        use std::io::Read as _;

        let (server_stream, mut client_sock) = tlsup::pair_tiny_tls();
        let (server_cfg, cert_der) = tlsup::bloated_server_config();
        let tls = rustls::server::ServerConnection::new(server_cfg).unwrap();
        let mut conn = Connection::new_tls(server_stream, Box::new(tls), 1 << 20);
        let mut client = tlsup::tls_client(&cert_der);

        // Real poll + slab entry, registered READABLE-only exactly as
        // `accept_ready` does (mirrors the worker-loop preconditions of G2).
        let mut poll = Poll::new().unwrap();
        let mut events = Events::with_capacity(16);
        let mut conns: slab::Slab<Entry> = slab::Slab::new();
        let vacant = conns.vacant_entry();
        let key = vacant.key();
        poll.registry()
            .register(conn.stream_mut(), Token(key), Interest::READABLE)
            .unwrap();
        vacant.insert(Entry {
            conn,
            inbuf: BytesMut::new(),
            token: Token(key),
            session: None,
            fragment: None,
            pending_establish: None,
        });

        // Echo-mode worker config + the notifier plumbing handle_handshake
        // needs (unused on the happy path, but part of its signature).
        let cfg = WorkerConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            max_payload: 1 << 20,
            max_message_bytes: 1 << 20,
            max_head_bytes: 16_384,
            handshake_timeout_ms: 10_000,
            high_water: 1 << 20,
            mode: Mode::Echo,
            rest_handoff: None,
            worker_id: 0,
            broadcast: None,
            per_worker_budget: 0,
            inflight_slot: None,
            accepted_slot: None,
            codel_dropped_slot: None,
            drophead_dropped_slot: None,
            mailbox_dropped_slot: None,
            codel: crate::transport::conn::CodelParams::DISABLED,
            budget_factor: None,
            shutdown_grace_ms: 0,
            tls: None,
        };
        let (dirty_tx, _dirty_rx) = std::sync::mpsc::channel::<usize>();
        let mailbox_waker = Arc::new(mio::Waker::new(poll.registry(), WORKER_WAKER).unwrap());
        let (resolved_tx, _resolved_rx) = std::sync::mpsc::channel::<ResolvedApp>();
        let mut next_gen = 0u64;
        let mut wheel = TimerWheel::with_timeouts(0, 0);

        // The client writes its ClientHello (small — the bloated SAN list is
        // only in the server's certificate).
        while client.wants_write() {
            client.write_tls(&mut client_sock).unwrap();
        }

        // Drive exactly like `run`'s event loop: readable → handle_readable,
        // writable → handle_writable; pump the raw client between polls.
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut writable_drives = 0usize;
        let mut sent_head = false;
        let mut plaintext = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            assert!(
                Instant::now() < deadline,
                "blocked TLS handshake never completed: the mid-flight write was \
                 never re-driven (G2: WRITABLE not armed in Handshaking state)"
            );
            poll.poll(&mut events, Some(Duration::from_millis(5)))
                .unwrap();
            for ev in events.iter() {
                if ev.token() == WORKER_WAKER {
                    continue;
                }
                let k = ev.token().0;
                if ev.is_readable() {
                    let action = handle_readable(
                        &poll,
                        &mut conns,
                        k,
                        &cfg,
                        0,
                        &dirty_tx,
                        &mailbox_waker,
                        &resolved_tx,
                        &mut next_gen,
                        &mut wheel,
                    );
                    assert_ne!(action, Action::Close, "handshake readable handling");
                }
                if ev.is_writable() && conns.contains(k) {
                    writable_drives += 1;
                    let action = handle_writable(
                        &poll,
                        &mut conns,
                        k,
                        &cfg,
                        0,
                        &dirty_tx,
                        &mailbox_waker,
                        &resolved_tx,
                        &mut next_gen,
                        &mut wheel,
                    );
                    assert_ne!(action, Action::Close, "handshake writable handling");
                }
            }
            // Pump the raw client: ingest ciphertext, run TLS, send its flight.
            tlsup::pump_client(&mut client, &mut client_sock);
            // TLS done → send the WS upgrade head as TLS application data.
            if !client.is_handshaking() && !sent_head {
                use std::io::Write as _;
                client
                    .writer()
                    .write_all(G2_UPGRADE_HEAD.as_bytes())
                    .expect("queue WS upgrade head");
                sent_head = true;
            }
            // Collect any server plaintext (the 101 response).
            loop {
                match client.reader().read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => plaintext.extend_from_slice(&chunk[..n]),
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                    Err(e) => panic!("client plaintext read failed: {e}"),
                }
            }
            if sent_head && plaintext.starts_with(b"HTTP/1.1 101") {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }

        assert!(
            writable_drives > 0,
            "the blocked flight must be completed by writable-event drives"
        );
        assert_eq!(conns[key].conn.state, ConnState::Open);
        assert!(
            !conns[key].conn.writable_armed(),
            "WRITABLE interest cleared once the flight + 101 drained"
        );
    }

    // ---- U3 / Task 7.3: per-version sink frames (two-version fixture) ---------

    /// The fixture's connection table: the same `ConnIndex` shape the
    /// fan-out bench drives (a slab of `Open` connections — the seam the
    /// production `slab::Slab<Entry>` impl resolves with its session check).
    struct FixtureConns {
        slab: slab::Slab<Connection>,
    }

    impl ConnIndex for FixtureConns {
        fn open_conn(&mut self, token: usize) -> Option<&mut Connection> {
            let conn = self.slab.get_mut(token)?;
            (conn.state == ConnState::Open).then_some(conn)
        }
    }

    /// One accepted loopback socket wrapped as a `Connection` (the bench's
    /// setup shape, minus the port-pool concerns of 100k scales): connect,
    /// accept, nonblocking, mio. Returns (connection, client half) so the
    /// test can read the bytes the drain actually flushed to the subscriber.
    fn fixture_conn(listener: &std::net::TcpListener) -> (Connection, std::net::TcpStream) {
        use std::os::fd::OwnedFd;
        let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (accepted, _) = listener.accept().unwrap();
        accepted.set_nonblocking(true).unwrap();
        let mut conn = Connection::new(mio::net::TcpStream::from(OwnedFd::from(accepted)), 1 << 20);
        conn.state = ConnState::Open; // model an established session
        (conn, client)
    }

    /// The finished WS frame for `event` at `version` — exactly what
    /// `fanout::frames_for` builds per slot (encode through the wire seam,
    /// wrap with the shared framing helper), reconstructed here so the
    /// assertion is over EXPECTED bytes, not the builder's own output.
    fn expected_frame(version: u8, event: &ServerEvent) -> Bytes {
        let json = crate::protocol::wire::encode(version, event);
        let mut buf = BytesMut::new();
        frame::encode_text(&mut buf, json.as_bytes());
        buf.freeze()
    }

    /// U3 / Task 7.3 fixture: TWO subscribers with DIFFERENT negotiated
    /// protocol versions on the SAME broadcast each receive THEIR version's
    /// frame bytes.
    ///
    /// The two-version world is built the test-safe way: the sink's frame
    /// builder is parameterized by the version list, so the fixture passes a
    /// LOCAL `&[7, 8]` slice (production passes `wire::ACTIVE_VERSIONS`) —
    /// no global `ACTIVE_VERSIONS` override, no state to leak across tests.
    /// Version 8 is a `#[cfg(test)]` wire arm whose bytes differ
    /// deterministically from v7's (`[8,<v7 json>]`), so a wrong-slot
    /// delivery cannot pass by aliasing.
    ///
    /// Delivery is asserted END TO END: after the drain, each connection is
    /// flushed and the exact bytes on each subscriber's socket must equal
    /// that subscriber's version's frame — and the two must differ.
    #[test]
    fn drain_delivers_each_subscriber_its_negotiated_versions_frame() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let (conn7, client7) = fixture_conn(&listener);
        let (conn8, client8) = fixture_conn(&listener);

        let mut conns = FixtureConns {
            slab: slab::Slab::new(),
        };
        let token7 = conns.slab.insert(conn7);
        let token8 = conns.slab.insert(conn8);

        // The worker's single-layout subscriber index: `(app, channel) →
        // socket_id → (slab token, negotiated version)`. One subscriber
        // negotiated v7, the other the test v8 — the entry values carry the
        // tokens the fixture's slab assigned.
        let sid7 = SocketId::generate();
        let sid8 = SocketId::generate();
        let app: Arc<str> = Arc::from("app");
        let channel: Arc<str> = Arc::from("presence-room-42");
        let mut local_subs: LocalSubs = HashMap::new();
        let mut subs: HashMap<SocketId, (usize, u8)> = HashMap::new();
        subs.insert(sid7, (token7, 7));
        subs.insert(sid8, (token8, 8));
        local_subs.insert((app.clone(), channel.clone()), subs);

        let event = ServerEvent::ChannelEvent {
            channel: "presence-room-42".to_string(),
            event: "client-message".to_string(),
            data: serde_json::json!({"msg": "per-version fan-out"}),
            user_id: None,
        };

        // The production builder over a TWO-version list (test-only; the
        // production call site passes `wire::ACTIVE_VERSIONS`).
        let frames = crate::transport::fanout::frames_for(&[7, 8], &event);
        // Fixture precondition: the two versions' frames are distinct bytes
        // (otherwise the delivery assertion could pass on the wrong slot).
        assert_ne!(frames[0].1, frames[1].1);

        let (tx, rx) = std::sync::mpsc::sync_channel(4);
        tx.send(crate::transport::fanout::BroadcastMsg {
            app,
            channel,
            frames,
            except: None,
        })
        .unwrap();
        drop(tx);

        let mut touched: HashSet<usize> = HashSet::new();
        let mut inflight: u64 = 0;
        let mut outbound: u64 = 0;
        let mut drophead: u64 = 0;
        drain_broadcast_inbox(
            &rx,
            &local_subs,
            &mut conns,
            64 << 20, // Normal band: every subscriber delivered
            &mut inflight,
            &mut outbound,
            &mut drophead,
            None,
            1,
            &mut touched,
            &HashSet::new(),
        );
        assert_eq!(touched.len(), 2, "both subscribers queued onto");

        // Flush + read each subscriber's socket: the EXACT frame bytes of
        // that subscriber's negotiated version, and nothing else.
        let expect7 = expected_frame(7, &event);
        let expect8 = expected_frame(8, &event);
        for (token, mut client, expect) in
            [(token7, client7, &expect7), (token8, client8, &expect8)]
        {
            let conn = conns.slab.get_mut(token).unwrap();
            assert_eq!(conn.flush(1), WriteStatus::Drained, "loopback flush");
            let mut got = vec![0u8; expect.len()];
            use std::io::Read as _;
            client
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            client.read_exact(&mut got).expect("read flushed frame");
            assert_eq!(&got[..], &expect[..], "subscriber's own version frame");
        }
    }

    /// A `BroadcastMsg` carrying no frames is skipped, not indexed into.
    /// `frames_for` returns an empty vec for an empty version list, and both
    /// it and the drain are public seams, so an empty `frames` must not take
    /// the whole core's connection set down with it.
    ///
    /// A second, normal message on the SAME channel and subscriber follows it
    /// through the same drain: its delivery is what proves the empty message
    /// really did reach a live, deliverable subscriber rather than being
    /// skipped by an unwired fixture.
    #[test]
    fn drain_skips_a_broadcast_with_no_frames() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let (conn, mut client) = fixture_conn(&listener);

        let mut conns = FixtureConns {
            slab: slab::Slab::new(),
        };
        let token = conns.slab.insert(conn);

        let app: Arc<str> = Arc::from("app");
        let channel: Arc<str> = Arc::from("room-1");
        let mut local_subs: LocalSubs = HashMap::new();
        let mut subs: HashMap<SocketId, (usize, u8)> = HashMap::new();
        subs.insert(SocketId::generate(), (token, 7));
        local_subs.insert((app.clone(), channel.clone()), subs);

        let event = ServerEvent::ChannelEvent {
            channel: "room-1".to_string(),
            event: "client-message".to_string(),
            data: serde_json::json!({"msg": "after the empty one"}),
            user_id: None,
        };

        let (tx, rx) = std::sync::mpsc::sync_channel(4);
        tx.send(crate::transport::fanout::BroadcastMsg {
            app: app.clone(),
            channel: channel.clone(),
            frames: Vec::new(),
            except: None,
        })
        .unwrap();
        tx.send(crate::transport::fanout::BroadcastMsg {
            app,
            channel,
            frames: crate::transport::fanout::frames_for(&[7], &event),
            except: None,
        })
        .unwrap();
        drop(tx);

        let mut touched: HashSet<usize> = HashSet::new();
        let mut inflight: u64 = 0;
        let mut outbound: u64 = 0;
        let mut drophead: u64 = 0;
        drain_broadcast_inbox(
            &rx,
            &local_subs,
            &mut conns,
            64 << 20,
            &mut inflight,
            &mut outbound,
            &mut drophead,
            None,
            1,
            &mut touched,
            &HashSet::new(),
        );

        assert_eq!(touched, HashSet::from([token]), "only the framed message");
        let expect = expected_frame(7, &event);
        assert_eq!(
            inflight,
            expect.len() as u64,
            "the empty message queued nothing"
        );

        let conn = conns.slab.get_mut(token).unwrap();
        assert_eq!(conn.flush(1), WriteStatus::Drained, "loopback flush");
        let mut got = vec![0u8; expect.len()];
        use std::io::Read as _;
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        client.read_exact(&mut got).expect("read flushed frame");
        assert_eq!(&got[..], &expect[..], "exactly the one framed broadcast");
    }

    // ---- Issue #48: the two saturation producers have separate lifetimes ------

    /// One fixture worker: the sink slot it OWNS (carrying its budget bit), the
    /// hand-off inbox it drains, and `subs` subscribers on one channel that
    /// never read — so everything queued onto them stays queued.
    struct FixtureWorker {
        slot: Arc<WorkerSlot>,
        rx: std::sync::mpsc::Receiver<BroadcastMsg>,
        conns: FixtureConns,
        local_subs: LocalSubs,
        app: Arc<str>,
        channel: Arc<str>,
        inflight: u64,
        _clients: Vec<std::net::TcpStream>,
    }

    impl FixtureWorker {
        fn new(listener: &std::net::TcpListener, subs: usize) -> Self {
            let mut conns = FixtureConns {
                slab: slab::Slab::new(),
            };
            let mut clients = Vec::with_capacity(subs);
            let mut subscribers: HashMap<SocketId, (usize, u8)> = HashMap::new();
            for _ in 0..subs {
                let (conn, client) = fixture_conn(listener);
                let token = conns.slab.insert(conn);
                subscribers.insert(SocketId::generate(), (token, 7));
                clients.push(client);
            }
            let app: Arc<str> = Arc::from("app");
            let channel: Arc<str> = Arc::from("budget-chan");
            let mut local_subs: LocalSubs = HashMap::new();
            local_subs.insert((app.clone(), channel.clone()), subscribers);
            let (tx, rx) = std::sync::mpsc::sync_channel(8);
            Self {
                slot: Arc::new(WorkerSlot {
                    tx,
                    waker: std::sync::OnceLock::new(),
                    dropped: AtomicU64::new(0),
                    budget_saturated: AtomicBool::new(false),
                }),
                rx,
                conns,
                local_subs,
                app,
                channel,
                inflight: 0,
                _clients: clients,
            }
        }

        /// Hand this worker one broadcast, returning the framed byte length —
        /// exactly what ONE subscriber's enqueue adds to `inflight`.
        fn publish(&self) -> u64 {
            let frames = crate::transport::fanout::frames_for(
                &[7],
                &ServerEvent::ChannelEvent {
                    channel: self.channel.to_string(),
                    event: "flood".to_string(),
                    data: serde_json::json!({"pad": "0123456789abcdef"}),
                    user_id: None,
                },
            );
            let len = frames[0].1.len() as u64;
            self.slot
                .tx
                .send(BroadcastMsg {
                    app: self.app.clone(),
                    channel: self.channel.clone(),
                    frames,
                    except: None,
                })
                .unwrap();
            len
        }

        /// One worker-loop pass: drain the inbox to empty, then run the SAME
        /// post-drain flag maintenance the real loop runs.
        fn loop_pass(&mut self, flag: &SaturationFlag, budget: u64) {
            let mut touched: HashSet<usize> = HashSet::new();
            let mut outbound: u64 = 0;
            let mut drophead: u64 = 0;
            drain_broadcast_inbox(
                &self.rx,
                &self.local_subs,
                &mut self.conns,
                budget,
                &mut self.inflight,
                &mut outbound,
                &mut drophead,
                Some(&self.slot.budget_saturated),
                1,
                &mut touched,
                &HashSet::new(),
            );
            flag.clear_inbox_full();
            update_budget_pressure(&self.slot.budget_saturated, self.inflight, budget);
        }
    }

    fn flag_over(slots: Vec<Arc<WorkerSlot>>) -> SaturationFlag {
        let flag = SaturationFlag::default();
        flag.bind_workers(Arc::new(slots));
        flag
    }

    /// Issue #48: a worker that blows its byte budget mid-fan-out must still
    /// read as saturated to an ADMISSION-PATH consumer AFTER the drain returns.
    /// The pre-fix code set the shared bit inside the drain and stored `false`
    /// over it on the very next statement, so no consumer could ever observe it.
    #[test]
    fn budget_saturation_is_observable_after_the_drain_returns() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut w = FixtureWorker::new(&listener, 3);
        let flag = flag_over(vec![w.slot.clone()]);

        // One frame's worth of budget: the first subscriber's enqueue puts the
        // worker AT 100%, so the rest of the fan-out classifies `Saturated`.
        let budget = w.publish();
        w.loop_pass(&flag, budget);

        assert!(
            w.inflight >= budget,
            "the fan-out must have blown the budget"
        );
        assert!(
            flag.is_saturated(),
            "budget pressure must survive the drain that observed it"
        );
    }

    /// Issue #48, the cross-worker stomp: the flag is node-wide, so a SECOND
    /// worker completing a trivially-empty drain used to clear what the first
    /// had just set. Each worker now owns its own bit.
    #[test]
    fn an_idle_workers_drain_cannot_clear_another_workers_pressure() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut busy = FixtureWorker::new(&listener, 3);
        let mut idle = FixtureWorker::new(&listener, 0);
        let flag = flag_over(vec![busy.slot.clone(), idle.slot.clone()]);

        let budget = busy.publish();
        busy.loop_pass(&flag, budget);
        assert!(flag.is_saturated());

        // The idle worker's loop pass: empty inbox, zero inflight, nothing owed.
        idle.loop_pass(&flag, budget);
        assert_eq!(idle.inflight, 0);
        assert!(
            flag.is_saturated(),
            "an idle worker's drain must not clear another worker's budget pressure"
        );
    }

    /// Producer 1 is unchanged: a publisher's full hand-off raises the
    /// inbox-full half, and a drain to empty releases it.
    #[test]
    fn inbox_full_still_clears_once_the_inbox_drains_empty() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut w = FixtureWorker::new(&listener, 1);
        let flag = flag_over(vec![w.slot.clone()]);

        flag.set_inbox_full();
        assert!(flag.is_saturated());

        // A budget nothing here can reach keeps producer 2 out of the verdict.
        w.publish();
        w.loop_pass(&flag, 64 << 20);
        assert!(
            !flag.is_saturated(),
            "a drain to empty must release the inbox-full half"
        );
    }

    // ---- #58 / #60: fragmented-TEXT reassembly bounds + byte accounting ------

    /// A slab entry over a real registered loopback socket, without a session:
    /// enough to drive the fragmentation state machine, which touches `conn`
    /// only for the reassembly accounting. `_client` keeps the peer end alive.
    struct FragmentFixture {
        poll: Poll,
        entry: Entry,
        _client: std::net::TcpStream,
    }

    fn fragment_fixture() -> FragmentFixture {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        server.set_nonblocking(true).unwrap();
        let mut conn = Connection::new(mio::net::TcpStream::from_std(server), 1 << 20);
        let poll = Poll::new().unwrap();
        poll.registry()
            .register(conn.stream_mut(), Token(0), Interest::READABLE)
            .unwrap();
        FragmentFixture {
            poll,
            entry: Entry {
                conn,
                inbuf: BytesMut::new(),
                token: Token(0),
                session: None,
                fragment: None,
                pending_establish: None,
            },
            _client: client,
        }
    }

    impl FragmentFixture {
        fn feed(&mut self, opcode: OpCode, fin: bool, payload: Vec<u8>, cap: usize) -> Action {
            let f = frame::Frame {
                fin,
                opcode,
                payload: bytes::Bytes::from(payload),
            };
            dispatch_frames(&self.poll, &mut self.entry, vec![f], cap, 0)
        }
    }

    /// Issue #58: an opening fragment over the per-message cap must never be
    /// buffered. Pre-fix it was copied wholesale onto the entry (bounded only by
    /// the 1 MiB per-frame ceiling) and billed to nothing.
    #[test]
    fn an_oversize_first_text_fragment_buffers_nothing() {
        let mut fx = fragment_fixture();

        let action = fx.feed(OpCode::Text, false, vec![b'x'; 64 * 1024], 1024);

        assert_eq!(action, Action::Keep);
        assert!(matches!(fx.entry.fragment, Some(Fragment::Oversize)));
        assert_eq!(fx.entry.conn.reassembly_bytes(), 0);
        assert_eq!(fx.entry.conn.take_inflight_delta(), 0);
    }

    /// Issue #58, the memory pin: a peer opens an oversize message and keeps
    /// feeding it without ever setting FIN. Nothing accumulates across the
    /// fragments, and the connection is never closed.
    #[test]
    fn an_unfinished_oversize_message_accumulates_nothing() {
        let mut fx = fragment_fixture();
        fx.feed(OpCode::Text, false, vec![b'x'; 64 * 1024], 1024);

        for _ in 0..64 {
            assert_eq!(
                fx.feed(OpCode::Continuation, false, vec![b'x'; 64 * 1024], 1024),
                Action::Keep
            );
            assert_eq!(fx.entry.conn.reassembly_bytes(), 0);
        }
        assert_eq!(fx.entry.conn.take_inflight_delta(), 0);
    }

    /// Issue #60: the fragments that follow an overflow are swallowed, not read
    /// as strays — the documented drop is silent and the connection survives.
    #[test]
    fn an_oversize_fragmented_message_is_dropped_without_closing() {
        let mut fx = fragment_fixture();

        assert_eq!(
            fx.feed(OpCode::Text, false, vec![b'a'; 800], 1024),
            Action::Keep
        );
        assert_eq!(
            fx.feed(OpCode::Continuation, false, vec![b'b'; 800], 1024),
            Action::Keep,
            "the overflowing fragment must not close the connection"
        );
        assert!(matches!(fx.entry.fragment, Some(Fragment::Oversize)));
        assert_eq!(
            fx.entry.conn.reassembly_bytes(),
            0,
            "the partial message's bytes are released on overflow"
        );
        assert_eq!(
            fx.feed(OpCode::Continuation, true, vec![b'c'; 8], 1024),
            Action::Keep,
            "the final fragment of a dropped message must not close the connection"
        );
        assert!(fx.entry.fragment.is_none());
    }

    /// The reassembly buffer of a legitimate message is billed to the
    /// connection, so the worker's `inflight_bytes` — and the shedding built on
    /// it — sees memory held on the peer's behalf.
    #[test]
    fn an_open_text_fragment_is_billed_to_the_connection() {
        let mut fx = fragment_fixture();

        fx.feed(OpCode::Text, false, vec![b'a'; 600], 4096);
        assert_eq!(fx.entry.conn.reassembly_bytes(), 600);
        assert_eq!(fx.entry.conn.accounted_bytes(), 600);
        assert_eq!(fx.entry.conn.take_inflight_delta(), 600);

        fx.feed(OpCode::Continuation, false, vec![b'b'; 200], 4096);
        assert_eq!(fx.entry.conn.reassembly_bytes(), 800);
        assert_eq!(
            fx.entry.conn.take_inflight_delta(),
            200,
            "an append bills only its growth"
        );

        fx.feed(OpCode::Continuation, false, vec![b'c'; 4096], 4096);
        assert_eq!(fx.entry.conn.reassembly_bytes(), 0);
        assert_eq!(
            fx.entry.conn.take_inflight_delta(),
            -800,
            "dropping the message returns its bytes to the budget"
        );
    }

    /// An ignored fragmented BINARY message keeps its ignore-mode across
    /// Continuations and bills nothing.
    #[test]
    fn a_fragmented_binary_message_is_ignored_and_bills_nothing() {
        let mut fx = fragment_fixture();

        assert_eq!(
            fx.feed(OpCode::Binary, false, vec![0u8; 4096], 1024),
            Action::Keep
        );
        assert!(matches!(fx.entry.fragment, Some(Fragment::Binary)));
        assert_eq!(
            fx.feed(OpCode::Continuation, true, vec![0u8; 4096], 1024),
            Action::Keep
        );
        assert!(fx.entry.fragment.is_none());
        assert_eq!(fx.entry.conn.take_inflight_delta(), 0);
    }

    /// RFC 6455 §5.4: a Continuation with no message open still fails the
    /// connection — the ignore-modes must not soften the stray guard.
    #[test]
    fn a_stray_continuation_still_closes_the_connection() {
        let mut fx = fragment_fixture();

        assert_eq!(
            fx.feed(OpCode::Continuation, true, b"stray".to_vec(), 1024),
            Action::Close
        );
    }

    /// Hysteresis: the bit is raised at the `Saturated` boundary and released
    /// only back in [`BUDGET_PRESSURE_RELEASE_BAND`]. The band between the two
    /// neither sets nor clears, so the signal cannot flap at either edge.
    #[test]
    fn budget_pressure_holds_between_the_set_and_release_bands() {
        const BUDGET: u64 = 1_000;
        let bit = AtomicBool::new(false);
        let raised = || bit.load(Ordering::Relaxed);

        update_budget_pressure(&bit, 799, BUDGET);
        assert!(!raised(), "below the release band: stays down");

        // Climbing through the hold band does NOT raise it — only ≥100% does.
        for inflight in [800, 949, 950, 999] {
            update_budget_pressure(&bit, inflight, BUDGET);
            assert!(!raised(), "{inflight} is under budget; must not raise");
        }
        update_budget_pressure(&bit, BUDGET, BUDGET);
        assert!(raised(), "at 100% the budget bit is raised");

        // Falling back through the same band does NOT release it.
        for inflight in [999, 950, 900, 800] {
            update_budget_pressure(&bit, inflight, BUDGET);
            assert!(
                raised(),
                "{inflight} is inside the hold band; must not release"
            );
        }
        update_budget_pressure(&bit, 799, BUDGET);
        assert!(!raised(), "back under the release band: released");
    }

    // ---- teardown paths keep `inflight_bytes` exact --------------------------

    /// The worker-total bundle every teardown path has to keep in step.
    struct Totals {
        inflight: u64,
        outbound: u64,
        codel: u64,
        drophead: u64,
        local_subs: LocalSubs,
        wheel: TimerWheel,
        conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
        app_registry: Arc<crate::adapter::app_registry::AppRegistry>,
        node_conns: Arc<AtomicUsize>,
    }

    impl Totals {
        fn new() -> Self {
            Totals {
                inflight: 0,
                outbound: 0,
                codel: 0,
                drophead: 0,
                local_subs: HashMap::new(),
                wheel: TimerWheel::new(),
                conn_counts: Arc::new(DashMap::new()),
                app_registry: Arc::new(crate::adapter::app_registry::AppRegistry::new()),
                node_conns: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    /// One `Open` connection over `stream`, in a slab and registered with
    /// `poll` exactly as `accept_ready` leaves it.
    fn slab_with_conn(poll: &Poll, stream: mio::net::TcpStream) -> (slab::Slab<Entry>, usize) {
        let mut conns: slab::Slab<Entry> = slab::Slab::new();
        let key = conns.insert(Entry {
            conn: Connection::new(stream, 1 << 20),
            inbuf: BytesMut::new(),
            token: Token(0),
            session: None,
            fragment: None,
            pending_establish: None,
        });
        let entry = &mut conns[key];
        entry.token = Token(key);
        entry.conn.state = ConnState::Open;
        poll.registry()
            .register(entry.conn.stream_mut(), Token(key), Interest::READABLE)
            .unwrap();
        (conns, key)
    }

    /// An accepted loopback socket whose peer has gone away with an RST, held
    /// until writes on it genuinely fail — the rolling-restart shape, where the
    /// LB drops clients while the node is being stopped.
    fn write_dead_stream() -> mio::net::TcpStream {
        use std::io::Write as _;
        use std::os::fd::OwnedFd;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (accepted, _) = listener.accept().unwrap();
        accepted.set_nonblocking(true).unwrap();
        // Data the client never reads, plus a zero linger, makes its close an
        // RST rather than a FIN.
        (&accepted).write_all(b"unread").unwrap();
        socket2::SockRef::from(&client)
            .set_linger(Some(Duration::ZERO))
            .unwrap();
        drop(client);

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while (&accepted)
            .write(b"probe")
            .err()
            .is_none_or(|e| e.kind() == ErrorKind::WouldBlock)
        {
            assert!(
                std::time::Instant::now() < deadline,
                "the peer's RST never landed"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        mio::net::TcpStream::from(OwnedFd::from(accepted))
    }

    /// Issue #61: the drain must honour the `Close` its flush reports. A peer
    /// that has already RST'd can never receive the 4200 frames, and the flush
    /// arms no WRITABLE interest for them — so leaving it in the slab pins
    /// `inflight_bytes` above zero, the drain's fast exit can never fire, and
    /// shutdown burns the whole grace window on every restart where one peer
    /// has gone away.
    #[test]
    fn drain_removes_a_connection_whose_close_frames_can_never_flush() {
        let poll = Poll::new().unwrap();
        let (mut conns, key) = slab_with_conn(&poll, write_dead_stream());
        let mut t = Totals::new();

        drain_close_connection(
            &poll,
            &mut conns,
            key,
            0,
            &mut t.local_subs,
            &mut t.wheel,
            &mut t.inflight,
            &mut t.outbound,
            &mut t.codel,
            &mut t.drophead,
            &t.conn_counts,
            &t.app_registry,
            &t.node_conns,
            &None,
        );

        assert!(
            conns.is_empty(),
            "a write-dead peer is torn down, not left holding unsendable frames"
        );
        assert_eq!(
            t.inflight, 0,
            "its queued 4200 frames leave the worker total with it"
        );
        assert_eq!(
            t.outbound, 0,
            "its queued 4200 frames leave the outbound total with it too"
        );
    }

    /// Issue #62: a REST handoff must return the connection's queued bytes to
    /// the worker total, exactly as `remove` does. The drain queues its 4200
    /// frames onto `Handshaking` entries too, so a request head arriving
    /// mid-drain hands off a connection with a non-empty queue; bytes left
    /// behind there are a phantom floor no connection holds, which the drain's
    /// `inflight_bytes == 0` exit can never clear again.
    #[test]
    fn rest_handoff_returns_the_connections_queued_bytes() {
        use std::os::fd::OwnedFd;
        let poll = Poll::new().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = std::net::TcpStream::connect(addr).unwrap();
        let (accepted, _) = listener.accept().unwrap();
        accepted.set_nonblocking(true).unwrap();
        let (mut conns, key) =
            slab_with_conn(&poll, mio::net::TcpStream::from(OwnedFd::from(accepted)));
        conns[key].conn.state = ConnState::Handshaking;
        let mut t = Totals::new();

        // The drain's shape: frames queued on a connection that has not flushed.
        queue_shutdown_error(&mut conns, key, 0);
        fold_delta(&mut conns, key, &mut t.inflight, &mut t.outbound);
        let queued = conns[key].conn.out_bytes() as u64;
        assert!(queued > 0, "the drain really queued something");
        assert_eq!(t.inflight, queued);
        assert_eq!(t.outbound, queued);

        handoff_rest(
            &poll,
            &mut conns,
            key,
            &echo_worker_config(addr),
            Vec::new(),
            &mut t.inflight,
            &mut t.outbound,
        );

        assert!(conns.is_empty(), "the entry left the slab");
        assert_eq!(t.inflight, 0, "and its bytes left the worker total with it");
        assert_eq!(t.outbound, 0, "and the outbound total with it too");
    }

    // ---- #74: a partially-received frame is billed to the connection --------

    /// A connected loopback pair: the server half as the worker sees it, plus
    /// the client half for the test to trickle bytes down.
    fn trickle_pair() -> (
        mio::net::TcpStream,
        std::net::TcpStream,
        std::net::SocketAddr,
    ) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::net::TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();
        server.set_nonblocking(true).unwrap();
        (mio::net::TcpStream::from_std(server), client, addr)
    }

    /// The header of a masked client TEXT frame (RFC 6455 §5.2) declaring
    /// `payload_len` bytes, in the 16-bit extended-length form.
    fn masked_text_header(payload_len: u16) -> Vec<u8> {
        let mut head = vec![0x81, 0x80 | 126];
        head.extend_from_slice(&payload_len.to_be_bytes());
        head.extend_from_slice(&[0x37, 0xfa, 0x21, 0x3d]);
        head
    }

    /// Drive the production read path until `done` holds, folding the worker's
    /// `inflight_bytes`/`outbound_bytes` after every pass exactly as the event
    /// loop does.
    fn read_until(
        poll: &Poll,
        conns: &mut slab::Slab<Entry>,
        key: usize,
        cfg: &WorkerConfig,
        inflight: &mut u64,
        outbound: &mut u64,
        done: impl Fn(&Entry) -> bool,
    ) {
        for _ in 0..1000 {
            assert_eq!(handle_frames(poll, &mut conns[key], cfg, 0), Action::Keep);
            fold_delta(conns, key, inflight, outbound);
            if done(&conns[key]) {
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("the peer's bytes never landed");
    }

    /// Issue #74: a peer that sends a large frame's header and then trickles the
    /// payload parks it in `Entry::inbuf` until the frame completes. Pre-fix
    /// those bytes were billed to nothing, so `inflight_bytes` — and the REST
    /// 503 admission path, the `client-*` ingress drop and the graduated shed
    /// bands that read it — could not see memory the peer was holding.
    #[test]
    fn a_partially_received_frame_is_billed_to_the_connection() {
        use std::io::Write as _;

        let poll = Poll::new().unwrap();
        let (server, mut client, addr) = trickle_pair();
        let (mut conns, key) = slab_with_conn(&poll, server);
        let cfg = echo_worker_config(addr);

        let mut partial = masked_text_header(32 * 1024);
        partial.extend_from_slice(&vec![b'x'; 4096]);
        client.write_all(&partial).unwrap();
        client.flush().unwrap();

        let mut inflight = 0u64;
        let mut outbound = 0u64;
        let want = partial.len();
        read_until(
            &poll,
            &mut conns,
            key,
            &cfg,
            &mut inflight,
            &mut outbound,
            |e| e.inbuf.len() >= want,
        );

        let conn = &conns[key].conn;
        assert_eq!(
            conn.out_bytes(),
            0,
            "an incomplete frame draws no reply, so every accounted byte is the buffer's"
        );
        assert_eq!(conn.frame_buffer_bytes(), want);
        assert_eq!(conn.accounted_bytes(), want);
        assert_eq!(inflight, want as u64, "and the worker total carries them");
        assert_eq!(
            outbound, 0,
            "a partial inbound frame owes nothing outbound, so the drain must not wait on it"
        );
    }

    /// The billing follows the buffer back down: completing the frame hands its
    /// payload to the parser and returns those bytes to the worker's budget.
    #[test]
    fn completing_a_frame_returns_its_buffered_bytes() {
        use std::io::Write as _;

        let poll = Poll::new().unwrap();
        let (server, mut client, addr) = trickle_pair();
        let (mut conns, key) = slab_with_conn(&poll, server);
        let cfg = echo_worker_config(addr);

        let payload = vec![b'x'; 512];
        let mut head = masked_text_header(payload.len() as u16);
        head.extend_from_slice(&payload[..256]);
        client.write_all(&head).unwrap();
        client.flush().unwrap();

        let mut inflight = 0u64;
        let mut outbound = 0u64;
        let buffered = head.len();
        read_until(
            &poll,
            &mut conns,
            key,
            &cfg,
            &mut inflight,
            &mut outbound,
            |e| e.inbuf.len() >= buffered,
        );
        assert_eq!(inflight, buffered as u64);
        assert_eq!(
            outbound, 0,
            "still just a buffered partial frame, nothing to send"
        );

        client.write_all(&payload[256..]).unwrap();
        client.flush().unwrap();
        read_until(
            &poll,
            &mut conns,
            key,
            &cfg,
            &mut inflight,
            &mut outbound,
            |e| e.inbuf.is_empty(),
        );

        let conn = &conns[key].conn;
        assert_eq!(conn.frame_buffer_bytes(), 0);
        assert_eq!(
            inflight,
            conn.out_bytes() as u64,
            "only the echoed reply is still owed"
        );
        assert_eq!(
            outbound,
            conn.out_bytes() as u64,
            "the echoed reply is outbound too, once the frame completed"
        );
    }
}
