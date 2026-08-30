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
use bytes::BytesMut;
use dashmap::DashMap;
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
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

/// Test-hooks instrumentation (G5): a live gauge of the worker-local delivery
/// indexes across this process — the number of `(app, channel) → socket_id`
/// membership slots held in every worker's `local_subs` (the sum of every
/// channel set's length). Maintained exactly at the two `local_subs` mutation
/// sites (`reconcile_membership`'s insert/remove and `deindex_connection`'s
/// remove, both keyed off the boolean return of the set operation), so a test
/// can assert the index fully EMPTIES when a connection closes — including the
/// same-batch [subscribe, Close] case, where the close path runs before any
/// reconcile ever saw the subscription. Signed so a bookkeeping bug surfaces as
/// a negative instead of a near-`u64::MAX` positive. Behind `test-hooks` so it
/// is free in release builds.
#[cfg(any(test, feature = "test-hooks"))]
pub static LOCAL_SUBS_SLOTS: AtomicI64 = AtomicI64::new(0);

/// Test-hooks accessor: the current number of `(app, channel) → socket_id`
/// membership slots across this process's workers' `local_subs` indexes (see
/// [`LOCAL_SUBS_SLOTS`]). 0 when every connection has been deindexed.
#[cfg(any(test, feature = "test-hooks"))]
pub fn percore_local_subs_len() -> i64 {
    LOCAL_SUBS_SLOTS.load(Ordering::Relaxed)
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
    pub saturated: Option<Arc<AtomicBool>>,
    /// SP11 §3.6: clustering toggle stamped onto every connection's
    /// [`ConnectionContext`] at session establish. `true` ⇒ this is a clustered
    /// percore node: the single-emit cluster edges (`subscription_count`,
    /// `channel_occupied` / `channel_vacated`) are deferred to the bridge, so the
    /// connection handler suppresses its node-local emits. `false` ⇒ the
    /// not-yet-clustered percore path keeps the node-local handler emits.
    pub clustered: bool,
    /// Task 4.2 (finding D2): the node's cluster bridge handle, for CLUSTER-WIDE
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
    /// layer holds unfragmented events to. An assembled message over this cap
    /// is dropped (and the fragment accumulator reset) WITHOUT closing the
    /// connection; each individual fragment stays bounded by `max_payload`.
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

/// RFC 6455 §5.4: the in-progress fragmented message on this connection.
///
/// `Text` accumulates the payload of a fragmented TEXT message (the only data
/// kind the Pusher protocol carries): Continuation frames append to it and the
/// FIN=1 Continuation dispatches the assembled payload through the normal Text
/// path, resetting this to `None`.
///
/// `Binary` marks an in-progress fragmented BINARY message. Binary is not part
/// of the Pusher protocol — a lone Binary frame is silently ignored — so a
/// fragmented one is ignored the same way: its Continuations are dropped until
/// the FIN=1 Continuation completes the message, after which the state resets.
/// The variant exists only so those Continuations are not mistaken for strays
/// (which fail the connection per §5.4).
enum Fragment {
    Text(Vec<u8>),
    Binary,
}

/// Per-connection slab entry: the [`Connection`] plus its read remainder and,
/// for dispatch workers, the v7 [`Session`] built at handshake completion.
///
/// `inbuf` is empty or tiny when the connection is idle (it only holds bytes
/// that arrived mid-frame). During [`ConnState::Handshaking`] it doubles as the
/// head-accumulation buffer until [`handshake::read_head`] returns something
/// other than [`HeadResult::NeedMore`]. Its growth while Handshaking is
/// bounded by `WorkerConfig::max_head_bytes` (G3): past the cap `read_head`
/// returns `Bad` and the connection closes.
struct Entry {
    conn: Connection,
    inbuf: BytesMut,
    /// The [`Token`] this connection is registered under (== `Token(slab_key)`).
    token: Token,
    /// v7 protocol state; `None` for echo workers and pre-handshake connections.
    session: Option<Session>,
    /// RFC 6455 §5.4: in-progress fragmented message, `Some` once a FIN=0 Text
    /// or Binary frame has opened one (see [`Fragment`]). Reset — and a partial
    /// TEXT message dropped — when the assembled size would exceed the
    /// per-message cap (`max_message_bytes`); the state dies with this `Entry`
    /// when the connection closes.
    fragment: Option<Fragment>,
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
    result: Result<crate::app::AppLookup, crate::app::AppLookupError>,
}

/// Build a `mio` listener bound to `addr` with `SO_REUSEADDR` + `SO_REUSEPORT`
/// set before bind. `SO_REUSEPORT` lets every per-core worker bind the SAME
/// `bind:port` independently; the kernel then load-balances incoming connections
/// across the workers' listener sockets (one accept queue per worker).
fn reuseport_listener(addr: std::net::SocketAddr) -> std::io::Result<TcpListener> {
    // Bind with a bounded retry on `AddrInUse`. `SO_REUSEADDR` already covers a port
    // in `TIME_WAIT`, but a port can still be briefly held by another holder: a fast
    // restart racing the previous instance's teardown, or a test harness whose
    // ephemeral-port probe just released the port a moment before this bind (a TOCTOU
    // window). Retry a few times over ~250ms before failing loud, so a transient
    // conflict doesn't abort startup while a genuine conflict still surfaces clearly.
    // A fresh socket is required per attempt — a socket whose bind failed can't rebind.
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
    let drophead_dropped_slot = cfg.drophead_dropped_slot.clone();
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
    // G8: worker-local accumulator for drop-head evictions; mirrored into
    // `drophead_dropped_slot` (same pattern as the CoDel accumulator above).
    let mut drophead_dropped_total: u64 = 0;

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

    // Task 4.2 (D2): the cluster bridge handle for the close-side capacity
    // release. `None` for echo workers and non-clustered dispatch workers.
    let cluster: Option<crate::cluster::bridge::ClusterHandle> = match &cfg.mode {
        Mode::Dispatch(env) => env.cluster.clone(),
        Mode::Echo => None,
    };

    let mut events = Events::with_capacity(1024);
    let mut conns: slab::Slab<Entry> = slab::Slab::new();

    // Adaptive poll timeout (G1): poll non-blocking only when the previous
    // iteration did real work; when idle, block up to 50ms to avoid spinning.
    // Queued out-bytes do NOT force a 0ms poll — see the in-loop comment for
    // why that would busy-spin on a backpressured connection.
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
    // listener, queue a `pusher:error` 4200 + WS Close(4200) on all open connections).
    // `drain_deadline` is the absolute Instant after which we force-close regardless
    // of inflight bytes.
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
                    // G8: fold this connection's drop counters too — the queued
                    // 4200 frames may have evicted older frames (drop-head) and
                    // the flush may have CoDel-dropped stale ones. Uniform with
                    // every other queue/flush site.
                    fold_codel(&mut conns, k, &mut codel_dropped_total);
                    fold_drophead(&mut conns, k, &mut drophead_dropped_total);
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
            // else: fall through — the rest of the loop polls writable events and
            // flushes the queued Close frames + any pending out-bytes. The
            // Close-queue flush armed WRITABLE on every still-backpressured
            // connection, so the poll wakes on the next drain event (or the
            // 50ms idle tick, whichever comes first). We re-check
            // inflight_bytes/deadline each iteration.
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

        // G1 invariant: any connection with queued out-bytes MUST hold WRITABLE
        // interest in the poll registry. This is what makes the idle 50ms poll
        // safe for a backpressured connection — the kernel wakes the loop with
        // a writable event the moment the socket drains, so queued bytes can
        // never be stranded behind a sleeping poll. Every queue site flushes
        // via `flush_and_arm` before control returns to the loop top, arming
        // WRITABLE on `WouldBlock`, so this holds by construction; a violation
        // means a queue path forgot to arm.
        debug_assert!(
            conns
                .iter()
                .all(|(_, e)| e.conn.out_bytes() == 0 || e.conn.writable_armed()),
            "connection has queued out-bytes but no WRITABLE interest armed; \
             the idle poll could strand its backlog"
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
        // G8: mirror drop-head eviction total into the shared slot (O(1), only
        // on actual evictions) — same pattern as the CoDel mirror above.
        if drophead_dropped_total > 0 {
            if let Some(slot) = &drophead_dropped_slot {
                slot.store(drophead_dropped_total, Ordering::Relaxed);
            }
        }
        // Adaptive poll timeout (G1): poll non-blocking ONLY when the previous
        // iteration did real work, so cross-worker mailbox deliveries drain
        // promptly under load; otherwise block up to 50ms (which also bounds
        // how long `shutdown` goes unchecked). Queued out-bytes deliberately do
        // NOT force a 0ms poll: mio is level-triggered, so a backpressured
        // connection (full send buffer) produces NO readiness event and a 0ms
        // poll on `inflight_bytes > 0` would busy-spin the whole core. This is
        // safe because every connection with queued bytes holds WRITABLE
        // interest (armed by `flush_and_arm` on `WouldBlock`; asserted at the
        // loop top) — the kernel wakes the loop the moment the socket drains.
        // A cross-connection mailbox send never waits for this idle poll: it
        // wakes the WORKER_WAKER and the selective drain delivers it on the
        // next pass.
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
                    // G3 (slowloris): the ABSOLUTE handshake deadline every new
                    // connection is born with — accept time plus the configured
                    // timeout. `None` when disabled (`0`) or on echo workers
                    // (whose `due()` loop never runs). It lives on the wheel's
                    // SEPARATE handshake side table, so inbound dribble (the
                    // `touch` every readable event does) can never postpone it.
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
                            &node_conns,
                            &cluster,
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
                            &mut wheel,
                        ) {
                            Action::Close => {
                                // G8: fold this connection's drop counters before
                                // teardown — the readable batch queued reject/close
                                // frames and flushed them, which can evict
                                // (drop-head) or CoDel-drop. Without this fold the
                                // counts die with the slab entry.
                                fold_codel(&mut conns, key, &mut codel_dropped_total);
                                fold_drophead(&mut conns, key, &mut drophead_dropped_total);
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
                                    &node_conns,
                                    &cluster,
                                );
                                continue;
                            }
                            Action::Handoff(prefix) => {
                                // A REST handoff is not a WS session; drop its
                                // (spurious) wheel entry so it can't fire later. A
                                // REST head queued nothing, so any folded delta is
                                // 0; fold anyway so a removed conn never leaks.
                                fold_delta(&mut conns, key, &mut inflight_bytes);
                                fold_codel(&mut conns, key, &mut codel_dropped_total);
                                fold_drophead(&mut conns, key, &mut drophead_dropped_total);
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
                                fold_drophead(&mut conns, key, &mut drophead_dropped_total);
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
                        // INCREMENTAL INFLIGHT: the flush sent bytes out; fold the
                        // (negative) delta before any close/handoff so the count
                        // is exact.
                        fold_delta(&mut conns, key, &mut inflight_bytes);
                        fold_codel(&mut conns, key, &mut codel_dropped_total);
                        fold_drophead(&mut conns, key, &mut drophead_dropped_total);
                        match action {
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
                                    &node_conns,
                                    &cluster,
                                );
                            }
                            // G2: a TLS handshake that completed on the WRITABLE
                            // path can yield a REST head exactly as the readable
                            // path does — its plaintext was waiting behind the
                            // blocked flight.
                            Action::Handoff(prefix) => {
                                wheel.remove(key);
                                handoff_rest(&poll, &mut conns, key, &cfg, prefix);
                            }
                            Action::Keep => {
                                // A session established by the writable-path
                                // handshake drive: reconcile its (empty initial)
                                // membership the same way the readable arm does,
                                // keeping the paths symmetric.
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
                &mut drophead_dropped_total,
                saturated.as_ref(),
                now_ns,
                &conn_counts,
                &app_registry,
                &node_conns,
                &cluster,
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
                        match queue_ping(&poll, &mut conns, key, now_ns) {
                            Some(action) => {
                                // INCREMENTAL INFLIGHT: the ping was queued +
                                // flushed; fold this connection's net delta
                                // into the total.
                                fold_delta(&mut conns, key, &mut inflight_bytes);
                                if action == Action::Close {
                                    // The ping flush failed (dead peer or a
                                    // failed re-registration): reap the
                                    // connection NOW — the queued ping bytes
                                    // would otherwise sit behind a poll
                                    // interest that never fires for a dead
                                    // socket.
                                    fold_codel(&mut conns, key, &mut codel_dropped_total);
                                    fold_drophead(&mut conns, key, &mut drophead_dropped_total);
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
                                        &node_conns,
                                        &cluster,
                                    );
                                } else {
                                    // Ping queued: arm the pong-timeout close
                                    // deadline.
                                    wheel.mark_ping_sent(key, now_ms);
                                }
                                work = true;
                            }
                            None => {
                                // The connection vanished (or had no session):
                                // drop its LIVENESS timer so the entry doesn't
                                // linger — but keep any armed ABSOLUTE deadline
                                // (handshake, and for parked conns the eventual
                                // lifetime) intact: a pre-session connection
                                // whose spurious idle timer fired here must
                                // still be reaped by the handshake deadline.
                                wheel.clear_liveness(key);
                            }
                        }
                    }
                    Due::Close4201(key) => {
                        send_close_4201(&poll, &mut conns, key, now_ns);
                        // INCREMENTAL INFLIGHT: the 4201 close frame was queued +
                        // flushed; fold the net delta before `remove` subtracts any
                        // bytes still queued on the connection being torn down.
                        fold_delta(&mut conns, key, &mut inflight_bytes);
                        fold_codel(&mut conns, key, &mut codel_dropped_total);
                        fold_drophead(&mut conns, key, &mut drophead_dropped_total);
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
                            &node_conns,
                            &cluster,
                        );
                        work = true;
                    }
                    Due::Close4202(key) => {
                        // Max connection lifetime reached: emit the in-band
                        // `pusher:error` 4202 first, then the WS Close 4202
                        // (same belt-and-suspenders convention as the drain
                        // path's 4200), and tear the connection down through
                        // the normal `remove` close path.
                        queue_lifetime_error(&mut conns, key, now_ns);
                        send_close_4202(&poll, &mut conns, key, now_ns);
                        // INCREMENTAL INFLIGHT: mirror the 4201 arm — fold the
                        // queued/flushed delta before `remove` subtracts the
                        // connection's still-queued bytes.
                        fold_delta(&mut conns, key, &mut inflight_bytes);
                        fold_codel(&mut conns, key, &mut codel_dropped_total);
                        fold_drophead(&mut conns, key, &mut drophead_dropped_total);
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
                            &node_conns,
                            &cluster,
                        );
                        work = true;
                    }
                    Due::HandshakeTimeout(key) => {
                        // G3 (slowloris) reap: the connection never completed
                        // its handshake within `handshake_timeout_ms` of
                        // accept. No WS session exists (maybe not even a 101),
                        // so there is no protocol close to emit — tear the TCP
                        // connection down through the normal `remove` path,
                        // which reclaims the fd, the slab slot and the wheel
                        // entries. Counters need no fixing here: BOTH
                        // `node_conns` and the per-app `conn_counts` are taken
                        // only in `finish_establish` (paired synchronously with
                        // `session = Some(..)`), so a pre-session connection
                        // holds none — `remove`'s `if let Some(session)` guard
                        // correctly decrements nothing.
                        let pre_session =
                            conns.get(key).is_some_and(|entry| entry.session.is_none());
                        if pre_session {
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
                                &node_conns,
                                &cluster,
                            );
                        } else {
                            // Established or gone: with the wheel's eager
                            // scrub the stale-entry case cannot arise (the
                            // establish path removed the side-table entry AND
                            // its timeline slot), but keep the defensive
                            // clear — a no-op when nothing is armed.
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
    let text = session.codec.encode(&ServerEvent::Ping);
    let mut out = BytesMut::new();
    frame::encode_text(&mut out, text.as_bytes());
    let _ = entry
        .conn
        .queue(Arc::from(out.to_vec().into_boxed_slice()), now_ns);
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
    let _ = entry
        .conn
        .queue(Arc::from(out.to_vec().into_boxed_slice()), now_ns);
}

/// Send a WebSocket Close frame with the given `code` and `reason` text —
/// queue it and flush so it actually reaches the peer — then let the caller
/// handle the connection (either `remove` it immediately or wait for flush).
/// The single generalized Close-reply helper; [`send_close_4200`] and
/// [`send_close_4201`] are thin callers.
fn send_close_reply(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    key: usize,
    code: u16,
    reason: &str,
    now_ns: u64,
) {
    let Some(entry) = conns.get_mut(key) else {
        return;
    };
    queue_close_frame(entry, code, reason, now_ns);
    // Flush so the Close frame actually reaches the peer before we deregister.
    let _ = flush_and_arm(poll, entry, now_ns);
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
/// The caller is responsible for the subsequent `fold_delta` + eventual `remove`.
fn send_close_4200(poll: &Poll, conns: &mut slab::Slab<Entry>, key: usize, now_ns: u64) {
    send_close_reply(
        poll,
        conns,
        key,
        4200,
        "Server is shutting down; please reconnect",
        now_ns,
    );
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
    // A lifetime close only ever fires on an ESTABLISHED session, but fall back
    // to the raw-JSON form (as `queue_shutdown_error` does) for a conn whose
    // session vanished between arming and firing.
    let text = if let Some(session) = entry.session.as_ref() {
        session.codec.encode(&ServerEvent::Error(error))
    } else {
        serde_json::json!({
            "event": "pusher:error",
            "data": { "code": error.code, "message": error.message }
        })
        .to_string()
    };
    let mut out = BytesMut::new();
    frame::encode_text(&mut out, text.as_bytes());
    let _ = entry
        .conn
        .queue(Arc::from(out.to_vec().into_boxed_slice()), now_ns);
    // No explicit flush here: the caller queues the Close frame next via
    // `send_close_4202`, whose `flush_and_arm` flushes both frames together.
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
                    fragment: None,
                    pending_establish: None,
                });
                // G3: arm the handshake deadline AFTER the slot exists (the
                // slab key is the wheel's ConnId). Cleared at session establish;
                // fires (reap) otherwise.
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
        ConnState::Open | ConnState::Closing => handle_frames(poll, entry, cfg, now_ns),
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
    // Pull all available bytes into the head-accumulation buffer (`inbuf`).
    // G2: `NeedsWrite` means a TLS handshake flight could not be fully written
    // (the peer's receive window filled mid-handshake). Nothing can be parsed
    // out of it either — the plaintext pull inside `drain_head_bytes` is
    // skipped when the flight write blocks — so fall through to the arm point
    // below (`NeedMore ⇒ arm_handshake_interest`), which registers
    // READABLE | WRITABLE so the next writable event completes the flight.
    // A readable event arriving while the flight is still blocked is unaffected:
    // it lands here first and processes any new TLS records before the write
    // retry inside `drain_head_bytes`.
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
            let _ = entry.conn.queue(Arc::from(response), now_ns);
            // A browser never sends data frames before the 101, so any bytes
            // after the head would be a protocol error anyway; clearing is safe.
            entry.inbuf.clear();
            entry.conn.state = ConnState::Open;

            // For a dispatch worker, build the v7 session now: resolve the app,
            // check capacity, create the mailbox + ConnectionContext, and queue
            // the connection_established frame. On a rejection (malformed path
            // 4005, unknown app 4001, unsupported protocol 4007, over-capacity
            // 4004) emit the `pusher:error` frame + a WS Close carrying the
            // error code.
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
                    // R1: unknown AND disabled keys share the single WS answer
                    // (4001 "Could not find app by key") — only REST carries the
                    // 403 distinction.
                    Some(Ok(crate::app::AppLookup::Disabled))
                    | Some(Ok(crate::app::AppLookup::NotFound)) => Err(Reject {
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
                        // Session established: the G3 handshake deadline has
                        // served its purpose — clear it so the absolute
                        // (activity-immune) reap can never fire on a live
                        // session.
                        wheel.clear_handshake(key);
                        // Session established: arm the ABSOLUTE
                        // max-connection-lifetime deadline (close 4202) from
                        // this moment. Activity on the connection never pushes
                        // it out — only this establish instant sets it.
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

    // Task 4.2 (finding D2): CLUSTER-WIDE per-app capacity admission. The local
    // check above only sees THIS node (`conn_counts`), so N nodes each admitted
    // up to `capacity` connections — the docs promise one cluster-wide ceiling.
    // On a clustered node the check is completed in Redis: ADMIT_APP_LUA on the
    // bridge atomically compares the CLUSTER count (`appconns`) against the
    // capacity and takes a unit for this connection. This is the ONE place a
    // worker deliberately blocks on the bridge (bounded by the handle's reply
    // timeout): the establish cannot proceed before the cluster-wide decision.
    // Only apps WITH a capacity pay the round trip — an unlimited app
    // (`capacity == 0`) needs no cluster check, and the close-side release
    // mirrors this same condition off the SAME `App` snapshot, so every unit
    // taken here is given back exactly once.
    if app.capacity != 0 {
        if let Some(bridge) = env.cluster.as_ref() {
            if bridge.admit_app(&app.id, app.capacity) == Some(false) {
                // At capacity CLUSTER-WIDE: reject with the same 4004 the local
                // check sends, rolling back BOTH local counters exactly like the
                // local-reject path above (the Redis unit was NOT taken — the
                // script rejects without incrementing).
                counter.fetch_sub(1, Ordering::SeqCst);
                env.node_conns.fetch_sub(1, Ordering::SeqCst);
                return Err(Reject {
                    error: PusherError::over_capacity(),
                    codec: Some(codec),
                });
            }
            // `Some(true)` = admitted (unit taken). `None` = the bridge is
            // unavailable (channel full/closed, verdict timed out, or Redis
            // errored): FAIL OPEN — admit. A degraded bridge must not lock
            // clients out of a node whose local checks already passed; no Redis
            // unit was taken, and this connection's close-side release is a
            // floor-0 no-op, so the counts stay consistent.
        }
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

/// [`Mode::Dispatch`]: decode each complete Text message to a [`ClientCommand`]
/// and drive `ctx.dispatch`, answer pings with pongs, echo a client-initiated
/// Close per RFC 6455 §5.5.1, then drain this connection's mailbox so any
/// self-directed replies go out.
///
/// Fragmented text messages (RFC 6455 §5.4) are reassembled in
/// [`Entry::fragment`] before dispatch: a FIN=0 Text frame opens the
/// accumulation, Continuation frames append (bounded by `max_message_bytes`),
/// and the FIN=1 Continuation dispatches the assembled payload through the
/// same path as an unfragmented Text frame. Fragmented BINARY messages are
/// ignored frame-by-frame (binary is outside the Pusher protocol, like a lone
/// Binary frame). Control frames interleaved mid-fragment are handled in
/// frame order, so a Ping between fragments is answered before the message
/// completes (RFC 6455 §5.5.2).
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
            // frame followed by Continuation frames ONLY. Interleaving a new
            // data frame with an open fragmented TEXT message is a protocol
            // violation — fail the connection with Close 1002. (A Binary frame
            // interleaved with a text fragment falls here too; Binary frames
            // while a BINARY fragment is open are ignored below — binary is
            // outside the protocol and never fatal on its own.)
            OpCode::Text if entry.fragment.is_some() => {
                return close_fragment_violation(
                    poll,
                    entry,
                    now_ns,
                    "data frame interleaved with a fragmented message",
                );
            }
            OpCode::Binary if matches!(entry.fragment, Some(Fragment::Text(_))) => {
                return close_fragment_violation(
                    poll,
                    entry,
                    now_ns,
                    "data frame interleaved with a fragmented message",
                );
            }
            OpCode::Text if !f.fin => {
                // First fragment of a new message: hold the payload until the
                // FIN=1 Continuation completes it. No cap check here — a lone
                // first fragment is already bounded by the per-frame
                // `max_payload`; the per-message cap fires on the next append.
                entry.fragment = Some(Fragment::Text(f.payload.to_vec()));
            }
            OpCode::Text => {
                // A complete (unfragmented) message.
                if dispatch_text_message(poll, entry, &f.payload, now_ns) == Action::Close {
                    return Action::Close;
                }
            }
            // Binary is not part of the Pusher protocol; ignore, never fatal.
            // A FIN=0 Binary opens a message whose frames must all be ignored:
            // mark the fragment state as [`Fragment::Binary`] so its
            // Continuation frames (below) are dropped until the FIN=1
            // Continuation completes it — instead of tripping the stray-
            // Continuation guard.
            OpCode::Binary => {
                if !f.fin && entry.fragment.is_none() {
                    entry.fragment = Some(Fragment::Binary);
                }
            }
            OpCode::Continuation => {
                // RFC 6455 §5.4: a Continuation must follow an open fragmented
                // message on this connection; a stray one is a protocol
                // violation — fail the connection with Close 1002. (This also
                // catches further fragments of a TEXT message already dropped
                // for exceeding the cap below.)
                let Some(fragment) = entry.fragment.take() else {
                    return close_fragment_violation(
                        poll,
                        entry,
                        now_ns,
                        "continuation frame without a fragmented message",
                    );
                };
                match fragment {
                    // Continuation of an ignored fragmented Binary: drop the
                    // payload and stay in ignore-mode until FIN=1 completes it.
                    Fragment::Binary => {
                        if !f.fin {
                            entry.fragment = Some(Fragment::Binary);
                        }
                    }
                    Fragment::Text(mut buf) => {
                        // Per-message byte cap (`max_event_payload_bytes`): the
                        // same budget an unfragmented protocol message is held
                        // to. Oversize → drop the partial message AND reset the
                        // accumulator; the connection stays usable for the next
                        // well-formed message.
                        if buf.len().saturating_add(f.payload.len()) > max_message_bytes {
                            tracing::trace!(
                                assembled = buf.len() + f.payload.len(),
                                max_message_bytes,
                                "dropping oversize fragmented message"
                            );
                            continue; // `buf` dropped here; accumulator left reset (`None`).
                        }
                        buf.extend_from_slice(&f.payload);
                        if f.fin {
                            // Message complete: dispatch the assembled payload
                            // through the normal Text path (`fragment` stays
                            // `None`).
                            if dispatch_text_message(poll, entry, &buf, now_ns) == Action::Close {
                                return Action::Close;
                            }
                        } else {
                            entry.fragment = Some(Fragment::Text(buf)); // still open
                        }
                    }
                }
            }
            OpCode::Ping => {
                let mut out = BytesMut::new();
                frame::encode(&mut out, true, OpCode::Pong, &f.payload);
                let _ = entry
                    .conn
                    .queue(Arc::from(out.to_vec().into_boxed_slice()), now_ns);
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

    // Drain this connection's mailbox: dispatch may have enqueued self-directed
    // replies (subscription_succeeded, pong, errors) plus the adapter may have
    // fanned a broadcast onto it. The readable path reconciles this connection's
    // membership after a `Keep` (see the `Action::Keep` arm in `run`), so any
    // `subscribed` change a drained `SubscriptionError` made here is picked up there.
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
    entry.fragment = None;
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
    entry.fragment = None;
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
    drophead_total: &mut u64,
    now_ns: u64,
    conn_counts: &Arc<DashMap<String, Arc<AtomicUsize>>>,
    app_registry: &Arc<AppRegistry>,
    node_conns: &Arc<AtomicUsize>,
    cluster: &Option<crate::cluster::bridge::ClusterHandle>,
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
        fold_drophead(conns, key, drophead_total);
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
                node_conns,
                cluster,
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
///   * `Ok(Found(app))` → `finish_establish`; on `Ok(session)` queue
///     `connection_established` + store the session + flush; on `Err(reject)`
///     queue the reject + flush + `remove`.
///   * `Ok(Disabled | NotFound)` → reject `app_not_found` (4001) + flush +
///     `remove` (R1: the WS side keeps ONE answer for an unusable key).
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
            entry
                .pending_establish
                .take()
                .expect("pending_establish present")
        };

        let outcome = match result {
            Ok(crate::app::AppLookup::Found(app)) => {
                finish_establish(env, app, pe.codec, pe.notify, pe.mailbox_dropped)
            }
            // R1: unknown AND disabled keys share the 4001 WS answer (REST
            // distinguishes disabled via 403; WS keeps one unusable-key code).
            Ok(crate::app::AppLookup::Disabled) | Ok(crate::app::AppLookup::NotFound) => {
                Err(Reject {
                    error: PusherError::app_not_found(),
                    codec: Some(pe.codec),
                })
            }
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
                fold_drophead(conns, token, drophead_total);
                wrote_any = true;
                if action == Action::Close {
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
                        node_conns,
                        cluster,
                    );
                } else {
                    // Re-arm this resumed connection's idle deadline from NOW. The
                    // upgrade-time `wheel.touch` was set BEFORE the park; the park is
                    // bounded by the driver lookup timeout (≪ `activity_timeout`), so
                    // in any sane config the wheel still holds this entry — but if a
                    // park ever outlived `activity_timeout`, the idle timer would have
                    // fired on the `session: None` entry and dropped it from the wheel
                    // (see the `Due::Ping` arm). Touching here re-arms it unconditionally
                    // so a resumed connection is ALWAYS liveness-monitored, and starts
                    // its idle clock at establish (`touch` reschedules — never duplicates).
                    wheel.touch(token, now_ns / 1_000_000);
                    // The max-connection-lifetime clock also starts at THIS establish
                    // moment (the park is not part of the connection's life).
                    arm_lifetime(wheel, env, token, now_ns / 1_000_000);
                    // G3: and the handshake deadline's job is done — the
                    // session is established (mirrors the synchronous path's
                    // clear at `entry.session = Some(..)` above).
                    wheel.clear_handshake(token);
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
                fold_drophead(conns, token, drophead_total);
                wrote_any = true;
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
/// handshake-flight write blocked (see [`arm_handshake_interest`]). The
/// writable event means the send buffer drained, so re-running
/// [`handle_handshake`] completes the flight, pulls any plaintext that was
/// waiting behind it, and — via `arm_handshake_interest` (`NeedMore`) or
/// `flush_and_arm` (upgrade) — clears WRITABLE once `!tls.wants_write()`. The
/// read at the top of that path is a `WouldBlock` no-op when the event carried
/// no data; a readable event arriving mid-block is handled by the normal
/// readable path.
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
        ConnState::Open | ConnState::Closing => flush_and_arm(poll, entry, now_ns),
    }
}

/// Flush the outbound queue and reconcile writable interest with what remains.
///
/// * [`WriteStatus::Drained`] → re-arm `READABLE`-only (drop `WRITABLE`).
/// * [`WriteStatus::WouldBlock`] → add `WRITABLE` so we get a writable event.
/// * [`WriteStatus::Closed`] → close.
///
/// The tracked `writable_armed` mirror on the [`Connection`] is updated after
/// every successful re-registration so the loop-top debug invariant ("queued
/// bytes ⇒ WRITABLE armed") can verify the arm really happened.
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
/// still has to write. Two things can be pending mid-handshake:
///
/// * a **blocked TLS flight** — rustls has ciphertext queued for the socket
///   ([`Connection::tls_wants_write`]) because the flight write hit a full
///   send buffer (the peer's receive window filled, e.g. a zero-window
///   client); and
/// * **queued frames** — the shutdown drain queues error/Close frames even on
///   a still-Handshaking connection.
///
/// When either is present we flush via [`flush_and_arm`], which drives the
/// pending TLS ciphertext FIRST (its Phase 1) and reconciles WRITABLE
/// interest from the outcome: mio is level-triggered, so arming it means the
/// kernel wakes the loop the moment the buffer drains and
/// [`handle_writable`] re-drives the handshake. That also keeps the loop-top
/// invariant ("queued bytes ⇒ WRITABLE armed") intact for the drain-path
/// frames. With nothing pending, a previously-armed WRITABLE drops back to
/// READABLE-only — mirroring `flush_and_arm`'s Drained arm — so the
/// connection never spins on an always-ready writable socket; and a plain-TCP
/// handshake (never pending writes) keeps its accept-time READABLE-only
/// registration with zero extra syscalls.
///
/// Task 3.3 (G3 handshake deadline) ultimately did NOT hook here: the
/// slowloris deadline is ABSOLUTE from accept (armed once in
/// [`accept_ready`], cleared at session establish), so a connection blocked
/// in this function on a stalled TLS flight is covered by that accept-time
/// arm — activity and interest churn never postpone it.
fn arm_handshake_interest(poll: &Poll, entry: &mut Entry, now_ns: u64) -> Action {
    let token = entry.token;
    if entry.conn.tls_wants_write() || entry.conn.has_pending_writes() {
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
/// Order matters: deregister the stream from this `Poll` and remove the slab
/// entry BEFORE moving the fd out of mio, so mio's registry/slab no longer
/// reference it. Then [`crate::transport::rest::mio_to_std`] transfers fd ownership into a
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

/// G8: drain this connection's drop-head eviction accumulator into the
/// worker-level total. Called at exactly the same sites as [`fold_codel`] so
/// the shared `drophead_dropped_slot` (→ `pylon_drophead_dropped_total`)
/// reflects actual evictions with zero per-frame cost on the normal path.
/// Folding is UNIFORM (Keep and pre-teardown sites alike): an eviction right
/// before a close would otherwise die with the slab entry, silently losing the
/// count — the exact unobservability this counter exists to close.
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
    local_subs: &mut HashMap<(Arc<str>, Arc<str>), HashSet<SocketId>>,
    sid_to_token: &mut HashMap<SocketId, usize>,
    wheel: &mut TimerWheel,
    inflight_bytes: &mut u64,
    conn_counts: &Arc<DashMap<String, Arc<AtomicUsize>>>,
    app_registry: &Arc<AppRegistry>,
    node_conns: &Arc<AtomicUsize>,
    cluster: &Option<crate::cluster::bridge::ClusterHandle>,
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
            node_conns.fetch_sub(1, Ordering::SeqCst);
            // Task 4.2 (finding D2): cluster-wide per-app capacity release —
            // give back the unit this connection's establish took in Redis
            // (ADMIT_APP_LUA). Fire-and-forget at the bridge, exactly like the
            // other close-time cluster edges. The SAME `App` snapshot taken at
            // establish gates both sides (`capacity != 0`), so the release
            // matches the admission exactly once; the floor-0, node-guarded
            // RELEASE script makes it a no-op when this node holds no recorded
            // unit (an admission that failed open), so firing on every
            // clustered close is always safe.
            if session.ctx.app.capacity != 0 {
                if let Some(bridge) = cluster {
                    bridge.release_app(app_id);
                }
            }
        }
        let _ = poll.registry().deregister(entry.conn.stream_mut());
    }
}

/// Drop a closing connection's `socket_id` from every `(app, channel)` it was
/// indexed under, and from the reverse `socket_id → token` map.
///
/// G5: walks the UNION of the session's last-reconciled baseline (`subs`) and
/// the live protocol set (`ctx.subscribed`). The baseline alone covers every
/// entry `reconcile_membership` inserted — but a readable batch containing
/// [subscribe, Close] (or a protocol-error/backpressure close right after a
/// subscribe in the same batch) returns `Action::Close` from `dispatch_frames`
/// BEFORE the `Action::Keep` arm's post-dispatch reconcile runs, so a
/// subscription that reached the index by any path the baseline missed would
/// otherwise stay indexed forever (dead socket ids accumulate; the channel's
/// subscriber set never empties). Deindexing the union makes the close path
/// self-sufficient: it cleans whatever the connection could still be indexed
/// under, without trusting the reconcile bookkeeping.
///
/// Dedup is free: iterating `ctx.subscribed` chained with the channels of
/// `subs` that `ctx.subscribed` lacks visits every union member exactly once,
/// and the removal itself is idempotent anyway — removing an absent
/// `(key, socket_id)` is a no-op, so a second pass over an overlapping channel
/// cannot double-subtract the test gauge or disturb another subscriber.
fn deindex_connection(
    session: &Session,
    local_subs: &mut HashMap<(Arc<str>, Arc<str>), HashSet<SocketId>>,
    sid_to_token: &mut HashMap<SocketId, usize>,
) {
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
        if let Some(set) = local_subs.get_mut(&k) {
            if set.remove(sid) {
                #[cfg(any(test, feature = "test-hooks"))]
                LOCAL_SUBS_SLOTS.fetch_sub(1, Ordering::Relaxed);
            }
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
        let inserted = local_subs
            .entry((Arc::clone(&app), Arc::<str>::from(channel.as_str())))
            .or_default()
            .insert(*sid);
        if inserted {
            #[cfg(any(test, feature = "test-hooks"))]
            LOCAL_SUBS_SLOTS.fetch_add(1, Ordering::Relaxed);
        }
    }
    // Removed channels: were recorded, no longer subscribed.
    for channel in session.subs.difference(&session.ctx.subscribed) {
        let k = (Arc::clone(&app), Arc::<str>::from(channel.as_str()));
        if let Some(set) = local_subs.get_mut(&k) {
            if set.remove(sid) {
                #[cfg(any(test, feature = "test-hooks"))]
                LOCAL_SUBS_SLOTS.fetch_sub(1, Ordering::Relaxed);
            }
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

/// Deliver every queued [`crate::transport::fanout::BroadcastMsg`] to this worker's local subscribers,
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
    drophead_total: &mut u64,
    saturated: Option<&Arc<AtomicBool>>,
    now_ns: u64,
    conn_counts: &Arc<DashMap<String, Arc<AtomicUsize>>>,
    app_registry: &Arc<AppRegistry>,
    node_conns: &Arc<AtomicUsize>,
    cluster: &Option<crate::cluster::bridge::ClusterHandle>,
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
            // G8: the enqueue may have evicted older frames (drop-head) — fold
            // the per-connection accumulator into the worker total NOW rather
            // than deferring to the post-drain flush fold, so the counter is
            // current even if the flush below closes the connection.
            let dh = entry.conn.take_drophead_dropped();
            if dh > 0 {
                *drophead_total = drophead_total.wrapping_add(dh);
            }
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
            // G8: fold any drop-head evictions the enqueue path left behind
            // (belt-and-suspenders — the enqueue fold above usually took them).
            let dh = entry.conn.take_drophead_dropped();
            if dh > 0 {
                *drophead_total = drophead_total.wrapping_add(dh);
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
            node_conns,
            cluster,
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
}
