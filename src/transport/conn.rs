//! Per-connection state and non-blocking I/O for the SP9 per-core transport.
//!
//! A [`Connection`] wraps a single non-blocking [`mio::net::TcpStream`] and owns
//! the two halves of a Pusher WebSocket session:
//!
//! * **Outbound.** A queue of pre-encoded frames ([`Arc<[u8]>`], so a broadcast
//!   payload is encoded once and fanned out as cheap `Arc` clones). [`Connection::flush`]
//!   drains the queue with *corked*, coalesced writes — whole queued frames are
//!   gathered into `IoSlice` batches and handed to the socket in one
//!   `writev(2)` per batch (bounded by an iovec count and a byte budget),
//!   advancing a cursor across partial writes, and reporting backpressure via
//!   [`WriteStatus::WouldBlock`]. [`Connection::queue`] enforces a high-water
//!   mark so a slow consumer cannot make us buffer unbounded memory.
//!
//! * **Inbound.** [`Connection::read_frames`] reads whatever the socket has available into a
//!   caller-supplied scratch [`BytesMut`] and parses every complete frame out of
//!   it, leaving any partial-frame remainder in the buffer for next time.
//!
//! Every method is non-blocking and 100% safe Rust (the crate root sets
//! `#![deny(unsafe_code)]`). None of them ever loops on `WouldBlock`; the
//! worker re-arms epoll interest and calls back.

use crate::transport::frame::{self, Frame, ParseError};
use bytes::BytesMut;
use rustls::server::ServerConnection as TlsConn;
use std::collections::VecDeque;
use std::io::{ErrorKind, IoSlice, Read, Write};
use std::sync::Arc;

/// Lifecycle of a connection as seen by the transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnState {
    /// HTTP upgrade in progress; no WS frames have flowed yet.
    Handshaking,
    /// Upgrade complete; WS frames flow in both directions.
    Open,
    /// A close handshake is underway; draining remaining writes.
    Closing,
}

/// Outcome of a [`Connection::flush`] call.
#[derive(Debug, PartialEq)]
pub enum WriteStatus {
    /// The outbound queue is now empty; clear writable interest.
    Drained,
    /// The socket's send buffer is full; data remains queued. The caller should
    /// (re-)arm writable interest and flush again on the next writable event.
    WouldBlock,
    /// The peer is gone (write error or a zero-length write); close the
    /// connection.
    Closed,
}

/// Outcome of a [`Connection::drain_head_bytes`] call (G2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainStatus {
    /// The socket is drained for now (read hit `WouldBlock`) and, for TLS, any
    /// pending handshake-flight write fully went out.
    Ok,
    /// A TLS handshake flight could not be fully written: `write_tls` hit
    /// `WouldBlock` with `wants_write()` still true (the peer's receive window
    /// filled mid-handshake — e.g. a zero-window client). The caller MUST arm
    /// `WRITABLE` interest so the flight is completed on the next writable
    /// event: nothing else ever re-drives it, so dropping this signal hangs the
    /// handshake forever. Plain connections never produce this variant.
    NeedsWrite,
    /// EOF or a hard I/O error; close the connection.
    Closed,
}

/// Error surfaced by the queue/read paths.
#[derive(Debug, PartialEq)]
pub enum ConnError {
    /// The outbound queue is over its high-water mark; the caller must close the
    /// connection (a slow consumer we refuse to buffer for unbounded).
    ///
    /// SP10: the per-connection out-queue is now byte-bounded **drop-head**
    /// ([`Connection::queue`] drops the oldest frame(s) to fit rather than
    /// rejecting), so this variant is no longer produced by the queue path. It is
    /// retained for the read paths' API shape and possible future use.
    #[allow(dead_code)]
    Backpressure,
    /// The peer closed (EOF with nothing buffered) or the socket errored.
    Closed,
    /// A fatal WebSocket protocol violation; close with status 1002.
    Protocol(&'static str),
}

/// CoDel time-in-queue freshness parameters (SP10 §7). folly's controlled-delay
/// rule applied on **dequeue**: track the minimum sojourn (time-in-queue) over
/// each `interval`; if that interval-minimum stays above `target`, the queue is
/// "overloaded" and we drop any frame whose sojourn exceeds `2 × target` instead
/// of sending stale data. A `target_ns` of `0` disables CoDel entirely (pure
/// drop-head behaviour — every queued frame is sent regardless of age).
#[derive(Debug, Clone, Copy)]
pub struct CodelParams {
    /// Acceptable standing sojourn (ns). folly default 5 ms. `0` disables CoDel.
    pub target_ns: u64,
    /// Window (ns) over which the minimum sojourn is tracked. folly default 100 ms.
    pub interval_ns: u64,
}

impl CodelParams {
    /// folly defaults: 5 ms target, 100 ms interval.
    pub const DEFAULT: CodelParams = CodelParams {
        target_ns: 5_000_000,
        interval_ns: 100_000_000,
    };

    /// A disabled CoDel overlay (`target_ns == 0`): [`Connection::flush`] skips
    /// the sojourn check entirely, so behaviour is pure drop-head.
    pub const DISABLED: CodelParams = CodelParams {
        target_ns: 0,
        interval_ns: 100_000_000,
    };

    /// Whether the CoDel overlay is active (a non-zero target).
    fn enabled(&self) -> bool {
        self.target_ns != 0
    }
}

/// Per-connection CoDel control state (folly's algorithm). Tracks the minimum
/// sojourn seen so far in the current interval and whether the queue is currently
/// in the "overloaded" regime in which stale frames are dropped on dequeue.
#[derive(Debug, Clone, Copy, Default)]
struct CodelState {
    /// Minimum sojourn (ns) observed so far in the current interval; `None`
    /// before the first dequeue of an interval.
    interval_min: Option<u64>,
    /// Monotonic time (ns) at which the current interval ends; `0` before the
    /// first dequeue ever (the first dequeue opens the first interval).
    interval_end: u64,
    /// Whether the queue is currently overloaded — set when an interval closes
    /// with `interval_min > target`, cleared when one closes with
    /// `interval_min <= target`. While `true`, frames with `sojourn > 2*target`
    /// are dropped on dequeue.
    overloaded: bool,
}

/// The queued outbound element: a pre-encoded frame paired with its monotonic
/// enqueue timestamp (for CoDel sojourn computation on dequeue).
///
/// F4/6.3 seam: everything in the flush path touches the frame only via
/// `len()`/indexing (no `Arc`-specific API), so swapping `Arc<[u8]>` for
/// `bytes::Bytes` later is a one-line change to this alias.
type OutFrame = (Arc<[u8]>, u64);

/// F4: coalescing limits for the flush path.
///
/// `WRITEV_MAX_SLICES` matches `IOV_MAX` (1024) on Linux and macOS — handing
/// `writev(2)` more iovecs than that is undefined behaviour, so the gather
/// stops there. `WRITEV_MAX_BYTES` caps one syscall's payload so a single
/// huge burst cannot monopolise the socket's kernel send-buffer share and
/// partial-write retries stay small.
const WRITEV_MAX_SLICES: usize = 1024;
/// Plain-TCP per-syscall byte budget (see [`WRITEV_MAX_SLICES`]).
const WRITEV_MAX_BYTES: usize = 256 * 1024;
/// TLS per-batch plaintext budget. Deliberately BELOW rustls's default 64 KiB
/// sendable-buffer limit: after the pre-drain in [`TlsBatchSink`] empties the
/// ciphertext buffer, a batch this size is always accepted whole by one
/// `Writer::write`, so rustls packs it into as few full 16 KiB records as the
/// record cap allows instead of one (nearly empty) record per frame.
const TLS_BATCH_MAX_BYTES: usize = 60 * 1024;

/// The write target of one coalesced batch (F4). Production uses
/// [`mio::net::TcpStream`] — mio's `Write` impl forwards `write_vectored`
/// straight to the std stream, i.e. a real `writev(2)` — and
/// [`TlsBatchSink`] for the encrypted path. The unit tests drive the exact
/// same flush loop with a call-counting mock, proving one batch is one
/// syscall-shaped call.
trait WriteSink {
    /// Write as many of `bufs`' bytes as the sink accepts right now,
    /// returning how many were consumed. `Ok(0)` on a non-empty batch means
    /// the sink can accept nothing further (mapped to
    /// [`WriteStatus::Closed`]).
    fn write_batch(&mut self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize>;
}

impl WriteSink for mio::net::TcpStream {
    fn write_batch(&mut self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        self.write_vectored(bufs)
    }
}

/// The TLS flavour of [`WriteSink`] (F4): concatenates the batch into one
/// contiguous plaintext buffer and hands it to rustls in a single
/// `Writer::write`, so rustls encrypts fewer, fuller records.
///
/// Ciphertext is drained to the socket both before the write (emptying
/// rustls's bounded send buffer so the plaintext is always accepted — a full
/// one makes `Writer::write` short- or zero-write) and after it. A post-write
/// drain that hits `WouldBlock` just leaves the ciphertext queued inside
/// rustls for the next flush's Phase 1: the plaintext has already been
/// consumed, so the app-side queue stays advanced and no byte is ever
/// encrypted twice. (This also fixes the pre-F4 shape, which returned
/// `WouldBlock` WITHOUT advancing the cursor over plaintext rustls had
/// already consumed — on resume the same bytes were written again,
/// duplicating them on the wire.)
struct TlsBatchSink<'a> {
    stream: &'a mut mio::net::TcpStream,
    tls: &'a mut TlsConn,
    /// Reusable contiguous plaintext buffer (owned by the `Connection`).
    scratch: &'a mut Vec<u8>,
}

impl WriteSink for TlsBatchSink<'_> {
    fn write_batch(&mut self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        // Pre-drain: make room under rustls's send-buffer limit.
        while self.tls.wants_write() {
            match self.tls.write_tls(self.stream) {
                Ok(_) => {}
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    // Socket full with ciphertext pending: no room for new
                    // plaintext either — surface plain backpressure. Nothing
                    // was consumed, so the caller's queue restore is exact.
                    return Err(std::io::Error::from(e.kind()));
                }
                Err(e) => return Err(e),
            }
        }
        self.scratch.clear();
        for b in bufs {
            self.scratch.extend_from_slice(b);
        }
        // (clippy::needless_borrow is wrong here: the explicit `&` keeps the
        // disjoint-field borrows of `self.tls` (mutable, through `writer()`)
        // and `self.scratch` (shared) syntactically obvious.)
        #[allow(clippy::needless_borrow)]
        let n = self.tls.writer().write(&self.scratch)?;
        // Post-drain: push the freshly encrypted records out now.
        while self.tls.wants_write() {
            match self.tls.write_tls(self.stream) {
                Ok(_) => {}
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
        Ok(n)
    }
}

/// The I/O backend for a connection: either a plain TCP stream or a rustls-
/// encrypted stream backed by the same underlying socket.
enum Io {
    Plain(mio::net::TcpStream),
    Tls(mio::net::TcpStream, Box<TlsConn>),
}

/// The full handoff payload returned by [`Connection::into_io_handoff`].
///
/// For a plain-TCP connection only the `mio` stream is returned. For a TLS
/// connection both the `mio` stream AND the live (already-handshaked) rustls
/// `ServerConnection` are returned so the async REST plane can continue
/// driving the encrypted session rather than seeing raw ciphertext.
pub enum IoHandoff {
    /// A plain (non-TLS) connection; only the socket is needed.
    Plain(mio::net::TcpStream),
    /// A TLS connection: the raw TCP socket plus the rustls session that has
    /// already completed its handshake. Application-layer bytes the worker
    /// already decrypted are stored separately in `RestConn::prefix`.
    Tls(mio::net::TcpStream, Box<TlsConn>),
}

impl Io {
    fn stream_mut(&mut self) -> &mut mio::net::TcpStream {
        match self {
            Io::Plain(s) | Io::Tls(s, _) => s,
        }
    }
}

/// A single non-blocking WebSocket connection.
pub struct Connection {
    /// The I/O backend: a plain TCP stream or a rustls-encrypted stream backed
    /// by the same underlying socket. `stream_mut()` exposes the raw TCP stream
    /// for mio poll registration and re-arming.
    io: Io,
    /// Current lifecycle state.
    pub state: ConnState,
    /// Pending outbound frames (pre-encoded bytes, shared via `Arc` for
    /// encode-once fan-out) paired with the monotonic enqueue time (ns since the
    /// owning worker's epoch) used for CoDel sojourn computation on dequeue. The
    /// front element is the one currently being written, possibly partially (see
    /// `out_cursor`).
    out: VecDeque<OutFrame>,
    /// Byte offset into `out.front()` already written (partial-write resume
    /// point).
    out_cursor: usize,
    /// Total bytes still queued across all of `out` (drives the high-water
    /// backpressure check without walking the deque). Counts only the `Arc`
    /// payload lengths, never the per-frame timestamp.
    out_bytes: usize,
    /// Backpressure threshold: if queuing a frame would push `out_bytes` over
    /// this, the frame is rejected and the caller closes.
    high_water: usize,
    /// CoDel freshness parameters (target / interval). `target_ns == 0` disables.
    codel: CodelParams,
    /// CoDel control state (interval minimum sojourn + overloaded flag).
    codel_state: CodelState,
    /// Count of frames dropped by CoDel on dequeue for being stale (sojourn
    /// `> 2 * target` while overloaded). Distinct from drop-head evictions.
    codel_dropped: u64,
    /// Count of frames evicted by drop-head at ENQUEUE time (the oldest
    /// droppable frame removed so a new one fits under `high_water`). Distinct
    /// from CoDel staleness drops (which happen at dequeue). Accumulated here
    /// exactly like `codel_dropped` so the owning worker can fold it into its
    /// worker-level total (→ `pylon_drophead_dropped_total`) via
    /// [`take_drophead_dropped`](Self::take_drophead_dropped).
    drophead_dropped: u64,
    /// Reusable frame batch for the coalescing flush (F4): the frames popped
    /// for the writev batch currently in flight. Kept on the connection so a
    /// flush performs no allocation once warmed up.
    writev_batch: Vec<OutFrame>,
    /// Reusable contiguous plaintext batch for the TLS flush (F4): the current
    /// frame batch, copied for one `rustls::Writer::write`.
    tls_batch: Vec<u8>,
    /// Whether this connection's `mio` poll registration currently includes
    /// [`mio::Interest::WRITABLE`] — the tracked mirror of the actual registry
    /// interest, maintained by the worker at every re-registration site
    /// ([`flush_and_arm`](crate::transport::worker) records each outcome; the
    /// accept-time READABLE-only registration matches the `false` construction
    /// default). Powers the worker-loop debug invariant "queued out-bytes ⇒
    /// WRITABLE armed" — the tripwire proving an idle 50 ms poll can never
    /// strand a backpressured connection's out-queue: with WRITABLE armed the
    /// kernel wakes the loop the moment the socket drains.
    writable_armed: bool,
    /// Signed accumulator of every change to `out_bytes` since the last
    /// [`take_inflight_delta`](Self::take_inflight_delta), so the worker can
    /// maintain its `inflight_bytes` total incrementally (O(work), not
    /// O(connections)) instead of re-summing every connection each loop. Every
    /// mutation site that changes `out_bytes` — the `queue` enqueue/drop-head
    /// eviction, the `flush` send, and the CoDel staleness drop — folds the exact
    /// signed delta in here. Bounded by the queue cap (≤ a few MiB), so `i64`
    /// never overflows. Invariant: across any sequence of operations the SUM of
    /// the deltas taken equals the net change in `out_bytes`.
    inflight_delta: i64,
}

impl Connection {
    /// Wrap a freshly-accepted non-blocking socket. Starts in
    /// [`ConnState::Handshaking`] with empty queues and CoDel disabled (the
    /// worker sets real parameters via [`Connection::set_codel`]).
    pub fn new(stream: mio::net::TcpStream, high_water: usize) -> Self {
        Connection {
            io: Io::Plain(stream),
            state: ConnState::Handshaking,
            out: VecDeque::new(),
            out_cursor: 0,
            out_bytes: 0,
            high_water,
            codel: CodelParams::DISABLED,
            codel_state: CodelState::default(),
            codel_dropped: 0,
            drophead_dropped: 0,
            writev_batch: Vec::new(),
            tls_batch: Vec::new(),
            writable_armed: false,
            inflight_delta: 0,
        }
    }

    /// Wrap a freshly-accepted non-blocking socket with a TLS server-side
    /// handshake in progress. Starts in [`ConnState::Handshaking`] with empty
    /// queues and CoDel disabled.
    pub fn new_tls(stream: mio::net::TcpStream, tls: Box<TlsConn>, high_water: usize) -> Self {
        Connection {
            io: Io::Tls(stream, tls),
            state: ConnState::Handshaking,
            out: VecDeque::new(),
            out_cursor: 0,
            out_bytes: 0,
            high_water,
            codel: CodelParams::DISABLED,
            codel_state: CodelState::default(),
            codel_dropped: 0,
            drophead_dropped: 0,
            writev_batch: Vec::new(),
            tls_batch: Vec::new(),
            writable_armed: false,
            inflight_delta: 0,
        }
    }

    /// Return a mutable reference to the underlying TCP stream so the worker
    /// can register/reregister it with its [`mio::Poll`].
    pub fn stream_mut(&mut self) -> &mut mio::net::TcpStream {
        self.io.stream_mut()
    }

    /// Install this connection's CoDel freshness parameters. Called once by the
    /// worker right after accept so every connection inherits the worker's
    /// (config-derived) target/interval. `target_ns == 0` leaves CoDel disabled.
    pub fn set_codel(&mut self, codel: CodelParams) {
        self.codel = codel;
    }

    /// Total frames this connection has dropped on dequeue for staleness (CoDel).
    /// Read by the worker to fold into its codel-dropped counter.
    pub fn codel_dropped(&self) -> u64 {
        self.codel_dropped
    }

    /// Take and reset the CoDel drop counter: returns the count of frames dropped
    /// since the last call and resets the per-connection accumulator to 0. The
    /// worker adds the returned delta to its worker-level `codel_dropped_total`
    /// after each flush, so the shared slot stays current without per-iteration cost.
    pub fn take_codel_dropped(&mut self) -> u64 {
        std::mem::take(&mut self.codel_dropped)
    }

    /// Total frames this connection has had evicted by drop-head (the oldest
    /// droppable frame removed at enqueue so a new one fits `high_water`).
    /// Read by the worker to fold into its drop-head-dropped counter.
    pub fn drophead_dropped(&self) -> u64 {
        self.drophead_dropped
    }

    /// Take and reset the drop-head eviction counter: returns the count of
    /// frames evicted since the last call and resets the per-connection
    /// accumulator to 0. Mirrors [`take_codel_dropped`](Self::take_codel_dropped):
    /// the worker adds the returned delta to its worker-level
    /// `drophead_dropped_total` at the same fold sites, so the shared slot
    /// stays current without per-iteration cost.
    pub fn take_drophead_dropped(&mut self) -> u64 {
        std::mem::take(&mut self.drophead_dropped)
    }

    /// Consume the connection and return ownership of its underlying socket.
    ///
    /// Used by the per-core worker's REST handoff (SP9 §3.4): a plain-HTTP
    /// connection is removed from the slab and its `mio` stream moved out, to be
    /// converted to a `std::net::TcpStream` and handed to the tokio/axum plane.
    /// Any queued outbound bytes are discarded (a REST connection has none — the
    /// head was only ever read).
    ///
    /// For TLS connections, this DROPS the rustls `ServerConnection`, which means
    /// the caller only gets the raw socket and must deal with raw ciphertext.
    /// Prefer [`into_io_handoff`](Self::into_io_handoff) when the TLS session must
    /// be preserved across the handoff.
    pub fn into_stream(self) -> mio::net::TcpStream {
        match self.io {
            Io::Plain(s) => s,
            Io::Tls(s, _) => s,
        }
    }

    /// Consume the connection and return the full I/O handoff payload — the mio
    /// TCP stream plus the rustls `ServerConnection` for TLS connections, or just
    /// the TCP stream for plain connections.
    ///
    /// Used by the REST handoff path when TLS is active: the rustls session has
    /// already completed the handshake and some application bytes have been
    /// decrypted into the `prefix` buffer; the session must be carried across the
    /// handoff so the async REST plane can continue driving it instead of seeing
    /// raw ciphertext. For plain connections this is equivalent to `into_stream`.
    pub fn into_io_handoff(self) -> IoHandoff {
        match self.io {
            Io::Plain(s) => IoHandoff::Plain(s),
            Io::Tls(s, tls) => IoHandoff::Tls(s, tls),
        }
    }

    /// Send a TLS `close_notify` alert to the peer (a graceful TLS shutdown).
    /// No-op for plain connections. Best-effort: write errors are ignored.
    pub fn send_close_notify(&mut self) {
        if let Io::Tls(stream, tls) = &mut self.io {
            tls.send_close_notify();
            while tls.wants_write() {
                match tls.write_tls(stream) {
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        }
    }

    /// Read all currently-available raw bytes into `buf`, stopping on
    /// `WouldBlock` or EOF. For plain connections this reads directly from the
    /// socket; for TLS connections this drives the TLS state machine (ingesting
    /// ciphertext, running the handshake, and pulling any available plaintext).
    ///
    /// Returns a [`DrainStatus`]: [`DrainStatus::Ok`] when drained for now,
    /// [`DrainStatus::NeedsWrite`] when a TLS handshake flight write blocked
    /// (the caller must arm WRITABLE interest — see the enum), and
    /// [`DrainStatus::Closed`] when the connection is closed (EOF or error).
    pub fn drain_head_bytes(&mut self, buf: &mut BytesMut) -> DrainStatus {
        let mut chunk = [0u8; 16 * 1024];
        match &mut self.io {
            Io::Plain(stream) => loop {
                match stream.read(&mut chunk) {
                    Ok(0) => return DrainStatus::Closed,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => return DrainStatus::Ok,
                    Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                    Err(_) => return DrainStatus::Closed,
                }
            },
            Io::Tls(stream, tls) => {
                // Read ciphertext from socket into rustls state machine.
                loop {
                    match tls.read_tls(stream) {
                        Ok(0) => return DrainStatus::Closed,
                        Ok(_) => {
                            if tls.process_new_packets().is_err() {
                                return DrainStatus::Closed;
                            }
                        }
                        Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                        Err(_) => return DrainStatus::Closed,
                    }
                }
                // Drive pending TLS writes (handshake responses). G2: breaking
                // out on WouldBlock leaves the flight half-written with
                // `wants_write()` still true — remember that and surface it as
                // NeedsWrite so the caller arms WRITABLE and re-drives on the
                // next writable event; nothing else would ever complete the
                // flight, so the handshake (and the connection) would hang.
                let mut flight_blocked = false;
                while tls.wants_write() {
                    match tls.write_tls(stream) {
                        Ok(_) => {}
                        Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                            flight_blocked = true;
                            break;
                        }
                        Err(_) => return DrainStatus::Closed,
                    }
                }
                // Pull available plaintext (empty during the handshake phase).
                // Nothing new can appear here when the flight write blocked
                // above: finishing the flight is the precondition for the peer
                // to send anything else the session could decrypt.
                loop {
                    match tls.reader().read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                        Err(_) => return DrainStatus::Closed,
                    }
                }
                if flight_blocked {
                    DrainStatus::NeedsWrite
                } else {
                    DrainStatus::Ok
                }
            }
        }
    }

    /// Queue a pre-encoded frame for sending (SP10 byte-bounded **drop-head**).
    ///
    /// Appends `frame`; if that would push the total queued bytes past
    /// `high_water`, the **oldest droppable** frame(s) are evicted (drop-head,
    /// freshest-wins for a live feed) until the new frame fits, decrementing the
    /// byte counter for each. WebSocket delivery is at-most-once, so dropping the
    /// stalest queued frame for a slow consumer is correct — and it keeps memory
    /// bounded under a publish flood (the SP9 hang fix).
    ///
    /// The frame currently mid-write — the front when `out_cursor > 0` — is
    /// **never** evicted: removing it would splice the peer's byte stream at an
    /// arbitrary offset and corrupt the connection. In that case the oldest
    /// droppable index is `1`, not `0`.
    ///
    /// If even after dropping everything droppable the new frame still doesn't fit
    /// (a single frame larger than the cap, or a locked front leaving no room),
    /// it is enqueued anyway — a single legitimate frame must remain deliverable;
    /// `high_water` is a soft target, not a hard per-frame reject.
    ///
    /// Returns the number of frames dropped. The appended frame still needs a
    /// [`flush`](Self::flush) (or a writable event) to actually go out.
    ///
    /// `now_ns` is the monotonic enqueue time (ns since the worker's epoch),
    /// stamped onto the frame so [`flush`](Self::flush) can compute its sojourn
    /// (time-in-queue) for the CoDel freshness check on dequeue.
    pub fn queue(&mut self, frame: Arc<[u8]>, now_ns: u64) -> usize {
        let flen = frame.len();
        let mut dropped = 0;
        // The frame currently mid-write (front when out_cursor>0) is "locked"; the
        // oldest droppable index is 1 in that case, else 0.
        let locked = if self.out_cursor > 0 { 1 } else { 0 };
        while self.out_bytes + flen > self.high_water && self.out.len() > locked {
            // Remove the oldest droppable frame.
            let (victim, _ts) = self.out.remove(locked).expect("len checked");
            self.out_bytes -= victim.len();
            // Drop-head eviction: this byte was queued earlier (counted into the
            // worker total then) and is now gone without being sent, so fold the
            // negative delta in so the worker's incremental total tracks it.
            self.inflight_delta -= victim.len() as i64;
            // G8: count the eviction on the connection accumulator (mirroring
            // `codel_dropped`) so the worker can fold it into
            // `pylon_drophead_dropped_total` — every `queue` call site is
            // covered by construction, no per-site threading needed.
            self.drophead_dropped += 1;
            dropped += 1;
        }
        self.out_bytes += flen;
        // The newly-queued frame adds to this connection's queued bytes; fold the
        // positive delta in for the worker's incremental inflight total.
        self.inflight_delta += flen as i64;
        self.out.push_back((frame, now_ns));
        dropped
    }

    /// Write as much of the queued data as the socket will accept, right now.
    ///
    /// Frames are coalesced into vectored write batches (F4): up to
    /// [`WRITEV_MAX_SLICES`] frames and `WRITEV_MAX_BYTES`/`TLS_BATCH_MAX_BYTES`
    /// bytes go out per write call until the socket returns `WouldBlock` or
    /// the queue empties. Partial writes advance `out_cursor` across frame
    /// boundaries; fully-written frames are popped and the cursor reset.
    /// Returns:
    ///
    /// * [`WriteStatus::Drained`] — queue empty, clear writable interest.
    /// * [`WriteStatus::WouldBlock`] — send buffer full, data remains; re-arm
    ///   writable interest.
    /// * [`WriteStatus::Closed`] — write error or a zero-length write (peer
    ///   gone); close.
    ///
    /// `now_ns` is the monotonic dequeue time (ns since the worker's epoch). With
    /// CoDel enabled (`target_ns != 0`), each frame's sojourn (`now_ns -
    /// enqueue_ns`) is checked as it reaches the batch head: see `codel_dequeue`.
    pub fn flush(&mut self, now_ns: u64) -> WriteStatus {
        match &self.io {
            Io::Plain(_) => self.flush_plain(now_ns),
            Io::Tls(_, _) => self.flush_tls(now_ns),
        }
    }

    /// Plain-TCP flush: drain the out-queue to the socket with coalesced
    /// `writev(2)` batches — one syscall per batch instead of one per frame
    /// (F4; a subscriber catching up on 50 queued frames paid 50 syscalls
    /// before). Frames are MOVED into the reusable `writev_batch` vec (no
    /// `Arc` clone, no per-flush allocation) and pushed back verbatim on any
    /// non-writing exit, so the queue state is never lost; the batch iovecs
    /// borrow that vec while the stream comes from `self.io` — disjoint
    /// fields, borrowed simultaneously without cost.
    fn flush_plain(&mut self, now_ns: u64) -> WriteStatus {
        let Io::Plain(stream) = &mut self.io else {
            unreachable!("flush_plain only called for Io::Plain")
        };
        flush_coalesced(
            &mut self.out,
            &mut self.out_cursor,
            &mut self.out_bytes,
            &mut self.inflight_delta,
            self.codel,
            &mut self.codel_state,
            &mut self.codel_dropped,
            &mut self.writev_batch,
            WRITEV_MAX_BYTES,
            stream,
            now_ns,
        )
    }

    /// TLS flush: encrypt app-data through rustls and drain ciphertext to the
    /// socket, one plaintext BATCH per `rustls::Writer::write` (F4): whole
    /// queued frames are concatenated (≤ [`TLS_BATCH_MAX_BYTES`]) and handed
    /// to rustls in a single write, so it packs fewer, fuller TLS records
    /// instead of one nearly-empty record per frame.
    fn flush_tls(&mut self, now_ns: u64) -> WriteStatus {
        // Phase 1: drain any pending TLS ciphertext that rustls has already
        // buffered (e.g. handshake records, or records a previous batch's
        // post-write drain left behind a blocked socket). Do this before
        // touching the app queue.
        {
            let Io::Tls(stream, tls) = &mut self.io else {
                unreachable!()
            };
            while tls.wants_write() {
                match tls.write_tls(stream) {
                    Ok(_) => {}
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                        return WriteStatus::WouldBlock;
                    }
                    Err(_) => return WriteStatus::Closed,
                }
            }
        }

        // Phase 2: encrypt and send the queued app-data frames in batches.
        // The sink (stream + tls from `self.io`) and the queue state are
        // disjoint fields of `self`, borrowed simultaneously without cost.
        let status = {
            let Io::Tls(stream, tls) = &mut self.io else {
                unreachable!("flush_tls only called for Io::Tls")
            };
            let mut sink = TlsBatchSink {
                stream,
                tls,
                scratch: &mut self.tls_batch,
            };
            flush_coalesced(
                &mut self.out,
                &mut self.out_cursor,
                &mut self.out_bytes,
                &mut self.inflight_delta,
                self.codel,
                &mut self.codel_state,
                &mut self.codel_dropped,
                &mut self.writev_batch,
                TLS_BATCH_MAX_BYTES,
                &mut sink,
                now_ns,
            )
        };
        if status != WriteStatus::Drained {
            return status;
        }

        // Final pass: flush any remaining TLS ciphertext rustls buffered
        // during the app-data writes.
        {
            let Io::Tls(stream, tls) = &mut self.io else {
                unreachable!()
            };
            while tls.wants_write() {
                match tls.write_tls(stream) {
                    Ok(_) => {}
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                        return WriteStatus::WouldBlock;
                    }
                    Err(_) => return WriteStatus::Closed,
                }
            }
        }

        WriteStatus::Drained
    }

    /// Read whatever the socket has available and parse every complete frame.
    ///
    /// `scratch` is the working buffer holding **this connection's** unparsed
    /// remainder from a previous call; new bytes are appended to it and any new
    /// partial-frame remainder is left in it for next time. (The worker owns the
    /// policy of whether `scratch` is shared or per-connection; this method only
    /// requires it to already contain *this* connection's remainder.)
    ///
    /// Returns the complete frames parsed in this call (possibly empty). Errors:
    ///
    /// * [`ConnError::Protocol`] — a fatal framing violation (also for an
    ///   oversized frame, reported as `"frame too large"`).
    /// * [`ConnError::Closed`] — EOF with no frames available, or a socket
    ///   error. On EOF *with* frames available we return the frames; the caller
    ///   sees the EOF on the next read.
    pub fn read_frames(
        &mut self,
        scratch: &mut BytesMut,
        max_payload: usize,
    ) -> Result<Vec<Frame>, ConnError> {
        // 1. Pull all currently-available bytes off the socket into `scratch`.
        //    Each read appends; we stop on WouldBlock (drained the socket) or
        //    EOF, and surface hard errors.
        let mut hit_eof = false;
        let mut chunk = [0u8; 16 * 1024];

        match &mut self.io {
            Io::Plain(stream) => loop {
                match stream.read(&mut chunk) {
                    Ok(0) => {
                        hit_eof = true;
                        break;
                    }
                    Ok(n) => scratch.extend_from_slice(&chunk[..n]),
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                    Err(_) => return Err(ConnError::Closed),
                }
            },
            Io::Tls(stream, tls) => {
                // Ingest ciphertext from the socket into the rustls state machine.
                loop {
                    match tls.read_tls(stream) {
                        Ok(0) => {
                            hit_eof = true;
                            break;
                        }
                        Ok(_) => {
                            if tls.process_new_packets().is_err() {
                                return Err(ConnError::Closed);
                            }
                        }
                        Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                        Err(_) => return Err(ConnError::Closed),
                    }
                }
                // Drive any pending TLS writes (handshake responses, alerts).
                while tls.wants_write() {
                    match tls.write_tls(stream) {
                        Ok(_) => {}
                        Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(_) => return Err(ConnError::Closed),
                    }
                }
                // Pull available plaintext out of the rustls decryption buffer.
                loop {
                    match tls.reader().read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => scratch.extend_from_slice(&chunk[..n]),
                        Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                        Err(_) => {
                            hit_eof = true;
                            break;
                        }
                    }
                }
            }
        }

        // 2. Drain every complete frame out of `scratch`, leaving any
        //    incomplete remainder in place for the next call.
        let mut frames = Vec::new();
        loop {
            match frame::parse(scratch, max_payload) {
                Ok(f) => frames.push(f),
                Err(ParseError::Incomplete) => break,
                Err(ParseError::Protocol(m)) => return Err(ConnError::Protocol(m)),
                Err(ParseError::TooLarge) => return Err(ConnError::Protocol("frame too large")),
            }
        }

        // 3. EOF with nothing to hand back means the peer is gone. With frames
        //    in hand we return them and let the caller hit EOF next time.
        if hit_eof && frames.is_empty() {
            return Err(ConnError::Closed);
        }
        Ok(frames)
    }

    /// Whether any outbound bytes are still queued (drives writable-interest
    /// re-arming).
    pub fn has_pending_writes(&self) -> bool {
        !self.out.is_empty()
    }

    /// Whether the TLS session still has ciphertext queued for the socket
    /// (rustls `wants_write()`); always `false` for plain connections. Read by
    /// the worker's handshake path (G2) to reconcile WRITABLE interest with a
    /// blocked handshake flight: while true, the flight write blocked on a
    /// full send buffer and must be completed on writable events.
    pub fn tls_wants_write(&self) -> bool {
        match &self.io {
            Io::Tls(_, tls) => tls.wants_write(),
            Io::Plain(_) => false,
        }
    }

    /// Whether WRITABLE interest is currently armed on this connection's poll
    /// registration (see the [`writable_armed`](Self::writable_armed) field
    /// doc). Read by the worker loop's debug invariant: a connection with
    /// queued out-bytes MUST hold WRITABLE interest, or an idle poll could
    /// strand its backlog.
    pub fn writable_armed(&self) -> bool {
        self.writable_armed
    }

    /// Record the connection's current WRITABLE-interest state. Called by the
    /// worker's `flush_and_arm` after every successful interest
    /// re-registration; the accept-time READABLE-only registration is matched
    /// by the `false` construction default.
    pub fn set_writable_armed(&mut self, armed: bool) {
        self.writable_armed = armed;
    }

    /// This connection's out-queue byte cap (its drop-head high-water). The
    /// graduated-shed decision (SP10 §6) compares `out_bytes()` against this to
    /// classify a subscriber as backed-up / slow.
    pub fn high_water(&self) -> usize {
        self.high_water
    }

    /// Total bytes currently queued across all of `out`. The per-worker
    /// `inflight_bytes` accounting (SP10) reads this before/after each
    /// `queue`/`flush` to maintain its counter as the exact sum of every
    /// connection's queued bytes — so a byte enqueued is decremented exactly once
    /// (on send via `flush`, or on drop-head eviction inside `queue`).
    pub fn out_bytes(&self) -> usize {
        self.out_bytes
    }

    /// Take and reset this connection's accumulated `out_bytes` delta since the
    /// last call, for the worker's INCREMENTAL inflight accounting (replaces the
    /// O(connections) re-sum every loop iteration with an O(work) fold).
    ///
    /// Every mutation site that changes `out_bytes` — `queue` (enqueue +
    /// drop-head eviction), `flush` (send), and the CoDel staleness drop — folds
    /// its exact signed delta into the accumulator. So the value returned here is
    /// precisely the net change in `out_bytes` over the operations since the
    /// previous take. The worker adds it to its running `inflight_bytes` after
    /// every site that touches this connection's out-queue; the sum of all deltas
    /// ever taken equals the connection's current `out_bytes`. Resets to `0`.
    ///
    /// A connection being `remove`d must have its delta taken (or its `out_bytes`
    /// subtracted) before it is dropped, so its still-queued bytes are removed
    /// from the worker total and the counter cannot leak upward.
    pub fn take_inflight_delta(&mut self) -> i64 {
        std::mem::take(&mut self.inflight_delta)
    }

    // ---- test accessors -------------------------------------------------------
    // Read-only views of the private out-queue state, used by the drop-head unit
    // tests. `#[cfg(test)]` so they add no surface (or dead-code warnings) to the
    // library build.

    /// Number of frames currently queued.
    #[cfg(test)]
    pub fn queued_len(&self) -> usize {
        self.out.len()
    }

    /// Byte offset already written into the front frame (partial-write cursor).
    #[cfg(test)]
    pub fn out_cursor(&self) -> usize {
        self.out_cursor
    }

    /// First byte of the front (oldest) queued frame.
    #[cfg(test)]
    pub fn peek_front_byte(&self) -> u8 {
        self.out.front().map(|f| f.0[0]).unwrap()
    }

    /// First byte of the back (newest) queued frame.
    #[cfg(test)]
    pub fn peek_back_byte(&self) -> u8 {
        self.out.back().map(|f| f.0[0]).unwrap()
    }

    /// Whether the front frame is the 4 MB "huge" frame the drop-head test
    /// enqueues first (identified by its length), i.e. index 0 is untouched.
    #[cfg(test)]
    pub fn front_is_the_huge_frame(&self) -> bool {
        self.out
            .front()
            .map(|f| f.0.len() == 4_000_000)
            .unwrap_or(false)
    }

    /// Whether the CoDel overlay is currently in the overloaded (stale-dropping)
    /// regime. Exposed for the CoDel timeline unit tests.
    #[cfg(test)]
    pub fn is_overloaded(&self) -> bool {
        self.codel_state.overloaded
    }
}

impl CodelState {
    /// Fold one real-dequeue sojourn sample into the current CoDel interval,
    /// advancing the overloaded flag when the interval window closes. `sojourn`
    /// is the candidate frame's time-in-queue.
    fn note_interval(&mut self, codel: CodelParams, now_ns: u64, sojourn: u64) {
        let interval = codel.interval_ns;
        let target = codel.target_ns;
        if self.interval_end == 0 {
            // First sample ever: open the first interval window.
            self.interval_end = now_ns.saturating_add(interval);
        }
        // Track the minimum sojourn seen this interval.
        self.interval_min = Some(match self.interval_min {
            Some(m) => m.min(sojourn),
            None => sojourn,
        });
        // Window closed: decide overloaded from the interval minimum, then reset
        // for the next window. Carry this very sample into the fresh interval so a
        // window that closes never starts the next one empty.
        if now_ns >= self.interval_end {
            let min = self.interval_min.unwrap_or(sojourn);
            self.overloaded = min > target;
            self.interval_min = Some(sojourn);
            self.interval_end = now_ns.saturating_add(interval);
        }
    }

    /// Age the overloaded flag when the queue is empty. A queue that has fully
    /// drained holds no stale frames, so once the current interval window has
    /// elapsed with the queue empty, the overloaded regime is cleared. Does not
    /// fold a (spuriously low) sojourn sample into a window that still has queued
    /// frames being tracked.
    fn age_empty(&mut self, codel: CodelParams, now_ns: u64) {
        if self.interval_end != 0 && now_ns >= self.interval_end {
            // The window elapsed and the queue is empty: nothing was backed up, so
            // clear overload and re-arm the window.
            self.overloaded = false;
            self.interval_min = None;
            self.interval_end = now_ns.saturating_add(codel.interval_ns);
        }
    }
}

/// CoDel freshness check, run on **dequeue** before a frame joins a write
/// batch (folly's controlled-delay algorithm).
///
/// For each candidate front frame, computes its sojourn (`now_ns -
/// enqueue_ns`) and folds it into the running per-interval minimum. When an
/// interval (`interval_ns`) closes, the queue enters/leaves the "overloaded"
/// regime based on whether that interval's minimum sojourn exceeded `target`.
/// While overloaded, any front frame whose sojourn exceeds `2 * target` is
/// **dropped** (popped, `out_bytes` decremented, the codel-dropped counter
/// bumped) rather than written — so cores always send *fresh* data. Stops at
/// the first frame that is kept (or when the queue empties).
///
/// Never drops the mid-write front: those bytes are already partly on the
/// wire, and splicing them out would corrupt the peer's stream. The caller
/// passes `front_locked` — true only while the mid-write frame is STILL the
/// deque's front (not yet popped into the flush batch); once the gather has
/// taken it, vetting resumes for the frames behind it (R19: the raw cursor
/// stays `> 0` until the resuming write completes, so keying off the cursor
/// inside the gather wrongly exempted every frame behind a mid-write front —
/// up to `WRITEV_MAX_SLICES` frames / `WRITEV_MAX_BYTES` bytes per resume —
/// from freshness vetting). A `target_ns` of `0` disables the overlay entirely
/// (pure drop-head).
// 8 parameters: the same disjoint out-queue state bundle `flush_coalesced`
// threads (see its allow note) — the Phase-3-reviewed accounting lives here.
#[allow(clippy::too_many_arguments)]
fn codel_dequeue(
    out: &mut VecDeque<OutFrame>,
    front_locked: bool,
    out_bytes: &mut usize,
    inflight_delta: &mut i64,
    codel: CodelParams,
    codel_state: &mut CodelState,
    codel_dropped: &mut u64,
    now_ns: u64,
) {
    if !codel.enabled() {
        return;
    }
    let two_target = codel.target_ns.saturating_mul(2);
    loop {
        let Some(&(_, enqueue_ns)) = out.front() else {
            // Empty queue: no item is standing in line. Do NOT fold a sample
            // (folly's algorithm samples real dequeues only), but let the
            // overloaded flag age out if the interval has since closed with no
            // sample — a backlog that fully drained is, by definition, fresh.
            codel_state.age_empty(codel, now_ns);
            return;
        };
        let sojourn = now_ns.saturating_sub(enqueue_ns);
        // Fold this real dequeue's sojourn into the interval minimum and (when
        // the window closes) update the overloaded flag.
        codel_state.note_interval(codel, now_ns, sojourn);

        // The mid-write front is locked: it is already partly on the wire and
        // must be sent to completion, stale or not.
        if front_locked {
            return;
        }
        // FRESHEST-WINS invariant: never CoDel-drop the LAST remaining frame.
        // When a slow consumer's whole backlog is stale, CoDel skips straight
        // past the old frames to the NEWEST one — maximally fresh — but the
        // newest itself is always kept and sent. So even a fully-stale queue
        // still delivers its freshest frame, exactly like drop-head's
        // freshest-wins (drop-head evicts the oldest; CoDel here drops stale
        // leading frames, but both always preserve the newest).
        if codel_state.overloaded && sojourn > two_target && out.len() > 1 {
            // Stale frame (and not the last one) in the overloaded regime:
            // drop it and look at the next one (which may also be stale).
            let (victim, _ts) = out.pop_front().expect("front checked");
            *out_bytes -= victim.len();
            // CoDel staleness drop: this queued byte is discarded unsent, so
            // fold the negative delta in for the worker's incremental total.
            *inflight_delta -= victim.len() as i64;
            *codel_dropped += 1;
            continue;
        }
        // Fresh enough, not overloaded, or the last remaining (freshest) frame:
        // keep it; the flush batch takes it.
        return;
    }
}

/// The coalescing flush core (F4), shared by the plain and TLS paths: gather
/// whole queued frames into one vectored batch, hand it to `sink` in a single
/// call, and apply the result across frame boundaries. One syscall per batch
/// instead of one per frame.
///
/// Per batch: up to [`WRITEV_MAX_SLICES`] frames and `max_bytes` bytes (the
/// FIRST frame is always included, so every write makes progress). A partial
/// `Ok(n)` advances `out_cursor` across the frames the batch covered —
/// fully-written frames are popped, folding `out_bytes`/`inflight_delta` by
/// their FULL lengths exactly like the one-frame-per-write loop did (a
/// mid-write frame's earlier partial bytes were never folded; its completing
/// write folds the whole frame) — and the mid-write remainder is pushed back
/// to the front of the deque, keeping the `locked = out_cursor > 0` eviction
/// guard meaningful. `WouldBlock`/`Interrupted`/`Closed` restore the
/// untouched batch verbatim, so queue state is never lost.
///
/// CoDel runs per frame exactly as before: each frame is checked as it
/// reaches the front, and the gather POPS the checked frame before checking
/// the next, so the deque each check sees (and therefore every drop decision)
/// is byte-identical to the one-write-per-frame loop's. The mid-write lock is
/// GATHER-AWARE (R19): a frame counts as locked only while it is still the
/// deque's front — once the gather has popped it into the batch, the frames
/// behind it are vetted normally, so staleness dropping resumes right behind a
/// mid-write front instead of being suspended for the whole batch. One benign
/// sampling difference: a frame the gather stops at (batch limit hit) or the
/// tail of a partially-written batch is re-sampled on the next batch/flush
/// attempt; CoDel folds interval MINIMA, so an extra early sample can only
/// understate staleness, never fabricate it.
// 11 parameters: the disjoint out-queue state the verified accounting
// invariants live in. Bundling them into a struct would re-home Phase-3-
// reviewed state for a perf change; the seam also lets the unit tests drive
// this loop with a mock sink.
#[allow(clippy::too_many_arguments)]
fn flush_coalesced<W: WriteSink>(
    out: &mut VecDeque<OutFrame>,
    out_cursor: &mut usize,
    out_bytes: &mut usize,
    inflight_delta: &mut i64,
    codel: CodelParams,
    codel_state: &mut CodelState,
    codel_dropped: &mut u64,
    batch: &mut Vec<OutFrame>,
    max_bytes: usize,
    sink: &mut W,
    now_ns: u64,
) -> WriteStatus {
    loop {
        // CoDel: drop stale leading frames; on return the front (if any) is
        // keepable. Completes before the batch borrows below. The front is
        // locked only while the mid-write frame is still the deque's front —
        // at this point the batch is always empty (every exit/continue path
        // drains it), so the locked frame, if any, has not been gathered yet.
        codel_dequeue(
            out,
            *out_cursor > 0 && batch.is_empty(),
            out_bytes,
            inflight_delta,
            codel,
            codel_state,
            codel_dropped,
            now_ns,
        );
        if out.is_empty() {
            // Release the last batch's frames promptly (they are only Arc
            // handles, but fan-out data should not outlive its send).
            batch.clear();
            break;
        }

        // Gather one batch of whole frames from the (keepable) front. Frames
        // are MOVED out of the deque, so CoDel's next check sees exactly the
        // deque the one-write-per-frame loop would have at the same point.
        let start_cursor = *out_cursor;
        batch.clear();
        let mut batch_bytes = 0usize;
        // INVARIANT (loop entry): the front (if any) is keepable —
        // codel_dequeue just ran, either at the top of the flush or after the
        // previous pop.
        while let Some((front, _ts)) = out.front() {
            let take = front.len() - if batch.is_empty() { start_cursor } else { 0 };
            // The first frame always joins (progress guarantee); later ones
            // only while BOTH limits hold.
            if !batch.is_empty()
                && (batch.len() >= WRITEV_MAX_SLICES || batch_bytes + take > max_bytes)
            {
                break;
            }
            let frame = out.pop_front().expect("front checked");
            batch_bytes += take;
            batch.push(frame);
            // Check the NEXT frame before it can join the batch, so CoDel's
            // per-frame dequeue decision is made against the same shrunken
            // deque the one-write-per-frame loop saw. The batch is non-empty
            // here, so a mid-write front gathered above no longer locks the
            // frame now at the deque front: vetting has resumed for the
            // frames behind it (R19).
            codel_dequeue(
                out,
                *out_cursor > 0 && batch.is_empty(),
                out_bytes,
                inflight_delta,
                codel,
                codel_state,
                codel_dropped,
                now_ns,
            );
        }

        // One vectored write for the whole batch. The iovecs borrow `batch`
        // (a local vec of owned frames), never `out`, so the deque stays
        // freely mutable while the slices are live.
        let slices: Vec<IoSlice<'_>> = batch
            .iter()
            .enumerate()
            .map(|(i, (f, _))| IoSlice::new(if i == 0 { &f[start_cursor..] } else { &f[..] }))
            .collect();
        match sink.write_batch(&slices) {
            Ok(0) => {
                // A zero-length write on a non-empty batch: the peer can no
                // longer accept data.
                restore_batch(out, batch);
                return WriteStatus::Closed;
            }
            Ok(mut n) => {
                debug_assert!(n <= batch_bytes, "sink reported more than it was given");
                // Consume fully-written frames. NOTE: the FULL frame length is
                // folded for each, even one that was already partially written
                // by an earlier batch — those earlier bytes were never folded,
                // so the completing write folds the whole frame, exactly like
                // the previous one-frame-per-write loop.
                let mut idx = 0;
                while idx < batch.len() {
                    let rem = batch[idx].0.len() - if idx == 0 { start_cursor } else { 0 };
                    if n < rem {
                        break;
                    }
                    n -= rem;
                    *out_bytes -= batch[idx].0.len();
                    *inflight_delta -= batch[idx].0.len() as i64;
                    idx += 1;
                }
                if idx == batch.len() {
                    // Batch fully written: the queue front (if any) is now a
                    // fresh, not-yet-written frame — reset the cursor so it
                    // never points into a frame that has already gone out.
                    *out_cursor = 0;
                    // Drop the sent frames' Arc handles now, not at the next
                    // flush.
                    batch.clear();
                    // Cork on with the next batch.
                    continue;
                }
                // Partial write: push the unwritten tail back to the front and
                // leave the cursor inside frame `idx`.
                *out_cursor = if idx == 0 { start_cursor + n } else { n };
                for frame in batch.drain(..).skip(idx).rev() {
                    out.push_front(frame);
                }
                // Cork on: keep writing (the remainder plus whatever the
                // limits now admit) until the socket blocks or the queue
                // empties — a short write is not a block.
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                restore_batch(out, batch);
                return WriteStatus::WouldBlock;
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {
                // Retry the same batch; nothing was consumed.
                restore_batch(out, batch);
            }
            Err(_) => {
                restore_batch(out, batch);
                return WriteStatus::Closed;
            }
        }
    }
    WriteStatus::Drained
}

/// Push an unwritten batch back to the FRONT of the queue, restoring the
/// exact pre-gather state (frame order; the cursor was never touched during
/// the gather, so it still points where it did). Used on every non-writing
/// outcome so no queued frame is ever lost.
fn restore_batch(out: &mut VecDeque<OutFrame>, batch: &mut Vec<OutFrame>) {
    for frame in batch.drain(..).rev() {
        out.push_front(frame);
    }
}

/// Shared TLS-handshake test support (G2): the raw materials for forcing a
/// *blocked* handshake flight. A connected socket pair whose server end has a
/// tiny `SO_SNDBUF` and whose peer end has a tiny `SO_RCVBUF`; a rustls server
/// config whose certificate is deliberately bloated (thousands of SANs, still
/// safely under rustls's 64 KiB inbound handshake-message cap) so the
/// ServerHello flight exceeds both buffers; and a raw
/// [`rustls::ClientConnection`] peer the test drives by hand (a real async
/// client keeps reading, so it can never pin its own receive window).
///
/// Used by the unit tests here and by the worker-loop test in
/// `transport::worker`.
#[cfg(test)]
pub(crate) mod tls_test_support {
    use std::io::ErrorKind;
    use std::net::TcpStream as StdTcpStream;
    use std::sync::Arc;

    /// `SO_SNDBUF` asked of the server (mio) end. Kernels clamp to a floor and
    /// Linux doubles the ask, so the effective value lands in single-digit KiB —
    /// far below the bloated flight either way.
    const SERVER_SNDBUF: usize = 1024;
    /// `SO_RCVBUF` asked of the peer end (same clamp story).
    const PEER_RCVBUF: usize = 1024;
    /// How many bloated SANs to put in the cert. Each is ~32 DER bytes, so the
    /// cert DER lands around 50 KiB: well above every clamped buffer sum, well
    /// below rustls's 64 KiB handshake-message cap.
    const SAN_COUNT: usize = 1500;

    /// A connected socket pair for the blocked-handshake tests: a non-blocking
    /// mio server end with a tiny send buffer and a non-blocking std peer end
    /// whose receive buffer was shrunk BEFORE connect (so the initial window is
    /// tiny too). A handshake flight bigger than both buffers makes the
    /// server's `write_tls` return `WouldBlock` mid-flight.
    pub(crate) fn pair_tiny_tls() -> (mio::net::TcpStream, StdTcpStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // Peer end: shrink the receive buffer BEFORE connecting so even the
        // initial advertised window is tiny.
        let peer_sock = socket2::Socket::new(
            socket2::Domain::for_address(addr),
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )
        .unwrap();
        peer_sock.set_recv_buffer_size(PEER_RCVBUF).unwrap();
        peer_sock.connect(&addr.into()).unwrap();
        let peer = StdTcpStream::from(peer_sock);
        peer.set_nonblocking(true).unwrap();

        let (server, _) = listener.accept().unwrap();
        socket2::SockRef::from(&server)
            .set_send_buffer_size(SERVER_SNDBUF)
            .unwrap();
        server.set_nonblocking(true).unwrap();
        (mio::net::TcpStream::from_std(server), peer)
    }

    /// A rustls server config whose certificate is bloated past every clamped
    /// socket buffer, plus the DER of that (self-signed) certificate so a test
    /// client can be built to trust exactly it.
    pub(crate) fn bloated_server_config() -> (
        Arc<rustls::ServerConfig>,
        rustls::pki_types::CertificateDer<'static>,
    ) {
        let _ = rustls::crypto::ring::default_provider().install_default();
        // "localhost" first (the client verifies that name); the rest is pure
        // bloat to push the Certificate message past the tiny socket buffers.
        let mut sans: Vec<String> = Vec::with_capacity(SAN_COUNT + 1);
        sans.push("localhost".to_string());
        sans.extend((0..SAN_COUNT).map(|i| format!("san-{i:04}-abcdefghijklmnopqrst")));
        let params = rcgen::CertificateParams::new(sans).unwrap();
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();

        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(key_pair.serialize_der().into());
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.der().clone()], key)
            .expect("build rustls server config");
        (Arc::new(config), cert.der().clone())
    }

    /// A raw rustls client that trusts only `cert` and connects as
    /// "localhost" (the bloated SAN list includes it).
    pub(crate) fn tls_client(
        cert: &rustls::pki_types::CertificateDer<'static>,
    ) -> rustls::ClientConnection {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.clone()).expect("trust test cert");
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let name = rustls::pki_types::ServerName::try_from("localhost").expect("parse server name");
        rustls::ClientConnection::new(Arc::new(config), name).expect("build client connection")
    }

    /// One non-blocking pump of the raw TLS client: ingest whatever ciphertext
    /// is currently readable, advance the state machine, and write out anything
    /// it wants to send. `WouldBlock` on either side just ends that side's
    /// pass. Panics on hard errors (a failed handshake fails the test loudly).
    pub(crate) fn pump_client(client: &mut rustls::ClientConnection, sock: &mut StdTcpStream) {
        loop {
            match client.read_tls(sock) {
                Ok(0) => panic!("server closed the socket mid-handshake"),
                Ok(_) => {
                    client
                        .process_new_packets()
                        .expect("client TLS state machine");
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => panic!("client read_tls failed: {e}"),
            }
        }
        while client.wants_write() {
            match client.write_tls(sock) {
                Ok(_) => {}
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => panic!("client write_tls failed: {e}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // `Read`/`Write` come in via `super::*` (the parent module imports them);
    // the tests call `.read`/`.write_all` on the std peer socket through those.
    use super::*;
    use std::net::TcpStream as StdTcpStream;

    /// A connected socket pair: a non-blocking mio server end (the side under
    /// test) and a blocking std client end (the test's "peer", kept blocking so
    /// reads/writes in the test are simple and deterministic).
    fn pair() -> (mio::net::TcpStream, StdTcpStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = StdTcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();
        server.set_nonblocking(true).unwrap();
        let mio_server = mio::net::TcpStream::from_std(server);
        client.set_nonblocking(false).unwrap(); // blocking peer for test simplicity
        (mio_server, client)
    }

    /// A socket pair like [`pair`], but with a tiny `SO_SNDBUF` on the server end
    /// so a multi-MB frame cannot be written in one `flush` — the first flush
    /// fills the kernel send buffer and stops partway, leaving `out_cursor > 0`
    /// on the front frame. The blocking peer is returned but deliberately *not*
    /// drained by the caller, so the send buffer stays full and the front stays
    /// mid-write.
    fn pair_tiny_sndbuf() -> (mio::net::TcpStream, StdTcpStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = StdTcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();
        server.set_nonblocking(true).unwrap();
        // Shrink the send buffer so a 4 MB frame is forced into a partial write.
        socket2::SockRef::from(&server)
            .set_send_buffer_size(8 * 1024)
            .unwrap();
        let mio_server = mio::net::TcpStream::from_std(server);
        client.set_nonblocking(false).unwrap();
        (mio_server, client)
    }

    /// Encode an unmasked server text frame into a fresh `Arc<[u8]>`.
    fn text_frame(payload: &[u8]) -> Arc<[u8]> {
        let mut out = BytesMut::new();
        frame::encode_text(&mut out, payload);
        Arc::from(out.to_vec().into_boxed_slice())
    }

    /// Read exactly `n` bytes from the blocking peer.
    fn read_exact_n(client: &mut StdTcpStream, n: usize) -> Vec<u8> {
        let mut buf = vec![0u8; n];
        client.read_exact(&mut buf).unwrap();
        buf
    }

    // ---- queue + flush drains -------------------------------------------------
    #[test]
    fn queue_then_flush_drains_all_frames() {
        let (server, mut client) = pair();
        let mut conn = Connection::new(server, 1 << 20);

        let f1 = text_frame(b"one");
        let f2 = text_frame(b"two");
        let f3 = text_frame(b"three");
        let mut expected = Vec::new();
        expected.extend_from_slice(&f1);
        expected.extend_from_slice(&f2);
        expected.extend_from_slice(&f3);

        assert_eq!(conn.queue(f1, 0), 0);
        assert_eq!(conn.queue(f2, 0), 0);
        assert_eq!(conn.queue(f3, 0), 0);
        assert!(conn.has_pending_writes());

        assert_eq!(conn.flush(0), WriteStatus::Drained);
        assert!(!conn.has_pending_writes());
        assert_eq!(conn.out_bytes, 0);

        // The peer receives exactly the three frames, back-to-back.
        let got = read_exact_n(&mut client, expected.len());
        assert_eq!(got, expected);
    }

    // ---- F4: writev coalescing ------------------------------------------------

    /// A call-counting stand-in for the flush sink (the same [`WriteSink`]
    /// interface the production `mio`/TLS sinks implement): records the bytes
    /// each `write_batch` call actually ACCEPTED (the scripted prefix — a
    /// short write's tail never left the queue) plus the offered slice count,
    /// and can script short writes or errors per call — deterministic
    /// partial-write driving that a real socket cannot force.
    struct CountingSink {
        /// The bytes each `write_batch` call accepted, in order.
        calls: Vec<Vec<u8>>,
        /// How many iovecs each call was offered.
        slice_counts: Vec<usize>,
        /// Total bytes each call was offered (a short write accepts less).
        offered: Vec<usize>,
        /// Scripted results, consumed one per call; missing entries mean
        /// "accept the whole batch".
        script: Vec<std::io::Result<usize>>,
    }

    impl CountingSink {
        fn accepting() -> Self {
            CountingSink {
                calls: Vec::new(),
                slice_counts: Vec::new(),
                offered: Vec::new(),
                script: Vec::new(),
            }
        }

        fn scripted(script: Vec<std::io::Result<usize>>) -> Self {
            CountingSink {
                calls: Vec::new(),
                slice_counts: Vec::new(),
                offered: Vec::new(),
                script,
            }
        }

        /// All bytes ever accepted by the sink, in order.
        fn received(&self) -> Vec<u8> {
            self.calls.iter().flatten().copied().collect()
        }
    }

    impl WriteSink for CountingSink {
        fn write_batch(&mut self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
            let total: usize = bufs.iter().map(|b| b.len()).sum();
            let ret = match self.script.get(self.calls.len()) {
                Some(Ok(n)) => Ok(*n),
                Some(Err(e)) => Err(std::io::Error::from(e.kind())),
                None => Ok(total),
            };
            // Record only the accepted prefix of the offered batch.
            let accepted = match ret {
                Ok(n) => n.min(total),
                Err(_) => 0,
            };
            let mut got = Vec::with_capacity(accepted);
            let mut need = accepted;
            for b in bufs {
                if need == 0 {
                    break;
                }
                let take = need.min(b.len());
                got.extend_from_slice(&b[..take]);
                need -= take;
            }
            self.calls.push(got);
            self.slice_counts.push(bufs.len());
            self.offered.push(total);
            ret
        }
    }

    /// Standalone out-queue state (exactly the fields [`Connection`] keeps)
    /// for driving [`flush_coalesced`] directly against a mock sink.
    struct OutState {
        out: VecDeque<OutFrame>,
        out_cursor: usize,
        out_bytes: usize,
        inflight_delta: i64,
        codel: CodelParams,
        codel_state: CodelState,
        codel_dropped: u64,
        batch: Vec<OutFrame>,
    }

    impl OutState {
        fn new() -> Self {
            OutState {
                out: VecDeque::new(),
                out_cursor: 0,
                out_bytes: 0,
                inflight_delta: 0,
                codel: CodelParams::DISABLED,
                codel_state: CodelState::default(),
                codel_dropped: 0,
                batch: Vec::new(),
            }
        }

        fn queue(&mut self, frame: Arc<[u8]>, now_ns: u64) {
            self.out_bytes += frame.len();
            self.inflight_delta += frame.len() as i64;
            self.out.push_back((frame, now_ns));
        }

        fn flush_with<W: WriteSink>(&mut self, sink: &mut W, now_ns: u64) -> WriteStatus {
            flush_coalesced(
                &mut self.out,
                &mut self.out_cursor,
                &mut self.out_bytes,
                &mut self.inflight_delta,
                self.codel,
                &mut self.codel_state,
                &mut self.codel_dropped,
                &mut self.batch,
                WRITEV_MAX_BYTES,
                sink,
                now_ns,
            )
        }

        /// Independent sum of the deque (cross-checks `out_bytes`).
        fn true_bytes(&self) -> usize {
            self.out.iter().map(|f| f.0.len()).sum()
        }
    }

    /// (a) Three small frames that fit one batch go out as exactly ONE
    /// vectored write carrying their concatenation, and the queue plus the
    /// inflight accounting fully drain.
    #[test]
    fn writev_coalesces_three_frames_into_one_call() {
        let mut st = OutState::new();
        let mut expected = Vec::new();
        for tag in 1..=3u8 {
            let f = small(tag);
            expected.extend_from_slice(&f[..]);
            st.queue(f, 0);
        }

        let mut sink = CountingSink::accepting();
        assert_eq!(st.flush_with(&mut sink, 0), WriteStatus::Drained);

        assert_eq!(sink.calls.len(), 1, "3 fitting frames must be ONE writev");
        assert_eq!(sink.slice_counts, vec![3], "one iovec per frame");
        assert_eq!(sink.received(), expected);
        assert!(st.out.is_empty());
        assert_eq!(st.out_cursor, 0);
        assert_eq!(st.out_bytes, 0);
        assert_eq!(st.true_bytes(), 0);
        assert_eq!(st.inflight_delta, 0, "+30 queued, then −30 sent");
    }

    /// (b) A short vectored write advances `out_cursor` across frame
    /// boundaries correctly: fully-covered frames pop (folding their FULL
    /// lengths into the accounting, like the one-frame-per-write loop), the
    /// mid-write remainder stays at the front, and the byte stream is
    /// identical to sequential per-frame writes. Sweeps cuts that land
    /// mid-frame and exactly on frame boundaries.
    #[test]
    fn writev_partial_write_advances_cursor_across_frames() {
        for short in [7usize, 10, 15, 20, 29] {
            let mut st = OutState::new();
            let mut expected = Vec::new();
            for tag in 1..=3u8 {
                let f = small(tag);
                expected.extend_from_slice(&f[..]);
                st.queue(f, 0);
            }

            // First call accepts only `short` of the 30 batch bytes; the
            // corkscrew keeps flushing, and the (now accepting) sink drains
            // the remainder.
            let mut sink = CountingSink::scripted(vec![Ok(short)]);
            assert_eq!(
                st.flush_with(&mut sink, 0),
                WriteStatus::Drained,
                "short={short}: a short write is not a block"
            );
            assert_eq!(sink.received(), expected, "short={short}: byte stream");
            assert_eq!(
                sink.calls.first().map(|c| c.len()),
                Some(short),
                "short={short}: first call carried exactly the short count"
            );
            assert!(st.out.is_empty());
            assert_eq!(st.out_cursor, 0);
            assert_eq!(st.out_bytes, 0);
            assert_eq!(st.inflight_delta, 0, "short={short}: +30 −30");
        }
    }

    /// (b, cont.) A short write followed by a BLOCKED socket: the flush
    /// surfaces `WouldBlock` with the partial state persisted — full frames
    /// gone, cursor pointing into the mid-write frame — and a later flush
    /// completes the stream with no duplication and no loss.
    #[test]
    fn writev_partial_then_blocked_persists_and_resumes() {
        let mut st = OutState::new();
        let mut expected = Vec::new();
        for tag in 1..=3u8 {
            let f = small(tag);
            expected.extend_from_slice(&f[..]);
            st.queue(f, 0);
        }

        // 15 bytes land: frame 1 fully (10), 5 into frame 2 — then the socket
        // blocks.
        let mut sink = CountingSink::scripted(vec![
            Ok(15),
            Err(std::io::Error::from(ErrorKind::WouldBlock)),
        ]);
        assert_eq!(st.flush_with(&mut sink, 0), WriteStatus::WouldBlock);

        // Persistent partial state: frames 2 (mid-write) and 3 remain, cursor
        // 5 into frame 2, accounting exact.
        assert_eq!(st.out.len(), 2);
        assert_eq!(st.out_cursor, 5);
        assert_eq!(st.out.front().unwrap().0.len(), 10);
        assert_eq!(st.out_bytes, 20, "counts FULL frames, cursor or not");
        assert_eq!(st.true_bytes(), 20);
        assert_eq!(st.inflight_delta, 20, "+30 queued, −10 sent so far");
        // The mid-write pairing the drop-head guard keys on: the cursor is an
        // offset into the CURRENT front.
        assert!(st.out_cursor < st.out.front().unwrap().0.len());

        // Resume: a fresh accepting flush drains the rest; total bytes are
        // exactly the concatenation (no dup, no gap).
        let mut sink2 = CountingSink::accepting();
        assert_eq!(st.flush_with(&mut sink2, 0), WriteStatus::Drained);
        let mut all = sink.received();
        all.extend(sink2.received());
        assert_eq!(all, expected);
        assert_eq!(st.out_bytes, 0);
        assert_eq!(st.inflight_delta, 0, "+30 −30 across both flushes");
    }

    /// `WouldBlock` before anything is written restores the queue verbatim:
    /// nothing lost, nothing folded.
    #[test]
    fn writev_blocked_flush_restores_queue_verbatim() {
        let mut st = OutState::new();
        for tag in 1..=3u8 {
            st.queue(small(tag), tag as u64);
        }
        let before: Vec<(u8, usize)> = st.out.iter().map(|f| (f.0[0], f.0.len())).collect();

        let mut sink =
            CountingSink::scripted(vec![Err(std::io::Error::from(ErrorKind::WouldBlock))]);
        assert_eq!(st.flush_with(&mut sink, 0), WriteStatus::WouldBlock);
        let after: Vec<(u8, usize)> = st.out.iter().map(|f| (f.0[0], f.0.len())).collect();
        assert_eq!(before, after, "frame order and identity preserved");
        assert_eq!(st.out_cursor, 0);
        assert_eq!(st.out_bytes, 30);
        // Net delta over the flush is zero (nothing sent, nothing dropped).
        assert_eq!(st.inflight_delta, 30);

        // And the queue is still flushable.
        let mut sink2 = CountingSink::accepting();
        assert_eq!(st.flush_with(&mut sink2, 0), WriteStatus::Drained);
        assert_eq!(sink2.calls.len(), 1);
    }

    /// `Interrupted` is retried with the batch restored verbatim (nothing was
    /// consumed), and a zero-length write on a non-empty batch means the peer
    /// is gone (`Closed`, queue still intact for post-mortem).
    #[test]
    fn writev_interrupted_retries_and_zero_write_closes() {
        let mut st = OutState::new();
        for tag in 1..=3u8 {
            st.queue(small(tag), 0);
        }
        let mut sink =
            CountingSink::scripted(vec![Err(std::io::Error::from(ErrorKind::Interrupted))]);
        assert_eq!(st.flush_with(&mut sink, 0), WriteStatus::Drained);
        assert_eq!(sink.calls.len(), 2, "EINTR then the retry");
        assert_eq!(sink.offered[0], sink.offered[1], "same batch both times");
        assert!(sink.calls[0].is_empty(), "EINTR consumed nothing");
        assert_eq!(sink.received().len(), 30);
        assert_eq!(st.out_bytes, 0);

        let mut st2 = OutState::new();
        for tag in 1..=3u8 {
            st2.queue(small(tag), 0);
        }
        let mut sink2 = CountingSink::scripted(vec![Ok(0)]);
        assert_eq!(st2.flush_with(&mut sink2, 0), WriteStatus::Closed);
        assert_eq!(st2.out.len(), 3, "closed with the queue untouched");
        assert_eq!(st2.out_bytes, 30);
        assert_eq!(st2.inflight_delta, 30);
    }

    /// The gather honours the IOV_MAX slice budget: 3000 tiny frames flush in
    /// ceil(3000 / 1024) = 3 calls, the first two carrying exactly 1024
    /// iovecs.
    #[test]
    fn writev_batch_respects_iov_max_slice_budget() {
        let mut st = OutState::new();
        for tag in 0..250u8 {
            for _ in 0..12 {
                st.queue(small(tag), 0);
            }
        }
        assert_eq!(st.out.len(), 3000);

        let mut sink = CountingSink::accepting();
        assert_eq!(st.flush_with(&mut sink, 0), WriteStatus::Drained);
        assert_eq!(
            sink.calls.len(),
            3,
            "3000 frames / 1024 iovecs = 3 syscalls"
        );
        assert_eq!(sink.slice_counts[0], 1024);
        assert_eq!(sink.slice_counts[1], 1024);
        assert_eq!(sink.slice_counts[2], 3000 - 2 * 1024);
        assert_eq!(st.out_bytes, 0);
    }

    /// The per-syscall byte budget splits oversized backlogs: three 100 KiB
    /// frames (300 KiB total > 256 KiB budget) flush as [2 frames, 1 frame],
    /// and the stream is still byte-exact.
    #[test]
    fn writev_batch_respects_byte_budget() {
        let mut st = OutState::new();
        let mut expected = Vec::new();
        for tag in 1..=3u8 {
            let f: Arc<[u8]> = Arc::from(vec![tag; 100 * 1024].into_boxed_slice());
            expected.extend_from_slice(&f[..]);
            st.queue(f, 0);
        }

        let mut sink = CountingSink::accepting();
        assert_eq!(st.flush_with(&mut sink, 0), WriteStatus::Drained);
        assert_eq!(
            sink.slice_counts,
            vec![2, 1],
            "200 KiB then the leftover 100 KiB"
        );
        assert_eq!(sink.received(), expected);
        assert_eq!(st.out_bytes, 0);
    }

    /// CoDel's per-frame dequeue decisions survive batching: once overloaded,
    /// a flush of stale frames drops every stale leading frame on dequeue and
    /// only the freshest reaches the sink — one writev, one frame's bytes.
    #[test]
    fn writev_batching_preserves_codel_staleness_drops() {
        let mut st = OutState::new();
        st.codel = CodelParams {
            target_ns: TARGET_NS,
            interval_ns: INTERVAL_NS,
        };

        // Drive one interval at 6 ms sojourn (> target, < 2×target: sent) to
        // flip into the overloaded regime — one frame per flush, like the
        // timeline tests.
        for k in 0..=20u8 {
            let now = (k as u64 + 1) * 6_000_000;
            st.queue(small(k), now - 6_000_000);
            let mut sink = CountingSink::accepting();
            assert_eq!(st.flush_with(&mut sink, now), WriteStatus::Drained);
        }
        assert!(st.codel_state.overloaded);

        // A batched flush of four stale frames (sojourn 13 ms > 2×target):
        // the first three drop on dequeue, the freshest is sent alone.
        let now = 200_000_000u64;
        for tag in 90..=93u8 {
            st.queue(small(tag), now - 13_000_000);
        }
        let mut sink = CountingSink::accepting();
        assert_eq!(st.flush_with(&mut sink, now), WriteStatus::Drained);
        assert_eq!(st.codel_dropped, 3, "three stale frames dropped");
        assert_eq!(sink.calls.len(), 1);
        assert_eq!(sink.received(), vec![93u8; 10], "only the freshest sent");
        assert_eq!(st.out_bytes, 0);
        assert_eq!(st.inflight_delta, 0, "+40 queued, −30 dropped, −10 sent");
    }

    /// R19 (Task 6.2 review carry-in): a flush that resumes with a mid-write
    /// front must still CoDel-vet the frames gathered BEHIND that front. The
    /// buggy shape threaded the raw `out_cursor` (still `> 0` until the
    /// resuming write completes) into the in-gather `codel_dequeue`, which read
    /// it as "the front is mid-write and locked" — but the front under check
    /// had already been popped into the batch, so stale frames queued behind a
    /// mid-write front escaped freshness vetting entirely (up to
    /// [`WRITEV_MAX_SLICES`] frames / [`WRITEV_MAX_BYTES`] bytes per resume).
    ///
    /// Overload-style timeline: drive the queue into the overloaded regime,
    /// lock a partially-written front (short write + block), queue stale frames
    /// behind it (sojourn 13 ms > 2×target 10 ms), then RESUME the flush — the
    /// stale leading frames must drop and only the freshest may follow the
    /// locked front out.
    #[test]
    fn codel_vetting_resumes_behind_mid_write_front() {
        let mut st = OutState::new();
        st.codel = CodelParams {
            target_ns: TARGET_NS,
            interval_ns: INTERVAL_NS,
        };

        // Drive one interval at 6 ms sojourn (> target, < 2×target) to flip
        // into the overloaded regime.
        for k in 0..=20u8 {
            let now = (k as u64 + 1) * 6_000_000;
            st.queue(small(k), now - 6_000_000);
            let mut sink = CountingSink::accepting();
            assert_eq!(st.flush_with(&mut sink, now), WriteStatus::Drained);
        }
        assert!(st.codel_state.overloaded);

        // A fresh front frame, partially written then blocked: the flush
        // accepts 5 of its 10 bytes and the socket fills — the front stays
        // mid-write (out_cursor 5). It must survive (locked, and fresh
        // regardless).
        let now = 200_000_000u64;
        st.queue(small(50), now - 1_000_000); // sojourn 1 ms — fresh
        let mut sink = CountingSink::scripted(vec![
            Ok(5),
            Err(std::io::Error::from(ErrorKind::WouldBlock)),
        ]);
        assert_eq!(st.flush_with(&mut sink, now), WriteStatus::WouldBlock);
        assert_eq!(st.out_cursor, 5, "front is mid-write");
        assert_eq!(st.out.len(), 1);

        // Three STALE frames queue behind the locked front (sojourn 13 ms >
        // 2×target 10 ms while overloaded).
        for tag in [60u8, 61, 62] {
            st.queue(small(tag), now - 13_000_000);
        }

        // RESUME the flush (same timestamp, so the staleness verdicts are
        // unchanged): the locked front completes; vetting must have resumed
        // behind it — tags 60 and 61 drop as stale, the freshest (62) follows
        // the front out.
        let mut sink2 = CountingSink::accepting();
        assert_eq!(st.flush_with(&mut sink2, now), WriteStatus::Drained);
        assert_eq!(
            st.codel_dropped, 2,
            "stale frames behind the mid-write front must drop on the resume"
        );
        let mut expected = vec![50u8; 5]; // the locked front's unwritten tail
        expected.extend_from_slice(&[62u8; 10]); // then the freshest frame
        assert_eq!(
            sink2.received(),
            expected,
            "no stale byte may reach the wire"
        );
        assert!(st.out.is_empty());
        assert_eq!(st.out_bytes, 0);
        assert_eq!(st.true_bytes(), 0);
        assert_eq!(
            st.inflight_delta, 0,
            "+40 queued, −10 front sent, −20 dropped, −10 freshest sent"
        );
    }

    // ---- partial write / WouldBlock ------------------------------------------
    #[test]
    fn partial_write_advances_cursor_across_flushes() {
        // Both ends non-blocking so the single-threaded test can interleave
        // flush (server) and drain (peer) without ever blocking on a read that
        // would deadlock when no more data is in flight.
        let (server, client) = pair();
        client.set_nonblocking(true).unwrap();

        // Shrink the send buffer so a multi-MB frame cannot go out in one write.
        socket2::SockRef::from(&server)
            .set_send_buffer_size(8 * 1024)
            .unwrap();

        // 4 MiB payload — far larger than any send/recv buffer, so writes are
        // forced partial and at least one flush must WouldBlock.
        let payload = vec![0xABu8; 4 * 1024 * 1024];
        let frame_bytes = text_frame(&payload);
        let total = frame_bytes.len();

        let mut conn = Connection::new(server, total + 1);
        assert_eq!(conn.queue(Arc::clone(&frame_bytes), 0), 0);

        // First flush: the kernel send buffer fills and we stop partway.
        assert_eq!(conn.flush(0), WriteStatus::WouldBlock);
        assert!(conn.has_pending_writes());
        let cursor_after_first = conn.out_cursor;
        assert!(
            cursor_after_first > 0 && cursor_after_first < total,
            "expected a partial write, cursor = {cursor_after_first}"
        );

        // Interleave: peer drains whatever is available (non-blocking), then the
        // server flushes more. Repeat until the queue drains. A single flush can
        // only push as much as the small send buffer holds, so the cursor
        // advances across many flushes.
        let mut received = Vec::with_capacity(total);
        let mut chunk = vec![0u8; 64 * 1024];
        let mut last_cursor = cursor_after_first;
        let mut advanced_again = false;
        let mut spins = 0usize;
        let mut drained = false;
        let mut client = client;
        while !drained {
            // Drain everything currently readable on the peer.
            loop {
                match client.read(&mut chunk) {
                    Ok(0) => break, // EOF (won't happen; server still open)
                    Ok(n) => received.extend_from_slice(&chunk[..n]),
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(e) => panic!("peer read failed: {e}"),
                }
            }
            match conn.flush(0) {
                WriteStatus::Drained => drained = true,
                WriteStatus::WouldBlock => {
                    if conn.out_cursor > last_cursor {
                        advanced_again = true;
                        last_cursor = conn.out_cursor;
                    }
                }
                WriteStatus::Closed => panic!("unexpected Closed during partial drain"),
            }
            spins += 1;
            assert!(spins < 1_000_000, "drain made no progress");
            // Brief yield so the kernel moves bytes into the peer's recv buffer.
            std::thread::sleep(std::time::Duration::from_micros(50));
        }

        // Pull whatever the final flush pushed but the peer hasn't read yet.
        let mut tail_spins = 0usize;
        while received.len() < total {
            match client.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => received.extend_from_slice(&chunk[..n]),
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    tail_spins += 1;
                    assert!(tail_spins < 1_000_000, "tail drain stalled");
                    std::thread::sleep(std::time::Duration::from_micros(50));
                }
                Err(e) => panic!("peer tail read failed: {e}"),
            }
        }

        assert!(advanced_again, "cursor never advanced on a second flush");
        assert!(!conn.has_pending_writes());
        assert_eq!(conn.out_bytes, 0);
        assert_eq!(received.len(), total);
        // Server frames are unmasked (the codec's client-only `parse` would
        // reject them), so just assert the payload bytes survived intact.
        let payload_start = total - payload.len();
        assert_eq!(&received[payload_start..], &payload[..]);
    }

    // ---- drop-head (SP10) -----------------------------------------------------
    #[test]
    fn queue_drops_oldest_when_over_cap_keeping_newest() {
        let (mio_s, _peer) = pair(); // existing test helper
        let mut c = Connection::new(mio_s, 100); // 100-byte cap
                                                 // frames of 40 bytes each; 3 of them = 120 > 100 → oldest dropped, newest kept
        let f = |n: u8| -> std::sync::Arc<[u8]> {
            std::sync::Arc::from(vec![n; 40].into_boxed_slice())
        };
        assert_eq!(c.queue(f(1), 0), 0); // returns dropped count = 0
        assert_eq!(c.queue(f(2), 0), 0); // out_bytes = 80
        let dropped = c.queue(f(3), 0); // 120 > 100 → drop oldest (f(1)) → out = [f2,f3], 80 bytes
        assert_eq!(dropped, 1);
        assert_eq!(c.out_bytes(), 80);
        assert_eq!(c.queued_len(), 2);
        // the surviving frames are the NEWEST two (f2, f3), not f1
        assert_eq!(c.peek_back_byte(), 3);
        assert_eq!(c.peek_front_byte(), 2);
    }

    #[test]
    fn drop_head_never_evicts_the_partially_written_front() {
        let (mio_s, peer) = pair_tiny_sndbuf(); // tiny SO_SNDBUF so flush leaves a partial front
        let mut c = Connection::new(mio_s, 100);
        c.queue(
            std::sync::Arc::from(vec![1u8; 4_000_000].into_boxed_slice()),
            0,
        ); // huge → partial write
        let _ = c.flush(0); // out_cursor now > 0 on front
        assert!(c.out_cursor() > 0);
        // queue more small frames past the cap; the mid-write front MUST survive (peer would corrupt otherwise)
        for n in 0..50u8 {
            let _ = c.queue(std::sync::Arc::from(vec![n; 40].into_boxed_slice()), 0);
        }
        assert!(c.out_cursor() > 0, "front still mid-write");
        assert!(c.front_is_the_huge_frame()); // i.e. index 0 is untouched
                                              // Keep the peer alive until here so the socket doesn't close mid-test and
                                              // turn the partial write into a Closed status.
        let _ = peer.peer_addr();
        drop(peer);
    }

    /// (a, real socket) Many queued frames flushed once arrive at the peer as
    /// their exact concatenation — the writev path on a REAL `mio` stream.
    #[test]
    fn flush_many_frames_matches_sequential_writes() {
        let (server, mut client) = pair();
        let mut conn = Connection::new(server, 1 << 20);

        let mut expected = Vec::new();
        for k in 0..200u8 {
            let f = text_frame(&[k; 16]);
            expected.extend_from_slice(&f);
            assert_eq!(conn.queue(f, 0), 0);
        }

        assert_eq!(conn.flush(0), WriteStatus::Drained);
        assert!(!conn.has_pending_writes());
        assert_eq!(conn.out_bytes, 0);
        assert_eq!(read_exact_n(&mut client, expected.len()), expected);
    }

    /// (c, real socket) A front left MID-WRITE by a batched flush is never
    /// evicted by drop-head, and once the backlog pressure clears, the peer
    /// receives the surviving frames' bytes in order — the stream is never
    /// spliced mid-frame. This drives the eviction guard against the REAL
    /// partial-write states the writev path now produces.
    #[test]
    fn partial_writev_front_survives_drop_head_eviction() {
        let (server, peer) = pair_tiny_sndbuf();
        let mut peer = peer;
        peer.set_nonblocking(true).unwrap();

        // ~4.1 MB cap with a ~4.1 MB enqueue of 1000-byte frames (1000 chosen
        // deliberately: the kernel accepts exactly the free send-buffer space
        // per flush — with SO_SNDBUF 8 KiB that is 8192 bytes, which 1000
        // does NOT divide, so partial writes reliably land MID-frame; 1024
        // would align perfectly and never produce a mid-write front here).
        let mut c = Connection::new(server, 4_100_000);
        for i in 0..4096u32 {
            let tag = (i % 251) as u8 + 1;
            let _ = c.queue(std::sync::Arc::from(vec![tag; 1000].into_boxed_slice()), 0);
        }
        assert!(c.out_bytes() > 3_000_000, "a real backlog must remain");

        // Flush until a batch stops MID-FRAME. A partial write can land
        // exactly on a frame boundary (cursor 0, front not mid-write — a
        // legal state where eviction is allowed); drain the peer and flush
        // again until the stop is genuinely mid-frame.
        let mut chunk = vec![0u8; 64 * 1024];
        let mut received = Vec::new();
        let mut rounds = 0usize;
        loop {
            rounds += 1;
            assert!(rounds < 500, "flush never stopped mid-frame");
            let status = c.flush(0);
            loop {
                match peer.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => received.extend_from_slice(&chunk[..n]),
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(e) => panic!("peer read failed: {e}"),
                }
            }
            match status {
                WriteStatus::WouldBlock if c.out_cursor() > 0 => break,
                WriteStatus::WouldBlock => continue, // boundary-aligned stop; retry
                WriteStatus::Drained => panic!("the multi-MB backlog cannot fully fit"),
                WriteStatus::Closed => panic!("unexpected Closed"),
            }
        }

        // The mid-write front: cursor points INTO the front frame.
        let front_byte = c.peek_front_byte();
        let front_len = c.out.front().unwrap().0.len();
        assert!(c.out_cursor() > 0 && c.out_cursor() < front_len);

        // Pressure the cap with more queues → drop-head evictions take the
        // oldest droppable slot; the MID-WRITE front must survive untouched.
        for i in 0..500u32 {
            let tag = (i % 251) as u8 + 1;
            let _ = c.queue(std::sync::Arc::from(vec![tag; 1000].into_boxed_slice()), 0);
        }
        assert!(c.out_cursor() > 0, "front still mid-write");
        assert_eq!(
            c.peek_front_byte(),
            front_byte,
            "mid-write front never evicted"
        );
        assert_eq!(c.out.front().unwrap().0.len(), front_len);
        assert!(c.drophead_dropped() > 0, "evictions actually fired");

        // Snapshot the survivors (same module: private field access) and drive
        // the drain to completion, interleaving peer reads like the classic
        // partial-write test. Bytes of the mid-write front already on the wire
        // (`cursor_at_snapshot`) were received earlier; everything still owed
        // is expected[cursor_at_snapshot..].
        let expected: Vec<u8> = c.out.iter().flat_map(|f| f.0.iter().copied()).collect();
        let already = received.len();
        let cursor_at_snapshot = c.out_cursor();
        loop {
            loop {
                match peer.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => received.extend_from_slice(&chunk[..n]),
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(e) => panic!("peer read failed: {e}"),
                }
            }
            if c.flush(0) == WriteStatus::Drained {
                break;
            }
            std::thread::sleep(std::time::Duration::from_micros(50));
        }
        // Pull the tail the final flush pushed.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let owed = expected.len() - cursor_at_snapshot;
        while received.len() - already < owed {
            match peer.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => received.extend_from_slice(&chunk[..n]),
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    assert!(std::time::Instant::now() < deadline, "tail drain stalled");
                    std::thread::sleep(std::time::Duration::from_micros(50));
                }
                Err(e) => panic!("peer tail read failed: {e}"),
            }
        }
        // Everything still owed after the snapshot arrived, in order, with no
        // duplication and no gap.
        assert_eq!(&received[already..], &expected[cursor_at_snapshot..]);
        assert_eq!(c.out_bytes, 0);
        assert!(!c.has_pending_writes());
    }

    /// (TLS, real rustls pair) The batched `flush_tls` path delivers every
    /// queued frame through rustls byte-exact: 30 frames queued, flushed in
    /// batches, the raw client decrypts exactly their concatenation. Drives
    /// the TlsBatchSink pre/post ciphertext drains and the plaintext-batch
    /// apply logic end to end.
    #[test]
    fn flush_tls_batches_frames_and_delivers_concatenation() {
        use crate::transport::conn::tls_test_support as tlsup;

        let (server_stream, mut client_sock) = tlsup::pair_tiny_tls();
        let (server_cfg, cert_der) = tlsup::bloated_server_config();
        let tls = rustls::server::ServerConnection::new(server_cfg).unwrap();
        let mut conn = Connection::new_tls(server_stream, Box::new(tls), 1 << 20);
        let mut client = tlsup::tls_client(&cert_der);

        // Client sends its ClientHello.
        while client.wants_write() {
            client.write_tls(&mut client_sock).unwrap();
        }

        // Drive the handshake to completion (both sides), like the G2 test.
        let mut buf = BytesMut::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while client.is_handshaking() {
            assert!(std::time::Instant::now() < deadline, "handshake stalled");
            match conn.drain_head_bytes(&mut buf) {
                DrainStatus::Ok | DrainStatus::NeedsWrite => {}
                DrainStatus::Closed => panic!("unexpected Closed mid-handshake"),
            }
            tlsup::pump_client(&mut client, &mut client_sock);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        // Queue 30 tagged frames and flush them in batches (tiny socket
        // buffers force WouldBlock + resume through Phase 1 redrains).
        let mut expected = Vec::new();
        for tag in 0..30u8 {
            let f = small(tag);
            expected.extend_from_slice(&f[..]);
            assert_eq!(conn.queue(f, 0), 0);
        }

        let mut plaintext = Vec::with_capacity(expected.len());
        let mut chunk = [0u8; 4096];
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            assert!(
                std::time::Instant::now() < deadline,
                "app-data drain stalled"
            );
            tlsup::pump_client(&mut client, &mut client_sock);
            loop {
                match client.reader().read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => plaintext.extend_from_slice(&chunk[..n]),
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                    Err(e) => panic!("client plaintext read failed: {e}"),
                }
            }
            if plaintext.len() >= expected.len() {
                break;
            }
            // Let the server ingest any client ciphertext (handshake tail),
            // then push another batch.
            match conn.drain_head_bytes(&mut buf) {
                DrainStatus::Ok | DrainStatus::NeedsWrite => {}
                DrainStatus::Closed => panic!("unexpected Closed ingesting client data"),
            }
            match conn.flush(0) {
                WriteStatus::Drained => {}
                WriteStatus::WouldBlock => {}
                WriteStatus::Closed => panic!("unexpected Closed flushing app data"),
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        assert_eq!(plaintext, expected, "decrypted stream == concatenation");
        assert!(!conn.has_pending_writes());
        assert_eq!(conn.out_bytes, 0);
    }

    // ---- incremental inflight-delta accounting --------------------------------

    /// The signed `out_bytes` accumulator tracks queue/flush/drop exactly: queue N
    /// bytes → delta +N; flush all → delta −N; a drop-head eviction reflects the
    /// evicted bytes; and the running sum of every delta taken equals the final
    /// `out_bytes`.
    #[test]
    fn inflight_delta_tracks_queue_flush_and_drop_head() {
        let (server, mut client) = pair();
        let mut c = Connection::new(server, 100); // 100-byte cap → drop-head fires
        let f = |n: u8, len: usize| -> Arc<[u8]> { Arc::from(vec![n; len].into_boxed_slice()) };

        // Running sum of all deltas taken; must always equal out_bytes().
        let mut running: i64 = 0;
        let take = |c: &mut Connection, running: &mut i64| {
            *running += c.take_inflight_delta();
            assert_eq!(
                *running,
                c.out_bytes() as i64,
                "delta sum must track out_bytes"
            );
        };

        // queue N bytes → delta +N.
        assert_eq!(c.queue(f(1, 40), 0), 0);
        assert_eq!(c.take_inflight_delta(), 40, "queue 40 → +40");
        running += 40;
        assert_eq!(running, c.out_bytes() as i64);

        assert_eq!(c.queue(f(2, 40), 0), 0); // out_bytes = 80, no drop
        take(&mut c, &mut running);

        // queue past the cap → drop-head evicts the oldest; delta = +new − evicted.
        let dropped = c.queue(f(3, 40), 0); // 120 > 100 → drop f(1) (40), add f(3) (40)
        assert_eq!(dropped, 1);
        // Net out_bytes unchanged (80), so the delta over this op is 0 (+40 − 40).
        assert_eq!(
            c.take_inflight_delta(),
            0,
            "drop-head: +40 added − 40 evicted = 0 net"
        );
        // running stays at 80 (matches out_bytes).
        assert_eq!(running, c.out_bytes() as i64);

        // flush all → delta −(bytes sent). Drain the peer so the writes complete.
        assert_eq!(c.flush(0), WriteStatus::Drained);
        let after_flush = c.take_inflight_delta();
        assert_eq!(after_flush, -80, "flush drained 80 queued bytes → −80");
        running += after_flush;
        assert_eq!(running, 0, "sum of all deltas == final out_bytes (0)");
        assert_eq!(c.out_bytes(), 0);
        // Consume what the peer received so the socket buffer doesn't wedge the test.
        let mut sink = [0u8; 256];
        let _ = client.read(&mut sink);
    }

    /// A CoDel staleness drop folds its evicted bytes into the delta too, so the
    /// running sum still equals `out_bytes` when CoDel drops a stale frame.
    #[test]
    fn inflight_delta_tracks_codel_staleness_drop() {
        let (server, peer) = pair();
        peer.set_nonblocking(true).unwrap();
        let mut c = Connection::new(server, 1 << 20);
        c.set_codel(CodelParams {
            target_ns: TARGET_NS,
            interval_ns: INTERVAL_NS,
        });

        let mut running: i64 = 0;
        // Drive one interval at 6 ms sojourn to flip into the overloaded regime.
        for k in 0..=20u8 {
            let now = (k as u64 + 1) * 6_000_000;
            let enqueue = now - 6_000_000;
            c.queue(small(k), enqueue);
            assert_eq!(c.flush(now), WriteStatus::Drained);
            running += c.take_inflight_delta();
            assert_eq!(running, c.out_bytes() as i64);
        }
        assert!(c.is_overloaded());

        // Two stale frames: the older is CoDel-dropped on dequeue, the newer sent.
        let now = 200_000_000;
        c.queue(small(98), now - 13_000_000);
        c.queue(small(99), now - 12_000_000);
        running += c.take_inflight_delta(); // two +10 enqueues
        assert_eq!(running, c.out_bytes() as i64);
        let dropped_before = c.codel_dropped();
        assert_eq!(c.flush(now), WriteStatus::Drained);
        assert_eq!(
            c.codel_dropped(),
            dropped_before + 1,
            "older stale frame dropped"
        );
        running += c.take_inflight_delta(); // −10 (CoDel drop) and −10 (sent)
        assert_eq!(
            running,
            c.out_bytes() as i64,
            "delta tracks CoDel drop + send"
        );
        assert_eq!(c.out_bytes(), 0);
    }

    // ---- CoDel time-in-queue freshness drop (SP10 §7) -------------------------

    // folly defaults used by the deterministic timeline below.
    const TARGET_NS: u64 = 5_000_000; // 5 ms
    const INTERVAL_NS: u64 = 100_000_000; // 100 ms

    /// A small frame (10 bytes) tagged by its first byte so we can identify which
    /// frames the peer received. Small enough that every `flush` write succeeds
    /// outright (no partial writes), keeping the CoDel timeline deterministic.
    fn small(tag: u8) -> Arc<[u8]> {
        Arc::from(vec![tag; 10].into_boxed_slice())
    }

    /// Drain the peer until exactly `expected` tags have been received in total
    /// (cumulative). Retries on WouldBlock — macOS loopback delivers writes
    /// asynchronously, so a byte written by `flush` may not be readable on the
    /// peer until a later scheduler turn. Panics with a clear message after a
    /// 2 s deadline so a real delivery bug still fails the test loudly.
    fn drain_tags_until(peer: &mut StdTcpStream, into: &mut Vec<u8>, expected: usize) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while into.len() < expected {
            let mut chunk = [0u8; 4096];
            match peer.read(&mut chunk) {
                Ok(0) => {}
                Ok(n) => {
                    // Frames are a fixed 10 bytes; record each frame's tag byte.
                    let mut i = 0;
                    while i < n {
                        into.push(chunk[i]);
                        i += 10;
                    }
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {}
                Err(e) => panic!("peer read failed: {e}"),
            }
            if into.len() < expected {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for tags: expected {expected}, got {}",
                    into.len()
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }

    /// (a) Frames whose sojourn stays under `target` are ALL sent — CoDel never
    /// drops a fresh consumer's frames.
    #[test]
    fn codel_sends_all_fresh_frames() {
        let (server, mut peer) = pair();
        peer.set_nonblocking(true).unwrap();
        let mut c = Connection::new(server, 1 << 20);
        c.set_codel(CodelParams {
            target_ns: TARGET_NS,
            interval_ns: INTERVAL_NS,
        });

        let mut got = Vec::new();
        // Enqueue and flush 200 frames, each with sojourn = 1 ms (< target). Span
        // several interval boundaries (now climbs to ~600 ms): the interval
        // minimum is always 1 ms ≤ target, so `overloaded` never sets and nothing
        // drops.
        for k in 0..200u8 {
            let now = (k as u64) * 3_000_000; // 3 ms apart → ~600 ms total
            let enqueue = now.saturating_sub(1_000_000); // sojourn = 1 ms
            assert_eq!(c.queue(small(k), enqueue), 0);
            assert_eq!(c.flush(now), WriteStatus::Drained);
            drain_tags_until(&mut peer, &mut got, k as usize + 1);
        }

        assert_eq!(c.codel_dropped(), 0, "no fresh frame should be dropped");
        assert_eq!(got.len(), 200, "every fresh frame was delivered");
        assert_eq!(got, (0..200u8).collect::<Vec<_>>());
    }

    /// (b)+(c) Once the per-interval minimum sojourn exceeds `target`, the queue
    /// enters the overloaded regime and drops frames whose sojourn > 2×target on
    /// dequeue; when latency recovers below `target`, dropping stops and frames
    /// flow again.
    #[test]
    fn codel_drops_stale_when_overloaded_then_recovers() {
        let (server, mut peer) = pair();
        peer.set_nonblocking(true).unwrap();
        let mut c = Connection::new(server, 1 << 20);
        c.set_codel(CodelParams {
            target_ns: TARGET_NS,
            interval_ns: INTERVAL_NS,
        });
        let mut got = Vec::new();

        // ── Phase 1: drive one full interval with every sojourn = 6 ms (> target
        // 5 ms but < 2×target 10 ms, so these are SENT, not dropped). This makes
        // the interval minimum 6 ms; crossing the interval boundary flips the
        // queue into the overloaded regime. tag bytes 0..=20.
        for k in 0..=20u8 {
            let now = (k as u64 + 1) * 6_000_000; // 6,12,…,126 ms → spans 100 ms
            let enqueue = now - 6_000_000; // sojourn = 6 ms for every frame
            assert_eq!(c.queue(small(k), enqueue), 0);
            assert_eq!(c.flush(now), WriteStatus::Drained);
            drain_tags_until(&mut peer, &mut got, k as usize + 1);
        }
        // All 6-ms-sojourn frames were sent (sojourn < 2×target); none dropped yet.
        assert_eq!(c.codel_dropped(), 0);
        assert_eq!(got.len(), 21);
        assert!(c.is_overloaded(), "interval min 6 ms > target ⇒ overloaded");

        // ── Phase 2: now overloaded. Enqueue TWO stale frames (sojourn 12 ms >
        // 2×target 10 ms): tag 98 (older) then tag 99 (newest). On dequeue the
        // OLDER one is DROPPED (counter up, its bytes reclaimed) but the NEWEST is
        // KEPT and sent — CoDel never drops the last/freshest frame, so
        // freshest-wins still holds even when the whole backlog is stale.
        let now = 200_000_000; // 200 ms (within the next interval)
        let before_dropped = c.codel_dropped();
        let before_got = got.len();
        c.queue(small(98), now - 13_000_000); // older stale frame, sojourn 13 ms
        c.queue(small(99), now - 12_000_000); // newest stale frame, sojourn 12 ms
        assert_eq!(c.out_bytes(), 20);
        assert_eq!(c.flush(now), WriteStatus::Drained);
        drain_tags_until(&mut peer, &mut got, before_got + 1);
        assert_eq!(
            c.codel_dropped(),
            before_dropped + 1,
            "older stale frame dropped"
        );
        assert_eq!(
            c.out_bytes(),
            0,
            "queue fully drained (one dropped, one sent)"
        );
        assert_eq!(
            got.len(),
            before_got + 1,
            "the freshest frame still reached peer"
        );
        assert_eq!(
            *got.last().unwrap(),
            99,
            "freshest-wins: newest frame delivered"
        );

        // ── Phase 3: latency recovers. Drive a full interval with sojourn = 1 ms
        // (< target). The interval minimum is now 1 ms ≤ target, so crossing the
        // boundary clears `overloaded`; a subsequent stale frame is no longer
        // dropped — it flows. tag bytes 100..=130 (1 ms sojourn, sent).
        let base = 300_000_000u64; // 300 ms
        let phase3_got = got.len(); // tags received so far (Phase 1 + tag 99)
        for k in 0..=30u8 {
            let now = base + (k as u64) * 5_000_000; // 5 ms apart → spans 150 ms
            let enqueue = now - 1_000_000; // sojourn 1 ms
            c.queue(small(100 + k), enqueue);
            assert_eq!(c.flush(now), WriteStatus::Drained);
            drain_tags_until(&mut peer, &mut got, phase3_got + k as usize + 1);
        }
        assert!(!c.is_overloaded(), "interval min 1 ms ≤ target ⇒ recovered");
        let recovered_sent = got.len();
        assert_eq!(
            c.codel_dropped(),
            before_dropped + 1,
            "no new drops once fresh"
        );

        // A frame that WOULD have been dropped while overloaded (sojourn 12 ms) is
        // now sent, because the queue recovered.
        let now = 500_000_000;
        c.queue(small(200), now - 12_000_000);
        assert_eq!(c.flush(now), WriteStatus::Drained);
        drain_tags_until(&mut peer, &mut got, recovered_sent + 1);
        assert_eq!(c.codel_dropped(), before_dropped + 1, "recovered ⇒ no drop");
        assert_eq!(got.len(), recovered_sent + 1, "the once-stale frame flowed");
        assert_eq!(*got.last().unwrap(), 200);
    }

    /// `target_ns == 0` disables CoDel: even a wildly stale frame is sent (pure
    /// drop-head behaviour, the Phase-1/2 invariant).
    #[test]
    fn codel_disabled_sends_even_stale_frames() {
        let (server, mut peer) = pair();
        peer.set_nonblocking(true).unwrap();
        let mut c = Connection::new(server, 1 << 20); // CoDel disabled by default
        let mut got = Vec::new();

        // A frame 1 full second stale, flushed with CoDel off → still sent.
        c.queue(small(7), 0);
        assert_eq!(c.flush(1_000_000_000), WriteStatus::Drained);
        drain_tags_until(&mut peer, &mut got, 1);
        assert_eq!(c.codel_dropped(), 0);
        assert_eq!(got, vec![7]);
    }

    // ---- read_frames parses a masked client frame ----------------------------
    #[test]
    fn read_frames_parses_masked_hello() {
        let (server, mut client) = pair();
        let mut conn = Connection::new(server, 1 << 20);

        // RFC 6455 §5.7 masked "Hello".
        client
            .write_all(&[
                0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58,
            ])
            .unwrap();
        client.flush().unwrap();

        // Give the bytes a moment to land, then read until we get the frame.
        let mut scratch = BytesMut::new();
        let frames = read_until_frames(&mut conn, &mut scratch, 1);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].opcode, frame::OpCode::Text);
        assert_eq!(&frames[0].payload[..], b"Hello");
    }

    // ---- read_frames partial --------------------------------------------------
    #[test]
    fn read_frames_keeps_incomplete_remainder() {
        let (server, mut client) = pair();
        let mut conn = Connection::new(server, 1 << 20);
        let mut scratch = BytesMut::new();

        let full = [
            0x81u8, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58,
        ];
        // Send only the first 3 bytes.
        client.write_all(&full[..3]).unwrap();
        client.flush().unwrap();

        // Spin until those 3 bytes have landed in scratch, asserting no frame
        // is ever produced from the partial header.
        let mut tries = 0;
        loop {
            let frames = conn.read_frames(&mut scratch, 1 << 20).unwrap();
            assert!(frames.is_empty(), "no frame from a partial header");
            if scratch.len() == 3 {
                break;
            }
            tries += 1;
            assert!(tries < 1000, "partial bytes never arrived");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(scratch.len(), 3, "remainder kept for next read");

        // Send the rest; the next read completes the frame.
        client.write_all(&full[3..]).unwrap();
        client.flush().unwrap();
        let frames = read_until_frames(&mut conn, &mut scratch, 1);
        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].payload[..], b"Hello");
        assert!(scratch.is_empty(), "buffer fully consumed");
    }

    // ---- read EOF -------------------------------------------------------------
    #[test]
    fn read_frames_eof_with_empty_scratch_is_closed() {
        let (server, client) = pair();
        let mut conn = Connection::new(server, 1 << 20);
        let mut scratch = BytesMut::new();

        // Peer closes its end.
        drop(client);

        // Spin until the EOF is observed (the close may take a moment to
        // propagate; before it does, read() returns WouldBlock -> empty Ok).
        let mut tries = 0;
        loop {
            match conn.read_frames(&mut scratch, 1 << 20) {
                Err(ConnError::Closed) => break,
                Ok(frames) => {
                    assert!(frames.is_empty());
                    tries += 1;
                    assert!(tries < 1000, "EOF never observed");
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(other) => panic!("unexpected error: {other:?}"),
            }
        }
    }

    /// Repeatedly read (sleeping briefly between tries to let the loopback
    /// deliver) until at least `want` frames have been collected.
    fn read_until_frames(conn: &mut Connection, scratch: &mut BytesMut, want: usize) -> Vec<Frame> {
        let mut collected = Vec::new();
        for _ in 0..1000 {
            let frames = conn.read_frames(scratch, 1 << 20).expect("read_frames ok");
            collected.extend(frames);
            if collected.len() >= want {
                return collected;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("did not collect {want} frame(s); got {}", collected.len());
    }

    // ---- G2: TLS handshake flight blocked on a full send buffer ---------------

    /// The conn-level contract behind G2: when a TLS handshake flight write
    /// hits `WouldBlock` with `wants_write()` still true, `drain_head_bytes`
    /// must surface [`DrainStatus::NeedsWrite`] — the caller's cue to arm
    /// WRITABLE — and a later re-drive (after the peer drains its window)
    /// completes the flight ([`DrainStatus::Ok`], `!wants_write`). The
    /// worker-level twin (`transport::worker::tests::
    /// tls_handshake_completes_when_flight_write_blocks`) drives the same
    /// scenario through the real event handlers.
    #[test]
    fn drain_head_bytes_signals_needs_write_when_flight_blocks() {
        use crate::transport::conn::tls_test_support as tlsup;

        let (server_stream, mut client_sock) = tlsup::pair_tiny_tls();
        let (server_cfg, cert_der) = tlsup::bloated_server_config();
        let tls = rustls::server::ServerConnection::new(server_cfg).unwrap();
        let mut conn = Connection::new_tls(server_stream, Box::new(tls), 1 << 20);
        let mut client = tlsup::tls_client(&cert_der);

        // The client sends its ClientHello (small — only the server cert is
        // bloated).
        while client.wants_write() {
            client.write_tls(&mut client_sock).unwrap();
        }

        // (a) The first drain that sees the ClientHello generates the flight
        // and blocks mid-write: NeedsWrite with wants_write still true. (Loop
        // until the hello has landed; earlier drains just return Ok.)
        let mut buf = BytesMut::new();
        let mut attempts = 0;
        loop {
            attempts += 1;
            assert!(attempts < 1000, "ClientHello never arrived");
            match conn.drain_head_bytes(&mut buf) {
                DrainStatus::NeedsWrite => break,
                DrainStatus::Ok => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                DrainStatus::Closed => panic!("unexpected Closed mid-handshake"),
            }
        }
        assert!(
            conn.tls_wants_write(),
            "NeedsWrite must mean the flight is half-written"
        );

        // (b) The worker's writable-drive behaviour: pump the client (opening
        // its receive window) and re-drive the drain until the flight
        // completes, bounded so a real stall fails fast.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            assert!(
                std::time::Instant::now() < deadline,
                "flight never completed across writable re-drives"
            );
            tlsup::pump_client(&mut client, &mut client_sock);
            match conn.drain_head_bytes(&mut buf) {
                DrainStatus::Ok => break,
                DrainStatus::NeedsWrite => {}
                DrainStatus::Closed => panic!("unexpected Closed completing the flight"),
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(!conn.tls_wants_write(), "flight fully written");
        // The client now needs a final pump round (or two) to ingest the tail
        // of the flight and finish its side of the handshake.
        let client_deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while client.is_handshaking() {
            assert!(
                std::time::Instant::now() < client_deadline,
                "client never completed the handshake behind the completed flight"
            );
            tlsup::pump_client(&mut client, &mut client_sock);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}
