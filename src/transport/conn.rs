//! Per-connection state and non-blocking I/O for the SP9 per-core transport.
//!
//! A [`Connection`] wraps a single non-blocking [`mio::net::TcpStream`] and owns
//! the two halves of a Pusher WebSocket session:
//!
//! * **Outbound.** A queue of pre-encoded frames ([`Arc<[u8]>`], so a broadcast
//!   payload is encoded once and fanned out as cheap `Arc` clones). [`flush`]
//!   drains the queue with *corked* writes — it coalesces as many queued frames
//!   as the socket will accept in a single call, advancing a cursor across
//!   partial writes, and reports backpressure via [`WriteStatus::WouldBlock`].
//!   [`queue`] enforces a high-water mark so a slow consumer cannot make us
//!   buffer unbounded memory.
//!
//! * **Inbound.** [`read_frames`] reads whatever the socket has available into a
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
use std::io::{ErrorKind, Read, Write};
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
    out: VecDeque<(Arc<[u8]>, u64)>,
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
    /// Returns `true` when more data may arrive (ok/would-block),
    /// `false` when the connection is closed (EOF or error).
    pub fn drain_head_bytes(&mut self, buf: &mut BytesMut) -> bool {
        let mut chunk = [0u8; 16 * 1024];
        match &mut self.io {
            Io::Plain(stream) => loop {
                match stream.read(&mut chunk) {
                    Ok(0) => return false,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => return true,
                    Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                    Err(_) => return false,
                }
            },
            Io::Tls(stream, tls) => {
                // Read ciphertext from socket into rustls state machine.
                loop {
                    match tls.read_tls(stream) {
                        Ok(0) => return false,
                        Ok(_) => {
                            if tls.process_new_packets().is_err() {
                                return false;
                            }
                        }
                        Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                        Err(_) => return false,
                    }
                }
                // Drive pending TLS writes (handshake responses).
                while tls.wants_write() {
                    match tls.write_tls(stream) {
                        Ok(_) => {}
                        Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(_) => return false,
                    }
                }
                // Pull available plaintext (empty during the handshake phase).
                loop {
                    match tls.reader().read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                        Err(_) => return false,
                    }
                }
                true
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
    /// Frames are written back-to-back in a single call (corking) until the
    /// socket returns `WouldBlock` or the queue empties. Partial writes are
    /// handled by advancing `out_cursor`; a fully-written frame is popped and
    /// the cursor reset. Returns:
    ///
    /// * [`WriteStatus::Drained`] — queue empty, clear writable interest.
    /// * [`WriteStatus::WouldBlock`] — send buffer full, data remains; re-arm
    ///   writable interest.
    /// * [`WriteStatus::Closed`] — write error or a zero-length write (peer
    ///   gone); close.
    ///
    /// `now_ns` is the monotonic dequeue time (ns since the worker's epoch). With
    /// CoDel enabled (`target_ns != 0`), each frame's sojourn (`now_ns -
    /// enqueue_ns`) is checked before it is written: see [`codel_dequeue`].
    pub fn flush(&mut self, now_ns: u64) -> WriteStatus {
        match &self.io {
            Io::Plain(_) => self.flush_plain(now_ns),
            Io::Tls(_, _) => self.flush_tls(now_ns),
        }
    }

    /// Plain-TCP flush: drain the out-queue directly to the socket.
    ///
    /// Behaviour-identical and allocation-/refcount-free vs the pre-TLS path: the
    /// front frame's bytes are borrowed from `self.out` and the socket from
    /// `self.io` SIMULTANEOUSLY — they are disjoint struct fields, which the borrow
    /// checker permits when each is accessed directly (not through a `&mut self`
    /// method). So there is NO per-frame `Arc::clone`; the hot path adds zero
    /// overhead over the original single-field `self.stream.write(&front…)`.
    fn flush_plain(&mut self, now_ns: u64) -> WriteStatus {
        loop {
            // CoDel: before sending the front frame, drop any leading frame that
            // is too stale to be worth sending (skips the mid-write front). When
            // this returns, the front is either fresh-enough to send or the queue
            // is empty. Takes `&mut self` but completes before the disjoint
            // `self.out` / `self.io` split below, so no borrow conflict.
            self.codel_dequeue(now_ns);
            // Borrow the front frame from `self.out` and the socket from `self.io`
            // at once: distinct fields, so this is a zero-cost disjoint split.
            let Some((front, _ts)) = self.out.front() else {
                break;
            };
            let front_len = front.len();
            let buf = &front[self.out_cursor..];
            let Io::Plain(stream) = &mut self.io else {
                unreachable!("flush_plain only called for Io::Plain")
            };
            match stream.write(buf) {
                Ok(0) => {
                    // A zero-length write on a non-empty buffer means the peer
                    // can no longer accept data.
                    return WriteStatus::Closed;
                }
                Ok(n) => {
                    self.out_cursor += n;
                    if self.out_cursor == front_len {
                        // Frame fully written: drop it, reset the cursor, and
                        // continue coalescing into the next frame.
                        let sent = front_len;
                        self.out_bytes -= sent;
                        // Sent bytes leave the queue: fold the negative delta in so
                        // the worker's incremental inflight total drops by exactly
                        // the bytes that went out (matching the `out_bytes` change).
                        self.inflight_delta -= sent as i64;
                        self.out.pop_front();
                        self.out_cursor = 0;
                    }
                    // else: partial write; loop and try to write the remainder.
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    return WriteStatus::WouldBlock;
                }
                Err(ref e) if e.kind() == ErrorKind::Interrupted => {
                    // Retry the same write; nothing was consumed.
                    continue;
                }
                Err(_) => return WriteStatus::Closed,
            }
        }
        WriteStatus::Drained
    }

    /// TLS flush: encrypt app-data through rustls and drain ciphertext to the socket.
    fn flush_tls(&mut self, now_ns: u64) -> WriteStatus {
        // Phase 1: drain any pending TLS ciphertext that rustls has already
        // buffered (e.g. handshake records). Do this before touching the app queue.
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

        // Phase 2: encrypt and send each queued app-data frame. Like `flush_plain`,
        // the front frame (`self.out`) and the TLS connection (`self.io`) are
        // borrowed from disjoint fields at once — no per-frame `Arc::clone`.
        loop {
            self.codel_dequeue(now_ns);
            let Some((front, _ts)) = self.out.front() else {
                break;
            };
            let front_len = front.len();
            let buf = &front[self.out_cursor..];
            let Io::Tls(stream, tls) = &mut self.io else {
                unreachable!("flush_tls only called for Io::Tls")
            };
            let n = match tls.writer().write(buf) {
                Ok(0) => return WriteStatus::Closed,
                Ok(n) => {
                    // Flush the freshly encrypted ciphertext to the socket.
                    while tls.wants_write() {
                        match tls.write_tls(stream) {
                            Ok(_) => {}
                            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                                return WriteStatus::WouldBlock;
                            }
                            Err(_) => return WriteStatus::Closed,
                        }
                    }
                    n
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    return WriteStatus::WouldBlock;
                }
                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(_) => return WriteStatus::Closed,
            };
            self.out_cursor += n;
            if self.out_cursor == front_len {
                let sent = front_len;
                self.out_bytes -= sent;
                self.inflight_delta -= sent as i64;
                self.out.pop_front();
                self.out_cursor = 0;
            }
        }

        // Final pass: flush any remaining TLS ciphertext rustls buffered during
        // the app-data writes (e.g. finishing a record after the last write).
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

    /// CoDel freshness check, run on **dequeue** before writing the front frame
    /// (folly's controlled-delay algorithm).
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
    /// Never drops the mid-write front (`out_cursor > 0`): those bytes are already
    /// partly on the wire, and splicing them out would corrupt the peer's stream.
    /// A `target_ns` of `0` disables the overlay entirely (pure drop-head).
    fn codel_dequeue(&mut self, now_ns: u64) {
        if !self.codel.enabled() {
            return;
        }
        let two_target = self.codel.target_ns.saturating_mul(2);
        loop {
            let Some(&(_, enqueue_ns)) = self.out.front() else {
                // Empty queue: no item is standing in line. Do NOT fold a sample
                // (folly's algorithm samples real dequeues only), but let the
                // overloaded flag age out if the interval has since closed with no
                // sample — a backlog that fully drained is, by definition, fresh.
                self.codel_age_empty(now_ns);
                return;
            };
            let sojourn = now_ns.saturating_sub(enqueue_ns);
            // Fold this real dequeue's sojourn into the interval minimum and (when
            // the window closes) update the overloaded flag.
            self.codel_note_interval(now_ns, sojourn);

            // The mid-write front is locked: it is already partly on the wire and
            // must be sent to completion, stale or not.
            if self.out_cursor > 0 {
                return;
            }
            // FRESHEST-WINS invariant: never CoDel-drop the LAST remaining frame.
            // When a slow consumer's whole backlog is stale, CoDel skips straight
            // past the old frames to the NEWEST one — maximally fresh — but the
            // newest itself is always kept and sent. So even a fully-stale queue
            // still delivers its freshest frame, exactly like drop-head's
            // freshest-wins (drop-head evicts the oldest; CoDel here drops stale
            // leading frames, but both always preserve the newest).
            if self.codel_state.overloaded && sojourn > two_target && self.out.len() > 1 {
                // Stale frame (and not the last one) in the overloaded regime:
                // drop it and look at the next one (which may also be stale).
                let (victim, _ts) = self.out.pop_front().expect("front checked");
                self.out_bytes -= victim.len();
                // CoDel staleness drop: this queued byte is discarded unsent, so
                // fold the negative delta in for the worker's incremental total.
                self.inflight_delta -= victim.len() as i64;
                self.codel_dropped += 1;
                continue;
            }
            // Fresh enough, not overloaded, or the last remaining (freshest) frame:
            // keep it; flush writes it.
            return;
        }
    }

    /// Fold one real-dequeue sojourn sample into the current CoDel interval,
    /// advancing the overloaded flag when the interval window closes. `sojourn`
    /// is the candidate frame's time-in-queue.
    fn codel_note_interval(&mut self, now_ns: u64, sojourn: u64) {
        let interval = self.codel.interval_ns;
        let target = self.codel.target_ns;
        let st = &mut self.codel_state;
        if st.interval_end == 0 {
            // First sample ever: open the first interval window.
            st.interval_end = now_ns.saturating_add(interval);
        }
        // Track the minimum sojourn seen this interval.
        st.interval_min = Some(match st.interval_min {
            Some(m) => m.min(sojourn),
            None => sojourn,
        });
        // Window closed: decide overloaded from the interval minimum, then reset
        // for the next window. Carry this very sample into the fresh interval so a
        // window that closes never starts the next one empty.
        if now_ns >= st.interval_end {
            let min = st.interval_min.unwrap_or(sojourn);
            st.overloaded = min > target;
            st.interval_min = Some(sojourn);
            st.interval_end = now_ns.saturating_add(interval);
        }
    }

    /// Age the overloaded flag when the queue is empty. A queue that has fully
    /// drained holds no stale frames, so once the current interval window has
    /// elapsed with the queue empty, the overloaded regime is cleared. Does not
    /// fold a (spuriously low) sojourn sample into a window that still has queued
    /// frames being tracked.
    fn codel_age_empty(&mut self, now_ns: u64) {
        let st = &mut self.codel_state;
        if st.interval_end != 0 && now_ns >= st.interval_end {
            // The window elapsed and the queue is empty: nothing was backed up, so
            // clear overload and re-arm the window.
            st.overloaded = false;
            st.interval_min = None;
            st.interval_end = now_ns.saturating_add(self.codel.interval_ns);
        }
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
            Io::Plain(stream) => {
                loop {
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
                }
            }
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
                Err(ParseError::TooLarge) => {
                    return Err(ConnError::Protocol("frame too large"))
                }
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
            assert_eq!(*running, c.out_bytes() as i64, "delta sum must track out_bytes");
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
        assert_eq!(c.take_inflight_delta(), 0, "drop-head: +40 added − 40 evicted = 0 net");
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
        assert_eq!(c.codel_dropped(), dropped_before + 1, "older stale frame dropped");
        running += c.take_inflight_delta(); // −10 (CoDel drop) and −10 (sent)
        assert_eq!(running, c.out_bytes() as i64, "delta tracks CoDel drop + send");
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

    /// Drain every byte currently readable on the non-blocking peer, recording the
    /// first byte (tag) of each 10-byte frame received.
    fn drain_tags(peer: &mut StdTcpStream, into: &mut Vec<u8>) {
        let mut chunk = [0u8; 4096];
        loop {
            match peer.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    // Frames are a fixed 10 bytes; record each frame's tag byte.
                    let mut i = 0;
                    while i < n {
                        into.push(chunk[i]);
                        i += 10;
                    }
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => panic!("peer read failed: {e}"),
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
            drain_tags(&mut peer, &mut got);
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
            drain_tags(&mut peer, &mut got);
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
        drain_tags(&mut peer, &mut got);
        assert_eq!(c.codel_dropped(), before_dropped + 1, "older stale frame dropped");
        assert_eq!(c.out_bytes(), 0, "queue fully drained (one dropped, one sent)");
        assert_eq!(got.len(), before_got + 1, "the freshest frame still reached peer");
        assert_eq!(*got.last().unwrap(), 99, "freshest-wins: newest frame delivered");

        // ── Phase 3: latency recovers. Drive a full interval with sojourn = 1 ms
        // (< target). The interval minimum is now 1 ms ≤ target, so crossing the
        // boundary clears `overloaded`; a subsequent stale frame is no longer
        // dropped — it flows. tag bytes 100..=130 (1 ms sojourn, sent).
        let base = 300_000_000u64; // 300 ms
        for k in 0..=30u8 {
            let now = base + (k as u64) * 5_000_000; // 5 ms apart → spans 150 ms
            let enqueue = now - 1_000_000; // sojourn 1 ms
            c.queue(small(100 + k), enqueue);
            assert_eq!(c.flush(now), WriteStatus::Drained);
            drain_tags(&mut peer, &mut got);
        }
        assert!(!c.is_overloaded(), "interval min 1 ms ≤ target ⇒ recovered");
        let recovered_sent = got.len();
        assert_eq!(c.codel_dropped(), before_dropped + 1, "no new drops once fresh");

        // A frame that WOULD have been dropped while overloaded (sojourn 12 ms) is
        // now sent, because the queue recovered.
        let now = 500_000_000;
        c.queue(small(200), now - 12_000_000);
        assert_eq!(c.flush(now), WriteStatus::Drained);
        drain_tags(&mut peer, &mut got);
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
        drain_tags(&mut peer, &mut got);
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
    fn read_until_frames(
        conn: &mut Connection,
        scratch: &mut BytesMut,
        want: usize,
    ) -> Vec<Frame> {
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
}
