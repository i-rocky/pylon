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
//! `#![forbid(unsafe_code)]`). None of them ever loops on `WouldBlock`; the
//! worker re-arms epoll interest and calls back.

use crate::transport::frame::{self, Frame, ParseError};
use bytes::BytesMut;
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

/// A single non-blocking WebSocket connection.
pub struct Connection {
    /// The non-blocking client socket. `pub` so the worker can register it with
    /// its [`mio::Poll`] and re-arm interest.
    pub stream: mio::net::TcpStream,
    /// Current lifecycle state.
    pub state: ConnState,
    /// Pending outbound frames (pre-encoded bytes, shared via `Arc` for
    /// encode-once fan-out). The front element is the one currently being
    /// written, possibly partially (see `out_cursor`).
    out: VecDeque<Arc<[u8]>>,
    /// Byte offset into `out.front()` already written (partial-write resume
    /// point).
    out_cursor: usize,
    /// Total bytes still queued across all of `out` (drives the high-water
    /// backpressure check without walking the deque).
    out_bytes: usize,
    /// Backpressure threshold: if queuing a frame would push `out_bytes` over
    /// this, the frame is rejected and the caller closes.
    high_water: usize,
}

impl Connection {
    /// Wrap a freshly-accepted non-blocking socket. Starts in
    /// [`ConnState::Handshaking`] with empty queues.
    pub fn new(stream: mio::net::TcpStream, high_water: usize) -> Self {
        Connection {
            stream,
            state: ConnState::Handshaking,
            out: VecDeque::new(),
            out_cursor: 0,
            out_bytes: 0,
            high_water,
        }
    }

    /// Consume the connection and return ownership of its underlying socket.
    ///
    /// Used by the per-core worker's REST handoff (SP9 §3.4): a plain-HTTP
    /// connection is removed from the slab and its `mio` stream moved out, to be
    /// converted to a `std::net::TcpStream` and handed to the tokio/axum plane.
    /// Any queued outbound bytes are discarded (a REST connection has none — the
    /// head was only ever read).
    pub fn into_stream(self) -> mio::net::TcpStream {
        self.stream
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
    pub fn queue(&mut self, frame: Arc<[u8]>) -> usize {
        let flen = frame.len();
        let mut dropped = 0;
        // The frame currently mid-write (front when out_cursor>0) is "locked"; the
        // oldest droppable index is 1 in that case, else 0.
        let locked = if self.out_cursor > 0 { 1 } else { 0 };
        while self.out_bytes + flen > self.high_water && self.out.len() > locked {
            // Remove the oldest droppable frame.
            let victim = self.out.remove(locked).expect("len checked");
            self.out_bytes -= victim.len();
            dropped += 1;
        }
        self.out_bytes += flen;
        self.out.push_back(frame);
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
    pub fn flush(&mut self) -> WriteStatus {
        while let Some(front) = self.out.front() {
            let buf = &front[self.out_cursor..];
            match self.stream.write(buf) {
                Ok(0) => {
                    // A zero-length write on a non-empty buffer means the peer
                    // can no longer accept data.
                    return WriteStatus::Closed;
                }
                Ok(n) => {
                    self.out_cursor += n;
                    if self.out_cursor == front.len() {
                        // Frame fully written: drop it, reset the cursor, and
                        // continue coalescing into the next frame.
                        self.out_bytes -= front.len();
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
        loop {
            match self.stream.read(&mut chunk) {
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

    // ---- test accessors -------------------------------------------------------
    // Read-only views of the private out-queue state, used by the drop-head unit
    // tests. `#[cfg(test)]` so they add no surface (or dead-code warnings) to the
    // library build.

    /// Total bytes currently queued across all of `out`.
    #[cfg(test)]
    pub fn out_bytes(&self) -> usize {
        self.out_bytes
    }

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
        self.out.front().map(|f| f[0]).unwrap()
    }

    /// First byte of the back (newest) queued frame.
    #[cfg(test)]
    pub fn peek_back_byte(&self) -> u8 {
        self.out.back().map(|f| f[0]).unwrap()
    }

    /// Whether the front frame is the 4 MB "huge" frame the drop-head test
    /// enqueues first (identified by its length), i.e. index 0 is untouched.
    #[cfg(test)]
    pub fn front_is_the_huge_frame(&self) -> bool {
        self.out.front().map(|f| f.len() == 4_000_000).unwrap_or(false)
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

        assert_eq!(conn.queue(f1), 0);
        assert_eq!(conn.queue(f2), 0);
        assert_eq!(conn.queue(f3), 0);
        assert!(conn.has_pending_writes());

        assert_eq!(conn.flush(), WriteStatus::Drained);
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
        assert_eq!(conn.queue(Arc::clone(&frame_bytes)), 0);

        // First flush: the kernel send buffer fills and we stop partway.
        assert_eq!(conn.flush(), WriteStatus::WouldBlock);
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
            match conn.flush() {
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
        assert_eq!(c.queue(f(1)), 0); // returns dropped count = 0
        assert_eq!(c.queue(f(2)), 0); // out_bytes = 80
        let dropped = c.queue(f(3)); // 120 > 100 → drop oldest (f(1)) → out = [f2,f3], 80 bytes
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
        c.queue(std::sync::Arc::from(
            vec![1u8; 4_000_000].into_boxed_slice(),
        )); // huge → partial write
        let _ = c.flush(); // out_cursor now > 0 on front
        assert!(c.out_cursor() > 0);
        // queue more small frames past the cap; the mid-write front MUST survive (peer would corrupt otherwise)
        for n in 0..50u8 {
            let _ = c.queue(std::sync::Arc::from(vec![n; 40].into_boxed_slice()));
        }
        assert!(c.out_cursor() > 0, "front still mid-write");
        assert!(c.front_is_the_huge_frame()); // i.e. index 0 is untouched
        // Keep the peer alive until here so the socket doesn't close mid-test and
        // turn the partial write into a Closed status.
        let _ = peer.peer_addr();
        drop(peer);
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
