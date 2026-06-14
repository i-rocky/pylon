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

    /// Queue a pre-encoded frame for sending.
    ///
    /// If appending would push the total queued bytes past `high_water`, the
    /// frame is **not** appended and [`ConnError::Backpressure`] is returned —
    /// the caller is expected to close the connection rather than let a slow
    /// consumer balloon our memory. Otherwise the frame is appended and `Ok` is
    /// returned. The frame still needs a [`flush`](Self::flush) (or a writable
    /// event) to actually go out.
    pub fn queue(&mut self, frame: Arc<[u8]>) -> Result<(), ConnError> {
        let len = frame.len();
        // Reject *before* appending so `out_bytes` never exceeds high_water and
        // the rejected frame leaves no trace.
        if self.out_bytes + len > self.high_water {
            return Err(ConnError::Backpressure);
        }
        self.out_bytes += len;
        self.out.push_back(frame);
        Ok(())
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

        conn.queue(f1).unwrap();
        conn.queue(f2).unwrap();
        conn.queue(f3).unwrap();
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
        conn.queue(Arc::clone(&frame_bytes)).unwrap();

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

    // ---- backpressure ---------------------------------------------------------
    #[test]
    fn queue_past_high_water_rejects_without_growing() {
        let (server, _client) = pair();
        let mut conn = Connection::new(server, 100);

        // A ~50-byte frame fits; queue two (~100 bytes) to sit at the limit.
        let small = text_frame(&[0u8; 44]); // 2 header + 44 = 46 bytes
        let len = small.len();
        conn.queue(Arc::clone(&small)).unwrap();
        conn.queue(Arc::clone(&small)).unwrap();
        let bytes_before = conn.out_bytes;
        assert_eq!(bytes_before, 2 * len);

        // The third would push past 100 -> rejected, queue unchanged.
        let err = conn.queue(Arc::clone(&small)).unwrap_err();
        assert_eq!(err, ConnError::Backpressure);
        assert_eq!(conn.out_bytes, bytes_before, "out_bytes grew on rejection");
        assert_eq!(conn.out.len(), 2, "rejected frame was appended");
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
