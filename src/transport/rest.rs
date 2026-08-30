//! REST handoff for the per-core transport (SP9 §3.4).
//!
//! The `mio` worker owns the listener and accepts every connection. WebSocket
//! clients are driven on the worker thread; a plain HTTP request (a Pusher REST
//! publish, `POST /apps/{id}/events`) cannot be served there. Instead the worker
//! hands the raw connection — plus the request head bytes it already read — to the
//! tokio runtime, where the axum [`Router`] serves it.
//!
//! The pieces:
//!
//! * [`RestConn`] — the unit of handoff: a `std::net::TcpStream` (ownership of
//!   the accepted fd, moved out of mio) plus the `prefix` bytes already consumed
//!   from the socket during head detection (these MUST be replayed before any
//!   further reads, or the HTTP parser sees a truncated request). For TLS
//!   connections, the live rustls `ServerConnection` is also carried so the
//!   async REST plane can continue driving the encrypted session.
//! * [`mio_to_std`] — the single audited `unsafe` site: transfer fd ownership
//!   from a `mio::net::TcpStream` to a `std::net::TcpStream` with no
//!   double-close. The crate root is `#![deny(unsafe_code)]`; this function
//!   opts in locally.
//! * `Rewind` — an `AsyncRead`/`AsyncWrite` adapter that yields `prefix`
//!   first, then delegates to the live tokio stream (plain path).
//! * `TlsRestStream` — an `AsyncRead`/`AsyncWrite` adapter that drives the
//!   synchronous rustls `ServerConnection` over a tokio `TcpStream`. It replays
//!   `prefix` (the already-decrypted HTTP head bytes) first, then pulls further
//!   plaintext from the TLS session. Reads process EVERY TLS record found in a
//!   single socket read ([`drain_tls_records`], G4) and stash any plaintext
//!   that overflows the caller's buffer. Waker-driven: uses
//!   `poll_read_ready`/`poll_write_ready` + `try_read`/`try_write` and returns
//!   `Poll::Pending` (never busy-loops) when the TCP socket isn't ready.
//! * [`serve`] — the tokio task: loop on the handoff channel, wrap each
//!   `RestConn` in the appropriate adapter, and serve it with hyper-util's auto
//!   (HTTP/1+2) connection builder against the cloned `Router` (each connection
//!   on its own `tokio::spawn` so a slow REST client never blocks the handoff
//!   loop).

use axum::Router;
use rustls::server::ServerConnection as TlsConn;
use std::io::{self, Read, Write};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc::UnboundedReceiver;

/// A connection accepted by the `mio` worker but destined for the tokio/axum
/// REST plane. `fd_stream` owns the raw fd (already non-blocking, inherited from
/// mio); `prefix` is the request-head bytes the worker already read off the
/// socket and which must be replayed to the HTTP parser. `tls` carries the live
/// rustls `ServerConnection` for TLS connections (already handshaked; the worker
/// decrypted the prefix bytes from it). `None` for plain-TCP connections.
pub struct RestConn {
    pub fd_stream: std::net::TcpStream,
    pub prefix: Vec<u8>,
    pub tls: Option<Box<TlsConn>>,
}

/// Transfer ownership of the accepted fd from a `mio::net::TcpStream` to a
/// `std::net::TcpStream`.
///
/// This is the sole `unsafe` site in the crate (root is `#![deny(unsafe_code)]`).
/// The caller MUST have deregistered `mio_stream` from its `Poll` and dropped
/// its slab entry first, so mio's registry no longer references the fd.
#[allow(unsafe_code)]
pub fn mio_to_std(mio_stream: mio::net::TcpStream) -> std::net::TcpStream {
    use std::os::fd::{FromRawFd, IntoRawFd};
    // SAFETY: into_raw_fd transfers ownership of the fd out of the mio stream
    // (mio will NOT close it — it forgets the fd); from_raw_fd takes sole
    // ownership into the std stream (which WILL close it on drop). Exactly one
    // owner at all times — no double-close, no use-after-close.
    let raw = mio_stream.into_raw_fd();
    unsafe { std::net::TcpStream::from_raw_fd(raw) }
}

// ── Plain path ─────────────────────────────────────────────────────────────────

/// `AsyncRead`/`AsyncWrite` adapter that replays `prefix` bytes before
/// delegating to the underlying tokio stream.
///
/// `poll_read` drains `prefix` into the caller's buffer first; once `prefix` is
/// exhausted it delegates straight to `inner`. Writes/flush/shutdown delegate
/// unconditionally — the prefix is read-side only.
struct Rewind {
    prefix: Vec<u8>,
    /// Read cursor into `prefix`.
    pos: usize,
    inner: tokio::net::TcpStream,
}

impl Rewind {
    fn new(prefix: Vec<u8>, inner: tokio::net::TcpStream) -> Self {
        Self {
            prefix,
            pos: 0,
            inner,
        }
    }
}

impl AsyncRead for Rewind {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.pos < this.prefix.len() {
            let remaining = &this.prefix[this.pos..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            this.pos += n;
            // Drop the buffer once fully consumed so its memory is released.
            if this.pos >= this.prefix.len() {
                this.prefix = Vec::new();
                this.pos = 0;
            }
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Rewind {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

// ── TLS path ───────────────────────────────────────────────────────────────────

/// `AsyncRead`/`AsyncWrite` adapter that drives a synchronous rustls
/// `ServerConnection` over a tokio `TcpStream`.
///
/// The TLS handshake is already **complete** (the mio worker completed it).
/// The `prefix` field holds application-layer bytes the worker already decrypted
/// and which must be fed to hyper before any further TCP reads.
///
/// # Waker/Pending handling
///
/// `poll_read` and `poll_write` use `poll_read_ready`/`poll_write_ready` plus
/// `try_read`/`try_write` — they never busy-loop. When the TCP socket is not
/// ready, `poll_read_ready`/`poll_write_ready` registers the waker and returns
/// `Pending`. When the socket IS ready but a non-blocking read/write returns
/// `WouldBlock`, we re-register the waker by calling `poll_read_ready`/
/// `poll_write_ready` again so the task will be woken when the socket is ready.
///
/// # Ciphertext buffering (C1 fix)
///
/// `out_ct`/`out_pos` is a persistent outbound ciphertext buffer. Rustls
/// produces ciphertext via `write_tls` — once that call returns the bytes are
/// consumed from rustls's internal buffer and live ONLY in `out_ct`. If the TCP
/// send buffer is full we must not discard them; instead we keep them in
/// `out_ct` and resume writing on the next wakeup. `poll_flush_ct` owns the
/// full drain loop: pull from rustls → write to socket → repeat until both
/// `out_ct` is empty AND `!tls.wants_write()`.
///
/// # Inbound record draining (G4 fix)
///
/// One TCP read can carry several complete TLS records (pipelined requests, a
/// body split across records) plus a partial tail. Every record must be
/// processed — [`drain_tls_records`] owns that loop — and the plaintext it
/// decrypts may exceed the caller's `ReadBuf`, so it lands in `in_pt`/`in_pos`
/// (the read-side mirror of `out_ct`/`out_pos`) and is handed over across
/// successive `poll_read` calls.
struct TlsRestStream {
    tcp: tokio::net::TcpStream,
    tls: Box<TlsConn>,
    prefix: Vec<u8>,
    prefix_pos: usize,
    /// Ciphertext drained from rustls but not yet fully written to the socket.
    out_ct: Vec<u8>,
    /// Write cursor into `out_ct`; bytes `[..out_pos]` have been sent.
    out_pos: usize,
    /// Plaintext decrypted by [`drain_tls_records`] but not yet handed to the
    /// caller: one socket read can decode more than the caller's `ReadBuf`
    /// accepts (`ReadBuf::put_slice` would panic past `remaining()`).
    in_pt: Vec<u8>,
    /// Read cursor into `in_pt`; bytes `[..in_pos]` have been delivered.
    in_pos: usize,
}

impl TlsRestStream {
    /// Poll-style flush: write all buffered ciphertext to the socket, then pull
    /// more from rustls and repeat, until BOTH the socket buffer is empty
    /// (`out_pos == out_ct.len()`) AND `!tls.wants_write()`.
    ///
    /// Returns `Poll::Ready(Ok(()))` only when fully drained. Returns
    /// `Poll::Pending` (with waker registered) when the TCP send buffer is full.
    /// Returns `Poll::Ready(Err(_))` on any fatal I/O error.
    fn poll_flush_ct(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            // (a) Write any already-buffered ciphertext to the TCP socket.
            while self.out_pos < self.out_ct.len() {
                match self.tcp.poll_write_ready(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(())) => {}
                }
                match self.tcp.try_write(&self.out_ct[self.out_pos..]) {
                    Ok(0) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "tls rest: socket closed",
                        )));
                    }
                    Ok(n) => self.out_pos += n,
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        // try_write returned WouldBlock; poll_write_ready
                        // already registered the waker — loop to re-check.
                        continue;
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }
            // All buffered bytes written; reset the buffer.
            self.out_ct.clear();
            self.out_pos = 0;

            // (b) Pull more ciphertext from rustls.
            if !self.tls.wants_write() {
                // Fully drained: nothing in our buffer AND rustls has nothing.
                return Poll::Ready(Ok(()));
            }
            match self.tls.write_tls(&mut self.out_ct) {
                Ok(0) => {
                    // rustls produced nothing despite wants_write; treat as done.
                    return Poll::Ready(Ok(()));
                }
                Ok(_) => {
                    // More ciphertext appended to out_ct; loop to send it.
                    self.out_pos = 0;
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }

    /// Move buffered plaintext into `buf`, capped at `buf.remaining()`
    /// (`ReadBuf::put_slice` panics past that, and the excess must survive for
    /// the next `poll_read`). Returns the number of bytes delivered. When the
    /// buffer empties, its memory is released (like `prefix` above).
    fn take_in_pt(&mut self, buf: &mut ReadBuf<'_>) -> usize {
        if self.in_pos >= self.in_pt.len() {
            return 0;
        }
        let remaining = &self.in_pt[self.in_pos..];
        let n = remaining.len().min(buf.remaining());
        buf.put_slice(&remaining[..n]);
        self.in_pos += n;
        if self.in_pos >= self.in_pt.len() {
            self.in_pt = Vec::new();
            self.in_pos = 0;
        }
        n
    }
}

/// Process EVERY TLS record contained in a ciphertext buffer (G4).
///
/// A single TCP read (`poll_read` pulls up to 16 KiB) can carry several
/// complete TLS records — pipelined HTTP requests, or a request body split
/// across records arriving together — plus a trailing partial record. Rustls's
/// `read_tls` moves at most ~4 KiB per call from the source into its internal
/// deframer (a trailing partial record is kept safely buffered INSIDE rustls
/// until its remaining bytes arrive), and `process_new_packets` then parses
/// every complete record it buffered. The pre-fix handoff called `read_tls`
/// ONCE and dropped the cursor, silently discarding every ciphertext byte past
/// that first ~4 KiB — the second record of a burst lost its tail mid-stream
/// and the HTTP byte stream corrupted (hangs / parse errors).
///
/// This loops until the cursor is exhausted. Per iteration:
///
/// * `read_tls` — ingest the next ~4 KiB of ciphertext. `Ok(0)` here means
///   `close_notify` was already received (the loop guard rules out an
///   exhausted cursor); per RFC 8446 §6.1 data after `close_notify` is
///   ignored, so the drain stops without error.
/// * `process_new_packets` — decrypt every complete record just ingested.
/// * pull the round's plaintext out via `tls.reader()` into `plaintext`.
///   Draining per round also keeps rustls's 16 KiB `received_plaintext` buffer
///   from filling up and back-pressure-erroring the next `read_tls`.
///
/// Error mapping matches the single-record path this replaces: `read_tls`
/// I/O errors propagate as-is; `process_new_packets` failures become
/// `InvalidData` I/O errors; `reader()` errors other than `WouldBlock`
/// propagate as-is. A trailing partial record is NOT an error: its bytes are
/// already inside rustls's deframer and complete on the next read.
fn drain_tls_records(
    tls: &mut TlsConn,
    cursor: &mut std::io::Cursor<&[u8]>,
    plaintext: &mut Vec<u8>,
) -> io::Result<()> {
    let mut chunk = [0u8; 16 * 1024];
    while cursor.position() < cursor.get_ref().len() as u64 {
        match tls.read_tls(cursor) {
            // The loop guard rules out an exhausted cursor, so Ok(0) means
            // close_notify was already received: stop (RFC 8446 §6.1 — data
            // after close_notify is ignored), keeping what was decrypted.
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(e) => return Err(e),
        }
        if let Err(e) = tls.process_new_packets() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
        }
        // Pull this round's plaintext out. Draining per round also keeps
        // rustls's 16 KiB `received_plaintext` buffer from filling up and
        // back-pressure-erroring the next `read_tls`.
        loop {
            match tls.reader().read(&mut chunk) {
                // close_notify: no further plaintext can follow.
                Ok(0) => return Ok(()),
                Ok(n) => plaintext.extend_from_slice(&chunk[..n]),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}

impl AsyncRead for TlsRestStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        // 1. Drain the already-decrypted prefix first (the HTTP request head
        //    the mio worker peeked before deciding this is a REST connection).
        if this.prefix_pos < this.prefix.len() {
            let remaining = &this.prefix[this.prefix_pos..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            this.prefix_pos += n;
            if this.prefix_pos >= this.prefix.len() {
                this.prefix = Vec::new();
                this.prefix_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        // 2. Deliver plaintext stashed by a previous round: one socket read
        //    can decrypt more than a single caller ReadBuf accepts.
        if this.take_in_pt(buf) > 0 {
            return Poll::Ready(Ok(()));
        }

        let mut chunk = [0u8; 16 * 1024];

        // 3. Try to pull plaintext still buffered inside rustls (from a prior
        //    `read_tls` that decoded more than one TLS record). It goes through
        //    `in_pt` so delivery is bounded by the caller's buffer (a direct
        //    `put_slice` could panic on a small ReadBuf).
        match this.tls.reader().read(&mut chunk) {
            Ok(0) => {} // no buffered plaintext; fall through to read ciphertext
            Ok(n) => {
                this.in_pt.extend_from_slice(&chunk[..n]);
                this.take_in_pt(buf);
                return Poll::Ready(Ok(()));
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) => return Poll::Ready(Err(e)),
        }

        // 4. Need more ciphertext from the TCP socket. Wait until readable.
        match this.tcp.poll_read_ready(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }

        // 5. Read ciphertext into a temp buffer.
        let mut ct_buf = [0u8; 16 * 1024];
        let n = match this.tcp.try_read(&mut ct_buf) {
            Ok(0) => {
                // TCP EOF → clean close.
                return Poll::Ready(Ok(()));
            }
            Ok(n) => n,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // Spurious readiness: socket not actually ready yet. The prior
                // `poll_read_ready` call consumed the readiness event, so we
                // MUST re-register the waker before returning Pending — otherwise
                // the task will never be woken (I1 fix).
                match this.tcp.poll_read_ready(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(())) => {
                        // Socket became ready again immediately; self-wake so
                        // the runtime re-polls this future without delay.
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                }
            }
            Err(e) => return Poll::Ready(Err(e)),
        };

        // 6. Feed ALL of it to rustls (G4): one read can carry several
        //    complete records plus a partial tail; every record must be
        //    processed or the HTTP stream loses bytes.
        let mut cursor = std::io::Cursor::new(&ct_buf[..n]);
        if let Err(e) = drain_tls_records(&mut this.tls, &mut cursor, &mut this.in_pt) {
            return Poll::Ready(Err(e));
        }

        // 7. After processing, drive any pending TLS writes (e.g. alerts,
        //    key-update). Best-effort: a write-side error here doesn't affect
        //    the read result.
        let _ = this.poll_flush_ct(cx);

        // 8. Hand the freshly decrypted plaintext to the caller (bounded by
        //    the caller's buffer; any excess stays in `in_pt`).
        if this.take_in_pt(buf) > 0 {
            return Poll::Ready(Ok(()));
        }

        // 9. Nothing decrypted: either close_notify (clean EOF) or the records
        //    in this read carried no new plaintext (need more ciphertext).
        match this.tls.reader().read(&mut chunk) {
            Ok(0) => Poll::Ready(Ok(())), // TLS close_notify received
            Ok(n) => {
                // Defensive: plaintext that appeared between the drain and now
                // (e.g. a post-handshake message with app data piggybacked).
                // Deliver bounded, like every other path.
                this.in_pt.extend_from_slice(&chunk[..n]);
                this.take_in_pt(buf);
                Poll::Ready(Ok(()))
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // More ciphertext needed; re-register the waker and wait.
                // We consumed ciphertext this round so tokio will re-poll when
                // the socket has more data — but we need to re-register the
                // waker since poll_read_ready consumed the readiness event.
                // Re-call poll_read_ready to register the waker for the next
                // round. If the socket is already readable again, we'll get
                // Ready and can proceed; if not, we get Pending and wait.
                match this.tcp.poll_read_ready(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                    // Socket already readable again (more data arrived); signal
                    // the runtime to re-poll this future immediately by returning
                    // Pending after waking ourselves.
                    Poll::Ready(Ok(())) => {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl AsyncWrite for TlsRestStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        // Hand the plaintext to rustls (in-memory; builds TLS records).
        let n = match this.tls.writer().write(buf) {
            Ok(n) => n,
            Err(e) => return Poll::Ready(Err(e)),
        };

        // Best-effort: drain whatever rustls just produced. The ciphertext is
        // safely buffered in out_ct/rustls even if the socket is not writable
        // yet, so if poll_flush_ct returns Pending we still report the plaintext
        // bytes as accepted — the caller will drive flush to completion.
        // Pending or Ok(()) — either way, plaintext was accepted.
        if let Poll::Ready(Err(e)) = this.poll_flush_ct(cx) {
            return Poll::Ready(Err(e));
        }

        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        // Drain all pending TLS write records to the TCP socket. Returns Ready
        // only when out_ct is empty AND !tls.wants_write().
        match this.poll_flush_ct(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }

        // Flush the underlying TCP socket.
        Pin::new(&mut this.tcp).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        // Queue a TLS close_notify alert (idempotent).
        this.tls.send_close_notify();

        // Drain all pending TLS records (including the close_notify) to the
        // TCP socket. Returns Ready only when fully drained.
        match this.poll_flush_ct(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }

        Pin::new(&mut this.tcp).poll_shutdown(cx)
    }
}

// ── Serve ─────────────────────────────────────────────────────────────────────

/// Drive the REST handoff: pull each [`RestConn`] off `rx` and serve it with the
/// cloned axum [`Router`] on its own task. Returns when the channel closes (all
/// senders dropped — i.e. the worker thread is gone).
pub async fn serve(mut rx: UnboundedReceiver<RestConn>, router: Router) {
    while let Some(conn) = rx.recv().await {
        let router = router.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_one(conn, router).await {
                tracing::debug!(error = %e, "percore REST connection ended with error");
            }
        });
    }
}

/// Serve a single handed-off connection.
///
/// For plain-TCP connections: rebuild a tokio stream from the fd, replay the
/// prefix via [`Rewind`], and run hyper-util's auto HTTP/1+2 server against the
/// router.
///
/// For TLS connections: rebuild a tokio stream from the fd, wrap it together with
/// the live rustls session and the prefix in a [`TlsRestStream`], and serve THAT
/// with the same hyper-util auto server. The decrypted prefix bytes are replayed
/// first, then further reads pull plaintext through the TLS session.
async fn serve_one(
    conn: RestConn,
    router: Router,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let RestConn {
        fd_stream,
        prefix,
        tls,
    } = conn;
    // It already came from mio (non-blocking), but be explicit for tokio.
    fd_stream.set_nonblocking(true)?;
    let tokio_stream = tokio::net::TcpStream::from_std(fd_stream)?;

    let service = hyper_util::service::TowerToHyperService::new(router);
    let builder =
        hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());

    match tls {
        None => {
            // Plain path — unchanged from the original implementation.
            let rewind = Rewind::new(prefix, tokio_stream);
            let io = hyper_util::rt::TokioIo::new(rewind);
            builder.serve_connection(io, service).await?;
        }
        Some(tls_conn) => {
            // TLS path: drive the rustls session from the async plane.
            let tls_stream = TlsRestStream {
                tcp: tokio_stream,
                tls: tls_conn,
                prefix,
                prefix_pos: 0,
                out_ct: Vec::new(),
                out_pos: 0,
                in_pt: Vec::new(),
                in_pos: 0,
            };
            let io = hyper_util::rt::TokioIo::new(tls_stream);
            builder.serve_connection(io, service).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::ClientConnection;
    use std::sync::Arc;

    /// Build an in-memory rustls pair with the handshake already complete: the
    /// server end is exactly what `TlsRestStream` drives (a
    /// `ServerConnection`), and the paired raw client is what produces its
    /// ciphertext. Follows the conn.rs `tls_test_support` conventions (raw
    /// client/server pairs driven by hand) but stays fully in memory — no
    /// sockets, so record boundaries are exactly what the test writes.
    fn tls_pair() -> (TlsConn, ClientConnection) {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("rcgen: generate self-signed cert");
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.cert.der().clone()], key)
            .expect("build rustls server config");

        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).expect("trust test cert");
        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let name = rustls::pki_types::ServerName::try_from("localhost").expect("parse name");

        let mut server = TlsConn::new(Arc::new(server_config)).expect("server connection");
        let mut client =
            ClientConnection::new(Arc::new(client_config), name).expect("client connection");
        complete_handshake(&mut server, &mut client);
        (server, client)
    }

    /// Shuttle handshake flights between the pair through a scratch buffer
    /// until neither side is handshaking. Feeding the server reuses
    /// [`drain_tls_records`] (dogfooding: the same record-drain the fix owns);
    /// feeding the client loops `read_tls`/`process_new_packets` until the
    /// flight's bytes are exhausted — `read_tls` ingests at most ~4 KiB per
    /// call, the exact chunking G4 is about.
    fn complete_handshake(server: &mut TlsConn, client: &mut ClientConnection) {
        let mut wire = Vec::new();
        for _ in 0..20 {
            if !server.is_handshaking() && !client.is_handshaking() {
                return;
            }
            if client.wants_write() {
                client.write_tls(&mut wire).expect("client flight");
                let mut scratch = Vec::new();
                drain_tls_records(server, &mut io::Cursor::new(&wire), &mut scratch)
                    .expect("server ingests client flight");
                assert!(scratch.is_empty(), "no plaintext during handshake");
                wire.clear();
            }
            if server.wants_write() {
                server.write_tls(&mut wire).expect("server flight");
                let mut cursor = io::Cursor::new(&wire);
                while cursor.position() < wire.len() as u64 {
                    client
                        .read_tls(&mut cursor)
                        .expect("client read_tls of flight");
                    client
                        .process_new_packets()
                        .expect("client TLS state machine");
                }
                wire.clear();
            }
        }
        panic!("in-memory TLS handshake did not complete in 20 rounds");
    }

    /// Have the client emit one application-data record per `write_tls` call:
    /// `writer().write_all` buffers the plaintext, `write_tls` drains it into
    /// exactly one record on the wire (payloads stay under the 16 KiB max
    /// fragment). Consecutive calls append, so one buffer ends up holding
    /// several complete records back-to-back — like one big TCP read.
    fn emit_record(client: &mut ClientConnection, wire: &mut Vec<u8>, payload: &[u8]) {
        client
            .writer()
            .write_all(payload)
            .expect("client buffers plaintext");
        client.write_tls(wire).expect("client emits record");
    }

    /// G4: a single buffer holding TWO complete records — both plaintexts must
    /// surface, in order, with nothing lost. The payloads are 4 KiB each so the
    /// combined wire size exceeds the ~4 KiB chunk `read_tls` ingests per call:
    /// this is exactly the burst the single-record handoff truncated (it
    /// dropped every ciphertext byte past the first chunk, cutting the second
    /// record's tail out of the HTTP byte stream).
    #[test]
    fn drain_tls_records_yields_every_record_in_a_multi_record_buffer() {
        let (mut server, mut client) = tls_pair();

        let p1: Vec<u8> = (0..4000u32).map(|i| (i % 251) as u8).collect();
        let p2: Vec<u8> = (0..4000u32).map(|i| 0x80 | (i % 128) as u8).collect();

        let mut wire = Vec::new();
        emit_record(&mut client, &mut wire, &p1);
        emit_record(&mut client, &mut wire, &p2);
        assert!(
            wire.len() > 4096,
            "precondition: two records must exceed one read_tls chunk, wire={}B",
            wire.len()
        );

        let mut plaintext = Vec::new();
        drain_tls_records(&mut server, &mut io::Cursor::new(&wire), &mut plaintext)
            .expect("drain must not fail");

        let mut expected = p1.clone();
        expected.extend_from_slice(&p2);
        assert_eq!(
            plaintext,
            expected,
            "both records' plaintext must surface in order (got {} of {} bytes)",
            plaintext.len(),
            expected.len()
        );
    }

    /// Two small pipelined requests in one buffer (well under one ~4 KiB
    /// chunk) — the common pipelining case: `process_new_packets` parses both
    /// records in one go and the drain must keep both plaintexts surfacing.
    #[test]
    fn drain_tls_records_yields_both_small_pipelined_records() {
        let (mut server, mut client) = tls_pair();

        let p1 = b"GET /apps/tls-app/channels HTTP/1.1\r\nHost: pylon\r\n\r\n";
        let p2 = b"GET /apps/tls-app/channels?filter=prefix HTTP/1.1\r\nHost: pylon\r\n\r\n";

        let mut wire = Vec::new();
        emit_record(&mut client, &mut wire, p1);
        emit_record(&mut client, &mut wire, p2);
        assert!(wire.len() <= 4096, "precondition: small pipelined pair");

        let mut plaintext = Vec::new();
        drain_tls_records(&mut server, &mut io::Cursor::new(&wire), &mut plaintext)
            .expect("drain must not fail");

        assert!(plaintext.starts_with(p1), "first request first");
        assert!(plaintext.ends_with(p2), "second request second");
        assert_eq!(plaintext.len(), p1.len() + p2.len(), "nothing lost");
    }

    /// A complete record followed by a PARTIAL second record: the first must
    /// surface now; the partial bytes live inside rustls's deframer and must
    /// complete — not be lost or double-fed — when the remainder arrives
    /// (models a request split across TCP segments). The two feeds go through
    /// SEPARATE cursors exactly like two `try_read` calls in `poll_read`.
    #[test]
    fn drain_tls_records_partial_tail_completes_on_next_feed() {
        let (mut server, mut client) = tls_pair();

        let p1 = b"first complete record";
        let p2: Vec<u8> = (0..2000u32).map(|i| (i % 199) as u8).collect();

        let mut wire = Vec::new();
        emit_record(&mut client, &mut wire, p1);
        let rec1_len = wire.len();
        emit_record(&mut client, &mut wire, &p2);
        // Split the second record's wire bytes 30 bytes in (a partial record),
        // keeping the large majority for the second feed.
        let split = rec1_len + 30;

        let mut plaintext = Vec::new();
        drain_tls_records(
            &mut server,
            &mut io::Cursor::new(&wire[..split]),
            &mut plaintext,
        )
        .expect("first drain");
        assert_eq!(plaintext, p1, "only the complete record surfaces");

        let mut rest = Vec::new();
        drain_tls_records(&mut server, &mut io::Cursor::new(&wire[split..]), &mut rest)
            .expect("second drain");
        assert_eq!(rest, p2, "the partial record completes from its remainder");
    }

    /// An empty ciphertext buffer is a no-op: Ok(()), nothing appended, no
    /// state consumed.
    #[test]
    fn drain_tls_records_empty_buffer_is_noop() {
        let (mut server, _client) = tls_pair();
        let mut plaintext = vec![1u8, 2, 3]; // pre-filled: must stay untouched
        drain_tls_records(&mut server, &mut io::Cursor::new(&[]), &mut plaintext)
            .expect("empty drain");
        assert_eq!(plaintext, vec![1, 2, 3]);
    }
}
