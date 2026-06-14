//! REST handoff for the per-core transport (SP9 §3.4).
//!
//! In `PYLON_TRANSPORT=percore` the `mio` worker owns the listener and accepts
//! every connection. WebSocket clients are driven on the worker thread; a plain
//! HTTP request (a Pusher REST publish, `POST /apps/{id}/events`) cannot be
//! served there. Instead the worker hands the raw connection — plus the request
//! head bytes it already read — to the tokio runtime, where the *same* axum
//! [`Router`] the legacy transport uses serves it.
//!
//! The pieces:
//!
//! * [`RestConn`] — the unit of handoff: a `std::net::TcpStream` (ownership of
//!   the accepted fd, moved out of mio) plus the `prefix` bytes already consumed
//!   from the socket during head detection (these MUST be replayed before any
//!   further reads, or the HTTP parser sees a truncated request).
//! * [`mio_to_std`] — the single audited `unsafe` site: transfer fd ownership
//!   from a `mio::net::TcpStream` to a `std::net::TcpStream` with no
//!   double-close. The crate root is `#![deny(unsafe_code)]`; this function
//!   opts in locally.
//! * [`Rewind`] — an `AsyncRead`/`AsyncWrite` adapter that yields `prefix`
//!   first, then delegates to the live tokio stream.
//! * [`serve`] — the tokio task: loop on the handoff channel, wrap each
//!   `RestConn` in a `Rewind`, and serve it with hyper-util's auto (HTTP/1+2)
//!   connection builder against the cloned `Router` (each connection on its own
//!   `tokio::spawn` so a slow REST client never blocks the handoff loop).

use axum::Router;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc::UnboundedReceiver;

/// A connection accepted by the `mio` worker but destined for the tokio/axum
/// REST plane. `fd_stream` owns the raw fd (already non-blocking, inherited from
/// mio); `prefix` is the request-head bytes the worker already read off the
/// socket and which must be replayed to the HTTP parser.
pub struct RestConn {
    pub fd_stream: std::net::TcpStream,
    pub prefix: Vec<u8>,
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
    ) -> Poll<std::io::Result<()>> {
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
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

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

/// Serve a single handed-off connection: rebuild a tokio stream from the fd,
/// replay the prefix via [`Rewind`], and run hyper-util's auto HTTP/1+2 server
/// against the router (converted to a hyper service via `TowerToHyperService` —
/// axum's `Router<()>` is a `Clone` tower `Service<Request<B>>`).
async fn serve_one(conn: RestConn, router: Router) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let RestConn { fd_stream, prefix } = conn;
    // It already came from mio (non-blocking), but be explicit for tokio.
    fd_stream.set_nonblocking(true)?;
    let tokio_stream = tokio::net::TcpStream::from_std(fd_stream)?;

    let rewind = Rewind::new(prefix, tokio_stream);
    let io = hyper_util::rt::TokioIo::new(rewind);
    let service = hyper_util::service::TowerToHyperService::new(router);

    hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
        .serve_connection(io, service)
        .await?;
    Ok(())
}
