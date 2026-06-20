use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

/// The per-worker notifier a [`Mailbox`] uses to wake its owning worker on a
/// cross-connection send. Built in `handle_handshake` (in `crate::transport::worker`)
/// from the worker's dirty-token channel + the `MAILBOX_WAKER`.
///
/// `token` is this connection's slab key (== its `mio::Token` value); `dirty` is
/// the worker's dirty-token queue; `waker` unblocks the worker's `mio::Poll`.
#[derive(Clone)]
pub struct MailboxNotify {
    /// This connection's slab key — pushed onto `dirty` so the worker drains
    /// exactly this connection's mailbox (and never scans idle ones).
    pub token: usize,
    /// The worker's dirty-token queue. A `send` here is a cheap unbounded push;
    /// the worker dedups the tokens into a `HashSet` before draining.
    pub dirty: std::sync::mpsc::Sender<usize>,
    /// The worker's `MAILBOX_WAKER`. Woken so an idle (blocked-on-poll) worker
    /// re-polls promptly and drains the just-marked connection.
    pub waker: Arc<mio::Waker>,
}

/// The choke point for every cross-connection delivery to a connection.
///
/// Wraps the connection's mailbox [`Sender`] (bounded) plus an optional
/// [`MailboxNotify`] and an optional per-worker drop counter. [`Mailbox::send`]
/// pushes the event onto the mailbox via `try_send` (non-blocking) and, if a
/// notifier is present, marks this connection dirty (pushes its slab token onto
/// the worker's dirty queue) and wakes the worker. When the mailbox is full the
/// frame is silently dropped and the per-worker `mailbox_dropped` counter is
/// incremented. Because EVERY cross-connection delivery routes through
/// [`ConnectionHandle::mailbox`], this single choke point guarantees the worker is
/// always nudged to drain a connection that received a message — so idle
/// connections are never scanned (O(dirty), not O(N)).
///
/// When no notifier is present (`None` — unit/integration tests that construct a
/// `ConnectionHandle` directly and `try_recv` the matching receiver themselves),
/// `send` simply forwards to the mailbox with no wake; correctness still holds
/// because those tests read the receiver directly rather than via the worker loop.
#[derive(Clone)]
pub struct Mailbox {
    inner: Sender<Box<ServerEvent>>,
    notify: Option<MailboxNotify>,
    /// Per-worker cumulative counter for mailbox-full drops. `None` in tests that
    /// wire mailboxes without a worker (the drop is still silent; the counter is
    /// just not observable). Incremented atomically on every `try_send` full-error.
    mailbox_dropped: Option<Arc<AtomicU64>>,
}

impl Mailbox {
    /// Build a mailbox over `inner`, optionally wired to wake `notify`'s worker
    /// and bump `mailbox_dropped` on a full-mailbox drop.
    pub fn new(
        inner: Sender<Box<ServerEvent>>,
        notify: Option<MailboxNotify>,
        mailbox_dropped: Option<Arc<AtomicU64>>,
    ) -> Self {
        Self {
            inner,
            notify,
            mailbox_dropped,
        }
    }

    /// Push `event` onto the bounded mailbox via `try_send` (non-blocking), then
    /// (if wired) mark this connection dirty and wake its worker so the send is
    /// drained promptly.
    ///
    /// When the mailbox is full the frame is silently dropped and the per-worker
    /// `mailbox_dropped` counter is incremented (if wired). This is acceptable
    /// under extreme overload (at-most-once, same as the out-queue) but MUST NOT
    /// drop under normal (non-full) load.
    ///
    /// Returns `Ok(())` on success or when the channel is full (drop-on-full);
    /// returns `Err` only when the receiver is gone (connection closed).
    pub fn send(
        &self,
        event: ServerEvent,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<ServerEvent>> {
        use tokio::sync::mpsc::error::TrySendError;
        match self.inner.try_send(Box::new(event)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                // Mailbox full under overload: drop the frame (fire-and-forget,
                // at-most-once semantics). Bump the per-worker counter if wired.
                if let Some(ctr) = &self.mailbox_dropped {
                    ctr.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(());
            }
            Err(TrySendError::Closed(ev)) => {
                return Err(tokio::sync::mpsc::error::SendError(*ev));
            }
        }
        if let Some(n) = &self.notify {
            // Mark dirty BEFORE the wake so the token is queued by the time the
            // woken worker drains its dirty queue (the wake only unblocks poll).
            let _ = n.dirty.send(n.token);
            let _ = n.waker.wake();
        }
        Ok(())
    }
}

/// Stored in the registry instead of a socket — broadcasting pushes into the
/// mailbox; the owning connection task writes to its own socket.
#[derive(Clone)]
pub struct ConnectionHandle {
    pub socket_id: SocketId,
    pub mailbox: Mailbox,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::event::ServerEvent;
    use tokio::sync::mpsc;

    /// Task 4 TDD: flood the mailbox past cap and assert:
    /// (a) sends beyond cap are DROPPED (not buffered unboundedly),
    /// (b) the `mailbox_dropped` counter increments for each dropped frame,
    /// (c) the worker stays LIVE (no panic/deadlock),
    /// (d) a subsequent drain delivers exactly the retained ≤cap messages.
    #[test]
    fn mailbox_flood_drops_excess_and_counts_them() {
        const CAP: usize = 4; // small cap for fast flood
        let drop_counter = Arc::new(AtomicU64::new(0));
        let (tx, mut rx) = mpsc::channel::<Box<ServerEvent>>(CAP);
        let mailbox = Mailbox::new(tx, None, Some(drop_counter.clone()));

        // Fill the mailbox to capacity.
        for _ in 0..CAP {
            assert!(mailbox.send(ServerEvent::Pong).is_ok());
        }
        // Now the channel is full; additional sends must be dropped.
        let extra = 3;
        for _ in 0..extra {
            assert!(
                mailbox.send(ServerEvent::Pong).is_ok(),
                "send must not return Err on full"
            );
        }
        // The drop counter must equal the number of excess sends.
        assert_eq!(
            drop_counter.load(Ordering::Relaxed),
            extra as u64,
            "drop counter must reflect every dropped frame"
        );
        // The channel holds exactly CAP messages, not CAP + extra.
        let mut received = 0usize;
        while rx.try_recv().is_ok() {
            received += 1;
        }
        assert_eq!(
            received, CAP,
            "channel must hold exactly CAP messages (no unbounded growth)"
        );
    }

    /// Task 4 TDD: under normal (non-full) load a single direct send is delivered
    /// with no spurious drop.
    #[test]
    fn mailbox_normal_load_delivers_without_drop() {
        const CAP: usize = 256;
        let drop_counter = Arc::new(AtomicU64::new(0));
        let (tx, mut rx) = mpsc::channel::<Box<ServerEvent>>(CAP);
        let mailbox = Mailbox::new(tx, None, Some(drop_counter.clone()));

        // Single send on a non-full mailbox must succeed and be receivable.
        assert!(mailbox.send(ServerEvent::Pong).is_ok());
        assert_eq!(
            drop_counter.load(Ordering::Relaxed),
            0,
            "no drop under normal load"
        );
        assert!(
            matches!(rx.try_recv().map(|b| *b), Ok(ServerEvent::Pong)),
            "event must be delivered"
        );
    }
}
