//! Per-core SHARDED broadcast fan-out sink (SP9).
//!
//! The legacy delivery path ([`ChannelState::broadcast`](crate::channel::state))
//! enqueues a broadcast onto every subscriber's per-connection `mpsc` mailbox
//! from ONE thread. With N (e.g. 10k) subscribers on a channel that is N
//! `UnboundedSender::send` calls — each an alloc + a futex wake — serialized on
//! the publishing thread, which walls fan-out long before the CPU is the bound.
//!
//! This sink replaces that with a per-WORKER hand-off: a broadcast notifies each
//! worker exactly ONCE (W messages, not N), and each worker then fans the
//! (already WS-framed) bytes out to its OWN local subscribers by direct
//! slab-enqueue (a refcount bump per subscriber, no per-connection mpsc, no
//! per-connection wake). The work to actually copy bytes onto each connection's
//! send queue is thereby spread across all worker cores instead of running
//! serially on the publisher.
//!
//! Only DELIVERY of channel broadcasts moves here; membership/counts still flow
//! through the registry, and DIRECT sends (connection_established, rosters,
//! send_to_user, terminate, …) still use the per-connection mailbox.
//!
//! Safe Rust — the crate root sets `#![deny(unsafe_code)]`; this module adds no
//! `unsafe`.

use crate::protocol::socket_id::SocketId;
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Default capacity of a worker's bounded broadcast hand-off channel (frames).
/// The publish→workers hand-off is bounded so a publish flood cannot grow it
/// unbounded (the SP9 hang). On `Full` the broadcast is dropped (at-most-once)
/// and the sink is flagged saturated. Overridable via config in Phase 2.
pub const DEFAULT_BROADCAST_HANDOFF_CAP: usize = 1024;

/// One sharded broadcast hand-off: the WS-framed bytes plus the routing keys
/// every worker needs to find its local subscribers. `frame` is already a
/// complete server→client WebSocket text frame (encoded once by the publisher,
/// frozen zero-copy from the encoder's buffer), shared via `Bytes` so each
/// worker's per-connection enqueue is a cheap refcount bump rather than a copy.
pub struct BroadcastMsg {
    pub app: Arc<str>,
    pub channel: Arc<str>,
    pub frame: Bytes,
    /// The originating connection's `socket_id`, excluded from delivery (sender
    /// exclusion for client events / count echoes). `None` ⇒ deliver to all.
    pub except: Option<SocketId>,
}

/// One slot per worker. The `SyncSender` is created in `run_percore` (paired
/// with the `Receiver` handed to the worker) over a **bounded** `sync_channel`,
/// so a publish flood that outruns delivery is dropped at the hand-off rather
/// than buffered unbounded (the SP9 hang fix). The `Waker` is created BY the
/// worker from its own `mio::Poll` registry at startup and published into the
/// `OnceLock` so the sink can nudge an idle worker to drain promptly.
pub struct WorkerSlot {
    /// Bounded hand-off to this worker's broadcast inbox. `broadcast` uses
    /// `try_send`; a `Full` channel means the worker is behind, so the message is
    /// dropped (at-most-once) and counted in `dropped`.
    pub tx: std::sync::mpsc::SyncSender<BroadcastMsg>,
    pub waker: std::sync::OnceLock<Arc<mio::Waker>>,
    /// Count of broadcasts dropped because this worker's hand-off channel was
    /// full. A monotonic saturation metric (Relaxed is fine — it's diagnostic).
    pub dropped: AtomicU64,
}

/// Cloneable handle the adapter holds to route broadcasts to every worker. The
/// `Arc<Vec<Arc<WorkerSlot>>>` is shared (one allocation) so cloning the sink
/// onto the adapter is cheap. Each slot is itself an `Arc` SHARED with the
/// owning worker, so the `Waker` a worker publishes into its slot's `OnceLock`
/// at startup is immediately visible to the sink.
#[derive(Clone, Default)]
pub struct BroadcastSink {
    pub workers: Arc<Vec<Arc<WorkerSlot>>>,
    /// Set whenever any worker's bounded hand-off channel is `Full` (a broadcast
    /// was dropped). The publish-admission path reads this via [`BroadcastSink::is_saturated`]
    /// to fail fast (503) under sustained overload; a worker clears it after
    /// fully draining its broadcast inbox to empty. Shared (`Arc`) so the cheap
    /// `Clone` of the sink onto the adapter keeps pointing at the same flag.
    pub saturated: Arc<AtomicBool>,
}

impl BroadcastSink {
    /// Hand the (already WS-framed) `frame` to EVERY worker; each worker filters
    /// to the subscribers it owns. The hand-off is BOUNDED: `try_send` on a full
    /// channel means that worker is behind delivery, so the broadcast is dropped
    /// (at-most-once delivery — dropping the freshest-loser is correct) and the
    /// slot's `dropped` counter is bumped + the sink flagged saturated. A
    /// `Disconnected` channel (a worker thread gone) and a failed `wake` are both
    /// ignored — a vanished worker has no live connections to deliver to.
    pub fn broadcast(
        &self,
        app: Arc<str>,
        channel: Arc<str>,
        frame: Bytes,
        except: Option<SocketId>,
    ) {
        for slot in self.workers.iter() {
            match slot.tx.try_send(BroadcastMsg {
                app: app.clone(),
                channel: channel.clone(),
                frame: frame.clone(),
                except,
            }) {
                Ok(()) => {}
                Err(std::sync::mpsc::TrySendError::Full(_)) => {
                    // Pipeline saturated for this worker: drop + flag. The worker
                    // clears `saturated` once it drains its inbox to empty.
                    slot.dropped.fetch_add(1, Ordering::Relaxed);
                    self.saturated.store(true, Ordering::Relaxed);
                    // Skip the wake: a full inbox needs no nudge to drain.
                    continue;
                }
                // Worker gone: nothing to deliver to.
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => continue,
            }
            if let Some(w) = slot.waker.get() {
                let _ = w.wake();
            }
        }
    }

    /// Whether the broadcast pipeline is currently saturated (a hand-off channel
    /// was found full). Read cheaply by the publish-admission path (Phase 2's 503
    /// gate). Cleared by a worker after it fully drains its broadcast inbox.
    pub fn is_saturated(&self) -> bool {
        self.saturated.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arc(s: &str) -> Arc<str> {
        Arc::from(s)
    }
    fn bytes(b: &[u8]) -> Bytes {
        Bytes::copy_from_slice(b)
    }

    /// A bounded hand-off with no draining receiver: capacity 2, send 5 → exactly
    /// 2 queue and 3 are dropped + counted, and the sink reports saturated.
    #[test]
    fn bounded_handoff_drops_on_full_and_flags_saturation() {
        let (tx, _rx) = std::sync::mpsc::sync_channel::<BroadcastMsg>(2);
        let slot = WorkerSlot {
            tx,
            waker: std::sync::OnceLock::new(),
            dropped: AtomicU64::new(0),
        };
        let sink = BroadcastSink {
            workers: Arc::new(vec![Arc::new(slot)]),
            saturated: Arc::new(AtomicBool::new(false)),
        };
        for _ in 0..5 {
            sink.broadcast(arc("a"), arc("c"), bytes(b"x"), None);
        }
        assert_eq!(sink.workers[0].dropped.load(Ordering::Relaxed), 3);
        assert!(sink.is_saturated());
    }
}
