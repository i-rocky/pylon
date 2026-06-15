//! Per-worker timer wheel for connection liveness (SP11 §4).
//!
//! The Pusher v7 liveness contract: after `activity_timeout` seconds of no
//! inbound traffic the server sends a `pusher:ping`; if that ping goes unanswered
//! for `pong_timeout` seconds it closes the connection with code `4201`. The
//! per-core transport has no per-connection tokio runtime, so it can't lean on a
//! timer-per-socket.
//!
//! [`TimerWheel`] reproduces those exact semantics with one structure per worker:
//! a [`BTreeMap`] keyed by absolute deadline (monotonic ms since the worker
//! epoch) holding the connection tokens that expire then, plus a side table
//! recording each connection's *current* scheduled deadline and what kind of
//! event it is (idle-ping vs. pong-timeout-close). Lookups by deadline are
//! `O(due-count)` per [`due`](TimerWheel::due) — the wheel only ever visits the
//! entries that have actually expired, never every connection.
//!
//! Rescheduling (a [`touch`](TimerWheel::touch) on inbound activity, or a
//! [`mark_ping_sent`](TimerWheel::mark_ping_sent) after emitting a ping) leaves
//! the old timeline entry in place and is reconciled lazily: when `due` pops a
//! deadline it checks the side table and discards the entry if the connection
//! has since been rescheduled past it. A `touch` arriving while a ping is
//! outstanding therefore *cancels* the pending `4201` close — any inbound frame
//! is liveness activity that clears the pong deadline.
//!
//! Time is injected (every method takes `now_ms`) so the unit test is fully
//! deterministic. The worker feeds it the same monotonic clock it already
//! computes for CoDel (the `worker_epoch` elapsed, in milliseconds).
//!
//! Safe Rust — the crate root sets `#![deny(unsafe_code)]`; this module adds no
//! `unsafe`.

use std::collections::{BTreeMap, HashMap};

/// A connection identifier within a worker — the slab token (== `mio::Token`
/// value) the worker keys its connection table on.
type ConnId = usize;

/// An action the wheel says is due for a connection at the current time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Due {
    /// The connection has been idle for `activity_timeout`: send a `pusher:ping`.
    /// The worker, after queuing the ping, calls
    /// [`mark_ping_sent`](TimerWheel::mark_ping_sent) to arm the pong deadline.
    Ping(ConnId),
    /// A `pusher:ping` went unanswered for `pong_timeout`: close with code 4201.
    Close4201(ConnId),
}

/// Which deadline a connection is currently waiting on.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// Idle deadline — fire a ping when it elapses.
    Idle,
    /// Pong deadline — close 4201 when it elapses (a ping is outstanding).
    Pong,
}

/// The connection's current (live) scheduled timer. A timeline entry is only
/// honoured by [`due`](TimerWheel::due) if it matches this — any earlier
/// timeline entry for the same connection is stale and skipped.
#[derive(Clone, Copy)]
struct Timer {
    deadline_ms: u64,
    kind: Kind,
}

/// Per-worker liveness timer wheel: idle-ping after `activity_timeout`, then
/// `4201` close after `pong_timeout` with no pong.
pub struct TimerWheel {
    /// Idle timeout in ms (`activity_timeout` seconds).
    activity_timeout_ms: u64,
    /// Pong timeout in ms (`pong_timeout` seconds).
    pong_timeout_ms: u64,
    /// Absolute deadline (ms) → the connections scheduled to expire then. A
    /// connection can appear under multiple deadlines after a reschedule; the
    /// side table [`live`](Self::live) disambiguates the current one. A `Vec`
    /// (not a set) is fine: a connection is inserted under a deadline at most
    /// once per schedule call.
    timeline: BTreeMap<u64, Vec<ConnId>>,
    /// Each connection's *current* timer. The source of truth; the timeline is
    /// an index into it that may carry stale (superseded) entries.
    live: HashMap<ConnId, Timer>,
}

impl TimerWheel {
    /// Build a wheel with the default timeouts (120 s idle / 30 s pong). Tests use
    /// this so the in-ms assertions match the default seconds.
    pub fn new() -> Self {
        Self::with_timeouts(120, 30)
    }

    /// Build a wheel from the configured `activity_timeout` / `pong_timeout`
    /// (seconds), converted to the ms the wheel works in.
    pub fn with_timeouts(activity_timeout_secs: u32, pong_timeout_secs: u32) -> Self {
        Self {
            activity_timeout_ms: activity_timeout_secs as u64 * 1000,
            pong_timeout_ms: pong_timeout_secs as u64 * 1000,
            timeline: BTreeMap::new(),
            live: HashMap::new(),
        }
    }

    /// Record inbound activity on `conn` at `now_ms`: (re)schedule its idle
    /// deadline at `now + activity_timeout`. If a ping was outstanding (a pong
    /// deadline was armed), this supersedes it — i.e. a pong arriving in time
    /// cancels the pending `4201` close (any inbound frame clears the pong
    /// deadline).
    pub fn touch(&mut self, conn: ConnId, now_ms: u64) {
        let deadline_ms = now_ms.saturating_add(self.activity_timeout_ms);
        self.schedule(conn, deadline_ms, Kind::Idle);
    }

    /// Arm the pong deadline for `conn` after a `pusher:ping` was sent at
    /// `now_ms`: schedule a `4201` close at `now + pong_timeout`. Supersedes the
    /// idle deadline that just fired.
    pub fn mark_ping_sent(&mut self, conn: ConnId, now_ms: u64) {
        let deadline_ms = now_ms.saturating_add(self.pong_timeout_ms);
        self.schedule(conn, deadline_ms, Kind::Pong);
    }

    /// Drop `conn` from the wheel entirely (on connection close, any reason).
    /// The stale timeline entries are reaped lazily by [`due`](Self::due); only
    /// the side-table entry must go now so a recycled slab token isn't matched
    /// against an old timer.
    pub fn remove(&mut self, conn: ConnId) {
        self.live.remove(&conn);
    }

    /// Advance the wheel to `now_ms` and return everything that has come due:
    /// `Due::Ping` for each idle-expired connection, `Due::Close4201` for each
    /// pong-timed-out connection. Pops every timeline bucket at or before
    /// `now_ms` and validates each token against the side table, discarding
    /// superseded entries. `O(due-count + popped-stale)`, never `O(N-conns)`.
    pub fn due(&mut self, now_ms: u64) -> Vec<Due> {
        let mut out = Vec::new();
        // Pop every bucket whose deadline has elapsed. `split_off(&(now+1))`
        // leaves the future buckets in `self.timeline` and hands us the expired
        // ones (keys <= now_ms).
        let mut expired = self.timeline.split_off(&(now_ms + 1));
        std::mem::swap(&mut expired, &mut self.timeline);
        for (deadline_ms, conns) in expired {
            for conn in conns {
                // Honour the entry only if it is still this connection's live
                // timer at this exact deadline; otherwise it was superseded by a
                // touch/mark_ping_sent (or the connection was removed).
                match self.live.get(&conn) {
                    Some(t) if t.deadline_ms == deadline_ms => match t.kind {
                        Kind::Idle => out.push(Due::Ping(conn)),
                        Kind::Pong => out.push(Due::Close4201(conn)),
                    },
                    _ => {} // stale or removed — skip
                }
            }
        }
        out
    }

    /// (Re)schedule `conn`'s timer to fire at `deadline_ms` with `kind`,
    /// updating the side table and inserting a fresh timeline entry. The old
    /// timeline entry (if any) is left to be skipped lazily by [`due`](Self::due).
    fn schedule(&mut self, conn: ConnId, deadline_ms: u64, kind: Kind) {
        self.live.insert(conn, Timer { deadline_ms, kind });
        self.timeline.entry(deadline_ms).or_default().push(conn);
    }
}

impl Default for TimerWheel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wheel_fires_idle_then_pong_timeout() {
        // activity_timeout=120s, pong_timeout=30s; a connection silent since t0.
        let mut w = TimerWheel::new();
        w.touch(7, /*now*/ 0); // conn id 7 active at t0
                               // before activity_timeout: nothing due
        assert!(w.due(119_000).is_empty()); // ms
                                            // at activity_timeout: ping due for conn 7
        assert_eq!(w.due(120_000), vec![Due::Ping(7)]);
        w.mark_ping_sent(7, 120_000);
        // pong not received within pong_timeout → close 4201
        assert_eq!(w.due(151_000), vec![Due::Close4201(7)]);
        // a pong (touch) before the deadline cancels the close
        let mut w2 = TimerWheel::new();
        w2.touch(9, 0);
        w2.due(120_000);
        w2.mark_ping_sent(9, 120_000);
        w2.touch(9, 140_000); // pong arrived
        assert!(w2.due(151_000).is_empty());
    }

    #[test]
    fn remove_cancels_pending_timer() {
        let mut w = TimerWheel::new();
        w.touch(3, 0);
        w.remove(3);
        // The idle deadline still sits in the timeline but the connection is
        // gone, so nothing fires.
        assert!(w.due(200_000).is_empty());
    }

    #[test]
    fn active_connection_is_never_pinged_early() {
        let mut w = TimerWheel::new();
        // A connection that keeps talking pushes its idle deadline forward each
        // time; it must never come due while it stays active.
        for t in (0..600_000).step_by(10_000) {
            w.touch(1, t);
            assert!(w.due(t).is_empty(), "active conn pinged at t={t}");
        }
        // Once it goes silent, the idle ping fires activity_timeout later.
        assert_eq!(w.due(590_000 + 120_000), vec![Due::Ping(1)]);
    }

    #[test]
    fn due_is_ordered_and_handles_multiple_conns() {
        let mut w = TimerWheel::with_timeouts(120, 30);
        w.touch(1, 0);
        w.touch(2, 1_000);
        w.touch(3, 2_000);
        // At t = 121_000 only conns 1 and 2 (deadlines 120_000 and 121_000) are
        // due; conn 3's deadline is 122_000.
        let due = w.due(121_000);
        assert_eq!(due, vec![Due::Ping(1), Due::Ping(2)]);
        assert_eq!(w.due(122_000), vec![Due::Ping(3)]);
    }
}
