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
//! epoch) holding the connection tokens that expire then, plus side tables
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
//! Besides the (activity-relative) liveness timer, each connection can carry ONE
//! absolute [`Kind::Lifetime`] deadline, armed once at session establish via
//! [`arm_lifetime`](TimerWheel::arm_lifetime): the max-connection-lifetime close
//! `4202` ("Closed after inactivity"). It lives in its OWN side table (and is
//! tagged in the timeline), so the `touch`/`mark_ping_sent` reschedule path —
//! which only rewrites the liveness timer — can never push it out: the lifetime
//! is measured from establishment, not from last activity.
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
    /// The connection reached its maximum lifetime (`established_at +
    /// max_conn_lifetime`): close with code 4202 ("Closed after inactivity").
    Close4202(ConnId),
}

/// Which deadline a connection is currently waiting on.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// Idle deadline — fire a ping when it elapses.
    Idle,
    /// Pong deadline — close 4201 when it elapses (a ping is outstanding).
    Pong,
    /// Max-lifetime deadline — close 4202 when it elapses. ABSOLUTE from
    /// establishment; only ever set by [`TimerWheel::arm_lifetime`], never
    /// rescheduled by activity.
    Lifetime,
}

/// The connection's current (live) scheduled timer. A timeline entry is only
/// honoured by [`due`](TimerWheel::due) if it matches this — any earlier
/// timeline entry for the same connection is stale and skipped.
#[derive(Clone, Copy)]
struct Timer {
    deadline_ms: u64,
    kind: Kind,
}

/// One scheduled timeline bucket member: a connection plus which of its
/// deadlines the bucket records. Needed because a connection can be waiting on
/// a liveness timer AND a lifetime timer simultaneously — the `kind` tag tells
/// `due` which side table to validate the entry against.
#[derive(Clone, Copy)]
struct Slot {
    conn: ConnId,
    kind: Kind,
}

/// Per-worker liveness timer wheel: idle-ping after `activity_timeout`, then
/// `4201` close after `pong_timeout` with no pong; plus the absolute
/// max-connection-lifetime `4202` close (Pusher: currently 24 h).
pub struct TimerWheel {
    /// Idle timeout in ms (`activity_timeout` seconds).
    activity_timeout_ms: u64,
    /// Pong timeout in ms (`pong_timeout` seconds).
    pong_timeout_ms: u64,
    /// Absolute deadline (ms) → the connections scheduled to expire then. A
    /// connection can appear under multiple deadlines after a reschedule; the
    /// side tables ([`live`](Self::live) / [`lifetime`](Self::lifetime))
    /// disambiguate the current ones. A `Vec` (not a set) is fine: a
    /// `(connection, kind)` pair is inserted under a deadline at most once per
    /// schedule call.
    timeline: BTreeMap<u64, Vec<Slot>>,
    /// Each connection's *current* liveness timer (idle or pong). The source of
    /// truth for `Kind::Idle`/`Kind::Pong` timeline entries; the timeline is an
    /// index into it that may carry stale (superseded) entries.
    live: HashMap<ConnId, Timer>,
    /// Each connection's armed max-lifetime deadline (absolute ms). The source
    /// of truth for `Kind::Lifetime` timeline entries. Never rewritten by
    /// `touch`/`mark_ping_sent` — the lifetime is not activity-relative.
    lifetime: HashMap<ConnId, u64>,
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
            lifetime: HashMap::new(),
        }
    }

    /// Record inbound activity on `conn` at `now_ms`: (re)schedule its idle
    /// deadline at `now + activity_timeout`. If a ping was outstanding (a pong
    /// deadline was armed), this supersedes it — i.e. a pong arriving in time
    /// cancels the pending `4201` close (any inbound frame clears the pong
    /// deadline). Does NOT touch an armed lifetime deadline: that is absolute
    /// from establishment, not activity-relative.
    pub fn touch(&mut self, conn: ConnId, now_ms: u64) {
        let deadline_ms = now_ms.saturating_add(self.activity_timeout_ms);
        self.schedule(conn, deadline_ms, Kind::Idle);
    }

    /// Arm the pong deadline for `conn` after a `pusher:ping` was sent at
    /// `now_ms`: schedule a `4201` close at `now + pong_timeout`. Supersedes the
    /// idle deadline that just fired. Does NOT touch an armed lifetime deadline.
    pub fn mark_ping_sent(&mut self, conn: ConnId, now_ms: u64) {
        let deadline_ms = now_ms.saturating_add(self.pong_timeout_ms);
        self.schedule(conn, deadline_ms, Kind::Pong);
    }

    /// Arm `conn`'s max-connection-lifetime deadline at the ABSOLUTE
    /// `deadline_ms` (the worker passes `established_at +
    /// max_conn_lifetime_secs × 1000`). Called ONCE per session, at establish;
    /// nothing on the activity path reschedules it, so it always fires at
    /// exactly the armed deadline. A connection without a lifetime configured
    /// is simply never armed.
    pub fn arm_lifetime(&mut self, conn: ConnId, deadline_ms: u64) {
        self.lifetime.insert(conn, deadline_ms);
        self.timeline.entry(deadline_ms).or_default().push(Slot {
            conn,
            kind: Kind::Lifetime,
        });
    }

    /// Drop `conn` from the wheel entirely (on connection close, any reason).
    /// The stale timeline entries are reaped lazily by [`due`](Self::due); only
    /// the side-table entries must go now so a recycled slab token isn't matched
    /// against an old timer.
    pub fn remove(&mut self, conn: ConnId) {
        self.live.remove(&conn);
        self.lifetime.remove(&conn);
    }

    /// Advance the wheel to `now_ms` and return everything that has come due:
    /// `Due::Ping` for each idle-expired connection, `Due::Close4201` for each
    /// pong-timed-out connection, `Due::Close4202` for each lifetime-expired
    /// connection. Pops every timeline bucket at or before `now_ms` and
    /// validates each entry against its kind's side table, discarding
    /// superseded entries. `O(due-count + popped-stale)`, never `O(N-conns)`.
    pub fn due(&mut self, now_ms: u64) -> Vec<Due> {
        let mut out = Vec::new();
        // Pop every bucket whose deadline has elapsed. `split_off(&(now+1))`
        // leaves the future buckets in `self.timeline` and hands us the expired
        // ones (keys <= now_ms).
        let mut expired = self.timeline.split_off(&(now_ms + 1));
        std::mem::swap(&mut expired, &mut self.timeline);
        for (deadline_ms, slots) in expired {
            for slot in slots {
                // Honour the entry only if it is still this connection's live
                // timer of that kind at this exact deadline; otherwise it was
                // superseded by a touch/mark_ping_sent (or the connection was
                // removed).
                match slot.kind {
                    Kind::Idle | Kind::Pong => {
                        if let Some(t) = self.live.get(&slot.conn) {
                            if t.deadline_ms == deadline_ms && t.kind == slot.kind {
                                out.push(if slot.kind == Kind::Idle {
                                    Due::Ping(slot.conn)
                                } else {
                                    Due::Close4201(slot.conn)
                                });
                            }
                        }
                    }
                    // The lifetime deadline is never rescheduled: honour the
                    // entry iff the side table still arms this exact deadline
                    // (a `remove` between arming and firing drops it).
                    Kind::Lifetime => {
                        if self.lifetime.get(&slot.conn) == Some(&deadline_ms) {
                            out.push(Due::Close4202(slot.conn));
                        }
                    }
                }
            }
        }
        out
    }

    /// (Re)schedule `conn`'s liveness timer to fire at `deadline_ms` with
    /// `kind`, updating the side table and inserting a fresh timeline entry. The
    /// old timeline entry (if any) is left to be skipped lazily by [`due`](Self::due).
    fn schedule(&mut self, conn: ConnId, deadline_ms: u64, kind: Kind) {
        self.live.insert(conn, Timer { deadline_ms, kind });
        self.timeline
            .entry(deadline_ms)
            .or_default()
            .push(Slot { conn, kind });
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

    #[test]
    fn lifetime_fires_at_absolute_deadline_despite_constant_activity() {
        let mut w = TimerWheel::new();
        w.touch(7, 0);
        w.arm_lifetime(7, 5_000); // absolute: 5s after the epoch
                                  // The connection stays continuously active — every touch
                                  // re-arms the IDLE deadline but must NOT push the lifetime
                                  // deadline out (it is not activity-relative).
        for t in (0..5_000).step_by(500) {
            w.touch(7, t);
        }
        assert!(w.due(4_999).is_empty(), "lifetime must not fire early");
        assert_eq!(w.due(5_000), vec![Due::Close4202(7)]);
    }

    #[test]
    fn lifetime_coexists_with_the_liveness_timers() {
        let mut w = TimerWheel::new();
        w.touch(3, 0); // idle deadline at 120_000
        w.arm_lifetime(3, 130_000); // lifetime fires after the idle cycle
        assert_eq!(w.due(120_000), vec![Due::Ping(3)]);
        // The idle ping fired; a pong deadline is armed (close at 150_000)…
        w.mark_ping_sent(3, 120_000);
        // …but the LIFETIME deadline still fires at its own absolute time,
        // beating the pong close. Only the lifetime entry is due at 130_000.
        assert_eq!(w.due(130_000), vec![Due::Close4202(3)]);
        // And with the connection gone, the superseded pong timer never fires.
        w.remove(3);
        assert!(w.due(200_000).is_empty());
    }

    #[test]
    fn remove_cancels_armed_lifetime() {
        let mut w = TimerWheel::new();
        w.arm_lifetime(5, 1_000);
        w.remove(5);
        // The lifetime entry still sits in the timeline but the connection is
        // gone, so nothing fires.
        assert!(w.due(2_000).is_empty());
    }

    #[test]
    fn unresponded_ping_still_closes_4201_alongside_a_later_lifetime() {
        // The 4201 path is unchanged by the lifetime deadline: an idle conn with
        // a far-future lifetime still gets ping → 4201 exactly as before.
        let mut w = TimerWheel::new();
        w.touch(9, 0);
        w.arm_lifetime(9, 86_400_000); // 24h
        assert_eq!(w.due(120_000), vec![Due::Ping(9)]);
        w.mark_ping_sent(9, 120_000);
        assert_eq!(w.due(151_000), vec![Due::Close4201(9)]);
        w.remove(9);
        assert!(
            w.due(86_400_000).is_empty(),
            "removed conn's lifetime must not fire"
        );
    }
}
