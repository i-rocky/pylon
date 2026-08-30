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
//! [`mark_ping_sent`](TimerWheel::mark_ping_sent) after emitting a ping)
//! EAGERLY removes the superseded timeline entry: without that, a chatty
//! connection (say 100 msg/s) accrues ~12,000 stale buckets before `due`
//! sweeps past their deadlines an `activity_timeout` (120 s) later — tens of
//! MB of dead entries per connection. With the eager removal the timeline
//! holds at most one liveness slot, one lifetime slot and one handshake slot
//! per connection; `due` still validates every popped entry against the side
//! tables as defense in depth. A `touch` arriving while a ping is outstanding
//! *cancels* the pending `4201` close — any inbound frame is liveness activity
//! that clears the pong deadline.
//!
//! Besides the (activity-relative) liveness timer, each connection can carry ONE
//! absolute [`Kind::Lifetime`] deadline, armed once at session establish via
//! [`arm_lifetime`](TimerWheel::arm_lifetime): the max-connection-lifetime close
//! `4202` ("Closed after inactivity"). It lives in its OWN side table (and is
//! tagged in the timeline), so the `touch`/`mark_ping_sent` reschedule path —
//! which only rewrites the liveness timer — can never push it out: the lifetime
//! is measured from establishment, not from last activity.
//!
//! Each connection can ALSO carry ONE absolute [`Kind::Handshake`] deadline,
//! armed at ACCEPT via [`arm_handshake`](TimerWheel::arm_handshake): the
//! slowloris reap — a connection that has not completed its handshake within
//! `handshake_timeout_ms` of accept is closed and its slot reclaimed. Same
//! side-table pattern as the lifetime deadline (so dribbled inbound bytes —
//! which `touch` the liveness timer — can NEVER postpone it), cleared by
//! [`clear_handshake`](TimerWheel::clear_handshake) the moment the session
//! establishes.
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
    /// A pre-session connection blew the handshake deadline (`accepted_at +
    /// `handshake_timeout_ms`): close it and reclaim its slot. No WS session
    /// exists, so the worker tears the TCP connection down without a close
    /// frame.
    HandshakeTimeout(ConnId),
}

/// Which deadline a connection is currently waiting on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// Idle deadline — fire a ping when it elapses.
    Idle,
    /// Pong deadline — close 4201 when it elapses (a ping is outstanding).
    Pong,
    /// Max-lifetime deadline — close 4202 when it elapses. ABSOLUTE from
    /// establishment; only ever set by [`TimerWheel::arm_lifetime`], never
    /// rescheduled by activity.
    Lifetime,
    /// Pre-session handshake deadline — reap when it elapses. ABSOLUTE from
    /// accept; only ever set by [`TimerWheel::arm_handshake`], never
    /// rescheduled by activity, cleared by [`TimerWheel::clear_handshake`] at
    /// session establish.
    Handshake,
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
    /// connection appears under at most one deadline PER KIND (liveness,
    /// lifetime, handshake) at a time: re-arms and teardown eagerly scrub the
    /// superseded slot, so no stale entries linger for
    /// [`due`](Self::due) to skip. A `Vec` (not a set) is fine: a
    /// `(connection, kind)` pair is inserted under a deadline at most once per
    /// schedule call.
    timeline: BTreeMap<u64, Vec<Slot>>,
    /// Each connection's *current* liveness timer (idle or pong). The source of
    /// truth for `Kind::Idle`/`Kind::Pong` timeline entries; the timeline is an
    /// index into it that is kept slot-exact by the eager scrub on re-arm.
    live: HashMap<ConnId, Timer>,
    /// Each connection's armed max-lifetime deadline (absolute ms). The source
    /// of truth for `Kind::Lifetime` timeline entries. Never rewritten by
    /// `touch`/`mark_ping_sent` — the lifetime is not activity-relative.
    lifetime: HashMap<ConnId, u64>,
    /// Each connection's armed pre-session handshake deadline (absolute ms,
    /// from accept). The source of truth for `Kind::Handshake` timeline
    /// entries. Never rewritten by `touch`/`mark_ping_sent` (dribbled bytes are
    /// activity on the liveness timer only) — the deadline is absolute from
    /// accept; cleared at session establish.
    handshake: HashMap<ConnId, u64>,
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
            handshake: HashMap::new(),
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
        // Defensive: a re-arm replaces the previous slot instead of stranding
        // it (the method is documented once-per-session, but the wheel must
        // not bloat if a caller ever disobeys).
        if let Some(old) = self.lifetime.insert(conn, deadline_ms) {
            self.scrub(conn, old, Kind::Lifetime);
        }
        self.timeline.entry(deadline_ms).or_default().push(Slot {
            conn,
            kind: Kind::Lifetime,
        });
    }

    /// Arm `conn`'s pre-session handshake deadline at the ABSOLUTE
    /// `deadline_ms` (the worker passes `accepted_at + handshake_timeout_ms`).
    /// Called ONCE per connection, at accept; nothing on the activity path
    /// reschedules it (a dribbling slowloris `touch`es only the liveness
    /// timer), so it always fires at exactly the armed deadline unless
    /// [`clear_handshake`](Self::clear_handshake) runs first (session
    /// establish) or the connection is [`remove`](Self::remove)d.
    pub fn arm_handshake(&mut self, conn: ConnId, deadline_ms: u64) {
        // Defensive: same replace-don't-strand rule as `arm_lifetime`.
        if let Some(old) = self.handshake.insert(conn, deadline_ms) {
            self.scrub(conn, old, Kind::Handshake);
        }
        self.timeline.entry(deadline_ms).or_default().push(Slot {
            conn,
            kind: Kind::Handshake,
        });
    }

    /// Clear `conn`'s armed handshake deadline (the session established within
    /// the window): drops BOTH the side-table entry and its timeline slot, so
    /// the deadline cannot fire on an established connection and nothing
    /// lingers for [`due`](Self::due) to sweep later.
    pub fn clear_handshake(&mut self, conn: ConnId) {
        if let Some(deadline_ms) = self.handshake.remove(&conn) {
            self.scrub(conn, deadline_ms, Kind::Handshake);
        }
    }

    /// Drop `conn`'s LIVENESS timer only (idle or pong), leaving any armed
    /// absolute deadlines (lifetime, handshake) in place. Used by the worker's
    /// `Due::Ping` path when it finds no session: the pre-session connection's
    /// idle timer is spurious (only established sessions are pinged), but its
    /// handshake deadline must survive so the slowloris reap still fires.
    /// The liveness timeline slot is scrubbed, not left for a lazy skip.
    pub fn clear_liveness(&mut self, conn: ConnId) {
        if let Some(t) = self.live.remove(&conn) {
            self.scrub(conn, t.deadline_ms, t.kind);
        }
    }

    /// Drop `conn` from the wheel entirely (on connection close, any reason):
    /// every side-table entry AND every timeline slot goes now, so a recycled
    /// slab token is never matched against an old timer and no slot lingers
    /// at a possibly 24 h-out lifetime deadline.
    pub fn remove(&mut self, conn: ConnId) {
        if let Some(t) = self.live.remove(&conn) {
            self.scrub(conn, t.deadline_ms, t.kind);
        }
        if let Some(deadline_ms) = self.lifetime.remove(&conn) {
            self.scrub(conn, deadline_ms, Kind::Lifetime);
        }
        if let Some(deadline_ms) = self.handshake.remove(&conn) {
            self.scrub(conn, deadline_ms, Kind::Handshake);
        }
    }

    /// Advance the wheel to `now_ms` and return everything that has come due:
    /// `Due::Ping` for each idle-expired connection, `Due::Close4201` for each
    /// pong-timed-out connection, `Due::Close4202` for each lifetime-expired
    /// connection. Pops every timeline bucket at or before `now_ms` and
    /// validates each entry against its kind's side table — defense in depth:
    /// the eager scrub on re-arm/teardown already keeps the timeline free of
    /// superseded entries, and the validation would discard any that slipped
    /// through. `O(due-count)`, never `O(N-conns)`.
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
                    // Same for the handshake deadline: honoured iff the side
                    // table still arms this exact deadline (a
                    // `clear_handshake` at establish or a `remove` at close
                    // drops it).
                    Kind::Handshake => {
                        if self.handshake.get(&slot.conn) == Some(&deadline_ms) {
                            out.push(Due::HandshakeTimeout(slot.conn));
                        }
                    }
                }
            }
        }
        out
    }

    /// (Re)schedule `conn`'s liveness timer to fire at `deadline_ms` with
    /// `kind`: scrub the superseded timeline entry (G6 — re-arms must not
    /// accrue stale buckets for [`due`](Self::due) to skip up to
    /// `activity_timeout` later), then update the side table and insert the
    /// fresh timeline entry.
    fn schedule(&mut self, conn: ConnId, deadline_ms: u64, kind: Kind) {
        let superseded = self.live.get(&conn).copied();
        if let Some(old) = superseded {
            self.scrub(conn, old.deadline_ms, old.kind);
        }
        self.live.insert(conn, Timer { deadline_ms, kind });
        self.timeline
            .entry(deadline_ms)
            .or_default()
            .push(Slot { conn, kind });
    }

    /// Eagerly remove `conn`'s `kind` timeline entry at `deadline_ms` (and the
    /// bucket itself once empty). Called on every re-arm and every teardown
    /// path, so the timeline never carries an entry the side tables would
    /// only lazily discard — a chatty connection holds exactly one liveness
    /// slot, an early disconnect leaves nothing parked at its (possibly
    /// 24 h-out) lifetime/handshake deadline.
    fn scrub(&mut self, conn: ConnId, deadline_ms: u64, kind: Kind) {
        if let std::collections::btree_map::Entry::Occupied(mut bucket) =
            self.timeline.entry(deadline_ms)
        {
            bucket
                .get_mut()
                .retain(|s| !(s.conn == conn && s.kind == kind));
            if bucket.get().is_empty() {
                bucket.remove();
            }
        }
    }
}

impl Default for TimerWheel {
    fn default() -> Self {
        Self::new()
    }
}

// ── test-only introspection ─────────────────────────────────────────────────
impl TimerWheel {
    /// Number of distinct deadline buckets currently in the timeline. Pins the
    /// no-stale-bloat invariant (G6): a repeatedly re-armed connection must
    /// keep this O(1), not grow one bucket per `touch`.
    #[cfg(test)]
    fn bucket_count(&self) -> usize {
        self.timeline.len()
    }

    /// Total timeline slot entries across all buckets. At rest this must equal
    /// `live.len() + lifetime.len() + handshake.len()` — every slot backed by
    /// a side-table entry, no strays.
    #[cfg(test)]
    fn slot_count(&self) -> usize {
        self.timeline.values().map(Vec::len).sum()
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
        // The connection is gone and its timeline slot was scrubbed, so
        // nothing fires.
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
        // The connection is gone and the lifetime entry was scrubbed with it,
        // so nothing fires.
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

    // ── G3: pre-session handshake deadline ─────────────────────────────────────

    #[test]
    fn handshake_deadline_fires_at_accept_plus_timeout_despite_constant_dribble() {
        let mut w = TimerWheel::new();
        w.arm_handshake(4, 500); // absolute: 500ms after accept
                                 // The slowloris keeps dribbling bytes — every touch re-arms the IDLE
                                 // deadline but must NOT push the handshake deadline out.
        for t in (0..500).step_by(50) {
            w.touch(4, t);
        }
        assert!(
            w.due(499).is_empty(),
            "handshake deadline must not fire early"
        );
        assert_eq!(w.due(500), vec![Due::HandshakeTimeout(4)]);
    }

    #[test]
    fn clear_handshake_at_establish_cancels_the_reap() {
        let mut w = TimerWheel::new();
        w.arm_handshake(4, 500);
        w.clear_handshake(4); // session established within the window
        assert!(
            w.due(10_000).is_empty(),
            "an established connection must never be reaped by the handshake deadline"
        );
    }

    #[test]
    fn remove_cancels_armed_handshake() {
        let mut w = TimerWheel::new();
        w.arm_handshake(5, 1_000);
        w.remove(5);
        // The side table and the timeline entry are both gone, so nothing
        // fires for the (possibly recycled) token.
        assert!(w.due(2_000).is_empty());
    }

    #[test]
    fn clear_liveness_keeps_the_handshake_deadline_armed() {
        // A pre-session conn whose (spurious) idle timer fires loses ONLY that
        // timer — its handshake deadline must survive so the reap still fires.
        // (Config where activity_timeout < handshake_timeout, so the idle timer
        // is the one that fires first on a dribbling conn.)
        let mut w = TimerWheel::new();
        w.touch(6, 0); // dribble armed an idle deadline at 120_000
        w.arm_handshake(6, 200_000); // handshake deadline AFTER the idle cycle
        assert_eq!(w.due(120_000), vec![Due::Ping(6)]); // spurious idle ping
        w.clear_liveness(6); // the worker found no session → liveness-only clear
        assert_eq!(
            w.bucket_count(),
            1,
            "clear_liveness scrubs its own slot; only the handshake bucket remains"
        );
        assert_eq!(
            w.due(200_000),
            vec![Due::HandshakeTimeout(6)],
            "the handshake deadline must survive the liveness clear"
        );
    }

    #[test]
    fn handshake_deadline_coexists_with_liveness_and_lifetime_timers() {
        let mut w = TimerWheel::new();
        w.arm_handshake(7, 1_000);
        w.touch(7, 0); // idle at 120_000
                       // Establish before the handshake deadline (the 1_000 slot is
                       // scrubbed, so the wheel never even pops it): liveness +
                       // lifetime live on after it.
        w.clear_handshake(7);
        w.arm_lifetime(7, 200_000);
        assert_eq!(w.due(120_000), vec![Due::Ping(7)]);
        w.mark_ping_sent(7, 120_000);
        assert_eq!(w.due(150_000), vec![Due::Close4201(7)]);
        assert_eq!(w.due(200_000), vec![Due::Close4202(7)]);
        w.remove(7);
        assert!(w.due(1_000_000).is_empty());
    }

    // ── G6: no stale-bucket bloat on re-arm / teardown ────────────────────────

    #[test]
    fn chatty_connection_does_not_accrue_stale_buckets() {
        // Every touch re-arms the idle deadline; the superseded timeline entry
        // must be dropped AT the re-arm, not left for `due` to lazily skip up
        // to activity_timeout (120 s) later. Without the eager removal a
        // 100 msg/s connection accrues ~12,000 dead slots — tens of MB per
        // chatty connection — before the sweep reaps them.
        let mut w = TimerWheel::new();
        w.touch(1, 0);
        for i in 1..=1_000u64 {
            w.touch(1, i * 100); // advancing activity → advancing deadlines
            assert!(
                w.bucket_count() <= 2,
                "touch #{i} left {} buckets — stale entries not reaped on re-arm",
                w.bucket_count()
            );
            assert_eq!(w.slot_count(), 1, "exactly one live slot expected");
        }
        assert_eq!(w.bucket_count(), 1);
        // The single surviving entry is the CURRENT deadline: the ping fires
        // exactly activity_timeout after the last touch, and the bucket pops.
        assert_eq!(w.due(100_000 + 120_000), vec![Due::Ping(1)]);
        assert_eq!(w.bucket_count(), 0);
    }

    #[test]
    fn chatty_connection_with_ping_cycles_does_not_accrue_stale_buckets() {
        // Same invariant across Idle↔Pong kind transitions: a
        // `mark_ping_sent` supersedes an armed idle deadline, and a late-but-
        // timely pong supersedes the pong deadline — the superseded slots must
        // go, not linger.
        let mut w = TimerWheel::new();
        w.touch(2, 0);
        let mut now = 0u64;
        for cycle in 0..50u64 {
            assert_eq!(w.due(now + 120_000), vec![Due::Ping(2)], "cycle {cycle}");
            assert_eq!(w.slot_count(), 0, "pop must empty the timeline");
            w.mark_ping_sent(2, now + 120_000);
            assert_eq!(w.slot_count(), 1);
            now += 125_000; // pong arrives late but in time
            w.touch(2, now);
            assert_eq!(w.slot_count(), 1, "pong must replace, not append");
            now += 5_000;
        }
        assert_eq!(w.bucket_count(), 1);
    }

    #[test]
    fn remove_scrubs_every_timeline_slot_immediately() {
        // Task 1.5 review noted stale lifetime slots linger to the full
        // lifetime on early disconnect: a conn armed for the default 24 h that
        // disconnects after 1 s must NOT park a timeline slot for 24 h.
        let mut w = TimerWheel::new();
        w.touch(5, 0);
        w.arm_lifetime(5, 86_400_000); // 24 h out
        w.arm_handshake(5, 30_000); // (pre-session dribble conn also reaped-armed)
        assert_eq!(w.bucket_count(), 3);
        w.remove(5);
        assert_eq!(
            w.bucket_count(),
            0,
            "remove must scrub every timeline slot eagerly"
        );
        assert!(w.live.is_empty());
        assert!(w.lifetime.is_empty());
        assert!(w.handshake.is_empty());
        // Nothing ever fires for the (possibly recycled) token.
        assert!(w.due(u64::MAX / 2).is_empty());
    }

    #[test]
    fn clear_handshake_scrubs_the_timeline_slot_too() {
        // Early establish clears BOTH the side table and the timeline slot: no
        // lingering reap entry for `due` to skip a handshake_timeout later.
        let mut w = TimerWheel::new();
        w.arm_handshake(4, 30_000);
        assert_eq!(w.bucket_count(), 1);
        w.clear_handshake(4);
        assert_eq!(w.bucket_count(), 0);
        assert!(w.handshake.is_empty());
        assert!(w.due(1_000_000).is_empty());
    }

    #[test]
    fn re_armed_lifetime_replaces_its_timeline_slot() {
        // arm_lifetime is documented once-per-session, but a defensive re-arm
        // must replace the previous slot, not strand it.
        let mut w = TimerWheel::new();
        w.arm_lifetime(8, 1_000);
        w.arm_lifetime(8, 2_000);
        assert_eq!(w.bucket_count(), 1);
        assert!(
            w.due(1_500).is_empty(),
            "the re-armed-away deadline must not fire"
        );
        assert_eq!(w.due(2_000), vec![Due::Close4202(8)]);
    }

    #[test]
    fn timeline_slots_are_exactly_the_armed_timers() {
        // Global invariant at rest: every timeline slot is the connection's
        // CURRENT timer of its kind (one liveness + at most one lifetime + at
        // most one handshake per connection), and every armed timer has its
        // slot — a mixed workload of re-arms, establishes and disconnects
        // leaves no strays and no unbacked arms.
        let mut w = TimerWheel::new();
        for conn in 0..30usize {
            w.touch(conn, (conn * 7) as u64);
            if conn % 3 == 0 {
                w.arm_lifetime(conn, 500_000 + conn as u64);
            }
            if conn % 4 != 0 {
                w.arm_handshake(conn, 400_000 + conn as u64);
            }
            if conn % 2 == 0 {
                w.remove(conn); // early disconnect mid-flight
            } else {
                for k in 1..=20u64 {
                    w.touch(conn, (conn * 7) as u64 + k * 13); // stays chatty
                }
            }
        }
        assert_eq!(
            w.slot_count(),
            w.live.len() + w.lifetime.len() + w.handshake.len(),
            "every timeline slot must be backed by a side-table entry"
        );
        for (deadline, slots) in &w.timeline {
            for s in slots {
                match s.kind {
                    Kind::Idle | Kind::Pong => {
                        let t = w.live[&s.conn];
                        assert_eq!(t.deadline_ms, *deadline);
                        assert_eq!(t.kind, s.kind);
                    }
                    Kind::Lifetime => assert_eq!(w.lifetime[&s.conn], *deadline),
                    Kind::Handshake => assert_eq!(w.handshake[&s.conn], *deadline),
                }
            }
        }
        // The survivors fire exactly at their current deadlines: odd conns
        // last touched at conn*7 + 260, so every idle deadline ≤ 120_463.
        let expected: Vec<Due> = (1..30).filter(|c| c % 2 == 1).map(Due::Ping).collect();
        assert_eq!(w.due(121_000), expected);
    }
}
