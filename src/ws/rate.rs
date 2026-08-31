//! Per-connection token-bucket rate limiter for client events (Pusher: 10
//! client events/sec/connection). `limit == 0` means unlimited.
//!
//! The bucket holds at most `limit` tokens (the bounded burst) and refills
//! continuously at `limit` tokens/sec, computed from the elapsed time at each
//! check — O(1), no background refill task. Unlike the fixed 1-second window
//! this replaces, a client cannot double-spend at a window edge (10 events
//! late in one window + 10 more early in the next): after the burst the
//! bucket is empty and only the elapsed-time refill is spendable.

use std::time::Instant;

/// Token bucket: capacity `limit` tokens, refill rate `limit` tokens/sec.
/// The bucket is born full at the first check (lazy initialization — no
/// retroactive penalty for the connection's idle lifetime).
#[derive(Debug)]
pub struct RateWindow {
    limit: u32,
    /// Fractional tokens currently available. Accurate for this purpose:
    /// refills are `elapsed_secs * limit` with f64, clamped to capacity on
    /// every check, so rounding error cannot accumulate past the clamp.
    tokens: f64,
    /// Instant of the last check (when refill was last accrued).
    last: Option<Instant>,
}

impl RateWindow {
    pub fn new(limit: u32) -> Self {
        Self {
            limit,
            tokens: limit as f64,
            last: None,
        }
    }

    /// Record one event observed at `now`. Returns true if ALLOWED (one token
    /// was available and is spent), false if the bucket is short of a token.
    pub fn check_at(&mut self, now: Instant) -> bool {
        if self.limit == 0 {
            return true; // unlimited / disabled
        }
        let cap = f64::from(self.limit);
        match self.last {
            // First event ever: the bucket starts full and spends one token.
            None => {
                self.last = Some(now);
                self.tokens = cap - 1.0;
                true
            }
            Some(prev) => {
                let elapsed = now.saturating_duration_since(prev).as_secs_f64();
                self.tokens = (self.tokens + elapsed * cap).min(cap);
                self.last = Some(now);
                if self.tokens >= 1.0 {
                    self.tokens -= 1.0;
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Production entry point: checks against the real clock.
    pub fn check(&mut self) -> bool {
        self.check_at(Instant::now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Updated for the token bucket (was: fixed-window "still same window"
    /// rejection at +999ms). At limit 3 the bucket refills 3 tokens/s, so an
    /// event 999ms after the burst HAS legitimately earned ~3 tokens and must
    /// pass — the old assertion pinned the fixed-window semantics, not the
    /// documented 3/s contract. The intent that survives: a 4th event fired
    /// almost immediately after the limit is reached is rejected.
    #[test]
    fn allows_up_to_limit_then_rejects_the_immediate_fourth() {
        let base = Instant::now();
        let mut w = RateWindow::new(3);
        assert!(w.check_at(base));
        assert!(w.check_at(base));
        assert!(w.check_at(base));
        assert!(
            !w.check_at(base),
            "4th event in the same instant is rejected"
        );
        assert!(
            !w.check_at(base + Duration::from_millis(200)),
            "+200ms refills only 0.6 of a token — still rejected"
        );
    }

    #[test]
    fn window_resets_after_one_second() {
        let base = Instant::now();
        let mut w = RateWindow::new(2);
        assert!(w.check_at(base));
        assert!(w.check_at(base));
        assert!(!w.check_at(base));
        // One full second later → fresh window.
        let later = base + Duration::from_secs(1);
        assert!(w.check_at(later));
        assert!(w.check_at(later));
        assert!(!w.check_at(later));
    }

    #[test]
    fn zero_limit_is_unlimited() {
        let base = Instant::now();
        let mut w = RateWindow::new(0);
        for _ in 0..1000 {
            assert!(w.check_at(base));
        }
    }

    // ── F13: true sustained 10/s with a bounded burst (token bucket) ─────────

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// The mandated edge case, verbatim: 10 events at t=0.999 + 1 more at
    /// t=1.001 → the 11th is REJECTED. 2ms after a full burst only 0.02
    /// tokens have refilled — far short of the 1 token the event costs.
    #[test]
    fn ten_at_0_999_plus_one_at_1_001_rejects_the_eleventh() {
        let base = Instant::now();
        let mut w = RateWindow::new(10);
        for _ in 0..10 {
            assert!(w.check_at(base + ms(999)), "first 10 at t=0.999 allowed");
        }
        assert!(
            !w.check_at(base + ms(1001)),
            "11th event 2ms after a full burst must be rejected"
        );
    }

    /// THE 2× window-edge flaw (RED against the fixed window): the first
    /// event anchors the window at t=0.000; 9 more at t=0.999 fill it to the
    /// limit. At t=1.001 the fixed window RESETS and admits 10 MORE events —
    /// up to 20 events within 1.001s. A token bucket instead carries ~1 token
    /// across the edge (2ms of refill): exactly ONE event at t=1.001 passes
    /// and the next (the 12th overall, 2ms after the burst) is rejected.
    #[test]
    fn window_edge_double_burst_is_capped_at_one_refilled_token() {
        let base = Instant::now();
        let mut w = RateWindow::new(10);
        assert!(w.check_at(base), "anchor event at t=0.000");
        for _ in 0..9 {
            assert!(
                w.check_at(base + ms(999)),
                "9 more at t=0.999 fill the budget"
            );
        }
        // Second burst 2ms later, on the other side of the 1s edge.
        let mut allowed = 0;
        for _ in 0..10 {
            if w.check_at(base + ms(1001)) {
                allowed += 1;
            }
        }
        assert_eq!(
            allowed, 1,
            "only the single token refilled in 2ms may cross the window edge"
        );
    }

    /// Sustained exactly-10/s for 5 virtual seconds: every event must pass.
    /// Pins continuous fractional refill: each 100ms interval must have
    /// refilled a full token by the time the next event arrives.
    #[test]
    fn sustained_ten_per_second_for_five_virtual_seconds_all_pass() {
        let base = Instant::now();
        let mut w = RateWindow::new(10);
        for i in 0..50u64 {
            let t = base + ms(i * 100);
            assert!(
                w.check_at(t),
                "event {} at t={}ms must pass",
                i + 1,
                i * 100
            );
        }
    }

    /// Idle 1s → the full burst of 10 is allowed again (capacity fully
    /// refilled), and the 11th is still rejected.
    #[test]
    fn idle_one_second_restores_full_burst() {
        let base = Instant::now();
        let mut w = RateWindow::new(10);
        for _ in 0..10 {
            assert!(w.check_at(base));
        }
        assert!(!w.check_at(base), "11th at t=0 rejected");
        let later = base + Duration::from_secs(1);
        for _ in 0..10 {
            assert!(w.check_at(later), "full burst allowed after 1s idle");
        }
        assert!(!w.check_at(later), "11th at t=1 rejected again");
    }

    /// Partial refill arithmetic: drain the bucket with 10 events at t=0,
    /// idle 0.5s → exactly 5 tokens refilled (10/s × 0.5s): five events at
    /// t=0.5 pass, the sixth does not. Pins the refill rate in both
    /// directions (≥5 spendable, <6 spendable).
    #[test]
    fn partial_refill_half_second_refills_exactly_five_tokens() {
        let base = Instant::now();
        let mut w = RateWindow::new(10);
        for _ in 0..10 {
            assert!(w.check_at(base));
        }
        let half = base + ms(500);
        for _ in 0..5 {
            assert!(w.check_at(half), "5 refilled tokens spendable at t=0.5");
        }
        assert!(
            !w.check_at(half),
            "6th event at t=0.5 rejected: exactly 5 tokens refilled, not 6"
        );
    }

    /// Fractional accounting below one token: after a full drain, 50ms of
    /// idle refills exactly 0.5 tokens → the next event is REJECTED; at
    /// exactly 100ms a full token has refilled → ALLOWED. (The fixed window
    /// wrongly rejects the 100ms event: 10/s sustained means a token every
    /// 100ms.)
    #[test]
    fn fractional_refill_rejects_at_50ms_allows_at_exactly_100ms() {
        let base = Instant::now();
        let mut w = RateWindow::new(10);
        for _ in 0..10 {
            assert!(w.check_at(base));
        }
        assert!(
            !w.check_at(base + ms(50)),
            "0.5 tokens refilled is not enough for one event"
        );
        assert!(
            w.check_at(base + ms(100)),
            "exactly 1.0 token refilled at t=0.1 must be spendable"
        );
    }

    /// Decided boundary (t=1.000): 10 events at t=0.000 then 1 at EXACTLY
    /// t=1.000 → ALLOWED. A full second refills exactly 10.0 tokens, so the
    /// bucket is full and the event is admitted. Pinned so rounding can never
    /// flip the boundary (e.g. a floor()/integer-token scheme would reject).
    #[test]
    fn exactly_one_second_boundary_refills_the_bucket_completely() {
        let base = Instant::now();
        let mut w = RateWindow::new(10);
        for _ in 0..10 {
            assert!(w.check_at(base));
        }
        assert!(
            w.check_at(base + Duration::from_secs(1)),
            "t=1.000 boundary falls on the ALLOW side: 1.000s × 10/s = 10.0 tokens"
        );
        // Just inside the boundary the bucket is also fully refilled.
        assert!(
            w.check_at(base + Duration::from_secs(1) + ms(999)),
            "0.999s more of refill keeps events flowing"
        );
    }

    /// The burst can never exceed 10 no matter how long the idle: after 60s
    /// of idle the bucket is still capped at 10 — the 11th back-to-back
    /// event is rejected.
    #[test]
    fn burst_cap_never_exceeds_ten_no_matter_how_long_the_idle() {
        let base = Instant::now();
        let mut w = RateWindow::new(10);
        for _ in 0..10 {
            assert!(w.check_at(base));
        }
        let much_later = base + Duration::from_secs(60);
        for _ in 0..10 {
            assert!(w.check_at(much_later), "10 events after long idle ok");
        }
        assert!(
            !w.check_at(much_later),
            "capacity is capped at 10; idle cannot buy an 11th burst event"
        );
    }

    /// Lazy initialization: the first event at a large t starts with a FULL
    /// bucket — no retroactive penalty for the connection's idle lifetime.
    #[test]
    fn first_event_at_large_t_initializes_a_full_bucket() {
        let base = Instant::now();
        let mut w = RateWindow::new(10);
        let late = base + Duration::from_secs(3600);
        for _ in 0..10 {
            assert!(w.check_at(late), "first 10 events at t=3600s allowed");
        }
        assert!(!w.check_at(late), "11th at t=3600s rejected");
    }

    /// No drift: exactly 10/s on a 100ms grid for 100 virtual seconds
    /// (1000 events) all pass, and the bucket's remaining balance is intact
    /// afterwards (9 full tokens — a downward-drifting accumulator would
    /// ratchet below that and fail the trailing drain).
    #[test]
    fn sustained_ten_per_second_does_not_drift_over_100_virtual_seconds() {
        let base = Instant::now();
        let mut w = RateWindow::new(10);
        for i in 0..1000u64 {
            let t = base + ms(i * 100);
            assert!(
                w.check_at(t),
                "event {} at t={}ms must pass",
                i + 1,
                i * 100
            );
        }
        // The 1000th event left ~9 tokens; drain exactly 9 more, then the
        // next must fail. A fractional accumulator that leaks even 1e-9 per
        // step would have lost ~1e-6 — still 9 spendable — so also assert
        // the drain rejects at exactly the 10th extra event.
        let end = base + ms(1000 * 100 - 100);
        for i in 0..9u64 {
            assert!(
                w.check_at(end + ms(i)),
                "drain event {} at the last virtual instant",
                i + 1
            );
        }
        assert!(
            !w.check_at(end + ms(9)),
            "bucket must hold exactly ~9 tokens after sustained 10/s, no drift"
        );
    }
}
