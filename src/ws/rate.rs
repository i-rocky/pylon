//! Per-connection fixed-1-second-window rate counter for client events
//! (Pusher: 10 client events/sec/connection). `limit == 0` means unlimited.

use std::time::{Duration, Instant};

/// Fixed 1-second window. Counts events; the first event after a full second
/// (or the first event ever) starts a fresh window.
#[derive(Debug)]
pub struct RateWindow {
    limit: u32,
    window_start: Option<Instant>,
    count: u32,
}

impl RateWindow {
    pub fn new(limit: u32) -> Self {
        Self {
            limit,
            window_start: None,
            count: 0,
        }
    }

    /// Record one event observed at `now`. Returns true if ALLOWED, false if it
    /// exceeds the per-second limit.
    pub fn check_at(&mut self, now: Instant) -> bool {
        if self.limit == 0 {
            return true; // unlimited / disabled
        }
        let reset = match self.window_start {
            None => true,
            Some(start) => now.duration_since(start) >= Duration::from_secs(1),
        };
        if reset {
            self.window_start = Some(now);
            self.count = 0;
        }
        self.count += 1;
        self.count <= self.limit
    }

    /// Production entry point: checks against the real clock.
    pub fn check(&mut self) -> bool {
        self.check_at(Instant::now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_up_to_limit_then_rejects_within_window() {
        let base = Instant::now();
        let mut w = RateWindow::new(3);
        assert!(w.check_at(base));
        assert!(w.check_at(base));
        assert!(w.check_at(base));
        assert!(
            !w.check_at(base),
            "4th event in the same window is rejected"
        );
        assert!(
            !w.check_at(base + Duration::from_millis(999)),
            "still same window"
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
}
