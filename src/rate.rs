#[derive(Debug)]
pub struct TokenBucket {
    rate_per_second: u32,
    rate: f64,
    capacity: f64,
    tokens: f64,
    last_ns: Option<u64>,
}

impl TokenBucket {
    pub fn new(rate_per_second: u32, burst: u32) -> Self {
        let capacity = f64::from(burst.max(rate_per_second));
        Self {
            rate_per_second,
            rate: f64::from(rate_per_second),
            capacity,
            tokens: capacity,
            last_ns: None,
        }
    }

    pub fn rate_per_second(&self) -> u32 {
        self.rate_per_second
    }

    fn refilled_at_ns(&self, now_ns: u64) -> f64 {
        match self.last_ns {
            None => self.capacity,
            Some(prev) => {
                let elapsed = now_ns.saturating_sub(prev) as f64 / 1_000_000_000.0;
                (self.tokens + elapsed * self.rate).min(self.capacity)
            }
        }
    }

    pub fn take_at_ns(&mut self, now_ns: u64, cost: u32) -> bool {
        if self.rate_per_second == 0 {
            return true;
        }
        let cost = f64::from(cost);
        if cost > self.capacity {
            return false;
        }
        let tokens = self.refilled_at_ns(now_ns);
        self.last_ns = Some(now_ns);
        if tokens >= cost {
            self.tokens = tokens - cost;
            true
        } else {
            self.tokens = tokens;
            false
        }
    }

    pub fn remaining_at_ns(&self, now_ns: u64) -> u32 {
        if self.rate_per_second == 0 {
            return u32::MAX;
        }
        self.refilled_at_ns(now_ns) as u32
    }

    pub fn retry_after_secs_at_ns(&self, now_ns: u64, cost: u32) -> u64 {
        if self.rate_per_second == 0 {
            return 0;
        }
        let deficit = f64::from(cost) - self.refilled_at_ns(now_ns);
        if deficit <= 0.0 {
            return 1;
        }
        ((deficit / self.rate).ceil() as u64).max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000;

    #[test]
    fn burst_is_independent_of_rate() {
        let mut b = TokenBucket::new(100, 250);
        for i in 0..250 {
            assert!(b.take_at_ns(0, 1), "burst token {i} must be spendable");
        }
        assert!(
            !b.take_at_ns(0, 1),
            "the 251st take at t=0 exceeds the burst"
        );
        assert!(b.take_at_ns(10 * MS, 1), "10ms refills 1.0 token at 100/s");
    }

    #[test]
    fn a_cost_larger_than_capacity_is_always_refused() {
        let mut b = TokenBucket::new(10, 10);
        assert!(!b.take_at_ns(0, 11));
        assert!(
            b.take_at_ns(0, 10),
            "the refusal must not have spent anything"
        );
    }

    #[test]
    fn a_zero_rate_is_unlimited() {
        let mut b = TokenBucket::new(0, 0);
        for _ in 0..10_000 {
            assert!(b.take_at_ns(0, 9_999));
        }
        assert_eq!(b.retry_after_secs_at_ns(0, 1), 0);
        assert_eq!(b.remaining_at_ns(0), u32::MAX);
    }

    #[test]
    fn retry_after_is_whole_seconds_rounded_up_with_a_floor_of_one() {
        let mut b = TokenBucket::new(10, 10);
        assert!(b.take_at_ns(0, 10));
        assert!(!b.take_at_ns(0, 1));
        assert_eq!(
            b.retry_after_secs_at_ns(0, 1),
            1,
            "0.1s of deficit rounds up to the 1s floor"
        );
        assert!(!b.take_at_ns(0, 10));
        assert_eq!(
            b.retry_after_secs_at_ns(0, 10),
            1,
            "10 tokens at 10/s is exactly 1s"
        );
        let mut slow = TokenBucket::new(1, 10);
        assert!(slow.take_at_ns(0, 10));
        assert_eq!(
            slow.retry_after_secs_at_ns(0, 4),
            4,
            "4 tokens at 1/s is 4s"
        );
    }

    #[test]
    fn remaining_reports_whole_spendable_tokens() {
        let mut b = TokenBucket::new(10, 10);
        assert_eq!(b.remaining_at_ns(0), 10);
        assert!(b.take_at_ns(0, 3));
        assert_eq!(b.remaining_at_ns(0), 7);
        assert_eq!(
            b.remaining_at_ns(50 * MS),
            7,
            "0.5 of a token is not spendable and must not round up"
        );
    }

    #[test]
    fn time_going_backwards_accrues_no_refill() {
        let mut b = TokenBucket::new(10, 10);
        assert!(b.take_at_ns(1_000 * MS, 10));
        assert!(!b.take_at_ns(0, 1), "an earlier instant must not refill");
    }
}
