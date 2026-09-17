use hdrhistogram::Histogram;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Thread-safe latency recorder (nanoseconds), reported in microseconds.
pub struct Latency {
    hist: Mutex<Histogram<u64>>,
}

impl Default for Latency {
    fn default() -> Self {
        // 1µs..60s, 3 significant figures.
        Self {
            hist: Mutex::new(Histogram::<u64>::new_with_bounds(1_000, 60_000_000_000, 3).unwrap()),
        }
    }
}

impl Latency {
    pub fn record_nanos(&self, nanos: u64) {
        let mut h = self.hist.lock().unwrap();
        h.saturating_record(nanos);
    }
    /// (count, p50_us, p99_us, p999_us, max_us)
    pub fn summary_us(&self) -> (u64, u64, u64, u64, u64) {
        let h = self.hist.lock().unwrap();
        (
            h.len(),
            h.value_at_quantile(0.50) / 1000,
            h.value_at_quantile(0.99) / 1000,
            h.value_at_quantile(0.999) / 1000,
            h.max() / 1000,
        )
    }
    /// Reset the histogram so the next step starts with a fresh measurement window.
    pub fn reset(&self) {
        self.hist.lock().unwrap().clear();
    }
}

#[derive(Default)]
pub struct Counters {
    pub connected: AtomicU64,
    pub connect_failed: AtomicU64,
    pub subscribed: AtomicU64,
    pub sent: AtomicU64,
    pub received: AtomicU64,
    pub received_foreign: AtomicU64,
}

impl Counters {
    pub fn inc(field: &AtomicU64) {
        field.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_quantiles() {
        let l = Latency::default();
        for _ in 0..1000 {
            l.record_nanos(2_000_000); // 2ms
        }
        let (count, p50, _p99, _p999, max) = l.summary_us();
        assert_eq!(count, 1000);
        assert!((1900..=2100).contains(&p50), "p50={p50}");
        assert!(max >= 1900);
    }

    #[test]
    fn counters_count() {
        let c = Counters::default();
        Counters::inc(&c.received);
        Counters::inc(&c.received);
        assert_eq!(c.received.load(Ordering::Relaxed), 2);
    }
}

/// Parse VmRSS (kB) from the contents of /proc/<pid>/status.
pub fn parse_rss_kb(status: &str) -> Option<u64> {
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.split_whitespace().next()?.parse().ok();
        }
    }
    None
}

/// Parse (utime, stime) clock ticks from the contents of /proc/<pid>/stat.
/// The comm field is in parens and may contain spaces/parens; everything after the
/// last ')' is space-delimited. After comm: index 0 = state; utime = index 11, stime = 12.
pub fn parse_cpu_ticks(stat: &str) -> Option<(u64, u64)> {
    let close = stat.rfind(')')?;
    let rest = &stat[close + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    let utime = f.get(11)?.parse().ok()?;
    let stime = f.get(12)?.parse().ok()?;
    Some((utime, stime))
}

#[cfg(test)]
mod proc_tests {
    use super::*;

    #[test]
    fn rss_parse() {
        let s = "Name:\tpylon\nVmRSS:\t  123456 kB\nThreads:\t8\n";
        assert_eq!(parse_rss_kb(s), Some(123456));
    }

    #[test]
    fn cpu_parse_handles_parens_in_comm() {
        let stat = "1234 (py lon) S 1 1234 1234 0 -1 0 0 0 0 0 100 250 0 0 20 0 8 0 99 0";
        assert_eq!(parse_cpu_ticks(stat), Some((100, 250)));
    }
}

use std::time::Duration;

/// Sample a server process's peak RSS (MB) and mean CPU% over `dur`, polling every 250ms.
/// Returns (peak_rss_mb, mean_cpu_percent). Reads /proc/<pid>/{status,stat}.
pub async fn sample_proc(pid: u32, dur: Duration) -> Option<(u64, f64)> {
    let ticks_per_sec = 100.0; // USER_HZ on Linux x86_64
    let read = |p: &str| std::fs::read_to_string(format!("/proc/{pid}/{p}")).ok();
    let (mut u0, mut s0) = parse_cpu_ticks(&read("stat")?)?;
    let mut peak_rss = 0u64;
    let steps = (dur.as_millis() / 250).max(1);
    let mut total_cpu = 0.0;
    for _ in 0..steps {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if let Some(rss) = read("status").and_then(|s| parse_rss_kb(&s)) {
            peak_rss = peak_rss.max(rss / 1024);
        }
        if let Some((u1, s1)) = read("stat").and_then(|s| parse_cpu_ticks(&s)) {
            let dticks = (u1 + s1).saturating_sub(u0 + s0) as f64;
            total_cpu += dticks / ticks_per_sec / 0.250 * 100.0;
            u0 = u1;
            s0 = s1;
        }
    }
    Some((peak_rss, total_cpu / steps as f64))
}
