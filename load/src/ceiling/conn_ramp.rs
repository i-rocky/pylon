//! Connection-ceiling sweep: ramp idle subscribers until memory or backpressure trips.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use super::child::PylonChild;
use super::spec::{effective_mem_bytes, BoxSpec};
use crate::cli::Cli;
use crate::scenario::{wait_subscribed, Harness};

// ── public types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    MemCeiling,
    ConnectFailures,
    MaxConns,
    Backpressure,
}

#[derive(Debug, Clone)]
pub struct ConnCeiling {
    pub max_conns: u64,
    pub rss_bytes_at_max: u64,
    pub bytes_per_conn: u64,
    pub conns_per_gb: u64,
    pub stop_reason: StopReason,
}

#[derive(Debug, Clone)]
pub struct ConnRampOpts {
    pub url: String,
    pub key: String,
    pub secret: String,
    pub conn_batch: usize,
    pub max_conns: usize,
    pub mem_ceiling_pct: u8,
    pub client_ips: Vec<String>,
    pub fail_threshold: u64,
}

// ── pure stop-condition predicate ───────────────────────────────────────────

/// Evaluate stop conditions in priority order.
/// Returns `Some(reason)` when the sweep should stop, `None` to keep ramping.
pub fn should_stop(
    rss: u64,
    mem_ceiling: u64,
    connect_failed: u64,
    fail_threshold: u64,
    conns: u64,
    max_conns: u64,
) -> Option<StopReason> {
    if rss >= mem_ceiling {
        return Some(StopReason::MemCeiling);
    }
    if connect_failed >= fail_threshold {
        return Some(StopReason::ConnectFailures);
    }
    if max_conns > 0 && conns >= max_conns {
        return Some(StopReason::MaxConns);
    }
    None
}

// ── async sweep loop ─────────────────────────────────────────────────────────

pub async fn run(child: &PylonChild, spec: &BoxSpec, opts: &ConnRampOpts) -> ConnCeiling {
    let rss_idle = child.rss_bytes().unwrap_or(0);
    let mem_ceiling = effective_mem_bytes(spec) * opts.mem_ceiling_pct as u64 / 100;

    // Build a Cli for the Harness — we only need url, key, secret, client_ips, ramp_per_sec.
    let cli = Cli {
        url: opts.url.clone(),
        url_b: None,
        rest: String::new(),
        app_id: String::new(),
        key: opts.key.clone(),
        secret: opts.secret.clone(),
        scenario: crate::cli::Scenario::Connect,
        conns: opts.conn_batch,
        channels: 1,
        rate: 0,
        publishers: 1,
        secs: 0,
        ramp_per_sec: 4000,
        private: false,
        server_pid: None,
        client_ips: opts.client_ips.clone(),
    };

    let mut h = Harness::new(Instant::now());
    let mut total: usize = 0;
    let mut rss: u64 = rss_idle;
    let stop_reason;

    loop {
        // Spawn one batch of subscribers, all on channel "ceil".
        h.spawn_clients(&cli, &opts.url, opts.conn_batch, |_| "ceil".to_string()).await;
        total += opts.conn_batch;

        // Wait for the batch to subscribe (up to 30 s).
        wait_subscribed(&h.counters, total as u64, Duration::from_secs(30)).await;

        // Settle 1 s, then sample.
        tokio::time::sleep(Duration::from_secs(1)).await;

        rss = child.rss_bytes().unwrap_or(rss);
        let connect_failed = h.counters.connect_failed.load(Ordering::Relaxed);

        if let Some(reason) = should_stop(
            rss,
            mem_ceiling,
            connect_failed,
            opts.fail_threshold,
            total as u64,
            opts.max_conns as u64,
        ) {
            stop_reason = reason;
            break;
        }
    }

    let max_conns = h.counters.subscribed.load(Ordering::Relaxed);
    let rss_bytes_at_max = rss;
    let bytes_per_conn = if max_conns > 0 {
        rss_bytes_at_max.saturating_sub(rss_idle) / max_conns
    } else {
        0
    };
    let conns_per_gb = if bytes_per_conn > 0 { (1u64 << 30) / bytes_per_conn } else { 0 };

    h.drain().await;

    ConnCeiling {
        max_conns,
        rss_bytes_at_max,
        bytes_per_conn,
        conns_per_gb,
        stop_reason,
    }
}

// ── unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stops_on_mem_ceiling() {
        assert!(matches!(should_stop(900, 800, 0, 100, 1000, 0), Some(StopReason::MemCeiling)));
    }

    #[test]
    fn stops_on_failures() {
        assert!(matches!(should_stop(100, 800, 200, 100, 1000, 0), Some(StopReason::ConnectFailures)));
    }

    #[test]
    fn stops_on_max_conns() {
        assert!(matches!(should_stop(100, 800, 0, 100, 1000, 1000), Some(StopReason::MaxConns)));
    }

    #[test]
    fn keeps_going() {
        assert!(should_stop(100, 800, 0, 100, 500, 0).is_none());
    }
}
