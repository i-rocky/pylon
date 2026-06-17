//! Throughput-ceiling sweep: open-loop publish rate ramp across many channels.
//!
//! Ramps publish rate from `rate_start` by `rate_step` each step, measuring
//! deliveries/s, drop%, p99 latency, and CPU until a stop condition trips.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::cli::Cli;
use crate::scenario::{wait_subscribed, Harness};

use super::child::PylonChild;
use super::openloop::publish_openloop;

// ── Public types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TputStep {
    pub rate: u64,
    pub delivered_per_s: u64,
    pub drop_pct: f64,
    pub p50_ms: u64,
    pub p99_ms: u64,
    pub cpu_busy_pct: f64,
    /// Per-core busy% sampled via mpstat during the publish window (empty if mpstat absent).
    pub per_core_busy: Vec<f64>,
}

#[derive(Debug, Clone)]
pub struct TputCeiling {
    pub best: TputStep,
    pub steps: Vec<TputStep>,
    pub stop_reason: TputStop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TputStop {
    Drops,
    LatencyBudget,
    CpuSaturated,
    MaxRate,
}

/// Options for the throughput-ceiling rate ramp.
pub struct RateRampOpts {
    pub url: String,
    pub rest: String,
    pub key: String,
    pub secret: String,
    pub tput_conns: usize,
    pub channels: usize,
    pub rate_start: u64,
    pub rate_step: u64,
    pub max_rate: u64,
    pub p99_budget_ms: u64,
    pub max_inflight: usize,
    pub step_secs: u64,
    pub server_cores: usize,
    pub client_ips: Vec<String>,
}

// ── Stop predicate ──────────────────────────────────────────────────────────

/// Pure stop predicate for the rate-ramp loop. Priority order:
/// 1. drop_pct > 0.1  → Drops
/// 2. p99_ms > p99_budget → LatencyBudget
/// 3. cpu_busy_pct >= 95.0 → CpuSaturated
/// 4. max_rate > 0 && rate >= max_rate → MaxRate
/// else None
pub fn tput_should_stop(
    drop_pct: f64,
    p99_ms: u64,
    p99_budget: u64,
    cpu_busy_pct: f64,
    rate: u64,
    max_rate: u64,
) -> Option<TputStop> {
    if drop_pct > 0.1 {
        return Some(TputStop::Drops);
    }
    if p99_ms > p99_budget {
        return Some(TputStop::LatencyBudget);
    }
    if cpu_busy_pct >= 95.0 {
        return Some(TputStop::CpuSaturated);
    }
    if max_rate > 0 && rate >= max_rate {
        return Some(TputStop::MaxRate);
    }
    None
}

// ── Async run ───────────────────────────────────────────────────────────────

/// Run the throughput-ceiling sweep. Subscribes `opts.tput_conns` across
/// `opts.channels` channels once, then ramps the publish rate, measuring each
/// step's deliveries/s, drop%, p99 latency, and CPU utilisation until a stop
/// condition trips.
pub async fn run(child: &PylonChild, opts: &RateRampOpts) -> TputCeiling {
    // Build a Cli that matches the opts (used only for client configuration).
    let cli = Cli {
        url: opts.url.clone(),
        url_b: None,
        rest: opts.rest.clone(),
        app_id: "app".into(),
        key: opts.key.clone(),
        secret: opts.secret.clone(),
        scenario: crate::cli::Scenario::Channels,
        conns: opts.tput_conns,
        channels: opts.channels,
        rate: opts.rate_start,
        publishers: 1,
        secs: opts.step_secs,
        ramp_per_sec: 2000,
        private: false,
        server_pid: None,
        client_ips: opts.client_ips.clone(),
    };

    let epoch = Instant::now();
    let mut h = Harness::new(epoch);

    // Subscribe tput_conns across channels.
    let ch_count = opts.channels.max(1);
    h.spawn_clients(&cli, &opts.url, opts.tput_conns, |i| {
        format!("bench-{}", i % ch_count)
    })
    .await;
    wait_subscribed(&h.counters, opts.tput_conns as u64, Duration::from_secs(60)).await;

    let channels_vec: Vec<String> =
        (0..ch_count).map(|i| format!("bench-{i}")).collect();

    let recipients_per_event = (opts.tput_conns / ch_count).max(1) as u64;

    // Rate ramp loop.
    let mut rate = opts.rate_start;
    let mut steps: Vec<TputStep> = Vec::new();
    let mut last_clean: Option<TputStep> = None;
    let final_stop;

    loop {
        // --- per-step baseline snapshots ---
        h.lat.reset();
        let rec0 = h.counters.received.load(Ordering::Relaxed);
        let (u0, s0) = child.cpu_ticks().unwrap_or((0, 0));

        // Run open-loop publisher concurrently with mpstat so the CPU sample
        // covers the active publish window rather than the idle settle period.
        let mpstat_fut = super::mpstat::sample(1, opts.step_secs.max(1));
        let (r, cpu_sample) = tokio::join!(
            publish_openloop(
                opts.rest.clone(),
                "app".into(),
                opts.key.clone(),
                opts.secret.clone(),
                channels_vec.clone(),
                rate,
                opts.max_inflight,
                opts.step_secs,
                h.counters.clone(),
            ),
            mpstat_fut
        );

        // Sample CPU at the END of the active publish window (before the settle below),
        // so cpu_busy_pct reflects load during step_secs, not the idle drain.
        let (u1, s1) = child.cpu_ticks().unwrap_or((u0, s0));

        // Settle: `publish_openloop` only drained the in-flight HTTP publishes; the
        // server-side N-way fan-out they triggered is still landing on the subscriber
        // sockets. Wait for it before counting deliveries, otherwise drop% is
        // over-reported and the sweep trips `Drops` prematurely — under-reporting the
        // true throughput ceiling.
        tokio::time::sleep(Duration::from_millis(800)).await;

        // --- post-step measurements ---
        let rec1 = h.counters.received.load(Ordering::Relaxed);
        let delivered = rec1 - rec0;
        let delivered_per_s = delivered / opts.step_secs.max(1);

        let expected = r.attempted * recipients_per_event;
        let drop_pct = if expected > 0 {
            100.0 * (expected.saturating_sub(delivered)) as f64 / expected as f64
        } else {
            0.0
        };

        let (_, p50_us, p99_us, _, _) = h.lat.summary_us();
        let (p50_ms, p99_ms) = (p50_us / 1000, p99_us / 1000);

        // CPU: delta ticks / USER_HZ / elapsed_secs / server_cores → busy%
        // (u1/s1 were sampled at the end of the active window, above).
        let cores_used =
            ((u1 + s1).saturating_sub(u0 + s0)) as f64 / 100.0 / opts.step_secs.max(1) as f64;
        let cpu_busy_pct = if opts.server_cores > 0 {
            100.0 * cores_used / opts.server_cores as f64
        } else {
            0.0
        };

        let per_core_busy = cpu_sample.map(|c| c.per_core_busy).unwrap_or_default();
        let step = TputStep {
            rate,
            delivered_per_s,
            drop_pct,
            p50_ms,
            p99_ms,
            cpu_busy_pct,
            per_core_busy,
        };
        steps.push(step.clone());

        let stop = tput_should_stop(
            drop_pct,
            p99_ms,
            opts.p99_budget_ms,
            cpu_busy_pct,
            rate,
            opts.max_rate,
        );

        if let Some(reason) = stop {
            // If the stop is MaxRate, the last step was still clean → best is last step.
            if reason == TputStop::MaxRate {
                last_clean = Some(step);
            }
            final_stop = reason;
            break;
        }

        last_clean = Some(step);
        rate += opts.rate_step;
    }

    // best = last clean step; fall back to steps[0] if every step tripped.
    let best = last_clean.unwrap_or_else(|| steps[0].clone());

    h.drain().await;

    TputCeiling { best, steps, stop_reason: final_stop }
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stops_on_drops() {
        assert!(matches!(tput_should_stop(0.5, 10, 100, 50.0, 1000, 0), Some(TputStop::Drops)));
    }
    #[test]
    fn stops_on_latency() {
        assert!(matches!(
            tput_should_stop(0.0, 150, 100, 50.0, 1000, 0),
            Some(TputStop::LatencyBudget)
        ));
    }
    #[test]
    fn stops_on_cpu() {
        assert!(matches!(
            tput_should_stop(0.0, 10, 100, 96.0, 1000, 0),
            Some(TputStop::CpuSaturated)
        ));
    }
    #[test]
    fn keeps_going() {
        assert!(tput_should_stop(0.05, 20, 100, 50.0, 500, 0).is_none());
    }
}
