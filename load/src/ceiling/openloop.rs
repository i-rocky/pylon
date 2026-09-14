use crate::metrics::Counters;
use crate::pusher::{stamp_payload, unix_now, Publisher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

pub struct OpenLoopResult {
    pub attempted: u64,
    pub succeeded: u64,
    pub inflight_saturated_pct: f64,
}

/// Workload recipe for a single open-loop publish run: target app, channels to
/// spread across, and the rate/concurrency/duration shape of the run.
pub struct OpenLoopConfig {
    pub rest: String,
    pub app_id: String,
    pub key: String,
    pub secret: String,
    pub channels: Vec<String>,
    pub target_rate: u64,
    pub max_inflight: usize,
    pub secs: u64,
}

pub async fn publish_openloop(
    cfg: OpenLoopConfig,
    counters: Arc<Counters>,
    // SHARED epoch — must be the SAME `Instant` the subscriber clients measure latency
    // against (the Harness epoch). Stamping payloads with a publisher-local epoch
    // created after the (multi-second) subscribe phase inflates every measured delivery
    // latency by the subscribe duration, which falsely trips the latency budget.
    epoch: Instant,
) -> OpenLoopResult {
    let OpenLoopConfig {
        rest,
        app_id,
        key,
        secret,
        channels,
        target_rate,
        max_inflight,
        secs,
    } = cfg;
    let pubr = Arc::new(Publisher::new(rest, app_id, key, secret));
    let sem = Arc::new(Semaphore::new(max_inflight));
    let attempted = Arc::new(AtomicU64::new(0));
    let succeeded = Arc::new(AtomicU64::new(0));
    let blocked = Arc::new(AtomicU64::new(0));
    let interval = Duration::from_secs_f64(1.0 / target_rate.max(1) as f64);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let end = Instant::now() + Duration::from_secs(secs);
    let mut seq = 0u64;
    while Instant::now() < end {
        ticker.tick().await;
        // fire-and-forget; bounded by the semaphore (don't await the publish)
        let permit = match Arc::clone(&sem).try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                blocked.fetch_add(1, Ordering::Relaxed);
                continue; // in-flight full → shed this tick
            }
        };
        let (p, ch) = (
            pubr.clone(),
            channels[(seq as usize) % channels.len()].clone(),
        );
        let payload = stamp_payload(seq, epoch.elapsed().as_nanos());
        let (att, ok, c) = (attempted.clone(), succeeded.clone(), counters.clone());
        tokio::spawn(async move {
            let _permit = permit;
            att.fetch_add(1, Ordering::Relaxed);
            if p.publish(&ch, "bench", &payload, unix_now()).await.is_ok() {
                ok.fetch_add(1, Ordering::Relaxed);
                c.sent.fetch_add(1, Ordering::Relaxed);
            }
        });
        seq += 1;
    }
    // let in-flight drain (clamp the permit count explicitly; max_inflight is small in
    // practice but the cast must not silently truncate)
    let _ = Arc::clone(&sem)
        .acquire_many_owned(u32::try_from(max_inflight).unwrap_or(u32::MAX))
        .await;
    let att = attempted.load(Ordering::Relaxed);
    let bl = blocked.load(Ordering::Relaxed);
    OpenLoopResult {
        attempted: att,
        succeeded: succeeded.load(Ordering::Relaxed),
        inflight_saturated_pct: if att + bl == 0 {
            0.0
        } else {
            100.0 * bl as f64 / (att + bl) as f64
        },
    }
}
