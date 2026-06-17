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

pub async fn publish_openloop(
    rest: String,
    app_id: String,
    key: String,
    secret: String,
    channels: Vec<String>,
    target_rate: u64,
    max_inflight: usize,
    secs: u64,
    counters: Arc<Counters>,
) -> OpenLoopResult {
    let pubr = Arc::new(Publisher::new(rest, app_id, key, secret));
    let sem = Arc::new(Semaphore::new(max_inflight));
    let attempted = Arc::new(AtomicU64::new(0));
    let succeeded = Arc::new(AtomicU64::new(0));
    let blocked = Arc::new(AtomicU64::new(0));
    let interval = Duration::from_secs_f64(1.0 / target_rate.max(1) as f64);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let end = Instant::now() + Duration::from_secs(secs);
    let epoch = Instant::now();
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
        let (p, ch) = (pubr.clone(), channels[(seq as usize) % channels.len()].clone());
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
    // let in-flight drain
    let _ = Arc::clone(&sem).acquire_many_owned(max_inflight as u32).await;
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
