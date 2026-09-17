use super::{wait_subscribed, Harness};
use crate::cli::Cli;
use crate::pusher::{stamp_payload, Publisher};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// S4: two pylon nodes (redis adapter). Half subscribe to node A, half to node B; publish
/// on node A only → measures cross-node delivery latency (the Redis pub/sub hop).
pub async fn run(cli: &Cli) -> anyhow::Result<()> {
    let url_b = cli
        .url_b
        .clone()
        .ok_or_else(|| anyhow::anyhow!("--url-b required for cluster"))?;
    let epoch = Instant::now();
    let mut h = Harness::new(epoch);
    let half = cli.conns / 2;
    let channel = "bench-cluster".to_string();
    let (cha, chb) = (channel.clone(), channel.clone());
    h.spawn_clients(cli, &cli.url, half, move |_| cha.clone())
        .await;
    h.spawn_clients(cli, &url_b, cli.conns - half, move |_| chb.clone())
        .await;
    wait_subscribed(&h.counters, cli.conns as u64, Duration::from_secs(120)).await;

    // publish only on node A's REST
    let pubr = Publisher::new(
        cli.rest.clone(),
        cli.app_id.clone(),
        cli.key.clone(),
        cli.secret.clone(),
    );
    let interval = Duration::from_secs_f64(1.0 / cli.rate.max(1) as f64);
    let mut ticker = tokio::time::interval(interval);
    let end = Instant::now() + Duration::from_secs(cli.secs);
    let mut seq = 0u64;
    while Instant::now() < end {
        ticker.tick().await;
        let payload = stamp_payload(seq, epoch.elapsed().as_nanos(), &h.run_id);
        if pubr
            .publish(&channel, "bench", &payload, crate::pusher::unix_now())
            .await
            .is_ok()
        {
            h.counters.sent.fetch_add(1, Ordering::Relaxed);
        }
        seq += 1;
    }
    tokio::time::sleep(Duration::from_millis(800)).await;
    // No double-delivery check: received should equal sent * conns (each conn gets each event once).
    let c = &h.counters;
    let (count, p50, p99, p999, max) = h.lat.summary_us();
    println!("=== scenario: cluster ===");
    println!(
        "conns={} (A={half} B={}) sent={} received={} expected={}",
        cli.conns,
        cli.conns - half,
        c.sent.load(Ordering::Relaxed),
        c.received.load(Ordering::Relaxed),
        c.sent.load(Ordering::Relaxed) * cli.conns as u64
    );
    println!("delivery latency µs (mixed same+cross-node): count={count} p50={p50} p99={p99} p99.9={p999} max={max}");
    h.drain().await;
    Ok(())
}
