use super::{wait_subscribed, Harness};
use crate::cli::Cli;
use crate::metrics::sample_proc;
use crate::pusher::{stamp_payload, Publisher};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// S1: ramp to N idle subscribers on one channel, hold, fire ONE broadcast, measure
/// its fan-out latency + report peak RSS and RSS/conn.
pub async fn run(cli: &Cli) -> anyhow::Result<()> {
    let epoch = Instant::now();
    let mut h = Harness::new(epoch);
    let channel = "bench-connect".to_string();
    let ch = channel.clone();
    let t0 = Instant::now();
    h.spawn_clients(cli, &cli.url, cli.conns, move |_| ch.clone()).await;
    wait_subscribed(&h.counters, cli.conns as u64, Duration::from_secs(300)).await;
    let ramp_secs = t0.elapsed().as_secs_f64();
    let established = h.counters.subscribed.load(Ordering::Relaxed);
    eprintln!("reached {established} subscribed in {ramp_secs:.1}s");

    // sample RSS now (steady plateau), then a single broadcast
    let proc = if let Some(pid) = cli.server_pid {
        sample_proc(pid, Duration::from_secs(2)).await
    } else {
        None
    };

    let pubr = Publisher::new(cli.rest.clone(), cli.app_id.clone(), cli.key.clone(), cli.secret.clone());
    let payload = stamp_payload(0, epoch.elapsed().as_nanos());
    pubr.publish(&channel, "bench", &payload, crate::pusher::unix_now()).await?;
    tokio::time::sleep(Duration::from_secs(2)).await; // let the fan-out land

    let c = &h.counters;
    let (count, p50, p99, p999, max) = h.lat.summary_us();
    println!("=== scenario: connect ===");
    println!("target={} connected={} subscribed={} ramp={:.1}s",
        cli.conns, c.connected.load(Ordering::Relaxed), established, ramp_secs);
    println!("single-broadcast fan-out: received={} latency µs p50={p50} p99={p99} p99.9={p999} max={max} (count={count})",
        c.received.load(Ordering::Relaxed));
    if let Some((rss_mb, cpu)) = proc {
        let per_conn_kb = (rss_mb * 1024).checked_div(established).unwrap_or(0);
        println!("server: peak_rss={rss_mb}MB ({per_conn_kb}KB/conn) cpu={cpu:.1}%");
    }
    h.drain().await;
    Ok(())
}
