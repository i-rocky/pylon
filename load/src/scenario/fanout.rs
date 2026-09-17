use super::{wait_subscribed, Harness};
use crate::cli::Cli;
use crate::metrics::sample_proc;
use crate::pusher::{stamp_payload, Publisher};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// S2: N subscribers on one channel; `publishers` concurrent publishers each push
/// `rate`/sec for `secs`, all fanning out to the same channel.
pub async fn run(cli: &Cli) -> anyhow::Result<()> {
    let epoch = Instant::now();
    let mut h = Harness::new(epoch);
    let channel = "bench-fanout".to_string();
    let ch = channel.clone();
    h.spawn_clients(cli, &cli.url, cli.conns, move |_| ch.clone())
        .await;
    wait_subscribed(&h.counters, cli.conns as u64, Duration::from_secs(60)).await;
    eprintln!(
        "subscribed {} clients",
        h.counters.subscribed.load(Ordering::Relaxed)
    );

    let sampler = cli
        .server_pid
        .map(|pid| tokio::spawn(sample_proc(pid, Duration::from_secs(cli.secs))));

    // Spawn `publishers` concurrent publisher tasks. Each owns its own `Publisher` and
    // ticker (at `rate` events/sec) and fans out to the SAME channel, all sharing the
    // single `sent` counter so the aggregate is the sum across publishers.
    let n_pub = cli.publishers.max(1);
    let interval = Duration::from_secs_f64(1.0 / cli.rate.max(1) as f64);
    let secs = cli.secs;
    let mut pub_tasks = Vec::with_capacity(n_pub);
    for _ in 0..n_pub {
        let pub_ = Publisher::new(
            cli.rest.clone(),
            cli.app_id.clone(),
            cli.key.clone(),
            cli.secret.clone(),
        );
        let channel = channel.clone();
        let counters: Arc<crate::metrics::Counters> = h.counters.clone();
        let run_id = h.run_id.clone();
        pub_tasks.push(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            let end = Instant::now() + Duration::from_secs(secs);
            let mut seq = 0u64;
            while Instant::now() < end {
                ticker.tick().await;
                let payload = stamp_payload(seq, epoch.elapsed().as_nanos(), &run_id);
                if pub_
                    .publish(&channel, "bench", &payload, crate::pusher::unix_now())
                    .await
                    .is_ok()
                {
                    counters.sent.fetch_add(1, Ordering::Relaxed);
                }
                seq += 1;
            }
        }));
    }
    for t in pub_tasks {
        let _ = t.await;
    }
    // allow in-flight deliveries to land
    tokio::time::sleep(Duration::from_millis(500)).await;

    let proc = if let Some(s) = sampler {
        s.await.ok().flatten()
    } else {
        None
    };
    // single channel → every connection is a recipient of every event
    report("fanout", cli, &h, proc, cli.conns as u64);
    h.drain().await;
    Ok(())
}

/// `recipients_per_event` = how many connections each published event is expected to reach
/// (all connections for single-channel fan-out; conns/channels for the many-channels case).
pub fn report(
    name: &str,
    cli: &Cli,
    h: &Harness,
    proc: Option<(u64, f64)>,
    recipients_per_event: u64,
) {
    let c = &h.counters;
    let sent = c.sent.load(Ordering::Relaxed);
    let recv = c.received.load(Ordering::Relaxed);
    let (count, p50, p99, p999, max) = h.lat.summary_us();
    println!("=== scenario: {name} ===");
    println!(
        "conns={} subscribed={} sent={} received={} (expected≈{}) received_foreign={}",
        cli.conns,
        c.subscribed.load(Ordering::Relaxed),
        sent,
        recv,
        sent * recipients_per_event,
        c.received_foreign.load(Ordering::Relaxed)
    );
    println!("latency µs: count={count} p50={p50} p99={p99} p99.9={p999} max={max}");
    if let Some((rss_mb, cpu)) = proc {
        println!("server: peak_rss={rss_mb}MB mean_cpu={cpu:.1}%");
    }
}
