use super::{wait_subscribed, Harness};
use crate::cli::Cli;
use crate::metrics::sample_proc;
use crate::pusher::{stamp_payload, Publisher};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

pub fn channel_name(run_id: &str, i: usize, m: usize) -> String {
    format!("bench-{run_id}-{}", i % m)
}

/// S3: N connections across M channels; publisher round-robins events at `rate`/sec.
pub async fn run(cli: &Cli) -> anyhow::Result<()> {
    let epoch = Instant::now();
    let mut h = Harness::new(epoch);
    let m = cli.channels.max(1);
    let run_id = h.run_id.clone();
    h.spawn_clients(cli, &cli.url, cli.conns, move |i| {
        channel_name(&run_id, i, m)
    })
    .await;
    wait_subscribed(&h.counters, cli.conns as u64, Duration::from_secs(120)).await;

    let pubr = Publisher::new(
        cli.rest.clone(),
        cli.app_id.clone(),
        cli.key.clone(),
        cli.secret.clone(),
    );
    let sampler = cli
        .server_pid
        .map(|pid| tokio::spawn(sample_proc(pid, Duration::from_secs(cli.secs))));

    let interval = Duration::from_secs_f64(1.0 / cli.rate.max(1) as f64);
    let mut ticker = tokio::time::interval(interval);
    let end = Instant::now() + Duration::from_secs(cli.secs);
    let mut seq = 0u64;
    while Instant::now() < end {
        ticker.tick().await;
        let channel = channel_name(&h.run_id, seq as usize, m);
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
    tokio::time::sleep(Duration::from_millis(500)).await;
    let proc = if let Some(s) = sampler {
        s.await.ok().flatten()
    } else {
        None
    };
    // conns are spread across `m` channels → each event reaches ~conns/m subscribers
    super::fanout::report("channels", cli, &h, proc, (cli.conns / m) as u64);
    h.drain().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_bench_id_0_through_m_minus_1() {
        let names: Vec<String> = (0..4).map(|i| channel_name("ab12cd34", i, 4)).collect();
        assert_eq!(
            names,
            vec![
                "bench-ab12cd34-0",
                "bench-ab12cd34-1",
                "bench-ab12cd34-2",
                "bench-ab12cd34-3",
            ]
        );
    }

    #[test]
    fn two_run_ids_never_collide() {
        let a: std::collections::HashSet<String> =
            (0..4).map(|i| channel_name("aaaa1111", i, 4)).collect();
        let b: std::collections::HashSet<String> =
            (0..4).map(|i| channel_name("bbbb2222", i, 4)).collect();
        assert!(a.is_disjoint(&b));
    }
}
