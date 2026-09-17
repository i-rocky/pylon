use crate::cli::Cli;
use crate::metrics::{Counters, Latency};
use crate::pusher::{run_client, ClientConfig};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub mod channels;
pub mod cluster;
pub mod connect;
pub mod fanout;

pub struct Harness {
    pub epoch: Instant,
    pub run_id: String,
    pub lat: Arc<Latency>,
    pub counters: Arc<Counters>,
    pub shutdown: Arc<tokio::sync::Notify>,
    pub tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Harness {
    pub fn new(epoch: Instant) -> Self {
        Self {
            epoch,
            run_id: crate::pusher::run_id(),
            lat: Arc::new(Latency::default()),
            counters: Arc::new(Counters::default()),
            shutdown: Arc::new(tokio::sync::Notify::new()),
            tasks: Vec::new(),
        }
    }

    /// Spawn `n` subscribers across the given channel assignment fn, ramped.
    pub async fn spawn_clients(
        &mut self,
        cli: &Cli,
        url: &str,
        n: usize,
        channel_of: impl Fn(usize) -> String,
    ) {
        // Smooth ramp: pace spawns in ~100ms slices so the accept rate is steady rather
        // than a burst-then-1s-pause sawtooth. `per_sec` = target new conns/sec (0 = all at
        // once). No pacing when per_sec >= n (the whole set fits in under a second).
        let per_sec = if cli.ramp_per_sec == 0 {
            n
        } else {
            cli.ramp_per_sec
        };
        let batch = (per_sec / 10).max(1);
        // Parse client source IPs once. If parsing fails or the list is just the single
        // default 127.0.0.1, keep the OS-default behavior (src_ip = None).
        let ips: Vec<IpAddr> = cli
            .client_ips
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        let use_binding = ips.len() == cli.client_ips.len()
            && !(ips.len() == 1 && cli.client_ips[0] == "127.0.0.1");
        for i in 0..n {
            let src_ip = if use_binding && !ips.is_empty() {
                Some(ips[i % ips.len()])
            } else {
                None
            };
            let cfg = ClientConfig {
                url: url.to_string(),
                key: cli.key.clone(),
                secret: cli.secret.clone(),
                channel: channel_of(i),
                private: cli.private,
                src_ip,
                own_run_id: self.run_id.clone(),
            };
            let (e, l, c, s) = (
                self.epoch,
                self.lat.clone(),
                self.counters.clone(),
                self.shutdown.clone(),
            );
            self.tasks.push(tokio::spawn(async move {
                let _ = run_client(cfg, e, l, c, s).await;
            }));
            if per_sec < n && (i + 1) % batch == 0 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    pub async fn drain(self) {
        self.shutdown.notify_waiters();
        for t in self.tasks {
            let _ = t.await;
        }
    }
}

/// Wait until at least `target` subscribes are acked or `timeout` elapses. On timeout with a
/// shortfall, emit a loud stderr warning so a truncated run is never silently reported as if it
/// hit the target (the spec's #1 footgun).
pub async fn wait_subscribed(counters: &Counters, target: u64, timeout: Duration) {
    let start = Instant::now();
    loop {
        let got = counters
            .subscribed
            .load(std::sync::atomic::Ordering::Relaxed);
        if got >= target {
            return;
        }
        if start.elapsed() > timeout {
            let failed = counters
                .connect_failed
                .load(std::sync::atomic::Ordering::Relaxed);
            eprintln!(
                "WARNING: TRUNCATED RUN — only {got}/{target} subscribed after {:.0}s \
                 ({failed} connect failures). Results below reflect {got} connections, NOT {target}. \
                 Likely causes: ephemeral-port exhaustion (add more --client-ips) or server backpressure.",
                start.elapsed().as_secs_f64()
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
