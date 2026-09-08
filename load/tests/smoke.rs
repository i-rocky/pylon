//! Low-N end-to-end smoke against a real pylon child (the percore transport — the only
//! transport since SP11). Confirms the harness's `run_client` subscriber and `Publisher`
//! deliver every published event to every subscriber (exact `recv == K*P`). CI-safe.
use pylon_load::ceiling::child::{default_pylon_bin, write_temp_apps, ChildOpts, PylonChild};
use pylon_load::metrics::{Counters, Latency};
use pylon_load::pusher::{run_client, stamp_payload, ClientConfig, Publisher};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fanout_smoke_delivers_to_all() {
    let apps = write_temp_apps().unwrap();
    let opts = ChildOpts {
        pylon_bin: default_pylon_bin(),
        port: 7720,
        workers: 2,
        cores: "0-1".into(),
        apps_path: apps,
    };
    let child = PylonChild::spawn(&opts).await.expect("spawn pylon child");
    let url = "ws://127.0.0.1:7720/app/app-key".to_string();
    let rest = "http://127.0.0.1:7720".to_string();

    let epoch = Instant::now();
    let lat = Arc::new(Latency::default());
    let counters = Arc::new(Counters::default());
    let shutdown = Arc::new(tokio::sync::Notify::new());

    const K: usize = 20;
    let mut tasks = Vec::new();
    for _ in 0..K {
        let cfg = ClientConfig {
            url: url.clone(),
            key: "app-key".into(),
            secret: "app-secret".into(),
            channel: "bench".into(),
            private: false,
            src_ip: None,
        };
        let (l, c, s) = (lat.clone(), counters.clone(), shutdown.clone());
        tasks.push(tokio::spawn(async move {
            let _ = run_client(cfg, epoch, l, c, s).await;
        }));
    }

    // wait for all K subscribed
    let start = Instant::now();
    while counters.subscribed.load(Ordering::Relaxed) < K as u64 {
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "subscribe timeout"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let pubr = Publisher::new(rest, "app".into(), "app-key".into(), "app-secret".into());
    const P: u64 = 5;
    for seq in 0..P {
        let payload = stamp_payload(seq, epoch.elapsed().as_nanos());
        pubr.publish("bench", "ev", &payload, pylon_load::pusher::unix_now())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    let recv = counters.received.load(Ordering::Relaxed);
    assert_eq!(recv, K as u64 * P, "expected {} got {recv}", K as u64 * P);
    let (count, _p50, _p99, _p999, max) = lat.summary_us();
    assert_eq!(count, K as u64 * P);
    assert!(max < 5_000_000, "max latency {max}µs too high"); // < 5s sanity

    shutdown.notify_waiters();
    for t in tasks {
        let _ = t.await;
    }
    // Explicit teardown, after fanout is confirmed, instead of leaving it to
    // the implicit end-of-scope drop.
    drop(child);
}
