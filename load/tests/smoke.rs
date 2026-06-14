//! Low-N end-to-end smoke test against an in-process pylon. CI-safe.
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::server::router::{build_router, AppState};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pylon_load::metrics::{Counters, Latency};
use pylon_load::pusher::{run_client, stamp_payload, ClientConfig, Publisher};

const APPS: &str = r#"[{"name":"T","id":"app","key":"app-key","secret":"app-secret","capacity":1000}]"#;

async fn spawn() -> std::net::SocketAddr {
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));
    let state = AppState {
        config: ServerConfig::default(),
        apps,
        adapter,
        conn_counts: Arc::new(Default::default()),
        webhooks: pylon::webhook::WebhookHandle::null(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, build_router(state)).await.unwrap(); });
    addr
}

#[tokio::test]
async fn fanout_smoke_delivers_to_all() {
    let addr = spawn().await;
    let url = format!("ws://{addr}/app/app-key");
    let rest = format!("http://{addr}");
    let epoch = Instant::now();
    let lat = Arc::new(Latency::default());
    let counters = Arc::new(Counters::default());
    let shutdown = Arc::new(tokio::sync::Notify::new());

    const K: usize = 20;
    let mut tasks = Vec::new();
    for _ in 0..K {
        let cfg = ClientConfig {
            url: url.clone(), key: "app-key".into(), secret: "app-secret".into(),
            channel: "bench".into(), private: false,
        };
        let (l, c, s) = (lat.clone(), counters.clone(), shutdown.clone());
        tasks.push(tokio::spawn(async move { let _ = run_client(cfg, epoch, l, c, s).await; }));
    }

    // wait for all subscribed
    let start = Instant::now();
    while counters.subscribed.load(std::sync::atomic::Ordering::Relaxed) < K as u64 {
        assert!(start.elapsed() < Duration::from_secs(10), "subscribe timeout");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let pubr = Publisher::new(rest, "app".into(), "app-key".into(), "app-secret".into());
    const P: u64 = 5;
    for seq in 0..P {
        let payload = stamp_payload(seq, epoch.elapsed().as_nanos());
        pubr.publish("bench", "ev", &payload, pylon_load::pusher::unix_now()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let recv = counters.received.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(recv, K as u64 * P, "expected {} got {recv}", K as u64 * P);
    let (count, _p50, _p99, _p999, max) = lat.summary_us();
    assert_eq!(count, K as u64 * P);
    assert!(max < 5_000_000, "max latency {max}µs too high"); // < 5s sanity

    shutdown.notify_waiters();
    for t in tasks { let _ = t.await; }
}
