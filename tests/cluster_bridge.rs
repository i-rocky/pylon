//! Integration test for the percore [`ClusterBridge`] skeleton (SP11 Task 3.2).
//!
//! The bridge owns a DEDICATED tokio runtime (on its own OS thread) hosting a
//! `RedisAdapter`; the percore workers fire fire-and-forget commands at it over a cheap-
//! clone, `Send` [`ClusterHandle`]. This test proves the runtime starts (a real connect to
//! the test Redis), the handle clones and crosses a thread boundary, a smoke `publish`
//! returns immediately without panicking, and dropping the bridge tears it down cleanly.
//!
//! Like `redis_cluster.rs` it talks to a REAL Redis (`PYLON_TEST_REDIS_URL`, default
//! `redis://127.0.0.1:6390`) and isolates every run behind a random key prefix — it NEVER
//! issues FLUSHALL/FLUSHDB. If Redis is unreachable the test SKIPS (prints + returns)
//! rather than failing, so the gate stays green where no test Redis is available.
//!
//! [`ClusterBridge`]: pylon::cluster::bridge::ClusterBridge
//! [`ClusterHandle`]: pylon::cluster::bridge::ClusterHandle

use pylon::adapter::local::LocalAdapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::cluster::bridge;
use pylon::server::config::ServerConfig;
use pylon::webhook::WebhookHandle;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Test Redis URL: `PYLON_TEST_REDIS_URL` or the documented test default (port 6390).
fn test_redis_url() -> String {
    std::env::var("PYLON_TEST_REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6390".to_string())
}

/// A random, run-unique key prefix for isolation on a shared Redis.
fn random_prefix() -> String {
    format!("pylontest:{}", Uuid::new_v4())
}

/// Build a `ServerConfig` for the Redis adapter against the test Redis with a random prefix.
fn redis_test_config(prefix: &str) -> ServerConfig {
    ServerConfig {
        adapter: "redis".into(),
        redis_url: test_redis_url(),
        redis_prefix: prefix.into(),
        ..ServerConfig::default()
    }
}

/// `host:port` of the test Redis URL, for a cheap reachability probe.
fn redis_host_port() -> String {
    test_redis_url()
        .strip_prefix("redis://")
        .unwrap_or("127.0.0.1:6390")
        .trim_end_matches('/')
        .to_string()
}

/// Whether the test Redis accepts a TCP connection. The bridge would `Err` on an
/// unreachable Redis; we skip instead so the gate is green where no Redis is provisioned.
fn redis_reachable() -> bool {
    use std::net::ToSocketAddrs;
    match redis_host_port().to_socket_addrs().ok().and_then(|mut it| it.next()) {
        Some(sa) => TcpStream::connect_timeout(&sa, Duration::from_millis(500)).is_ok(),
        None => false,
    }
}

#[tokio::test]
async fn cluster_bridge_starts_clones_publishes_and_drops_cleanly() {
    if !redis_reachable() {
        eprintln!(
            "skipping cluster_bridge test: test Redis at {} is unreachable",
            redis_host_port()
        );
        return;
    }

    // The whole body is bounded so a wedged Redis or a hung shutdown fails the test fast
    // instead of stalling CI. The bridge runs on its OWN runtime thread, so this outer
    // tokio runtime only hosts the `WebhookHandle::null()` drainer and this timeout.
    tokio::time::timeout(Duration::from_secs(8), async {
        let cfg = redis_test_config(&random_prefix());
        // The SAME `LocalAdapter` the percore workers would broadcast through.
        let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));
        let webhooks = WebhookHandle::null();
        // The bridge resolves per-app flags itself; a single app is enough for the smoke.
        let apps: Arc<dyn AppManager> = Arc::new(
            StaticFileAppManager::from_json(
                r#"[{"name":"T","id":"app","key":"k","secret":"s"}]"#,
            )
            .expect("apps json must parse"),
        );

        // 1. Start: a real connect to the test Redis must succeed.
        let bridge = bridge::start(&cfg, local, webhooks, apps)
            .expect("ClusterBridge::start must connect to the test Redis and report ready");

        // The live node id is non-empty (a UUID minted by the adapter).
        assert!(
            !bridge.handle().node_id().is_empty(),
            "the bridge handle must carry the real cluster node id"
        );

        // 2. The handle clones and is `Send`: move a clone into a plain OS thread and use
        //    it there (node_id + a smoke publish), exactly as a percore worker would.
        let worker_handle = bridge.handle();
        let node_id_from_thread = std::thread::spawn(move || {
            let id = worker_handle.node_id().to_string();
            // 3. Smoke publish: `try_send`s and returns immediately — must not panic.
            worker_handle.publish(
                Arc::from("app"),
                Arc::from("chan"),
                "{\"event\":\"x\"}".to_string(),
                None,
            );
            id
        })
        .join()
        .expect("worker thread must not panic");

        assert_eq!(
            node_id_from_thread,
            bridge.handle().node_id(),
            "the node id seen on the worker thread must match the bridge's"
        );

        // 4. A publish on THIS task's clone is likewise immediate and panic-free.
        bridge.handle().publish(
            Arc::from("app"),
            Arc::from("chan2"),
            "{\"event\":\"y\"}".to_string(),
            None,
        );

        // 5. Dropping the bridge signals shutdown and joins the runtime thread — it must
        //    not hang (the surrounding timeout would catch a hang and fail the test).
        drop(bridge);
    })
    .await
    .expect("cluster_bridge test must not hang (Redis up? shutdown clean?)");
}
