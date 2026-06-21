//! Integration tests for GET /health, /healthz, /ready, /readyz.

use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::server::router::{build_router, AppState};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;

const APPS: &str = r#"[
    {"name":"HealthTest","id":"happ1","key":"happ-key","secret":"happ-secret",
     "client_messages_enabled":false,"subscription_count_enabled":false}
]"#;

/// Spawn a full percore node (1 worker) with a live REST plane on a random port.
/// Returns the bound `SocketAddr`.
async fn spawn() -> SocketAddr {
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new()), Arc::new(pylon::adapter::app_registry::AppRegistry::new())));
    let adapter: Arc<dyn Adapter> = local.clone();
    let conn_counts: Arc<dashmap::DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());
    let webhooks = pylon::webhook::WebhookHandle::null();

    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let config = ServerConfig {
        bind: "127.0.0.1".into(),
        port,
        workers: 1,
        ..ServerConfig::default()
    };

    let (rest_tx, rest_rx) = tokio::sync::mpsc::unbounded_channel::<pylon::transport::RestConn>();
    let rest_state = AppState {
        config: config.clone(),
        apps: apps.clone(),
        adapter: adapter.clone(),
        conn_counts: Arc::clone(&conn_counts),
        webhooks: webhooks.clone(),
        saturated: Some(local.saturation_flag()),
        draining: Arc::new(AtomicBool::new(false)),
        cluster_metrics: None,
        invalidator: None,
    };
    tokio::spawn(pylon::transport::rest::serve(
        rest_rx,
        build_router(rest_state),
    ));

    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = shutdown.clone();
    let worker_config = config.clone();
    let local_for_sink = Some(local.clone());
    // Phase 7: capture the runtime handle here (async context) before spawning.
    let worker_runtime = tokio::runtime::Handle::current();
    let handle = std::thread::spawn(move || {
        let _ = pylon::transport::run_percore(
            worker_config,
            apps,
            adapter,
            conn_counts,
            Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            webhooks,
            Some(rest_tx),
            worker_shutdown,
            local_for_sink,
            false,
            None,
            worker_runtime,
        );
    });
    // Leak the worker thread + shutdown flag; the test process is short-lived.
    std::mem::forget((shutdown, handle));

    // Give the worker time to bind and register its metrics slot.
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    format!("127.0.0.1:{port}").parse().unwrap()
}

/// GET /health → 200 "ok".
#[tokio::test]
async fn health_returns_200_ok() {
    let addr = spawn().await;
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "GET /health must return 200");
    let body = resp.text().await.unwrap();
    assert_eq!(body, "ok", "GET /health body must be 'ok'; got: {body}");
}

/// GET /healthz (alias) → 200 "ok".
#[tokio::test]
async fn healthz_returns_200_ok() {
    let addr = spawn().await;
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "GET /healthz must return 200");
}

/// GET /ready → 200 "ready" when percore fleet is up.
#[tokio::test]
async fn ready_returns_200_when_fleet_is_up() {
    let addr = spawn().await;
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/ready"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "GET /ready must return 200 when fleet is up"
    );
    let body = resp.text().await.unwrap();
    assert_eq!(
        body, "ready",
        "GET /ready body must be 'ready'; got: {body}"
    );
}

/// GET /readyz (alias) → 200 "ready" when percore fleet is up.
#[tokio::test]
async fn readyz_returns_200_when_fleet_is_up() {
    let addr = spawn().await;
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/readyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "GET /readyz must return 200 when fleet is up"
    );
}
