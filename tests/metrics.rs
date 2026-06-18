//! Integration tests for GET /metrics (Prometheus text exposition).

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::server::router::{build_router, AppState};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_tungstenite::tungstenite::Message;

const APPS: &str = r#"[
    {"name":"MetricsTest","id":"mapp1","key":"mapp-key","secret":"mapp-secret",
     "client_messages_enabled":false,"subscription_count_enabled":true}
]"#;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn spawn() -> SocketAddr {
    use std::sync::atomic::AtomicBool;

    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));
    let adapter: Arc<dyn Adapter> = local.clone();
    let conn_counts = Arc::new(Default::default());
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

    let (rest_tx, rest_rx) =
        tokio::sync::mpsc::unbounded_channel::<pylon::transport::RestConn>();
    let rest_state = AppState {
        config: config.clone(),
        apps: apps.clone(),
        adapter: adapter.clone(),
        conn_counts: Arc::clone(&conn_counts),
        webhooks: webhooks.clone(),
        saturated: Some(local.saturation_flag()),
    };
    tokio::spawn(pylon::transport::rest::serve(rest_rx, build_router(rest_state)));

    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = shutdown.clone();
    let worker_config = config.clone();
    let local_for_sink = Some(local.clone());
    let handle = std::thread::spawn(move || {
        let _ = pylon::transport::run_percore(
            worker_config,
            apps,
            adapter,
            conn_counts,
            webhooks,
            Some(rest_tx),
            worker_shutdown,
            local_for_sink,
            false,
        );
    });
    std::mem::forget((shutdown, handle));

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    format!("127.0.0.1:{port}").parse().unwrap()
}

async fn connect_ws(addr: SocketAddr) -> Ws {
    let (ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{addr}/app/mapp-key?protocol=7"))
            .await
            .unwrap();
    ws
}

async fn next_json(ws: &mut Ws) -> Value {
    loop {
        if let Message::Text(t) = ws.next().await.unwrap().unwrap() {
            return serde_json::from_str(&t).unwrap();
        }
    }
}

/// GET /metrics always returns 200 with correct Content-Type.
#[tokio::test]
async fn metrics_returns_200_with_correct_content_type() {
    let addr = spawn().await;
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "GET /metrics must return 200");
    let ct = resp.headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.starts_with("text/plain"),
        "Content-Type must be text/plain; got: {ct}"
    );
    assert!(ct.contains("0.0.4"), "Content-Type must include version=0.0.4; got: {ct}");
}

/// pylon_up 1 is always present.
#[tokio::test]
async fn metrics_contains_pylon_up() {
    let addr = spawn().await;
    let body = reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("pylon_up 1"), "pylon_up 1 must be present:\n{body}");
    assert!(body.contains("# TYPE pylon_up gauge"), "TYPE pylon_up must be present:\n{body}");
}

/// After a WebSocket subscribe, per-app connection and subscription gauges appear.
#[tokio::test]
async fn metrics_per_app_gauges_reflect_subscription() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let _ = next_json(&mut ws).await; // connection_established

    // Subscribe to a channel.
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":"public-metrics-room"}}).to_string(),
    ))
    .await
    .unwrap();
    let _ = next_json(&mut ws).await; // subscription_succeeded

    // Allow a tick for the conn_counts to be updated.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let body = reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    // Connection count for the app should be >= 1.
    assert!(
        body.contains(r#"pylon_connections{app="mapp1"}"#),
        "pylon_connections for mapp1 must appear:\n{body}"
    );
    // Channel should be occupied.
    assert!(
        body.contains(r#"pylon_channels_occupied{app="mapp1"} 1"#),
        "pylon_channels_occupied must be 1:\n{body}"
    );
    // Subscription count should be 1.
    assert!(
        body.contains(r#"pylon_subscriptions{app="mapp1"} 1"#),
        "pylon_subscriptions must be 1:\n{body}"
    );
}

/// Per-core worker metrics appear (inflight, budget, drops).
#[tokio::test]
async fn metrics_percore_metrics_present() {
    let addr = spawn().await;
    // Give the worker a moment to start and install the registry.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let body = reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(
        body.contains("pylon_inflight_bytes_sum"),
        "pylon_inflight_bytes_sum must appear:\n{body}"
    );
    assert!(
        body.contains("pylon_worker_budget_bytes"),
        "pylon_worker_budget_bytes must appear:\n{body}"
    );
    assert!(
        body.contains("pylon_budget_factor"),
        "pylon_budget_factor must appear:\n{body}"
    );
    assert!(
        body.contains(r#"pylon_broadcast_dropped_total{worker="0"}"#),
        "per-worker drop counter must appear:\n{body}"
    );
    assert!(
        body.contains(r#"pylon_inflight_bytes{worker="0"}"#),
        "per-worker inflight must appear:\n{body}"
    );
}
