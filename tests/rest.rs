//! REST HTTP API integration tests: signed requests, delivery, info endpoints.

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::auth::signature::{hmac_sha256_hex, md5_hex};
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::server::router::{build_router, AppState};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_tungstenite::tungstenite::Message;

const APPS: &str = r#"[
    {"name":"Test","id":"app1","key":"app-key","secret":"app-secret",
     "client_messages_enabled":true,"subscription_count_enabled":false}
]"#;
const SECRET: &str = "app-secret";

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn spawn() -> SocketAddr {
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));
    let state = AppState {
        config: ServerConfig::default(),
        apps,
        adapter,
        conn_counts: Arc::new(Default::default()),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });
    addr
}

/// Build the signed query string for a request, returning the full URL query.
fn signed_query(method: &str, path: &str, body: &[u8], extra: &[(&str, &str)]) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut p: BTreeMap<String, String> = BTreeMap::new();
    p.insert("auth_key".into(), "app-key".into());
    p.insert("auth_timestamp".into(), now.to_string());
    p.insert("auth_version".into(), "1.0".into());
    if !body.is_empty() {
        p.insert("body_md5".into(), md5_hex(body));
    }
    for (k, v) in extra {
        p.insert((*k).to_string(), (*v).to_string());
    }
    let canon = p
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let signed = format!("{}\n{}\n{}", method, path, canon);
    let sig = hmac_sha256_hex(SECRET, &signed);
    format!("{canon}&auth_signature={sig}")
}

async fn connect_ws(addr: SocketAddr) -> Ws {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/app/app-key?protocol=7"))
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

#[tokio::test]
async fn rest_trigger_delivers_to_subscriber() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let _ = next_json(&mut ws).await; // established
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":"public-room"}}).to_string(),
    ))
    .await
    .unwrap();
    let _ = next_json(&mut ws).await; // subscription_succeeded

    let body =
        json!({"name":"my-event","data":"{\"hi\":1}","channels":["public-room"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "my-event");
    assert_eq!(frame["channel"], "public-room");
    assert_eq!(frame["data"], "{\"hi\":1}"); // delivered verbatim as a string
}

#[tokio::test]
async fn rest_bad_signature_is_401() {
    let addr = spawn().await;
    let body = json!({"name":"e","data":"{}","channels":["c"]}).to_string();
    let mut q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    q = q.replace(
        &q[q.rfind("auth_signature=").unwrap()..],
        "auth_signature=deadbeef",
    );
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn rest_get_channel_reports_occupancy() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let _ = next_json(&mut ws).await;
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":"public-room"}}).to_string(),
    ))
    .await
    .unwrap();
    let _ = next_json(&mut ws).await;

    let q = signed_query(
        "GET",
        "/apps/app1/channels/public-room",
        b"",
        &[("info", "subscription_count")],
    );
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app1/channels/public-room?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["occupied"], true);
    assert_eq!(v["subscription_count"], 1);
}
