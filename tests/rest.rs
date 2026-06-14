//! REST HTTP API integration tests: signed requests, delivery, info endpoints.

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::auth::signature::{channel_signature, hmac_sha256_hex, md5_hex};
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
        webhooks: pylon::webhook::WebhookHandle::null(),
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
    let signed = format!("{}\n{}\n{}", method.to_uppercase(), path, canon);
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

/// Read the `connection_established` frame and return the assigned socket_id.
async fn established_socket_id(ws: &mut Ws) -> String {
    let frame = next_json(ws).await;
    let data: Value = serde_json::from_str(frame["data"].as_str().unwrap()).unwrap();
    data["socket_id"].as_str().unwrap().to_string()
}

/// Subscribe `ws` to a public channel and consume the success frame.
async fn subscribe_public(ws: &mut Ws, channel: &str) {
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":channel}}).to_string(),
    ))
    .await
    .unwrap();
    let _ = next_json(ws).await; // subscription_succeeded
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

#[tokio::test]
async fn rest_batch_events_delivers_to_two_channels() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let _ = next_json(&mut ws).await; // established
    subscribe_public(&mut ws, "room-a").await;
    subscribe_public(&mut ws, "room-b").await;

    let body = json!({"batch":[
        {"name":"ev-a","data":"1","channel":"room-a"},
        {"name":"ev-b","data":"2","channel":"room-b"}
    ]})
    .to_string();
    let q = signed_query("POST", "/apps/app1/batch_events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/batch_events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Both events are fanned out; collect by channel to be order-independent.
    let mut got = std::collections::HashMap::new();
    for _ in 0..2 {
        let f = next_json(&mut ws).await;
        got.insert(
            f["channel"].as_str().unwrap().to_string(),
            f["event"].as_str().unwrap().to_string(),
        );
    }
    assert_eq!(got.get("room-a").map(String::as_str), Some("ev-a"));
    assert_eq!(got.get("room-b").map(String::as_str), Some("ev-b"));
}

#[tokio::test]
async fn rest_get_channels_lists_occupied_channel() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let _ = next_json(&mut ws).await;
    subscribe_public(&mut ws, "public-room").await;

    let q = signed_query("GET", "/apps/app1/channels", b"", &[]);
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app1/channels?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert!(v["channels"]["public-room"].is_object());
}

#[tokio::test]
async fn rest_get_users_lists_presence_members() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let socket_id = established_socket_id(&mut ws).await;

    let channel = "presence-room";
    let channel_data = json!({"user_id":"u1","user_info":{"name":"U"}}).to_string();
    let token = format!(
        "app-key:{}",
        channel_signature(SECRET, &socket_id, channel, Some(&channel_data))
    );
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{
            "channel": channel, "auth": token, "channel_data": channel_data
        }})
        .to_string(),
    ))
    .await
    .unwrap();
    let _ = next_json(&mut ws).await; // subscription_succeeded (presence roster)

    let q = signed_query("GET", "/apps/app1/channels/presence-room/users", b"", &[]);
    let resp = reqwest::Client::new()
        .get(format!(
            "http://{addr}/apps/app1/channels/presence-room/users?{q}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["users"], json!([{"id": "u1"}]));
}

#[tokio::test]
async fn rest_trigger_relays_to_encrypted_subscriber() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let socket_id = established_socket_id(&mut ws).await;

    // Subscribe to an encrypted channel (private-style token, no channel_data).
    let channel = "private-encrypted-room";
    let token = format!(
        "app-key:{}",
        channel_signature(SECRET, &socket_id, channel, None)
    );
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":channel,"auth":token}}).to_string(),
    ))
    .await
    .unwrap();
    let succ = next_json(&mut ws).await;
    assert_eq!(succ["event"], "pusher_internal:subscription_succeeded");

    // REST-trigger an opaque ciphertext payload; pylon must relay it verbatim.
    // `data` is a string on the wire (what Pusher server SDKs send for encrypted).
    let cipher = "{\"nonce\":\"abc\",\"ciphertext\":\"xyz\"}";
    let body = json!({"name":"secret","data":cipher,"channels":[channel]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "secret");
    assert_eq!(frame["channel"], channel);
    assert_eq!(frame["data"], cipher); // verbatim, untouched
}

#[tokio::test]
async fn rest_trigger_two_encrypted_channels_is_400() {
    let addr = spawn().await;
    let body = json!({
        "name": "secret",
        "data": "x",
        "channels": ["private-encrypted-a", "private-encrypted-b"]
    })
    .to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

/// Encrypted channel alongside ANY other channel must be rejected (400).
#[tokio::test]
async fn rest_trigger_encrypted_plus_public_is_400() {
    let addr = spawn().await;
    let body = json!({
        "name": "secret",
        "data": "x",
        "channels": ["private-encrypted-a", "public-b"]
    })
    .to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

/// A single encrypted channel alone is allowed (200).
#[tokio::test]
async fn rest_trigger_encrypted_solo_is_200() {
    let addr = spawn().await;
    let body = json!({
        "name": "secret",
        "data": "x",
        "channels": ["private-encrypted-a"]
    })
    .to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

/// Two plaintext channels together are still allowed (200).
#[tokio::test]
async fn rest_trigger_two_plaintext_channels_is_200() {
    let addr = spawn().await;
    let body = json!({
        "name": "ev",
        "data": "x",
        "channels": ["public-a", "public-b"]
    })
    .to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn rest_trigger_caches_event_for_later_subscriber() {
    let addr = spawn().await;

    // Trigger to a cache channel BEFORE anyone subscribes — only the cache write matters.
    let body = json!({"name":"my-event","data":"{\"hi\":1}","channels":["cache-feed"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // A new subscriber gets subscription_succeeded, then the replayed cached event.
    let mut ws = connect_ws(addr).await;
    let _ = next_json(&mut ws).await; // established
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":"cache-feed"}}).to_string(),
    ))
    .await
    .unwrap();
    let succ = next_json(&mut ws).await;
    assert_eq!(succ["event"], "pusher_internal:subscription_succeeded");
    let replay = next_json(&mut ws).await;
    assert_eq!(replay["event"], "my-event");
    assert_eq!(replay["channel"], "cache-feed");
    assert_eq!(replay["data"], "{\"hi\":1}"); // verbatim
}

#[tokio::test]
async fn cache_subscribe_with_no_cache_emits_cache_miss() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let _ = next_json(&mut ws).await; // established
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":"cache-empty"}}).to_string(),
    ))
    .await
    .unwrap();
    let succ = next_json(&mut ws).await;
    assert_eq!(succ["event"], "pusher_internal:subscription_succeeded");
    let miss = next_json(&mut ws).await;
    assert_eq!(miss["event"], "pusher:cache_miss");
    assert_eq!(miss["channel"], "cache-empty");
    assert!(miss.get("data").is_none(), "cache_miss has no data field");
}

#[tokio::test]
async fn private_cache_subscribe_replays_after_auth() {
    let addr = spawn().await;

    // Cache an event on a private-cache channel via REST.
    let body = json!({"name":"e","data":"\"v\"","channels":["private-cache-x"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Authenticate + subscribe, then receive the replay.
    let mut ws = connect_ws(addr).await;
    let socket_id = established_socket_id(&mut ws).await;
    let token = format!(
        "app-key:{}",
        channel_signature(SECRET, &socket_id, "private-cache-x", None)
    );
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":"private-cache-x","auth":token}})
            .to_string(),
    ))
    .await
    .unwrap();
    let succ = next_json(&mut ws).await;
    assert_eq!(succ["event"], "pusher_internal:subscription_succeeded");
    let replay = next_json(&mut ws).await;
    assert_eq!(replay["event"], "e");
    assert_eq!(replay["channel"], "private-cache-x");
}

#[tokio::test]
async fn rest_body_too_large_is_413() {
    let addr = spawn().await;
    // Default limits → body cap = 10*10240 + 64KiB ≈ 164KiB; exceed it. The
    // limit fires at body extraction, before the signature check runs.
    let big = "x".repeat(200 * 1024);
    let body = json!({"name": "e", "data": big, "channels": ["c"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413);
}
