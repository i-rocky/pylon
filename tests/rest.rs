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
     "client_messages_enabled":true,"subscription_count_enabled":false},
    {"name":"Test2","id":"app2","key":"app2-key","secret":"app2-secret",
     "client_messages_enabled":true,"subscription_count_enabled":true}
]"#;
const SECRET: &str = "app-secret";
const SECRET2: &str = "app2-secret";

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Spawn the multi-app REST + WS server on a real single-worker percore fleet
/// (the only transport). The worker serves WS clients directly and hands plain-
/// HTTP (REST) connections off to the axum `Router` via the handoff plane, exactly
/// as `main.rs` wires the single-node percore path. The worker thread + shutdown
/// flag are leaked; the OS reclaims them at process exit (test processes are
/// short-lived).
async fn spawn() -> SocketAddr {
    use std::sync::atomic::AtomicBool;

    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new()), Arc::new(pylon::adapter::app_registry::AppRegistry::new())));
    let adapter: Arc<dyn Adapter> = local.clone();
    let conn_counts = Arc::new(Default::default());
    let webhooks = pylon::webhook::WebhookHandle::null();

    // Reserve a free ephemeral port, then release it before the worker re-binds
    // with SO_REUSEPORT (race-free in practice).
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    // One worker keeps subscribe/broadcast ordering a single sequential stream.
    let config = ServerConfig {
        bind: "127.0.0.1".into(),
        port,
        workers: 1,
        ..ServerConfig::default()
    };

    // REST handoff plane: the worker hands plain-HTTP connections to this axum
    // router via `rest_tx`; `rest::serve` drives them on the tokio runtime.
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

    // Run the blocking `mio` worker on a dedicated thread.
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
            // Single-node (not clustered).
            false,
            None,
            worker_runtime,
        );
    });
    std::mem::forget((shutdown, handle));

    // Give the worker a moment to bind its SO_REUSEPORT listener.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    format!("127.0.0.1:{port}").parse().unwrap()
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

async fn connect_ws2(addr: SocketAddr) -> Ws {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/app/app2-key?protocol=7"))
        .await
        .unwrap();
    ws
}

fn signed_query2(method: &str, path: &str, body: &[u8], extra: &[(&str, &str)]) -> String {
    use pylon::auth::signature::hmac_sha256_hex;
    use pylon::auth::signature::md5_hex;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut p: BTreeMap<String, String> = BTreeMap::new();
    p.insert("auth_key".into(), "app2-key".into());
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
    let sig = hmac_sha256_hex(SECRET2, &signed);
    format!("{canon}&auth_signature={sig}")
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
    // app1 has subscription_count_enabled:false → attribute must be omitted
    assert!(
        v.get("subscription_count").is_none(),
        "subscription_count must be absent when flag is off, got: {v}"
    );
}

/// GET /channels/:name with subscription_count_enabled=true → attribute present.
#[tokio::test]
async fn rest_get_channel_subscription_count_enabled() {
    let addr = spawn().await;
    let mut ws = connect_ws2(addr).await;
    let _ = next_json(&mut ws).await;
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":"public-room"}}).to_string(),
    ))
    .await
    .unwrap();
    let _ = next_json(&mut ws).await;

    let q = signed_query2(
        "GET",
        "/apps/app2/channels/public-room",
        b"",
        &[("info", "subscription_count")],
    );
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app2/channels/public-room?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["occupied"], true);
    // app2 has subscription_count_enabled:true → attribute must be present
    assert_eq!(
        v["subscription_count"], 1,
        "subscription_count must be 1 when flag is on, got: {v}"
    );
}

/// POST /events with info=subscription_count and flag OFF → attribute omitted.
#[tokio::test]
async fn rest_trigger_info_subscription_count_disabled() {
    let addr = spawn().await;
    let body =
        json!({"name":"ev","data":"{}","channels":["public-room"],"info":"subscription_count"})
            .to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    // channels key present but subscription_count must be absent per-channel
    let ch = &v["channels"]["public-room"];
    assert!(
        ch.get("subscription_count").is_none(),
        "subscription_count must be absent when flag is off, got: {v}"
    );
}

/// POST /events with info=subscription_count and flag ON → attribute present.
#[tokio::test]
async fn rest_trigger_info_subscription_count_enabled() {
    let addr = spawn().await;
    // Subscribe a client to the channel so subscription_count > 0.
    let mut ws = connect_ws2(addr).await;
    let _ = next_json(&mut ws).await;
    subscribe_public(&mut ws, "public-room").await;

    let body =
        json!({"name":"ev","data":"{}","channels":["public-room"],"info":"subscription_count"})
            .to_string();
    let q = signed_query2("POST", "/apps/app2/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app2/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    let ch = &v["channels"]["public-room"];
    assert_eq!(
        ch["subscription_count"], 1,
        "subscription_count must be present when flag is on, got: {v}"
    );
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

// P15 — GET /channels list must emit per-channel subscription_count

/// GET /channels?info=subscription_count with flag ON → each channel carries subscription_count.
#[tokio::test]
async fn rest_get_channels_list_subscription_count_enabled() {
    let addr = spawn().await;
    // Connect on app2 which has subscription_count_enabled=true.
    let mut ws = connect_ws2(addr).await;
    let _ = next_json(&mut ws).await; // established
    subscribe_public(&mut ws, "public-room").await;

    let q = signed_query2(
        "GET",
        "/apps/app2/channels",
        b"",
        &[("info", "subscription_count")],
    );
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app2/channels?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(
        v["channels"]["public-room"]["subscription_count"], 1,
        "GET /channels with flag ON must emit subscription_count per channel (P15), got: {v}"
    );
}

/// GET /channels?info=subscription_count with flag OFF → attribute absent.
#[tokio::test]
async fn rest_get_channels_list_subscription_count_disabled() {
    let addr = spawn().await;
    // app1 has subscription_count_enabled=false.
    let mut ws = connect_ws(addr).await;
    let _ = next_json(&mut ws).await;
    subscribe_public(&mut ws, "public-room").await;

    let q = signed_query(
        "GET",
        "/apps/app1/channels",
        b"",
        &[("info", "subscription_count")],
    );
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app1/channels?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert!(
        v["channels"]["public-room"]
            .get("subscription_count")
            .is_none(),
        "GET /channels with flag OFF must NOT emit subscription_count (P15), got: {v}"
    );
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

/// An empty channel name must be rejected on the REST trigger path (400) — parity P14.
#[tokio::test]
async fn rest_trigger_empty_channel_name_is_400() {
    let addr = spawn().await;
    let body = json!({
        "name": "e",
        "data": "x",
        "channels": [""]
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

// ── P7 parity tests ─────────────────────────────────────────────────────────

/// P7(a): event `data` exceeding per-event cap → 413, not 400.
#[tokio::test]
async fn rest_event_data_too_large_is_413() {
    let addr = spawn().await;
    // max_event_payload_bytes default = 10 240; craft a data string just over it.
    let big_data = "x".repeat(10_241);
    let body = json!({"name":"e","data": big_data,"channels":["c"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413, "oversized event data must be 413");
}

/// P7(a) batch: any item's `data` exceeding per-event cap → 413.
#[tokio::test]
async fn rest_batch_event_data_too_large_is_413() {
    let addr = spawn().await;
    let big_data = "x".repeat(10_241);
    let body = json!({"batch":[{"name":"e","data": big_data,"channel":"c"}]}).to_string();
    let q = signed_query("POST", "/apps/app1/batch_events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/batch_events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413, "oversized batch item data must be 413");
}

/// P7(b): GET /channels?info=user_count without a presence filter → 400.
#[tokio::test]
async fn rest_channels_user_count_without_presence_filter_is_400() {
    let addr = spawn().await;
    let q = signed_query("GET", "/apps/app1/channels", b"", &[("info", "user_count")]);
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app1/channels?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "user_count without presence filter must be 400"
    );
}

/// P7(b): GET /channels?info=user_count&filter_by_prefix=presence- → 200.
#[tokio::test]
async fn rest_channels_user_count_with_presence_filter_is_200() {
    let addr = spawn().await;
    let q = signed_query(
        "GET",
        "/apps/app1/channels",
        b"",
        &[("info", "user_count"), ("filter_by_prefix", "presence-")],
    );
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app1/channels?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "user_count with presence filter must be 200"
    );
}

/// P7(c): GET /channels/{channel}/users on a non-presence channel → 400.
#[tokio::test]
async fn rest_users_on_non_presence_channel_is_400() {
    let addr = spawn().await;
    let q = signed_query("GET", "/apps/app1/channels/public-room/users", b"", &[]);
    let resp = reqwest::Client::new()
        .get(format!(
            "http://{addr}/apps/app1/channels/public-room/users?{q}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "users endpoint on non-presence channel must be 400"
    );
}

/// P7(c): GET /channels/{channel}/users on a presence- channel → 200.
#[tokio::test]
async fn rest_users_on_presence_channel_is_200() {
    let addr = spawn().await;
    // No members — but the channel name is valid so it must return 200 + empty list.
    let q = signed_query("GET", "/apps/app1/channels/presence-empty/users", b"", &[]);
    let resp = reqwest::Client::new()
        .get(format!(
            "http://{addr}/apps/app1/channels/presence-empty/users?{q}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "users endpoint on presence channel must be 200"
    );
}

// ── P8 parity tests — channel-name length + charset ─────────────────────────

/// P8: POST /events with a channel name exceeding 164 chars → 400.
#[tokio::test]
async fn rest_trigger_channel_name_over_length_is_400() {
    let addr = spawn().await;
    let long_name = "a".repeat(165);
    let body = json!({"name":"ev","data":"{}","channels":[long_name]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "channel name over 164 chars must be 400"
    );
}

/// P8: POST /events with a channel name containing an illegal char → 400.
#[tokio::test]
async fn rest_trigger_channel_name_bad_charset_is_400() {
    let addr = spawn().await;
    let body = json!({"name":"ev","data":"{}","channels":["bad channel!"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "channel name with illegal chars must be 400"
    );
}

/// P8: POST /events with a valid channel name → 200 (regression guard).
#[tokio::test]
async fn rest_trigger_valid_channel_name_is_200() {
    let addr = spawn().await;
    let body =
        json!({"name":"ev","data":"{}","channels":["my-valid_channel.name@here"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "valid channel name must still be 200");
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

// ── P9 parity tests — event-name length (max 200 chars) ─────────────────────

/// P9: POST /events with an event name exceeding 200 chars → 400.
#[tokio::test]
async fn rest_trigger_event_name_over_200_is_400() {
    let addr = spawn().await;
    let long_name = "a".repeat(201);
    let body = json!({"name": long_name, "data": "{}", "channels": ["room"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "event name over 200 chars must be 400");
}

/// P9: POST /events with an event name of exactly 200 chars → 200.
#[tokio::test]
async fn rest_trigger_event_name_exactly_200_is_200() {
    let addr = spawn().await;
    let name_200 = "a".repeat(200);
    let body = json!({"name": name_200, "data": "{}", "channels": ["room"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "event name of exactly 200 chars must be 200"
    );
}

/// P9: POST /batch_events with an event name exceeding 200 chars → 400.
#[tokio::test]
async fn rest_batch_event_name_over_200_is_400() {
    let addr = spawn().await;
    let long_name = "a".repeat(201);
    let body = json!({"batch": [{"name": long_name, "data": "{}", "channel": "room"}]}).to_string();
    let q = signed_query("POST", "/apps/app1/batch_events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/batch_events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "batch event name over 200 chars must be 400"
    );
}

// ── P13 parity tests — pre-handshake reject must carry Pusher 4xxx close code ─

/// Connecting to an unknown app key triggers a 4001 rejection.  The WebSocket Close
/// frame must carry code 4001 (not 1005 / no-status-received), so pusher-js
/// resolves `getCloseAction` → `"refused"` rather than `null → backoff`.
#[tokio::test]
async fn ws_unknown_app_key_close_frame_carries_4001() {
    use tokio_tungstenite::tungstenite::Message;
    let addr = spawn().await;
    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{addr}/app/no-such-key?protocol=7"))
            .await
            .unwrap();

    // Drain frames until we see a Close.
    let mut close_code: Option<u16> = None;
    while let Some(Ok(msg)) = ws.next().await {
        if let Message::Close(frame) = msg {
            close_code = frame.map(|f| u16::from(f.code));
            break;
        }
    }
    assert_eq!(
        close_code,
        Some(4001),
        "unknown-app-key reject must close with code 4001 (P13), got: {close_code:?}"
    );
}
