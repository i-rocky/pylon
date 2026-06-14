//! In-process integration tests driving the server with a real WS client.

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::auth::signature::channel_signature;
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::server::router::{build_router, AppState};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_tungstenite::tungstenite::Message;

const SECRET: &str = "app-secret";
const KEY: &str = "app-key";

fn auth_token(socket_id: &str, channel: &str, channel_data: Option<&str>) -> String {
    format!(
        "{KEY}:{}",
        channel_signature(SECRET, socket_id, channel, channel_data)
    )
}

async fn established_socket_id(ws: &mut Ws) -> String {
    let frame = next_json(ws).await; // connection_established
    let data: Value = serde_json::from_str(frame["data"].as_str().unwrap()).unwrap();
    data["socket_id"].as_str().unwrap().to_string()
}

/// Read frames until one with the given event name arrives, skipping others
/// (e.g. the interleaved pusher_internal:subscription_count frames emitted
/// because the test app has subscription_count_enabled = true).
async fn next_event_named(ws: &mut Ws, event: &str) -> Value {
    loop {
        let f = next_json(ws).await;
        if f["event"] == event {
            return f;
        }
    }
}

const APPS: &str = r#"[
    {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
     "capacity":2,"client_messages_enabled":true,"subscription_count_enabled":true}
]"#;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn spawn(config: ServerConfig) -> SocketAddr {
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let registry = Arc::new(Registry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry));
    let state = AppState {
        config,
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

async fn connect(addr: SocketAddr, query: &str) -> Ws {
    let url = format!("ws://{addr}/app/app-key{query}");
    let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    ws
}

async fn next_json(ws: &mut Ws) -> Value {
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(t) => return serde_json::from_str(&t).unwrap(),
            Message::Close(_) => panic!("unexpected close while awaiting a frame"),
            _ => continue,
        }
    }
}

async fn send_json(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string())).await.unwrap();
}

#[tokio::test]
async fn connection_established_on_connect() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "pusher:connection_established");
    let data: Value = serde_json::from_str(frame["data"].as_str().unwrap()).unwrap();
    assert!(data["socket_id"].as_str().unwrap().contains('.'));
    assert_eq!(data["activity_timeout"], 120);
}

#[tokio::test]
async fn ping_pong() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = next_json(&mut ws).await; // established
    send_json(&mut ws, json!({ "event": "pusher:ping", "data": {} })).await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "pusher:pong");
}

#[tokio::test]
async fn public_subscribe_succeeds() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = next_json(&mut ws).await;
    send_json(
        &mut ws,
        json!({ "event": "pusher:subscribe", "data": { "channel": "room" } }),
    )
    .await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "pusher_internal:subscription_succeeded");
    assert_eq!(frame["channel"], "room");
    assert_eq!(frame["data"], ""); // empty-string data for non-presence
}

#[tokio::test]
async fn subscription_count_broadcast_to_all_subscribers() {
    let addr = spawn(ServerConfig::default()).await; // subscription_count_enabled = true
    let mut a = connect(addr, "?protocol=7").await;
    let _ = next_json(&mut a).await; // established
    send_json(
        &mut a,
        json!({ "event": "pusher:subscribe", "data": { "channel": "room" } }),
    )
    .await;
    let _succeeded = next_json(&mut a).await; // subscription_succeeded
    let count1 = next_json(&mut a).await; // count = 1 (a is the only subscriber)
    assert_eq!(count1["event"], "pusher_internal:subscription_count");
    let c1: Value = serde_json::from_str(count1["data"].as_str().unwrap()).unwrap();
    assert_eq!(c1["subscription_count"], 1);

    // a second subscriber joins -> existing subscriber `a` receives an updated count
    let mut b = connect(addr, "?protocol=7").await;
    let _ = next_json(&mut b).await; // established
    send_json(
        &mut b,
        json!({ "event": "pusher:subscribe", "data": { "channel": "room" } }),
    )
    .await;
    let count2 = next_json(&mut a).await;
    assert_eq!(count2["event"], "pusher_internal:subscription_count");
    let c2: Value = serde_json::from_str(count2["data"].as_str().unwrap()).unwrap();
    assert_eq!(c2["subscription_count"], 2);
}

#[tokio::test]
async fn unknown_app_key_errors_4001() {
    let addr = spawn(ServerConfig::default()).await;
    let url = format!("ws://{addr}/app/nope?protocol=7");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "pusher:error");
    assert_eq!(frame["data"]["code"], 4001);
}

#[tokio::test]
async fn unsupported_protocol_errors_4007() {
    let addr = spawn(ServerConfig::default()).await;
    let url = format!("ws://{addr}/app/app-key?protocol=3");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "pusher:error");
    assert_eq!(frame["data"]["code"], 4007);
}

#[tokio::test]
async fn over_capacity_errors_4004() {
    let addr = spawn(ServerConfig::default()).await; // capacity = 2
    let mut a = connect(addr, "?protocol=7").await;
    let _ = next_json(&mut a).await;
    let mut b = connect(addr, "?protocol=7").await;
    let _ = next_json(&mut b).await;
    let mut c = connect(addr, "?protocol=7").await;
    let frame = next_json(&mut c).await;
    assert_eq!(frame["event"], "pusher:error");
    assert_eq!(frame["data"]["code"], 4004);
}

#[tokio::test]
async fn idle_connection_closed_4201() {
    let config = ServerConfig {
        activity_timeout: 1,
        pong_timeout: 1,
        ..ServerConfig::default()
    };
    let addr = spawn(config).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let est = next_json(&mut ws).await;
    assert_eq!(est["event"], "pusher:connection_established");

    // Stay silent. Server pings after ~1s, then closes ~1s later with a real
    // WebSocket Close frame carrying code 4201.
    // (tokio-tungstenite auto-replies to protocol-level Pings, but pusher:ping is
    //  an application Text frame, so the server gets no pong and must close.)
    let mut saw_ping = false;
    let mut saw_close_4201 = false;
    for _ in 0..6 {
        match tokio::time::timeout(std::time::Duration::from_secs(3), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["event"] == "pusher:ping" {
                    saw_ping = true;
                }
                // After the fix, the server must NOT emit a pusher:error 4201 text
                // frame — it must send a WebSocket Close frame instead.
                if v["event"] == "pusher:error" {
                    panic!("server sent pusher:error text frame instead of a WS Close frame");
                }
            }
            Ok(Some(Ok(Message::Close(Some(cf))))) => {
                assert_eq!(
                    u16::from(cf.code),
                    4201,
                    "expected WS close code 4201, got {}",
                    cf.code
                );
                saw_close_4201 = true;
                break;
            }
            Ok(Some(Ok(Message::Close(None)))) | Ok(None) => break,
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) => break,
            Err(_) => break, // timed out
        }
    }
    assert!(saw_ping, "server should have sent a pusher:ping");
    assert!(
        saw_close_4201,
        "server should have closed with WS close code 4201"
    );
}

#[tokio::test]
async fn root_route_identifies_server() {
    let addr = spawn(ServerConfig::default()).await;
    let body = reqwest::get(format!("http://{addr}/"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.to_lowercase().contains("pylon"));
}

#[tokio::test]
async fn private_subscribe_invalid_auth_is_non_fatal() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = established_socket_id(&mut ws).await;
    send_json(
        &mut ws,
        json!({
            "event": "pusher:subscribe",
            "data": { "channel": "private-x", "auth": "app-key:bad" }
        }),
    )
    .await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "pusher:subscription_error");
    assert_eq!(frame["channel"], "private-x");
    assert_eq!(frame["data"]["status"], 401);
    // Connection still works: a ping is answered.
    send_json(&mut ws, json!({ "event": "pusher:ping", "data": {} })).await;
    assert_eq!(next_json(&mut ws).await["event"], "pusher:pong");
}

#[tokio::test]
async fn private_subscribe_valid_auth_succeeds() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let sid = established_socket_id(&mut ws).await;
    let token = auth_token(&sid, "private-x", None);
    send_json(
        &mut ws,
        json!({
            "event": "pusher:subscribe",
            "data": { "channel": "private-x", "auth": token }
        }),
    )
    .await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "pusher_internal:subscription_succeeded");
    assert_eq!(frame["channel"], "private-x");
}

#[tokio::test]
async fn presence_member_added_and_removed() {
    let addr = spawn(ServerConfig::default()).await;

    // First member.
    let mut a = connect(addr, "?protocol=7").await;
    let sid_a = established_socket_id(&mut a).await;
    let cd_a = r#"{"user_id":"ua","user_info":{"n":"A"}}"#;
    send_json(
        &mut a,
        json!({
            "event": "pusher:subscribe",
            "data": {
                "channel": "presence-room",
                "auth": auth_token(&sid_a, "presence-room", Some(cd_a)),
                "channel_data": cd_a
            }
        }),
    )
    .await;
    let succ = next_json(&mut a).await;
    assert_eq!(succ["event"], "pusher_internal:subscription_succeeded");
    let roster: Value = serde_json::from_str(succ["data"].as_str().unwrap()).unwrap();
    assert_eq!(roster["presence"]["count"], 1);

    // Second member joins -> a receives member_added for ub.
    let mut b = connect(addr, "?protocol=7").await;
    let sid_b = established_socket_id(&mut b).await;
    let cd_b = r#"{"user_id":"ub","user_info":{"n":"B"}}"#;
    send_json(
        &mut b,
        json!({
            "event": "pusher:subscribe",
            "data": {
                "channel": "presence-room",
                "auth": auth_token(&sid_b, "presence-room", Some(cd_b)),
                "channel_data": cd_b
            }
        }),
    )
    .await;
    let _ = next_json(&mut b).await; // b's own subscription_succeeded
    let added = next_event_named(&mut a, "pusher_internal:member_added").await;
    assert_eq!(added["event"], "pusher_internal:member_added");
    let added_data: Value = serde_json::from_str(added["data"].as_str().unwrap()).unwrap();
    assert_eq!(added_data["user_id"], "ub");

    // b disconnects -> a receives member_removed for ub.
    drop(b);
    let removed = next_event_named(&mut a, "pusher_internal:member_removed").await;
    assert_eq!(removed["event"], "pusher_internal:member_removed");
    let removed_data: Value = serde_json::from_str(removed["data"].as_str().unwrap()).unwrap();
    assert_eq!(removed_data["user_id"], "ub");
}

#[tokio::test]
async fn client_event_broadcast_excludes_sender() {
    let addr = spawn(ServerConfig::default()).await;
    let mut a = connect(addr, "?protocol=7").await;
    let sid_a = established_socket_id(&mut a).await;
    let mut b = connect(addr, "?protocol=7").await;
    let sid_b = established_socket_id(&mut b).await;
    for (ws, sid) in [(&mut a, &sid_a), (&mut b, &sid_b)] {
        send_json(
            ws,
            json!({
                "event": "pusher:subscribe",
                "data": { "channel": "private-x", "auth": auth_token(sid, "private-x", None) }
            }),
        )
        .await;
        let _ = next_json(ws).await; // subscription_succeeded
    }
    send_json(
        &mut a,
        json!({
            "event": "client-greet", "channel": "private-x", "data": { "hi": true }
        }),
    )
    .await;
    let got = next_event_named(&mut b, "client-greet").await;
    assert_eq!(got["event"], "client-greet");
    assert_eq!(got["channel"], "private-x");
    // a (the sender) must not receive its own client event; a ping round-trips instead.
    send_json(&mut a, json!({ "event": "pusher:ping", "data": {} })).await;
    assert_eq!(
        next_event_named(&mut a, "pusher:pong").await["event"],
        "pusher:pong"
    );
}

#[tokio::test]
async fn malformed_frame_silently_dropped_connection_stays_alive() {
    // P2: sending an unparseable text frame must NOT produce a pusher:error 4200
    // in-band event. The server must drop the frame silently and keep the
    // connection alive (a subsequent ping/pong must succeed).
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = established_socket_id(&mut ws).await; // consume connection_established

    // Send a garbage text frame that cannot be decoded as a valid Pusher command.
    ws.send(Message::Text("not json at all".into()))
        .await
        .unwrap();

    // Give the server a brief window to (incorrectly) emit a pusher:error frame.
    // If it does, we catch it here and fail the test.
    let maybe_err = tokio::time::timeout(std::time::Duration::from_millis(300), ws.next()).await;

    match maybe_err {
        Ok(Some(Ok(Message::Text(t)))) => {
            let v: Value = serde_json::from_str(&t).unwrap_or(Value::Null);
            if v["event"] == "pusher:error" {
                panic!(
                    "server emitted pusher:error in response to malformed frame \
                     (got code {}); should have silently dropped it",
                    v["data"]["code"]
                );
            }
            // Any other text frame (unexpected but not the bug) — fall through.
        }
        Ok(Some(Ok(Message::Close(_)))) => {
            panic!("server closed connection on malformed frame; should silently drop");
        }
        // Timeout (no frame) or non-text frames are fine.
        _ => {}
    }

    // Connection must still be alive: a ping must get a pong.
    send_json(&mut ws, json!({ "event": "pusher:ping", "data": {} })).await;
    let pong = next_json(&mut ws).await;
    assert_eq!(
        pong["event"], "pusher:pong",
        "connection should remain alive after malformed frame"
    );
}
