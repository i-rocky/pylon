//! End-to-end watchlist lifecycle over real WebSockets.
//!
//! The spawn/connect helpers live in `tests/common/mod.rs` and run the percore
//! worker fleet (the only transport). Exercises the signed-in watchlist flow
//! against the live server:
//!   1. online/offline events cross between two signed-in users; and
//!   2. an oversized watchlist yields a non-fatal 4302 (connection survives).

mod common;
use common::*;

use pylon::auth::signature::user_signature;
use pylon::server::config::ServerConfig;
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use futures_util::StreamExt;

// capacity 10: this suite runs several simultaneous clients
const APPS_C10: &str = r#"[
    {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
     "capacity":10,"client_messages_enabled":true,"subscription_count_enabled":true}
]"#;

/// Spawn the capacity-10 app on the selected transport.
async fn spawn(config: ServerConfig) -> SocketAddr {
    common::spawn(SpawnSpec::with_apps(config, APPS_C10)).await
}

/// Read frames for a short window, skipping non-text frames; returns the first
/// text frame parsed as JSON, or None if the window elapses with no text frame.
/// Used for "assert nothing arrives" checks — short timeout so the test stays fast.
async fn try_next_json_short(ws: &mut Ws) -> Option<Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(400);
    loop {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return Some(serde_json::from_str(&t).unwrap()),
            Ok(Some(Ok(_))) => continue, // skip ping/pong/binary, keep waiting
            Ok(Some(Err(_))) | Ok(None) => return None, // stream closed/errored
            Err(_) => return None,       // deadline elapsed: nothing arrived
        }
    }
}

/// Sign and send a `pusher:signin` for the EXACT `user_data` string, then read
/// and assert the `pusher:signin_success` ack.
async fn signin(ws: &mut Ws, socket_id: &str, user_data: &str) {
    let auth = format!("{KEY}:{}", user_signature(SECRET, socket_id, user_data));
    send_json(
        ws,
        json!({
            "event": "pusher:signin",
            // user_data is a STRING value inside data, not a nested object.
            "data": { "auth": auth, "user_data": user_data }
        }),
    )
    .await;
    let ack = next_json(ws).await;
    assert_eq!(ack["event"], "pusher:signin_success");
}

/// Test 1 — online then offline cross between two signed-in users.
///
/// A watches B. B is offline at A's signin (no snapshot, clean stream). When B
/// signs in, A gets `online [B]`; when B's socket drops, A gets `offline [B]`.
#[tokio::test]
async fn watchlist_online_then_offline_across_two_users() {
    let addr = spawn(ServerConfig::default()).await;

    // Client A signs in watching B.
    let mut a = connect(addr, "?protocol=7").await;
    let socket_id_a = established_socket_id(&mut a).await;
    signin(&mut a, &socket_id_a, r#"{"id":"A","watchlist":["B"]}"#).await;

    // B is offline -> A must NOT receive any watchlist snapshot yet.
    assert!(
        try_next_json_short(&mut a).await.is_none(),
        "A must get no watchlist frame while B is offline"
    );

    // Client B signs in -> brings user B online.
    let mut b = connect(addr, "?protocol=7").await;
    let socket_id_b = established_socket_id(&mut b).await;
    signin(&mut b, &socket_id_b, r#"{"id":"B"}"#).await;

    // A receives the `online` event for B.
    let online = next_json(&mut a).await;
    assert_eq!(online["event"], "pusher_internal:watchlist_events");
    assert!(
        online.get("channel").is_none(),
        "watchlist is connection-level"
    );
    assert_eq!(online["data"]["events"][0]["name"], "online");
    assert_eq!(online["data"]["events"][0]["user_ids"], json!(["B"]));

    // Drop B's socket so the server observes a disconnect and runs on_close.
    drop(b);

    // A receives the `offline` event for B.
    let offline = next_json(&mut a).await;
    assert_eq!(offline["event"], "pusher_internal:watchlist_events");
    assert_eq!(offline["data"]["events"][0]["name"], "offline");
    assert_eq!(offline["data"]["events"][0]["user_ids"], json!(["B"]));
}

/// Test 2 — an oversized watchlist yields a non-fatal 4302 after signin_success,
/// and the connection stays open (proven by a ping/pong round-trip).
#[tokio::test]
async fn watchlist_overflow_emits_4302_but_connection_survives() {
    let config = ServerConfig::default();
    let max = config.max_watchlist_size; // 100
    let addr = spawn(config).await;

    let mut ws = connect(addr, "?protocol=7").await;
    let socket_id = established_socket_id(&mut ws).await;

    // Build user_data with max+1 = 101 watchlist ids. Serialize ONCE so the
    // string we sign is byte-identical to the string we send.
    let watchlist: Vec<String> = (0..=max).map(|i| format!("w{i}")).collect();
    assert_eq!(watchlist.len(), max + 1);
    let user_data = json!({ "id": "X", "watchlist": watchlist }).to_string();

    let auth = format!("{KEY}:{}", user_signature(SECRET, &socket_id, &user_data));
    send_json(
        &mut ws,
        json!({
            "event": "pusher:signin",
            "data": { "auth": auth, "user_data": user_data }
        }),
    )
    .await;

    // Order: signin_success FIRST, then the non-fatal 4302 overflow error.
    let ack = next_json(&mut ws).await;
    assert_eq!(ack["event"], "pusher:signin_success");

    let err = next_json(&mut ws).await;
    assert_eq!(err["event"], "pusher:error");
    assert_eq!(err["data"]["code"], 4302);

    // The connection must STAY OPEN: a ping must round-trip to a pong.
    send_json(&mut ws, json!({ "event": "pusher:ping", "data": {} })).await;
    let pong = next_json(&mut ws).await;
    assert_eq!(
        pong["event"], "pusher:pong",
        "connection must survive 4302 and answer ping with pong"
    );
}
