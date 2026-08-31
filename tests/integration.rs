//! In-process integration tests driving the server with a real WS client.
//!
//! The spawn/connect/helper plumbing lives in `tests/common/mod.rs`; the
//! `spawn_default` helper runs the percore worker fleet (the only transport), so
//! this suite is the single-node percore proof.

mod common;
use common::*;

use futures_util::SinkExt;
use futures_util::StreamExt;
use pylon::server::config::ServerConfig;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::protocol::frame::coding::Data as WsData;
use tokio_tungstenite::tungstenite::protocol::frame::coding::OpCode as WsOpCode;
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::Frame as WsFrame;
use tokio_tungstenite::tungstenite::Message;

/// Spawn the standard capacity-2 app on the selected transport.
async fn spawn(config: ServerConfig) -> std::net::SocketAddr {
    spawn_default(config).await
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

/// `?version=` with no `?protocol=`: the server infers the protocol from the
/// library version's major ("7.4.1" → 7, per the Pusher protocol spec's
/// documented fallback for legacy JS clients) and the connection establishes.
#[tokio::test]
async fn version_param_infers_protocol_and_establishes() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?version=7.4.1").await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "pusher:connection_established");
    let data: Value = serde_json::from_str(frame["data"].as_str().unwrap()).unwrap();
    assert!(data["socket_id"].as_str().unwrap().contains('.'));
}

/// A malformed `version` string (no `protocol`) is a 4006 "Invalid version
/// string format" — scoped to unparseable version strings.
#[tokio::test]
async fn unparseable_version_errors_4006() {
    let addr = spawn(ServerConfig::default()).await;
    let url = format!("ws://{addr}/app/app-key?version=banana");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "pusher:error");
    assert_eq!(frame["data"]["code"], 4006);
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

/// Byte-stable roster ordering, end to end (F8 / Task 6.6): members joining in
/// NON-sorted order must receive `subscription_succeeded` frames with EXACTLY
/// the pinned bytes — ids byte-sorted, `hash` keys in the same order, `count`
/// = distinct users, escaping intact (emoji, embedded quotes) through the
/// double-encoded `data` string. The roster data carries no socket ids, so
/// the frames are fully deterministic. Literals captured from the pre-refactor
/// server; any byte drift is a parity regression. (Deeper ordering cases —
/// 4 members, escaping-worthy user_ids, same-user dedup, partial removal —
/// are pinned at unit level in `channel::state::tests::golden_*`.)
#[tokio::test]
async fn presence_roster_wire_bytes_pinned_across_join_order() {
    let addr = spawn(ServerConfig::default()).await;

    // First member: single-entry roster.
    let mut z = connect(addr, "?protocol=7").await;
    let sid_z = established_socket_id(&mut z).await;
    let cd_z = r#"{"user_id":"z-user","user_info":{"n":"z"}}"#;
    send_json(
        &mut z,
        json!({
            "event": "pusher:subscribe",
            "data": {
                "channel": "presence-roster",
                "auth": auth_token(&sid_z, "presence-roster", Some(cd_z)),
                "channel_data": cd_z
            }
        }),
    )
    .await;
    let raw = tokio::time::timeout(std::time::Duration::from_secs(5), z.next())
        .await
        .expect("frame within 5s")
        .expect("stream open")
        .expect("ws ok");
    let Message::Text(t) = raw else {
        panic!("expected a text frame")
    };
    assert_eq!(
        t,
        r#"{"event":"pusher_internal:subscription_succeeded","channel":"presence-roster","data":"{\"presence\":{\"ids\":[\"z-user\"],\"hash\":{\"z-user\":{\"n\":\"z\"}},\"count\":1}}"}"#
    );

    // Second member (joins after z, sorts before z): two-entry roster with
    // escaping-worthy user_info.
    let mut a = connect(addr, "?protocol=7").await;
    let sid_a = established_socket_id(&mut a).await;
    let cd_a = r#"{"user_id":"a-user","user_info":{"em":"🚀","q":"a\"b"}}"#;
    send_json(
        &mut a,
        json!({
            "event": "pusher:subscribe",
            "data": {
                "channel": "presence-roster",
                "auth": auth_token(&sid_a, "presence-roster", Some(cd_a)),
                "channel_data": cd_a
            }
        }),
    )
    .await;
    let raw = tokio::time::timeout(std::time::Duration::from_secs(5), a.next())
        .await
        .expect("frame within 5s")
        .expect("stream open")
        .expect("ws ok");
    let Message::Text(t) = raw else {
        panic!("expected a text frame")
    };
    assert_eq!(
        t,
        r#"{"event":"pusher_internal:subscription_succeeded","channel":"presence-roster","data":"{\"presence\":{\"ids\":[\"a-user\",\"z-user\"],\"hash\":{\"a-user\":{\"em\":\"🚀\",\"q\":\"a\\\"b\"},\"z-user\":{\"n\":\"z\"}},\"count\":2}}"}"#
    );
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

// ── P9 parity tests — client-event name length (max 200 chars) ──────────────

/// P9: a client-event with an event name over 200 chars must be dropped — the
/// other subscriber receives nothing (the connection stays alive).
#[tokio::test]
async fn ws_client_event_name_over_200_is_dropped() {
    let addr = spawn(ServerConfig::default()).await;
    let mut a = connect(addr, "?protocol=7").await;
    let sid_a = established_socket_id(&mut a).await;
    let mut b = connect(addr, "?protocol=7").await;
    let sid_b = established_socket_id(&mut b).await;

    // Both join the same private channel.
    for (ws, sid) in [(&mut a, &sid_a), (&mut b, &sid_b)] {
        send_json(
            ws,
            json!({
                "event": "pusher:subscribe",
                "data": { "channel": "private-x", "auth": auth_token(sid, "private-x", None) }
            }),
        )
        .await;
        // Drain: subscription_succeeded + possible subscription_count frame.
        let _ = next_event_named(ws, "pusher_internal:subscription_succeeded").await;
    }
    // Drain any lingering subscription_count frames from b's queue before the test.
    while tokio::time::timeout(std::time::Duration::from_millis(50), b.next())
        .await
        .is_ok()
    {}

    // a sends a client-event whose name is 201 chars (over the 200-char limit).
    let long_event = format!("client-{}", "a".repeat(194)); // "client-" (7) + 194 = 201
    send_json(
        &mut a,
        json!({ "event": long_event, "channel": "private-x", "data": {} }),
    )
    .await;

    // b must NOT receive anything within a short window.
    let got = tokio::time::timeout(std::time::Duration::from_millis(300), next_json(&mut b)).await;
    assert!(
        got.is_err(),
        "subscriber b must not receive a client-event with an oversized name"
    );

    // a's connection must still be alive.
    send_json(&mut a, json!({ "event": "pusher:ping", "data": {} })).await;
    assert_eq!(
        next_event_named(&mut a, "pusher:pong").await["event"],
        "pusher:pong",
        "connection must remain alive after oversized client-event name"
    );
}

/// P9: a client-event with an event name of exactly 200 chars IS broadcast.
#[tokio::test]
async fn ws_client_event_name_exactly_200_is_broadcast() {
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
                "data": { "channel": "private-y", "auth": auth_token(sid, "private-y", None) }
            }),
        )
        .await;
        let _ = next_json(ws).await; // subscription_succeeded
    }

    // "client-" (7) + 193 'a' chars = 200 total
    let exact_event = format!("client-{}", "a".repeat(193));
    send_json(
        &mut a,
        json!({ "event": exact_event.clone(), "channel": "private-y", "data": {} }),
    )
    .await;

    let got = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        next_event_named(&mut b, &exact_event),
    )
    .await
    .expect("b must receive a client-event with a 200-char name");
    assert_eq!(got["event"], exact_event);
    assert_eq!(got["channel"], "private-y");
}

// ── P1 parity tests — fragmented text messages (RFC 6455 §5.4) ──────────────

/// Send one raw WebSocket frame (the tokio-tungstenite writer applies the
/// mandatory client masking itself, RFC 6455 §5.1). This is how the tests
/// below split a message into FIN=0 / FIN=1 fragments.
async fn send_raw_frame(ws: &mut Ws, frame: WsFrame) {
    ws.send(Message::Frame(frame)).await.unwrap();
}

/// P1: a `pusher:subscribe` split across a FIN=0 Text frame and a FIN=1
/// Continuation frame must be reassembled into one protocol message and
/// answered with `pusher_internal:subscription_succeeded` (RFC 6455 §5.4).
#[tokio::test]
async fn fragmented_text_message_is_reassembled_and_dispatched() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = established_socket_id(&mut ws).await;

    // `pusher:subscribe` for "frag-ch", split mid-word.
    send_raw_frame(
        &mut ws,
        WsFrame::message(
            b"{\"event\":\"pusher:sub".to_vec(),
            WsOpCode::Data(WsData::Text),
            false,
        ),
    )
    .await;
    send_raw_frame(
        &mut ws,
        WsFrame::message(
            b"scribe\",\"data\":{\"channel\":\"frag-ch\"}}".to_vec(),
            WsOpCode::Data(WsData::Continue),
            true,
        ),
    )
    .await;

    let frame = next_event_named(&mut ws, "pusher_internal:subscription_succeeded").await;
    assert_eq!(frame["channel"], "frag-ch");
}

/// P1 / RFC 6455 §5.5.2: control frames interleaved between fragments must be
/// answered immediately — a Ping sent between the two fragments of a message
/// gets its Pong back BEFORE the completed message is dispatched.
#[tokio::test]
async fn ping_between_fragments_is_answered_before_message_completes() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = established_socket_id(&mut ws).await;

    send_raw_frame(
        &mut ws,
        WsFrame::message(
            b"{\"event\":\"pusher:sub".to_vec(),
            WsOpCode::Data(WsData::Text),
            false,
        ),
    )
    .await;
    ws.send(Message::Ping(b"mid-fragment".to_vec()))
        .await
        .unwrap();
    send_raw_frame(
        &mut ws,
        WsFrame::message(
            b"scribe\",\"data\":{\"channel\":\"frag-mid\"}}".to_vec(),
            WsOpCode::Data(WsData::Continue),
            true,
        ),
    )
    .await;

    // The very first inbound frame must be the mid-fragment Pong, not the
    // completed message's reply.
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .expect("a frame within 5s")
        .expect("stream open");
    match first {
        Ok(Message::Pong(p)) => assert_eq!(p.as_slice(), b"mid-fragment"),
        other => panic!("expected Pong before the message completes, got {other:?}"),
    }
    let frame = next_event_named(&mut ws, "pusher_internal:subscription_succeeded").await;
    assert_eq!(frame["channel"], "frag-mid");
}

/// P1 cap rule: the assembled message is capped at `max_event_payload_bytes`
/// (default 10 KiB) per message. An oversize assembled message is dropped and
/// the accumulator reset WITHOUT closing the connection — the follow-up
/// well-formed fragmented message below must still reassemble and dispatch.
#[tokio::test]
async fn oversize_assembled_message_is_dropped_and_connection_stays_usable() {
    let addr = spawn(ServerConfig::default()).await; // max_event_payload_bytes = 10_240
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = established_socket_id(&mut ws).await;

    // Two 8 KiB fragments: each alone is under the per-message cap, the
    // assembled 16 KiB message exceeds it.
    let first = format!(
        "{{\"event\":\"pusher:subscribe\",\"data\":{{\"channel\":\"cap-ch\",\"pad\":\"{}",
        "a".repeat(8 * 1024)
    );
    let second = format!("{}\"}}}}", "b".repeat(8 * 1024));
    send_raw_frame(
        &mut ws,
        WsFrame::message(first.into_bytes(), WsOpCode::Data(WsData::Text), false),
    )
    .await;
    send_raw_frame(
        &mut ws,
        WsFrame::message(second.into_bytes(), WsOpCode::Data(WsData::Continue), true),
    )
    .await;

    // The oversize message must be dropped silently: no error, no close, no reply.
    if let Some(v) = try_next_json_short(&mut ws).await {
        panic!("oversize assembled message must be dropped silently, got {v}");
    }

    // The accumulator was reset, so a small well-formed fragmented message
    // still reassembles and dispatches (proves the connection is usable AND
    // the accumulator was not left wedged open).
    send_raw_frame(
        &mut ws,
        WsFrame::message(
            b"{\"event\":\"pusher:sub".to_vec(),
            WsOpCode::Data(WsData::Text),
            false,
        ),
    )
    .await;
    send_raw_frame(
        &mut ws,
        WsFrame::message(
            b"scribe\",\"data\":{\"channel\":\"after-cap\"}}".to_vec(),
            WsOpCode::Data(WsData::Continue),
            true,
        ),
    )
    .await;
    let frame = next_event_named(&mut ws, "pusher_internal:subscription_succeeded").await;
    assert_eq!(frame["channel"], "after-cap");
}

/// P1 / RFC 6455 §5.4: a new Text data frame arriving while a fragmented
/// message is open is a protocol violation — the server must fail the
/// connection with a WebSocket Close carrying code 1002 (protocol error), and
/// the interleaved message must NOT be dispatched.
#[tokio::test]
async fn text_frame_while_fragment_open_closes_1002() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = established_socket_id(&mut ws).await;

    // Open a fragment...
    send_raw_frame(
        &mut ws,
        WsFrame::message(
            b"{\"event\":\"pusher:sub".to_vec(),
            WsOpCode::Data(WsData::Text),
            false,
        ),
    )
    .await;
    // ...then send a NEW complete Text message instead of the continuation.
    send_json(
        &mut ws,
        json!({ "event": "pusher:subscribe", "data": { "channel": "viol-ch" } }),
    )
    .await;

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .expect("a frame within 5s")
        .expect("stream open");
    match first {
        Ok(Message::Close(Some(cf))) => assert_eq!(
            u16::from(cf.code),
            1002,
            "expected close code 1002, got {}",
            cf.code
        ),
        other => panic!("expected Close(1002) on interleaved Text frame, got {other:?}"),
    }
}

/// P1 pin (RFC 6455 §5.4): a Continuation frame with NO fragmented message
/// open is a protocol violation — Close 1002, and nothing dispatched.
#[tokio::test]
async fn stray_continuation_with_no_fragment_open_closes_1002() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = established_socket_id(&mut ws).await;

    // No fragment was opened; a FIN=1 Continuation out of nowhere.
    send_raw_frame(
        &mut ws,
        WsFrame::message(b"garbage".to_vec(), WsOpCode::Data(WsData::Continue), true),
    )
    .await;

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .expect("a frame within 5s")
        .expect("stream open");
    match first {
        Ok(Message::Close(Some(cf))) => assert_eq!(
            u16::from(cf.code),
            1002,
            "expected close code 1002, got {}",
            cf.code
        ),
        other => panic!("expected Close(1002) on stray Continuation, got {other:?}"),
    }
}

/// P1 pin (RFC 6455 §5.4): a new Binary data frame arriving while a
/// fragmented TEXT message is open is a protocol violation — Close 1002.
#[tokio::test]
async fn binary_frame_while_text_fragment_open_closes_1002() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = established_socket_id(&mut ws).await;

    // Open a TEXT fragment...
    send_raw_frame(
        &mut ws,
        WsFrame::message(
            b"{\"event\":\"pusher:sub".to_vec(),
            WsOpCode::Data(WsData::Text),
            false,
        ),
    )
    .await;
    // ...then interleave a complete Binary frame instead of continuing.
    send_raw_frame(
        &mut ws,
        WsFrame::message(
            vec![0xDE, 0xAD, 0xBE, 0xEF],
            WsOpCode::Data(WsData::Binary),
            true,
        ),
    )
    .await;

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .expect("a frame within 5s")
        .expect("stream open");
    match first {
        Ok(Message::Close(Some(cf))) => assert_eq!(
            u16::from(cf.code),
            1002,
            "expected close code 1002, got {}",
            cf.code
        ),
        other => panic!("expected Close(1002) on interleaved Binary frame, got {other:?}"),
    }
}

/// P1 / R14: a FRAGMENTED Binary message (Binary FIN=0 … Continuation, RFC
/// 6455 §5.4) is ignored exactly like a lone Binary frame — binary is not
/// part of the Pusher protocol, so it must neither kill the connection nor
/// disturb the text protocol. After the fragmented binary completes, a
/// normal text subscribe on the SAME connection must succeed.
#[tokio::test]
async fn fragmented_binary_is_ignored_and_connection_stays_usable() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = established_socket_id(&mut ws).await;

    // A 3-frame fragmented binary: FIN=0 Binary, FIN=0 Continuation, FIN=1
    // Continuation. Every frame is ignored (no close, no reply).
    send_raw_frame(
        &mut ws,
        WsFrame::message(vec![1, 2, 3], WsOpCode::Data(WsData::Binary), false),
    )
    .await;
    send_raw_frame(
        &mut ws,
        WsFrame::message(vec![4, 5, 6], WsOpCode::Data(WsData::Continue), false),
    )
    .await;
    send_raw_frame(
        &mut ws,
        WsFrame::message(vec![7, 8], WsOpCode::Data(WsData::Continue), true),
    )
    .await;

    // The binary noise must be silently ignored: no error/close frame first.
    if let Some(v) = try_next_json_short(&mut ws).await {
        panic!("fragmented binary must be ignored silently, got {v}");
    }

    // The connection is still alive and the fragment state was not wedged:
    // a normal text subscribe succeeds.
    send_json(
        &mut ws,
        json!({ "event": "pusher:subscribe", "data": { "channel": "bin-ign-ch" } }),
    )
    .await;
    let frame = next_event_named(&mut ws, "pusher_internal:subscription_succeeded").await;
    assert_eq!(frame["channel"], "bin-ign-ch");
}

// ── P2 parity tests — closing handshake (RFC 6455 §5.5.1) ───────────────────

/// P2: on a client-initiated Close the server MUST echo a Close frame back
/// before tearing the socket down, carrying the client's close code. The
/// client sends a DISTINCTIVE code (4000, a wire-legal private-use code — not
/// the 1000 the parameterless-Close fallback produces) so this pins the
/// code-carrying branch of the echo: a regression that hardcodes 1000 fails.
#[tokio::test]
async fn client_initiated_close_is_echoed_before_teardown() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = established_socket_id(&mut ws).await;

    ws.send(Message::Close(Some(CloseFrame {
        code: 4000.into(),
        reason: "bye".into(),
    })))
    .await
    .unwrap();

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .expect("a frame within 5s")
        .expect("stream open");
    match first {
        Ok(Message::Close(Some(cf))) => assert_eq!(
            u16::from(cf.code),
            4000,
            "expected the echoed Close to carry the client's code 4000, got {}",
            cf.code
        ),
        other => panic!("expected Close(4000) echo before teardown, got {other:?}"),
    }
}

/// P2: a parameterless Close (no status code in the payload) is echoed with
/// code 1000 (normal closure) before teardown.
#[tokio::test]
async fn parameterless_close_is_echoed_with_1000() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = established_socket_id(&mut ws).await;

    ws.send(Message::Close(None)).await.unwrap();

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .expect("a frame within 5s")
        .expect("stream open");
    match first {
        Ok(Message::Close(Some(cf))) => assert_eq!(
            u16::from(cf.code),
            1000,
            "expected Close(1000) echo for a parameterless Close, got {}",
            cf.code
        ),
        other => panic!("expected Close(1000) echo for a parameterless Close, got {other:?}"),
    }
}

// ── P3 parity tests — invalid UTF-8 text messages (RFC 6455 §8.1) ───────────

/// Assert the connection is torn down after a fatal Close: the stream must
/// end (EOF or a transport error) with no further data frame after the Close.
async fn assert_stream_ends_after_close(ws: &mut Ws) {
    let tail = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next()).await;
    match tail {
        Ok(None) | Ok(Some(Err(_))) | Err(_) => {} // stream ended / errored: torn down
        Ok(Some(Ok(msg))) => panic!("connection must end after the fatal Close, got {msg:?}"),
    }
}

/// P3 / RFC 6455 §8.1: a Text frame whose payload is not valid UTF-8 must fail
/// the CONNECTION — a WebSocket Close carrying code 1007 (invalid frame payload
/// data). 1007 is a WS-level failure, not a Pusher protocol error, so no
/// `pusher:error` text frame may precede the Close: the Close must be the very
/// first frame the client receives, and the connection must be torn down.
#[tokio::test]
async fn non_utf8_text_frame_closes_1007() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = established_socket_id(&mut ws).await;

    // A raw Text frame with a payload that can never be valid UTF-8 (0xFF is
    // not a valid UTF-8 byte at all). Sent as a raw frame so the client
    // library's own UTF-8 validation does not reject it first.
    send_raw_frame(
        &mut ws,
        WsFrame::message(vec![0xFF, 0xFE], WsOpCode::Data(WsData::Text), true),
    )
    .await;

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .expect("a frame within 5s")
        .expect("stream open");
    match first {
        // Receiving the Close as the first frame also proves no pusher:error
        // frame was emitted for this WS-level failure.
        Ok(Message::Close(Some(cf))) => assert_eq!(
            u16::from(cf.code),
            1007,
            "expected Close(1007) for non-UTF-8 text, got {}",
            cf.code
        ),
        other => panic!("expected Close(1007) on non-UTF-8 text frame, got {other:?}"),
    }
    assert_stream_ends_after_close(&mut ws).await;
}

/// P3 / RFC 6455 §8.1 + §5.4: the UTF-8 validity check runs on the ASSEMBLED
/// payload of a fragmented text message. A message whose fragments reassemble
/// into invalid UTF-8 (here: an incomplete 3-byte sequence followed by 0xFF)
/// must fail the connection with Close 1007 once the FIN=1 Continuation
/// completes it — with no `pusher:error` frame and teardown afterwards.
#[tokio::test]
async fn fragmented_non_utf8_text_closes_1007() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = established_socket_id(&mut ws).await;

    // FIN=0 Text: 0xE2 0x82 is a truncated 3-byte sequence (invalid alone);
    // FIN=1 Continuation: 0xFF can never continue any sequence. Assembled the
    // message [0xE2, 0x82, 0xFF] is invalid UTF-8 as a WHOLE.
    send_raw_frame(
        &mut ws,
        WsFrame::message(vec![0xE2, 0x82], WsOpCode::Data(WsData::Text), false),
    )
    .await;
    send_raw_frame(
        &mut ws,
        WsFrame::message(vec![0xFF], WsOpCode::Data(WsData::Continue), true),
    )
    .await;

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .expect("a frame within 5s")
        .expect("stream open");
    match first {
        Ok(Message::Close(Some(cf))) => assert_eq!(
            u16::from(cf.code),
            1007,
            "expected Close(1007) for non-UTF-8 assembled text, got {}",
            cf.code
        ),
        other => panic!("expected Close(1007) on non-UTF-8 assembled text, got {other:?}"),
    }
    assert_stream_ends_after_close(&mut ws).await;
}
