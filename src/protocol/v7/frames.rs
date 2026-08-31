//! v7 wire (de)serialization. Every `data` is double-encoded EXCEPT pusher:error.

use crate::protocol::codec::DecodeError;
use crate::protocol::command::ClientCommand;
use crate::protocol::event::ServerEvent;
use serde_json::{json, Value};

/// Convenience wrapper: encode into a fresh `String`. Delegates to
/// [`encode_into`], so both paths are byte-identical by construction.
pub fn encode(event: &ServerEvent) -> String {
    let mut out = String::new();
    encode_into(event, &mut out);
    out
}

/// `io::Write` adapter appending UTF-8 slices into a `String`, so
/// `serde_json::to_writer` serializes straight into the append buffer with no
/// intermediate `String` (which the delegating `encode`/`push_str` shape would
/// otherwise allocate per frame). The crate is `#![deny(unsafe_code)]`, so the
/// bytes go through a CHECKED `str::from_utf8` — serde_json only ever emits
/// valid UTF-8, making the check free in practice.
struct StrWriter<'a>(&'a mut String);

impl std::io::Write for StrWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let s = std::str::from_utf8(buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        self.0.push_str(s);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Serialize one finished frame `Value` compactly into `out`. Produces the
/// exact bytes `Value::to_string` always produced (`to_string` IS `to_writer`
/// into a `Vec<u8>`), so the golden equivalence with the old encoder holds
/// byte-for-byte. Infallible in practice: the only error source is the
/// adapter's UTF-8 check, which serde_json output can never trip — the
/// `debug_assert` keeps that contract observable in tests instead of the
/// result being silently discarded.
fn write_frame(out: &mut String, frame: Value) {
    let res = serde_json::to_writer(StrWriter(out), &frame);
    debug_assert!(res.is_ok(), "serde_json emitted invalid UTF-8: {res:?}");
}

/// Append the wire form of `event` to `out` WITHOUT clearing it (append
/// semantics — callers may reuse one buffer across many frames).
///
/// The [`ServerEvent::Raw`] arm appends the payload by reference: the relayed
/// frame (redis pubsub receive → fan-out) is shared by `Arc` and reused
/// verbatim, so encoding the SAME event for N subscribers performs N appends
/// into their respective buffers but ZERO clones of the shared payload
/// (F6 / Task 6.4). Every other arm serializes exactly the JSON [`encode`]
/// always produced.
pub fn encode_into(event: &ServerEvent, out: &mut String) {
    match event {
        ServerEvent::ConnectionEstablished {
            socket_id,
            activity_timeout,
        } => {
            let data =
                json!({ "socket_id": socket_id.as_str(), "activity_timeout": activity_timeout })
                    .to_string();
            write_frame(
                out,
                json!({ "event": "pusher:connection_established", "data": data }),
            );
        }
        ServerEvent::Ping => write_frame(out, json!({ "event": "pusher:ping", "data": {} })),
        ServerEvent::Pong => write_frame(out, json!({ "event": "pusher:pong", "data": {} })),
        ServerEvent::SubscriptionSucceeded { channel, presence } => {
            let data = match presence {
                None => String::new(),
                Some(p) => {
                    json!({ "presence": { "ids": p.ids, "hash": p.hash, "count": p.count } })
                        .to_string()
                }
            };
            write_frame(
                out,
                json!({ "event": "pusher_internal:subscription_succeeded", "channel": channel, "data": data }),
            );
        }
        ServerEvent::SubscriptionCount { channel, count } => {
            let data = json!({ "subscription_count": count }).to_string();
            write_frame(
                out,
                json!({ "event": "pusher_internal:subscription_count", "channel": channel, "data": data }),
            );
        }
        ServerEvent::Error(e) => {
            write_frame(
                out,
                json!({ "event": "pusher:error", "data": { "code": e.code, "message": e.message } }),
            );
        }
        ServerEvent::ChannelEvent {
            channel,
            event,
            data,
            user_id,
        } => {
            let mut frame = json!({ "event": event, "channel": channel, "data": data });
            // Presence client-events carry the originator's `user_id` at the top
            // level. Emit the key ONLY when present — never as `null`.
            if let Some(uid) = user_id {
                frame["user_id"] = json!(uid);
            }
            write_frame(out, frame);
        }
        ServerEvent::SubscriptionError {
            channel,
            error_type,
            error,
            status,
        } => {
            write_frame(
                out,
                json!({
                    "event": "pusher:subscription_error",
                    "channel": channel,
                    "data": { "type": error_type, "error": error, "status": status }
                }),
            );
        }
        ServerEvent::MemberAdded {
            channel,
            user_id,
            user_info,
        } => {
            let data = json!({ "user_id": user_id, "user_info": user_info }).to_string();
            write_frame(
                out,
                json!({ "event": "pusher_internal:member_added", "channel": channel, "data": data }),
            );
        }
        ServerEvent::MemberRemoved { channel, user_id } => {
            let data = json!({ "user_id": user_id }).to_string();
            write_frame(
                out,
                json!({ "event": "pusher_internal:member_removed", "channel": channel, "data": data }),
            );
        }
        ServerEvent::CacheMiss { channel } => {
            write_frame(
                out,
                json!({ "event": "pusher:cache_miss", "channel": channel }),
            );
        }
        ServerEvent::SigninSuccess { user_data } => {
            write_frame(
                out,
                json!({ "event": "pusher:signin_success", "data": { "user_data": user_data } }),
            );
        }
        ServerEvent::WatchlistEvents { events } => {
            let events: Vec<Value> = events
                .iter()
                .map(|e| json!({ "name": e.name, "user_ids": e.user_ids }))
                .collect();
            write_frame(
                out,
                json!({ "event": "pusher_internal:watchlist_events", "data": { "events": events } }),
            );
        }
        ServerEvent::ClientEventError { code, message } => {
            // Strict Pusher parity (1.8/P8): pusher:error is EXACTLY
            // { event, data } — the protocol defines no top-level `channel`
            // (channel-scoping belongs to pusher:subscription_error only).
            write_frame(
                out,
                json!({ "event": "pusher:error", "data": { "code": code, "message": message } }),
            );
        }
        // Control frame — the connection task intercepts `Close` before encoding,
        // so this arm is unreachable in practice; present only for exhaustiveness.
        ServerEvent::Close { .. } => {}
        // Already a finished v7 wire frame (produced on the originating node and
        // relayed verbatim by the Redis adapter): append it byte-for-byte BY
        // REFERENCE — `&Arc<str>` deref-coerces to `&str` for `push_str`, so
        // the shared payload is never cloned per subscriber (F6 / Task 6.4).
        ServerEvent::Raw(s) => out.push_str(s),
    }
}

pub fn decode(text: &str) -> Result<ClientCommand, DecodeError> {
    let v: Value = serde_json::from_str(text)?;
    let event = v
        .get("event")
        .and_then(Value::as_str)
        .ok_or(DecodeError::MissingField("event"))?;
    match event {
        "pusher:ping" => Ok(ClientCommand::Ping),
        "pusher:subscribe" => {
            let data = v.get("data").ok_or(DecodeError::MissingField("data"))?;
            let channel = data
                .get("channel")
                .and_then(Value::as_str)
                .ok_or(DecodeError::MissingField("channel"))?
                .to_string();
            let auth = data.get("auth").and_then(Value::as_str).map(String::from);
            let channel_data = data
                .get("channel_data")
                .and_then(Value::as_str)
                .map(String::from);
            Ok(ClientCommand::Subscribe {
                channel,
                auth,
                channel_data,
            })
        }
        "pusher:unsubscribe" => {
            let data = v.get("data").ok_or(DecodeError::MissingField("data"))?;
            let channel = data
                .get("channel")
                .and_then(Value::as_str)
                .ok_or(DecodeError::MissingField("channel"))?
                .to_string();
            Ok(ClientCommand::Unsubscribe { channel })
        }
        "pusher:signin" => {
            let data = v.get("data").ok_or(DecodeError::MissingField("data"))?;
            let auth = data
                .get("auth")
                .and_then(Value::as_str)
                .ok_or(DecodeError::MissingField("auth"))?
                .to_string();
            let user_data = data
                .get("user_data")
                .and_then(Value::as_str)
                .ok_or(DecodeError::MissingField("user_data"))?
                .to_string();
            Ok(ClientCommand::Signin { auth, user_data })
        }
        name if name.starts_with("client-") => {
            let channel = v
                .get("channel")
                .and_then(Value::as_str)
                .ok_or(DecodeError::MissingField("channel"))?
                .to_string();
            let data = v.get("data").cloned().unwrap_or(Value::Null);
            Ok(ClientCommand::ClientEvent {
                event: name.to_string(),
                channel,
                data,
            })
        }
        other => Ok(ClientCommand::Unknown(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::error::PusherError;
    use crate::protocol::event::ServerEvent;
    use crate::protocol::socket_id::SocketId;
    use serde_json::Value;

    fn parse(s: &str) -> Value {
        serde_json::from_str(s).unwrap()
    }

    /// Assert the frame's top-level object has EXACTLY `keys` — strict Pusher
    /// parity: no extra, non-standard fields (e.g. no `channel` on pusher:error).
    fn assert_exact_keys(v: &Value, keys: &[&str]) {
        let mut got: Vec<&str> = v
            .as_object()
            .expect("frame must be an object")
            .keys()
            .map(String::as_str)
            .collect();
        got.sort_unstable();
        let mut want: Vec<&str> = keys.to_vec();
        want.sort_unstable();
        assert_eq!(
            got, want,
            "top-level keys must be exactly {want:?}, got {got:?}"
        );
    }

    #[test]
    fn raw_arc_encodes_verbatim() {
        use std::sync::Arc;
        let frame: Arc<str> = Arc::from("{\"event\":\"x\"}");
        let ev = crate::protocol::event::ServerEvent::Raw(frame);
        assert_eq!(encode(&ev), "{\"event\":\"x\"}");
    }

    /// Golden drift-proof (F6 / Task 6.4, fix round): every `ServerEvent`
    /// variant, built with FIXED inputs, must append EXACTLY the literal wire
    /// bytes pinned below. The literals were derived from the pre-change
    /// encoder's output and eyeballed against the Pusher v7 wire shapes:
    /// double-encoded `data` STRINGS on connection_established /
    /// subscription_succeeded / subscription_count / member_added /
    /// member_removed; object `data` on pusher:error / subscription_error /
    /// signin_success; `user_id` present only on the presence client-event;
    /// cache_miss with no `data` key; `Close` empty (intercepted upstream);
    /// `Raw` verbatim.
    ///
    /// Unlike an encode-vs-encode_into comparison — tautological now that
    /// `encode` delegates to `encode_into` — these literals pin the WIRE FORMAT
    /// itself: any edit to a `json!` arm or to `write_frame` that changes a
    /// single byte fails here until the change is consciously re-pinned.
    /// (Field order is insertion order via the workspace-unified serde_json
    /// `preserve_order` feature; if that feature set ever changes, this test
    /// flags the byte-order drift too.)
    ///
    /// Also proves APPEND semantics per variant: each frame encoded at an
    /// offset behind a sentinel prefix must yield `"<sentinel>" + literal`.
    #[test]
    fn every_variant_appends_its_pinned_wire_literal() {
        use crate::protocol::event::{PresencePayload, WatchlistChange};
        use std::sync::Arc;

        let mut presence_hash = serde_json::Map::new();
        presence_hash.insert("u1".into(), serde_json::json!({"name":"Ann"}));

        let cases: Vec<(ServerEvent, &str)> = vec![
            (
                ServerEvent::ConnectionEstablished {
                    socket_id: SocketId::from_raw("123.456"),
                    activity_timeout: 120,
                },
                r#"{"event":"pusher:connection_established","data":"{\"socket_id\":\"123.456\",\"activity_timeout\":120}"}"#,
            ),
            (ServerEvent::Ping, r#"{"event":"pusher:ping","data":{}}"#),
            (ServerEvent::Pong, r#"{"event":"pusher:pong","data":{}}"#),
            (
                ServerEvent::SubscriptionSucceeded {
                    channel: "test-channel".into(),
                    presence: None,
                },
                r#"{"event":"pusher_internal:subscription_succeeded","channel":"test-channel","data":""}"#,
            ),
            (
                ServerEvent::SubscriptionSucceeded {
                    channel: "presence-test".into(),
                    presence: Some(PresencePayload {
                        ids: vec!["u1".into()],
                        hash: presence_hash,
                        count: 1,
                    }),
                },
                r#"{"event":"pusher_internal:subscription_succeeded","channel":"presence-test","data":"{\"presence\":{\"ids\":[\"u1\"],\"hash\":{\"u1\":{\"name\":\"Ann\"}},\"count\":1}}"}"#,
            ),
            (
                ServerEvent::SubscriptionCount {
                    channel: "test-channel".into(),
                    count: 2,
                },
                r#"{"event":"pusher_internal:subscription_count","channel":"test-channel","data":"{\"subscription_count\":2}"}"#,
            ),
            (
                ServerEvent::Error(PusherError::app_not_found()),
                r#"{"event":"pusher:error","data":{"code":4001,"message":"Could not find app by key"}}"#,
            ),
            (
                ServerEvent::ChannelEvent {
                    channel: "test-channel".into(),
                    event: "client-test".into(),
                    data: serde_json::json!({"msg":"hello"}),
                    user_id: None,
                },
                r#"{"event":"client-test","channel":"test-channel","data":{"msg":"hello"}}"#,
            ),
            (
                ServerEvent::ChannelEvent {
                    channel: "presence-test".into(),
                    event: "client-test".into(),
                    data: serde_json::json!({"msg":"hello"}),
                    user_id: Some("u1".into()),
                },
                r#"{"event":"client-test","channel":"presence-test","data":{"msg":"hello"},"user_id":"u1"}"#,
            ),
            (
                ServerEvent::SubscriptionError {
                    channel: "private-test".into(),
                    error_type: "AuthError".into(),
                    error: "Invalid signature".into(),
                    status: 401,
                },
                r#"{"event":"pusher:subscription_error","channel":"private-test","data":{"type":"AuthError","error":"Invalid signature","status":401}}"#,
            ),
            (
                ServerEvent::MemberAdded {
                    channel: "presence-test".into(),
                    user_id: "u1".into(),
                    user_info: serde_json::json!({"name":"Ann"}),
                },
                r#"{"event":"pusher_internal:member_added","channel":"presence-test","data":"{\"user_id\":\"u1\",\"user_info\":{\"name\":\"Ann\"}}"}"#,
            ),
            (
                ServerEvent::MemberRemoved {
                    channel: "presence-test".into(),
                    user_id: "u1".into(),
                },
                r#"{"event":"pusher_internal:member_removed","channel":"presence-test","data":"{\"user_id\":\"u1\"}"}"#,
            ),
            (
                ServerEvent::CacheMiss {
                    channel: "cache-test".into(),
                },
                r#"{"event":"pusher:cache_miss","channel":"cache-test"}"#,
            ),
            (
                ServerEvent::SigninSuccess {
                    user_data: r#"{"id":"7"}"#.into(),
                },
                r#"{"event":"pusher:signin_success","data":{"user_data":"{\"id\":\"7\"}"}}"#,
            ),
            (
                ServerEvent::WatchlistEvents {
                    events: vec![WatchlistChange {
                        name: "online".into(),
                        user_ids: vec!["7".into()],
                    }],
                },
                r#"{"event":"pusher_internal:watchlist_events","data":{"events":[{"name":"online","user_ids":["7"]}]}}"#,
            ),
            (
                ServerEvent::ClientEventError {
                    code: 4301,
                    message: "Client event rejected due to rate limit".into(),
                },
                r#"{"event":"pusher:error","data":{"code":4301,"message":"Client event rejected due to rate limit"}}"#,
            ),
            // Intercepted by the connection task before encoding; the encoder
            // is an exhaustive no-op.
            (
                ServerEvent::Close {
                    code: 4009,
                    reason: "x".into(),
                },
                "",
            ),
            // A relayed frame is appended VERBATIM (keys here deliberately in
            // non-insertion order to show it is passthrough, not re-serialized).
            (
                ServerEvent::Raw(Arc::from(
                    r#"{"channel":"test-channel","data":"{\"msg\":\"relayed\"}","event":"client-relayed"}"#,
                )),
                r#"{"channel":"test-channel","data":"{\"msg\":\"relayed\"}","event":"client-relayed"}"#,
            ),
        ];

        for (ev, literal) in &cases {
            // (1) append into an EMPTY buffer == the pinned literal.
            let mut fresh = String::new();
            encode_into(ev, &mut fresh);
            assert_eq!(&fresh, literal, "wire bytes drifted for {ev:?}");
            // (2) APPEND semantics: the sentinel prefix survives and the
            //     literal lands AT THE OFFSET — never overwrites the front.
            let mut at_offset = String::from("<sentinel>");
            encode_into(ev, &mut at_offset);
            assert_eq!(
                at_offset,
                format!("<sentinel>{literal}"),
                "encode_into must append at the offset for {ev:?}"
            );
            // (3) the delegating convenience must agree with the literal too —
            //     a REAL cross-check now that the expected value is external.
            assert_eq!(encode(ev), *literal, "encode() drifted for {ev:?}");
        }
    }

    #[test]
    fn connection_established_double_encodes_data() {
        let id = SocketId::generate();
        let out = parse(&encode(&ServerEvent::ConnectionEstablished {
            socket_id: id,
            activity_timeout: 120,
        }));
        assert_eq!(out["event"], "pusher:connection_established");
        let data = parse(out["data"].as_str().expect("data is a stringified JSON"));
        assert_eq!(data["socket_id"], id.as_str());
        assert_eq!(data["activity_timeout"], 120);
    }

    #[test]
    fn ping_frame() {
        let out = parse(&encode(&ServerEvent::Ping));
        assert_eq!(out["event"], "pusher:ping");
        assert!(out["data"].is_object());
    }

    #[test]
    fn pong_frame() {
        let out = parse(&encode(&ServerEvent::Pong));
        assert_eq!(out["event"], "pusher:pong");
        assert!(out["data"].is_object());
    }

    #[test]
    fn public_subscription_succeeded_has_empty_string_data() {
        let out = parse(&encode(&ServerEvent::SubscriptionSucceeded {
            channel: "c".into(),
            presence: None,
        }));
        assert_eq!(out["event"], "pusher_internal:subscription_succeeded");
        assert_eq!(out["channel"], "c");
        assert_eq!(out["data"], ""); // empty string per spec
    }

    #[test]
    fn subscription_count_double_encodes() {
        let out = parse(&encode(&ServerEvent::SubscriptionCount {
            channel: "c".into(),
            count: 2,
        }));
        assert_eq!(out["event"], "pusher_internal:subscription_count");
        let data = parse(out["data"].as_str().unwrap());
        assert_eq!(data["subscription_count"], 2);
    }

    #[test]
    fn error_data_is_object_not_string() {
        let out = parse(&encode(&ServerEvent::Error(PusherError::app_not_found())));
        assert_eq!(out["event"], "pusher:error");
        assert!(
            out["data"].is_object(),
            "error data must be an object, not stringified"
        );
        assert_eq!(out["data"]["code"], 4001);
    }

    use crate::protocol::command::ClientCommand;

    #[test]
    fn decodes_ping() {
        assert_eq!(
            decode(r#"{"event":"pusher:ping","data":{}}"#).unwrap(),
            ClientCommand::Ping
        );
    }

    #[test]
    fn decodes_public_subscribe() {
        let cmd =
            decode(r#"{"event":"pusher:subscribe","data":{"channel":"my-channel"}}"#).unwrap();
        assert_eq!(
            cmd,
            ClientCommand::Subscribe {
                channel: "my-channel".into(),
                auth: None,
                channel_data: None
            }
        );
    }

    #[test]
    fn decodes_unsubscribe() {
        let cmd = decode(r#"{"event":"pusher:unsubscribe","data":{"channel":"c"}}"#).unwrap();
        assert_eq!(
            cmd,
            ClientCommand::Unsubscribe {
                channel: "c".into()
            }
        );
    }

    #[test]
    fn decodes_client_event() {
        let cmd = decode(r#"{"event":"client-foo","channel":"private-x","data":{"a":1}}"#).unwrap();
        match cmd {
            ClientCommand::ClientEvent { event, channel, .. } => {
                assert_eq!(event, "client-foo");
                assert_eq!(channel, "private-x");
            }
            other => panic!("expected ClientEvent, got {other:?}"),
        }
    }

    #[test]
    fn unknown_event_is_unknown() {
        assert_eq!(
            decode(r#"{"event":"pusher:pong"}"#).unwrap(),
            ClientCommand::Unknown("pusher:pong".into())
        );
    }

    #[test]
    fn subscription_error_data_is_object() {
        let out = parse(&encode(&ServerEvent::SubscriptionError {
            channel: "private-x".into(),
            error_type: "AuthError".into(),
            error: "Invalid signature".into(),
            status: 401,
        }));
        assert_eq!(out["event"], "pusher:subscription_error");
        assert_eq!(out["channel"], "private-x");
        // subscription_error is its OWN event type and legitimately carries
        // `channel` at top level — exact shape {event, channel, data}.
        assert_exact_keys(&out, &["event", "channel", "data"]);
        assert!(
            out["data"].is_object(),
            "subscription_error data must be an object"
        );
        assert_eq!(out["data"]["type"], "AuthError");
        assert_eq!(out["data"]["status"], 401);
    }

    #[test]
    fn member_added_double_encodes() {
        let out = parse(&encode(&ServerEvent::MemberAdded {
            channel: "presence-x".into(),
            user_id: "u1".into(),
            user_info: serde_json::json!({"name":"Ann"}),
        }));
        assert_eq!(out["event"], "pusher_internal:member_added");
        assert_eq!(out["channel"], "presence-x");
        let data = parse(out["data"].as_str().expect("data is stringified JSON"));
        assert_eq!(data["user_id"], "u1");
        assert_eq!(data["user_info"]["name"], "Ann");
    }

    #[test]
    fn member_removed_double_encodes_user_id_only() {
        let out = parse(&encode(&ServerEvent::MemberRemoved {
            channel: "presence-x".into(),
            user_id: "u1".into(),
        }));
        assert_eq!(out["event"], "pusher_internal:member_removed");
        let data = parse(out["data"].as_str().unwrap());
        assert_eq!(data["user_id"], "u1");
        assert!(data.get("user_info").is_none());
    }

    #[test]
    fn cache_miss_frame_has_no_data_field() {
        let out = parse(&encode(&ServerEvent::CacheMiss {
            channel: "cache-x".into(),
        }));
        assert_eq!(out["event"], "pusher:cache_miss");
        assert_eq!(out["channel"], "cache-x");
        assert!(
            out.get("data").is_none(),
            "cache_miss carries no data field"
        );
    }

    #[test]
    fn decodes_signin() {
        let cmd = decode(
            r#"{"event":"pusher:signin","data":{"auth":"k:sig","user_data":"{\"id\":\"7\"}"}}"#,
        )
        .unwrap();
        assert_eq!(
            cmd,
            ClientCommand::Signin {
                auth: "k:sig".into(),
                user_data: r#"{"id":"7"}"#.into()
            }
        );
    }

    #[test]
    fn signin_success_data_is_object_with_user_data_string() {
        let out = parse(&encode(&ServerEvent::SigninSuccess {
            user_data: r#"{"id":"7"}"#.into(),
        }));
        assert_eq!(out["event"], "pusher:signin_success");
        assert!(
            out["data"].is_object(),
            "signin_success data is a plain object"
        );
        assert_eq!(out["data"]["user_data"], r#"{"id":"7"}"#);
    }

    #[test]
    fn watchlist_events_frame_is_connection_level_object() {
        use crate::protocol::event::WatchlistChange;
        let out = parse(&encode(&ServerEvent::WatchlistEvents {
            events: vec![WatchlistChange {
                name: "online".into(),
                user_ids: vec!["7".into()],
            }],
        }));
        assert_eq!(out["event"], "pusher_internal:watchlist_events");
        assert!(
            out.get("channel").is_none(),
            "watchlist events are not channel-scoped"
        );
        assert!(out["data"].is_object());
        assert_eq!(out["data"]["events"][0]["name"], "online");
        assert_eq!(out["data"]["events"][0]["user_ids"][0], "7");
    }

    #[test]
    fn pusher_error_frames_have_exactly_event_and_data() {
        // Task 1.8 (P8): the protocol page defines pusher:error as
        // `{ "event": "pusher:error", "data": { "message": String, "code": Integer } }`
        // — NO top-level `channel`, for connection-level AND channel-scoped
        // (client-event rejection) errors alike.
        let conn = parse(&encode(&ServerEvent::Error(PusherError::app_not_found())));
        assert_eq!(conn["event"], "pusher:error");
        assert_exact_keys(&conn, &["event", "data"]);

        let scoped = parse(&encode(&ServerEvent::ClientEventError {
            code: 4301,
            message: "Client event rejected due to rate limit".into(),
        }));
        assert_eq!(scoped["event"], "pusher:error");
        assert_exact_keys(&scoped, &["event", "data"]);
        assert_eq!(scoped["data"]["code"], 4301);
        assert_eq!(
            scoped["data"]["message"],
            "Client event rejected due to rate limit"
        );
    }

    #[test]
    fn close_encodes_to_empty_text() {
        // Close is intercepted by the connection task; encode is a no-op safety net.
        assert_eq!(
            encode(&ServerEvent::Close {
                code: 4009,
                reason: "x".into()
            }),
            ""
        );
    }

    #[test]
    fn presence_subscription_succeeded_double_encodes_roster() {
        use crate::protocol::event::PresencePayload;
        let mut hash = serde_json::Map::new();
        hash.insert("u1".into(), serde_json::json!({"name":"Ann"}));
        let out = parse(&encode(&ServerEvent::SubscriptionSucceeded {
            channel: "presence-x".into(),
            presence: Some(PresencePayload {
                ids: vec!["u1".into()],
                hash,
                count: 1,
            }),
        }));
        let data = parse(out["data"].as_str().unwrap());
        assert_eq!(data["presence"]["count"], 1);
        assert_eq!(data["presence"]["ids"][0], "u1");
        assert_eq!(data["presence"]["hash"]["u1"]["name"], "Ann");
    }
}
