//! Broadcast envelope for cross-node pub/sub. One node PUBLISHes this;
//! other nodes receive it, check `is_from`, and route to local sockets.

/// Discriminator selecting how a receiver routes an [`Envelope`]. Defaults to
/// `Broadcast` so legacy SP7a/b payloads (written before this field existed)
/// decode unchanged. For the user-directed kinds, `Envelope::channel` carries
/// the target `user_id` rather than a channel name.
#[derive(Debug, Clone, Copy, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub enum EnvelopeKind {
    #[default]
    Broadcast,
    UserSend,
    UserTerminate,
    WatchOnline,
    WatchOffline,
}

/// Serialized payload published on a Redis PubSub channel. Receivers use
/// `is_from` to drop messages they published themselves (self-dedup) and
/// honour `except` to skip one socket even when relaying a remote event.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Envelope {
    pub node_id: String,
    pub app: String,
    #[serde(default)]
    pub kind: EnvelopeKind,
    pub channel: String,
    pub event: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub except: Option<String>,
    /// Additive (F16): the pre-encoded v7 frame as base64 of its RAW bytes.
    /// `event` still carries the frame as a JSON string — its exact pre-F16
    /// shape — so the two fields travel together and BOTH old and new nodes
    /// relay correctly during a mixed-version rolling upgrade:
    /// * an OLD receiver ignores this unknown field (the struct does NOT use
    ///   `deny_unknown_fields`, so serde skips it) and reads `event`;
    /// * a NEW receiver prefers this field ([`Envelope::frame`]) because base64
    ///   needs no JSON escape/unescape round-trip, falling back to `event`
    ///   when an old sender emitted none.
    ///
    /// `None` for the non-frame kinds (`event` is `Null` there).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub frame_b64: Option<String>,
}

impl Envelope {
    /// Standard-alphabet base64 of the raw frame bytes — the value of the
    /// additive `frame_b64` field.
    pub fn encode_frame_b64(frame: &str) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(frame.as_bytes())
    }

    /// Serialize to JSON bytes for PUBLISH.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Envelope is always serializable")
    }

    /// Deserialize from the bytes received in a SUBSCRIBE message.
    pub fn decode(bytes: &[u8]) -> serde_json::Result<Envelope> {
        serde_json::from_slice(bytes)
    }

    /// The pre-encoded v7 frame this envelope carries, if any — `None` for the
    /// non-frame kinds. Prefers the additive `frame_b64` (base64 of the raw
    /// frame bytes: no JSON string escaping on either end) and falls back to
    /// the legacy `event` JSON string, so a mixed-version cluster relays in
    /// both directions. A malformed `frame_b64` (impossible from our senders)
    /// or invalid UTF-8 after decode falls back to the legacy field rather
    /// than dropping the frame.
    pub fn frame(&self) -> Option<std::sync::Arc<str>> {
        use base64::Engine as _;
        if let Some(b64) = self.frame_b64.as_deref() {
            if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(b64) {
                if let Ok(frame) = String::from_utf8(bytes) {
                    return Some(std::sync::Arc::from(frame));
                }
            }
        }
        self.event
            .as_str()
            .map(|s| std::sync::Arc::from(s.to_string()))
    }

    /// Returns `true` when this envelope was published by the local node
    /// (`my_node_id`). The receiver should drop the message in that case.
    pub fn is_from(&self, my_node_id: &str) -> bool {
        self.node_id == my_node_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip_and_self_dedup() {
        let e = Envelope {
            node_id: "n1".into(),
            app: "app1".into(),
            kind: EnvelopeKind::Broadcast,
            channel: "public-room".into(),
            event: serde_json::json!({"event":"x","channel":"public-room","data":"{}"}),
            except: Some("9.9".into()),
            frame_b64: None,
        };
        let bytes = e.encode();
        let got = Envelope::decode(&bytes).unwrap();
        assert_eq!(got.node_id, "n1");
        assert_eq!(got.app, "app1");
        assert_eq!(got.channel, "public-room");
        assert_eq!(got.except.as_deref(), Some("9.9"));
        assert_eq!(
            got.event,
            serde_json::json!({"event":"x","channel":"public-room","data":"{}"})
        );
        assert!(got.is_from("n1")); // self -> drop
        assert!(!got.is_from("n2")); // remote -> deliver
    }
    #[test]
    fn except_none_roundtrips() {
        let e = Envelope {
            node_id: "n2".into(),
            app: "a".into(),
            kind: EnvelopeKind::Broadcast,
            channel: "c".into(),
            event: serde_json::json!({"k":1}),
            except: None,
            frame_b64: None,
        };
        let got = Envelope::decode(&e.encode()).unwrap();
        assert_eq!(got.except, None);
    }
    #[test]
    fn kind_defaults_to_broadcast_for_legacy_payloads() {
        // A payload written before SP7c (no `kind`) must decode as Broadcast.
        let legacy = br#"{"node_id":"n1","app":"a","channel":"c","event":{"k":1}}"#;
        let got = Envelope::decode(legacy).unwrap();
        assert_eq!(got.kind, EnvelopeKind::Broadcast);
    }
    #[test]
    fn user_kind_roundtrips() {
        let e = Envelope {
            node_id: "n1".into(),
            app: "a".into(),
            kind: EnvelopeKind::UserSend,
            channel: "user-7".into(),
            event: serde_json::json!("frame"),
            except: None,
            frame_b64: None,
        };
        assert_eq!(
            Envelope::decode(&e.encode()).unwrap().kind,
            EnvelopeKind::UserSend
        );
    }

    /// The pre-F16 envelope shape, field-for-field (same serde derives as the old
    /// `Envelope`). Decoding the NEW payload into this struct proves an OLD node —
    /// whose binary deserializes exactly this shape with serde's default
    /// ignore-unknown-fields behavior — tolerates the additive `frame_b64`.
    #[derive(serde::Deserialize)]
    struct OldEnvelopeShape {
        node_id: String,
        app: String,
        #[serde(default)]
        kind: EnvelopeKind,
        channel: String,
        event: serde_json::Value,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        except: Option<String>,
    }

    fn frame_payload() -> String {
        // A representative v7 frame: JSON with quotes/backslashes/braces, i.e.
        // exactly the bytes the legacy `event` JSON-string field had to escape.
        r#"{"event":"client-greeting","channel":"private-room","data":"{\"hello\":\"world\"}"}"#
            .to_string()
    }

    #[test]
    fn frame_b64_roundtrips_alongside_event() {
        let frame = frame_payload();
        let e = Envelope {
            node_id: "n1".into(),
            app: "a".into(),
            kind: EnvelopeKind::Broadcast,
            channel: "private-room".into(),
            event: serde_json::Value::String(frame.clone()),
            except: None,
            frame_b64: Some(Envelope::encode_frame_b64(&frame)),
        };
        let got = Envelope::decode(&e.encode()).unwrap();
        assert_eq!(
            got.frame_b64.as_deref(),
            Some(Envelope::encode_frame_b64(&frame).as_str())
        );
        // The receiver prefers frame_b64 and reconstructs the EXACT frame bytes.
        assert_eq!(got.frame().as_deref(), Some(frame.as_str()));
    }

    #[test]
    fn new_receiver_decodes_old_envelope_without_frame_b64() {
        // OLD sender payload: no frame_b64 at all. The new receiver must fall back
        // to the legacy `event` string.
        let frame = frame_payload();
        let old = format!(
            r#"{{"node_id":"n1","app":"a","channel":"c","event":{}}}"#,
            serde_json::Value::String(frame.clone())
        );
        let got = Envelope::decode(old.as_bytes()).unwrap();
        assert_eq!(
            got.frame_b64, None,
            "old payload must decode with no b64 field"
        );
        assert_eq!(got.frame().as_deref(), Some(frame.as_str()));
    }

    #[test]
    fn old_receiver_ignores_frame_b64() {
        // NEW sender payload (both fields). An OLD receiver — modeled by the
        // field-for-field pre-F16 struct above with serde's default unknown-field
        // tolerance — decodes it fine and still reads the untouched `event`.
        let frame = frame_payload();
        let e = Envelope {
            node_id: "n1".into(),
            app: "a".into(),
            kind: EnvelopeKind::UserSend,
            channel: "u7".into(),
            event: serde_json::Value::String(frame.clone()),
            except: None,
            frame_b64: Some(Envelope::encode_frame_b64(&frame)),
        };
        let old: OldEnvelopeShape = serde_json::from_slice(&e.encode()).unwrap();
        assert_eq!(old.node_id, "n1");
        assert_eq!(old.app, "a");
        assert_eq!(old.kind, EnvelopeKind::UserSend);
        assert_eq!(old.channel, "u7");
        assert_eq!(old.except, None);
        assert_eq!(old.event, serde_json::Value::String(frame));
    }

    #[test]
    fn frame_b64_field_is_optional_and_skipped_when_none() {
        // A frame-carrying envelope with frame_b64 still set serializes BOTH fields;
        // the legacy `event` field keeps its exact pre-F16 shape (the frame as a
        // JSON string), so old receivers lose nothing.
        let frame = frame_payload();
        let e = Envelope {
            node_id: "n1".into(),
            app: "a".into(),
            kind: EnvelopeKind::Broadcast,
            channel: "c".into(),
            event: serde_json::Value::String(frame.clone()),
            except: None,
            frame_b64: Some(Envelope::encode_frame_b64(&frame)),
        };
        let json = serde_json::to_value(&e).unwrap();
        assert_eq!(
            json.get("event"),
            Some(&serde_json::Value::String(frame.clone())),
            "event field shape is unchanged"
        );
        assert!(json.get("frame_b64").is_some());
        // A non-frame envelope (event Null) omits the field entirely.
        let ctrl = Envelope {
            node_id: "n1".into(),
            app: "a".into(),
            kind: EnvelopeKind::UserTerminate,
            channel: "u7".into(),
            event: serde_json::Value::Null,
            except: None,
            frame_b64: None,
        };
        let ctrl_json = serde_json::to_value(&ctrl).unwrap();
        assert!(ctrl_json.get("frame_b64").is_none());
    }

    #[test]
    fn frame_prefers_b64_over_event_string() {
        // Preference order: when both are present the receiver MUST take
        // frame_b64 (base64 of the raw frame bytes), never the escaped event
        // string — pinned by making the two deliberately disagree.
        let raw = frame_payload();
        let e = Envelope {
            node_id: "n1".into(),
            app: "a".into(),
            kind: EnvelopeKind::Broadcast,
            channel: "c".into(),
            event: serde_json::Value::String("stale-legacy-string".into()),
            except: None,
            frame_b64: Some(Envelope::encode_frame_b64(&raw)),
        };
        let got = Envelope::decode(&e.encode()).unwrap();
        assert_eq!(got.frame().as_deref(), Some(raw.as_str()));
    }
}
