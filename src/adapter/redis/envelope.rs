//! Broadcast envelope for cross-node pub/sub. One node PUBLISHes this;
//! other nodes receive it, check `is_from`, and route to local sockets.

/// Serialized payload published on a Redis PubSub channel. Receivers use
/// `is_from` to drop messages they published themselves (self-dedup) and
/// honour `except` to skip one socket even when relaying a remote event.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Envelope {
    pub node_id: String,
    pub app: String,
    pub channel: String,
    pub event: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub except: Option<String>,
}

impl Envelope {
    /// Serialize to JSON bytes for PUBLISH.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Envelope is always serializable")
    }

    /// Deserialize from the bytes received in a SUBSCRIBE message.
    pub fn decode(bytes: &[u8]) -> serde_json::Result<Envelope> {
        serde_json::from_slice(bytes)
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
            channel: "public-room".into(),
            event: serde_json::json!({"event":"x","channel":"public-room","data":"{}"}),
            except: Some("9.9".into()),
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
            channel: "c".into(),
            event: serde_json::json!({"k":1}),
            except: None,
        };
        let got = Envelope::decode(&e.encode()).unwrap();
        assert_eq!(got.except, None);
    }
}
