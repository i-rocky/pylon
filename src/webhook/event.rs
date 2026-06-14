//! The six webhook triggers and their exact JSON serialization (spec §4).
//! Each `WebhookEvent` carries the app id (used to route at flush) plus the
//! per-event fields; `to_json()` produces the object that goes in the envelope's
//! `events` array, byte-shaped to match what pusher-http-node consumers expect.

use serde_json::{json, Value};

/// One webhook trigger. The `app` field routes the trigger to its app's config
/// at flush time; it is NOT serialized into the wire object.
#[derive(Debug, Clone, PartialEq)]
pub enum WebhookEvent {
    ChannelOccupied {
        app: String,
        channel: String,
    },
    ChannelVacated {
        app: String,
        channel: String,
    },
    MemberAdded {
        app: String,
        channel: String,
        user_id: String,
    },
    MemberRemoved {
        app: String,
        channel: String,
        user_id: String,
    },
    ClientEvent {
        app: String,
        channel: String,
        event: String,
        data: Value,
        socket_id: String,
        /// Present only when the sender is a presence member of `channel`.
        user_id: Option<String>,
    },
    CacheMiss {
        app: String,
        channel: String,
    },
}

impl WebhookEvent {
    /// The app id this trigger belongs to (used to route at flush; not serialized).
    pub fn app(&self) -> &str {
        match self {
            WebhookEvent::ChannelOccupied { app, .. }
            | WebhookEvent::ChannelVacated { app, .. }
            | WebhookEvent::MemberAdded { app, .. }
            | WebhookEvent::MemberRemoved { app, .. }
            | WebhookEvent::ClientEvent { app, .. }
            | WebhookEvent::CacheMiss { app, .. } => app,
        }
    }

    /// The `name` field of this event's wire object (also the `event_types` key).
    pub fn name(&self) -> &'static str {
        match self {
            WebhookEvent::ChannelOccupied { .. } => "channel_occupied",
            WebhookEvent::ChannelVacated { .. } => "channel_vacated",
            WebhookEvent::MemberAdded { .. } => "member_added",
            WebhookEvent::MemberRemoved { .. } => "member_removed",
            WebhookEvent::ClientEvent { .. } => "client_event",
            WebhookEvent::CacheMiss { .. } => "cache_miss",
        }
    }

    /// The exact JSON object placed in the envelope's `events` array (spec §4).
    pub fn to_json(&self) -> Value {
        match self {
            WebhookEvent::ChannelOccupied { channel, .. } => {
                json!({ "name": "channel_occupied", "channel": channel })
            }
            WebhookEvent::ChannelVacated { channel, .. } => {
                json!({ "name": "channel_vacated", "channel": channel })
            }
            WebhookEvent::MemberAdded {
                channel, user_id, ..
            } => json!({ "name": "member_added", "channel": channel, "user_id": user_id }),
            WebhookEvent::MemberRemoved {
                channel, user_id, ..
            } => json!({ "name": "member_removed", "channel": channel, "user_id": user_id }),
            WebhookEvent::ClientEvent {
                channel,
                event,
                data,
                socket_id,
                user_id,
                ..
            } => {
                let mut obj = serde_json::Map::new();
                obj.insert("name".into(), Value::String("client_event".into()));
                obj.insert("channel".into(), Value::String(channel.clone()));
                obj.insert("event".into(), Value::String(event.clone()));
                obj.insert("data".into(), data.clone());
                obj.insert("socket_id".into(), Value::String(socket_id.clone()));
                if let Some(uid) = user_id {
                    obj.insert("user_id".into(), Value::String(uid.clone()));
                }
                Value::Object(obj)
            }
            WebhookEvent::CacheMiss { channel, .. } => {
                json!({ "name": "cache_miss", "channel": channel })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_occupied_and_vacated_serialize() {
        assert_eq!(
            WebhookEvent::ChannelOccupied {
                app: "a".into(),
                channel: "ch".into(),
            }
            .to_json(),
            json!({ "name": "channel_occupied", "channel": "ch" })
        );
        assert_eq!(
            WebhookEvent::ChannelVacated {
                app: "a".into(),
                channel: "ch".into(),
            }
            .to_json(),
            json!({ "name": "channel_vacated", "channel": "ch" })
        );
    }

    #[test]
    fn member_added_and_removed_serialize_with_user_id() {
        assert_eq!(
            WebhookEvent::MemberAdded {
                app: "a".into(),
                channel: "presence-x".into(),
                user_id: "u1".into(),
            }
            .to_json(),
            json!({ "name": "member_added", "channel": "presence-x", "user_id": "u1" })
        );
        assert_eq!(
            WebhookEvent::MemberRemoved {
                app: "a".into(),
                channel: "presence-x".into(),
                user_id: "u1".into(),
            }
            .to_json(),
            json!({ "name": "member_removed", "channel": "presence-x", "user_id": "u1" })
        );
    }

    #[test]
    fn cache_miss_serializes_channel_only() {
        assert_eq!(
            WebhookEvent::CacheMiss {
                app: "a".into(),
                channel: "cache-x".into(),
            }
            .to_json(),
            json!({ "name": "cache_miss", "channel": "cache-x" })
        );
    }

    #[test]
    fn client_event_omits_user_id_when_absent() {
        let v = WebhookEvent::ClientEvent {
            app: "a".into(),
            channel: "private-c".into(),
            event: "client-msg".into(),
            data: json!({"k":"v"}),
            socket_id: "123.456".into(),
            user_id: None,
        }
        .to_json();
        assert_eq!(
            v,
            json!({
                "name": "client_event",
                "channel": "private-c",
                "event": "client-msg",
                "data": {"k":"v"},
                "socket_id": "123.456"
            })
        );
        assert!(
            v.get("user_id").is_none(),
            "user_id must be omitted, not null"
        );
    }

    #[test]
    fn client_event_includes_user_id_when_present_and_data_verbatim() {
        let v = WebhookEvent::ClientEvent {
            app: "a".into(),
            channel: "presence-c".into(),
            event: "client-msg".into(),
            // a JSON STRING payload must survive verbatim (not re-parsed).
            data: Value::String("{\"raw\":1}".into()),
            socket_id: "9.9".into(),
            user_id: Some("u7".into()),
        }
        .to_json();
        assert_eq!(
            v,
            json!({
                "name": "client_event",
                "channel": "presence-c",
                "event": "client-msg",
                "data": "{\"raw\":1}",
                "socket_id": "9.9",
                "user_id": "u7"
            })
        );
    }
}
