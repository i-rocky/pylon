//! Outbound events, encoded by the negotiated codec (version-agnostic).

use crate::protocol::error::PusherError;
use crate::protocol::socket_id::SocketId;
use serde_json::{Map, Value};

/// Presence roster. Empty in SP1 (no presence channels yet); filled in SP2.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PresencePayload {
    pub ids: Vec<String>,
    pub hash: Map<String, Value>,
    pub count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ServerEvent {
    ConnectionEstablished {
        socket_id: SocketId,
        activity_timeout: u32,
    },
    Ping,
    Pong,
    SubscriptionSucceeded {
        channel: String,
        presence: Option<PresencePayload>,
    },
    SubscriptionCount {
        channel: String,
        count: usize,
    },
    Error(PusherError),
    /// Generic channel delivery (client events SP2, REST triggers SP2).
    ChannelEvent {
        channel: String,
        event: String,
        data: Value,
    },
    /// `pusher:subscription_error` — non-fatal, channel-scoped. Data is an OBJECT.
    SubscriptionError {
        channel: String,
        error_type: String,
        error: String,
        status: u16,
    },
    /// `pusher_internal:member_added` (presence). Data double-encoded.
    MemberAdded {
        channel: String,
        user_id: String,
        user_info: Value,
    },
    /// `pusher_internal:member_removed` (presence). Data double-encoded.
    MemberRemoved {
        channel: String,
        user_id: String,
    },
    /// `pusher:cache_miss` — sent to a new cache-channel subscriber when no event
    /// is cached. Carries only the channel (no `data` field).
    CacheMiss {
        channel: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_construct_and_match() {
        let e = ServerEvent::SubscriptionSucceeded {
            channel: "c".into(),
            presence: None,
        };
        match e {
            ServerEvent::SubscriptionSucceeded { channel, presence } => {
                assert_eq!(channel, "c");
                assert!(presence.is_none());
            }
            _ => panic!("wrong variant"),
        }
    }
}
