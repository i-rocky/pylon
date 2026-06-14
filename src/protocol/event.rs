//! Outbound events, encoded by the negotiated codec (version-agnostic).

use crate::protocol::error::PusherError;
use crate::protocol::socket_id::SocketId;
use serde_json::{Map, Value};

/// One online/offline change inside a `watchlist_events` batch.
#[derive(Debug, Clone, PartialEq)]
pub struct WatchlistChange {
    /// "online" or "offline".
    pub name: String,
    pub user_ids: Vec<String>,
}

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
    /// `user_id` is the originator's presence `user_id`, present ONLY on a
    /// presence client-event broadcast (pusher-js `presence_channel.ts` reads
    /// `event.user_id` → `metadata.user_id`). `None` everywhere else; the v7
    /// encoder omits the key when `None` (never emits `null`).
    ChannelEvent {
        channel: String,
        event: String,
        data: Value,
        user_id: Option<String>,
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
    /// `pusher:signin_success` — connection-level; `data` is a plain object
    /// `{ "user_data": "<echoed string>" }`. pusher-js (`user.ts:99-101`) reads
    /// only `data.user_data`, so we echo just that. (soketi `ws-handler.ts:688`
    /// echoes the entire incoming `data` including the `auth` token; we
    /// intentionally do NOT reflect the credential back.)
    SigninSuccess {
        user_data: String,
    },
    /// `pusher_internal:watchlist_events` — connection-level; data is a plain
    /// object `{ "events": [ { "name", "user_ids" } ] }` (pusher-js
    /// watchlist.ts:17-29 reads `data.events[].name` / `.user_ids`).
    WatchlistEvents {
        events: Vec<WatchlistChange>,
    },
    /// `pusher:error` scoped to a channel (client-event rejections: 4301).
    /// Unlike the connection-level `Error`, this carries the `channel` so the
    /// wire frame matches soketi: `{ event, channel, data: { code, message } }`.
    ClientEventError {
        channel: String,
        code: u16,
        message: String,
    },
    /// A control directive: write a WebSocket Close frame with this code/reason
    /// and end the connection. Intercepted by the connection task — never encoded
    /// to text. Used for fatal `4009` paths (bad signin, terminate).
    Close {
        code: u16,
        reason: String,
    },
    /// A pre-encoded v7 wire frame delivered verbatim. Used by the Redis adapter
    /// to fan a broadcast out to local sockets on a REMOTE node without
    /// re-encoding (the originating node already produced the exact frame).
    Raw(std::sync::Arc<str>),
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
