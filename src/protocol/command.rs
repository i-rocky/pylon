//! Inbound commands, decoded from any protocol version (version-agnostic).

use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum ClientCommand {
    Ping,
    Subscribe {
        channel: String,
        auth: Option<String>,
        channel_data: Option<String>,
    },
    Unsubscribe {
        channel: String,
    },
    /// `client-*` event on a channel (validated at the handler).
    ClientEvent {
        event: String,
        channel: String,
        data: Value,
    },
    /// `pusher:signin` — bind this connection to a user (handled in
    /// `ws::signin`).
    Signin {
        auth: String,
        user_data: String,
    },
    /// Unrecognized event name (e.g. `pusher:pong`); logged and ignored.
    Unknown(String),
}
