//! The version seam: each protocol version implements `Codec`.

use crate::protocol::command::ClientCommand;
use crate::protocol::event::ServerEvent;

/// What a given protocol version supports. This is the version feature set
/// the DISPATCH layer consults (U1 / Task 7.2): every feature the handler
/// family would otherwise take for granted (client events, presence,
/// encrypted/cache channels, user auth + watchlist) is behind one of these
/// flags, so a future version that lacks a feature degrades gracefully —
/// reusing the error frames the analogous v7 path already emits — instead of
/// assuming v7.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Capabilities {
    /// `client-*` events may be triggered over this connection.
    pub client_events: bool,
    /// `presence-*` channels may be subscribed to.
    pub presence: bool,
    /// `private-encrypted-*` channels may be subscribed to.
    pub encrypted_channels: bool,
    /// `cache-` channels replay / report `cache_miss` on subscribe.
    pub cache_channels: bool,
    /// `pusher:signin` (user authentication) is accepted.
    pub user_auth: bool,
    /// Watchlists in signin `user_data` are registered and reported.
    pub watchlist: bool,
}

impl Capabilities {
    /// The v7 profile: every feature on. Single source of truth for "v7
    /// supports everything" — [`crate::protocol::v7::V7Codec::capabilities`]
    /// returns this, and tests that build a `ConnectionContext` directly
    /// (mirroring what `finish_establish` does with the negotiated codec)
    /// start from this profile.
    pub fn v7() -> Self {
        Self {
            client_events: true,
            presence: true,
            encrypted_channels: true,
            cache_channels: true,
            user_auth: true,
            watchlist: true,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("missing field: {0}")]
    MissingField(&'static str),
}

pub trait Codec: Send + Sync + std::fmt::Debug {
    fn version(&self) -> u8;
    fn capabilities(&self) -> Capabilities;
    fn decode(&self, text: &str) -> Result<ClientCommand, DecodeError>;

    /// Append the wire form of `event` to `out`. APPEND semantics: `out` is
    /// never cleared — callers may pre-fill (golden tests prove append-at-
    /// offset) and reuse one buffer across many frames.
    ///
    /// This is the allocation-aware encoding seam (F6 / Task 6.4): a
    /// [`ServerEvent::Raw`] payload is appended BY REFERENCE (the `Arc<str>`
    /// deref-coerces to `&str`) — the relayed frame is shared across every
    /// subscriber of the same event instead of being cloned once per
    /// subscriber. Later protocol versions (7.1's wire module) build on this
    /// method.
    fn encode_into(&self, event: &ServerEvent, out: &mut String);

    /// Encode into a fresh `String`. Default implementation delegates to
    /// [`Codec::encode_into`], so existing callers stay source-compatible and
    /// every codec automatically keeps both paths byte-identical.
    fn encode(&self, event: &ServerEvent) -> String {
        let mut out = String::new();
        self.encode_into(event, &mut out);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// U1 / Task 7.2 pin: the v7 codec reports the FULL feature set — this is
    /// the invariant that makes the dispatch capability gates a no-op for v7
    /// (wire behavior byte-identical to the pre-capability code).
    #[test]
    fn v7_codec_reports_the_full_capability_set() {
        assert_eq!(
            crate::protocol::v7::V7Codec.capabilities(),
            Capabilities::v7()
        );
        let caps = Capabilities::v7();
        assert!(caps.client_events);
        assert!(caps.presence);
        assert!(caps.encrypted_channels);
        assert!(caps.cache_channels);
        assert!(caps.user_auth);
        assert!(caps.watchlist);
    }
}
