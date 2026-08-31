//! The version seam: each protocol version implements `Codec`.

use crate::protocol::command::ClientCommand;
use crate::protocol::event::ServerEvent;

/// What a given protocol version supports. Extended in later SPs
/// (encrypted, cache, signin, watchlist).
#[derive(Debug, Clone, Copy, Default)]
pub struct Capabilities {
    pub client_events: bool,
    pub presence: bool,
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
