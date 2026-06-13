//! Verify the `key:signature` auth token a client sends to join a private or
//! presence channel. Verified against the Pusher auth reference.

use crate::auth::signature::{channel_signature, constant_time_eq};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChannelAuthError {
    /// Token not in "key:signature" form.
    #[error("Bad auth token")]
    Malformed,
    /// Token's key is not this app's key.
    #[error("Auth key mismatch")]
    KeyMismatch,
    /// Signature does not match.
    #[error("Invalid signature")]
    BadSignature,
}

impl ChannelAuthError {
    pub fn message(self) -> &'static str {
        match self {
            ChannelAuthError::Malformed => "Bad auth token",
            ChannelAuthError::KeyMismatch => "Auth key mismatch",
            ChannelAuthError::BadSignature => "Invalid signature",
        }
    }
}

/// Verify `token` ("<app_key>:<hex_signature>") for `channel`. For presence
/// channels pass the raw `channel_data` JSON string the client sent (it is part
/// of the signed message); pass `None` for private channels.
pub fn verify(
    app_key: &str,
    app_secret: &str,
    socket_id: &str,
    channel: &str,
    channel_data: Option<&str>,
    token: &str,
) -> Result<(), ChannelAuthError> {
    let (key, sig) = token.split_once(':').ok_or(ChannelAuthError::Malformed)?;
    if key != app_key {
        return Err(ChannelAuthError::KeyMismatch);
    }
    let expected = channel_signature(app_secret, socket_id, channel, channel_data);
    if constant_time_eq(sig, &expected) {
        Ok(())
    } else {
        Err(ChannelAuthError::BadSignature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::signature::channel_signature;

    const KEY: &str = "app-key";
    const SECRET: &str = "app-secret";
    const SID: &str = "123.456";

    fn token(channel: &str, data: Option<&str>) -> String {
        format!("{KEY}:{}", channel_signature(SECRET, SID, channel, data))
    }

    #[test]
    fn accepts_valid_private_token() {
        assert_eq!(
            verify(
                KEY,
                SECRET,
                SID,
                "private-x",
                None,
                &token("private-x", None)
            ),
            Ok(())
        );
    }

    #[test]
    fn accepts_valid_presence_token() {
        let cd = r#"{"user_id":"7"}"#;
        let t = token("presence-x", Some(cd));
        assert_eq!(verify(KEY, SECRET, SID, "presence-x", Some(cd), &t), Ok(()));
    }

    #[test]
    fn rejects_wrong_key() {
        let t = format!(
            "other:{}",
            channel_signature(SECRET, SID, "private-x", None)
        );
        assert_eq!(
            verify(KEY, SECRET, SID, "private-x", None, &t),
            Err(ChannelAuthError::KeyMismatch)
        );
    }

    #[test]
    fn rejects_bad_signature() {
        let t = format!("{KEY}:deadbeef");
        assert_eq!(
            verify(KEY, SECRET, SID, "private-x", None, &t),
            Err(ChannelAuthError::BadSignature)
        );
    }

    #[test]
    fn rejects_tampered_channel_data() {
        let signed = token("presence-x", Some(r#"{"user_id":"7"}"#));
        // Same token, but verifying against different channel_data must fail.
        assert_eq!(
            verify(
                KEY,
                SECRET,
                SID,
                "presence-x",
                Some(r#"{"user_id":"8"}"#),
                &signed
            ),
            Err(ChannelAuthError::BadSignature)
        );
    }

    #[test]
    fn rejects_malformed_token() {
        assert_eq!(
            verify(KEY, SECRET, SID, "private-x", None, "no-colon"),
            Err(ChannelAuthError::Malformed)
        );
    }
}
