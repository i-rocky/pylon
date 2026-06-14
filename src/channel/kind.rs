//! Channel-name classification. Two orthogonal dimensions: the auth kind
//! (public / private / presence / private-encrypted) and whether it is a cache
//! channel (any auth kind plus a `cache-` segment after the auth prefix).

/// Reserved channel namespace for Pusher server-to-user messaging:
/// `#server-to-user-<user_id>`. Delivery is routed to the user's connections
/// via the user registry, never the channel registry.
pub const SERVER_TO_USER_PREFIX: &str = "#server-to-user-";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthKind {
    Public,
    Private,
    Presence,
    PrivateEncrypted,
}

impl AuthKind {
    pub fn requires_auth(self) -> bool {
        matches!(
            self,
            Self::Private | Self::Presence | Self::PrivateEncrypted
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelInfo {
    pub auth: AuthKind,
    pub cache: bool,
}

impl ChannelInfo {
    pub fn of(name: &str) -> ChannelInfo {
        let (auth, rest) = if let Some(r) = name.strip_prefix("private-encrypted-") {
            (AuthKind::PrivateEncrypted, r)
        } else if let Some(r) = name.strip_prefix("presence-") {
            (AuthKind::Presence, r)
        } else if let Some(r) = name.strip_prefix("private-") {
            (AuthKind::Private, r)
        } else {
            (AuthKind::Public, name)
        };
        ChannelInfo {
            auth,
            cache: rest.starts_with("cache-"),
        }
    }
}

/// Returns `true` if `name` is a valid channel name under Pusher's rules:
/// - length ≤ `max_len`
/// - every character is in `[A-Za-z0-9_\-=@,.;]`
///
/// The `#` prefix used by server-to-user channels is handled before this
/// function is called and must NOT be stripped here.
pub fn validate_channel_name(name: &str, max_len: usize) -> bool {
    if name.is_empty() || name.len() > max_len {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '=' | '@' | ',' | '.' | ';'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_all_eight_prefix_combinations() {
        let cases = [
            ("foo", AuthKind::Public, false),
            ("cache-foo", AuthKind::Public, true),
            ("private-foo", AuthKind::Private, false),
            ("private-cache-foo", AuthKind::Private, true),
            ("presence-foo", AuthKind::Presence, false),
            ("presence-cache-foo", AuthKind::Presence, true),
            ("private-encrypted-foo", AuthKind::PrivateEncrypted, false),
            (
                "private-encrypted-cache-foo",
                AuthKind::PrivateEncrypted,
                true,
            ),
        ];
        for (name, auth, cache) in cases {
            let info = ChannelInfo::of(name);
            assert_eq!(info.auth, auth, "auth for {name}");
            assert_eq!(info.cache, cache, "cache for {name}");
        }
    }

    #[test]
    fn auth_requirement() {
        assert!(!AuthKind::Public.requires_auth());
        assert!(AuthKind::Private.requires_auth());
        assert!(AuthKind::Presence.requires_auth());
        assert!(AuthKind::PrivateEncrypted.requires_auth());
    }

    // P14 — empty channel name must be rejected
    #[test]
    fn validate_channel_name_rejects_empty() {
        assert!(
            !validate_channel_name("", 164),
            "empty name must be invalid (P14)"
        );
    }

    // P8 — channel-name validation
    #[test]
    fn validate_channel_name_accepts_valid_names() {
        assert!(validate_channel_name("my-channel", 164));
        assert!(validate_channel_name("presence-room", 164));
        assert!(validate_channel_name("private-x", 164));
        assert!(validate_channel_name("a", 164));
        assert!(validate_channel_name(
            "abc_123-def=ghi@jkl,mno.pqr;stu",
            164
        ));
        // exactly 164 chars must pass
        let at_limit = "a".repeat(164);
        assert!(validate_channel_name(&at_limit, 164));
    }

    #[test]
    fn validate_channel_name_rejects_over_length() {
        let long = "a".repeat(165);
        assert!(!validate_channel_name(&long, 164));
        let exactly_200 = "a".repeat(200);
        assert!(!validate_channel_name(&exactly_200, 164));
    }

    #[test]
    fn validate_channel_name_rejects_illegal_chars() {
        assert!(!validate_channel_name("bad channel", 164)); // space
        assert!(!validate_channel_name("bad!channel", 164)); // !
        assert!(!validate_channel_name("bad#channel", 164)); // # (not in charset)
        assert!(!validate_channel_name("bad/channel", 164)); // /
        assert!(!validate_channel_name("bad\tchannel", 164)); // tab
    }
}
