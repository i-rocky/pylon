//! Channel-name classification. Two orthogonal dimensions: the auth kind
//! (public / private / presence / private-encrypted) and whether it is a cache
//! channel (any auth kind plus a `cache-` segment after the auth prefix).

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
}
