//! HMAC-SHA256 / MD5 primitives and constant-time comparison for Pusher auth.
//! Known-answer tests below are computed directly from the documented signing
//! strings, e.g. a private-channel token signs "<socket_id>:<channel>".

use hmac::{Hmac, Mac};
use md5::{Digest, Md5};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Lowercase-hex HMAC-SHA256 of `msg` keyed by `secret`.
pub fn hmac_sha256_hex(secret: &str, msg: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(msg.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Lowercase-hex MD5 (used for the `body_md5` field of the REST signature in SP2b).
pub fn md5_hex(data: &[u8]) -> String {
    let mut h = Md5::new();
    h.update(data);
    hex::encode(h.finalize())
}

/// Length-checked constant-time string comparison for signatures.
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
}

/// Channel auth signature. Private channels sign "<socket_id>:<channel>";
/// presence channels append ":<channel_data>" (the exact JSON string the client sent).
/// An empty `channel_data` (`Some("")`) is treated as no channel data — it signs
/// the private-channel string `"<socket_id>:<channel>"`.
pub fn channel_signature(
    secret: &str,
    socket_id: &str,
    channel: &str,
    channel_data: Option<&str>,
) -> String {
    let msg = match channel_data {
        Some(cd) if !cd.is_empty() => format!("{socket_id}:{channel}:{cd}"),
        _ => format!("{socket_id}:{channel}"),
    };
    hmac_sha256_hex(secret, &msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_known_answer() {
        // openssl: printf '%s' "The quick brown fox" | openssl dgst -sha256 -hmac key
        assert_eq!(
            hmac_sha256_hex("key", "The quick brown fox"),
            "203d1e5cedd2d18f8c5a3beff0bd9c1ebcb97097dfcb288c46b00c9227fde2c0"
        );
    }

    #[test]
    fn md5_known_answer() {
        assert_eq!(md5_hex(b"hello world"), "5eb63bbbe01eeed093cb22bb8f5acdc3");
    }

    #[test]
    fn private_channel_signature_matches_signing_string() {
        // via: printf '%s' "123.456:private-foo" | openssl dgst -sha256 -hmac secret
        assert_eq!(
            channel_signature("secret", "123.456", "private-foo", None),
            "70492d107085f5eed6c826e9deabe88bd9466b7349d812f7579b263318287644"
        );
    }

    #[test]
    fn presence_channel_signature_includes_channel_data() {
        // via: printf '%s' '123.456:presence-foo:{"user_id":"42"}' | openssl dgst -sha256 -hmac secret
        assert_eq!(
            channel_signature(
                "secret",
                "123.456",
                "presence-foo",
                Some(r#"{"user_id":"42"}"#)
            ),
            "78d7eba8791f1c6a06c3d98b0a5cf37c94f440e8132173320996d824a8c1e433"
        );
    }

    #[test]
    fn empty_channel_data_signs_as_private() {
        assert_eq!(
            channel_signature("secret", "123.456", "private-foo", Some("")),
            channel_signature("secret", "123.456", "private-foo", None)
        );
    }

    #[test]
    fn constant_time_eq_behaviour() {
        assert!(constant_time_eq("abc", "abc"));
        assert!(!constant_time_eq("abc", "abd"));
        assert!(!constant_time_eq("abc", "abcd"));
    }
}
