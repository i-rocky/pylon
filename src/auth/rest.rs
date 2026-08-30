//! Verify Pusher REST API signed requests. The signature is
//! `HMAC_SHA256(secret, "<METHOD>\n<path>\n<sorted-query>")`, where the query is
//! every param except `auth_signature`, keys lowercased and sorted, joined `k=v&…`.

use crate::auth::signature::{constant_time_eq, hmac_sha256_hex, md5_hex};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RestAuthError {
    #[error("missing required auth parameter")]
    MissingParam,
    #[error("unsupported auth_version")]
    BadVersion,
    #[error("auth_key does not match app")]
    KeyMismatch,
    #[error("auth_timestamp outside allowed window")]
    Expired,
    #[error("body_md5 mismatch")]
    BadBodyMd5,
    #[error("invalid auth_signature")]
    BadSignature,
}

/// The GENERIC 401 message for auth failures that must stay
/// indistinguishable (R3 anti-enumeration pin): an unknown `auth_key` gets the
/// exact same string as the REST layer's unknown-app path, so probing ids or
/// keys learns nothing beyond "not valid for this app".
pub const GENERIC_AUTH_FAILURE: &str = "invalid authentication";

impl RestAuthError {
    /// Map a verify failure to the 401 body message, distinguishing causes the
    /// way the hosted API does (its troubleshooting docs quote `Timestamp
    /// expired: Given timestamp … not within 600s of server time` and
    /// `Invalid signature: Expected HMAC SHA256 hex digest` as real response
    /// wording; the REST reference only promises "Authentication error:
    /// response body will contain an explanation").
    ///
    /// `KeyMismatch` deliberately returns [`GENERIC_AUTH_FAILURE`]: a specific
    /// message there would turn key-probing into a key↔app binding oracle.
    /// [`Expired`] is the one message that needs the configured window (the
    /// hosted default is 600s), so it takes `window_secs`.
    pub fn message(&self, window_secs: u64) -> String {
        match self {
            RestAuthError::MissingParam => "Missing auth parameters".into(),
            RestAuthError::BadVersion => "Invalid auth version".into(),
            RestAuthError::KeyMismatch => GENERIC_AUTH_FAILURE.into(),
            RestAuthError::Expired => {
                format!(
                    "Timestamp expired: Given timestamp not within {window_secs}s of server time"
                )
            }
            RestAuthError::BadBodyMd5 => "Invalid body_md5".into(),
            RestAuthError::BadSignature => {
                "Invalid signature: Expected HMAC SHA256 hex digest".into()
            }
        }
    }
}

/// The exact string that is HMAC-signed. `params` must already exclude
/// `auth_signature`; a `BTreeMap` guarantees the keys are sorted.
pub fn signing_string(method: &str, path: &str, params: &BTreeMap<String, String>) -> String {
    let query = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    format!("{}\n{}\n{}", method.to_uppercase(), path, query)
}

/// Verify a signed request. `params` is the full decoded query map; `now` is the
/// current unix time (secs); `window` is the allowed clock skew (secs).
#[allow(clippy::too_many_arguments)]
pub fn verify(
    app_key: &str,
    app_secret: &str,
    method: &str,
    path: &str,
    params: &HashMap<String, String>,
    body: &[u8],
    now: u64,
    window: u64,
) -> Result<(), RestAuthError> {
    let get = |k: &str| params.get(k).map(String::as_str);
    if get("auth_version") != Some("1.0") {
        return Err(RestAuthError::BadVersion);
    }
    if get("auth_key") != Some(app_key) {
        return Err(RestAuthError::KeyMismatch);
    }
    let ts: u64 = get("auth_timestamp")
        .ok_or(RestAuthError::MissingParam)?
        .parse()
        .map_err(|_| RestAuthError::MissingParam)?;
    if now.abs_diff(ts) > window {
        return Err(RestAuthError::Expired);
    }
    // Enforce body_md5 whenever the signed request committed to one (the param is
    // present) OR a body was received. This closes a body-stripping gap: a captured
    // request whose body is removed in transit still carries the signed `body_md5`
    // param, so the signature alone would pass — without this check verify would
    // accept the now-empty body. md5_hex(b"") won't match the committed hash.
    if !body.is_empty() || get("body_md5").is_some() {
        match get("body_md5") {
            Some(m) if constant_time_eq(m, &md5_hex(body)) => {}
            _ => return Err(RestAuthError::BadBodyMd5),
        }
    }
    let signature = get("auth_signature").ok_or(RestAuthError::MissingParam)?;
    let signed: BTreeMap<String, String> = params
        .iter()
        .map(|(k, v)| (k.to_lowercase(), v.clone()))
        .filter(|(k, _)| k != "auth_signature")
        .collect();
    let expected = hmac_sha256_hex(app_secret, &signing_string(method, path, &signed));
    if constant_time_eq(signature, &expected) {
        Ok(())
    } else {
        Err(RestAuthError::BadSignature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signed_params(
        secret: &str,
        method: &str,
        path: &str,
        ts: u64,
        body: &[u8],
    ) -> HashMap<String, String> {
        let mut p: BTreeMap<String, String> = BTreeMap::new();
        p.insert("auth_key".into(), "app-key".into());
        p.insert("auth_timestamp".into(), ts.to_string());
        p.insert("auth_version".into(), "1.0".into());
        if !body.is_empty() {
            p.insert("body_md5".into(), md5_hex(body));
        }
        let sig = hmac_sha256_hex(secret, &signing_string(method, path, &p));
        let mut out: HashMap<String, String> = p.into_iter().collect();
        out.insert("auth_signature".into(), sig);
        out
    }

    #[test]
    fn signing_string_matches_pusher_doc_example() {
        let mut p = BTreeMap::new();
        p.insert("auth_key".to_string(), "278d425bdf160c739803".to_string());
        p.insert("auth_timestamp".to_string(), "1353088179".to_string());
        p.insert("auth_version".to_string(), "1.0".to_string());
        p.insert(
            "body_md5".to_string(),
            "ec365a775a4cd0599faeb73354201b6f".to_string(),
        );
        assert_eq!(
            signing_string("POST", "/apps/3/events", &p),
            "POST\n/apps/3/events\nauth_key=278d425bdf160c739803&auth_timestamp=1353088179&auth_version=1.0&body_md5=ec365a775a4cd0599faeb73354201b6f"
        );
    }

    #[test]
    fn accepts_valid_signed_request_with_body() {
        let body = br#"{"name":"e","data":"{}"}"#;
        let p = signed_params("secret", "POST", "/apps/1/events", 1000, body);
        assert_eq!(
            verify(
                "app-key",
                "secret",
                "POST",
                "/apps/1/events",
                &p,
                body,
                1000,
                600
            ),
            Ok(())
        );
    }

    #[test]
    fn accepts_valid_signed_get_without_body() {
        let p = signed_params("secret", "GET", "/apps/1/channels", 1000, b"");
        assert_eq!(
            verify(
                "app-key",
                "secret",
                "GET",
                "/apps/1/channels",
                &p,
                b"",
                1000,
                600
            ),
            Ok(())
        );
    }

    #[test]
    fn rejects_wrong_version() {
        let mut p = signed_params("secret", "GET", "/apps/1/channels", 1000, b"");
        p.insert("auth_version".into(), "2.0".into());
        assert_eq!(
            verify(
                "app-key",
                "secret",
                "GET",
                "/apps/1/channels",
                &p,
                b"",
                1000,
                600
            ),
            Err(RestAuthError::BadVersion)
        );
    }

    #[test]
    fn rejects_expired_timestamp() {
        let p = signed_params("secret", "GET", "/apps/1/channels", 1000, b"");
        // now is 2000, window 600 → |2000-1000| = 1000 > 600
        assert_eq!(
            verify(
                "app-key",
                "secret",
                "GET",
                "/apps/1/channels",
                &p,
                b"",
                2000,
                600
            ),
            Err(RestAuthError::Expired)
        );
    }

    #[test]
    fn rejects_bad_body_md5() {
        let body = br#"{"name":"e","data":"{}"}"#;
        let mut p = signed_params("secret", "POST", "/apps/1/events", 1000, body);
        p.insert("body_md5".into(), md5_hex(b"different"));
        assert_eq!(
            verify(
                "app-key",
                "secret",
                "POST",
                "/apps/1/events",
                &p,
                body,
                1000,
                600
            ),
            Err(RestAuthError::BadBodyMd5)
        );
    }

    #[test]
    fn rejects_missing_timestamp() {
        let mut p = signed_params("secret", "GET", "/apps/1/channels", 1000, b"");
        p.remove("auth_timestamp");
        assert_eq!(
            verify(
                "app-key",
                "secret",
                "GET",
                "/apps/1/channels",
                &p,
                b"",
                1000,
                600
            ),
            Err(RestAuthError::MissingParam)
        );
    }

    #[test]
    fn rejects_wrong_key() {
        // params carry auth_key="app-key" but the app's key is different.
        let p = signed_params("secret", "GET", "/apps/1/channels", 1000, b"");
        assert_eq!(
            verify(
                "other-key",
                "secret",
                "GET",
                "/apps/1/channels",
                &p,
                b"",
                1000,
                600
            ),
            Err(RestAuthError::KeyMismatch)
        );
    }

    #[test]
    fn rejects_stripped_body_when_md5_present() {
        // A request signed over a non-empty body (so body_md5 is committed), but the
        // body is stripped to empty in transit. The signature still matches the
        // params, yet verify must reject because the committed body_md5 no longer
        // matches the (now empty) body.
        let body = br#"{"name":"e","data":"{}"}"#;
        let p = signed_params("secret", "POST", "/apps/1/events", 1000, body);
        assert_eq!(
            verify(
                "app-key",
                "secret",
                "POST",
                "/apps/1/events",
                &p,
                b"",
                1000,
                600
            ),
            Err(RestAuthError::BadBodyMd5)
        );
    }

    #[test]
    fn rejects_bad_signature() {
        let mut p = signed_params("secret", "GET", "/apps/1/channels", 1000, b"");
        p.insert("auth_signature".into(), "deadbeef".into());
        assert_eq!(
            verify(
                "app-key",
                "secret",
                "GET",
                "/apps/1/channels",
                &p,
                b"",
                1000,
                600,
            ),
            Err(RestAuthError::BadSignature)
        );
    }

    // ── R3: variant → 401 body message mapping ─────────────────────────────────

    /// Every variant maps to its own pinned message (window 600 = the hosted
    /// default and our config default).
    #[test]
    fn message_maps_every_variant_distinctly() {
        assert_eq!(
            RestAuthError::MissingParam.message(600),
            "Missing auth parameters"
        );
        assert_eq!(
            RestAuthError::BadVersion.message(600),
            "Invalid auth version"
        );
        // Anti-enumeration: a wrong auth_key is the ONE cause that stays
        // generic — same string the unknown-app path emits.
        assert_eq!(
            RestAuthError::KeyMismatch.message(600),
            GENERIC_AUTH_FAILURE
        );
        assert_eq!(
            RestAuthError::Expired.message(600),
            "Timestamp expired: Given timestamp not within 600s of server time"
        );
        assert_eq!(RestAuthError::BadBodyMd5.message(600), "Invalid body_md5");
        assert_eq!(
            RestAuthError::BadSignature.message(600),
            "Invalid signature: Expected HMAC SHA256 hex digest"
        );
    }

    /// The timestamp message carries the CONFIGURED window, so a deployment
    /// that widens the skew allowance does not lie about "600s".
    #[test]
    fn expired_message_carries_configured_window() {
        assert_eq!(
            RestAuthError::Expired.message(300),
            "Timestamp expired: Given timestamp not within 300s of server time"
        );
    }

    /// No two variants may collapse onto one message (that collapse is the bug
    /// this mapping fixes).
    #[test]
    fn messages_are_pairwise_distinct() {
        let all = [
            RestAuthError::MissingParam,
            RestAuthError::BadVersion,
            RestAuthError::KeyMismatch,
            RestAuthError::Expired,
            RestAuthError::BadBodyMd5,
            RestAuthError::BadSignature,
        ]
        .map(|e| e.message(600));
        let unique: std::collections::HashSet<&str> = all.iter().map(String::as_str).collect();
        assert_eq!(
            unique.len(),
            all.len(),
            "messages must be distinct: {all:?}"
        );
    }
}
