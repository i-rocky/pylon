//! Admin endpoints (require `Authorization: Bearer <PYLON_ADMIN_TOKEN>`).
//!
//! These endpoints are disabled (404) when `PYLON_ADMIN_TOKEN` is not configured.

use crate::http::error::RestError;
use crate::server::router::AppState;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::HeaderMap,
};

#[derive(serde::Deserialize)]
pub struct InvalidateBody {
    pub key: String,
    #[serde(default)]
    pub action: crate::app::invalidation::InvalidateAction,
}

/// `POST /admin/apps/{app_id}/invalidate`  body: `{ "key": "<app key>" }`
///
/// Disabled (404) unless `PYLON_ADMIN_TOKEN` is set.
/// Requires `Authorization: Bearer <token>` (constant-time compare).
/// Returns 401 on bad/missing token, 400 on a malformed body, 503 if no
/// invalidator (no L2 Redis), 202 on success.
///
/// The request body is taken as raw [`Bytes`] (not `Json<…>`) so that the
/// disabled-check and token-check run BEFORE the body is parsed. An
/// unauthenticated caller therefore cannot probe whether the admin API is
/// enabled by varying the body — the auth decision is identical regardless of
/// body content, and no parse work is done on its behalf.
pub async fn post_invalidate(
    State(state): State<AppState>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<axum::http::StatusCode, RestError> {
    let Some(expected) = state.config.app_admin_token.as_deref() else {
        return Err(RestError::not_found("admin api disabled"));
    };
    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if !check_token(expected, presented) {
        return Err(RestError::unauthorized("invalid admin token"));
    }
    // Authenticated past this point — only now parse the body.
    let parsed: InvalidateBody = serde_json::from_slice(&body)
        .map_err(|e| RestError::bad_request(format!("invalid body: {e}")))?;
    match &state.invalidator {
        Some(inv) => {
            inv.publish(&app_id, &parsed.key, parsed.action)
                .await
                .map_err(|e| RestError::service_unavailable(format!("invalidate publish failed: {e}")))?;
            Ok(axum::http::StatusCode::ACCEPTED)
        }
        None => Err(RestError::service_unavailable(
            "invalidation requires PYLON_APP_CACHE_REDIS_URL",
        )),
    }
}

// ── Pure-logic helpers extracted for unit-testability ───────────────────────

/// Verify a presented Bearer token against the expected value using constant-time
/// comparison. Returns `true` only if both strings are identical.
///
/// The length guard is NOT a timing side-channel risk because the token length is
/// not secret (it is fixed by the operator). It does prevent a potential
/// "all-zeros" short-circuit in some `ct_eq` implementations.
pub fn check_token(expected: &str, presented: &str) -> bool {
    use subtle::ConstantTimeEq;
    presented.len() == expected.len()
        && presented.as_bytes().ct_eq(expected.as_bytes()).unwrap_u8() == 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correct_token_passes() {
        assert!(check_token("supersecret", "supersecret"));
    }

    #[test]
    fn wrong_token_fails() {
        assert!(!check_token("supersecret", "wrongtoken!"));
    }

    #[test]
    fn length_mismatch_fails() {
        assert!(!check_token("supersecret", "short"));
    }

    #[test]
    fn empty_presented_fails() {
        assert!(!check_token("supersecret", ""));
    }

    #[test]
    fn both_empty_passes() {
        // Edge-case: empty==empty is technically "correct" but the admin token
        // would never be set to empty string (from_env uses .ok() which treats
        // an unset var as None, not "").
        assert!(check_token("", ""));
    }

    #[test]
    fn invalidate_body_defaults_action_to_refresh() {
        let b: InvalidateBody = serde_json::from_str(r#"{"key":"k"}"#).unwrap();
        assert_eq!(b.key, "k");
        assert_eq!(b.action, crate::app::invalidation::InvalidateAction::Refresh);
    }

    #[test]
    fn invalidate_body_parses_remove_action() {
        let b: InvalidateBody = serde_json::from_str(r#"{"key":"k","action":"remove"}"#).unwrap();
        assert_eq!(b.action, crate::app::invalidation::InvalidateAction::Remove);
    }

    #[test]
    fn invalidate_body_parses_refresh_action() {
        let b: InvalidateBody = serde_json::from_str(r#"{"key":"k","action":"refresh"}"#).unwrap();
        assert_eq!(b.action, crate::app::invalidation::InvalidateAction::Refresh);
    }

    // ── Handler behavior: the full `post_invalidate` flow ────────────────────
    // (`State`/`Path`/`Bytes`/`HeaderMap`/`AppState`/`post_invalidate` come via
    // `use super::*`.)
    use axum::http::StatusCode;
    use std::sync::Arc;

    /// Minimal `AppState` to drive the handler directly. `token` sets/clears the
    /// admin token; `invalidator` is the publish handle (None ⇒ the 503 path).
    fn test_state(
        token: Option<&str>,
        invalidator: Option<Arc<crate::app::invalidation::AppInvalidator>>,
    ) -> AppState {
        let config = crate::server::config::ServerConfig {
            app_admin_token: token.map(|t| t.to_string()),
            ..crate::server::config::ServerConfig::default()
        };
        AppState {
            config,
            apps: Arc::new(crate::app::static_file::StaticFileAppManager::from_json("[]").unwrap()),
            adapter: Arc::new(crate::adapter::local::LocalAdapter::new(
                Arc::new(crate::channel::registry::Registry::new()),
                Arc::new(crate::adapter::app_registry::AppRegistry::new()),
            )),
            conn_counts: Arc::new(dashmap::DashMap::new()),
            webhooks: crate::webhook::WebhookHandle::null(),
            saturated: None,
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            cluster_metrics: None,
            invalidator,
        }
    }

    fn bearer(token: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        h
    }

    #[tokio::test]
    async fn handler_disabled_when_no_admin_token_returns_404() {
        let err = post_invalidate(
            State(test_state(None, None)),
            Path("app1".into()),
            HeaderMap::new(),
            Bytes::from(r#"{"key":"k"}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn handler_bad_or_missing_token_returns_401() {
        // Wrong token.
        let err = post_invalidate(
            State(test_state(Some("secret"), None)),
            Path("app1".into()),
            bearer("wrong"),
            Bytes::from(r#"{"key":"k"}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::UNAUTHORIZED);
        // No Authorization header at all.
        let err2 = post_invalidate(
            State(test_state(Some("secret"), None)),
            Path("app1".into()),
            HeaderMap::new(),
            Bytes::from(r#"{"key":"k"}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(err2.status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn handler_auth_runs_strictly_before_body_parse() {
        // A bad token with a MALFORMED body must return 401 (auth), NOT 400 (parse):
        // an unauthenticated caller's body is never parsed (the Plan-5 hardening).
        let err = post_invalidate(
            State(test_state(Some("secret"), None)),
            Path("app1".into()),
            bearer("wrong"),
            Bytes::from("definitely not json"),
        )
        .await
        .unwrap_err();
        assert_eq!(
            err.status,
            StatusCode::UNAUTHORIZED,
            "auth must reject before the malformed body is parsed"
        );
    }

    #[tokio::test]
    async fn handler_authed_with_bad_body_returns_400() {
        let err = post_invalidate(
            State(test_state(Some("secret"), None)),
            Path("app1".into()),
            bearer("secret"),
            Bytes::from("not json"),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn handler_authed_without_invalidator_returns_503() {
        let err = post_invalidate(
            State(test_state(Some("secret"), None)),
            Path("app1".into()),
            bearer("secret"),
            Bytes::from(r#"{"key":"k"}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Redis-gated: the authenticated success path publishes to Redis pub/sub and
    /// returns 202 Accepted.
    #[tokio::test]
    async fn handler_authed_with_invalidator_returns_202() {
        use crate::app::cache::{CacheConfig, CachingAppManager};
        let url = std::env::var("PYLON_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6390".into());
        let apps: Arc<dyn crate::app::AppManager> =
            Arc::new(crate::app::static_file::StaticFileAppManager::from_json("[]").unwrap());
        let cache = Arc::new(CachingAppManager::new(
            apps,
            CacheConfig { max_capacity: 16, ttl_secs: 60, neg_max: 16, neg_ttl_secs: 60 },
            None,
        ));
        let adapter: Arc<dyn crate::adapter::Adapter> =
            Arc::new(crate::adapter::local::LocalAdapter::new(
                Arc::new(crate::channel::registry::Registry::new()),
                Arc::new(crate::adapter::app_registry::AppRegistry::new()),
            ));
        let purger = Arc::new(crate::app::purger::AppPurger::new(
            adapter,
            Arc::new(dashmap::DashMap::new()),
            cache,
        ));
        let inv = crate::app::invalidation::AppInvalidator::spawn(&url, purger)
            .await
            .expect("invalidator must connect to the test Redis");
        let status = post_invalidate(
            State(test_state(Some("secret"), Some(inv))),
            Path("app1".into()),
            bearer("secret"),
            Bytes::from(r#"{"key":"k","action":"refresh"}"#),
        )
        .await
        .expect("authed valid request must return 202");
        assert_eq!(status, StatusCode::ACCEPTED);
    }
}
