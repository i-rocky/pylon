//! Resolve the app from the path `app_id` and verify the signed request.

use crate::app::{App, AppLookup};
use crate::http::error::RestError;
use crate::server::router::AppState;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Current unix time in seconds.
pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Resolve `app_id` and verify the Pusher signed request. Returns the `App` or a
/// `RestError`. The three lookup outcomes map to Pusher's documented responses
/// (R1): a DISABLED app gets **403** (Pusher documents 403 Forbidden for it);
/// an UNKNOWN app gets the GENERIC 401 (anti-enumeration: the server does not
/// reveal which app ids exist). Signature failures (R3) keep 401 but
/// distinguish causes via [`RestAuthError::message`] — EXCEPT `KeyMismatch`,
/// which maps to the same generic string as the unknown-app path.
pub async fn authenticate(
    state: &AppState,
    app_id: &str,
    method: &str,
    path: &str,
    params: &HashMap<String, String>,
    body: &[u8],
) -> Result<App, RestError> {
    let app = match state.apps.by_id(app_id).await {
        Ok(AppLookup::Found(a)) => a,
        Ok(AppLookup::Disabled) => return Err(RestError::forbidden("app is disabled")),
        Ok(AppLookup::NotFound) => {
            return Err(RestError::unauthorized(
                crate::auth::rest::GENERIC_AUTH_FAILURE,
            ))
        }
        Err(e) => {
            tracing::warn!(app_id = %app_id, error = %e, "app lookup failed (transient)");
            return Err(RestError::service_unavailable(
                "app store temporarily unavailable",
            ));
        }
    };
    crate::auth::rest::verify(
        &app.key,
        &app.secret,
        method,
        path,
        params,
        body,
        now_unix(),
        state.config.rest_auth_window_secs,
    )
    .map_err(|e| RestError::unauthorized(e.message(state.config.rest_auth_window_secs)))?;
    Ok((*app).clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{AppLookupError, AppManager};
    use crate::server::router::AppState;
    use std::sync::Arc;

    /// An app store that is reachable but broken — every lookup is a transient
    /// backend error, never a verdict about the app.
    struct BrokenStore;

    #[async_trait::async_trait]
    impl AppManager for BrokenStore {
        async fn by_id(&self, _id: &str) -> Result<crate::app::AppLookup, AppLookupError> {
            Err(AppLookupError::Backend("connection refused".into()))
        }
        async fn by_key(&self, _key: &str) -> Result<crate::app::AppLookup, AppLookupError> {
            Err(AppLookupError::Backend("connection refused".into()))
        }
    }

    fn state_over(apps: Arc<dyn AppManager>) -> AppState {
        AppState {
            config: crate::server::config::ServerConfig::default(),
            apps,
            adapter: Arc::new(crate::adapter::local::LocalAdapter::new(
                Arc::new(crate::channel::registry::Registry::new()),
                Arc::new(crate::adapter::app_registry::AppRegistry::new()),
            )),
            conn_counts: Arc::new(dashmap::DashMap::new()),
            webhooks: crate::webhook::WebhookHandle::null(),
            saturated: None,
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            cluster_metrics: None,
            invalidator: None,
        }
    }

    /// A store OUTAGE must not be reported as an auth verdict. 401 would tell a
    /// legitimate caller its credentials are wrong and 403 that its app is
    /// disabled; both are lies that make an operator debug the wrong thing. The
    /// documented answer is 503 — retry, the server is degraded.
    #[tokio::test]
    async fn a_transient_app_store_failure_is_503_not_an_auth_verdict() {
        let state = state_over(Arc::new(BrokenStore));
        let err = authenticate(
            &state,
            "app1",
            "POST",
            "/apps/app1/events",
            &HashMap::new(),
            b"",
        )
        .await
        .expect_err("a broken app store must not authenticate the request");

        assert_eq!(
            err.status,
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "an app-store outage is a 503, never a 401/403 auth verdict"
        );
    }
}
