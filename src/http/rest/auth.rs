//! Resolve the app from the path `app_id` and verify the signed request.

use crate::app::App;
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
/// 401 `RestError`. Unknown app and any signature failure both yield 401 (the
/// server does not distinguish, to avoid leaking which app ids exist).
pub async fn authenticate(
    state: &AppState,
    app_id: &str,
    method: &str,
    path: &str,
    params: &HashMap<String, String>,
    body: &[u8],
) -> Result<App, RestError> {
    let app = match state.apps.by_id(app_id).await {
        Ok(Some(a)) => a,
        Ok(None) => return Err(RestError::unauthorized("invalid authentication")),
        Err(e) => {
            tracing::warn!(app_id = %app_id, error = %e, "app lookup failed (transient)");
            return Err(RestError::service_unavailable("app store temporarily unavailable"));
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
    .map_err(|_| RestError::unauthorized("invalid authentication"))?;
    Ok((*app).clone())
}
