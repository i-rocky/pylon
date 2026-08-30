//! REST error type that renders as an HTTP status + JSON body, matching
//! Pusher's HTTP error responses (`{"error": ..., "status": ...}`), which the
//! official server SDKs parse (or pass through verbatim — see
//! pusher-http-node `lib/requests.js`, which reads the body as text and
//! attaches it to `RequestError`).

use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

#[derive(Debug, Clone)]
pub struct RestError {
    pub status: StatusCode,
    pub message: String,
}

/// Build the Pusher-style JSON error body: `{"error": "<message>", "status": <code>}`.
///
/// Shared by [`RestError`]'s rendering and (from phase 2) the 403 body, so every
/// REST error carries the same shape. `message` is JSON-escaped by `serde_json`.
pub fn error_body(message: &str, status: u16) -> String {
    serde_json::json!({ "error": message, "status": status }).to_string()
}

impl RestError {
    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
    /// R10: a matched path with an unsupported method. Rendered by the
    /// router-wide `method_not_allowed_fallback` (src/server/router.rs), so a
    /// 405 carries the same JSON shape as every other REST error. Pusher's
    /// docs say nothing about wrong-method bodies — the wording is ours.
    pub fn method_not_allowed(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::METHOD_NOT_ALLOWED,
            message: message.into(),
        }
    }
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }
    /// R1: Pusher's HTTP API documents **403 Forbidden** for a disabled app —
    /// distinct from the 401 an unknown app / bad signature gets.
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }
    pub fn payload_too_large(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: message.into(),
        }
    }
    /// SP10 admission control: the publish pipeline is saturated, so reject the
    /// publish instead of broadcasting (fail-fast). Renders 503 + a `Retry-After`
    /// header so a well-behaved publisher backs off rather than retrying instantly.
    pub fn service_unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
        }
    }

    /// Wrap an axum extractor rejection (e.g. `BytesRejection` when the request
    /// body limit fires) so it renders our JSON body while keeping axum's status
    /// code (413 for the length limit, 400 for other buffering failures).
    pub fn from_rejection(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl IntoResponse for RestError {
    fn into_response(self) -> Response {
        let body = error_body(&self.message, self.status.as_u16());
        let mut headers = HeaderMap::new();
        // `HeaderMap` parts `extend` (replace) the String body's implicit
        // text/plain content-type, so the JSON type wins.
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        // A 503 (overload) carries `Retry-After: 1` so publishers back off.
        if self.status == StatusCode::SERVICE_UNAVAILABLE {
            headers.insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        (self.status, headers, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    /// Render an error and return `(status, content-type, retry-after, body)`.
    async fn render(err: RestError) -> (StatusCode, String, Option<String>, String) {
        let resp = err.into_response();
        let status = resp.status();
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let ra = resp
            .headers()
            .get(header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, ct, ra, String::from_utf8(body.to_vec()).unwrap())
    }

    /// Every status class must render the JSON shape with the right content-type.
    #[tokio::test]
    async fn renders_json_body_for_every_status_class() {
        for (err, code) in [
            (RestError::bad_request("bad"), 400),
            (RestError::unauthorized("no"), 401),
            (RestError::forbidden("app is disabled"), 403),
            (RestError::not_found("gone"), 404),
            (RestError::method_not_allowed("no"), 405),
            (RestError::payload_too_large("big"), 413),
            (RestError::service_unavailable("busy"), 503),
        ] {
            let (status, ct, _ra, body) = render(err).await;
            assert_eq!(status.as_u16(), code);
            assert!(
                ct.starts_with("application/json"),
                "content-type must be application/json, got {ct}"
            );
            let v: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(
                v["status"], code,
                "`status` field must mirror the HTTP status, got {v}"
            );
            assert!(
                v["error"].as_str().is_some_and(|e| !e.is_empty()),
                "`error` must be a non-empty string, got {v}"
            );
        }
    }

    /// The 503 (overload) response keeps its `Retry-After: 1` back-off header.
    #[tokio::test]
    async fn service_unavailable_keeps_retry_after() {
        let (status, _ct, ra, _body) =
            render(RestError::service_unavailable("Server overloaded")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(ra.as_deref(), Some("1"));
    }

    /// Non-503 errors must NOT carry a Retry-After header.
    #[tokio::test]
    async fn bad_request_has_no_retry_after() {
        let (_status, _ct, ra, _body) = render(RestError::bad_request("x")).await;
        assert!(ra.is_none());
    }

    /// The message is JSON-escaped (quotes must not break the body).
    #[tokio::test]
    async fn message_is_json_escaped() {
        let body = error_body("quote \" and \\ slash", 400);
        let v: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["error"], "quote \" and \\ slash");
    }

    #[test]
    fn maps_to_status() {
        assert_eq!(RestError::bad_request("x").status, StatusCode::BAD_REQUEST);
        assert_eq!(
            RestError::unauthorized("x").status,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(RestError::forbidden("x").status, StatusCode::FORBIDDEN);
        assert_eq!(
            RestError::payload_too_large("x").status,
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(RestError::not_found("x").status, StatusCode::NOT_FOUND);
        assert_eq!(
            RestError::method_not_allowed("x").status,
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert_eq!(
            RestError::service_unavailable("x").status,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
