//! Local I/O plumbing for the conformance harness: the anti-blindness spine.
//!
//! - [`http_get`]: minimal raw-TCP HTTP/1.0 GET used to poll the pylon
//!   server's health endpoint while it boots (no reqwest dependency; only
//!   ever pointed at localhost).
//! - [`AuthServer`]: an axum server the SDK clients call for channel/user
//!   authorization. It does NO crypto itself: it forwards the raw request
//!   body to a [`SignerFn`] (in production wiring, the pusher-http-node
//!   runner's `--sign` mode) and returns the signer's string verbatim.
//! - [`WebhookReceiver`]: an axum server recording every POST webhook
//!   envelope (lowercased headers + raw body) in arrival order, and serving
//!   the captured envelopes back as JSON for the S-WEBHOOK-VERIFY runner.

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::future::BoxFuture;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Signing callback: raw auth-request body bytes in, the SDK-shaped auth
/// response string out (pusher-http-node `--sign` mode in production).
pub type SignerFn = Arc<dyn Fn(Vec<u8>) -> BoxFuture<'static, Result<String>> + Send + Sync>;

/// One recorded webhook delivery, in arrival order.
#[derive(Debug, Clone)]
pub struct RecordedEnvelope {
    /// Header pairs, names lowercased.
    pub headers: Vec<(String, String)>,
    /// Raw body string (lossy UTF-8).
    pub body: String,
}

impl RecordedEnvelope {
    /// The JSON shape served by `GET /last` and `GET /all`, consumed by the
    /// S-WEBHOOK-VERIFY runner to feed the SDK's webhook verifier:
    /// `{"headers": {...}, "body": "..."}`. Repeated header names are joined
    /// with `", "` per RFC 9110 §5.2.
    fn to_json(&self) -> serde_json::Value {
        let mut headers = serde_json::Map::new();
        for (name, value) in &self.headers {
            match headers.get_mut(name) {
                Some(existing) => {
                    *existing = format!("{existing}, {value}").into();
                }
                None => {
                    headers.insert(name.clone(), serde_json::Value::String(value.clone()));
                }
            }
        }
        serde_json::json!({ "headers": headers, "body": self.body })
    }
}

/// Shared record buffer behind the webhook receiver.
type SharedEnvelopes = Arc<Mutex<Vec<RecordedEnvelope>>>;

/// GET `path` over raw TCP and return the response status code.
///
/// Deliberately dependency-free HTTP/1.0: the server closes the connection
/// after responding, so the reply is read to EOF. Only ever used against
/// localhost (server::wait_ready polling).
pub async fn http_get(host: &str, port: u16, path: &str) -> Result<u16> {
    let mut stream = tokio::net::TcpStream::connect((host, port))
        .await
        .with_context(|| format!("connect {host}:{port} for GET {path}"))?;
    let request = format!("GET {path} HTTP/1.0\r\nHost: h\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await?;
    let text = String::from_utf8_lossy(&raw);
    text.lines()
        .next()
        .and_then(|status_line| status_line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .with_context(|| format!("parse status line from {text:?}"))
}

/// Bind `127.0.0.1:{port}` (port 0 asks the OS for an ephemeral port), serve
/// `app` on a detached task, and return the actually-bound port.
async fn serve_http(port: u16, app: Router) -> Result<u16> {
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .with_context(|| format!("bind 127.0.0.1:{port}"))?;
    let bound = listener.local_addr()?.port();
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            eprintln!("plumbing server error: {e}");
        }
    });
    Ok(bound)
}

/// SDK-facing authorization endpoint.
///
/// A single POST fallback handler serves `/auth`, `/pusher/auth` and
/// `/pusher/user-auth` identically: request body bytes go to the [`SignerFn`]
/// and the signer's string comes back as the 200 body verbatim
/// (content-type application/json).
pub struct AuthServer {
    port: u16,
}

impl AuthServer {
    pub async fn spawn(port: u16, signer: SignerFn) -> Result<Self> {
        let app = Router::new()
            .fallback(post(sign_and_respond))
            .with_state(signer);
        Ok(Self {
            port: serve_http(port, app).await?,
        })
    }

    /// The actually-bound port (ephemeral when spawned with 0).
    pub fn port(&self) -> u16 {
        self.port
    }
}

async fn sign_and_respond(State(signer): State<SignerFn>, body: axum::body::Bytes) -> Response {
    match signer(body.to_vec()).await {
        Ok(signed) => (
            StatusCode::OK,
            [("content-type", "application/json")],
            signed,
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Records every POSTed webhook envelope and serves them back for the
/// S-WEBHOOK-VERIFY runner.
///
/// - Any POST (registered `/hooks` path and every fallback path) is recorded
///   and answered 204.
/// - `GET /last` answers the most recent envelope as
///   `{"headers": {...}, "body": "..."}`, 404 when nothing arrived yet.
/// - `GET /all` answers all recorded envelopes as a JSON array.
pub struct WebhookReceiver {
    port: u16,
    envelopes: SharedEnvelopes,
}

impl WebhookReceiver {
    pub async fn spawn(port: u16) -> Result<Self> {
        let envelopes: SharedEnvelopes = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/hooks", post(record))
            .route("/last", get(last))
            .route("/all", get(all))
            .fallback(post(record))
            .with_state(envelopes.clone());
        Ok(Self {
            port: serve_http(port, app).await?,
            envelopes,
        })
    }

    /// The actually-bound port (ephemeral when spawned with 0).
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Clone-out snapshot of every recorded envelope, in arrival order.
    pub fn envelopes(&self) -> Vec<RecordedEnvelope> {
        self.envelopes.lock().unwrap().clone()
    }

    /// Base URL to hand to the pylon server as its webhook target.
    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

async fn record(
    State(envelopes): State<SharedEnvelopes>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let envelope = RecordedEnvelope {
        // HeaderName is lowercase by construction; to_ascii_lowercase keeps
        // the contract explicit.
        headers: headers
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_ascii_lowercase(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect(),
        body: String::from_utf8_lossy(&body).into_owned(),
    };
    envelopes.lock().unwrap().push(envelope);
    StatusCode::NO_CONTENT.into_response()
}

async fn last(State(envelopes): State<SharedEnvelopes>) -> Response {
    match envelopes.lock().unwrap().last() {
        Some(envelope) => Json(envelope.to_json()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn all(State(envelopes): State<SharedEnvelopes>) -> Response {
    Json(
        envelopes
            .lock()
            .unwrap()
            .iter()
            .map(RecordedEnvelope::to_json)
            .collect::<Vec<_>>(),
    )
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Minimal raw-TCP POST (same shape as `http_get`, plus Content-Length
    /// and extra headers); returns the full response string.
    async fn raw_post(port: u16, path: &str, extra_headers: &[(&str, &str)], body: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let mut headers = format!(
            "POST {path} HTTP/1.1\r\nHost: h\r\nContent-Length: {}\r\nConnection: close\r\n",
            body.len()
        );
        for (name, value) in extra_headers {
            headers.push_str(&format!("{name}: {value}\r\n"));
        }
        let request = format!("{headers}\r\n{body}");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut out = String::new();
        stream.read_to_string(&mut out).await.unwrap();
        out
    }

    /// Minimal raw-TCP GET returning the full response string (status line +
    /// headers + body).
    async fn raw_get(port: u16, path: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n").as_bytes(),
            )
            .await
            .unwrap();
        let mut out = String::new();
        stream.read_to_string(&mut out).await.unwrap();
        out
    }

    /// Response body half of a raw response string.
    fn response_body(response: &str) -> &str {
        response.split("\r\n\r\n").nth(1).unwrap_or("")
    }

    #[tokio::test]
    async fn health_get_returns_status() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 256];
            let _ = tokio::io::AsyncReadExt::read(&mut s, &mut buf).await;
            let _ = tokio::io::AsyncWriteExt::write_all(
                &mut s,
                b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
            )
            .await;
        });
        assert_eq!(http_get("127.0.0.1", port, "/health").await.unwrap(), 200);
    }

    #[tokio::test]
    async fn auth_endpoint_forwards_body_to_signer_and_returns_verbatim() {
        let signer: SignerFn = Arc::new(|body| {
            Box::pin(async move {
                assert_eq!(
                    body,
                    b"{\"socket_id\":\"1\",\"channel_name\":\"private-x\"}".to_vec()
                );
                Ok("{\"auth\":\"signed-by-stub\"}".to_string())
            })
        });
        let srv = AuthServer::spawn(0, signer).await.unwrap();
        let port = srv.port();
        let out = raw_post(
            port,
            "/pusher/auth",
            &[],
            "{\"socket_id\":\"1\",\"channel_name\":\"private-x\"}",
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 200"), "expected 200, got: {out}");
        assert!(out.contains("content-type: application/json"), "got: {out}");
        assert!(out.contains("signed-by-stub"));
    }

    #[tokio::test]
    async fn auth_endpoint_serves_all_three_sdk_paths_identically() {
        let signer: SignerFn = Arc::new(|_body| {
            Box::pin(async move { Ok("{\"auth\":\"signed-by-stub\"}".to_string()) })
        });
        let srv = AuthServer::spawn(0, signer).await.unwrap();
        for path in ["/auth", "/pusher/auth", "/pusher/user-auth"] {
            let out = raw_post(srv.port(), path, &[], "{\"socket_id\":\"1\"}").await;
            assert!(out.starts_with("HTTP/1.1 200"), "{path}: {out}");
            assert!(out.contains("signed-by-stub"), "{path}: {out}");
        }
    }

    #[tokio::test]
    async fn auth_endpoint_maps_signer_error_to_500() {
        let signer: SignerFn =
            Arc::new(|_body| Box::pin(async move { Err(anyhow::anyhow!("signer exploded")) }));
        let srv = AuthServer::spawn(0, signer).await.unwrap();
        let out = raw_post(srv.port(), "/pusher/auth", &[], "{}").await;
        assert!(out.starts_with("HTTP/1.1 500"), "got: {out}");
    }

    #[tokio::test]
    async fn webhook_receiver_records_envelopes_in_order() {
        let rx = WebhookReceiver::spawn(0).await.unwrap();
        let port = rx.port();
        let first = raw_post(
            port,
            "/hooks",
            &[("x-pusher-key", "cf-key-main")],
            r#"{"time_ms":1}"#,
        )
        .await;
        assert!(first.starts_with("HTTP/1.1 204"), "got: {first}");
        raw_post(
            port,
            "/hooks",
            &[("x-pusher-key", "cf-key-secondary")],
            r#"{"time_ms":2}"#,
        )
        .await;

        let envelopes = rx.envelopes();
        assert_eq!(envelopes.len(), 2);
        assert!(envelopes[0]
            .headers
            .contains(&("x-pusher-key".to_string(), "cf-key-main".to_string())));
        assert_eq!(envelopes[0].body, r#"{"time_ms":1}"#);
        assert!(envelopes[1]
            .headers
            .contains(&("x-pusher-key".to_string(), "cf-key-secondary".to_string())));
        assert_eq!(envelopes[1].body, r#"{"time_ms":2}"#);

        assert_eq!(rx.base_url(), format!("http://127.0.0.1:{port}"));
    }

    #[tokio::test]
    async fn webhook_last_and_all_serve_recorded_json() {
        let rx = WebhookReceiver::spawn(0).await.unwrap();
        let port = rx.port();

        // Nothing recorded yet: /last reports 404, /all an empty array.
        assert_eq!(http_get("127.0.0.1", port, "/last").await.unwrap(), 404);
        let all_empty = raw_get(port, "/all").await;
        assert_eq!(response_body(&all_empty), "[]");

        // One envelope, fetched back on /last with the verifier's shape.
        raw_post(
            port,
            "/hooks",
            &[
                ("x-pusher-key", "cf-key-main"),
                ("content-type", "application/json"),
            ],
            r#"{"events":[]}"#,
        )
        .await;
        let last = raw_get(port, "/last").await;
        assert!(last.starts_with("HTTP/1.1 200"), "got: {last}");
        let v: serde_json::Value = serde_json::from_str(response_body(&last)).unwrap();
        assert_eq!(v["headers"]["x-pusher-key"], "cf-key-main");
        assert_eq!(v["headers"]["content-type"], "application/json");
        assert_eq!(v["body"], r#"{"events":[]}"#);
        assert_eq!(v.as_object().unwrap().len(), 2, "exactly headers + body");

        // A second envelope: /last moves on, /all keeps both in order.
        raw_post(
            port,
            "/hooks",
            &[("x-pusher-key", "cf-key-2")],
            r#"{"more":true}"#,
        )
        .await;
        let v: serde_json::Value =
            serde_json::from_str(response_body(&raw_get(port, "/last").await)).unwrap();
        assert_eq!(v["body"], r#"{"more":true}"#);
        let all: serde_json::Value =
            serde_json::from_str(response_body(&raw_get(port, "/all").await)).unwrap();
        assert_eq!(all.as_array().unwrap().len(), 2);
        assert_eq!(all[0]["headers"]["x-pusher-key"], "cf-key-main");
        assert_eq!(all[1]["body"], r#"{"more":true}"#);
    }
}
