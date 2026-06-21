//! Integration tests for the admin invalidate endpoint over the REAL REST plane
//! (the percore mio→tokio HTTP handoff + the axum router): routing, the
//! auth-before-parse + 4KB body-cap hardening, content-type independence, and the
//! RestError→HTTP rendering. The Redis-backed 202 success path is unit-tested in
//! `http::rest::admin` (it needs a live `AppInvalidator`); the shared harness here
//! wires `invalidator: None`, which exercises every other path end-to-end.

mod common;

use pylon::server::config::ServerConfig;
use std::net::SocketAddr;

const APPS: &str = r#"[{"name":"A","id":"app1","key":"app-key","secret":"s"}]"#;

fn config(admin_token: Option<&str>) -> ServerConfig {
    ServerConfig {
        app_admin_token: admin_token.map(|t| t.to_string()),
        ..ServerConfig::default()
    }
}

fn url(addr: SocketAddr) -> String {
    format!("http://{addr}/admin/apps/app1/invalidate")
}

/// No `PYLON_ADMIN_TOKEN` ⇒ the admin API is disabled and the route returns 404
/// (over real HTTP, proving routing + the disabled gate + error rendering).
#[tokio::test]
async fn admin_disabled_returns_404() {
    let addr = common::spawn(common::SpawnSpec::with_apps(config(None), APPS)).await;
    let r = reqwest::Client::new()
        .post(url(addr))
        .body(r#"{"key":"app-key"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), reqwest::StatusCode::NOT_FOUND);
}

/// A wrong Bearer token ⇒ 401, end-to-end.
#[tokio::test]
async fn admin_bad_token_returns_401() {
    let addr = common::spawn(common::SpawnSpec::with_apps(config(Some("secret")), APPS)).await;
    let r = reqwest::Client::new()
        .post(url(addr))
        .header("Authorization", "Bearer wrong")
        .body(r#"{"key":"app-key"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), reqwest::StatusCode::UNAUTHORIZED);
}

/// A body larger than the admin route's 4 KB cap is rejected by the
/// `DefaultBodyLimit` layer BEFORE the handler runs (defense-in-depth: the admin
/// body is a tiny `{"key":…}`). This hardening has no unit-level equivalent — the
/// cap lives on the router, so only an HTTP request exercises it.
#[tokio::test]
async fn admin_oversized_body_hits_the_4kb_cap_413() {
    let addr = common::spawn(common::SpawnSpec::with_apps(config(Some("secret")), APPS)).await;
    let big = format!(r#"{{"key":"{}"}}"#, "x".repeat(5000));
    let r = reqwest::Client::new()
        .post(url(addr))
        .header("Authorization", "Bearer secret")
        .body(big)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        reqwest::StatusCode::PAYLOAD_TOO_LARGE,
        "a >4KB admin body must hit the 4KB cap (413), never the handler"
    );
}

/// A fully-valid authenticated request reaches the handler; with the harness's
/// `invalidator: None` it returns 503. Sending `text/plain` (not
/// `application/json`) still works because the body is taken as raw `Bytes` — there
/// is NO content-type gate (a `Json<…>` extractor would have 415'd). This proves
/// both content-type independence and the no-invalidator path over real HTTP.
#[tokio::test]
async fn admin_authed_parses_any_content_type_then_503_without_invalidator() {
    let addr = common::spawn(common::SpawnSpec::with_apps(config(Some("secret")), APPS)).await;
    let r = reqwest::Client::new()
        .post(url(addr))
        .header("Authorization", "Bearer secret")
        .header("Content-Type", "text/plain")
        .body(r#"{"key":"app-key"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "valid authed request, no invalidator ⇒ 503 (content-type is irrelevant)"
    );
}
