//! REST HTTP API integration tests: signed requests, delivery, info endpoints.

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::auth::signature::{channel_signature, hmac_sha256_hex, md5_hex};
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::server::router::{build_router, AppState};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_tungstenite::tungstenite::Message;

const APPS: &str = r#"[
    {"name":"Test","id":"app1","key":"app-key","secret":"app-secret",
     "client_messages_enabled":true,"subscription_count_enabled":false},
    {"name":"Test2","id":"app2","key":"app2-key","secret":"app2-secret",
     "client_messages_enabled":true,"subscription_count_enabled":true}
]"#;
/// Same as [`APPS`] plus a DISABLED app (`"enabled": false`) — the R1 fixture:
/// a disabled app exists but must not authenticate (REST 403 / WS 4003).
const APPS_WITH_DISABLED: &str = r#"[
    {"name":"Test","id":"app1","key":"app-key","secret":"app-secret",
     "client_messages_enabled":true,"subscription_count_enabled":false},
    {"name":"Test2","id":"app2","key":"app2-key","secret":"app2-secret",
     "client_messages_enabled":true,"subscription_count_enabled":true},
    {"name":"Off","id":"off-app","key":"off-key","secret":"off-secret",
     "enabled":false}
]"#;
const SECRET: &str = "app-secret";
const SECRET2: &str = "app2-secret";
const OFF_SECRET: &str = "off-secret";

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Spawn the multi-app REST + WS server on a real single-worker percore fleet
/// (the only transport). The worker serves WS clients directly and hands plain-
/// HTTP (REST) connections off to the axum `Router` via the handoff plane, exactly
/// as `main.rs` wires the single-node percore path. The worker thread + shutdown
/// flag are leaked; the OS reclaims them at process exit (test processes are
/// short-lived).
async fn spawn() -> SocketAddr {
    spawn_with_apps(APPS).await
}

/// [`spawn`] with a custom apps.json fixture (e.g. [`APPS_WITH_DISABLED`]).
async fn spawn_with_apps(apps_json: &str) -> SocketAddr {
    spawn_configured(apps_json, |_| {}).await
}

/// [`spawn_with_apps`] plus a [`ServerConfig`] tuning hook — e.g. a short
/// `cache_ttl_secs` for expired-cache tests.
async fn spawn_configured(apps_json: &str, with: impl FnOnce(&mut ServerConfig)) -> SocketAddr {
    use std::sync::atomic::AtomicBool;

    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(apps_json).unwrap());
    let local = Arc::new(LocalAdapter::new(
        Arc::new(Registry::new()),
        Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
    ));
    let adapter: Arc<dyn Adapter> = local.clone();
    let conn_counts = Arc::new(Default::default());
    let webhooks = pylon::webhook::WebhookHandle::null();

    // Reserve a free ephemeral port, then release it before the worker re-binds
    // with SO_REUSEPORT (race-free in practice).
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    // One worker keeps subscribe/broadcast ordering a single sequential stream.
    let mut config = ServerConfig {
        bind: "127.0.0.1".into(),
        port,
        workers: 1,
        ..ServerConfig::default()
    };
    with(&mut config);

    // REST handoff plane: the worker hands plain-HTTP connections to this axum
    // router via `rest_tx`; `rest::serve` drives them on the tokio runtime.
    let (rest_tx, rest_rx) = tokio::sync::mpsc::unbounded_channel::<pylon::transport::RestConn>();
    let rest_state = AppState {
        config: config.clone(),
        apps: apps.clone(),
        adapter: adapter.clone(),
        conn_counts: Arc::clone(&conn_counts),
        webhooks: webhooks.clone(),
        saturated: Some(local.saturation_flag()),
        draining: Arc::new(AtomicBool::new(false)),
        cluster_metrics: None,
        invalidator: None,
    };
    tokio::spawn(pylon::transport::rest::serve(
        rest_rx,
        build_router(rest_state),
    ));

    // Run the blocking `mio` worker on a dedicated thread.
    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = shutdown.clone();
    let worker_config = config.clone();
    let local_for_sink = Some(local.clone());
    // Phase 7: capture the runtime handle here (async context) before spawning.
    let worker_runtime = tokio::runtime::Handle::current();
    let handle = std::thread::spawn(move || {
        let _ = pylon::transport::run_percore(
            worker_config,
            apps,
            adapter,
            conn_counts,
            Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            webhooks,
            Some(rest_tx),
            worker_shutdown,
            local_for_sink,
            // Single-node (not clustered).
            false,
            None,
            None,
            worker_runtime,
        );
    });
    std::mem::forget((shutdown, handle));

    // Give the worker a moment to bind its SO_REUSEPORT listener.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    format!("127.0.0.1:{port}").parse().unwrap()
}

/// Build the signed query string for a request, returning the full URL query.
fn signed_query(method: &str, path: &str, body: &[u8], extra: &[(&str, &str)]) -> String {
    signed_query_as("app-key", SECRET, method, path, body, extra)
}

/// [`signed_query`] for app2 (the subscription_count-enabled app).
fn signed_query2(method: &str, path: &str, body: &[u8], extra: &[(&str, &str)]) -> String {
    signed_query_as("app2-key", SECRET2, method, path, body, extra)
}

/// [`signed_query`] for the DISABLED app in [`APPS_WITH_DISABLED`].
fn signed_query_off(method: &str, path: &str, body: &[u8], extra: &[(&str, &str)]) -> String {
    signed_query_as("off-key", OFF_SECRET, method, path, body, extra)
}

/// Build the signed query string for `key`/`secret` (the shared core of the
/// per-app `signed_query*` helpers).
fn signed_query_as(
    key: &str,
    secret: &str,
    method: &str,
    path: &str,
    body: &[u8],
    extra: &[(&str, &str)],
) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut p: BTreeMap<String, String> = BTreeMap::new();
    p.insert("auth_key".into(), key.to_string());
    p.insert("auth_timestamp".into(), now.to_string());
    p.insert("auth_version".into(), "1.0".into());
    if !body.is_empty() {
        p.insert("body_md5".into(), md5_hex(body));
    }
    for (k, v) in extra {
        p.insert((*k).to_string(), (*v).to_string());
    }
    let canon = p
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let signed = format!("{}\n{}\n{}", method.to_uppercase(), path, canon);
    let sig = hmac_sha256_hex(secret, &signed);
    format!("{canon}&auth_signature={sig}")
}

async fn connect_ws(addr: SocketAddr) -> Ws {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/app/app-key?protocol=7"))
        .await
        .unwrap();
    ws
}

async fn connect_ws2(addr: SocketAddr) -> Ws {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/app/app2-key?protocol=7"))
        .await
        .unwrap();
    ws
}

async fn next_json(ws: &mut Ws) -> Value {
    loop {
        if let Message::Text(t) = ws.next().await.unwrap().unwrap() {
            return serde_json::from_str(&t).unwrap();
        }
    }
}

/// Read the `connection_established` frame and return the assigned socket_id.
async fn established_socket_id(ws: &mut Ws) -> String {
    let frame = next_json(ws).await;
    let data: Value = serde_json::from_str(frame["data"].as_str().unwrap()).unwrap();
    data["socket_id"].as_str().unwrap().to_string()
}

/// Subscribe an established `ws` (socket_id already read) to a presence
/// channel as `user_id`, consuming the roster success frame.
async fn subscribe_presence(ws: &mut Ws, socket_id: &str, channel: &str, user_id: &str) {
    let channel_data = json!({"user_id": user_id}).to_string();
    let token = format!(
        "app-key:{}",
        channel_signature(SECRET, socket_id, channel, Some(&channel_data))
    );
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{
            "channel": channel, "auth": token, "channel_data": channel_data
        }})
        .to_string(),
    ))
    .await
    .unwrap();
    let _ = next_json(ws).await; // pusher_internal:subscription_succeeded (roster)
}

/// Subscribe `ws` to a public channel and consume the success frame.
async fn subscribe_public(ws: &mut Ws, channel: &str) {
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":channel}}).to_string(),
    ))
    .await
    .unwrap();
    let _ = next_json(ws).await; // subscription_succeeded
}

#[tokio::test]
async fn rest_trigger_delivers_to_subscriber() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let _ = next_json(&mut ws).await; // established
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":"public-room"}}).to_string(),
    ))
    .await
    .unwrap();
    let _ = next_json(&mut ws).await; // subscription_succeeded

    let body =
        json!({"name":"my-event","data":"{\"hi\":1}","channels":["public-room"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "my-event");
    assert_eq!(frame["channel"], "public-room");
    assert_eq!(frame["data"], "{\"hi\":1}"); // delivered verbatim as a string
}

#[tokio::test]
async fn rest_bad_signature_is_401() {
    let addr = spawn().await;
    let body = json!({"name":"e","data":"{}","channels":["c"]}).to_string();
    let mut q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    q = q.replace(
        &q[q.rfind("auth_signature=").unwrap()..],
        "auth_signature=deadbeef",
    );
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn rest_get_channel_reports_occupancy() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let _ = next_json(&mut ws).await;
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":"public-room"}}).to_string(),
    ))
    .await
    .unwrap();
    let _ = next_json(&mut ws).await;

    let q = signed_query(
        "GET",
        "/apps/app1/channels/public-room",
        b"",
        &[("info", "subscription_count")],
    );
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app1/channels/public-room?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["occupied"], true);
    // app1 has subscription_count_enabled:false → attribute must be omitted
    assert!(
        v.get("subscription_count").is_none(),
        "subscription_count must be absent when flag is off, got: {v}"
    );
}

/// GET /channels/:name with subscription_count_enabled=true → attribute present.
#[tokio::test]
async fn rest_get_channel_subscription_count_enabled() {
    let addr = spawn().await;
    let mut ws = connect_ws2(addr).await;
    let _ = next_json(&mut ws).await;
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":"public-room"}}).to_string(),
    ))
    .await
    .unwrap();
    let _ = next_json(&mut ws).await;

    let q = signed_query2(
        "GET",
        "/apps/app2/channels/public-room",
        b"",
        &[("info", "subscription_count")],
    );
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app2/channels/public-room?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["occupied"], true);
    // app2 has subscription_count_enabled:true → attribute must be present
    assert_eq!(
        v["subscription_count"], 1,
        "subscription_count must be 1 when flag is on, got: {v}"
    );
}

/// R7: trigger an event on a cache channel, then GET info=cache — the response
/// carries the doc shape `{"cache": {"data": ..., "ttl": ...}}` with the cached
/// payload (verbatim data string) and the channel's cache TTL in seconds.
#[tokio::test]
async fn rest_get_channel_cache_info_returns_cached_payload() {
    let addr = spawn().await;
    let body = json!({"name":"my-event","data":"{\"hi\":1}","channels":["cache-feed"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let q = signed_query(
        "GET",
        "/apps/app1/channels/cache-feed",
        b"",
        &[("info", "cache")],
    );
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app1/channels/cache-feed?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["occupied"], false, "no subscriber is attached");
    // Doc: "Cached data and TTL (in seconds) for this channel or null in case
    // the cache is empty." — the data is the verbatim event payload string.
    assert_eq!(v["cache"]["data"], "{\"hi\":1}", "got: {v}");
    // The channel's cache TTL (default `cache_ttl_secs`) in seconds.
    assert_eq!(v["cache"]["ttl"], 1800, "got: {v}");
}

/// R7: GET info=cache on a cache channel that never saw an event → the doc's
/// empty case: `"cache": null`.
#[tokio::test]
async fn rest_get_channel_cache_info_null_when_cache_empty() {
    let addr = spawn().await;
    let q = signed_query(
        "GET",
        "/apps/app1/channels/cache-empty",
        b"",
        &[("info", "cache")],
    );
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app1/channels/cache-empty?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["occupied"], false);
    assert!(v["cache"].is_null(), "empty cache must be null, got: {v}");
}

/// R7: after the cache TTL elapses the entry reads as empty ("null in case the
/// cache is empty") — `cache_get` is TTL-aware on every adapter.
#[tokio::test]
async fn rest_get_channel_cache_info_null_after_ttl_expiry() {
    let addr = spawn_configured(APPS, |c| c.cache_ttl_secs = 1).await;
    let body = json!({"name":"my-event","data":"{\"hi\":1}","channels":["cache-feed"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let get = || async {
        let q = signed_query(
            "GET",
            "/apps/app1/channels/cache-feed",
            b"",
            &[("info", "cache")],
        );
        let resp = reqwest::Client::new()
            .get(format!("http://{addr}/apps/app1/channels/cache-feed?{q}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        resp.json::<Value>().await.unwrap()
    };

    // Fresh cache: payload present.
    let fresh = get().await;
    assert_eq!(fresh["cache"]["data"], "{\"hi\":1}", "got: {fresh}");
    // Past the 1s TTL: expired reads as empty → null.
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let expired = get().await;
    assert!(
        expired["cache"].is_null(),
        "expired cache must be null, got: {expired}"
    );
}

// ── R8 — inapplicable info attributes on channel queries are 400 ────────────
//
// Verified against https://pusher.com/docs/channels/library_auth_reference/rest-api/
// (fetched 2026-08-30). GET /apps/[app_id]/channels/[channel_name], "Available
// info attributes" table (applicability column):
//
//   user_count         — Presence
//   subscription_count — All (except Presence channels)
//   cache              — Cache
//
// "Requesting an attribute which is not available for the requested channel
// will return an error (for example requesting a the `user_count` for a public
// channel)."

/// R8: GET /channels/:name?info=user_count on each non-presence kind → 400
/// (the doc's literal example is user_count on a public channel).
#[tokio::test]
async fn rest_get_channel_user_count_on_non_presence_is_400() {
    let addr = spawn().await;
    for ch in ["public-room", "private-room", "private-encrypted-room"] {
        let q = signed_query(
            "GET",
            &format!("/apps/app1/channels/{ch}"),
            b"",
            &[("info", "user_count")],
        );
        let resp = reqwest::Client::new()
            .get(format!("http://{addr}/apps/app1/channels/{ch}?{q}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400, "user_count on {ch} must be 400");
        let v: Value = resp.json().await.unwrap();
        assert_eq!(v["status"], 400, "error body must be JSON, got: {v}");
        assert!(
            v["error"].is_string(),
            "error body must carry error, got: {v}"
        );
    }
}

/// R8 positive control: user_count on a presence channel → 200 with the count.
#[tokio::test]
async fn rest_get_channel_user_count_on_presence_is_200() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let socket_id = established_socket_id(&mut ws).await;
    subscribe_presence(&mut ws, &socket_id, "presence-room", "u1").await;

    let q = signed_query(
        "GET",
        "/apps/app1/channels/presence-room",
        b"",
        &[("info", "user_count")],
    );
    let resp = reqwest::Client::new()
        .get(format!(
            "http://{addr}/apps/app1/channels/presence-room?{q}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["user_count"], 1, "got: {v}");
}

/// R8: subscription_count on a presence channel → 400 ("All (except Presence
/// channels)"). Uses app2 (subscription_count_enabled=true) so the 400 is
/// proven to come from channel-type applicability, not the app setting.
/// Positive controls on non-presence channels: rest_get_channel_subscription_
/// count_enabled / rest_get_channel_reports_occupancy.
#[tokio::test]
async fn rest_get_channel_subscription_count_on_presence_is_400() {
    let addr = spawn().await;
    let q = signed_query2(
        "GET",
        "/apps/app2/channels/presence-room",
        b"",
        &[("info", "subscription_count")],
    );
    let resp = reqwest::Client::new()
        .get(format!(
            "http://{addr}/apps/app2/channels/presence-room?{q}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "subscription_count on presence must be 400"
    );
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["status"], 400, "error body must be JSON, got: {v}");
    assert!(
        v["error"].is_string(),
        "error body must carry error, got: {v}"
    );
}

/// R8: cache on each non-cache channel kind → 400 ("Cache" applicability).
/// `presence-room` proves auth kind and cache-ness are orthogonal dimensions.
/// Positive controls on cache channels: the R7 cache tests above.
#[tokio::test]
async fn rest_get_channel_cache_on_non_cache_is_400() {
    let addr = spawn().await;
    for ch in ["public-room", "private-room", "presence-room"] {
        let q = signed_query(
            "GET",
            &format!("/apps/app1/channels/{ch}"),
            b"",
            &[("info", "cache")],
        );
        let resp = reqwest::Client::new()
            .get(format!("http://{addr}/apps/app1/channels/{ch}?{q}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400, "cache on {ch} must be 400");
        let v: Value = resp.json().await.unwrap();
        assert_eq!(v["status"], 400, "error body must be JSON, got: {v}");
        assert!(
            v["error"].is_string(),
            "error body must carry error, got: {v}"
        );
    }
}

/// R8: `cache` stays applicable on a presence-cache channel (a cache channel of
/// any auth kind): no cached event → the doc's empty case, `"cache": null`.
#[tokio::test]
async fn rest_get_channel_cache_on_presence_cache_channel_is_200() {
    let addr = spawn().await;
    let q = signed_query(
        "GET",
        "/apps/app1/channels/presence-cache-room",
        b"",
        &[("info", "cache")],
    );
    let resp = reqwest::Client::new()
        .get(format!(
            "http://{addr}/apps/app1/channels/presence-cache-room?{q}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert!(
        v["cache"].is_null(),
        "empty cache on a presence-cache channel must be null, got: {v}"
    );
}

/// POST /events with info=subscription_count and flag OFF → attribute omitted.
#[tokio::test]
async fn rest_trigger_info_subscription_count_disabled() {
    let addr = spawn().await;
    let body =
        json!({"name":"ev","data":"{}","channels":["public-room"],"info":"subscription_count"})
            .to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    // channels key present but subscription_count must be absent per-channel
    let ch = &v["channels"]["public-room"];
    assert!(
        ch.get("subscription_count").is_none(),
        "subscription_count must be absent when flag is off, got: {v}"
    );
}

/// POST /events with info=subscription_count and flag ON → attribute present.
#[tokio::test]
async fn rest_trigger_info_subscription_count_enabled() {
    let addr = spawn().await;
    // Subscribe a client to the channel so subscription_count > 0.
    let mut ws = connect_ws2(addr).await;
    let _ = next_json(&mut ws).await;
    subscribe_public(&mut ws, "public-room").await;

    let body =
        json!({"name":"ev","data":"{}","channels":["public-room"],"info":"subscription_count"})
            .to_string();
    let q = signed_query2("POST", "/apps/app2/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app2/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    let ch = &v["channels"]["public-room"];
    assert_eq!(
        ch["subscription_count"], 1,
        "subscription_count must be present when flag is on, got: {v}"
    );
}

#[tokio::test]
async fn rest_batch_events_delivers_to_two_channels() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let _ = next_json(&mut ws).await; // established
    subscribe_public(&mut ws, "room-a").await;
    subscribe_public(&mut ws, "room-b").await;

    let body = json!({"batch":[
        {"name":"ev-a","data":"1","channel":"room-a"},
        {"name":"ev-b","data":"2","channel":"room-b"}
    ]})
    .to_string();
    let q = signed_query("POST", "/apps/app1/batch_events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/batch_events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Both events are fanned out; collect by channel to be order-independent.
    let mut got = std::collections::HashMap::new();
    for _ in 0..2 {
        let f = next_json(&mut ws).await;
        got.insert(
            f["channel"].as_str().unwrap().to_string(),
            f["event"].as_str().unwrap().to_string(),
        );
    }
    assert_eq!(got.get("room-a").map(String::as_str), Some("ev-a"));
    assert_eq!(got.get("room-b").map(String::as_str), Some("ev-b"));
}

// ── R9 parity tests — POST trigger params MAY go in the query string ─────────
//
// Pusher REST doc (https://pusher.com/docs/channels/library_auth_reference/
// rest-api/), General section: "For POST requests, parameters MAY be submitted
// in the query string but SHOULD be submitted in the POST body as a JSON hash
// (while setting Content-Type:application/json)." The trigger endpoint adds:
// "NOTE: For POST requests, we recommend including parameters in the JSON body.
// If using the query string, send arrays as channels[]=channel1&channels[]=
// channel2". Pylon previously read trigger fields ONLY from the JSON body, so a
// query-string-encoded trigger was rejected with 400.

/// R9: a fully query-string-encoded trigger with an EMPTY body → 200 `{}` and
/// the subscriber receives the event. Auth params and trigger params share the
/// query string, so the signature covers the trigger fields too (body_md5 is
/// simply absent — there is no body). Pusher SDKs sign the DECODED `k=v` pairs
/// and percent-encode only on the wire: `data=%22hi%22` decodes to the string
/// `"hi"` (quotes included) — exactly the string the body form
/// `{"data":"\"hi\""}` carries — so the two paths must be byte-identical.
#[tokio::test]
async fn rest_trigger_all_params_in_query_string_is_200() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let _ = next_json(&mut ws).await; // established
    subscribe_public(&mut ws, "qc").await;

    // Sign over the DECODED values; empty body → no body_md5 param.
    let q = signed_query(
        "POST",
        "/apps/app1/events",
        b"",
        &[("name", "qse"), ("channel", "qc"), ("data", "\"hi\"")],
    );
    // Percent-encode the wire form of `data` only (the signature, computed over
    // the decoded value, is untouched) — mimics what a Pusher SDK sends.
    let q = set_query_param(&q, "data", "%22hi%22");
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v, json!({}));

    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "qse");
    assert_eq!(frame["channel"], "qc");
    // Delivered verbatim: the data string is `"hi"` WITH the quotes.
    assert_eq!(frame["data"], "\"hi\"");
}

/// R9 precedence pin: when the body and the query string both carry a field,
/// the BODY's value wins (the doc's SHOULD form beats its MAY form). Body
/// `channel=body-ch` + query `channel=query-ch` → the event goes to body-ch
/// under the body's event name; the query channel hears nothing.
#[tokio::test]
async fn rest_trigger_body_field_beats_query_field() {
    let addr = spawn().await;
    let mut body_ws = connect_ws(addr).await;
    let _ = next_json(&mut body_ws).await; // established
    subscribe_public(&mut body_ws, "body-ch").await;
    let mut query_ws = connect_ws(addr).await;
    let _ = next_json(&mut query_ws).await; // established
    subscribe_public(&mut query_ws, "query-ch").await;

    let body = json!({"name":"ev","data":"{}","channel":"body-ch"}).to_string();
    let q = signed_query(
        "POST",
        "/apps/app1/events",
        body.as_bytes(),
        &[("channel", "query-ch"), ("name", "query-name")],
    );
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // The event lands on the BODY's channel under the BODY's event name.
    let frame = tokio::time::timeout(std::time::Duration::from_secs(2), next_json(&mut body_ws))
        .await
        .expect("body channel must receive the event");
    assert_eq!(frame["channel"], "body-ch");
    assert_eq!(frame["event"], "ev");

    // The query-string channel must NOT receive anything (body won).
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(300),
            next_json(&mut query_ws)
        )
        .await
        .is_err(),
        "query-string channel must not receive the event when the body carries the field"
    );
}

/// R9 scope pin: batch_events takes NO query fallback. Its sole parameter
/// `batch` is an array of event objects with no documented query-string
/// representation (the doc's only arrays-in-query note — `channels[]=…` —
/// appears under the single trigger endpoint), so an empty body stays a 400.
#[tokio::test]
async fn rest_batch_events_empty_body_still_400() {
    let addr = spawn().await;
    let q = signed_query("POST", "/apps/app1/batch_events", b"", &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/batch_events?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn rest_get_channels_lists_occupied_channel() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let _ = next_json(&mut ws).await;
    subscribe_public(&mut ws, "public-room").await;

    let q = signed_query("GET", "/apps/app1/channels", b"", &[]);
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app1/channels?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert!(v["channels"]["public-room"].is_object());
}

// P15 — GET /channels list must emit per-channel subscription_count

/// GET /channels?info=subscription_count with flag ON → each channel carries subscription_count.
#[tokio::test]
async fn rest_get_channels_list_subscription_count_enabled() {
    let addr = spawn().await;
    // Connect on app2 which has subscription_count_enabled=true.
    let mut ws = connect_ws2(addr).await;
    let _ = next_json(&mut ws).await; // established
    subscribe_public(&mut ws, "public-room").await;

    let q = signed_query2(
        "GET",
        "/apps/app2/channels",
        b"",
        &[("info", "subscription_count")],
    );
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app2/channels?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(
        v["channels"]["public-room"]["subscription_count"], 1,
        "GET /channels with flag ON must emit subscription_count per channel (P15), got: {v}"
    );
}

/// GET /channels?info=subscription_count with flag OFF → attribute absent.
#[tokio::test]
async fn rest_get_channels_list_subscription_count_disabled() {
    let addr = spawn().await;
    // app1 has subscription_count_enabled=false.
    let mut ws = connect_ws(addr).await;
    let _ = next_json(&mut ws).await;
    subscribe_public(&mut ws, "public-room").await;

    let q = signed_query(
        "GET",
        "/apps/app1/channels",
        b"",
        &[("info", "subscription_count")],
    );
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app1/channels?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert!(
        v["channels"]["public-room"]
            .get("subscription_count")
            .is_none(),
        "GET /channels with flag OFF must NOT emit subscription_count (P15), got: {v}"
    );
}

// R8, collection endpoint (GET /apps/[app_id]/channels): the doc's "Available
// info attributes" table there has a single row — `user_count` (Presence) —
// and "If an attribute such as `user_count` is requested, and the request is
// not limited to presence channels, the API will return an error (400 code)."
// That 400 is pinned by rest_channels_user_count_without_presence_filter_is_
// 400 / _with_presence_filter_is_200 (P7b). `cache` read-back is documented
// only for the single-channel endpoint, so requesting it here is the
// inapplicable-attribute 400. `subscription_count` is absent from the
// collection table too, but is deliberately kept working (flag-gated) — see
// the comment on the handler.

/// R8: GET /channels?info=cache → 400 (no `cache` row in the collection
/// endpoint's info table; cached-data read-back is single-channel only).
#[tokio::test]
async fn rest_get_channels_cache_info_is_400() {
    let addr = spawn().await;
    let q = signed_query("GET", "/apps/app1/channels", b"", &[("info", "cache")]);
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app1/channels?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "cache on the collection endpoint must be 400"
    );
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["status"], 400, "error body must be JSON, got: {v}");
    assert!(
        v["error"].is_string(),
        "error body must carry error, got: {v}"
    );
}

/// R8 positive control: GET /channels?info=user_count&filter_by_prefix=
/// presence- returns the count for an occupied presence channel (P7b only
/// pinned the status code, not the payload).
#[tokio::test]
async fn rest_get_channels_user_count_with_presence_filter_returns_count() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let socket_id = established_socket_id(&mut ws).await;
    subscribe_presence(&mut ws, &socket_id, "presence-room", "u1").await;

    let q = signed_query(
        "GET",
        "/apps/app1/channels",
        b"",
        &[("info", "user_count"), ("filter_by_prefix", "presence-")],
    );
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app1/channels?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["channels"]["presence-room"]["user_count"], 1, "got: {v}");
}

#[tokio::test]
async fn rest_get_users_lists_presence_members() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let socket_id = established_socket_id(&mut ws).await;

    let channel = "presence-room";
    let channel_data = json!({"user_id":"u1","user_info":{"name":"U"}}).to_string();
    let token = format!(
        "app-key:{}",
        channel_signature(SECRET, &socket_id, channel, Some(&channel_data))
    );
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{
            "channel": channel, "auth": token, "channel_data": channel_data
        }})
        .to_string(),
    ))
    .await
    .unwrap();
    let _ = next_json(&mut ws).await; // subscription_succeeded (presence roster)

    let q = signed_query("GET", "/apps/app1/channels/presence-room/users", b"", &[]);
    let resp = reqwest::Client::new()
        .get(format!(
            "http://{addr}/apps/app1/channels/presence-room/users?{q}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["users"], json!([{"id": "u1"}]));
}

#[tokio::test]
async fn rest_trigger_relays_to_encrypted_subscriber() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let socket_id = established_socket_id(&mut ws).await;

    // Subscribe to an encrypted channel (private-style token, no channel_data).
    let channel = "private-encrypted-room";
    let token = format!(
        "app-key:{}",
        channel_signature(SECRET, &socket_id, channel, None)
    );
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":channel,"auth":token}}).to_string(),
    ))
    .await
    .unwrap();
    let succ = next_json(&mut ws).await;
    assert_eq!(succ["event"], "pusher_internal:subscription_succeeded");

    // REST-trigger an opaque ciphertext payload; pylon must relay it verbatim.
    // `data` is a string on the wire (what Pusher server SDKs send for encrypted).
    let cipher = "{\"nonce\":\"abc\",\"ciphertext\":\"xyz\"}";
    let body = json!({"name":"secret","data":cipher,"channels":[channel]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "secret");
    assert_eq!(frame["channel"], channel);
    assert_eq!(frame["data"], cipher); // verbatim, untouched
}

#[tokio::test]
async fn rest_trigger_two_encrypted_channels_is_400() {
    let addr = spawn().await;
    let body = json!({
        "name": "secret",
        "data": "x",
        "channels": ["private-encrypted-a", "private-encrypted-b"]
    })
    .to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

/// Encrypted channel alongside ANY other channel must be rejected (400).
#[tokio::test]
async fn rest_trigger_encrypted_plus_public_is_400() {
    let addr = spawn().await;
    let body = json!({
        "name": "secret",
        "data": "x",
        "channels": ["private-encrypted-a", "public-b"]
    })
    .to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

/// An empty channel name must be rejected on the REST trigger path (400) — parity P14.
#[tokio::test]
async fn rest_trigger_empty_channel_name_is_400() {
    let addr = spawn().await;
    let body = json!({
        "name": "e",
        "data": "x",
        "channels": [""]
    })
    .to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

/// A single encrypted channel alone is allowed (200).
#[tokio::test]
async fn rest_trigger_encrypted_solo_is_200() {
    let addr = spawn().await;
    let body = json!({
        "name": "secret",
        "data": "x",
        "channels": ["private-encrypted-a"]
    })
    .to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

/// Two plaintext channels together are still allowed (200).
#[tokio::test]
async fn rest_trigger_two_plaintext_channels_is_200() {
    let addr = spawn().await;
    let body = json!({
        "name": "ev",
        "data": "x",
        "channels": ["public-a", "public-b"]
    })
    .to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn rest_trigger_caches_event_for_later_subscriber() {
    let addr = spawn().await;

    // Trigger to a cache channel BEFORE anyone subscribes — only the cache write matters.
    let body = json!({"name":"my-event","data":"{\"hi\":1}","channels":["cache-feed"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // A new subscriber gets subscription_succeeded, then the replayed cached event.
    let mut ws = connect_ws(addr).await;
    let _ = next_json(&mut ws).await; // established
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":"cache-feed"}}).to_string(),
    ))
    .await
    .unwrap();
    let succ = next_json(&mut ws).await;
    assert_eq!(succ["event"], "pusher_internal:subscription_succeeded");
    let replay = next_json(&mut ws).await;
    assert_eq!(replay["event"], "my-event");
    assert_eq!(replay["channel"], "cache-feed");
    assert_eq!(replay["data"], "{\"hi\":1}"); // verbatim
}

#[tokio::test]
async fn cache_subscribe_with_no_cache_emits_cache_miss() {
    let addr = spawn().await;
    let mut ws = connect_ws(addr).await;
    let _ = next_json(&mut ws).await; // established
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":"cache-empty"}}).to_string(),
    ))
    .await
    .unwrap();
    let succ = next_json(&mut ws).await;
    assert_eq!(succ["event"], "pusher_internal:subscription_succeeded");
    let miss = next_json(&mut ws).await;
    assert_eq!(miss["event"], "pusher:cache_miss");
    assert_eq!(miss["channel"], "cache-empty");
    assert!(miss.get("data").is_none(), "cache_miss has no data field");
}

#[tokio::test]
async fn private_cache_subscribe_replays_after_auth() {
    let addr = spawn().await;

    // Cache an event on a private-cache channel via REST.
    let body = json!({"name":"e","data":"\"v\"","channels":["private-cache-x"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Authenticate + subscribe, then receive the replay.
    let mut ws = connect_ws(addr).await;
    let socket_id = established_socket_id(&mut ws).await;
    let token = format!(
        "app-key:{}",
        channel_signature(SECRET, &socket_id, "private-cache-x", None)
    );
    ws.send(Message::Text(
        json!({"event":"pusher:subscribe","data":{"channel":"private-cache-x","auth":token}})
            .to_string(),
    ))
    .await
    .unwrap();
    let succ = next_json(&mut ws).await;
    assert_eq!(succ["event"], "pusher_internal:subscription_succeeded");
    let replay = next_json(&mut ws).await;
    assert_eq!(replay["event"], "e");
    assert_eq!(replay["channel"], "private-cache-x");
}

// ── P7 parity tests ─────────────────────────────────────────────────────────

/// P7(a): event `data` exceeding per-event cap → 413, not 400.
#[tokio::test]
async fn rest_event_data_too_large_is_413() {
    let addr = spawn().await;
    // max_event_payload_bytes default = 10 240; craft a data string just over it.
    let big_data = "x".repeat(10_241);
    let body = json!({"name":"e","data": big_data,"channels":["c"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413, "oversized event data must be 413");
}

/// P7(a) batch: any item's `data` exceeding per-event cap → 413.
#[tokio::test]
async fn rest_batch_event_data_too_large_is_413() {
    let addr = spawn().await;
    let big_data = "x".repeat(10_241);
    let body = json!({"batch":[{"name":"e","data": big_data,"channel":"c"}]}).to_string();
    let q = signed_query("POST", "/apps/app1/batch_events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/batch_events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413, "oversized batch item data must be 413");
}

/// P7(b): GET /channels?info=user_count without a presence filter → 400.
#[tokio::test]
async fn rest_channels_user_count_without_presence_filter_is_400() {
    let addr = spawn().await;
    let q = signed_query("GET", "/apps/app1/channels", b"", &[("info", "user_count")]);
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app1/channels?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "user_count without presence filter must be 400"
    );
}

/// P7(b): GET /channels?info=user_count&filter_by_prefix=presence- → 200.
#[tokio::test]
async fn rest_channels_user_count_with_presence_filter_is_200() {
    let addr = spawn().await;
    let q = signed_query(
        "GET",
        "/apps/app1/channels",
        b"",
        &[("info", "user_count"), ("filter_by_prefix", "presence-")],
    );
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app1/channels?{q}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "user_count with presence filter must be 200"
    );
}

/// P7(c): GET /channels/{channel}/users on a non-presence channel → 400.
#[tokio::test]
async fn rest_users_on_non_presence_channel_is_400() {
    let addr = spawn().await;
    let q = signed_query("GET", "/apps/app1/channels/public-room/users", b"", &[]);
    let resp = reqwest::Client::new()
        .get(format!(
            "http://{addr}/apps/app1/channels/public-room/users?{q}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "users endpoint on non-presence channel must be 400"
    );
}

/// P7(c): GET /channels/{channel}/users on a presence- channel → 200.
#[tokio::test]
async fn rest_users_on_presence_channel_is_200() {
    let addr = spawn().await;
    // No members — but the channel name is valid so it must return 200 + empty list.
    let q = signed_query("GET", "/apps/app1/channels/presence-empty/users", b"", &[]);
    let resp = reqwest::Client::new()
        .get(format!(
            "http://{addr}/apps/app1/channels/presence-empty/users?{q}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "users endpoint on presence channel must be 200"
    );
}

// ── P8 parity tests — channel-name length + charset ─────────────────────────

/// P8: POST /events with a channel name exceeding 164 chars → 400.
#[tokio::test]
async fn rest_trigger_channel_name_over_length_is_400() {
    let addr = spawn().await;
    let long_name = "a".repeat(165);
    let body = json!({"name":"ev","data":"{}","channels":[long_name]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "channel name over 164 chars must be 400"
    );
}

/// P8: POST /events with a channel name containing an illegal char → 400.
#[tokio::test]
async fn rest_trigger_channel_name_bad_charset_is_400() {
    let addr = spawn().await;
    let body = json!({"name":"ev","data":"{}","channels":["bad channel!"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "channel name with illegal chars must be 400"
    );
}

/// P8: POST /events with a valid channel name → 200 (regression guard).
#[tokio::test]
async fn rest_trigger_valid_channel_name_is_200() {
    let addr = spawn().await;
    let body =
        json!({"name":"ev","data":"{}","channels":["my-valid_channel.name@here"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "valid channel name must still be 200");
}

#[tokio::test]
async fn rest_body_too_large_is_413() {
    let addr = spawn().await;
    // Default limits → body cap = 10*10240 + 64KiB ≈ 164KiB; exceed it. The
    // limit fires at body extraction, before the signature check runs.
    let big = "x".repeat(200 * 1024);
    let body = json!({"name": "e", "data": big, "channels": ["c"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413);
}

// ── P9 parity tests — event-name length (max 200 chars) ─────────────────────

/// P9: POST /events with an event name exceeding 200 chars → 400.
#[tokio::test]
async fn rest_trigger_event_name_over_200_is_400() {
    let addr = spawn().await;
    let long_name = "a".repeat(201);
    let body = json!({"name": long_name, "data": "{}", "channels": ["room"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "event name over 200 chars must be 400");
}

/// P9: POST /events with an event name of exactly 200 chars → 200.
#[tokio::test]
async fn rest_trigger_event_name_exactly_200_is_200() {
    let addr = spawn().await;
    let name_200 = "a".repeat(200);
    let body = json!({"name": name_200, "data": "{}", "channels": ["room"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "event name of exactly 200 chars must be 200"
    );
}

/// P9: POST /batch_events with an event name exceeding 200 chars → 400.
#[tokio::test]
async fn rest_batch_event_name_over_200_is_400() {
    let addr = spawn().await;
    let long_name = "a".repeat(201);
    let body = json!({"batch": [{"name": long_name, "data": "{}", "channel": "room"}]}).to_string();
    let q = signed_query("POST", "/apps/app1/batch_events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/batch_events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "batch event name over 200 chars must be 400"
    );
}

// ── R2 parity tests — REST errors render JSON bodies {error, status} ────────

/// Assert an error response carries the Pusher-style JSON error body: the body
/// parses as JSON, `error` is a non-empty string, `status` mirrors the HTTP
/// status code, and the content-type is `application/json`.
async fn assert_json_error(resp: reqwest::Response, expected: u16) {
    assert_eq!(resp.status(), expected);
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        content_type.starts_with("application/json"),
        "error body content-type must be application/json, got: {content_type}"
    );
    let v: Value = resp.json().await.unwrap();
    assert!(
        v.get("error")
            .and_then(Value::as_str)
            .is_some_and(|e| !e.is_empty()),
        "error body must carry a non-empty `error` string, got: {v}"
    );
    assert_eq!(
        v["status"],
        json!(expected),
        "error body `status` field must mirror the HTTP status, got: {v}"
    );
}

/// R2: 400 (invalid body) renders a JSON error body.
#[tokio::test]
async fn rest_error_body_400_is_json() {
    let addr = spawn().await;
    // Correctly signed request whose body is not valid JSON.
    let body = "definitely not json".to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_json_error(resp, 400).await;
}

/// R2: 401 (bad auth signature) renders a JSON error body.
#[tokio::test]
async fn rest_error_body_401_is_json() {
    let addr = spawn().await;
    let body = json!({"name":"e","data":"{}","channels":["c"]}).to_string();
    let mut q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    q = q.replace(
        &q[q.rfind("auth_signature=").unwrap()..],
        "auth_signature=deadbeef",
    );
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_json_error(resp, 401).await;
}

/// R2: handler-produced 404 (admin API disabled — the app-scoped 404 a handler
/// can emit; the unknown-route 404 shape is Task 2.9's scope) renders JSON.
#[tokio::test]
async fn rest_error_body_404_admin_disabled_is_json() {
    let addr = spawn().await;
    // No PYLON_ADMIN_TOKEN configured → the admin endpoint is disabled (404).
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/admin/apps/app1/invalidate"))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_json_error(resp, 404).await;
}

/// R2: 413 from the handler validation (event data over the per-event cap).
#[tokio::test]
async fn rest_error_body_413_event_data_is_json() {
    let addr = spawn().await;
    let big_data = "x".repeat(10_241);
    let body = json!({"name":"e","data": big_data,"channels":["c"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_json_error(resp, 413).await;
}

/// R2: 413 from the request body limit (axum `Bytes` rejection, mapped into
/// `RestError` so it renders the JSON body too).
#[tokio::test]
async fn rest_error_body_413_body_limit_is_json() {
    let addr = spawn().await;
    // Default limits → body cap = 10*10240 + 64KiB ≈ 164KiB; exceed it.
    let big = "x".repeat(200 * 1024);
    let body = json!({"name": "e", "data": big, "channels": ["c"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_json_error(resp, 413).await;
}

// ── R1 parity tests — disabled app: REST 403, unknown app: REST 401 ──────────

/// R1: an app with `"enabled": false` in apps.json must get **403 Forbidden**
/// (Pusher documents 403 for a disabled app) with the JSON error body — NOT the
/// 401 "invalid authentication" that a missing app gets.
#[tokio::test]
async fn rest_disabled_app_is_403() {
    let addr = spawn_with_apps(APPS_WITH_DISABLED).await;
    // A CORRECTLY signed request for the disabled app (right key + secret) —
    // the 403 comes from the app's disabled state, not a signature failure.
    let body = json!({"name":"e","data":"{}","channels":["c"]}).to_string();
    let q = signed_query_off("POST", "/apps/off-app/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/off-app/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_json_error(resp, 403).await;
}

/// R1: the 403 for a disabled app is not a blanket change — an UNKNOWN app id
/// keeps 401 "invalid authentication" (anti-enumeration: the server must not
/// reveal which app ids exist; disabled is the one documented distinction).
#[tokio::test]
async fn rest_unknown_app_id_is_still_401() {
    let addr = spawn().await;
    // Sign with app1's (valid) credentials over a path naming a nonexistent app.
    let body = json!({"name":"e","data":"{}","channels":["c"]}).to_string();
    let q = signed_query("POST", "/apps/no-such-app/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/no-such-app/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_json_error(resp, 401).await;
}

/// R1 regression pin: an ENABLED app keeps normal operation (200) — the
/// found/disabled/not-found split must not disturb the happy path.
#[tokio::test]
async fn rest_enabled_app_still_200() {
    let addr = spawn_with_apps(APPS_WITH_DISABLED).await;
    let body = json!({"name":"e","data":"{}","channels":["c"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

/// P13 (WS side): connecting with a DISABLED app's KEY closes with the doc's
/// dedicated close code — 4003 "Application disabled" (the Pusher protocol
/// doc's close-code table assigns 4003 to exactly this trigger; 4001 "Could
/// not find app by key" stays reserved for an unknown key). REST keeps 403.
#[tokio::test]
async fn ws_disabled_app_key_close_frame_carries_4003() {
    let addr = spawn_with_apps(APPS_WITH_DISABLED).await;
    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{addr}/app/off-key?protocol=7"))
            .await
            .unwrap();

    // The in-band pusher:error frame precedes the Close (queue_reject shape)
    // and carries the same code + the doc's message text.
    let mut error_frame: Option<Value> = None;
    let mut close_code: Option<u16> = None;
    while let Some(Ok(msg)) = ws.next().await {
        match msg {
            Message::Text(t) => {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["event"] == "pusher:error" {
                    error_frame = Some(v);
                }
            }
            Message::Close(frame) => {
                close_code = frame.map(|f| u16::from(f.code));
                break;
            }
            _ => {}
        }
    }
    let error_frame = error_frame.expect("pusher:error frame before Close");
    assert_eq!(error_frame["data"]["code"], 4003, "frame: {error_frame}");
    assert_eq!(
        error_frame["data"]["message"], "Application disabled",
        "message text must match the Pusher doc's 4003 row, frame: {error_frame}"
    );
    assert_eq!(
        close_code,
        Some(4003),
        "disabled-app-key reject must close with code 4003 (P13), got: {close_code:?}"
    );
}

// ── R3 parity tests — distinct 401 auth-failure messages ─────────────────────

/// Overwrite one `k=v` pair in a signed query string (string surgery on the
/// canonical `k=v&…` form `signed_query_as` produces), keeping the tail intact.
fn set_query_param(q: &str, key: &str, value: &str) -> String {
    let start = q.find(&format!("{key}=")).unwrap();
    let end = q[start..].find('&').map(|i| start + i).unwrap_or(q.len());
    format!("{}{}={}{}", &q[..start], key, value, &q[end..])
}

/// Drop one `k=v` pair (and its trailing `&` if present) from a signed query
/// string.
fn drop_query_param(q: &str, key: &str) -> String {
    let start = q.find(&format!("{key}=")).unwrap();
    let end = match q[start..].find('&') {
        Some(i) => start + i + 1, // drop through the separator
        None => q.len(),          // param is last: drop to the end
    };
    format!("{}{}", &q[..start], &q[end..])
}

/// POST the (mutated) signed query and return the parsed 401 error message.
async fn post_expect_401_message(addr: &str, path: &str, q: &str, body: &str) -> String {
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}{path}?{q}"))
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(
        v["status"],
        json!(401),
        "status field must mirror 401, got: {v}"
    );
    v["error"].as_str().unwrap().to_string()
}

/// R3: a stale `auth_timestamp` (outside the 600s window) gets the hosted
/// wording `Timestamp expired: …` — not the generic "invalid authentication".
#[tokio::test]
async fn rest_stale_timestamp_says_timestamp_expired() {
    let addr = spawn().await;
    let body = json!({"name":"e","data":"{}","channels":["c"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    // 1970 is far outside the 600s window; the window check fires before the
    // signature check, so the (now invalid) signature is irrelevant.
    let stale = set_query_param(&q, "auth_timestamp", "1");
    let msg = post_expect_401_message(&addr.to_string(), "/apps/app1/events", &stale, &body).await;
    assert!(
        msg.starts_with("Timestamp expired"),
        "stale timestamp must say `Timestamp expired: …`, got: {msg:?}"
    );
}

/// R3: a tampered `auth_signature` gets `Invalid signature: …`.
#[tokio::test]
async fn rest_bad_signature_says_invalid_signature() {
    let addr = spawn().await;
    let body = json!({"name":"e","data":"{}","channels":["c"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let tampered = set_query_param(&q, "auth_signature", "deadbeef");
    let msg =
        post_expect_401_message(&addr.to_string(), "/apps/app1/events", &tampered, &body).await;
    assert!(
        msg.starts_with("Invalid signature"),
        "bad signature must say `Invalid signature: …`, got: {msg:?}"
    );
}

/// R3: an unsupported `auth_version` gets `Invalid auth version`.
#[tokio::test]
async fn rest_bad_auth_version_says_invalid_auth_version() {
    let addr = spawn().await;
    let body = json!({"name":"e","data":"{}","channels":["c"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    // The version check fires first, so the (now invalid) signature is moot.
    let bad = set_query_param(&q, "auth_version", "2.0");
    let msg = post_expect_401_message(&addr.to_string(), "/apps/app1/events", &bad, &body).await;
    assert_eq!(
        msg, "Invalid auth version",
        "unsupported auth_version must say `Invalid auth version`, got: {msg:?}"
    );
}

/// R3: a missing required auth param (`auth_timestamp`) gets the specific
/// `Missing auth parameters`.
#[tokio::test]
async fn rest_missing_auth_param_says_missing_auth_parameters() {
    let addr = spawn().await;
    let body = json!({"name":"e","data":"{}","channels":["c"]}).to_string();
    let q = signed_query("POST", "/apps/app1/events", body.as_bytes(), &[]);
    let missing = drop_query_param(&q, "auth_timestamp");
    let msg =
        post_expect_401_message(&addr.to_string(), "/apps/app1/events", &missing, &body).await;
    assert_eq!(
        msg, "Missing auth parameters",
        "missing auth param must say `Missing auth parameters`, got: {msg:?}"
    );
}

/// R3 anti-enumeration pin: an UNKNOWN `auth_key` on an otherwise well-formed
/// request stays the GENERIC `invalid authentication` — the same string the
/// unknown-app path emits — so probing keys learns nothing beyond "not this
/// app's key" (keys/ids are public identifiers; the app secret is the secret).
#[tokio::test]
async fn rest_unknown_auth_key_stays_generic() {
    let addr = spawn().await;
    let body = json!({"name":"e","data":"{}","channels":["c"]}).to_string();
    // Correctly signed (over its own params) but carrying a key that is not
    // app1's: shape-valid, credentials wrong.
    let q = signed_query_as(
        "no-such-key",
        SECRET,
        "POST",
        "/apps/app1/events",
        body.as_bytes(),
        &[],
    );
    let msg = post_expect_401_message(&addr.to_string(), "/apps/app1/events", &q, &body).await;
    assert_eq!(
        msg, "invalid authentication",
        "unknown auth_key must stay the generic message, got: {msg:?}"
    );
}

// ── R15 carry-in — malformed query strings ────────────────────────────────────

/// R15: a malformed percent-escape (`%zz`) in the query string is parsed
/// LOSSILY by serde_urlencoded for `HashMap<String, String>` targets — it does
/// NOT reject (verified against serde_urlencoded 0.7: every input parses), so
/// the request flows into auth and, correctly signed including the odd param,
/// succeeds. Pin that the extractor wiring never turns it into a bare-text 400.
#[tokio::test]
async fn rest_malformed_query_percent_escape_is_parsed_lossily() {
    let addr = spawn().await;
    let body = json!({"name":"e","data":"{}","channels":["c"]}).to_string();
    let q = signed_query(
        "POST",
        "/apps/app1/events",
        body.as_bytes(),
        &[("x", "%zz")],
    );
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app1/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "a lossily-parsed percent escape must not break the request"
    );
}

// ── P13 parity tests — pre-handshake reject must carry Pusher 4xxx close code ─

/// Connecting to an unknown app key triggers a 4001 rejection.  The WebSocket Close
/// frame must carry code 4001 (not 1005 / no-status-received), so pusher-js
/// resolves `getCloseAction` → `"refused"` rather than `null → backoff`.
#[tokio::test]
async fn ws_unknown_app_key_close_frame_carries_4001() {
    use tokio_tungstenite::tungstenite::Message;
    let addr = spawn().await;
    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{addr}/app/no-such-key?protocol=7"))
            .await
            .unwrap();

    // Drain frames until we see a Close.
    let mut close_code: Option<u16> = None;
    while let Some(Ok(msg)) = ws.next().await {
        if let Message::Close(frame) = msg {
            close_code = frame.map(|f| u16::from(f.code));
            break;
        }
    }
    assert_eq!(
        close_code,
        Some(4001),
        "unknown-app-key reject must close with code 4001 (P13), got: {close_code:?}"
    );
}

// ── R10 parity tests — router-level 404 fallback ──────────────────────────────

/// R10: assert a router-level miss (no route matches the path) renders the
/// exact Pusher JSON error body — not axum's default EMPTY 404. The fallback
/// fires before any handler (and thus before auth), so the request carries no
/// signature at all.
async fn assert_fallback_404(resp: reqwest::Response) {
    assert_eq!(resp.status(), 404);
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert_eq!(
        content_type, "application/json",
        "router-level 404 must be application/json, got: {content_type}"
    );
    assert_eq!(
        resp.text().await.unwrap(),
        r#"{"error":"Not found","status":404}"#,
        "router-level 404 must carry the Pusher JSON error shape"
    );
}

/// R10: a trailing-slash variant of a real route (`/apps/{id}/events/`) matches
/// nothing (axum does not treat `/foo/` as `/foo`), so it hits the router
/// fallback — previously axum's default EMPTY 404, now the JSON error shape.
#[tokio::test]
async fn rest_trailing_slash_route_is_json_404() {
    let addr = spawn().await;
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/apps/app1/events/"))
        .send()
        .await
        .unwrap();
    assert_fallback_404(resp).await;
}

/// R10: a completely unknown path (`/nope`) hits the router fallback → JSON 404.
#[tokio::test]
async fn rest_unknown_route_is_json_404() {
    let addr = spawn().await;
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/nope"))
        .send()
        .await
        .unwrap();
    assert_fallback_404(resp).await;
}

/// A wrong method on a VALID path is a **405**, and — like every other REST
/// error (Task 2.2's class) — it renders the Pusher JSON shape. axum's
/// MethodRouter does NOT send method mismatches through the router fallback;
/// they are answered by `Router::method_not_allowed_fallback`, wired once in
/// `build_router` for all registered routes. Pusher's REST docs say nothing
/// about wrong-method bodies, so the `Method not allowed` wording is ours.
#[tokio::test]
async fn rest_wrong_method_on_valid_path_is_405() {
    let addr = spawn().await;
    let resp = reqwest::Client::new()
        .delete(format!("http://{addr}/apps/app1/channels/public-room"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        405,
        "a matched path with an unsupported method must be 405, not 404"
    );
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert_eq!(
        content_type, "application/json",
        "405 must be application/json, got: {content_type}"
    );
    assert_eq!(
        resp.text().await.unwrap(),
        r#"{"error":"Method not allowed","status":405}"#,
        "405 must carry the Pusher JSON error shape"
    );

    // The router-wide wiring must cover EVERY route plane, not just the REST
    // endpoints: a wrong method on a probe route 405s in the same JSON shape.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 405);
    assert_eq!(
        resp.text().await.unwrap(),
        r#"{"error":"Method not allowed","status":405}"#
    );
}
