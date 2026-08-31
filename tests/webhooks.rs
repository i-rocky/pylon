//! End-to-end webhook delivery: a live pylon server with a real `HttpTransport`,
//! a real WS subscribe that occupies a channel, and a local axum receiver that
//! captures the signed POST. Verifies the envelope shape AND the
//! `X-Pusher-Signature` exactly as pusher-http-node's WebHook validator would.
//!
//! The pylon spawn runs the percore worker fleet via `tests/common`'s
//! [`common::spawn`], but wires a REAL `webhook::spawn` dispatcher with a live
//! `HttpTransport` instead of the null sink — so the occupied/vacated webhook
//! fires end-to-end.

mod common;
use common::{spawn, SpawnSpec, Ws};

use futures_util::SinkExt;
use futures_util::StreamExt;
use pylon::adapter::local::LocalAdapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::auth::signature::hmac_sha256_hex;
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::webhook::dispatcher::SystemClock;
use pylon::webhook::transport::{HttpTransport, WebhookTransport};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

const SECRET: &str = "app-secret";
const KEY: &str = "app-key";

/// Shared receiver state: where to send captured POSTs, and the status to
/// answer every request with (use a non-2xx to exercise retries).
type ReceiverState = (
    Arc<mpsc::UnboundedSender<(String, String)>>,
    axum::http::StatusCode,
);

/// Spawn a local axum receiver that captures each POST body + signature header,
/// returning its address and a channel that yields `(raw_body, signature)` per
/// POST. Every response uses `status` — use a non-2xx to exercise retries.
async fn spawn_receiver_status(
    status: axum::http::StatusCode,
) -> (SocketAddr, mpsc::UnboundedReceiver<(String, String)>) {
    use axum::extract::State;
    use axum::http::HeaderMap;
    use axum::routing::post;
    use axum::Router;

    let (tx, rx) = mpsc::unbounded_channel::<(String, String)>();
    let state: ReceiverState = (Arc::new(tx), status);

    async fn handler(
        State((tx, status)): State<ReceiverState>,
        headers: HeaderMap,
        body: String,
    ) -> axum::http::StatusCode {
        let sig = headers
            .get("X-Pusher-Signature")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let _ = tx.send((body, sig));
        status
    }

    let app = Router::new()
        .route("/pusher/webhooks", post(handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, rx)
}

/// A receiver that always answers 200 (webhook successfully received).
async fn spawn_receiver() -> (SocketAddr, mpsc::UnboundedReceiver<(String, String)>) {
    spawn_receiver_status(axum::http::StatusCode::OK).await
}

/// Spawn the pylon server pointed at a webhook endpoint with a small batch
/// window and a small retry budget (failures resolve fast in tests).
async fn spawn_pylon(receiver: SocketAddr) -> SocketAddr {
    spawn_pylon_with(receiver, 50, 100, 250).await
}

/// Like [`spawn_pylon`] but with explicit webhook retry knobs (backoff base ms,
/// backoff cap ms, total retry budget ms).
async fn spawn_pylon_with(
    receiver: SocketAddr,
    backoff_base_ms: u64,
    backoff_cap_ms: u64,
    retry_budget_ms: u64,
) -> SocketAddr {
    let apps_json = format!(
        r#"[
            {{"name":"Test","id":"app","key":"{KEY}","secret":"{SECRET}",
              "client_messages_enabled":true,
              "webhooks":[{{"url":"http://{receiver}/pusher/webhooks",
                            "event_types":["channel_occupied","channel_vacated"]}}]}}
        ]"#
    );
    spawn_pylon_apps(&apps_json, backoff_base_ms, backoff_cap_ms, retry_budget_ms).await
}

/// Like [`spawn_pylon_with`] but with the caller's raw `apps.json` (extra per-app
/// flags like `subscription_count_enabled` and arbitrary `event_types`).
async fn spawn_pylon_apps(
    apps_json: &str,
    backoff_base_ms: u64,
    backoff_cap_ms: u64,
    retry_budget_ms: u64,
) -> SocketAddr {
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(apps_json).unwrap());
    let local = Arc::new(LocalAdapter::new(
        Arc::new(Registry::new()),
        Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
    ));
    let webhooks = pylon::webhook::spawn(
        apps.clone(),
        move |metrics| {
            // These delivery tests target the loopback mock receiver, so they
            // opt into the SSRF guard's escape hatch; the guard itself is
            // exercised end-to-end by the ssrf_* tests below.
            HttpTransport::new(
                backoff_base_ms,
                backoff_cap_ms,
                retry_budget_ms,
                5000,
                100,
                true,
                metrics,
            )
            .map(|t| Arc::new(t) as Arc<dyn WebhookTransport>)
        },
        Arc::new(SystemClock),
        30, // 30ms batch window
        1024,
        0,    // local path: vacated fires immediately (no grace)
        None, // no cluster occupancy source
    )
    .expect("webhook transport builds in tests");
    let config = ServerConfig {
        webhook_batch_ms: 30,
        ..ServerConfig::default()
    };
    // Route through the transport-parameterized harness with the REAL webhook
    // dispatcher (not the null sink) and the concrete local adapter (so the
    // percore sharded sink installs on it).
    spawn(SpawnSpec {
        config,
        apps,
        local,
        conn_counts: Arc::new(Default::default()),
        webhooks,
    })
    .await
}

async fn connect(addr: SocketAddr) -> Ws {
    let url = format!("ws://{addr}/app/{KEY}?protocol=7");
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .expect("connect within 5s")
    .expect("ws handshake");
    ws
}

async fn next_json(ws: &mut Ws) -> Value {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return serde_json::from_str(&t).unwrap(),
            Ok(Some(Ok(_))) => continue,
            other => panic!("unexpected ws frame: {other:?}"),
        }
    }
}

#[tokio::test]
async fn occupied_webhook_is_posted_and_signature_validates() {
    let (receiver_addr, mut rx) = spawn_receiver().await;
    let pylon_addr = spawn_pylon(receiver_addr).await;

    let mut ws = connect(pylon_addr).await;
    // drain connection_established
    let est = next_json(&mut ws).await;
    assert_eq!(est["event"], "pusher:connection_established");

    // Subscribe to a public channel → 0→1 → channel_occupied webhook.
    ws.send(Message::Text(
        json!({ "event": "pusher:subscribe", "data": { "channel": "my-channel" } }).to_string(),
    ))
    .await
    .unwrap();
    let ack = next_json(&mut ws).await;
    assert_eq!(ack["event"], "pusher_internal:subscription_succeeded");

    // The receiver must get one signed POST within the window + delivery time.
    let (body, signature) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("webhook POST arrived")
        .expect("channel open");

    // Envelope shape.
    let env: Value = serde_json::from_str(&body).unwrap();
    assert!(env["time_ms"].is_u64());
    let events = env["events"].as_array().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["name"], "channel_occupied");
    assert_eq!(events[0]["channel"], "my-channel");

    // Signature validates exactly the way pusher-http-node's WebHook does:
    // hex(HMAC_SHA256(secret, raw_body)) == X-Pusher-Signature.
    assert_eq!(signature, hmac_sha256_hex(SECRET, &body));
}

/// R4 end-to-end (Pusher parity): "If a non 2XX status code is returned,
/// Channels will retry sending the webhook, with exponential backoff, for 5
/// minutes." A receiver that always answers 404 must be hit more than once.
/// Uses a tiny backoff/budget so the test stays fast; the retry POLICY (any
/// non-2xx retried) is what is under test here, not the 5-minute budget
/// (pinned by the paused-time transport unit tests).
#[tokio::test]
async fn non_2xx_receiver_is_retried() {
    let (receiver_addr, mut rx) = spawn_receiver_status(axum::http::StatusCode::NOT_FOUND).await;
    // base 20ms / cap 40ms / budget 400ms → ~10 attempts inside the budget.
    let pylon_addr = spawn_pylon_with(receiver_addr, 20, 40, 400).await;

    let mut ws = connect(pylon_addr).await;
    let est = next_json(&mut ws).await;
    assert_eq!(est["event"], "pusher:connection_established");

    // Subscribe → channel_occupied webhook fires against the 404 receiver.
    ws.send(Message::Text(
        json!({ "event": "pusher:subscribe", "data": { "channel": "retry-room" } }).to_string(),
    ))
    .await
    .unwrap();
    let ack = next_json(&mut ws).await;
    assert_eq!(ack["event"], "pusher_internal:subscription_succeeded");

    // The first POST plus at least one RETRY must arrive (each fully signed).
    let (body1, sig1) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("first webhook POST arrived")
        .expect("channel open");
    let (body2, sig2) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("retry POST arrived (non-2xx must be retried)")
        .expect("channel open");
    assert_eq!(sig1, hmac_sha256_hex(SECRET, &body1));
    assert_eq!(sig2, hmac_sha256_hex(SECRET, &body2));
    // Same signed envelope retried verbatim.
    assert_eq!(body1, body2);
}

/// Parse a single `metric_name value` series out of a Prometheus text body,
/// returning the parsed `u64` value (or `None` if the line is absent).
fn metric_value(body: &str, line_prefix: &str) -> Option<u64> {
    body.lines()
        .find(|l| l.starts_with(line_prefix))
        .and_then(|l| l.split_whitespace().last())
        .and_then(|v| v.parse().ok())
}

/// End-to-end: driving a webhook (subscribe → `channel_occupied`) must move the
/// `pylon_webhook_*` counters in `GET /metrics`. The receiver returns 2xx so the
/// delivery resolves `ok`. Polls (webhooks are async: batch window + spawned
/// delivery task) rather than assuming a fixed sleep.
#[tokio::test]
async fn metrics_reflect_a_driven_webhook() {
    let (receiver_addr, mut rx) = spawn_receiver().await;
    let pylon_addr = spawn_pylon(receiver_addr).await;

    let mut ws = connect(pylon_addr).await;
    let est = next_json(&mut ws).await;
    assert_eq!(est["event"], "pusher:connection_established");

    // Subscribe to a public channel → 0→1 → channel_occupied webhook fires.
    ws.send(Message::Text(
        json!({ "event": "pusher:subscribe", "data": { "channel": "metrics-room" } }).to_string(),
    ))
    .await
    .unwrap();
    let ack = next_json(&mut ws).await;
    assert_eq!(ack["event"], "pusher_internal:subscription_succeeded");

    // The delivery must actually land (the receiver returns 200) so the spawned
    // transport task bumps delivered_ok before we scrape.
    let _ = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("webhook POST arrived")
        .expect("channel open");

    // Poll /metrics until both the enqueued and the delivered{ok} counters reflect
    // the driven webhook (bounded so a real regression fails fast).
    let client = reqwest::Client::new();
    let mut enqueued = 0u64;
    let mut delivered_ok = 0u64;
    for _ in 0..50 {
        let body = client
            .get(format!("http://{pylon_addr}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        enqueued = metric_value(&body, "pylon_webhook_enqueued_total").unwrap_or(0);
        delivered_ok =
            metric_value(&body, r#"pylon_webhook_delivered_total{status="ok"}"#).unwrap_or(0);
        if enqueued >= 1 && delivered_ok >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        enqueued >= 1,
        "pylon_webhook_enqueued_total must be >= 1 after a driven webhook, got {enqueued}"
    );
    assert!(
        delivered_ok >= 1,
        "pylon_webhook_delivered_total{{status=\"ok\"}} must be >= 1 after a 2xx delivery, got {delivered_ok}"
    );
}

/// Collect every event object with `name == want` from the POSTs the receiver
/// has captured so far, oldest first (POST order + in-envelope order).
fn events_named(captured: &[(String, String)], want: &str) -> Vec<Value> {
    let mut out = Vec::new();
    for (body, _) in captured {
        let env: Value = serde_json::from_str(body).unwrap();
        if let Some(events) = env["events"].as_array() {
            for e in events {
                if e["name"].as_str() == Some(want) {
                    out.push(e.clone());
                }
            }
        }
    }
    out
}

/// Task 2.5 / audit R6 (verified against
/// https://pusher.com/docs/channels/server_api/webhooks/, 2026-08-30: "Channels
/// will send a subscription_count webhook whenever a new client subscribes or
/// unsubscribes to a channel", payload `{name, channel, subscription_count}`).
/// An app with BOTH `subscription_count_enabled: true` (the App-Settings
/// feature toggle the doc requires) AND the `subscription_count` webhook event
/// type must see, in order: subscribe → count 1, second subscribe → count 2,
/// unsubscribe → count 1. No zero-count event on the final unsubscribe is
/// pinned here too (mirrors the cluster path's `count > 0` broadcast guard —
/// `channel_vacated` is the vacancy signal).
#[tokio::test]
async fn subscription_count_webhook_carries_counts_across_sub_and_unsub() {
    let (receiver_addr, mut rx) = spawn_receiver().await;
    let apps_json = format!(
        r#"[
            {{"name":"Test","id":"app","key":"{KEY}","secret":"{SECRET}",
              "client_messages_enabled":true,
              "subscription_count_enabled":true,
              "webhooks":[{{"url":"http://{receiver_addr}/pusher/webhooks",
                            "event_types":["subscription_count"]}}]}}
        ]"#
    );
    let pylon_addr = spawn_pylon_apps(&apps_json, 50, 100, 250).await;

    let mut ws1 = connect(pylon_addr).await;
    assert_eq!(
        next_json(&mut ws1).await["event"],
        "pusher:connection_established"
    );
    let mut ws2 = connect(pylon_addr).await;
    assert_eq!(
        next_json(&mut ws2).await["event"],
        "pusher:connection_established"
    );

    for ws in [&mut ws1, &mut ws2] {
        ws.send(Message::Text(
            json!({ "event": "pusher:subscribe", "data": { "channel": "count-room" } }).to_string(),
        ))
        .await
        .unwrap();
    }
    for ws in [&mut ws1, &mut ws2] {
        assert_eq!(
            next_json(ws).await["event"],
            "pusher_internal:subscription_succeeded"
        );
    }
    // Unsubscribe ws2 only → the count goes 2 → 1 (ws1 stays subscribed).
    ws2.send(Message::Text(
        json!({ "event": "pusher:unsubscribe", "data": { "channel": "count-room" } }).to_string(),
    ))
    .await
    .unwrap();

    // Collect POSTs until three subscription_count events have landed (bounded;
    // the 30ms batch window may split or merge them, order is preserved).
    let mut captured: Vec<(String, String)> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while events_named(&captured, "subscription_count").len() < 3 {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(pair)) => captured.push(pair),
            other => panic!(
                "expected 3 subscription_count webhooks, got {}: {other:?}",
                events_named(&captured, "subscription_count").len()
            ),
        }
    }
    // Drain anything still inside the batch window so the "no more" assertions
    // below see the full picture.
    while let Ok(Some(pair)) = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
        captured.push(pair);
    }

    let events = events_named(&captured, "subscription_count");
    let counts: Vec<u64> = events
        .iter()
        .map(|e| {
            e["subscription_count"]
                .as_u64()
                .expect("count must be a JSON number")
        })
        .collect();
    assert_eq!(
        counts,
        vec![1, 2, 1],
        "counts must track subscribe/sub/unsub"
    );
    for e in &events {
        assert_eq!(e["channel"], "count-room");
        assert_eq!(e["name"], "subscription_count");
        assert!(e.get("user_id").is_none(), "no extra fields in the payload");
    }
    assert_eq!(
        events_named(&captured, "channel_occupied").len()
            + events_named(&captured, "channel_vacated").len(),
        0,
        "endpoint subscribes to subscription_count only — no other event types"
    );
    assert!(
        !counts.contains(&0),
        "no zero-count webhook on vacate (count > 0 guard; channel_vacated is the vacancy signal)"
    );

    // ws1 is still subscribed; closing it vacates the channel — still no count-0
    // subscription_count webhook.
    ws1.send(Message::Close(None)).await.unwrap();
    while let Ok(Some(pair)) = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
        captured.push(pair);
    }
    let counts_after: Vec<u64> = events_named(&captured, "subscription_count")
        .iter()
        .map(|e| e["subscription_count"].as_u64().unwrap())
        .collect();
    assert_eq!(
        counts_after,
        vec![1, 2, 1],
        "vacate must not emit a count-0 event"
    );
}

/// Negative pin 1: an app WITHOUT `subscription_count` in its webhook
/// `event_types` must never receive the event, even with
/// `subscription_count_enabled: true` (the two toggles are independent: the
/// feature flag enables the count machinery, the event_types entry routes the
/// webhook).
#[tokio::test]
async fn subscription_count_webhook_absent_without_event_type() {
    let (receiver_addr, mut rx) = spawn_receiver().await;
    let apps_json = format!(
        r#"[
            {{"name":"Test","id":"app","key":"{KEY}","secret":"{SECRET}",
              "client_messages_enabled":true,
              "subscription_count_enabled":true,
              "webhooks":[{{"url":"http://{receiver_addr}/pusher/webhooks",
                            "event_types":["channel_occupied","channel_vacated"]}}]}}
        ]"#
    );
    let pylon_addr = spawn_pylon_apps(&apps_json, 50, 100, 250).await;

    let mut ws1 = connect(pylon_addr).await;
    assert_eq!(
        next_json(&mut ws1).await["event"],
        "pusher:connection_established"
    );
    let mut ws2 = connect(pylon_addr).await;
    assert_eq!(
        next_json(&mut ws2).await["event"],
        "pusher:connection_established"
    );

    for ws in [&mut ws1, &mut ws2] {
        ws.send(Message::Text(
            json!({ "event": "pusher:subscribe", "data": { "channel": "neg-room" } }).to_string(),
        ))
        .await
        .unwrap();
    }
    for ws in [&mut ws1, &mut ws2] {
        assert_eq!(
            next_json(ws).await["event"],
            "pusher_internal:subscription_succeeded"
        );
    }
    ws2.send(Message::Text(
        json!({ "event": "pusher:unsubscribe", "data": { "channel": "neg-room" } }).to_string(),
    ))
    .await
    .unwrap();

    // The pipeline is provably live once the occupied webhook POST arrives.
    let mut captured: Vec<(String, String)> = Vec::new();
    loop {
        let pair = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("channel_occupied webhook POST arrived")
            .expect("channel open");
        captured.push(pair);
        if !events_named(&captured, "channel_occupied").is_empty() {
            break;
        }
    }
    // Exposure window well beyond the 30ms batch window (+ unsubscribe edge).
    while let Ok(Some(pair)) = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
        captured.push(pair);
    }
    assert_eq!(
        events_named(&captured, "subscription_count").len(),
        0,
        "no subscription_count webhook without the event_types entry"
    );
}

/// Negative pin 2: the webhook doc gates the event on the App-Settings
/// Subscription Count feature toggle ("navigate to the Channels dashboard for
/// your app > App Settings and switch the toggle on") — Pylon's
/// `subscription_count_enabled`. With the flag off, listing
/// `subscription_count` in `event_types` must NOT produce events.
#[tokio::test]
async fn subscription_count_webhook_absent_without_feature_toggle() {
    let (receiver_addr, mut rx) = spawn_receiver().await;
    let apps_json = format!(
        r#"[
            {{"name":"Test","id":"app","key":"{KEY}","secret":"{SECRET}",
              "client_messages_enabled":true,
              "subscription_count_enabled":false,
              "webhooks":[{{"url":"http://{receiver_addr}/pusher/webhooks",
                            "event_types":["subscription_count","channel_occupied"]}}]}}
        ]"#
    );
    let pylon_addr = spawn_pylon_apps(&apps_json, 50, 100, 250).await;

    let mut ws1 = connect(pylon_addr).await;
    assert_eq!(
        next_json(&mut ws1).await["event"],
        "pusher:connection_established"
    );
    let mut ws2 = connect(pylon_addr).await;
    assert_eq!(
        next_json(&mut ws2).await["event"],
        "pusher:connection_established"
    );

    for ws in [&mut ws1, &mut ws2] {
        ws.send(Message::Text(
            json!({ "event": "pusher:subscribe", "data": { "channel": "neg2-room" } }).to_string(),
        ))
        .await
        .unwrap();
    }
    for ws in [&mut ws1, &mut ws2] {
        assert_eq!(
            next_json(ws).await["event"],
            "pusher_internal:subscription_succeeded"
        );
    }
    ws2.send(Message::Text(
        json!({ "event": "pusher:unsubscribe", "data": { "channel": "neg2-room" } }).to_string(),
    ))
    .await
    .unwrap();

    // Pipeline live: the occupied POST arrives.
    let mut captured: Vec<(String, String)> = Vec::new();
    loop {
        let pair = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("channel_occupied webhook POST arrived")
            .expect("channel open");
        captured.push(pair);
        if !events_named(&captured, "channel_occupied").is_empty() {
            break;
        }
    }
    while let Ok(Some(pair)) = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
        captured.push(pair);
    }
    assert_eq!(
        events_named(&captured, "subscription_count").len(),
        0,
        "no subscription_count webhook with subscription_count_enabled = false (doc: the App Settings feature toggle gates the event)"
    );
}

// ── S2: webhook target SSRF guard ─────────────────────────────────────────────

use pylon::webhook::transport::Resolver;
use pylon::webhook::WebhookMetrics;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};

/// Mock resolver for the SSRF tests: every host resolves to the canned list.
struct FixedResolver {
    ips: Vec<IpAddr>,
}

#[async_trait::async_trait]
impl Resolver for FixedResolver {
    async fn resolve(&self, _host: &str) -> Vec<IpAddr> {
        self.ips.clone()
    }
}

fn ssrf_metrics() -> Arc<WebhookMetrics> {
    Arc::new(WebhookMetrics::new(64))
}

/// Poll (bounded, real time) until `f` is true.
async fn until(timeout: Duration, mut f: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    f()
}

fn signed_delivery_to(url: &str) -> pylon::webhook::transport::WebhookDelivery {
    pylon::webhook::transport::build_signed_delivery(
        url,
        KEY,
        SECRET,
        1,
        &[json!({ "name": "channel_occupied", "channel": "ssrf-room" })],
        &std::collections::BTreeMap::new(),
    )
}

/// S2 case 1: the resolver returns a private address (10.0.0.5) → the delivery
/// is REFUSED. Observable end-to-end: `delivered_failed` reaches 1 in well
/// under the 60-second retry budget (a retrying delivery cannot have given up
/// by then), and nothing was delivered.
#[tokio::test]
async fn ssrf_private_resolution_refuses_delivery() {
    let metrics = ssrf_metrics();
    let t = HttpTransport::with_resolver(
        Arc::new(FixedResolver {
            ips: vec!["10.0.0.5".parse().unwrap()],
        }),
        false, // guard armed
        1000,
        60_000,
        60_000, // budget 60s: refusal must NOT wait for this
        5_000,
        10,
        metrics.clone(),
    )
    .expect("client builds");
    let started = std::time::Instant::now();
    t.deliver(signed_delivery_to("https://internal.example.test/wh"))
        .await;

    let refused = until(Duration::from_secs(3), || {
        metrics.delivered_failed.load(Ordering::Relaxed) >= 1
    })
    .await;
    assert!(refused, "delivery must be refused quickly");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "refusal is fast-fail (config error), not budget-bounded; took {:?}",
        started.elapsed()
    );
    assert_eq!(metrics.delivered_ok.load(Ordering::Relaxed), 0);
}

/// S2 case 2: the resolver returns a PUBLIC address → the delivery is attempted
/// (not refused). The pinned address (93.184.216.34) is unroutable from the
/// test host, so the delivery cannot succeed — the observable distinction is
/// WHEN it fails: an attempted delivery rides the retry loop and records its
/// failure only once the small budget (400 ms) has elapsed, whereas a refusal
/// is instant. The delivered-and-signed path over a public resolution is
/// pinned by the transport unit tests (public → 2xx → delivered_ok).
#[tokio::test]
async fn ssrf_public_resolution_is_attempted_not_refused() {
    let metrics = ssrf_metrics();
    let t = HttpTransport::with_resolver(
        Arc::new(FixedResolver {
            ips: vec!["93.184.216.34".parse().unwrap()],
        }),
        false,
        50,
        100,
        400, // small budget
        300, // per-attempt timeout
        10,
        metrics.clone(),
    )
    .expect("client builds");
    let started = std::time::Instant::now();
    t.deliver(signed_delivery_to("https://public.example.test/wh"))
        .await;

    let failed = until(Duration::from_secs(5), || {
        metrics.delivered_failed.load(Ordering::Relaxed) >= 1
    })
    .await;
    assert!(failed, "unroutable public target exhausts the budget");
    assert!(
        started.elapsed() >= Duration::from_millis(400),
        "failure must come from the retry budget (attempted), not refusal; took {:?}",
        started.elapsed()
    );
    assert_eq!(metrics.delivered_ok.load(Ordering::Relaxed), 0);
}

/// S2 case 3: allow-flag TRUE → a private (loopback) target IS delivered, and
/// the POST arrives fully signed at the receiver behind the mocked hostname.
#[tokio::test]
async fn ssrf_allow_flag_lets_private_target_deliver() {
    let (receiver_addr, mut rx) = spawn_receiver().await;
    // The URL uses a HOSTNAME; the resolver pins it to the receiver's real
    // loopback address — delivery must reach the receiver through the pin.
    let metrics = ssrf_metrics();
    let t = HttpTransport::with_resolver(
        Arc::new(FixedResolver {
            ips: vec![receiver_addr.ip()],
        }),
        true, // PYLON_WEBHOOK_ALLOW_PRIVATE_TARGETS=1 equivalent
        1000,
        60_000,
        60_000,
        5_000,
        10,
        metrics.clone(),
    )
    .expect("client builds");
    t.deliver(signed_delivery_to(&format!(
        "http://receiver.example.test:{port}/pusher/webhooks",
        port = receiver_addr.port()
    )))
    .await;

    let (body, signature) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("webhook POST arrived through the pin")
        .expect("channel open");
    assert_eq!(signature, hmac_sha256_hex(SECRET, &body));
    let ok = until(Duration::from_secs(3), || {
        metrics.delivered_ok.load(Ordering::Relaxed) >= 1
    })
    .await;
    assert!(ok, "delivered_ok must be recorded");
}

/// S2 case 4: a `file://` URL is refused regardless of the allow flag (the
/// allow flag only widens the ADDRESS policy, never the scheme policy).
#[tokio::test]
async fn ssrf_file_scheme_refused_even_with_allow_flag() {
    let metrics = ssrf_metrics();
    let t = HttpTransport::with_resolver(
        Arc::new(FixedResolver {
            ips: vec!["127.0.0.1".parse().unwrap()],
        }),
        true,
        1000,
        60_000,
        60_000,
        5_000,
        10,
        metrics.clone(),
    )
    .expect("client builds");
    let started = std::time::Instant::now();
    t.deliver(signed_delivery_to("file:///etc/passwd")).await;

    let refused = until(Duration::from_secs(3), || {
        metrics.delivered_failed.load(Ordering::Relaxed) >= 1
    })
    .await;
    assert!(refused, "file:// must be refused fast");
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(metrics.delivered_ok.load(Ordering::Relaxed), 0);
}

/// Redirect bypass closure — shared trial harness (fix-round 1): a
/// webhook endpoint answering `status` with `Location: http://<honeypot>/…`
/// must NOT have its redirect followed — otherwise an attacker-controlled
/// redirect to a metadata/loopback address sidesteps the SSRF pre-flight
/// pinning entirely.
///
/// The honeypot is registered with `routing::any` (fix-round 1, Important 1):
/// a followed 302 is rewritten POST→GET, and a GET against a POST-only route
/// answers 405 WITHOUT invoking the handler — so a POST-only honeypot would
/// stay at 0 hits even with redirect-following enabled and prove nothing.
/// `any` counts every method shape: GET (302/303 rewrite), POST (307/308
/// method-and-body preservation).
async fn redirect_honeypot_trial(
    status: axum::http::StatusCode,
) -> (Arc<AtomicU64>, Arc<AtomicU64>, Arc<WebhookMetrics>) {
    // Honeypot: ANY hit proves a redirect was followed.
    let honeypot_hits = Arc::new(AtomicU64::new(0));
    let hits = honeypot_hits.clone();
    let honeypot = {
        use axum::extract::State;
        async fn handler(State(hits): State<Arc<AtomicU64>>) -> axum::http::StatusCode {
            hits.fetch_add(1, Ordering::SeqCst);
            axum::http::StatusCode::OK
        }
        let app = axum::Router::new()
            .route("/honeypot", axum::routing::any(handler))
            .with_state(hits);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        addr
    };

    // The "public" endpoint: every POST answers `status` → the honeypot (a
    // private address). Its hit counter proves the FIRST hop happened.
    let first_hits = Arc::new(AtomicU64::new(0));
    let redirect_app = {
        use axum::extract::State;
        // Hand-rolled responder so we can attach the Location header.
        async fn handler(
            State((first_hits, honeypot, status)): State<(
                Arc<AtomicU64>,
                SocketAddr,
                axum::http::StatusCode,
            )>,
        ) -> axum::response::Response {
            first_hits.fetch_add(1, Ordering::SeqCst);
            let loc = format!("http://{honeypot}/honeypot");
            let mut resp = axum::response::Response::new(axum::body::Body::empty());
            *resp.status_mut() = status;
            resp.headers_mut().insert("location", loc.parse().unwrap());
            resp
        }
        let app = axum::Router::new()
            .route("/pusher/webhooks", axum::routing::post(handler))
            .with_state((first_hits.clone(), honeypot, status));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        addr
    };

    let metrics = ssrf_metrics();
    let t = HttpTransport::with_resolver(
        Arc::new(FixedResolver {
            ips: vec![redirect_app.ip()],
        }),
        true, // allow private: the FIRST hop is loopback by test necessity
        50,
        100,
        600, // small budget: retried redirects give up fast
        5_000,
        10,
        metrics.clone(),
    )
    .expect("client builds");
    t.deliver(signed_delivery_to(&format!(
        "http://redirector.example.test:{port}/pusher/webhooks",
        port = redirect_app.port()
    )))
    .await;

    let failed = until(Duration::from_secs(5), || {
        metrics.delivered_failed.load(Ordering::Relaxed) >= 1
    })
    .await;
    assert!(
        failed,
        "the {status} (non-2xx) must exhaust the retry budget"
    );
    assert!(
        first_hits.load(Ordering::SeqCst) >= 1,
        "the first hop must have been attempted (and retried)"
    );
    (first_hits, honeypot_hits, metrics)
}

/// 302: the classic rewrite form — a followed 302 drops the method to GET,
/// so a route+handler pair that counts GETs too (any-method honeypot) is what
/// makes this assertion bite. The redirect target must NEVER be contacted.
#[tokio::test]
async fn ssrf_redirect_302_to_private_target_is_not_followed() {
    let (first_hits, honeypot_hits, metrics) =
        redirect_honeypot_trial(axum::http::StatusCode::FOUND).await;
    assert_eq!(
        honeypot_hits.load(Ordering::SeqCst),
        0,
        "the 302 redirect target must NEVER be contacted (Policy::none)"
    );
    assert_eq!(metrics.delivered_ok.load(Ordering::Relaxed), 0);
    assert!(first_hits.load(Ordering::SeqCst) >= 1);
}

/// 307/308: the method-and-body-PRESERVING redirect — the more dangerous
/// follow form, because the honeypot would receive the exact signed POST
/// (body intact) if the client followed it. Both variants must be refused
/// the same way: the 3xx is returned to the retry loop as-is.
#[tokio::test]
async fn ssrf_redirect_307_308_to_private_target_is_not_followed() {
    for status in [
        axum::http::StatusCode::TEMPORARY_REDIRECT,
        axum::http::StatusCode::PERMANENT_REDIRECT,
    ] {
        let (first_hits, honeypot_hits, metrics) = redirect_honeypot_trial(status).await;
        assert_eq!(
            honeypot_hits.load(Ordering::SeqCst),
            0,
            "the {status} redirect target must NEVER be contacted — the signed \
             POST must not be replayed to it (Policy::none)"
        );
        assert_eq!(metrics.delivered_ok.load(Ordering::Relaxed), 0);
        assert!(first_hits.load(Ordering::SeqCst) >= 1);
    }
}
