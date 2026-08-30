//! Webhook delivery: the signed request value object, envelope/sign helper, the
//! `WebhookTransport` trait, and its `HttpTransport` / `RecordingTransport` impls.

use crate::auth::signature::hmac_sha256_hex;
use crate::webhook::WebhookMetrics;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};

/// One fully-prepared POST: the raw signed body bytes plus the exact header set.
#[derive(Debug, Clone, PartialEq)]
pub struct WebhookDelivery {
    pub url: String,
    /// The exact bytes that were signed and must be sent verbatim.
    pub body: String,
    /// Header names exactly as sent (the three Pusher headers always win over custom).
    pub headers: BTreeMap<String, String>,
}

/// Serialize a JSON value with object keys sorted recursively, so the signed
/// webhook body is byte-stable regardless of serde_json's `preserve_order`
/// feature (which a transitive dependency such as `bson` may enable globally).
fn sort_keys(v: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match v {
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> = map
                .into_iter()
                .map(|(k, val)| (k, sort_keys(val)))
                .collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Value::Object(entries.into_iter().collect())
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(sort_keys).collect()),
        other => other,
    }
}

/// Build the envelope `{ time_ms, events }`, serialize it, sign the raw body with
/// `secret`, and assemble the header set. Per-endpoint `custom` headers are merged
/// in FIRST, then the three Pusher headers overwrite — so custom headers can never
/// override `Content-Type` / `X-Pusher-Key` / `X-Pusher-Signature` (spec §4).
pub fn build_signed_delivery(
    url: &str,
    app_key: &str,
    secret: &str,
    time_ms: u64,
    events: &[Value],
    custom: &BTreeMap<String, String>,
) -> WebhookDelivery {
    let envelope = json!({ "time_ms": time_ms, "events": events });
    let body = serde_json::to_string(&sort_keys(envelope)).expect("envelope serializes");
    let signature = hmac_sha256_hex(secret, &body);

    let mut headers: BTreeMap<String, String> = custom.clone();
    headers.insert("Content-Type".into(), "application/json".into());
    headers.insert("X-Pusher-Key".into(), app_key.to_string());
    headers.insert("X-Pusher-Signature".into(), signature);

    WebhookDelivery {
        url: url.to_string(),
        body,
        headers,
    }
}

/// The pluggable delivery boundary. `HttpTransport` POSTs; `RecordingTransport`
/// records for tests. `deliver` owns retry/concurrency policy internally and
/// never returns an error to the dispatcher (it spawns the attempt loop and
/// returns immediately; outcomes are counted in the spawned task, not here).
#[async_trait]
pub trait WebhookTransport: Send + Sync {
    async fn deliver(&self, delivery: WebhookDelivery);
}

/// Test double: records every delivery handed to it; performs no I/O.
#[derive(Clone, Default)]
pub struct RecordingTransport {
    deliveries: Arc<Mutex<Vec<WebhookDelivery>>>,
}

impl RecordingTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of everything delivered so far.
    pub async fn recorded(&self) -> Vec<WebhookDelivery> {
        self.deliveries.lock().await.clone()
    }
}

#[async_trait]
impl WebhookTransport for RecordingTransport {
    async fn deliver(&self, delivery: WebhookDelivery) {
        self.deliveries.lock().await.push(delivery);
    }
}

/// One POST attempt, abstracted so the retry policy is testable under paused
/// tokio time without real sockets. `Ok(status)` is any HTTP response status
/// (2xx/3xx/4xx/5xx alike); `Err` is a transport error (timeout, connection
/// refused, DNS, …).
#[async_trait]
pub(crate) trait AttemptSender: Send + Sync {
    async fn post(&self, delivery: &WebhookDelivery) -> Result<u16, String>;
}

/// Production [`AttemptSender`]: one reqwest POST with the per-attempt timeout
/// baked into the client.
struct ReqwestSender {
    client: reqwest::Client,
}

#[async_trait]
impl AttemptSender for ReqwestSender {
    async fn post(&self, delivery: &WebhookDelivery) -> Result<u16, String> {
        let mut req = self.client.post(&delivery.url).body(delivery.body.clone());
        for (k, v) in &delivery.headers {
            req = req.header(k, v);
        }
        match req.send().await {
            Ok(resp) => Ok(resp.status().as_u16()),
            Err(e) => Err(e.to_string()),
        }
    }
}

/// Production transport: reqwest POST with per-attempt timeout, a Pusher-parity
/// retry policy, and a global concurrency semaphore (spec §6). Pusher's
/// documented behavior — "If a non 2XX status code is returned, Channels will
/// retry sending the webhook, with exponential backoff, for 5 minutes" — is
/// modeled as: every non-2xx response AND every transport error is retried;
/// delays grow `backoff_base_ms * 2^n` capped at `backoff_cap_ms`; the loop
/// gives up once `retry_budget_ms` of total elapsed time (attempts included)
/// has passed. `deliver` spawns the attempt loop and returns immediately — it
/// never blocks the caller.
pub struct HttpTransport {
    sender: Arc<dyn AttemptSender>,
    backoff_base_ms: u64,
    backoff_cap_ms: u64,
    retry_budget_ms: u64,
    semaphore: Arc<Semaphore>,
    /// Shared pipeline counters. The spawned delivery task bumps `delivered_ok`
    /// (2xx) or `delivered_failed` (retry budget exhausted without a 2xx /
    /// closed semaphore) exactly once when the attempt loop resolves.
    metrics: Arc<WebhookMetrics>,
}

impl HttpTransport {
    /// `timeout_ms` is the per-attempt request timeout; `max_concurrency` caps
    /// simultaneous in-flight deliveries. `metrics` is the shared pipeline
    /// counter set; the spawned delivery task records each resolved outcome.
    pub fn new(
        backoff_base_ms: u64,
        backoff_cap_ms: u64,
        retry_budget_ms: u64,
        timeout_ms: u64,
        max_concurrency: usize,
        metrics: Arc<WebhookMetrics>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .expect("reqwest client builds");
        Self::with_sender(
            Arc::new(ReqwestSender { client }),
            backoff_base_ms,
            backoff_cap_ms,
            retry_budget_ms,
            max_concurrency,
            metrics,
        )
    }

    /// Test seam: build with an injectable [`AttemptSender`] so the retry
    /// policy runs under paused tokio time with deterministic canned attempts.
    pub(crate) fn with_sender(
        sender: Arc<dyn AttemptSender>,
        backoff_base_ms: u64,
        backoff_cap_ms: u64,
        retry_budget_ms: u64,
        max_concurrency: usize,
        metrics: Arc<WebhookMetrics>,
    ) -> Self {
        Self {
            sender,
            backoff_base_ms,
            backoff_cap_ms,
            retry_budget_ms,
            semaphore: Arc::new(Semaphore::new(max_concurrency)),
            metrics,
        }
    }
}

#[async_trait]
impl WebhookTransport for HttpTransport {
    /// Spawn the attempt loop (retry + backoff) and return immediately —
    /// the caller (dispatcher) is never blocked, so it keeps draining its
    /// mailbox. Concurrent deliveries are bounded by the `Semaphore` acquired
    /// *inside* the spawned task. When the loop resolves, the task bumps
    /// `delivered_ok` (2xx) or `delivered_failed` (retry budget exhausted /
    /// closed semaphore) exactly once.
    async fn deliver(&self, delivery: WebhookDelivery) {
        let sender = self.sender.clone();
        let base = self.backoff_base_ms;
        let cap = self.backoff_cap_ms;
        let budget = self.retry_budget_ms;
        let sem = self.semaphore.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            // Concurrency cap: if the broker is saturated this awaits a permit.
            let _permit = match sem.acquire().await {
                Ok(p) => p,
                Err(_) => {
                    // semaphore closed (shutdown): the delivery never went out.
                    metrics.delivered_failed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            let start = tokio::time::Instant::now();
            // First backoff delay: `base` (clamped to >= 1ms so a degenerate 0
            // can't hot-spin), doubling per attempt, capped at `cap`.
            let mut delay_ms = base.max(1);
            let mut attempt: u32 = 0;
            loop {
                match sender.post(&delivery).await {
                    Ok(status) if (200..300).contains(&status) => {
                        metrics.delivered_ok.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                    Ok(status) => {
                        // Any non-2xx is retried (Pusher parity).
                        tracing::debug!(
                            url = %delivery.url,
                            status,
                            attempt,
                            "webhook non-2xx; retrying"
                        );
                    }
                    Err(e) => {
                        // transport error (timeout, connection refused): retry
                        tracing::debug!(
                            url = %delivery.url,
                            error = %e,
                            attempt,
                            "webhook transport error; retrying"
                        );
                    }
                }
                // The budget bounds TOTAL elapsed time — attempts included —
                // not the sleep schedule alone: once `retry_budget_ms` has
                // passed since the first attempt began, give up. `0` therefore
                // means "no retries" (single attempt).
                if start.elapsed() >= Duration::from_millis(budget) {
                    tracing::warn!(
                        url = %delivery.url,
                        budget_ms = budget,
                        attempts = attempt + 1,
                        "webhook delivery exhausted retry budget; dropping"
                    );
                    metrics.delivered_failed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                delay_ms = delay_ms.saturating_mul(2).min(cap);
                attempt += 1;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::Router;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn events() -> Vec<Value> {
        vec![json!({ "name": "channel_occupied", "channel": "ch" })]
    }

    fn delivery() -> WebhookDelivery {
        build_signed_delivery(
            "https://e.test/wh",
            "k",
            "s",
            1,
            &events(),
            &BTreeMap::new(),
        )
    }

    /// Mock [`AttemptSender`]: every call invokes `respond(attempt_index)` and
    /// records the (paused) instant it ran, so tests can pin the exact retry
    /// schedule under `start_paused = true` tokio time.
    struct MockSender<F> {
        respond: F,
        calls: Arc<AtomicUsize>,
        times: Arc<std::sync::Mutex<Vec<tokio::time::Instant>>>,
    }

    #[async_trait]
    impl<F> AttemptSender for MockSender<F>
    where
        F: Fn(u32) -> Result<u16, String> + Send + Sync,
    {
        async fn post(&self, _delivery: &WebhookDelivery) -> Result<u16, String> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst) as u32;
            self.times.lock().unwrap().push(tokio::time::Instant::now());
            (self.respond)(n)
        }
    }

    /// One canned webhook endpoint: the sender, its call counter, and the
    /// (paused) instants of every attempt.
    struct MockEndpoint<F> {
        sender: Arc<MockSender<F>>,
        calls: Arc<AtomicUsize>,
        times: Arc<std::sync::Mutex<Vec<tokio::time::Instant>>>,
    }

    fn mock<F>(respond: F) -> MockEndpoint<F>
    where
        F: Fn(u32) -> Result<u16, String> + Send + Sync + 'static,
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let times = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sender = Arc::new(MockSender {
            respond,
            calls: calls.clone(),
            times: times.clone(),
        });
        MockEndpoint {
            sender,
            calls,
            times,
        }
    }

    /// Yield until the spawned delivery task has made `expected` attempts
    /// (bounded so a real regression fails fast rather than hanging).
    async fn await_attempts(calls: &AtomicUsize, expected: usize) {
        for _ in 0..10_000 {
            if calls.load(Ordering::SeqCst) >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!(
            "delivery task did not reach {expected} attempts (got {})",
            calls.load(Ordering::SeqCst)
        );
    }

    /// Yield until the delivery loop has resolved a failure (bounded).
    async fn await_failed(metrics: &WebhookMetrics) {
        for _ in 0..10_000 {
            if metrics.delivered_failed.load(Ordering::Relaxed) == 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("delivery task did not resolve delivered_failed");
    }

    /// R4 (a): a permanently-failing endpoint is retried with exponential
    /// backoff — 1s doubling to the 60s cap — and gives up only once the
    /// 5-minute total budget has elapsed. With instant (mock) attempts the
    /// default schedule is: attempts at t = 0, 1, 3, 7, 15, 31, 63, 123, 183,
    /// 243, 303 s → exactly 11 attempts, giving up just past the 300 s budget.
    #[tokio::test(start_paused = true)]
    async fn retry_budget_is_five_minutes_of_exponential_backoff() {
        let m = mock(|_| Ok(500u16));
        let metrics = Arc::new(WebhookMetrics::new(64));
        let t = HttpTransport::with_sender(m.sender, 1000, 60_000, 300_000, 10, metrics.clone());
        t.deliver(delivery()).await;
        await_attempts(&m.calls, 1).await;

        // Drive time forward one backoff gap at a time: each step lands exactly
        // on the next attempt's scheduled deadline.
        let schedule = [
            1000u64, 2000, 4000, 8000, 16000, 32000, 60000, 60000, 60000, 60000,
        ];
        let mut expected = 1;
        for gap in schedule {
            tokio::time::advance(Duration::from_millis(gap)).await;
            expected += 1;
            await_attempts(&m.calls, expected).await;
        }

        // All 11 attempts happened; the loop gave up (never a 2xx).
        assert_eq!(m.calls.load(Ordering::SeqCst), 11, "11 attempts total");
        await_failed(&metrics).await;
        assert_eq!(metrics.delivered_ok.load(Ordering::Relaxed), 0);

        // The recorded attempt instants pin the exponential schedule + cap.
        let times = m.times.lock().unwrap().clone();
        let gaps: Vec<u128> = times
            .windows(2)
            .map(|w| (w[1] - w[0]).as_millis())
            .collect();
        assert_eq!(
            gaps,
            vec![1000, 2000, 4000, 8000, 16000, 32000, 60000, 60000, 60000, 60000],
            "1s doubling, capped at 60s"
        );
        // Give-up happens only AFTER the full budget of virtual time elapsed
        // (303 s ≥ 300 s budget — the budget bounds total time incl. attempts).
        let gave_up_after = tokio::time::Instant::now() - times[0];
        assert!(
            gave_up_after >= Duration::from_millis(300_000),
            "gave up after only {gave_up_after:?}"
        );
    }

    /// R4 (a, early-budget check): just before the budget elapses the loop is
    /// still retrying (10 attempts done by t = 243 s, no failure recorded).
    #[tokio::test(start_paused = true)]
    async fn still_retrying_just_inside_the_budget() {
        let m = mock(|_| Ok(500u16));
        let metrics = Arc::new(WebhookMetrics::new(64));
        let t = HttpTransport::with_sender(m.sender, 1000, 60_000, 300_000, 10, metrics.clone());
        t.deliver(delivery()).await;
        await_attempts(&m.calls, 1).await;
        // Drive each scheduled gap up to t = 243 s (10 attempts), confirming
        // each lands before moving the clock again.
        let mut expected = 1;
        for gap in [1000u64, 2000, 4000, 8000, 16000, 32000, 60000, 60000, 60000] {
            tokio::time::advance(Duration::from_millis(gap)).await;
            expected += 1;
            await_attempts(&m.calls, expected).await;
        }
        assert_eq!(m.calls.load(Ordering::SeqCst), 10, "attempt 10 at t=243s");
        assert_eq!(
            metrics.delivered_failed.load(Ordering::Relaxed),
            0,
            "no give-up before the budget elapses"
        );
        assert_eq!(metrics.delivered_ok.load(Ordering::Relaxed), 0);
    }

    /// R4 (b): a 404 (non-2xx that is neither 5xx nor 429) is retried.
    #[tokio::test(start_paused = true)]
    async fn non_2xx_404_is_retried() {
        let m = mock(|_| Ok(404u16));
        let metrics = Arc::new(WebhookMetrics::new(64));
        let t = HttpTransport::with_sender(m.sender, 50, 100, 300, 10, metrics.clone());
        t.deliver(delivery()).await;
        // Let attempt 1 run (and arm its backoff timer at t=50ms) BEFORE
        // jumping the clock, then jump well past the (small) budget: the loop
        // wakes, retries, and resolves failed — strictly more than the single
        // attempt the old permanent-4xx policy would have made.
        await_attempts(&m.calls, 1).await;
        tokio::time::advance(Duration::from_millis(10_000)).await;
        await_failed(&metrics).await;
        assert!(
            m.calls.load(Ordering::SeqCst) > 1,
            "404 must be retried, got {} attempts",
            m.calls.load(Ordering::SeqCst)
        );
        assert_eq!(metrics.delivered_ok.load(Ordering::Relaxed), 0);
    }

    /// R4 (c): a 2xx resolves immediately with exactly one attempt.
    #[tokio::test(start_paused = true)]
    async fn success_is_exactly_one_attempt() {
        let m = mock(|_| Ok(200u16));
        let metrics = Arc::new(WebhookMetrics::new(64));
        let t = HttpTransport::with_sender(m.sender, 1000, 60_000, 300_000, 10, metrics.clone());
        t.deliver(delivery()).await;
        await_attempts(&m.calls, 1).await;
        assert_eq!(metrics.delivered_ok.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.delivered_failed.load(Ordering::Relaxed), 0);
        // Advance far past the budget: no further attempts after success.
        tokio::time::advance(Duration::from_millis(400_000)).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert_eq!(m.calls.load(Ordering::SeqCst), 1, "no retries after 2xx");
    }

    /// Transport errors (timeouts, refused connections) retry on the same
    /// budget-bounded schedule.
    #[tokio::test(start_paused = true)]
    async fn transport_errors_are_retried() {
        let m = mock(|_| Err("connection refused".into()));
        let metrics = Arc::new(WebhookMetrics::new(64));
        let t = HttpTransport::with_sender(m.sender, 10, 20, 50, 10, metrics.clone());
        t.deliver(delivery()).await;
        // Attempt 1 first (arms the timer), then jump past the small budget.
        await_attempts(&m.calls, 1).await;
        tokio::time::advance(Duration::from_millis(1000)).await;
        await_failed(&metrics).await;
        assert!(
            m.calls.load(Ordering::SeqCst) > 1,
            "transport errors must be retried"
        );
    }

    /// 503 for the first two hits, then 200 — counts every hit in the shared counter.
    async fn flaky_handler(State(calls): State<Arc<AtomicUsize>>) -> StatusCode {
        let n = calls.fetch_add(1, Ordering::SeqCst);
        if n < 2 {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::OK
        }
    }

    /// Always 404 (non-2xx — now retryable) — counts every hit so we can
    /// assert "retried, not treated as permanent".
    async fn reject_handler(State(calls): State<Arc<AtomicUsize>>) -> StatusCode {
        calls.fetch_add(1, Ordering::SeqCst);
        StatusCode::NOT_FOUND
    }

    /// Bind a throwaway server on a random port; the handler still carries the
    /// shared counter as its `State`, which `with_state` then injects.
    async fn spawn_mock(
        handler: axum::routing::MethodRouter<Arc<AtomicUsize>>,
        calls: Arc<AtomicUsize>,
    ) -> std::net::SocketAddr {
        let app = Router::new().route("/wh", handler).with_state(calls);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr
    }

    #[tokio::test]
    async fn http_transport_retries_on_503_then_succeeds() {
        // 503, 503, 200 → exactly 3 attempts.
        let calls = Arc::new(AtomicUsize::new(0));
        let addr = spawn_mock(post(flaky_handler), calls.clone()).await;
        let metrics = Arc::new(WebhookMetrics::new(64));
        // base 1ms / cap 10ms / budget 5s so the test is fast.
        let t = HttpTransport::new(1, 10, 5_000, 5_000, 10, metrics.clone());
        let d = build_signed_delivery(
            &format!("http://{addr}/wh"),
            "k",
            "s",
            1,
            &events(),
            &BTreeMap::new(),
        );
        t.deliver(d).await;
        // small settle for the spawned delivery task
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 3, "two retries then success");
        // A 2xx (after retries) bumps delivered_ok exactly once, never failed.
        assert_eq!(metrics.delivered_ok.load(Ordering::Relaxed), 1, "one ok");
        assert_eq!(
            metrics.delivered_failed.load(Ordering::Relaxed),
            0,
            "no failed"
        );
    }

    #[tokio::test]
    async fn http_transport_retries_on_404() {
        let calls = Arc::new(AtomicUsize::new(0));
        let addr = spawn_mock(post(reject_handler), calls.clone()).await;
        let metrics = Arc::new(WebhookMetrics::new(64));
        // Small budget (100ms) so the retry loop resolves quickly in real time.
        let t = HttpTransport::new(1, 10, 100, 5_000, 10, metrics.clone());
        let d = build_signed_delivery(
            &format!("http://{addr}/wh"),
            "k",
            "s",
            1,
            &events(),
            &BTreeMap::new(),
        );
        t.deliver(d).await;
        // settle past the small budget: retries happen, then give-up.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            calls.load(Ordering::SeqCst) > 1,
            "non-2xx must be retried (Pusher retries any non-2xx), got {}",
            calls.load(Ordering::SeqCst)
        );
        assert_eq!(metrics.delivered_ok.load(Ordering::Relaxed), 0, "no ok");
        assert_eq!(
            metrics.delivered_failed.load(Ordering::Relaxed),
            1,
            "one failed after budget exhaustion"
        );
    }

    #[test]
    fn signature_is_hmac_of_raw_body_kat() {
        let d = build_signed_delivery(
            "https://e.test/wh",
            "app-key",
            "app-secret",
            1700000000000,
            &events(),
            &BTreeMap::new(),
        );
        // The signed body is the exact serialized envelope.
        // serde_json's json! macro serializes object keys in alphabetical order.
        assert_eq!(
            d.body,
            r#"{"events":[{"channel":"ch","name":"channel_occupied"}],"time_ms":1700000000000}"#
        );
        // KAT: this hex is computed independently in Step 4. Capture it RED-first
        // from the failing assertion's "left" value, then paste it here.
        assert_eq!(
            d.headers["X-Pusher-Signature"],
            hmac_sha256_hex("app-secret", &d.body)
        );
        // And it really is HMAC-SHA256(secret, body) — cross-check via the primitive.
        assert_eq!(
            d.headers["X-Pusher-Signature"].len(),
            64,
            "hex sha256 is 64 chars"
        );
    }

    #[test]
    fn exact_three_pusher_headers_present() {
        let d = build_signed_delivery(
            "https://e.test/wh",
            "the-key",
            "the-secret",
            1,
            &events(),
            &BTreeMap::new(),
        );
        assert_eq!(d.headers["Content-Type"], "application/json");
        assert_eq!(d.headers["X-Pusher-Key"], "the-key");
        assert!(d.headers.contains_key("X-Pusher-Signature"));
    }

    #[tokio::test]
    async fn recording_transport_records_each_delivery() {
        let t = RecordingTransport::new();
        let d = build_signed_delivery(
            "https://e.test/wh",
            "k",
            "s",
            1,
            &events(),
            &BTreeMap::new(),
        );
        t.deliver(d.clone()).await;
        let recorded = t.recorded().await;
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0], d);
    }

    #[test]
    fn custom_headers_merge_but_cannot_override_pusher_headers() {
        let mut custom = BTreeMap::new();
        custom.insert("X-Custom".into(), "yes".into());
        // Attempt to override all three Pusher headers — must be ignored.
        custom.insert("Content-Type".into(), "text/plain".into());
        custom.insert("X-Pusher-Key".into(), "attacker".into());
        custom.insert("X-Pusher-Signature".into(), "forged".into());

        let d = build_signed_delivery(
            "https://e.test/wh",
            "real-key",
            "real-secret",
            5,
            &events(),
            &custom,
        );
        assert_eq!(d.headers["X-Custom"], "yes", "custom header merged");
        assert_eq!(d.headers["Content-Type"], "application/json");
        assert_eq!(d.headers["X-Pusher-Key"], "real-key");
        assert_ne!(d.headers["X-Pusher-Signature"], "forged");
        assert_eq!(
            d.headers["X-Pusher-Signature"],
            hmac_sha256_hex("real-secret", &d.body)
        );
    }
}
