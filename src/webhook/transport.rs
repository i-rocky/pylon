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
/// delays grow `backoff_base_ms * 2^n` clamped to `[1ms, backoff_cap_ms]` (the
/// final sleep is additionally clamped to the budget's remaining time); the
/// loop gives up once `retry_budget_ms` of total elapsed time (attempts
/// included) has passed. The semaphore permit is held per ATTEMPT — released
/// across backoff sleeps — so `max_concurrency` bounds in-flight HTTP requests
/// without letting a dead endpoint starve healthy ones. `deliver` spawns the
/// attempt loop and returns immediately — it never blocks the caller.
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
    ///
    /// Returns `Err` when the underlying reqwest client cannot be built (e.g. a
    /// TLS-backend initialization failure). This runs at startup even when zero
    /// webhooks are configured, so a build failure must fail startup cleanly
    /// with a real error (propagated by the caller) rather than aborting the
    /// process with a panic (G9).
    pub fn new(
        backoff_base_ms: u64,
        backoff_cap_ms: u64,
        retry_budget_ms: u64,
        timeout_ms: u64,
        max_concurrency: usize,
        metrics: Arc<WebhookMetrics>,
    ) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()?;
        Ok(Self::with_sender(
            Arc::new(ReqwestSender { client }),
            backoff_base_ms,
            backoff_cap_ms,
            retry_budget_ms,
            max_concurrency,
            metrics,
        ))
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
            // Floor the cap at 1ms so a degenerate 0 can't instant-fire
            // retries for the whole budget.
            backoff_cap_ms: backoff_cap_ms.max(1),
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
    /// mailbox. Concurrent IN-FLIGHT attempts are bounded by the `Semaphore`
    /// acquired *per attempt, inside* the spawned task: the permit is held
    /// only while the HTTP request is in flight and released before each
    /// backoff sleep, so a dead endpoint parked in backoff cannot starve
    /// deliveries to healthy endpoints. When the loop resolves, the task
    /// bumps `delivered_ok` (2xx) or `delivered_failed` (retry budget
    /// exhausted / closed semaphore) exactly once.
    async fn deliver(&self, delivery: WebhookDelivery) {
        let sender = self.sender.clone();
        let base = self.backoff_base_ms;
        let cap = self.backoff_cap_ms;
        let budget = self.retry_budget_ms;
        let sem = self.semaphore.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            // First permit before the clock starts: the budget bounds time
            // spent RETRYING, not time queued behind a saturated semaphore.
            // (semaphore closed = shutdown: the delivery never went out)
            let mut permit = match sem.acquire().await {
                Ok(p) => p,
                Err(_) => {
                    metrics.delivered_failed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            let start = tokio::time::Instant::now();
            // First backoff delay: `base`, clamped to [1ms, cap] (so neither a
            // degenerate 0 base nor a base > cap can violate the bounds),
            // doubling per attempt, capped at `cap`.
            let mut delay_ms = base.max(1).min(cap);
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
                // Release the permit for the backoff (and any permit wait):
                // sleeping is not "in flight" — a dead endpoint must not hold
                // one of the global concurrency slots while it backs off.
                drop(permit);
                // The budget bounds TOTAL elapsed time — attempts included —
                // not just the sleep schedule alone: once `retry_budget_ms`
                // has passed since the first attempt began, give up. `0`
                // therefore means "no retries" (single attempt).
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
                // Clamp the sleep so the FINAL backoff cannot overshoot the
                // budget: give-up time stays exact (worst overshoot is one
                // attempt's duration, not cap + timeout).
                let remaining_ms = budget - start.elapsed().as_millis() as u64;
                tokio::time::sleep(Duration::from_millis(delay_ms.min(remaining_ms))).await;
                delay_ms = delay_ms.saturating_mul(2).min(cap);
                attempt += 1;
                // Re-acquire for the next attempt (closed = shutdown).
                permit = match sem.acquire().await {
                    Ok(p) => p,
                    Err(_) => {
                        metrics.delivered_failed.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                };
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

    fn delivery_to(url: &str) -> WebhookDelivery {
        build_signed_delivery(url, "k", "s", 1, &events(), &BTreeMap::new())
    }

    fn delivery() -> WebhookDelivery {
        delivery_to("https://e.test/wh")
    }

    /// Mock [`AttemptSender`]: every call invokes `respond(attempt_index,
    /// delivery)` and records the (paused) instant it ran, so tests can pin the
    /// exact retry schedule under `start_paused = true` tokio time (and route
    /// canned responses per URL).
    struct MockSender<F> {
        respond: F,
        calls: Arc<AtomicUsize>,
        times: Arc<std::sync::Mutex<Vec<tokio::time::Instant>>>,
    }

    #[async_trait]
    impl<F> AttemptSender for MockSender<F>
    where
        F: Fn(u32, &WebhookDelivery) -> Result<u16, String> + Send + Sync,
    {
        async fn post(&self, delivery: &WebhookDelivery) -> Result<u16, String> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst) as u32;
            self.times.lock().unwrap().push(tokio::time::Instant::now());
            (self.respond)(n, delivery)
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
        F: Fn(u32, &WebhookDelivery) -> Result<u16, String> + Send + Sync + 'static,
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
    /// backoff — 1s doubling to the 60s cap — and gives up once the 5-minute
    /// total budget elapses. With instant (mock) attempts the default schedule
    /// is: attempts at t = 0, 1, 3, 7, 15, 31, 63, 123, 183, 243 s, then a
    /// FINAL backoff clamped to the budget's remaining 57 s → the 11th attempt
    /// lands exactly at the 300 s budget and the loop gives up there.
    #[tokio::test(start_paused = true)]
    async fn retry_budget_is_five_minutes_of_exponential_backoff() {
        let m = mock(|_, _| Ok(500u16));
        let metrics = Arc::new(WebhookMetrics::new(64));
        let t = HttpTransport::with_sender(m.sender, 1000, 60_000, 300_000, 10, metrics.clone());
        t.deliver(delivery()).await;
        await_attempts(&m.calls, 1).await;

        // Drive time forward one backoff gap at a time: each step lands exactly
        // on the next attempt's scheduled deadline.
        let schedule = [
            1000u64, 2000, 4000, 8000, 16000, 32000, 60000, 60000, 60000, 57_000,
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

        // The recorded attempt instants pin the exponential schedule + cap; the
        // final gap is the backoff clamped to the budget's remaining 57 s.
        let times = m.times.lock().unwrap().clone();
        let gaps: Vec<u128> = times
            .windows(2)
            .map(|w| (w[1] - w[0]).as_millis())
            .collect();
        assert_eq!(
            gaps,
            vec![1000, 2000, 4000, 8000, 16000, 32000, 60000, 60000, 60000, 57000],
            "1s doubling, capped at 60s, final sleep clamped to the budget"
        );
        // Give-up time is EXACT (within paused-time scheduling epsilon): the
        // budget bounds total elapsed time incl. attempts, and the final sleep
        // is clamped so it cannot overshoot by up to cap + timeout.
        let gave_up_after = tokio::time::Instant::now() - times[0];
        assert!(
            gave_up_after >= Duration::from_millis(300_000),
            "gave up after only {gave_up_after:?}"
        );
        assert!(
            gave_up_after <= Duration::from_millis(300_000 + 5),
            "gave up late: {gave_up_after:?}"
        );
    }

    /// R4 (a, early-budget check): just before the budget elapses the loop is
    /// still retrying (10 attempts done by t = 243 s, no failure recorded).
    #[tokio::test(start_paused = true)]
    async fn still_retrying_just_inside_the_budget() {
        let m = mock(|_, _| Ok(500u16));
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

    /// Yield until the delivery loop has resolved a success (bounded).
    async fn await_ok(metrics: &WebhookMetrics) {
        for _ in 0..10_000 {
            if metrics.delivered_ok.load(Ordering::Relaxed) == 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("delivery task did not resolve delivered_ok");
    }

    /// Regression (fix round): the concurrency permit must be held PER ATTEMPT,
    /// not across backoff sleeps. With `max_concurrency = 1` and a dead
    /// endpoint parked mid-backoff, a delivery to a HEALTHY endpoint must still
    /// proceed during the sleep window — otherwise one dead endpoint starves
    /// the whole semaphore and healthy endpoints stop receiving webhooks.
    #[tokio::test(start_paused = true)]
    async fn permit_released_during_backoff_lets_other_deliveries_proceed() {
        let t0 = tokio::time::Instant::now();
        let m = mock(|_, d| {
            if d.url.ends_with("/dead") {
                Ok(500u16)
            } else {
                Ok(200u16)
            }
        });
        let metrics = Arc::new(WebhookMetrics::new(64));
        // ONE transport, ONE permit (max_concurrency = 1), shared by both URLs.
        let t = HttpTransport::with_sender(m.sender, 100, 200, 400, 1, metrics.clone());

        // The dead endpoint goes first: attempt 1 fails, the permit is released,
        // and the task parks on its 100ms backoff sleep.
        t.deliver(delivery_to("https://e.test/dead")).await;
        await_attempts(&m.calls, 1).await;

        // The healthy endpoint is delivered while the clock has NOT advanced
        // past the dead endpoint's backoff window: it acquires the (released)
        // permit, gets its 200, and resolves ok — all before t = 100ms.
        t.deliver(delivery_to("https://e.test/healthy")).await;
        await_ok(&metrics).await;
        assert_eq!(
            m.calls.load(Ordering::SeqCst),
            2,
            "healthy delivery attempted exactly once"
        );
        assert!(
            t0.elapsed() < Duration::from_millis(100),
            "healthy delivery proceeded during the dead endpoint's backoff, got {}ms",
            t0.elapsed().as_millis()
        );
        assert_eq!(metrics.delivered_failed.load(Ordering::Relaxed), 0);

        // Drive the dead delivery to its (small) budget: attempts at
        // 0, 100, 300, then a final sleep clamped to the remaining 100ms →
        // attempt 4 at exactly t = 400ms gives up.
        for gap in [100u64, 200, 100] {
            tokio::time::advance(Duration::from_millis(gap)).await;
            tokio::task::yield_now().await;
        }
        await_failed(&metrics).await;
        assert_eq!(
            m.calls.load(Ordering::SeqCst),
            5,
            "dead endpoint: 4 attempts + the healthy delivery's 1"
        );
    }

    /// The FIRST backoff delay is clamped to the cap too: a `base > cap`
    /// configuration must not overshoot the documented per-delay upper bound,
    /// and a degenerate `cap = 0` must not instant-fire retries (floor 1ms).
    #[tokio::test(start_paused = true)]
    async fn first_delay_is_clamped_to_cap() {
        // base 5000 > cap 100: the first delay must be the cap (100ms), not base.
        let m = mock(|_, _| Ok(500u16));
        let metrics = Arc::new(WebhookMetrics::new(64));
        let t = HttpTransport::with_sender(m.sender, 5000, 100, 10_000, 10, metrics.clone());
        t.deliver(delivery()).await;
        await_attempts(&m.calls, 1).await;
        tokio::time::advance(Duration::from_millis(100)).await;
        await_attempts(&m.calls, 2).await;
        let times = m.times.lock().unwrap().clone();
        assert_eq!(
            (times[1] - times[0]).as_millis(),
            100,
            "first delay clamped to cap"
        );

        // cap = 0 is clamped to 1ms: retries are paced, not instant-fire.
        let m2 = mock(|_, _| Ok(500u16));
        let metrics2 = Arc::new(WebhookMetrics::new(64));
        let t2 = HttpTransport::with_sender(m2.sender, 50, 0, 10_000, 10, metrics2);
        t2.deliver(delivery()).await;
        await_attempts(&m2.calls, 1).await;
        tokio::time::advance(Duration::from_millis(1)).await;
        await_attempts(&m2.calls, 2).await;
        let times2 = m2.times.lock().unwrap().clone();
        assert_eq!(
            (times2[1] - times2[0]).as_millis(),
            1,
            "cap=0 floored to 1ms"
        );
    }

    /// R4 (b): a 404 (non-2xx that is neither 5xx nor 429) is retried.
    #[tokio::test(start_paused = true)]
    async fn non_2xx_404_is_retried() {
        let m = mock(|_, _| Ok(404u16));
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
        let m = mock(|_, _| Ok(200u16));
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
        let m = mock(|_, _| Err("connection refused".into()));
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
        let t = HttpTransport::new(1, 10, 5_000, 5_000, 10, metrics.clone())
            .expect("reqwest client builds in tests");
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
        let t = HttpTransport::new(1, 10, 100, 5_000, 10, metrics.clone())
            .expect("reqwest client builds in tests");
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

    /// G9: `new` returns a Result instead of panicking on a client-build
    /// failure (which runs at startup even with zero webhooks configured).
    /// In a healthy environment the client builds and `new` is `Ok`. Making
    /// reqwest's builder fail DETERMINISTICALLY (a TLS-backend init failure)
    /// is not injectable without mocking reqwest — deliberately not done —
    /// so the Err path itself is covered at the plumbing level in
    /// `webhook::spawn`'s tests (a factory error propagates out of spawn).
    #[test]
    fn new_returns_ok_when_the_client_builds() {
        let metrics = Arc::new(WebhookMetrics::new(64));
        assert!(HttpTransport::new(1000, 60_000, 300_000, 5_000, 10, metrics).is_ok());
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
