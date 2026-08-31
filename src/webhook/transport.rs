//! Webhook delivery: the signed request value object, envelope/sign helper, the
//! `WebhookTransport` trait, and its `HttpTransport` / `RecordingTransport` impls.

use crate::auth::signature::hmac_sha256_hex;
use crate::webhook::WebhookMetrics;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};

// ── S2: webhook target SSRF guard ─────────────────────────────────────────────

/// Classify a resolved (or literal) webhook target address as "private" — i.e.
/// a target the guard refuses unless `PYLON_WEBHOOK_ALLOW_PRIVATE_TARGETS=1`:
///
/// * IPv4: loopback (127/8), unspecified (`0.0.0.0`), link-local
///   (169.254.0.0/16 — includes the cloud metadata addresses), RFC1918
///   (10/8, 172.16/12, 192.168/16).
/// * IPv6: loopback (`::1`), unspecified (`::`), unique-local (fc00::/7),
///   link-local (fe80::/10).
/// * IPv4-mapped (`::ffff:a.b.c.d`) and the deprecated IPv4-compatible
///   (`::a.b.c.d`) IPv6 forms are classified by their embedded IPv4 address —
///   a dual-stack socket interprets them as that v4 address.
///
/// Pure function over [`IpAddr`] so every class is unit-testable.
pub fn is_private_target(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_unspecified()
                || v4.is_link_local()
                || v4.is_private()
                // RFC 6598 shared/CGNAT space (100.64.0.0/10) — Tailscale and
                // several k8s CNIs run real internal infrastructure there.
                // (`Ipv4Addr::is_shared` is unstable at our MSRV, so the /10
                // is matched on octets: 100.64.0.0 — 100.127.255.255.)
                || (v4.octets()[0] == 100 && (0x40..=0x7f).contains(&v4.octets()[1]))
                // Multicast and broadcast are never legitimate webhook
                // receivers. (240.0.0.0/4 class-E and NAT64 64:ff9b::/96 are
                // deliberately NOT classified here — ledgered only.)
                || v4.is_multicast()
                || v4.is_broadcast()
        }
        IpAddr::V6(v6) => {
            // The v6 special forms come FIRST: `to_ipv4` also converts the
            // IPv4-compatible range ::/96, which would otherwise swallow ::1
            // and :: into "0.0.0.1" / "0.0.0.0".
            if v6.is_loopback() || v6.is_unspecified() {
                return true;
            }
            // IPv4-mapped ::ffff:a.b.c.d — classify by the embedded v4.
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_private_target(IpAddr::V4(v4));
            }
            // The (deprecated) IPv4-compatible form ::a.b.c.d — still
            // interpreted as the v4 address by dual-stack sockets; classify
            // it the same way (fail closed).
            if let Some(v4) = v6.to_ipv4() {
                return is_private_target(IpAddr::V4(v4));
            }
            (v6.segments()[0] & 0xfe00) == 0xfc00 // fc00::/7 unique-local
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // fe80::/10 link-local
                || v6.is_multicast() // ff00::/8
        }
    }
}

/// DNS resolution seam for the SSRF pre-flight. Production uses
/// [`SystemResolver`] (`tokio::net::lookup_host`); tests inject canned answers.
#[async_trait]
pub trait Resolver: Send + Sync {
    /// Resolve a hostname to its addresses (empty = the name did not resolve).
    async fn resolve(&self, host: &str) -> Vec<IpAddr>;
}

/// Production [`Resolver`]: `tokio::net::lookup_host`. `lookup_host` requires a
/// `host:port` pair, so the (irrelevant) conventional ports are tried in turn;
/// only the addresses are used — the URL's own port always wins in the pin.
pub struct SystemResolver;

#[async_trait]
impl Resolver for SystemResolver {
    async fn resolve(&self, host: &str) -> Vec<IpAddr> {
        for port in [80u16, 443] {
            if let Ok(addrs) = tokio::net::lookup_host((host, port)).await {
                let mut ips: Vec<IpAddr> = Vec::new();
                for sa in addrs {
                    if !ips.contains(&sa.ip()) {
                        ips.push(sa.ip());
                    }
                }
                if !ips.is_empty() {
                    return ips;
                }
            }
        }
        Vec::new()
    }
}

/// A delivery's pinned target: the URL's hostname plus the exact addresses the
/// pre-flight resolved. Passed to `ClientBuilder::resolve_to_addrs` so the
/// actual request cannot drift to a second, un-checked lookup.
#[derive(Debug)]
pub(crate) struct DeliveryPin {
    /// The hostname exactly as it appears in the URL (matches what reqwest
    /// resolves — no brackets for IPv6, which never reaches here: literal IPs
    /// are not pinned, they are classified directly).
    pub(crate) host: String,
    pub(crate) addrs: Vec<SocketAddr>,
}

/// The SSRF pre-flight: parse the delivery URL, refuse non-`http`/`https`
/// schemes, classify every address the hostname resolves to (or the literal IP
/// itself), and — for hostnames — pin the delivery to the resolved addresses.
#[derive(Clone)]
pub(crate) struct SsrfGuard {
    pub(crate) resolver: Arc<dyn Resolver>,
    pub(crate) allow_private: bool,
}

impl SsrfGuard {
    /// `Ok(None)` — allowed, literal-IP URL: no lookup, so no pin needed (the
    /// URL host IS the address). `Ok(Some(pin))` — allowed hostname URL, pinned
    /// to its pre-flight addresses. `Err(reason)` — refuse the delivery.
    pub(crate) async fn check(&self, url: &str) -> Result<Option<DeliveryPin>, String> {
        let parsed = reqwest::Url::parse(url).map_err(|e| format!("invalid URL: {e}"))?;
        let scheme = parsed.scheme().to_ascii_lowercase();
        if scheme != "http" && scheme != "https" {
            return Err(format!(
                "unsupported scheme {scheme:?}: webhook targets must be http:// or https://"
            ));
        }
        let host = parsed
            .host_str()
            .ok_or_else(|| "URL has no host".to_string())?
            .to_string();
        let port = parsed
            .port_or_known_default()
            .ok_or_else(|| "unknown port for scheme".to_string())?;

        // Literal IP hosts skip resolution but run the SAME classification.
        // IPv6 hosts arrive bracketed ("[::1]"); a zone suffix fails the parse
        // and falls through to the resolver, which cannot resolve it either —
        // both paths fail closed.
        let literal = match host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
            Some(v6) => v6.parse::<Ipv6Addr>().ok().map(IpAddr::V6),
            None => host.parse::<IpAddr>().ok(),
        };
        match literal {
            Some(ip) => {
                if is_private_target(ip) && !self.allow_private {
                    Err(format!(
                        "target {ip} is a private/loopback/link-local address"
                    ))
                } else {
                    Ok(None)
                }
            }
            None => {
                let ips = self.resolver.resolve(&host).await;
                if ips.is_empty() {
                    return Err(format!("host {host} did not resolve"));
                }
                let mut addrs = Vec::with_capacity(ips.len());
                for ip in ips {
                    if is_private_target(ip) && !self.allow_private {
                        // ANY private address in the answer set refuses the
                        // delivery (a round-robin DNS split across public and
                        // private space must not leak requests inward).
                        return Err(format!(
                            "host {host} resolves to private/loopback/link-local address {ip}"
                        ));
                    }
                    addrs.push(SocketAddr::new(ip, port));
                }
                Ok(Some(DeliveryPin { host, addrs }))
            }
        }
    }
}

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
/// baked into the client. Redirects are NEVER followed (`Policy::none`) — a
/// 3xx is returned as-is and lands in the non-2xx retry path. Following a
/// redirect would let an attacker-controlled 302 to e.g.
/// `http://169.254.169.254/` bypass the SSRF pre-flight pinning entirely.
struct ReqwestSender {
    client: reqwest::Client,
}

impl ReqwestSender {
    /// Shared (unpinned) client: redirects off, per-attempt timeout on.
    fn shared(timeout_ms: u64) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: reqwest_client_builder(timeout_ms).build()?,
        })
    }

    /// Per-delivery client pinned to the pre-flight's resolved addresses, so
    /// the connect cannot drift from the addresses the guard classified.
    /// (The URL's own port always wins over the pin's port — reqwest contract.)
    fn pinned(pin: &DeliveryPin, timeout_ms: u64) -> Result<Self, String> {
        let client = reqwest_client_builder(timeout_ms)
            .resolve_to_addrs(&pin.host, &pin.addrs)
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self { client })
    }
}

/// The common client recipe: per-attempt timeout + NO redirect following.
fn reqwest_client_builder(timeout_ms: u64) -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .timeout(Duration::from_millis(timeout_ms))
        .redirect(reqwest::redirect::Policy::none())
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
    /// Per-attempt timeout (kept for building the per-delivery pinned client).
    timeout_ms: u64,
    /// S2 SSRF pre-flight. `None` on the pure retry-policy test seam
    /// ([`HttpTransport::with_sender`]); `Some` on every production path.
    guard: Option<SsrfGuard>,
    /// Shared pipeline counters. The spawned delivery task bumps `delivered_ok`
    /// (2xx) or `delivered_failed` (retry budget exhausted without a 2xx /
    /// closed semaphore / target refused by the SSRF guard) exactly once when
    /// the attempt loop resolves.
    metrics: Arc<WebhookMetrics>,
}

impl HttpTransport {
    /// `timeout_ms` is the per-attempt request timeout; `max_concurrency` caps
    /// simultaneous in-flight deliveries. `allow_private_targets` is the
    /// `PYLON_WEBHOOK_ALLOW_PRIVATE_TARGETS` escape hatch (default `false`):
    /// when `false`, deliveries whose target resolves to (or literally is) a
    /// private/loopback/link-local address are refused before any HTTP is
    /// sent — see [`SsrfGuard`]. `metrics` is the shared pipeline counter set;
    /// the spawned delivery task records each resolved outcome.
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
        allow_private_targets: bool,
        metrics: Arc<WebhookMetrics>,
    ) -> Result<Self, reqwest::Error> {
        let sender = Arc::new(ReqwestSender::shared(timeout_ms)?);
        let guard = SsrfGuard {
            resolver: Arc::new(SystemResolver),
            allow_private: allow_private_targets,
        };
        Ok(Self::with_sender_and_guard(
            sender,
            Some(guard),
            timeout_ms,
            backoff_base_ms,
            backoff_cap_ms,
            retry_budget_ms,
            max_concurrency,
            metrics,
        ))
    }

    /// Test seam: like [`HttpTransport::new`] but with an injectable DNS
    /// [`Resolver`], so the SSRF pre-flight can be pinned to exact addresses
    /// (and real HTTP still exercised) without touching the system resolver.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn with_resolver(
        resolver: Arc<dyn Resolver>,
        allow_private_targets: bool,
        backoff_base_ms: u64,
        backoff_cap_ms: u64,
        retry_budget_ms: u64,
        timeout_ms: u64,
        max_concurrency: usize,
        metrics: Arc<WebhookMetrics>,
    ) -> Result<Self, reqwest::Error> {
        let sender = Arc::new(ReqwestSender::shared(timeout_ms)?);
        let guard = SsrfGuard {
            resolver,
            allow_private: allow_private_targets,
        };
        Ok(Self::with_sender_and_guard(
            sender,
            Some(guard),
            timeout_ms,
            backoff_base_ms,
            backoff_cap_ms,
            retry_budget_ms,
            max_concurrency,
            metrics,
        ))
    }

    /// Test seam: build with an injectable [`AttemptSender`] so the retry
    /// policy runs under paused tokio time with deterministic canned attempts.
    /// No SSRF guard runs on this seam (the retry policy is what is under
    /// test); see [`HttpTransport::with_sender_and_guard`] for the guarded
    /// variant. Only used by this module's tests (`cfg(test)`).
    #[cfg(test)]
    pub(crate) fn with_sender(
        sender: Arc<dyn AttemptSender>,
        backoff_base_ms: u64,
        backoff_cap_ms: u64,
        retry_budget_ms: u64,
        max_concurrency: usize,
        metrics: Arc<WebhookMetrics>,
    ) -> Self {
        Self::with_sender_and_guard(
            sender,
            None,
            0,
            backoff_base_ms,
            backoff_cap_ms,
            retry_budget_ms,
            max_concurrency,
            metrics,
        )
    }

    /// Test seam: injectable [`AttemptSender`] AND [`SsrfGuard`] — the guarded
    /// delivery path with a counting mock sender, so a refusal can be pinned
    /// to "zero HTTP attempts".
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_sender_and_guard(
        sender: Arc<dyn AttemptSender>,
        guard: Option<SsrfGuard>,
        timeout_ms: u64,
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
            timeout_ms,
            guard,
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
        let guard = self.guard.clone();
        let pin_timeout_ms = self.timeout_ms;
        let base = self.backoff_base_ms;
        let cap = self.backoff_cap_ms;
        let budget = self.retry_budget_ms;
        let sem = self.semaphore.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            // S2 pre-flight (inside the spawn: deliver must never block the
            // dispatcher, and a DNS lookup takes real time). A refusal is a
            // CONFIGURATION error — retrying a refused target for the whole
            // budget cannot succeed — so it fails the delivery attempt fast:
            // one `delivered_failed` outcome, a warn log naming the reason,
            // zero HTTP attempts, and no budget or semaphore permit consumed.
            // (Empty DNS resolution is treated the same way: there is nothing
            // to pin, so the delivery cannot be made responsibly.)
            let sender: Arc<dyn AttemptSender> = match guard.as_ref() {
                None => sender,
                Some(g) => match g.check(&delivery.url).await {
                    // Allowed literal-IP URL: the URL host IS the address —
                    // no lookup can drift, the shared sender is safe.
                    Ok(None) => sender,
                    // Allowed hostname URL: pin this delivery to the
                    // pre-flight addresses so the connect cannot resolve
                    // again on its own.
                    Ok(Some(pin)) => match ReqwestSender::pinned(&pin, pin_timeout_ms) {
                        Ok(s) => Arc::new(s),
                        Err(e) => {
                            tracing::warn!(
                                url = %delivery.url,
                                error = %e,
                                "webhook pinned client build failed; dropping"
                            );
                            metrics.delivered_failed.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    },
                    Err(reason) => {
                        tracing::warn!(
                            url = %delivery.url,
                            reason = %reason,
                            "webhook target refused by SSRF guard; dropping without retry"
                        );
                        metrics.delivered_failed.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                },
            };

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
                        // Any non-2xx is retried (Pusher parity). That
                        // includes 3xx: redirects are never followed
                        // (`Policy::none`), so a 302 is just another non-2xx
                        // outcome — following it would let an attacker's
                        // redirect sidestep the SSRF pre-flight pin.
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
        // base 1ms / cap 10ms / budget 5s so the test is fast. The receiver is
        // loopback, so this live-HTTP test opts into the SSRF escape hatch.
        let t = HttpTransport::new(1, 10, 5_000, 5_000, 10, true, metrics.clone())
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
        // Loopback receiver → SSRF escape hatch opted in.
        let t = HttpTransport::new(1, 10, 100, 5_000, 10, true, metrics.clone())
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
        assert!(HttpTransport::new(1000, 60_000, 300_000, 5_000, 10, false, metrics).is_ok());
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

    // ── S2: SSRF guard — address classification (every class) ────────────────

    /// A "private" (refused) target per the plan: v4 loopback / unspecified /
    /// link-local / RFC1918; v6 loopback / unspecified / unique-local (fc00::/7) /
    /// link-local (fe80::/10); plus v4-mapped and v4-compatible v6 forms
    /// classified by their embedded v4 address.
    fn private(s: &str) -> bool {
        is_private_target(s.parse().expect("valid ip literal for the test"))
    }

    #[test]
    fn classifier_blocks_v4_loopback_range() {
        assert!(private("127.0.0.1"));
        assert!(private("127.0.0.0"));
        assert!(private("127.255.255.254"), "the whole 127/8 is loopback");
    }

    #[test]
    fn classifier_blocks_v4_unspecified() {
        assert!(private("0.0.0.0"));
    }

    #[test]
    fn classifier_blocks_v4_link_local_169_254() {
        assert!(private("169.254.0.1"));
        assert!(private("169.254.169.254"), "the cloud metadata address");
        assert!(private("169.254.255.255"));
        assert!(!private("169.253.255.255"), "just below the /16");
        assert!(!private("169.255.0.1"), "just above the /16");
    }

    #[test]
    fn classifier_blocks_rfc1918() {
        // 10.0.0.0/8
        assert!(private("10.0.0.5"));
        assert!(private("10.255.255.255"));
        assert!(private("10.1.2.3"));
        // 172.16.0.0/12 — the bounds matter (is_private must cover 172.16–172.31)
        assert!(private("172.16.0.1"));
        assert!(private("172.31.255.255"));
        assert!(!private("172.15.255.255"), "just below the /12");
        assert!(!private("172.32.0.1"), "just above the /12");
        // 192.168.0.0/16
        assert!(private("192.168.0.1"));
        assert!(private("192.168.255.255"));
        assert!(!private("192.169.0.1"), "just above the /16");
    }

    #[test]
    fn classifier_allows_public_v4() {
        assert!(!private("8.8.8.8"));
        assert!(!private("1.1.1.1"));
        assert!(!private("203.0.113.10"), "TEST-NET is not RFC1918-private");
        assert!(!private("172.32.0.1"));
    }

    #[test]
    fn classifier_blocks_v6_loopback_and_unspecified() {
        assert!(private("::1"));
        assert!(private("::"), "unspecified v6");
    }

    #[test]
    fn classifier_blocks_v6_unique_local_fc00_slash_7() {
        assert!(private("fc00::1"));
        assert!(private("fd00::1"));
        assert!(private("fd12:3456:789a::1"));
        assert!(!private("fbff::1"), "just below fc00::/7");
        assert!(!private("fe00::1"), "just above fc00::/7");
    }

    #[test]
    fn classifier_blocks_v6_link_local_fe80_slash_10() {
        assert!(private("fe80::1"));
        assert!(private("fe80::ffff:ffff:ffff:ffff"));
        assert!(private("febf::1"), "top of fe80::/10");
        assert!(!private("fec0::1"), "just above fe80::/10 (old site-local)");
    }

    #[test]
    fn classifier_blocks_v4_mapped_v6_by_embedded_v4() {
        assert!(private("::ffff:10.0.0.5"), "mapped RFC1918");
        assert!(private("::ffff:127.0.0.1"), "mapped loopback");
        assert!(
            private("::ffff:169.254.169.254"),
            "mapped link-local/metadata"
        );
        assert!(private("::ffff:0.0.0.0"), "mapped unspecified");
        assert!(!private("::ffff:8.8.8.8"), "mapped public is allowed");
    }

    #[test]
    fn classifier_blocks_v4_compatible_v6_by_embedded_v4() {
        // The deprecated ::a.b.c.d form is still interpreted as the v4 address
        // by dual-stack sockets — classify it the same way (fail closed).
        assert!(private("::127.0.0.1"));
        assert!(private("::10.0.0.5"));
        assert!(!private("::8.8.8.8"));
    }

    #[test]
    fn classifier_allows_public_v6() {
        assert!(!private("2001:4860:4860::8888"));
        assert!(!private("2606:4700:4700::1111"));
    }

    /// Fix-round 1 (Important 2): 100.64.0.0/10 — RFC 6598 shared/CGNAT
    /// space. Tailscale and several k8s CNIs run real internal infrastructure
    /// there, exactly the threat model, so it must classify as private.
    #[test]
    fn classifier_blocks_cgnat_shared_100_64_slash_10() {
        assert!(!private("100.63.255.255"), "just below the /10");
        assert!(private("100.64.0.0"), "bottom of the /10");
        assert!(private("100.64.0.1"));
        assert!(private("100.100.100.100"));
        assert!(private("100.127.255.255"), "top of the /10");
        assert!(!private("100.128.0.1"), "just above the /10");
    }

    /// Fix-round 1 (authorized extra): multicast and broadcast targets are
    /// never legitimate webhook receivers — refuse them too. NAT64
    /// (64:ff9b::/96) is deliberately NOT classified here (ledgered only).
    #[test]
    fn classifier_blocks_multicast_and_broadcast() {
        // v4 multicast 224.0.0.0/4
        assert!(!private("223.255.255.255"), "just below 224/4");
        assert!(private("224.0.0.1"));
        assert!(private("239.255.255.255"), "top of 224/4");
        // 240.0.0.0/4 (class E reserved) is out of scope — stays public here.
        assert!(!private("240.0.0.1"));
        // v4 broadcast
        assert!(private("255.255.255.255"));
        // v6 multicast ff00::/8
        assert!(private("ff02::1"));
        assert!(private("ffff::1"), "top of ff00::/8");
        assert!(!private("fe00::1"), "below ff00::/8");
    }

    // ── S2: SSRF guard — the pre-flight over a mock resolver ──────────────────

    /// Mock [`Resolver`]: returns the same canned address list for every host.
    struct MockResolver {
        ips: Vec<IpAddr>,
    }

    #[async_trait]
    impl Resolver for MockResolver {
        async fn resolve(&self, _host: &str) -> Vec<IpAddr> {
            self.ips.clone()
        }
    }

    fn guard_with(ips: &[IpAddr], allow_private: bool) -> SsrfGuard {
        SsrfGuard {
            resolver: Arc::new(MockResolver { ips: ips.to_vec() }),
            allow_private,
        }
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[tokio::test]
    async fn preflight_refuses_when_any_resolved_ip_is_private() {
        let g = guard_with(&[ip("8.8.8.8"), ip("10.0.0.5")], false);
        let err = g.check("https://e.test/wh").await.unwrap_err();
        assert!(
            err.contains("10.0.0.5"),
            "the refusal must name the offending address: {err}"
        );
    }

    #[tokio::test]
    async fn preflight_pins_public_resolution() {
        let g = guard_with(&[ip("93.184.216.34"), ip("1.1.1.1")], false);
        let pin = g.check("https://e.test/wh").await.unwrap().unwrap();
        assert_eq!(pin.host, "e.test");
        assert_eq!(
            pin.addrs,
            vec![
                SocketAddr::new(ip("93.184.216.34"), 443),
                SocketAddr::new(ip("1.1.1.1"), 443),
            ]
        );
    }

    #[tokio::test]
    async fn preflight_uses_the_urls_explicit_port_in_the_pin() {
        let g = guard_with(&[ip("93.184.216.34")], false);
        let pin = g.check("http://e.test:9090/wh").await.unwrap().unwrap();
        assert_eq!(pin.addrs, vec![SocketAddr::new(ip("93.184.216.34"), 9090)]);
    }

    #[tokio::test]
    async fn preflight_pins_even_when_private_targets_are_allowed() {
        let g = guard_with(&[ip("10.0.0.5")], true);
        assert!(g.check("https://e.test/wh").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn preflight_refuses_when_resolution_is_empty() {
        let g = guard_with(&[], false);
        let err = g.check("https://nx.test/wh").await.unwrap_err();
        assert!(
            err.contains("nx.test") && err.to_lowercase().contains("resolve"),
            "empty resolution must refuse with the host named: {err}"
        );
    }

    #[tokio::test]
    async fn preflight_literal_ip_skips_resolution_but_classifies() {
        // Private literal, guard on → refused without consulting the resolver
        // (which would panic-fail the test if called for a literal… instead it
        // simply must not matter: classification is on the literal itself).
        let g = guard_with(&[ip("8.8.8.8")], false);
        let err = g.check("http://10.0.0.5/wh").await.unwrap_err();
        assert!(err.contains("10.0.0.5"), "{err}");
        // Public literal → allowed, no pin needed (the URL host IS the address).
        let out = g.check("http://93.184.216.34/wh").await.unwrap();
        assert!(out.is_none(), "literal IP needs no DNS pin");
        // v6 literal, private → refused.
        let err = g.check("http://[::1]/wh").await.unwrap_err();
        assert!(err.contains("::1"), "{err}");
        // v6 literal, public → allowed.
        assert!(g
            .check("http://[2001:4860:4860::8888]/wh")
            .await
            .unwrap()
            .is_none());
        // v4-mapped v6 literal → classified by the embedded v4.
        let err = g.check("http://[::ffff:10.0.0.5]/wh").await.unwrap_err();
        assert!(err.contains("10.0.0.5"), "mapped form: {err}");
    }

    #[tokio::test]
    async fn preflight_literal_private_allowed_when_flag_set() {
        let g = guard_with(&[], true);
        assert!(g.check("http://127.0.0.1:8080/wh").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn preflight_refuses_non_http_schemes() {
        let g = guard_with(&[ip("8.8.8.8")], true);
        for url in [
            "file:///etc/passwd",
            "ftp://e.test/wh",
            "gopher://e.test/wh",
            "unix:///var/run/sock",
        ] {
            let err = g.check(url).await.unwrap_err();
            assert!(
                err.to_lowercase().contains("scheme"),
                "{url} must be refused as a scheme violation: {err}"
            );
        }
    }

    // ── S2: SSRF guard — full delivery through the transport (mock sender) ────

    /// Build a transport with a counting mock sender AND a real guard over the
    /// mock resolver, so refusal can be pinned to "zero HTTP attempts".
    fn guarded_transport(
        respond: impl Fn(u32, &WebhookDelivery) -> Result<u16, String> + Send + Sync + 'static,
        ips: &[IpAddr],
        allow_private: bool,
        metrics: Arc<WebhookMetrics>,
    ) -> (HttpTransport, Arc<AtomicUsize>) {
        let m = mock(respond);
        let guard = SsrfGuard {
            resolver: Arc::new(MockResolver { ips: ips.to_vec() }),
            allow_private,
        };
        let t = HttpTransport::with_sender_and_guard(
            m.sender.clone(),
            Some(guard),
            0, // timeout placeholder: only used when a pin must build a client
            100,
            1_000,
            10_000,
            10,
            metrics,
        );
        (t, m.calls.clone())
    }

    /// Private resolution + guard on → refused: ZERO HTTP attempts, exactly one
    /// delivered_failed, resolved immediately (never consumes the retry budget).
    #[tokio::test(start_paused = true)]
    async fn private_target_refused_without_any_http_attempt() {
        let metrics = Arc::new(WebhookMetrics::new(64));
        let (t, calls) =
            guarded_transport(|_, _| Ok(200u16), &[ip("10.0.0.5")], false, metrics.clone());
        t.deliver(delivery_to("https://internal.test/wh")).await;
        for _ in 0..1_000 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "no HTTP attempt may happen"
        );
        assert_eq!(metrics.delivered_failed.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.delivered_ok.load(Ordering::Relaxed), 0);
        // And the clock never moved: refusal is immediate, not budget-bounded.
        assert_eq!(tokio::time::Instant::now().elapsed(), Duration::ZERO);
    }

    /// Public target + 2xx sender → delivered exactly once. Uses a LITERAL
    /// public IP URL (pre-flight `Ok(None)` → the shared counting sender runs;
    /// a hostname URL would pin a REAL reqwest client instead, which is what
    /// the integration tests exercise).
    #[tokio::test(start_paused = true)]
    async fn public_target_delivered() {
        let metrics = Arc::new(WebhookMetrics::new(64));
        let (t, calls) = guarded_transport(
            |_, _| Ok(200u16),
            &[ip("93.184.216.34")],
            false,
            metrics.clone(),
        );
        t.deliver(delivery_to("http://93.184.216.34/wh")).await;
        for _ in 0..1_000 {
            if metrics.delivered_ok.load(Ordering::Relaxed) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(metrics.delivered_ok.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.delivered_failed.load(Ordering::Relaxed), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// Private LITERAL target + allow-flag → classification is bypassed and
    /// the delivery proceeds through the shared sender (2xx → ok).
    #[tokio::test(start_paused = true)]
    async fn private_target_delivered_when_allow_flag_set() {
        let metrics = Arc::new(WebhookMetrics::new(64));
        let (t, calls) =
            guarded_transport(|_, _| Ok(200u16), &[ip("10.0.0.5")], true, metrics.clone());
        t.deliver(delivery_to("http://10.0.0.5/wh")).await;
        for _ in 0..1_000 {
            if metrics.delivered_ok.load(Ordering::Relaxed) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(metrics.delivered_ok.load(Ordering::Relaxed), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// A refused scheme (file://) fails fast the same way — zero attempts.
    #[tokio::test(start_paused = true)]
    async fn file_scheme_refused_without_any_http_attempt() {
        let metrics = Arc::new(WebhookMetrics::new(64));
        let (t, calls) =
            guarded_transport(|_, _| Ok(200u16), &[ip("8.8.8.8")], true, metrics.clone());
        t.deliver(delivery_to("file:///etc/passwd")).await;
        for _ in 0..1_000 {
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(metrics.delivered_failed.load(Ordering::Relaxed), 1);
    }
}
