//! Router assembly + shared application state.

use crate::adapter::Adapter;
use crate::app::AppManager;
use crate::cluster::bridge::ClusterMetrics;
use crate::server::config::ServerConfig;
use crate::webhook::WebhookHandle;
use axum::routing::get;
use axum::Router;
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub config: ServerConfig,
    pub apps: Arc<dyn AppManager>,
    pub adapter: Arc<dyn Adapter>,
    pub conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
    pub webhooks: WebhookHandle,
    /// SP10 admission control: the percore broadcast pipeline's saturation flag,
    /// threaded as a side channel (NOT via the `Adapter` trait, which stays
    /// unchanged). `Some` whenever a concrete `LocalAdapter` backs the broadcast
    /// sink (a clone of its flag).
    ///
    /// X2 — latent trap: `None` **disables the saturation gate entirely**
    /// ([`AppState::is_saturated`] is then always `false`, so the REST 503
    /// admission gate and the WS client-event ingress drop are silent no-ops).
    /// Both production paths in `main.rs` (standalone local and clustered
    /// redis+percore) pass `Some(local.saturation_flag())`; `None` appears only
    /// in tests. Do not wire a production `AppState` without the flag.
    pub saturated: Option<Arc<AtomicBool>>,
    /// C2b graceful-shutdown draining flag. Set to `true` by the C2a two-phase
    /// shutdown sequence in `main.rs`. The `/ready` handler returns 503 while draining
    /// so load balancers stop routing new connections before we close existing ones.
    /// Always `false` at startup; the flag is only toggled by the shutdown sequence.
    pub draining: Arc<AtomicBool>,
    /// Phase-2 cluster metrics (B3): present on the clustered Redis path, absent
    /// (`None`) on the local single-node path. The `/metrics` handler emits
    /// `pylon_cluster_cmd_dropped_total` and `pylon_redis_connected` only when `Some`.
    pub cluster_metrics: Option<Arc<ClusterMetrics>>,
    /// Phase-5 app-cache invalidation handle: present when `PYLON_APP_CACHE_REDIS_URL`
    /// is set and caching is enabled, `None` otherwise. Required by the admin
    /// `POST /admin/apps/{id}/invalidate` endpoint to publish cross-node evictions.
    pub invalidator: Option<Arc<crate::app::invalidation::AppInvalidator>>,
}

impl AppState {
    /// Cheap admission-control check: is the publish pipeline saturated? With no
    /// saturation flag wired (`saturated == None`) this is always `false`, so the
    /// REST 503 gate and the WS client-event drop are no-ops.
    pub fn is_saturated(&self) -> bool {
        self.saturated
            .as_ref()
            .is_some_and(|s| s.load(std::sync::atomic::Ordering::Relaxed))
    }
}

/// R10: router-level fallback for unmatched paths — trailing-slash variants of
/// real routes (`/apps/{id}/events/`; axum does not treat `/foo/` as `/foo`),
/// unknown paths (`/nope`), anything no route matches. Without it axum answers
/// with its default EMPTY 404; render the Pusher JSON error shape
/// (`{"error":"Not found","status":404}`) via [`RestError`] instead, so every
/// error the REST plane emits is machine-parseable by the official SDKs.
///
/// Scope notes:
/// * This fires BEFORE any handler — and thus before auth — so an unsigned
///   request to an unknown path still gets the JSON 404.
/// * Handler-level 404s (e.g. the admin API's disabled 404) are unaffected:
///   they render the same shape through `RestError::not_found` on their own.
/// * The WS plane is unaffected: the per-core worker answers WS upgrades
///   (including bad-path 4005 rejects) before the REST handoff, so those never
///   reach this router.
async fn not_found_fallback() -> crate::http::error::RestError {
    crate::http::error::RestError::not_found("Not found")
}

/// R10: a matched path with an unsupported METHOD renders the same JSON shape
/// with 405 (`{"error":"Method not allowed","status":405}`). A method mismatch
/// does NOT flow through the router fallback — axum's `MethodRouter` answers it
/// — so this is wired via `Router::method_not_allowed_fallback`, which applies
/// one handler to every registered route (axum 0.8; it only touches routes
/// added BEFORE the call, hence the position after `merge`). It replaces each
/// `MethodRouter`'s DEFAULT 405 (empty body) without touching valid-method
/// routing. Pusher's docs say nothing about wrong-method bodies; the wording
/// follows the "every REST error is JSON" bar (Task 2.2's class).
async fn method_not_allowed_fallback() -> crate::http::error::RestError {
    crate::http::error::RestError::method_not_allowed("Method not allowed")
}

pub fn build_router(state: AppState) -> Router {
    // Cap the REST request body to what the configured limits can legitimately
    // produce (a full batch of max-size events) plus headroom for JSON framing,
    // so the body limit tracks the operator's configured limits rather than a
    // fixed magic number.
    let body_limit = state
        .config
        .max_batch_events
        .saturating_mul(state.config.max_event_payload_bytes)
        .saturating_add(64 * 1024);
    let router = Router::new().route("/", get(crate::http::root));
    crate::http::rest::merge(router, body_limit)
        .fallback(not_found_fallback)
        .method_not_allowed_fallback(method_not_allowed_fallback)
        .with_state(state)
}
