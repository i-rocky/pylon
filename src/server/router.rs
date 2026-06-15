//! Router assembly + shared application state.

use crate::adapter::Adapter;
use crate::app::AppManager;
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
    /// sink (a clone of its flag); `None` when there's no concrete local adapter
    /// (the redis+percore fallback) or in tests, where [`AppState::is_saturated`]
    /// is always `false` so the 503 gate is a no-op.
    pub saturated: Option<Arc<AtomicBool>>,
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
    crate::http::rest::merge(router, body_limit).with_state(state)
}
