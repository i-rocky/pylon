//! Router assembly + shared application state.

use crate::adapter::Adapter;
use crate::app::AppManager;
use crate::server::config::ServerConfig;
use axum::routing::get;
use axum::Router;
use dashmap::DashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub config: ServerConfig,
    pub apps: Arc<dyn AppManager>,
    pub adapter: Arc<dyn Adapter>,
    pub conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
}

pub fn build_router(state: AppState) -> Router {
    let router = Router::new()
        .route("/", get(crate::http::root))
        .route("/app/{key}", get(crate::ws::upgrade::upgrade));
    crate::http::rest::merge(router).with_state(state)
}
