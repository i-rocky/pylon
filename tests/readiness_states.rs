//! The three `/ready` verdicts, driven at the handler.
//!
//! `tests/health.rs` covers the READY answer on a live percore fleet. The other
//! two verdicts need the opposite: a process where NO fleet has ever started, so
//! `percore_metrics_snapshot()` is `None`. That cannot be arranged inside a
//! binary that spawns a fleet (the registry is a process-global, installed once
//! and never removed), which is why these live in their own test crate — and why
//! each test asserts that precondition rather than assuming it.
//!
//! The distinction matters operationally: `starting` and `draining` are both
//! 503, but a load balancer reads the body to tell "not up yet, keep waiting"
//! from "going away, stop sending traffic".

use axum::extract::State;
use axum::http::StatusCode;
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::http::rest::health::{get_health, get_ready};
use pylon::server::config::ServerConfig;
use pylon::server::router::AppState;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// An `AppState` with no percore fleet behind it, whose `draining` flag the
/// caller keeps a handle on.
fn state() -> (AppState, Arc<AtomicBool>) {
    let draining = Arc::new(AtomicBool::new(false));
    let apps: Arc<dyn AppManager> = Arc::new(
        StaticFileAppManager::from_json(r#"[{"name":"T","id":"app1","key":"k","secret":"s"}]"#)
            .expect("apps json must parse"),
    );
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(
        Arc::new(Registry::new()),
        Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
    ));
    (
        AppState {
            config: ServerConfig::default(),
            apps,
            adapter,
            conn_counts: Arc::new(Default::default()),
            webhooks: pylon::webhook::WebhookHandle::null(),
            saturated: None,
            draining: draining.clone(),
            cluster_metrics: None,
            invalidator: None,
        },
        draining,
    )
}

/// This crate must never start a percore fleet, or the "no fleet" precondition
/// below is silently false and both tests pass for the wrong reason.
fn assert_no_fleet() {
    assert!(
        pylon::transport::percore_metrics_snapshot().is_none(),
        "these tests require a process with no percore fleet; one has been started"
    );
}

/// Before the fleet reports its first snapshot the node is still initialising:
/// 503 `starting`, so an orchestrator waits rather than routing traffic here.
#[tokio::test]
async fn ready_reports_starting_while_the_fleet_has_not_reported() {
    assert_no_fleet();
    let (state, _draining) = state();
    assert_eq!(
        get_ready(State(state)).await,
        (StatusCode::SERVICE_UNAVAILABLE, "starting")
    );
}

/// Once the shutdown signal fires the node is going away: 503 `draining`, which
/// tells the load balancer to stop sending NEW connections while the existing
/// ones are closed gracefully. Draining wins over every other input.
#[tokio::test]
async fn ready_reports_draining_once_the_shutdown_signal_fires() {
    assert_no_fleet();
    let (state, draining) = state();
    draining.store(true, Ordering::Relaxed);
    assert_eq!(
        get_ready(State(state)).await,
        (StatusCode::SERVICE_UNAVAILABLE, "draining"),
        "a draining node must be distinguishable from one that is merely starting"
    );
}

/// Liveness is not readiness: a process that can answer HTTP is alive even while
/// it is starting or draining, so `/health` stays 200 throughout.
#[tokio::test]
async fn health_stays_200_while_the_node_is_not_ready() {
    assert_no_fleet();
    assert_eq!(get_health().await, (StatusCode::OK, "ok"));
}
