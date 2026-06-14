//! pylon binary entrypoint.

use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::server::config::{ServerConfig, TransportMode};
use pylon::server::router::{build_router, AppState};
use pylon::server::shutdown::shutdown_signal;
use pylon::webhook::dispatcher::SystemClock;
use pylon::webhook::transport::{HttpTransport, WebhookTransport};
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pylon::init_tracing();
    let config = ServerConfig::from_env();
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_file(&config.apps_path)?);
    let is_redis = config.adapter == "redis";
    // Keep the concrete `Arc<RedisAdapter>` so we can start the sweeper AFTER the
    // webhook dispatcher exists (the sweeper needs the `WebhookHandle`; the dispatcher
    // needs the adapter-backed occupancy source — start_sweeper breaks that cycle).
    let redis: Option<Arc<pylon::adapter::redis::RedisAdapter>> = if is_redis {
        Some(Arc::new(
            pylon::adapter::redis::RedisAdapter::new(&config).await?,
        ))
    } else {
        None
    };
    let adapter: Arc<dyn Adapter> = match &redis {
        Some(r) => r.clone(),
        None => Arc::new(LocalAdapter::new(Arc::new(Registry::new()))),
    };

    // Webhook dispatcher: real HTTP transport (reqwest+rustls), system clock.
    let transport: Arc<dyn WebhookTransport> = Arc::new(HttpTransport::new(
        config.webhook_max_retries,
        config.webhook_retry_base_ms,
        config.webhook_timeout_ms,
        config.webhook_max_concurrency,
    ));
    // Cluster-aware `channel_vacated` grace window (Task D1): only the Redis
    // (multi-node) path debounces+rechecks vacated. The local path fires
    // immediately (grace = 0, no occupancy source).
    let (vacated_grace_ms, occupancy): (u64, Option<Arc<dyn pylon::webhook::OccupancySource>>) =
        if is_redis {
            (
                config.webhook_vacated_grace_ms,
                Some(Arc::new(pylon::webhook::AdapterOccupancy(adapter.clone()))),
            )
        } else {
            (0, None)
        };
    let webhooks = pylon::webhook::spawn(
        apps.clone(),
        transport,
        Arc::new(SystemClock),
        config.webhook_batch_ms,
        // Generously sized mailbox (the §8 backpressure safety valve).
        config.webhook_max_concurrency.saturating_mul(100).max(1024),
        vacated_grace_ms,
        occupancy,
    );

    // Now that the webhook handle exists, start the Redis sweeper with the SAME
    // handle AppState uses, so vacated webhooks from sweeps and from WS-driven
    // unsubscribes share one dispatcher (grace + cluster re-check).
    if let Some(r) = &redis {
        r.start_sweeper(webhooks.clone());
    }

    // Shared connection counters, used by both transports (axum `AppState` and
    // the percore `DispatchEnv` mirror this type exactly).
    let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());

    match config.transport {
        TransportMode::Legacy => {
            let state = AppState {
                config: config.clone(),
                apps,
                adapter,
                conn_counts,
                webhooks,
            };
            let listener =
                tokio::net::TcpListener::bind((config.bind.as_str(), config.port)).await?;
            tracing::info!("pylon listening on {}:{}", config.bind, config.port);
            axum::serve(listener, build_router(state))
                .with_graceful_shutdown(shutdown_signal())
                .await?;
        }
        TransportMode::Percore => {
            // The percore worker is a blocking `mio` loop; run it on a dedicated
            // blocking thread and flip the shared shutdown flag when the signal
            // future resolves. Webhooks/adapter background tasks (e.g. the Redis
            // sweeper) were already spawned on this tokio runtime above and keep
            // running independently of the worker thread.
            let shutdown = Arc::new(AtomicBool::new(false));
            let worker_shutdown = shutdown.clone();
            let worker_config = config.clone();
            let worker = tokio::task::spawn_blocking(move || {
                pylon::transport::run_percore(
                    worker_config,
                    apps,
                    adapter,
                    conn_counts,
                    webhooks,
                    worker_shutdown,
                )
            });

            // Wait for Ctrl-C / SIGTERM, then signal the worker to stop and join.
            shutdown_signal().await;
            shutdown.store(true, Ordering::SeqCst);
            worker.await??;
        }
    }
    Ok(())
}
