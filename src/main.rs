//! pylon binary entrypoint.

use dashmap::DashMap;
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::server::config::AppManagerKind;
use pylon::channel::registry::Registry;
use pylon::cluster::adapter::ClusterAdapter;
use pylon::server::config::ServerConfig;
use pylon::server::router::{build_router, AppState};
use pylon::server::shutdown::shutdown_signal;
use pylon::webhook::dispatcher::SystemClock;
use pylon::webhook::transport::{HttpTransport, WebhookTransport};
use pylon::webhook::{OccupancySource, WebhookHandle};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[cfg(not(feature = "dhat-heap"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// Heap-profiling build (`--features dhat-heap`): dhat replaces the allocator so it
// can attribute every allocation to its call site. Writes dhat-heap.json on exit.
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static GLOBAL: dhat::Alloc = dhat::Alloc;

/// Spawn the webhook dispatcher with the production HTTP transport + system clock.
/// `vacated_grace_ms` + `occupancy` enable the cluster-aware `channel_vacated` grace
/// window (only the multi-node Redis paths pass them; the local paths pass `0` / `None`).
/// Shared by every transport/adapter combination so the dispatcher is built identically.
fn spawn_webhooks(
    config: &ServerConfig,
    apps: Arc<dyn AppManager>,
    vacated_grace_ms: u64,
    occupancy: Option<Arc<dyn OccupancySource>>,
) -> WebhookHandle {
    let max_retries = config.webhook_max_retries;
    let retry_base_ms = config.webhook_retry_base_ms;
    let timeout_ms = config.webhook_timeout_ms;
    let max_concurrency = config.webhook_max_concurrency;
    pylon::webhook::spawn(
        apps,
        move |metrics| {
            Arc::new(HttpTransport::new(
                max_retries,
                retry_base_ms,
                timeout_ms,
                max_concurrency,
                metrics,
            )) as Arc<dyn WebhookTransport>
        },
        Arc::new(SystemClock),
        config.webhook_batch_ms,
        // Generously sized mailbox (the §8 backpressure safety valve).
        config.webhook_max_concurrency.saturating_mul(100).max(1024),
        vacated_grace_ms,
        occupancy,
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Heap profiler guard: lives for the whole process; its Drop (after the worker
    // joins on shutdown) writes dhat-heap.json. The gmax snapshot inside captures the
    // heap at peak — i.e. while the connections were held.
    #[cfg(feature = "dhat-heap")]
    let _dhat = dhat::Profiler::new_heap();
    pylon::init_tracing();
    let config = ServerConfig::from_env();
    let apps: Arc<dyn AppManager> = match config.app_manager {
        AppManagerKind::StaticFile => Arc::new(StaticFileAppManager::from_file(&config.apps_path)?),
        AppManagerKind::Sqlite | AppManagerKind::Mysql | AppManagerKind::Postgres => {
            let dsn = config.app_dsn.clone()
                .ok_or_else(|| anyhow::anyhow!("PYLON_APP_MANAGER=sqlite|mysql|postgres requires PYLON_APP_DSN"))?;
            Arc::new(pylon::app::sql::SqlAppManager::connect(&dsn).await?)
        }
        AppManagerKind::Mongo => {
            let dsn = config.app_dsn.clone()
                .ok_or_else(|| anyhow::anyhow!("PYLON_APP_MANAGER=mongo requires PYLON_APP_DSN (mongodb://host/db)"))?;
            Arc::new(pylon::app::mongo::MongoAppManager::connect(&dsn).await?)
        }
    };
    let (apps, caching_for_purge, uncached_driver) = if config.app_cache && config.app_manager != AppManagerKind::StaticFile {
        use pylon::app::cache::{CacheConfig, CachingAppManager};
        let l2 = match &config.app_cache_redis_url {
            Some(url) => Some(Arc::new(pylon::app::l2::RedisAppCache::connect(url, 4, config.app_cache_ttl).await?)),
            None => None,
        };
        let cfg = CacheConfig {
            max_capacity: config.app_cache_max, ttl_secs: config.app_cache_ttl,
            neg_max: config.app_cache_neg_max, neg_ttl_secs: config.app_cache_neg_ttl,
        };
        let uncached_driver: Arc<dyn AppManager> = apps.clone();
        let caching = Arc::new(CachingAppManager::new(apps, cfg, l2));
        let apps_erased: Arc<dyn AppManager> = caching.clone();
        (apps_erased, Some(caching), Some(uncached_driver))
    } else {
        (apps, None, None)
    };

    // The redis adapter is the CLUSTERED production path: the node's single
    // `RedisAdapter` is owned by a `ClusterBridge` (on its own runtime), the percore workers
    // drive a `ClusterAdapter` over the shared `LocalAdapter` + the bridge handle, and each
    // connection defers its single-emit cluster edges to the bridge. It has its own startup
    // ordering (the webhook dispatcher's occupancy source reads through the bridge's adapter,
    // so webhooks are attached AFTER the bridge is up), so it runs in a dedicated function.
    // The local (single-node) path keeps the straight-line wiring below.
    if config.adapter == "redis" {
        return run_redis_percore(config, apps, caching_for_purge, uncached_driver).await;
    }

    // Per-app connection index shared with `LocalAdapter::purge_app` (Phase 6),
    // created once and threaded into both the worker fleet and the adapter.
    let app_registry: Arc<pylon::adapter::app_registry::AppRegistry> =
        Arc::new(pylon::adapter::app_registry::AppRegistry::new());

    // The CONCRETE local adapter, so the percore transport can install its sharded
    // broadcast sink on it.
    let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new()), app_registry.clone()));
    let adapter: Arc<dyn Adapter> = local.clone();

    // The single-node local path fires `channel_vacated` immediately (no grace
    // window, no occupancy source — those are the Redis multi-node path's, handled
    // in `run_redis_percore`).
    let webhooks = spawn_webhooks(&config, apps.clone(), 0, None);

    // Shared connection counters (the axum REST `AppState` and the percore
    // `DispatchEnv` mirror this type exactly).
    let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());
    // Node-level live connection counter for the ceiling check — one per process,
    // shared across all of this node's workers. Created alongside `conn_counts`.
    let node_conns: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

    // Build the AppPurger + invalidation subscriber once the adapter + conn_counts
    // exist. Only for DB-backed cache paths with an L2/pub-sub Redis URL.
    let invalidator = match (&caching_for_purge, &config.app_cache_redis_url) {
        (Some(cache), Some(url)) => {
            let purger = Arc::new(pylon::app::purger::AppPurger::new(
                adapter.clone(),
                conn_counts.clone(),
                cache.clone(),
            ));
            Some(pylon::app::invalidation::AppInvalidator::spawn(url, purger).await?)
        }
        _ => None,
    };

    // App-purge sweep backstop: DB-backed cache path only, gated by the interval.
    if let (Some(cache), Some(driver)) = (&caching_for_purge, &uncached_driver) {
        if config.app_sweep_interval_secs > 0 {
            let purger = Arc::new(pylon::app::purger::AppPurger::new(
                adapter.clone(),
                conn_counts.clone(),
                cache.clone(),
            ));
            pylon::app::purger::spawn_sweep(
                config.app_sweep_interval_secs,
                app_registry.clone(),
                driver.clone(),
                cache.clone(),
                purger,
            );
        }
    }

    // The percore worker is a blocking `mio` loop; run it on a dedicated blocking
    // thread and flip the shared shutdown flag when the signal future resolves.
    // Webhooks/adapter background tasks (e.g. the Redis sweeper) were already
    // spawned on this tokio runtime above and keep running independently of the
    // worker thread.
    //
    // REST handoff (SP9 §3.4): the worker accepts every connection but can only
    // drive WS itself. It transfers plain-HTTP (Pusher REST publish) connections
    // over `rest_tx` to a tokio task that serves them with the axum `Router`. The
    // task is spawned HERE, on the runtime, BEFORE the blocking worker call — so a
    // runtime handle exists to serve the handed-off fds.
    let (rest_tx, rest_rx) = tokio::sync::mpsc::unbounded_channel::<pylon::transport::RestConn>();
    // C2b: shared draining flag (always false at startup; set true by the shutdown
    // sequence in C2a). Created here so it lives for the entire server lifetime and
    // can be cloned into AppState and the future shutdown sequence.
    let draining = Arc::new(AtomicBool::new(false));
    // Clone for use in the shutdown sequence — the original is moved into AppState.
    let draining_for_shutdown = draining.clone();
    let rest_state = AppState {
        config: config.clone(),
        apps: apps.clone(),
        adapter: adapter.clone(),
        conn_counts: conn_counts.clone(),
        webhooks: webhooks.clone(),
        // SP10: the REST 503 admission gate reads the percore saturation flag (the
        // LocalAdapter's, shared with the sink).
        saturated: Some(local.saturation_flag()),
        draining,
        cluster_metrics: None,
        invalidator: invalidator.clone(),
    };
    let rest_router = build_router(rest_state);
    tokio::spawn(pylon::transport::rest::serve(rest_rx, rest_router));

    let local_for_sink = Some(local.clone());

    let tls = pylon::transport::tls::resolve_tls(
        &config.tls_cert_path,
        &config.tls_key_path,
        &config.tls_ca_path,
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    if tls.is_some() {
        tracing::info!(cert = ?config.tls_cert_path, "TLS enabled");
    } else {
        tracing::info!("TLS disabled (plain mode)");
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = shutdown.clone();
    let worker_config = config.clone();
    // Phase 7: capture the runtime handle in THIS async context (inside the tokio
    // runtime) BEFORE `spawn_blocking` (where `Handle::try_current()` is `Err`).
    let worker_runtime = tokio::runtime::Handle::current();
    let worker = tokio::task::spawn_blocking(move || {
        pylon::transport::run_percore(
            worker_config,
            apps,
            adapter,
            conn_counts,
            app_registry,
            node_conns,
            webhooks,
            Some(rest_tx),
            worker_shutdown,
            local_for_sink,
            // The dedicated cluster bridge (SP11 §3.6) wires `clustered: true` via
            // its own harness (`run_redis_percore`); this standalone `main` percore
            // path is not clustered.
            false,
            tls,
            worker_runtime,
        )
    });

    // C2a two-phase graceful shutdown:
    //   1. Set draining=true  → /ready returns 503; LBs stop sending new traffic.
    //   2. Sleep predrain_ms  → allow LBs to observe the 503.
    //   3. Set shutdown=true  → workers deregister listeners, queue Close(1001),
    //      flush in-flight bytes, run on_close cleanup, then exit.
    //   4. Join the worker.
    shutdown_signal().await;
    draining_for_shutdown.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(config.shutdown_predrain_ms)).await;
    shutdown.store(true, Ordering::SeqCst);
    worker.await??;
    Ok(())
}

/// The CLUSTERED production path: Redis adapter + percore transport. Mirrors the test
/// harness `tests/common/mod.rs::spawn_percore_cluster` exactly, but for the real binary
/// (bound to `config.bind`/`config.port`, with graceful shutdown).
///
/// One shared `LocalAdapter` `local` is shared by (a) the node's [`ClusterBridge`], which
/// owns the node's single `RedisAdapter` (built with `with_local`, on its own runtime, so
/// its pub/sub recv loop shards remote frames through `local`'s broadcast sink), (b) the
/// REST plane's [`AppState`] (driving that `RedisAdapter` for cluster-wide reads/publishes),
/// and (c) the percore worker fleet's sharded broadcast sink (installed by `run_percore`
/// when `local` is `Some`). The worker drives a [`ClusterAdapter`] = `{ local, bridge
/// handle }` and runs with `clustered = true`, so each connection's handler defers the
/// single-emit cluster edges (count / occupied / vacated / member_added/removed / cache
/// replay) to the bridge.
///
/// Startup ordering breaks the webhooks→adapter→bridge→webhooks cycle: the bridge is started
/// FIRST (building the `RedisAdapter`), THEN the webhook dispatcher is built with an
/// `AdapterOccupancy` over `bridge.adapter()`, and only THEN `bridge.attach_webhooks` wires
/// the deferred handle into the drain loop and starts the Redis sweeper. The bridge is held
/// alive until after the worker joins; its `Drop` tears down the dedicated Redis runtime.
async fn run_redis_percore(
    config: ServerConfig,
    apps: Arc<dyn AppManager>,
    caching_for_purge: Option<Arc<pylon::app::cache::CachingAppManager>>,
    uncached_driver: Option<Arc<dyn AppManager>>,
) -> anyhow::Result<()> {
    // Per-app connection index shared with `LocalAdapter::purge_app` (Phase 6),
    // created once and threaded into both the worker fleet and the adapter.
    let app_registry: Arc<pylon::adapter::app_registry::AppRegistry> =
        Arc::new(pylon::adapter::app_registry::AppRegistry::new());

    // The single shared LocalAdapter: the bridge's RedisAdapter shares it (so its recv
    // loop's `local.broadcast(Raw)` shards remote frames to this node's workers), the REST
    // plane reads the saturation flag off it, and the worker's ClusterAdapter + the sharded
    // sink install on it.
    let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new()), app_registry.clone()));

    // Start the bridge: builds the node's single `RedisAdapter` (sharing `local`) on its own
    // runtime and returns once Redis is connected, or `Err` if the connect failed.
    let bridge = pylon::cluster::bridge::start(&config, local.clone(), apps.clone())?;

    // REST + occupancy drive the node's single `RedisAdapter` through the bridge.
    let adapter: Arc<dyn Adapter> = bridge.adapter();

    // Webhook dispatcher with the cluster-aware `channel_vacated` grace window (Task D1),
    // same as the redis-legacy path: debounce + re-check the cluster subscription_count
    // (via `AdapterOccupancy` over the RedisAdapter) before firing a surviving vacated.
    let occupancy: Option<Arc<dyn OccupancySource>> =
        Some(Arc::new(pylon::webhook::AdapterOccupancy(adapter.clone())));
    let webhooks = spawn_webhooks(
        &config,
        apps.clone(),
        config.webhook_vacated_grace_ms,
        occupancy,
    );

    // Now the dispatcher exists: wire its handle into the bridge's drain loop AND start the
    // Redis sweeper with the SAME handle (so sweep-driven and command-driven vacated
    // webhooks share one dispatcher). This closes the startup cycle.
    bridge.attach_webhooks(webhooks.clone());

    let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());
    // Node-level live connection counter for the ceiling check — one per process,
    // shared across all of this node's workers. Created alongside `conn_counts`.
    let node_conns: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

    // Build the AppPurger + invalidation subscriber once the adapter + conn_counts
    // exist. Only for DB-backed cache paths with an L2/pub-sub Redis URL.
    let invalidator = match (&caching_for_purge, &config.app_cache_redis_url) {
        (Some(cache), Some(url)) => {
            let purger = Arc::new(pylon::app::purger::AppPurger::new(
                adapter.clone(),
                conn_counts.clone(),
                cache.clone(),
            ));
            Some(pylon::app::invalidation::AppInvalidator::spawn(url, purger).await?)
        }
        _ => None,
    };

    // App-purge sweep backstop: DB-backed cache path only, gated by the interval.
    if let (Some(cache), Some(driver)) = (&caching_for_purge, &uncached_driver) {
        if config.app_sweep_interval_secs > 0 {
            let purger = Arc::new(pylon::app::purger::AppPurger::new(
                adapter.clone(),
                conn_counts.clone(),
                cache.clone(),
            ));
            pylon::app::purger::spawn_sweep(
                config.app_sweep_interval_secs,
                app_registry.clone(),
                driver.clone(),
                cache.clone(),
                purger,
            );
        }
    }

    // REST handoff plane: the worker hands plain-HTTP connections to this axum router via
    // `rest_tx`; `rest::serve` drives them on the tokio runtime. The REST `AppState` drives
    // the node's `RedisAdapter` (cluster-wide reads/publishes) and reads the percore
    // saturation flag off the shared `local`.
    let (rest_tx, rest_rx) = tokio::sync::mpsc::unbounded_channel::<pylon::transport::RestConn>();
    // C2b: shared draining flag (always false at startup; set true by the shutdown
    // sequence in C2a). Created here so it lives for the entire server lifetime and
    // can be cloned into AppState and the future shutdown sequence.
    let draining = Arc::new(AtomicBool::new(false));
    // Clone for use in the shutdown sequence — the original is moved into AppState.
    let draining_for_shutdown = draining.clone();
    let rest_state = AppState {
        config: config.clone(),
        apps: apps.clone(),
        adapter: adapter.clone(),
        conn_counts: conn_counts.clone(),
        webhooks: webhooks.clone(),
        saturated: Some(local.saturation_flag()),
        draining,
        cluster_metrics: Some(bridge.metrics()),
        invalidator,
    };
    tokio::spawn(pylon::transport::rest::serve(
        rest_rx,
        build_router(rest_state),
    ));

    // The percore worker drives a `ClusterAdapter` over the shared `local` + the bridge
    // handle: node-local subscribes are synchronous, cross-node edges are fired (never
    // awaited) at the bridge. With `Some(local)` the sharded sink installs on the SAME
    // `local` the bridge's RedisAdapter holds, so cross-node received frames shard to this
    // worker. `clustered = true` flips each connection into deferred single-emit mode.
    let worker_adapter: Arc<dyn Adapter> =
        Arc::new(ClusterAdapter::new(local.clone(), bridge.handle()));

    let tls = pylon::transport::tls::resolve_tls(
        &config.tls_cert_path,
        &config.tls_key_path,
        &config.tls_ca_path,
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    if tls.is_some() {
        tracing::info!(cert = ?config.tls_cert_path, "TLS enabled (redis/cluster mode)");
    } else {
        tracing::info!("TLS disabled (plain mode, redis/cluster)");
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = shutdown.clone();
    let worker_config = config.clone();
    let worker_local = local.clone();
    // Phase 7: capture the runtime handle here (async context) before spawn_blocking.
    let worker_runtime = tokio::runtime::Handle::current();
    let worker = tokio::task::spawn_blocking(move || {
        pylon::transport::run_percore(
            worker_config,
            apps,
            worker_adapter,
            conn_counts,
            app_registry,
            node_conns,
            webhooks,
            Some(rest_tx),
            worker_shutdown,
            Some(worker_local),
            // This IS a clustered node: defer the single-emit cluster edges.
            true,
            tls,
            worker_runtime,
        )
    });

    // C2a two-phase graceful shutdown (same sequence as main()):
    //   1. Set draining=true  → /ready returns 503; LBs stop sending new traffic.
    //   2. Sleep predrain_ms  → allow LBs to observe the 503.
    //   3. Set shutdown=true  → workers drain + close connections, then exit.
    //   4. Join the worker. `bridge` stays in scope until AFTER the join so its
    //      Drop (which tears down the dedicated Redis runtime) runs only once the
    //      worker has stopped firing commands at it.
    shutdown_signal().await;
    draining_for_shutdown.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(config.shutdown_predrain_ms)).await;
    shutdown.store(true, Ordering::SeqCst);
    worker.await??;
    drop(bridge);
    Ok(())
}
