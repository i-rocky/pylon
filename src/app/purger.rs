//! `AppPurger`: the end-to-end "make a removed app leave zero footprint" action.
//! Driven by the `remove` invalidation message and the sweep backstop. `refresh`
//! is the config/secret-change action (cache eviction only); `purge` is the
//! remove/disable action (close conns + reclaim counter + evict cache).

use crate::adapter::app_registry::AppRegistry;
use crate::adapter::Adapter;
use crate::app::cache::CachingAppManager;
use crate::app::AppManager;
use dashmap::DashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

/// Owns the three structures a remove must reclaim node-locally. Built once in
/// `main.rs` (DB-backed cache paths only) and shared by the invalidation
/// subscriber and the sweep backstop.
pub struct AppPurger {
    adapter: Arc<dyn Adapter>,
    conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
    cache: Arc<CachingAppManager>,
}

impl AppPurger {
    pub fn new(
        adapter: Arc<dyn Adapter>,
        conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
        cache: Arc<CachingAppManager>,
    ) -> Self {
        Self { adapter, conn_counts, cache }
    }

    /// Config/secret changed: evict the cache only (TTL backstop bounds staleness;
    /// the next lookup re-fetches). Connections stay up.
    pub async fn refresh(&self, id: &str, key: &str) {
        self.cache.invalidate(id, key).await;
    }

    /// App removed/disabled: evict the cache FIRST, then force-close every
    /// connection (4009) and reclaim the per-app counter. In cluster mode the
    /// composing `RedisAdapter::purge_app` already SREM'd `{prefix}:apps`.
    ///
    /// Cache-first ordering is deliberate: once the L1/L2 entry is gone, any new
    /// connect for this app re-fetches the (uncached) driver — which returns
    /// `Ok(None)` for a removed/disabled app — and is rejected at `by_key` rather
    /// than establishing against a stale-positive cache entry and surviving the
    /// drain. The only residual window is a connect that already passed `by_key`
    /// (cache hit) but registers in `AppRegistry` after the drain; that bounded
    /// straggler is the sweep backstop's + TTL's job, by design.
    pub async fn purge(&self, id: &str, key: &str) {
        self.cache.invalidate(id, key).await;
        self.adapter.purge_app(id).await;
        self.conn_counts.remove(id);
    }
}

/// One sweep pass: for each distinct connected app, query the AUTHORITATIVE
/// (uncached) driver by id. `Ok(None)` ⇒ the app is gone/disabled ⇒ purge it.
/// `Ok(Some)` ⇒ live, skip. `Err` ⇒ backend trouble, LOG + skip (an outage must
/// never evict a live app). Returns the number of apps purged this pass.
///
/// The app key needed for cache eviction is resolved from the still-warm cache
/// (`cache.by_id`) BEFORE purging; if the cache no longer holds it the id-keyed
/// L1/L2 eviction still runs and the key alias falls back to TTL. The `cache`
/// lookup here is the only place the key is recoverable for an already-removed app.
pub(crate) async fn sweep_once(
    app_registry: &Arc<AppRegistry>,
    driver: &Arc<dyn AppManager>,
    cache: &Arc<CachingAppManager>,
    purger: &AppPurger,
) -> usize {
    let mut purged = 0usize;
    for id in app_registry.connected_app_ids() {
        // Resolve the key from the warm cache BEFORE the driver verdict (a removed
        // app returns no App, so we couldn't learn its key afterwards). Empty key
        // ⇒ id-keyed eviction only (key alias falls back to TTL); never blocks.
        let key = cache
            .by_id(&id)
            .await
            .ok()
            .flatten()
            .map(|a| a.key.clone())
            .unwrap_or_default();
        match driver.by_id(&id).await {
            Ok(None) => {
                purger.purge(&id, &key).await;
                purged += 1;
            }
            Ok(Some(_)) => {} // live — skip
            Err(e) => {
                tracing::warn!(error = %e, app = %id, "app-purge sweep: driver lookup failed; skipping (no purge on error)");
            }
        }
    }
    purged
}

/// Spawn the low-frequency sweep loop. Off when `interval_secs == 0`.
pub fn spawn_sweep(
    interval_secs: u64,
    app_registry: Arc<AppRegistry>,
    driver: Arc<dyn AppManager>,
    cache: Arc<CachingAppManager>,
    purger: Arc<AppPurger>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
        loop {
            ticker.tick().await;
            let n = sweep_once(&app_registry, &driver, &cache, &purger).await;
            if n > 0 {
                tracing::info!(purged = n, "app-purge sweep reclaimed removed apps");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::app_registry::AppRegistry;
    use crate::adapter::local::LocalAdapter;
    use crate::app::cache::CacheConfig;
    use crate::app::{App, AppLookupError, AppManager};
    use crate::channel::registry::Registry;
    use crate::connection::handle::{ConnectionHandle, Mailbox};
    use crate::protocol::event::ServerEvent;
    use crate::protocol::socket_id::SocketId;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;

    fn app(id: &str, key: &str) -> Arc<App> {
        let mut a: App = serde_json::from_value(serde_json::json!({
            "name": "t", "id": id, "key": key, "secret": "s", "enabled": true
        }))
        .unwrap();
        a.recompute_has_flags();
        Arc::new(a)
    }

    /// Call-counting mock: every `by_id` hit increments `calls`.
    struct Mock {
        app: Option<Arc<App>>,
        calls: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl AppManager for Mock {
        async fn by_id(&self, _: &str) -> Result<Option<Arc<App>>, AppLookupError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.app.clone())
        }
        async fn by_key(&self, k: &str) -> Result<Option<Arc<App>>, AppLookupError> {
            self.by_id(k).await
        }
    }

    #[tokio::test]
    async fn purge_closes_conns_reclaims_counter_and_evicts_cache() {
        let app_registry = Arc::new(AppRegistry::new());
        let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new()), app_registry.clone()));
        let adapter: Arc<dyn Adapter> = local.clone();

        // A live connection of app "a".
        let (tx, mut rx) = mpsc::channel(1024);
        let sid = SocketId::generate();
        app_registry.insert("a", ConnectionHandle { socket_id: sid, mailbox: Mailbox::new(tx, None, None) });

        // A conn_counts entry for "a" (as establish would create).
        let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(DashMap::new());
        conn_counts.insert("a".to_string(), Arc::new(AtomicUsize::new(1)));

        let driver_calls = Arc::new(AtomicUsize::new(0));
        let mock_driver = Arc::new(Mock { app: Some(app("a", "k")), calls: driver_calls.clone() });
        let cfg = CacheConfig { max_capacity: 100, ttl_secs: 300, neg_max: 100, neg_ttl_secs: 300 };
        let cache = Arc::new(CachingAppManager::new(mock_driver, cfg, None));

        // Warm the cache: driver must be hit exactly once.
        assert_eq!(cache.by_id("a").await.unwrap().unwrap().key, "k");
        let calls_after_warm = driver_calls.load(Ordering::SeqCst);
        assert_eq!(calls_after_warm, 1, "warm-up must hit the driver once");

        // Second lookup before purge must be an L1 cache hit (driver NOT called again).
        let _ = cache.by_id("a").await.unwrap();
        assert_eq!(driver_calls.load(Ordering::SeqCst), 1, "pre-purge second lookup must be L1 hit");

        let purger = AppPurger::new(adapter, conn_counts.clone(), cache.clone());
        purger.purge("a", "k").await;

        // (1) Connection force-closed 4009.
        assert!(matches!(rx.try_recv().map(|b| *b), Ok(ServerEvent::Error(e)) if e.code == 4009));
        assert!(matches!(rx.try_recv().map(|b| *b), Ok(ServerEvent::Close { code: 4009, .. })));
        // (2) conn_counts entry reclaimed.
        assert!(!conn_counts.contains_key("a"), "purge must remove the conn_counts entry");
        // (3) AppRegistry entry drained.
        assert!(app_registry.connected_app_ids().is_empty());
        // (4) Cache was evicted: a lookup after purge must reach the driver again.
        let _ = cache.by_id("a").await.unwrap();
        assert!(
            driver_calls.load(Ordering::SeqCst) > 1,
            "post-purge lookup must miss L1 and re-hit the driver (cache was evicted)"
        );
    }

    enum Outcome {
        Some,
        None,
        Err,
    }
    struct VarMock {
        app: Arc<App>,
        outcome: Outcome,
    }
    #[async_trait::async_trait]
    impl AppManager for VarMock {
        async fn by_id(&self, _: &str) -> Result<Option<Arc<App>>, AppLookupError> {
            match self.outcome {
                Outcome::Some => Ok(std::option::Option::Some(self.app.clone())),
                Outcome::None => Ok(std::option::Option::None),
                Outcome::Err => Err(AppLookupError::Backend("down".into())),
            }
        }
        async fn by_key(&self, k: &str) -> Result<Option<Arc<App>>, AppLookupError> {
            self.by_id(k).await
        }
    }

    type SetupResult = (Arc<AppRegistry>, Arc<dyn AppManager>, Arc<CachingAppManager>, AppPurger, mpsc::Receiver<Box<ServerEvent>>);

    fn setup(outcome: Outcome) -> SetupResult {
        let app_registry = Arc::new(AppRegistry::new());
        let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new()), app_registry.clone()));
        let adapter: Arc<dyn Adapter> = local.clone();
        let (tx, rx) = mpsc::channel(1024);
        app_registry.insert("a", ConnectionHandle { socket_id: SocketId::generate(), mailbox: Mailbox::new(tx, None, None) });
        let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(DashMap::new());
        conn_counts.insert("a".to_string(), Arc::new(AtomicUsize::new(1)));
        let driver: Arc<dyn AppManager> = Arc::new(VarMock { app: app("a", "k"), outcome });
        let cfg = CacheConfig { max_capacity: 100, ttl_secs: 300, neg_max: 100, neg_ttl_secs: 300 };
        let cache = Arc::new(CachingAppManager::new(driver.clone(), cfg, None));
        let purger = AppPurger::new(adapter, conn_counts, cache.clone());
        (app_registry, driver, cache, purger, rx)
    }

    #[tokio::test]
    async fn sweep_purges_app_whose_driver_returns_none() {
        let (app_registry, driver, cache, purger, mut rx) = setup(Outcome::None);
        // Warm the cache so the sweep can resolve "a"'s key before purging.
        let _ = cache.by_id("a").await;
        let purged = sweep_once(&app_registry, &driver, &cache, &purger).await;
        assert_eq!(purged, 1, "an Ok(None) app must be purged");
        assert!(matches!(rx.try_recv().map(|b| *b), Ok(ServerEvent::Error(e)) if e.code == 4009));
    }

    #[tokio::test]
    async fn sweep_does_not_purge_on_ok_some() {
        let (app_registry, driver, cache, purger, mut rx) = setup(Outcome::Some);
        let _ = cache.by_id("a").await;
        let purged = sweep_once(&app_registry, &driver, &cache, &purger).await;
        assert_eq!(purged, 0, "a live (Ok(Some)) app must NOT be purged");
        assert!(rx.try_recv().is_err(), "no 4009 frame for a live app");
    }

    #[tokio::test]
    async fn sweep_does_not_purge_on_err() {
        let (app_registry, driver, cache, purger, mut rx) = setup(Outcome::Err);
        let _ = cache.by_id("a").await;
        let purged = sweep_once(&app_registry, &driver, &cache, &purger).await;
        assert_eq!(purged, 0, "a backend Err must NEVER purge a live app");
        assert!(rx.try_recv().is_err(), "no 4009 frame on a backend error");
    }

    /// The spawned sweep LOOP fires on its own and purges a now-disabled app
    /// (driver `Ok(None)`) with NO invalidation signal — the backstop's whole
    /// contract. (A `tokio::time::interval`'s first tick is immediate, so this
    /// resolves promptly rather than waiting a full interval.)
    #[tokio::test]
    async fn spawn_sweep_periodically_purges_a_disabled_app() {
        let (app_registry, driver, cache, purger, mut rx) = setup(Outcome::None);
        // Warm the cache so the sweep can resolve "a"'s key before purging.
        let _ = cache.by_id("a").await;

        let handle = spawn_sweep(1, app_registry, driver, cache, Arc::new(purger));

        // The first tick drives one sweep_once immediately → the disabled app is
        // force-closed (4009) by the background loop, no signal required.
        let got_4009 = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                match rx.recv().await {
                    Some(b) if matches!(*b, ServerEvent::Error(ref e) if e.code == 4009) => {
                        return true
                    }
                    Some(_) => continue,
                    None => return false,
                }
            }
        })
        .await
        .unwrap_or(false);
        handle.abort();
        assert!(
            got_4009,
            "the spawned sweep loop must purge the disabled app (4009) on its own"
        );
    }
}
