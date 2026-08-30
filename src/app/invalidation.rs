use super::purger::AppPurger;
use fred::interfaces::PubsubInterface;
use fred::prelude::*;
use std::sync::Arc;

/// Cross-node app-cache invalidation over Redis pub/sub.
pub struct AppInvalidator {
    publish_pool: Pool,
}

pub const INVALIDATE_CHANNEL: &str = "pylon:app:invalidate";

/// The invalidation action. `Refresh` (config/secret change) evicts cache only;
/// `Remove` (disabled/deleted) additionally force-closes connections + reclaims
/// the per-app counter. `#[serde(default)]` on the field + `#[default] Refresh`
/// means any legacy/blank message degrades to the SAFE (non-destructive) action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InvalidateAction {
    #[default]
    Refresh,
    Remove,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct InvalidateMsg {
    id: String,
    key: String,
    #[serde(default)]
    action: InvalidateAction,
}

impl AppInvalidator {
    /// Connect to `url`, subscribe to the invalidation channel (dispatching each
    /// message through `purger`), and return a handle that can publish
    /// invalidations.
    pub async fn spawn(url: &str, purger: Arc<AppPurger>) -> anyhow::Result<Arc<Self>> {
        // `max_attempts = 0` means retry forever; min 100ms, max 30s, base 2.
        let policy = ReconnectPolicy::new_exponential(0, 100, 30_000, 2);
        let mut builder = Builder::from_config(Config::from_url(url)?);
        builder.set_policy(policy);

        let publish_pool = builder.build_pool(2)?;
        publish_pool.init().await?;

        let sub = builder.build_subscriber_client()?;
        sub.init().await?;
        // Keep the resubscribe task handle so it isn't dropped (which would stop it).
        let _mgr = sub.manage_subscriptions();
        sub.subscribe(INVALIDATE_CHANNEL).await?;
        let mut rx = sub.message_rx();
        tokio::spawn(async move {
            // hold `sub` and `_mgr` for the task's lifetime so the subscription stays open
            let _sub = sub;
            let _sub_mgr = _mgr;
            loop {
                match rx.recv().await {
                    Ok(msg) => match msg.value.into_string() {
                        Some(s) => {
                            if let Ok(m) = serde_json::from_str::<InvalidateMsg>(&s) {
                                match m.action {
                                    InvalidateAction::Refresh => {
                                        purger.refresh(&m.id, &m.key).await
                                    }
                                    InvalidateAction::Remove => purger.purge(&m.id, &m.key).await,
                                }
                            } else {
                                tracing::warn!(payload = %s, "bad app-invalidate message");
                            }
                        }
                        None => {
                            tracing::warn!("dropped non-UTF8 app-invalidate payload");
                        }
                    },
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            skipped = n,
                            "app-invalidate sub stream lagged; dropped messages"
                        );
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        Ok(Arc::new(Self { publish_pool }))
    }

    pub async fn publish(
        &self,
        id: &str,
        key: &str,
        action: InvalidateAction,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_string(&InvalidateMsg {
            id: id.into(),
            key: key.into(),
            action,
        })?;
        let _: () = self
            .publish_pool
            .next()
            .publish(INVALIDATE_CHANNEL, payload)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::app_registry::AppRegistry;
    use crate::adapter::local::LocalAdapter;
    use crate::adapter::Adapter;
    use crate::app::cache::{CacheConfig, CachingAppManager};
    use crate::app::{App, AppLookupError, AppManager};
    use crate::channel::registry::Registry;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn redis_url() -> String {
        std::env::var("PYLON_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6390".into())
    }
    fn app(id: &str, key: &str) -> std::sync::Arc<App> {
        let mut a: App = serde_json::from_value(serde_json::json!({
            "name":"t","id":id,"key":key,"secret":"s","enabled":true}))
        .unwrap();
        a.recompute_has_flags();
        std::sync::Arc::new(a)
    }
    struct Mock {
        app: Option<std::sync::Arc<App>>,
        calls: std::sync::Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl AppManager for Mock {
        async fn by_id(&self, _: &str) -> Result<crate::app::AppLookup, AppLookupError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(crate::app::AppLookup::from(self.app.clone()))
        }
        async fn by_key(&self, k: &str) -> Result<crate::app::AppLookup, AppLookupError> {
            self.by_id(k).await
        }
    }

    #[tokio::test]
    async fn publish_on_one_node_evicts_another() {
        let cfg = CacheConfig {
            max_capacity: 100,
            ttl_secs: 300,
            neg_max: 100,
            neg_ttl_secs: 300,
        };
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        // node B: the cache that should get evicted
        let cache_b = std::sync::Arc::new(CachingAppManager::new(
            std::sync::Arc::new(Mock {
                app: Some(app("a", "k")),
                calls: calls.clone(),
            }),
            cfg.clone(),
            None,
        ));
        let purger_b = std::sync::Arc::new(crate::app::purger::AppPurger::new(
            {
                let app_registry = std::sync::Arc::new(AppRegistry::new());
                let local: std::sync::Arc<dyn Adapter> = std::sync::Arc::new(LocalAdapter::new(
                    std::sync::Arc::new(Registry::new()),
                    app_registry,
                ));
                local
            },
            std::sync::Arc::new(dashmap::DashMap::new()),
            cache_b.clone(),
        ));
        let _inv_b = AppInvalidator::spawn(&redis_url(), purger_b).await.unwrap();
        // node A: only publishes
        let cache_a = std::sync::Arc::new(CachingAppManager::new(
            std::sync::Arc::new(Mock {
                app: Some(app("a", "k")),
                calls: std::sync::Arc::new(AtomicUsize::new(0)),
            }),
            cfg,
            None,
        ));
        let purger_a = std::sync::Arc::new(crate::app::purger::AppPurger::new(
            {
                let app_registry = std::sync::Arc::new(AppRegistry::new());
                let local: std::sync::Arc<dyn Adapter> = std::sync::Arc::new(LocalAdapter::new(
                    std::sync::Arc::new(Registry::new()),
                    app_registry,
                ));
                local
            },
            std::sync::Arc::new(dashmap::DashMap::new()),
            cache_a,
        ));
        let inv_a = AppInvalidator::spawn(&redis_url(), purger_a).await.unwrap();

        // warm node B's cache (driver call 1), then invalidate from node A
        assert!(
            matches!(&cache_b.by_id("a").await.unwrap(), crate::app::AppLookup::Found(a) if a.key == "k")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        inv_a
            .publish("a", "k", InvalidateAction::Refresh)
            .await
            .unwrap();
        // wait for the pub/sub round-trip, then re-fetch to prove eviction
        for _ in 0..50 {
            if cache_b_evicted(&cache_b, &calls).await {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        // node B re-fetches => driver called again (calls == 2 if eviction happened in loop)
        let calls_before = calls.load(Ordering::SeqCst);
        if calls_before < 2 {
            // eviction not yet detected by probe; do re-fetch here as the binding assertion
            assert!(
                matches!(&cache_b.by_id("a").await.unwrap(), crate::app::AppLookup::Found(a) if a.key == "k")
            );
            assert_eq!(
                calls.load(Ordering::SeqCst),
                2,
                "node B must have been evicted by node A's publish"
            );
        } else {
            assert_eq!(
                calls_before, 2,
                "node B must have been evicted by node A's publish"
            );
        }
    }

    #[tokio::test]
    async fn remove_publish_force_closes_conn_clears_counter_and_evicts_cache_on_node_b() {
        use crate::connection::handle::{ConnectionHandle, Mailbox};
        use crate::protocol::event::ServerEvent;
        use crate::protocol::socket_id::SocketId;
        use tokio::sync::mpsc;

        let cfg = CacheConfig {
            max_capacity: 100,
            ttl_secs: 300,
            neg_max: 100,
            neg_ttl_secs: 300,
        };
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let cache_b = std::sync::Arc::new(CachingAppManager::new(
            std::sync::Arc::new(Mock {
                app: Some(app("a", "k")),
                calls: calls.clone(),
            }),
            cfg.clone(),
            None,
        ));

        // node B: a live connection registered for "a", and a conn_counts entry.
        let app_registry_b = std::sync::Arc::new(AppRegistry::new());
        let local_b = std::sync::Arc::new(LocalAdapter::new(
            std::sync::Arc::new(Registry::new()),
            app_registry_b.clone(),
        ));
        let (tx, mut rx) = mpsc::channel(1024);
        let sid = SocketId::generate();
        app_registry_b.insert(
            "a",
            ConnectionHandle {
                socket_id: sid,
                mailbox: Mailbox::new(tx, None, None),
            },
        );
        let conn_counts_b: std::sync::Arc<dashmap::DashMap<String, std::sync::Arc<AtomicUsize>>> =
            std::sync::Arc::new(dashmap::DashMap::new());
        conn_counts_b.insert("a".to_string(), std::sync::Arc::new(AtomicUsize::new(1)));

        let adapter_b: std::sync::Arc<dyn Adapter> = local_b.clone();
        let purger_b = std::sync::Arc::new(crate::app::purger::AppPurger::new(
            adapter_b,
            conn_counts_b.clone(),
            cache_b.clone(),
        ));
        let _inv_b = AppInvalidator::spawn(&redis_url(), purger_b).await.unwrap();

        // Warm node B's cache.
        assert!(
            matches!(&cache_b.by_id("a").await.unwrap(), crate::app::AppLookup::Found(a) if a.key == "k")
        );
        let warmed = calls.load(Ordering::SeqCst);

        // node A publishes a REMOVE.
        let purger_a = std::sync::Arc::new(crate::app::purger::AppPurger::new(
            {
                let ar = std::sync::Arc::new(AppRegistry::new());
                let l: std::sync::Arc<dyn Adapter> = std::sync::Arc::new(LocalAdapter::new(
                    std::sync::Arc::new(Registry::new()),
                    ar,
                ));
                l
            },
            std::sync::Arc::new(dashmap::DashMap::new()),
            std::sync::Arc::new(CachingAppManager::new(
                std::sync::Arc::new(Mock {
                    app: Some(app("a", "k")),
                    calls: std::sync::Arc::new(AtomicUsize::new(0)),
                }),
                cfg,
                None,
            )),
        ));
        let inv_a = AppInvalidator::spawn(&redis_url(), purger_a).await.unwrap();
        inv_a
            .publish("a", "k", InvalidateAction::Remove)
            .await
            .unwrap();

        // Wait for the pub/sub round-trip: the connection gets 4009.
        let got_close = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match rx.recv().await {
                    Some(b) => {
                        if matches!(*b, ServerEvent::Close { code: 4009, .. }) {
                            return true;
                        }
                    }
                    None => return false,
                }
            }
        })
        .await
        .unwrap_or(false);
        assert!(
            got_close,
            "node B's connection must be force-closed 4009 by the remove"
        );
        // conn_counts entry reclaimed.
        assert!(
            !conn_counts_b.contains_key("a"),
            "node B conn_counts entry must be cleared"
        );
        // Cache evicted: a re-fetch hits the driver again.
        let _ = cache_b.by_id("a").await;
        assert!(
            calls.load(Ordering::SeqCst) > warmed,
            "node B cache must be evicted"
        );
    }

    async fn cache_b_evicted(
        c: &std::sync::Arc<CachingAppManager>,
        calls: &std::sync::Arc<AtomicUsize>,
    ) -> bool {
        // Probe eviction: do a lookup — if the entry was evicted, the driver is hit (calls bump).
        // We look for calls == 2 to confirm the eviction round-trip completed.
        let _ = c.by_id("a").await;
        calls.load(Ordering::SeqCst) >= 2
    }
}
