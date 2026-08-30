use super::l2::{L2Hit, RedisAppCache};
use super::{App, AppLookup, AppLookupError, AppManager};
use moka::future::Cache;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub max_capacity: u64,
    pub ttl_secs: u64,
    pub neg_max: u64,
    pub neg_ttl_secs: u64,
}

/// Outcome of a cache-miss load, distinguishing "not found" (cache as negative)
/// from "found but disabled" (cache as disabled, positive TTL like Found) and a
/// real backend error (propagate, never cache).
enum LoadErr {
    NotFound,
    Disabled,
    Lookup(AppLookupError),
}

enum LookupBy {
    Id(String),
    Key(String),
}
impl LookupBy {
    async fn load_from_l2(&self, l2: &RedisAppCache) -> Result<Option<L2Hit>, AppLookupError> {
        match self {
            LookupBy::Id(id) => l2.get_id(id).await,
            LookupBy::Key(k) => l2.get_key(k).await,
        }
    }
    /// Persist the disabled marker under THIS lookup's alias (the id- or the
    /// key-keyed L2 entry — a disabled answer only knows the alias it probed).
    async fn mark_disabled_l2(&self, l2: &RedisAppCache) -> Result<(), AppLookupError> {
        match self {
            LookupBy::Id(id) => l2.put_disabled_id(id).await,
            LookupBy::Key(k) => l2.put_disabled_key(k).await,
        }
    }
    async fn load_from_driver(&self, d: &dyn AppManager) -> Result<AppLookup, AppLookupError> {
        match self {
            LookupBy::Id(id) => d.by_id(id).await,
            LookupBy::Key(k) => d.by_key(k).await,
        }
    }
}

pub struct CachingAppManager {
    inner: Arc<dyn AppManager>,
    pos: Cache<String, Arc<App>>,
    neg: Cache<String, ()>,
    /// R1: disabled apps cached under their own keyspace with the POSITIVE TTL
    /// (like `Found`) — a disabled answer is as stable as a found one, and must
    /// never be conflated with `neg`'s NotFound (REST maps them 403 vs 401).
    dis: Cache<String, ()>,
    l2: Option<Arc<RedisAppCache>>,
}

impl CachingAppManager {
    pub fn new(
        inner: Arc<dyn AppManager>,
        cfg: CacheConfig,
        l2: Option<Arc<RedisAppCache>>,
    ) -> Self {
        let pos = Cache::builder()
            .max_capacity(cfg.max_capacity)
            .time_to_live(Duration::from_secs(cfg.ttl_secs))
            .build();
        let neg = Cache::builder()
            .max_capacity(cfg.neg_max)
            .time_to_live(Duration::from_secs(cfg.neg_ttl_secs))
            .build();
        let dis = Cache::builder()
            .max_capacity(cfg.max_capacity)
            .time_to_live(Duration::from_secs(cfg.ttl_secs))
            .build();
        Self {
            inner,
            pos,
            neg,
            dis,
            l2,
        }
    }

    async fn cached(&self, pkey: String, by: LookupBy) -> Result<AppLookup, AppLookupError> {
        if self.neg.get(&pkey).await.is_some() {
            return Ok(AppLookup::NotFound);
        }
        if self.dis.get(&pkey).await.is_some() {
            return Ok(AppLookup::Disabled);
        }
        let inner = self.inner.clone();
        let l2 = self.l2.clone();
        let res = self
            .pos
            .try_get_with(pkey.clone(), async move {
                // L2 first — best-effort: errors degrade to the driver, never fail the lookup.
                if let Some(l2) = &l2 {
                    match by.load_from_l2(l2).await {
                        Ok(Some(L2Hit::Found(app))) => return Ok(app),
                        Ok(Some(L2Hit::Disabled)) => return Err(LoadErr::Disabled),
                        Ok(None) => {}
                        Err(e) => tracing::warn!(error = %e, "app L2 get failed; using driver"),
                    }
                }
                match by.load_from_driver(&*inner).await {
                    Ok(AppLookup::Found(app)) => {
                        if let Some(l2) = &l2 {
                            if let Err(e) = l2.put(&app).await {
                                tracing::warn!(error = %e, "app L2 put failed (ignored)");
                            }
                        }
                        Ok(app)
                    }
                    Ok(AppLookup::Disabled) => {
                        if let Some(l2) = &l2 {
                            if let Err(e) = by.mark_disabled_l2(l2).await {
                                tracing::warn!(error = %e, "app L2 disabled-mark failed (ignored)");
                            }
                        }
                        Err(LoadErr::Disabled)
                    }
                    Ok(AppLookup::NotFound) => Err(LoadErr::NotFound),
                    Err(e) => Err(LoadErr::Lookup(e)),
                }
            })
            .await;

        match res {
            Ok(app) => Ok(AppLookup::Found(app)),
            Err(arc) => match &*arc {
                LoadErr::NotFound => {
                    self.neg.insert(pkey, ()).await;
                    Ok(AppLookup::NotFound)
                }
                LoadErr::Disabled => {
                    self.dis.insert(pkey, ()).await;
                    Ok(AppLookup::Disabled)
                }
                LoadErr::Lookup(e) => Err(e.clone()),
            },
        }
    }

    /// Evict an app from L1 (positive + negative + disabled, both id and key
    /// aliases) and L2. L2 errors are best-effort (logged). Carries `key` so the
    /// key alias evicts reliably without waiting for TTL.
    pub async fn invalidate(&self, id: &str, key: &str) {
        let id_pkey = format!("id:{id}");
        let key_pkey = format!("key:{key}");
        self.pos.invalidate(&id_pkey).await;
        self.pos.invalidate(&key_pkey).await;
        self.neg.invalidate(&id_pkey).await;
        self.neg.invalidate(&key_pkey).await;
        self.dis.invalidate(&id_pkey).await;
        self.dis.invalidate(&key_pkey).await;
        if let Some(l2) = &self.l2 {
            if let Err(e) = l2.del(id, key).await {
                tracing::warn!(error = %e, "L2 del during invalidate failed (ignored)");
            }
        }
    }
}

#[async_trait::async_trait]
impl AppManager for CachingAppManager {
    async fn by_id(&self, id: &str) -> Result<AppLookup, AppLookupError> {
        self.cached(format!("id:{id}"), LookupBy::Id(id.to_string()))
            .await
    }
    async fn by_key(&self, key: &str) -> Result<AppLookup, AppLookupError> {
        self.cached(format!("key:{key}"), LookupBy::Key(key.to_string()))
            .await
    }

    fn by_key_cached(&self, key: &str) -> Option<Result<AppLookup, AppLookupError>> {
        // SYNC L1-ONLY probe. Use the SAME pkey format as `by_key`. Touch ONLY the
        // in-memory `neg`/`dis`/`pos` moka caches — never `inner` (the driver) or
        // `l2` (Redis): those are the I/O we offload. `block_on` of an in-memory
        // moka `get` is instant (no reactor/IO is driven — it is exactly what
        // today's establish `block_on` already drives on a hit). The cache never
        // stores errors, so this never yields `Some(Err(_))`.
        let pkey = format!("key:{key}");
        if futures_executor::block_on(self.neg.get(&pkey)).is_some() {
            return Some(Ok(AppLookup::NotFound));
        }
        if futures_executor::block_on(self.dis.get(&pkey)).is_some() {
            return Some(Ok(AppLookup::Disabled));
        }
        futures_executor::block_on(self.pos.get(&pkey)).map(|app| Ok(AppLookup::Found(app)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{App, AppLookup, AppLookupError, AppManager};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn app(id: &str, key: &str) -> Arc<App> {
        let mut a: App = serde_json::from_value(serde_json::json!({
            "name":"t","id":id,"key":key,"secret":"s","enabled":true}))
        .unwrap();
        a.recompute_has_flags();
        Arc::new(a)
    }

    /// The mock driver's canned answer.
    #[derive(Clone)]
    enum Answer {
        Found(Arc<App>),
        Disabled,
        NotFound,
        Fail,
    }

    struct Mock {
        answer: Answer,
        calls: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl AppManager for Mock {
        async fn by_id(&self, _id: &str) -> Result<AppLookup, AppLookupError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.answer.clone() {
                Answer::Found(a) => Ok(AppLookup::Found(a)),
                Answer::Disabled => Ok(AppLookup::Disabled),
                Answer::NotFound => Ok(AppLookup::NotFound),
                Answer::Fail => Err(AppLookupError::Backend("boom".into())),
            }
        }
        async fn by_key(&self, _k: &str) -> Result<AppLookup, AppLookupError> {
            self.by_id(_k).await
        }
    }
    fn cfg() -> CacheConfig {
        CacheConfig {
            max_capacity: 100,
            ttl_secs: 60,
            neg_max: 100,
            neg_ttl_secs: 60,
        }
    }
    fn mock(answer: Answer) -> (Arc<dyn AppManager>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(Mock {
                answer,
                calls: calls.clone(),
            }),
            calls,
        )
    }

    #[tokio::test]
    async fn hit_serves_from_l1_without_touching_driver_again() {
        let (m, calls) = mock(Answer::Found(app("a", "k")));
        let c = CachingAppManager::new(m, cfg(), None);
        let AppLookup::Found(a) = c.by_id("a").await.unwrap() else {
            panic!("expected Found");
        };
        assert_eq!(a.key, "k");
        let AppLookup::Found(a2) = c.by_id("a").await.unwrap() else {
            panic!("expected Found");
        };
        assert_eq!(a2.key, "k");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "second lookup must be an L1 hit"
        );
    }

    #[tokio::test]
    async fn negative_is_cached_separately_and_not_refetched() {
        let (m, calls) = mock(Answer::NotFound);
        let c = CachingAppManager::new(m, cfg(), None);
        assert!(matches!(
            c.by_id("nope").await.unwrap(),
            AppLookup::NotFound
        ));
        assert!(matches!(
            c.by_id("nope").await.unwrap(),
            AppLookup::NotFound
        ));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "negative result must be cached"
        );
    }

    /// R1: Disabled is cached under its own L1 keyspace (positive TTL like
    /// Found), distinct from NotFound — and never refetched while warm.
    #[tokio::test]
    async fn disabled_is_cached_distinctly_and_not_refetched() {
        let (m, calls) = mock(Answer::Disabled);
        let c = CachingAppManager::new(m, cfg(), None);
        assert!(matches!(c.by_id("off").await.unwrap(), AppLookup::Disabled));
        assert!(matches!(c.by_id("off").await.unwrap(), AppLookup::Disabled));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "disabled result must be cached"
        );
        // The two negatives must not alias each other's keyspace: probing the
        // SAME id keeps answering Disabled, not NotFound.
        assert!(matches!(
            c.by_key("off").await.unwrap(),
            AppLookup::Disabled
        ));
    }

    #[tokio::test]
    async fn driver_error_propagates_and_is_not_cached() {
        let (m, calls) = mock(Answer::Fail);
        let c = CachingAppManager::new(m, cfg(), None);
        assert!(matches!(
            c.by_id("x").await,
            Err(AppLookupError::Backend(_))
        ));
        assert!(matches!(
            c.by_id("x").await,
            Err(AppLookupError::Backend(_))
        ));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "errors must NOT be cached (driver retried)"
        );
    }

    #[tokio::test]
    async fn concurrent_misses_collapse_to_one_driver_call() {
        let (m, calls) = mock(Answer::Found(app("a", "k")));
        let c = Arc::new(CachingAppManager::new(m, cfg(), None));
        let mut hs = Vec::new();
        for _ in 0..50 {
            let c = c.clone();
            hs.push(tokio::spawn(async move { c.by_id("a").await }));
        }
        for h in hs {
            assert!(matches!(h.await.unwrap().unwrap(), AppLookup::Found(_)));
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "single-flight: 50 concurrent misses => 1 driver call"
        );
    }

    #[tokio::test]
    async fn invalidate_evicts_l1_positive() {
        let (m, calls) = mock(Answer::Found(app("a", "k")));
        let c = CachingAppManager::new(m, cfg(), None);
        assert!(matches!(c.by_id("a").await.unwrap(), AppLookup::Found(_))); // driver call 1
        c.invalidate("a", "k").await;
        assert!(matches!(c.by_id("a").await.unwrap(), AppLookup::Found(_))); // re-fetch (call 2)
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "invalidate must evict L1"
        );
    }

    #[tokio::test]
    async fn invalidate_evicts_negative_and_key_alias() {
        let (m, calls) = mock(Answer::Found(app("a", "k")));
        let c = CachingAppManager::new(m, cfg(), None);
        assert!(matches!(c.by_key("k").await.unwrap(), AppLookup::Found(_))); // caches key alias (call 1)
        c.invalidate("a", "k").await;
        assert!(matches!(c.by_key("k").await.unwrap(), AppLookup::Found(_))); // re-fetch by key (call 2)
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "invalidate must evict the key alias too"
        );
    }

    /// R1: invalidate also evicts a warm DISABLED entry (an app re-enabled in
    /// the store must be re-resolved, not stuck on the cached 403 answer).
    #[tokio::test]
    async fn invalidate_evicts_disabled() {
        let (m, calls) = mock(Answer::Disabled);
        let c = CachingAppManager::new(m, cfg(), None);
        assert!(matches!(c.by_key("k").await.unwrap(), AppLookup::Disabled)); // caches disabled (call 1)
        c.invalidate("a", "k").await;
        assert!(matches!(c.by_key("k").await.unwrap(), AppLookup::Disabled)); // re-fetch (call 2)
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "invalidate must evict the disabled entry"
        );
    }

    #[tokio::test]
    async fn l2_hit_avoids_driver() {
        // populate L2, then a CachingAppManager whose driver would PANIC if called serves from L2.
        let url = std::env::var("PYLON_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6390".into());
        let l2 = Arc::new(
            crate::app::l2::RedisAppCache::connect(&url, 2, 60)
                .await
                .unwrap(),
        );
        let a = app(
            &format!("id-{}", uuid::Uuid::new_v4()),
            &format!("key-{}", uuid::Uuid::new_v4()),
        );
        l2.put(&a).await.unwrap();
        let (m, calls) = mock(Answer::Fail); // driver returns Err if reached
        let c = CachingAppManager::new(m, cfg(), Some(l2));
        let AppLookup::Found(got) = c.by_id(&a.id).await.unwrap() else {
            panic!("expected Found from L2");
        };
        assert_eq!(got.key, a.key);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "L2 hit must not reach the driver"
        );
    }

    /// R1: the L2 disabled marker round-trips through the caching layer — an L2
    /// Disabled hit answers without touching the driver.
    #[tokio::test]
    async fn l2_disabled_marker_avoids_driver() {
        let url = std::env::var("PYLON_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6390".into());
        let l2 = Arc::new(
            crate::app::l2::RedisAppCache::connect(&url, 2, 60)
                .await
                .unwrap(),
        );
        let n = uuid::Uuid::new_v4().to_string();
        let id = format!("id-{n}");
        l2.put_disabled_id(&id).await.unwrap();
        let (m, calls) = mock(Answer::Fail); // driver fails if reached
        let c = CachingAppManager::new(m, cfg(), Some(l2));
        assert!(matches!(c.by_id(&id).await.unwrap(), AppLookup::Disabled));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "L2 disabled hit must not reach the driver"
        );
    }

    /// R1: a driver Disabled answer is written to L2 for the probed alias, so a
    /// SECOND CachingAppManager (same L2, driver that would fail) resolves it.
    #[tokio::test]
    async fn driver_disabled_answer_is_written_to_l2() {
        let url = std::env::var("PYLON_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6390".into());
        let l2 = Arc::new(
            crate::app::l2::RedisAppCache::connect(&url, 2, 60)
                .await
                .unwrap(),
        );
        let n = uuid::Uuid::new_v4().to_string();
        let id = format!("id-{n}");
        // Node A resolves via the driver (Disabled) and marks L2.
        let (m, _calls) = mock(Answer::Disabled);
        let a = CachingAppManager::new(m, cfg(), Some(l2.clone()));
        assert!(matches!(a.by_id(&id).await.unwrap(), AppLookup::Disabled));
        // Node B has NO L1 entry; its L2 hit must answer Disabled without a driver call.
        let (m2, calls2) = mock(Answer::Fail);
        let b = CachingAppManager::new(m2, cfg(), Some(l2));
        assert!(matches!(b.by_id(&id).await.unwrap(), AppLookup::Disabled));
        assert_eq!(
            calls2.load(Ordering::SeqCst),
            0,
            "node B must serve the disabled marker from L2"
        );
    }

    #[tokio::test]
    async fn by_key_cached_returns_none_on_cold_l1_without_touching_driver() {
        let (m, calls) = mock(Answer::Found(app("a", "k")));
        let c = CachingAppManager::new(m, cfg(), None);
        // Cold: nothing in L1 yet → must offload (None) and NOT call the driver.
        assert!(
            c.by_key_cached("k").is_none(),
            "cold probe must return None"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "cold probe must not call the driver"
        );
    }

    #[tokio::test]
    async fn by_key_cached_returns_some_some_when_l1_warm() {
        let (m, _calls) = mock(Answer::Found(app("a", "k")));
        let c = CachingAppManager::new(m, cfg(), None);
        // Warm the positive L1 via the normal async path.
        assert!(matches!(c.by_key("k").await.unwrap(), AppLookup::Found(_)));
        let probed = c.by_key_cached("k").expect("warm L1 resolves");
        assert!(matches!(probed.unwrap(), AppLookup::Found(_)));
    }

    #[tokio::test]
    async fn by_key_cached_returns_some_none_when_neg_cached() {
        let (m, _calls) = mock(Answer::NotFound);
        let c = CachingAppManager::new(m, cfg(), None);
        // Warm the negative L1: a miss caches "not found".
        assert!(matches!(c.by_key("k").await.unwrap(), AppLookup::NotFound));
        let probed = c.by_key_cached("k").expect("neg-cached resolves");
        assert!(
            matches!(probed.unwrap(), AppLookup::NotFound),
            "neg-cached probe is Some(Ok(NotFound))"
        );
    }

    /// R1: the sync probe answers Disabled distinctly once the disabled L1 is warm.
    #[tokio::test]
    async fn by_key_cached_returns_disabled_when_dis_cached() {
        let (m, _calls) = mock(Answer::Disabled);
        let c = CachingAppManager::new(m, cfg(), None);
        assert!(matches!(c.by_key("k").await.unwrap(), AppLookup::Disabled));
        let probed = c.by_key_cached("k").expect("dis-cached resolves");
        assert!(
            matches!(probed.unwrap(), AppLookup::Disabled),
            "dis-cached probe is Some(Ok(Disabled)), not NotFound"
        );
    }
}
