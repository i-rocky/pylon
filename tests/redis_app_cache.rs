//! Integration tests for the Redis-backed L2 app cache and cross-node
//! invalidation: [`pylon::app::l2`], the L2 paths of [`pylon::app::cache`],
//! [`pylon::app::invalidation`], and the admin invalidate handler's 202 path.
//!
//! Like `redis_cluster.rs` these talk to a REAL Redis (`PYLON_TEST_REDIS_URL`,
//! default `redis://127.0.0.1:6390`) and isolate every run behind a random or
//! UUID-suffixed key — never FLUSHALL/FLUSHDB. They FAIL LOUD if Redis is
//! unreachable — there is no silent skip.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use dashmap::DashMap;
use pylon::adapter::app_registry::AppRegistry;
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::cache::{CacheConfig, CachingAppManager};
use pylon::app::invalidation::{AppInvalidator, InvalidateAction};
use pylon::app::l2::{L2Hit, RedisAppCache};
use pylon::app::purger::AppPurger;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::{App, AppLookup, AppLookupError, AppManager};
use pylon::channel::registry::Registry;
use pylon::connection::handle::{ConnectionHandle, Mailbox};
use pylon::http::rest::admin::post_invalidate;
use pylon::http::rest::ratelimit::RestRateLimits;
use pylon::protocol::event::ServerEvent;
use pylon::protocol::socket_id::SocketId;
use pylon::server::config::ServerConfig;
use pylon::server::router::AppState;
use pylon::webhook::WebhookHandle;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

fn redis_url() -> String {
    std::env::var("PYLON_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6390".into())
}

fn app(id: &str, key: &str) -> Arc<App> {
    let mut a: App = serde_json::from_value(serde_json::json!({
        "name":"t","id":id,"key":key,"secret":"s","enabled":true}))
    .unwrap();
    a.recompute_has_flags();
    Arc::new(a)
}

fn uniq_app() -> App {
    let n = uuid::Uuid::new_v4().to_string();
    let mut a: App = serde_json::from_value(serde_json::json!({
        "name":"t","id":format!("id-{n}"),"key":format!("key-{n}"),"secret":"s",
        "capacity":3,"client_messages_enabled":true,"enabled":true,
        "webhooks":[{"url":"https://e.test","event_types":["channel_occupied"]}]
    }))
    .unwrap();
    a.recompute_has_flags();
    a
}

fn cfg() -> CacheConfig {
    CacheConfig {
        max_capacity: 100,
        ttl_secs: 60,
        neg_max: 100,
        neg_ttl_secs: 60,
    }
}

#[derive(Clone)]
enum Answer {
    Disabled,
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
            Answer::Disabled => Ok(AppLookup::Disabled),
            Answer::Fail => Err(AppLookupError::Backend("boom".into())),
        }
    }
    async fn by_key(&self, k: &str) -> Result<AppLookup, AppLookupError> {
        self.by_id(k).await
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

struct InvalidationMock {
    app: Option<Arc<App>>,
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl AppManager for InvalidationMock {
    async fn by_id(&self, _: &str) -> Result<AppLookup, AppLookupError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(AppLookup::from(self.app.clone()))
    }
    async fn by_key(&self, k: &str) -> Result<AppLookup, AppLookupError> {
        self.by_id(k).await
    }
}

#[tokio::test]
async fn put_then_get_by_id_and_key_round_trips() {
    let c = RedisAppCache::connect(&redis_url(), 2, 60).await.unwrap();
    let app = uniq_app();
    c.put(&app).await.unwrap();
    let L2Hit::Found(by_id) = c.get_id(&app.id).await.unwrap().expect("get_id hit") else {
        panic!("expected Found");
    };
    assert_eq!(by_id.key, app.key);
    assert!(by_id.has_channel_occupied_webhooks); // recompute ran on read-back
    let L2Hit::Found(by_key) = c.get_key(&app.key).await.unwrap().expect("get_key hit") else {
        panic!("expected Found");
    };
    assert_eq!(by_key.id, app.id);
}

/// R1: the disabled marker round-trips distinctly from both a miss and a
/// Found app, under the id- AND the key-keyed entry.
#[tokio::test]
async fn disabled_marker_round_trips_under_both_aliases() {
    let c = RedisAppCache::connect(&redis_url(), 2, 60).await.unwrap();
    let n = uuid::Uuid::new_v4();
    let (id, key) = (format!("id-{n}"), format!("key-{n}"));
    c.put_disabled_id(&id).await.unwrap();
    c.put_disabled_key(&key).await.unwrap();
    assert!(matches!(
        c.get_id(&id).await.unwrap(),
        Some(L2Hit::Disabled)
    ));
    assert!(matches!(
        c.get_key(&key).await.unwrap(),
        Some(L2Hit::Disabled)
    ));
    // A neighbouring miss stays a miss (no bleed between keys).
    assert!(c.get_id(&format!("other-{n}")).await.unwrap().is_none());
    // `del` evicts the markers like any other entry.
    c.del(&id, &key).await.unwrap();
    assert!(c.get_id(&id).await.unwrap().is_none());
    assert!(c.get_key(&key).await.unwrap().is_none());
}

#[tokio::test]
async fn get_miss_is_ok_none() {
    let c = RedisAppCache::connect(&redis_url(), 2, 60).await.unwrap();
    let n = uuid::Uuid::new_v4();
    assert!(c.get_id(&format!("absent-{n}")).await.unwrap().is_none());
}

#[tokio::test]
async fn del_removes_both_keys() {
    let c = RedisAppCache::connect(&redis_url(), 2, 60).await.unwrap();
    let app = uniq_app();
    c.put(&app).await.unwrap();
    assert!(c.get_id(&app.id).await.unwrap().is_some());
    c.del(&app.id, &app.key).await.unwrap();
    assert!(c.get_id(&app.id).await.unwrap().is_none());
    assert!(c.get_key(&app.key).await.unwrap().is_none());
}

#[tokio::test]
async fn l2_hit_avoids_driver() {
    // populate L2, then a CachingAppManager whose driver would PANIC if called serves from L2.
    let l2 = Arc::new(RedisAppCache::connect(&redis_url(), 2, 60).await.unwrap());
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
    let l2 = Arc::new(RedisAppCache::connect(&redis_url(), 2, 60).await.unwrap());
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
    let l2 = Arc::new(RedisAppCache::connect(&redis_url(), 2, 60).await.unwrap());
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
async fn publish_on_one_node_evicts_another() {
    let cfg = CacheConfig {
        max_capacity: 100,
        ttl_secs: 300,
        neg_max: 100,
        neg_ttl_secs: 300,
    };
    let calls = Arc::new(AtomicUsize::new(0));
    // node B: the cache that should get evicted
    let cache_b = Arc::new(CachingAppManager::new(
        Arc::new(InvalidationMock {
            app: Some(app("a", "k")),
            calls: calls.clone(),
        }),
        cfg.clone(),
        None,
    ));
    let purger_b = Arc::new(AppPurger::new(
        {
            let app_registry = Arc::new(AppRegistry::new());
            let local: Arc<dyn Adapter> =
                Arc::new(LocalAdapter::new(Arc::new(Registry::new()), app_registry));
            local
        },
        Arc::new(DashMap::new()),
        cache_b.clone(),
    ));
    let _inv_b = AppInvalidator::spawn(&redis_url(), purger_b).await.unwrap();
    // node A: only publishes
    let cache_a = Arc::new(CachingAppManager::new(
        Arc::new(InvalidationMock {
            app: Some(app("a", "k")),
            calls: Arc::new(AtomicUsize::new(0)),
        }),
        cfg,
        None,
    ));
    let purger_a = Arc::new(AppPurger::new(
        {
            let app_registry = Arc::new(AppRegistry::new());
            let local: Arc<dyn Adapter> =
                Arc::new(LocalAdapter::new(Arc::new(Registry::new()), app_registry));
            local
        },
        Arc::new(DashMap::new()),
        cache_a,
    ));
    let inv_a = AppInvalidator::spawn(&redis_url(), purger_a).await.unwrap();

    // warm node B's cache (driver call 1), then invalidate from node A
    assert!(matches!(&cache_b.by_id("a").await.unwrap(), AppLookup::Found(a) if a.key == "k"));
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
        assert!(matches!(&cache_b.by_id("a").await.unwrap(), AppLookup::Found(a) if a.key == "k"));
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
    let cfg = CacheConfig {
        max_capacity: 100,
        ttl_secs: 300,
        neg_max: 100,
        neg_ttl_secs: 300,
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let cache_b = Arc::new(CachingAppManager::new(
        Arc::new(InvalidationMock {
            app: Some(app("a", "k")),
            calls: calls.clone(),
        }),
        cfg.clone(),
        None,
    ));

    // node B: a live connection registered for "a", and a conn_counts entry.
    let app_registry_b = Arc::new(AppRegistry::new());
    let local_b = Arc::new(LocalAdapter::new(
        Arc::new(Registry::new()),
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
    let conn_counts_b: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(DashMap::new());
    conn_counts_b.insert("a".to_string(), Arc::new(AtomicUsize::new(1)));

    let adapter_b: Arc<dyn Adapter> = local_b.clone();
    let purger_b = Arc::new(AppPurger::new(
        adapter_b,
        conn_counts_b.clone(),
        cache_b.clone(),
    ));
    let _inv_b = AppInvalidator::spawn(&redis_url(), purger_b).await.unwrap();

    // Warm node B's cache.
    assert!(matches!(&cache_b.by_id("a").await.unwrap(), AppLookup::Found(a) if a.key == "k"));
    let warmed = calls.load(Ordering::SeqCst);

    // node A publishes a REMOVE.
    let purger_a = Arc::new(AppPurger::new(
        {
            let ar = Arc::new(AppRegistry::new());
            let l: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(Arc::new(Registry::new()), ar));
            l
        },
        Arc::new(DashMap::new()),
        Arc::new(CachingAppManager::new(
            Arc::new(InvalidationMock {
                app: Some(app("a", "k")),
                calls: Arc::new(AtomicUsize::new(0)),
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

async fn cache_b_evicted(c: &Arc<CachingAppManager>, calls: &Arc<AtomicUsize>) -> bool {
    // Probe eviction: do a lookup — if the entry was evicted, the driver is hit (calls bump).
    // We look for calls == 2 to confirm the eviction round-trip completed.
    let _ = c.by_id("a").await;
    calls.load(Ordering::SeqCst) >= 2
}

/// Redis-gated: the authenticated success path publishes to Redis pub/sub and
/// returns 202 Accepted.
#[tokio::test]
async fn handler_authed_with_invalidator_returns_202() {
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json("[]").unwrap());
    let cache = Arc::new(CachingAppManager::new(
        apps,
        CacheConfig {
            max_capacity: 16,
            ttl_secs: 60,
            neg_max: 16,
            neg_ttl_secs: 60,
        },
        None,
    ));
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(
        Arc::new(Registry::new()),
        Arc::new(AppRegistry::new()),
    ));
    let purger = Arc::new(AppPurger::new(adapter, Arc::new(DashMap::new()), cache));
    let inv = AppInvalidator::spawn(&redis_url(), purger)
        .await
        .expect("invalidator must connect to the test Redis");

    let config = ServerConfig {
        app_admin_token: Some("secret".into()),
        ..ServerConfig::default()
    };
    let state = AppState {
        rest_limits: Arc::new(RestRateLimits::new(&config)),
        config,
        apps: Arc::new(StaticFileAppManager::from_json("[]").unwrap()),
        adapter: Arc::new(LocalAdapter::new(
            Arc::new(Registry::new()),
            Arc::new(AppRegistry::new()),
        )),
        conn_counts: Arc::new(DashMap::new()),
        webhooks: WebhookHandle::null(),
        saturated: None,
        draining: Arc::new(AtomicBool::new(false)),
        app_store_up: Arc::new(AtomicBool::new(true)),
        cluster_metrics: None,
        invalidator: Some(inv),
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        "Bearer secret".parse().unwrap(),
    );

    let status = post_invalidate(
        State(state),
        Path("app1".into()),
        headers,
        Ok(Bytes::from(r#"{"key":"k","action":"refresh"}"#)),
    )
    .await
    .expect("authed valid request must return 202");
    assert_eq!(status, StatusCode::ACCEPTED);
}
