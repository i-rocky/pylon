//! App-manager lookup microbenchmark (design spec Phase 1 deliverable).
//!
//! Validates the perf claims behind the DB-backed app-manager + cache:
//!   * **L1 hit** — a warm `CachingAppManager` lookup is a moka hit (the steady
//!     state for ~all lookups). Claim: ~100–300 ns, and crucially *constant* in
//!     the number of apps.
//!   * **Static O(n) scan** — `StaticFileAppManager` does a linear `Vec` scan, so
//!     its cost grows with the app count. The cross-over with the constant L1 hit
//!     is the whole reason the cache wins "at scale".
//!   * **Cold miss** — the per-miss CPU cost of the cache machinery
//!     (`try_get_with` single-flight + backfill) against an *instant* driver, i.e.
//!     everything a real miss pays on top of the actual DB/Redis I/O.
//!   * **Single-flight** — N concurrent misses for the same key collapse into one
//!     driver call; this measures the coalesced throughput.
//!
//! All benches are Redis-free and deterministic (an in-process mock driver), so
//! `cargo bench --bench app_lookup` needs no external services. Each measured
//! group reports time *per lookup* via `Throughput::Elements`, amortising the
//! one `block_on` per batch so the numbers reflect the lookup itself.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use pylon::app::cache::{CacheConfig, CachingAppManager};
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::{App, AppLookupError, AppManager};

/// Lookups folded into a single `block_on` so the runtime/executor overhead is
/// amortised across the batch and the per-element number reflects the lookup.
const BATCH: u64 = 1000;

fn app(id: &str, key: &str) -> Arc<App> {
    // The bench never exercises webhook gate flags, so `recompute_has_flags` is
    // intentionally not run here — only the lookup path is measured.
    let a: App = serde_json::from_value(serde_json::json!({
        "name": "bench", "id": id, "key": key, "secret": "s", "enabled": true
    }))
    .expect("valid app json");
    Arc::new(a)
}

/// Instant in-process driver that counts calls (so single-flight coalescing and
/// cache-hit avoidance are observable).
struct MockDriver {
    app: Option<Arc<App>>,
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl AppManager for MockDriver {
    async fn by_id(&self, _id: &str) -> Result<Option<Arc<App>>, AppLookupError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.app.clone())
    }
    async fn by_key(&self, k: &str) -> Result<Option<Arc<App>>, AppLookupError> {
        self.by_id(k).await
    }
}

fn cache_cfg() -> CacheConfig {
    // Capacity comfortably above the working set so the warm bench never evicts.
    CacheConfig { max_capacity: 10_000, ttl_secs: 3600, neg_max: 10_000, neg_ttl_secs: 3600 }
}

fn driver(app: Option<Arc<App>>) -> (Arc<dyn AppManager>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    (Arc::new(MockDriver { app, calls: calls.clone() }), calls)
}

/// Single-threaded runtime: a warm lookup never suspends on real I/O, so a
/// current-thread executor is the cheapest honest driver for the per-op cost.
fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("current-thread runtime")
}

/// Multi-threaded runtime for the concurrent single-flight bench.
fn rt_multi() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .expect("multi-thread runtime")
}

/// Warm `CachingAppManager` lookup → moka L1 hit (the steady state).
///
/// The reported per-lookup cost is the WHOLE public `by_id` path, which includes
/// one `format!("id:{id}")` heap allocation for the cache key on every call (the
/// same alloc production pays) plus the negative-cache check and the moka `pos`
/// get — not the raw moka lookup in isolation. That is the honest per-lookup cost.
fn bench_l1_hit(c: &mut Criterion) {
    let rt = rt();
    let (drv, calls) = driver(Some(app("app-0", "key-0")));
    let cache = CachingAppManager::new(drv, cache_cfg(), None);
    // Warm L1 once.
    rt.block_on(async { cache.by_id("app-0").await.unwrap().unwrap() });
    assert_eq!(calls.load(Ordering::Relaxed), 1, "warm-up must be the only driver call");

    let mut group = c.benchmark_group("app_lookup");
    group.throughput(Throughput::Elements(BATCH));
    group.bench_function("l1_hit", |b| {
        b.iter(|| {
            rt.block_on(async {
                for _ in 0..BATCH {
                    black_box(cache.by_id(black_box("app-0")).await.unwrap().unwrap());
                }
            });
        });
    });
    group.finish();
    // The whole point: a hit never re-touches the driver.
    assert_eq!(calls.load(Ordering::Relaxed), 1, "L1 hits must not call the driver");
}

/// `StaticFileAppManager` O(n) `Vec` scan at increasing app counts. The id looked
/// up is the LAST one (worst-case scan), the comparison point for the constant
/// L1 hit above.
fn bench_static_scan(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("app_lookup");
    group.throughput(Throughput::Elements(BATCH));
    for n in [10u64, 100, 1000] {
        let apps: Vec<serde_json::Value> = (0..n)
            .map(|i| {
                serde_json::json!({
                    "name": format!("app{i}"), "id": format!("app-{i}"),
                    "key": format!("key-{i}"), "secret": "s"
                })
            })
            .collect();
        let raw = serde_json::to_string(&apps).unwrap();
        let mgr = StaticFileAppManager::from_json(&raw).expect("valid static apps");
        let last = format!("app-{}", n - 1); // worst-case: last element of the scan
        group.bench_with_input(BenchmarkId::new("static_scan", n), &last, |b, last| {
            b.iter(|| {
                rt.block_on(async {
                    for _ in 0..BATCH {
                        black_box(mgr.by_id(black_box(last)).await.unwrap().unwrap());
                    }
                });
            });
        });
    }
    group.finish();
}

/// Per-miss cost of the cache machinery (single-flight `try_get_with` + backfill)
/// against an instant driver — i.e. the CPU a real miss pays on top of the actual
/// DB/Redis round-trip. Measured on a LONG-LIVED warm cache (moka init paid once,
/// in the warm-up) with a brand-new unique key per op, so each timed lookup is a
/// genuine steady-state miss (a fresh cache per op would instead measure one-time
/// cache construction, not the miss).
///
/// This is the POSITIVE-resolution miss (the driver returns `Some`, so the result
/// is stored in the positive cache); the negative-cache `None` path has a slightly
/// different (also cheap) cost and is not what's measured here.
fn bench_cold_miss(c: &mut Criterion) {
    let rt = rt();
    let (drv, _calls) = driver(Some(app("tmpl", "tmpl")));
    let cache = CachingAppManager::new(drv, cache_cfg(), None);
    rt.block_on(async { let _ = cache.by_id("warmup").await; }); // pay moka first-touch once
    let next = AtomicUsize::new(0);

    let mut group = c.benchmark_group("app_lookup");
    group.bench_function("cold_miss", |b| {
        b.iter_batched(
            // setup (untimed): a never-seen key ⇒ guaranteed miss regardless of capacity.
            || format!("miss-{}", next.fetch_add(1, Ordering::Relaxed)),
            |id| {
                rt.block_on(async {
                    black_box(cache.by_id(black_box(&id)).await.unwrap().unwrap());
                });
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Coalesced throughput of K concurrent misses for the SAME (fresh) key — moka
/// `try_get_with` single-flight serves all K from one in-flight init, so the
/// K-lookup latency tracks a single miss rather than K independent ones.
///
/// This bench MEASURES that coalesced cost on a real multi-thread runtime; it does
/// not assert the driver-call count. Single-flight CORRECTNESS (concurrent misses
/// collapse to one driver call) is covered deterministically by the unit test
/// `cache::tests::concurrent_misses_collapse_to_one_driver_call`. That test uses a
/// single-threaded runtime (cooperative tasks ⇒ exactly one init); under the true
/// parallelism here the collapse is to ~1 (occasionally 2 when two cores race past
/// moka's init check), which is still a herd-collapse, not K separate driver hits.
fn bench_single_flight(c: &mut Criterion) {
    let rt = rt_multi();
    let (drv, _calls) = driver(Some(app("tmpl", "tmpl")));
    let cache = Arc::new(CachingAppManager::new(drv, cache_cfg(), None));
    rt.block_on(async { let _ = cache.by_id("warmup").await; });
    let next = AtomicUsize::new(0);

    let mut group = c.benchmark_group("app_lookup");
    for k in [8u64, 64] {
        group.throughput(Throughput::Elements(k));
        group.bench_with_input(BenchmarkId::new("single_flight", k), &k, |b, &k| {
            b.iter_batched(
                || format!("sf-{}", next.fetch_add(1, Ordering::Relaxed)),
                |id| {
                    rt.block_on(async {
                        let mut handles = Vec::with_capacity(k as usize);
                        for _ in 0..k {
                            let c = cache.clone();
                            let id = id.clone();
                            handles.push(tokio::spawn(async move {
                                black_box(c.by_id(&id).await.unwrap().unwrap())
                            }));
                        }
                        for h in handles {
                            let _ = h.await;
                        }
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_l1_hit,
    bench_static_scan,
    bench_cold_miss,
    bench_single_flight
);
criterion_main!(benches);
