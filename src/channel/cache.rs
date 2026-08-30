//! Cache-channel storage types. A cache channel retains its last event so a new
//! subscriber can be replayed it (or told `pusher:cache_miss` when empty).

use std::time::{Duration, Instant};

/// The last event seen on a cache channel: the event name and its verbatim,
/// already-serialized `data` string — stored exactly as it was relayed.
///
/// `Serialize`/`Deserialize` let the Redis adapter persist the cached last-event as
/// JSON so it is shared across nodes (a plain data struct — the derives add no
/// behaviour and the in-memory `LocalAdapter` ignores them).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CachedEvent {
    pub event: String,
    pub data: String,
}

/// The stored form: the cached event plus the TTL stamped at insert time. The
/// TTL rides with the value because moka's per-entry [`Expiry`] policy reads it
/// from the entry it is expiring — the TTL arrives per `cache_set` call, not at
/// store construction.
#[derive(Debug, Clone)]
struct CacheEntry {
    event: CachedEvent,
    ttl: Duration,
}

/// moka expiry policy for [`CacheStore`]: a create or an overwrite stamps the
/// entry's own TTL; reads leave the deadline untouched (a `cache_get` must not
/// extend an entry's lifetime). A zero TTL yields an already-expired entry.
struct PerEntryTtl;

impl moka::Expiry<(String, String), CacheEntry> for PerEntryTtl {
    fn expire_after_create(
        &self,
        _key: &(String, String),
        value: &CacheEntry,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(value.ttl)
    }

    fn expire_after_update(
        &self,
        _key: &(String, String),
        value: &CacheEntry,
        _updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        Some(value.ttl)
    }
}

/// In-process cache store keyed by `(app, channel)`, used by `LocalAdapter`.
///
/// G7: every entry carries its per-insert TTL and moka evicts it once that
/// passes, so a cache channel published once and never subscribed again no
/// longer keeps its entry (and its serialized event) forever. Reads never
/// extend a deadline and a re-`set` of the same key wins with a fresh TTL,
/// matching the previous lazily-expiring DashMap store. No `max_capacity` is
/// set on purpose: a still-live entry must never be evicted early because
/// other channels churn — the TTL alone bounds memory.
pub struct CacheStore {
    inner: moka::sync::Cache<(String, String), CacheEntry>,
}

impl CacheStore {
    pub fn new() -> Self {
        Self {
            inner: moka::sync::Cache::builder()
                .expire_after(PerEntryTtl)
                .build(),
        }
    }

    /// Record `channel`'s last event under `app`, expiring after `ttl`. A zero
    /// `ttl` stores an already-expired entry (the next [`CacheStore::get`]
    /// misses), and re-setting the same key replaces both the event and the
    /// deadline.
    pub fn set(&self, app: &str, channel: &str, event: CachedEvent, ttl: Duration) {
        self.inner.insert(
            (app.to_string(), channel.to_string()),
            CacheEntry { event, ttl },
        );
    }

    /// The still-live cached event for `(app, channel)`, if any. An entry whose
    /// TTL has passed reads as a miss: moka checks the deadline on read, before
    /// the (coalesced) maintenance pass has physically evicted the entry.
    pub fn get(&self, app: &str, channel: &str) -> Option<CachedEvent> {
        self.inner
            .get(&(app.to_string(), channel.to_string()))
            .map(|e| e.event)
    }

    /// Run moka's pending maintenance (normally coalesced into later writes)
    /// synchronously, so expired entries are physically evicted now rather than
    /// on some future write. Used by tests asserting [`CacheStore::entry_count`].
    pub fn run_pending_tasks(&self) {
        self.inner.run_pending_tasks();
    }

    /// The number of entries currently resident. An estimate unless preceded by
    /// [`CacheStore::run_pending_tasks`] (moka counts entries pending eviction).
    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
}

impl Default for CacheStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(name: &str, data: &str) -> CachedEvent {
        CachedEvent {
            event: name.into(),
            data: data.into(),
        }
    }

    #[test]
    fn cached_event_round_trips_fields() {
        let e = CachedEvent {
            event: "my-event".into(),
            data: "{\"hi\":1}".into(),
        };
        assert_eq!(e.event, "my-event");
        assert_eq!(e.data, "{\"hi\":1}");
        assert_eq!(e.clone(), e);
    }

    #[test]
    fn cache_store_holds_entries() {
        let store = CacheStore::new();
        store.set("app", "cache-x", ev("e", "d"), Duration::from_secs(60));
        assert_eq!(store.get("app", "cache-x"), Some(ev("e", "d")));
    }

    #[test]
    fn get_misses_when_absent() {
        let store = CacheStore::new();
        assert_eq!(store.get("app", "cache-missing"), None);
    }

    #[test]
    fn zero_ttl_is_immediately_expired() {
        let store = CacheStore::new();
        store.set("app", "cache-x", ev("e", "d"), Duration::ZERO);
        assert_eq!(store.get("app", "cache-x"), None);
    }

    #[test]
    fn get_hits_before_ttl_and_misses_after() {
        let store = CacheStore::new();
        store.set("app", "cache-x", ev("e", "d"), Duration::from_millis(250));
        assert_eq!(store.get("app", "cache-x"), Some(ev("e", "d")));
        std::thread::sleep(Duration::from_millis(400));
        assert_eq!(store.get("app", "cache-x"), None);
    }

    #[test]
    fn same_key_reset_wins_with_fresh_ttl() {
        let store = CacheStore::new();
        store.set("app", "cache-x", ev("e", "one"), Duration::from_millis(50));
        store.set("app", "cache-x", ev("e", "two"), Duration::from_secs(60));
        std::thread::sleep(Duration::from_millis(150)); // first TTL has passed
        assert_eq!(store.get("app", "cache-x"), Some(ev("e", "two")));
    }

    #[test]
    fn apps_are_keyed_independently() {
        let store = CacheStore::new();
        store.set("app-a", "cache-x", ev("e", "a"), Duration::from_secs(60));
        assert_eq!(store.get("app-b", "cache-x"), None);
        assert_eq!(store.get("app-a", "cache-x"), Some(ev("e", "a")));
    }

    /// G7 regression: 10k distinct cache channels churned once and never read
    /// again must not stay resident after their TTLs pass. (The previous
    /// DashMap store retained all of them: expiry happened only on read.)
    ///
    /// moka evicts expired entries during its maintenance pass, driven by the
    /// hierarchical timer wheel — whose level-0 buckets are ~1s wide — so the
    /// test polls `run_pending_tasks` until the wheel has advanced past the
    /// bucket holding the expired mass.
    #[test]
    fn expired_entries_are_evicted_without_reads() {
        let store = CacheStore::new();
        for i in 0..10_000 {
            store.set(
                "app",
                &format!("cache-{i}"),
                ev("e", "d"),
                Duration::from_millis(50),
            );
        }
        // The entries' TTLs pass without a single read; maintenance must then
        // physically evict the whole expired mass within the deadline.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            store.run_pending_tasks();
            if store.entry_count() <= 64 || Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        assert!(
            store.entry_count() <= 64,
            "expected expired entries to be evicted, {} still resident",
            store.entry_count()
        );
    }
}
