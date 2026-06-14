//! Cache-channel storage types. A cache channel retains its last event so a new
//! subscriber can be replayed it (or told `pusher:cache_miss` when empty).

use dashmap::DashMap;
use std::time::Instant;

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

/// In-process cache store keyed by `(app, channel)`, valued with the cached
/// event plus its expiry instant. Used by `LocalAdapter`; entries expire lazily
/// on read.
pub type CacheStore = DashMap<(String, String), (CachedEvent, Instant)>;

#[cfg(test)]
mod tests {
    use super::*;

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
        let store: CacheStore = CacheStore::new();
        store.insert(
            ("app".into(), "cache-x".into()),
            (
                CachedEvent {
                    event: "e".into(),
                    data: "d".into(),
                },
                Instant::now(),
            ),
        );
        assert!(store.contains_key(&("app".to_string(), "cache-x".to_string())));
    }
}
