//! App (tenant) definitions and the AppManager seam (static file, SQL, or Mongo).

pub mod cache;
pub mod invalidation;
pub mod l2;
pub mod mongo;
pub mod purger;
pub mod sql;
pub mod static_file;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// The seven webhook event names, in canonical order. The only legal `event_types`.
/// `subscription_count` additionally requires the app's Subscription Count feature
/// flag (`subscription_count_enabled`) at emission time — the webhook doc gates the
/// event on the App-Settings toggle.
pub const WEBHOOK_EVENT_TYPES: [&str; 7] = [
    "channel_occupied",
    "channel_vacated",
    "member_added",
    "member_removed",
    "client_event",
    "cache_miss",
    "subscription_count",
];

/// A per-app webhook endpoint (apps.json `webhooks[]`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct WebhookConfig {
    pub url: String,
    pub event_types: Vec<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct App {
    pub name: String,
    pub id: String,
    pub key: String,
    pub secret: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub client_messages_enabled: bool,
    #[serde(default)]
    pub capacity: u32,
    #[serde(default)]
    pub subscription_count_enabled: bool,
    #[serde(default)]
    pub webhooks: Vec<WebhookConfig>,

    // Precomputed gates (NOT deserialized): set by `recompute_has_flags()` after
    // load so the WS hot path is a single bool read. (mirrors soketi app.ts:203-208)
    #[serde(skip)]
    pub has_channel_occupied_webhooks: bool,
    #[serde(skip)]
    pub has_channel_vacated_webhooks: bool,
    #[serde(skip)]
    pub has_member_added_webhooks: bool,
    #[serde(skip)]
    pub has_member_removed_webhooks: bool,
    #[serde(skip)]
    pub has_client_event_webhooks: bool,
    #[serde(skip)]
    pub has_cache_miss_webhooks: bool,
    #[serde(skip)]
    pub has_subscription_count_webhooks: bool,
}

fn default_enabled() -> bool {
    true
}

impl App {
    /// Recompute the `has_*` gates from `webhooks`. Call once after deserialization.
    pub fn recompute_has_flags(&mut self) {
        let any = |name: &str| {
            self.webhooks
                .iter()
                .any(|w| w.event_types.iter().any(|t| t == name))
        };
        self.has_channel_occupied_webhooks = any("channel_occupied");
        self.has_channel_vacated_webhooks = any("channel_vacated");
        self.has_member_added_webhooks = any("member_added");
        self.has_member_removed_webhooks = any("member_removed");
        self.has_client_event_webhooks = any("client_event");
        self.has_cache_miss_webhooks = any("cache_miss");
        self.has_subscription_count_webhooks = any("subscription_count");
    }

    /// Fail-fast load validation (spec §6): non-empty `event_types`, every entry
    /// one of the seven, non-empty `url`.
    pub fn validate(&self) -> Result<(), String> {
        for (i, w) in self.webhooks.iter().enumerate() {
            if w.url.trim().is_empty() {
                return Err(format!("app '{}' webhook[{i}]: url is empty", self.id));
            }
            if w.event_types.is_empty() {
                return Err(format!(
                    "app '{}' webhook[{i}]: event_types must be non-empty",
                    self.id
                ));
            }
            for t in &w.event_types {
                if !WEBHOOK_EVENT_TYPES.contains(&t.as_str()) {
                    return Err(format!(
                        "app '{}' webhook[{i}]: unknown event_type '{t}'",
                        self.id
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum AppLookupError {
    /// Transient backend failure (DB/Redis down, timeout). Reject retryably; do not cache.
    Backend(String),
    /// A row/document could not be decoded into `App`. Operational bug; reject retryably.
    Decode(String),
}
impl std::fmt::Display for AppLookupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppLookupError::Backend(m) => write!(f, "app store backend error: {m}"),
            AppLookupError::Decode(m) => write!(f, "app row decode error: {m}"),
        }
    }
}
impl std::error::Error for AppLookupError {}

/// The outcome of an app lookup, distinguishing a DISABLED app (found in the
/// store, `enabled=false` — Pusher documents HTTP **403 Forbidden** for it) from
/// a NOT-FOUND id/key (**401**, indistinguishable from a signature failure for
/// anti-enumeration). REST auth maps the three states to 200/403/401; the WS
/// establish path deliberately collapses `Disabled` and `NotFound` into the
/// single 4001 "Could not find app by key" answer.
#[derive(Debug)]
pub enum AppLookup {
    Found(Arc<App>),
    Disabled,
    NotFound,
}

impl AppLookup {
    /// Collapse to the pre-R1 `Option`: `Some` only when found AND enabled.
    /// For call sites that treat a disabled app exactly like a missing one
    /// (WS 4001, webhook/cluster edges).
    pub fn into_enabled(self) -> Option<Arc<App>> {
        match self {
            AppLookup::Found(app) => Some(app),
            AppLookup::Disabled | AppLookup::NotFound => None,
        }
    }
}

impl From<Option<Arc<App>>> for AppLookup {
    /// Lift an `Option<Arc<App>>` (Some = enabled app) into the enum. Used by
    /// in-memory managers and test doubles whose source already filters.
    fn from(v: Option<Arc<App>>) -> Self {
        match v {
            Some(app) => AppLookup::Found(app),
            None => AppLookup::NotFound,
        }
    }
}

#[async_trait::async_trait]
pub trait AppManager: Send + Sync {
    async fn by_key(&self, key: &str) -> Result<AppLookup, AppLookupError>;
    async fn by_id(&self, id: &str) -> Result<AppLookup, AppLookupError>;

    /// SYNCHRONOUS L1-only probe for the per-core establish fast path.
    ///
    /// Returns `Some(result)` ONLY when the answer is known without performing any
    /// I/O (a static-file scan, or an in-memory L1 cache hit). Returns `None` when
    /// resolving would require a DB/Redis round-trip — the caller then offloads the
    /// async [`Self::by_key`] to a tokio task and parks the connection rather than
    /// blocking the worker. The default is `None`: a raw `SqlAppManager` /
    /// `MongoAppManager` has no in-memory tier, so it always offloads.
    fn by_key_cached(&self, _key: &str) -> Option<Result<AppLookup, AppLookupError>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: serde_json::Value) -> App {
        let mut a = serde_json::from_value::<App>(json).unwrap();
        a.recompute_has_flags();
        a
    }

    #[test]
    fn app_without_webhooks_has_all_flags_false() {
        let a = parse(serde_json::json!({
            "name": "t", "id": "app", "key": "k", "secret": "s"
        }));
        assert!(a.webhooks.is_empty());
        assert!(!a.has_channel_occupied_webhooks);
        assert!(!a.has_client_event_webhooks);
        assert!(a.validate().is_ok());
    }

    #[test]
    fn parses_webhooks_and_computes_has_flags() {
        let a = parse(serde_json::json!({
            "name": "t", "id": "app", "key": "k", "secret": "s",
            "webhooks": [
                { "url": "https://e.test/a", "event_types": ["channel_occupied","cache_miss"] },
                { "url": "https://e.test/b", "event_types": ["client_event"],
                  "headers": { "X-Custom": "v" } }
            ]
        }));
        assert_eq!(a.webhooks.len(), 2);
        assert_eq!(a.webhooks[1].headers["X-Custom"], "v");
        assert!(a.has_channel_occupied_webhooks);
        assert!(a.has_cache_miss_webhooks);
        assert!(a.has_client_event_webhooks);
        assert!(!a.has_channel_vacated_webhooks);
        assert!(!a.has_member_added_webhooks);
        assert!(a.validate().is_ok());
    }

    #[test]
    fn subscription_count_is_a_valid_event_type_and_sets_its_flag() {
        let a = parse(serde_json::json!({
            "name": "t", "id": "app", "key": "k", "secret": "s",
            "webhooks": [{ "url": "https://e.test", "event_types": ["subscription_count"] }]
        }));
        assert!(a.has_subscription_count_webhooks);
        assert!(!a.has_channel_occupied_webhooks);
        assert!(a.validate().is_ok());
    }

    #[test]
    fn unknown_event_type_fails_validation() {
        let a = parse(serde_json::json!({
            "name": "t", "id": "app", "key": "k", "secret": "s",
            "webhooks": [{ "url": "https://e.test", "event_types": ["bogus"] }]
        }));
        let err = a.validate().unwrap_err();
        assert!(err.contains("unknown event_type 'bogus'"), "got: {err}");
    }

    #[test]
    fn empty_event_types_fails_validation() {
        let a = parse(serde_json::json!({
            "name": "t", "id": "app", "key": "k", "secret": "s",
            "webhooks": [{ "url": "https://e.test", "event_types": [] }]
        }));
        assert!(a.validate().unwrap_err().contains("non-empty"));
    }

    #[test]
    fn empty_url_fails_validation() {
        let a = parse(serde_json::json!({
            "name": "t", "id": "app", "key": "k", "secret": "s",
            "webhooks": [{ "url": "", "event_types": ["cache_miss"] }]
        }));
        assert!(a.validate().unwrap_err().contains("url is empty"));
    }

    #[test]
    fn app_json_round_trips_and_recomputes_flags() {
        let mut a = parse(serde_json::json!({
            "name":"t","id":"app","key":"k","secret":"s","capacity":5,
            "client_messages_enabled": true, "enabled": true,
            "webhooks":[{"url":"https://e.test","event_types":["channel_occupied"]}]
        }));
        a.recompute_has_flags();
        let json = serde_json::to_string(&a).expect("serialize");
        let mut back: App = serde_json::from_str(&json).expect("deserialize");
        back.recompute_has_flags();
        assert_eq!(back.id, "app");
        assert_eq!(back.capacity, 5);
        assert!(back.client_messages_enabled);
        assert_eq!(back.webhooks.len(), 1);
        assert!(back.has_channel_occupied_webhooks);
    }

    #[test]
    fn app_lookup_error_is_clone() {
        let e = AppLookupError::Backend("x".into());
        let _c = e.clone();
    }

    #[test]
    fn app_lookup_into_enabled_collapses_disabled_and_not_found() {
        let app = Arc::new(parse(serde_json::json!({
            "name": "t", "id": "a", "key": "k", "secret": "s"
        })));
        assert!(AppLookup::Found(app).into_enabled().is_some());
        assert!(AppLookup::Disabled.into_enabled().is_none());
        assert!(AppLookup::NotFound.into_enabled().is_none());
    }

    #[test]
    fn app_lookup_from_option_lifts_found_and_not_found() {
        let app = Arc::new(parse(serde_json::json!({
            "name": "t", "id": "a", "key": "k", "secret": "s"
        })));
        assert!(matches!(AppLookup::from(Some(app)), AppLookup::Found(_)));
        assert!(matches!(AppLookup::from(None), AppLookup::NotFound));
    }
}
