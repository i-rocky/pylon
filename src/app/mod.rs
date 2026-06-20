//! App (tenant) definitions and the AppManager seam (static file now; DB in SP6).

pub mod sql;
pub mod static_file;

use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::Arc;

/// The six webhook event names, in canonical order. The only legal `event_types`.
pub const WEBHOOK_EVENT_TYPES: [&str; 6] = [
    "channel_occupied",
    "channel_vacated",
    "member_added",
    "member_removed",
    "client_event",
    "cache_miss",
];

/// A per-app webhook endpoint (apps.json `webhooks[]`).
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct WebhookConfig {
    pub url: String,
    pub event_types: Vec<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
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
    }

    /// Fail-fast load validation (spec §6): non-empty `event_types`, every entry
    /// one of the six, non-empty `url`.
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

#[derive(Debug)]
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

#[async_trait::async_trait]
pub trait AppManager: Send + Sync {
    async fn by_key(&self, key: &str) -> Result<Option<Arc<App>>, AppLookupError>;
    async fn by_id(&self, id: &str) -> Result<Option<Arc<App>>, AppLookupError>;
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
}
