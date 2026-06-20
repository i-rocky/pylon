use super::{App, AppLookupError, AppManager};
use std::sync::Arc;

#[derive(Debug)]
pub struct StaticFileAppManager {
    apps: Vec<Arc<App>>,
}

impl StaticFileAppManager {
    pub fn from_json(raw: &str) -> anyhow::Result<Self> {
        let parsed: Vec<App> = serde_json::from_str(raw)?;
        let mut apps: Vec<Arc<App>> = Vec::with_capacity(parsed.len());
        for mut app in parsed {
            app.recompute_has_flags();
            app.validate().map_err(|e| anyhow::anyhow!(e))?;
            apps.push(Arc::new(app));
        }
        Ok(Self { apps })
    }
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        Self::from_json(&std::fs::read_to_string(path)?)
    }
}

#[async_trait::async_trait]
impl AppManager for StaticFileAppManager {
    async fn by_key(&self, key: &str) -> Result<Option<Arc<App>>, AppLookupError> {
        Ok(self.apps.iter().find(|a| a.key == key && a.enabled).cloned())
    }
    async fn by_id(&self, id: &str) -> Result<Option<Arc<App>>, AppLookupError> {
        Ok(self.apps.iter().find(|a| a.id == id && a.enabled).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"[
        {"name":"Example","id":"app-id","key":"app-key","secret":"app-secret",
         "capacity":2,"client_messages_enabled":true,"subscription_count_enabled":true}
    ]"#;

    #[tokio::test]
    async fn looks_up_by_key_and_id() {
        let m = StaticFileAppManager::from_json(SAMPLE).unwrap();
        let app = m.by_key("app-key").await.unwrap().expect("found by key");
        assert_eq!(app.id, "app-id");
        assert_eq!(app.capacity, 2);
        assert!(m.by_id("app-id").await.unwrap().is_some());
        assert!(m.by_key("nope").await.unwrap().is_none()); // Ok(None), not Err
    }

    #[tokio::test]
    async fn disabled_app_resolves_to_none() {
        let raw = r#"[{"name":"X","id":"a","key":"k","secret":"s","enabled":false}]"#;
        let m = StaticFileAppManager::from_json(raw).unwrap();
        assert!(m.by_id("a").await.unwrap().is_none());
        assert!(m.by_key("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn app_without_enabled_field_defaults_enabled() {
        let m = StaticFileAppManager::from_json(SAMPLE).unwrap(); // SAMPLE has no "enabled"
        assert!(m.by_id("app-id").await.unwrap().is_some());
    }

    #[test]
    fn rejects_app_with_unknown_webhook_event_type() {
        let raw = r#"[
            {"name":"X","id":"a","key":"k","secret":"s",
             "webhooks":[{"url":"https://e.test","event_types":["nope"]}]}
        ]"#;
        let err = StaticFileAppManager::from_json(raw)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown event_type 'nope'"), "got: {err}");
    }

    #[tokio::test]
    async fn loads_app_with_valid_webhooks_and_flags() {
        let raw = r#"[
            {"name":"X","id":"a","key":"k","secret":"s",
             "webhooks":[{"url":"https://e.test","event_types":["channel_occupied"]}]}
        ]"#;
        let m = StaticFileAppManager::from_json(raw).unwrap();
        let app = m.by_id("a").await.unwrap().unwrap();
        assert!(app.has_channel_occupied_webhooks);
    }

    #[tokio::test]
    async fn by_id_and_by_key_share_one_arc() {
        let m = StaticFileAppManager::from_json(SAMPLE).unwrap();
        let a1 = m.by_id("app-id").await.unwrap().unwrap();
        let a2 = m.by_id("app-id").await.unwrap().unwrap();
        // Two lookups of the same app return the SAME backing Arc — no per-lookup clone.
        assert!(
            std::sync::Arc::ptr_eq(&a1, &a2),
            "by_id must share one Arc<App>"
        );
        let k1 = m.by_key("app-key").await.unwrap().unwrap();
        assert!(
            std::sync::Arc::ptr_eq(&a1, &k1),
            "by_key/by_id must share the same Arc<App>"
        );
    }
}
