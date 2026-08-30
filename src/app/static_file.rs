use super::{App, AppLookup, AppLookupError, AppManager};
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

    /// Resolve by predicate, distinguishing a found-but-disabled app (`Disabled`,
    /// REST 403) from no match at all (`NotFound`, REST 401).
    fn resolve<F: Fn(&App) -> bool>(&self, pred: F) -> AppLookup {
        match self.apps.iter().find(|a| pred(a)) {
            Some(a) if a.enabled => AppLookup::Found(a.clone()),
            Some(_) => AppLookup::Disabled,
            None => AppLookup::NotFound,
        }
    }
}

#[async_trait::async_trait]
impl AppManager for StaticFileAppManager {
    async fn by_key(&self, key: &str) -> Result<AppLookup, AppLookupError> {
        Ok(self.resolve(|a| a.key == key))
    }
    async fn by_id(&self, id: &str) -> Result<AppLookup, AppLookupError> {
        Ok(self.resolve(|a| a.id == id))
    }

    fn by_key_cached(&self, key: &str) -> Option<Result<AppLookup, AppLookupError>> {
        // The whole app set is in memory; resolving never does I/O, so the static
        // path always answers synchronously and never offloads.
        Some(Ok(self.resolve(|a| a.key == key)))
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
        let AppLookup::Found(app) = m.by_key("app-key").await.unwrap() else {
            panic!("found by key");
        };
        assert_eq!(app.id, "app-id");
        assert_eq!(app.capacity, 2);
        assert!(matches!(
            m.by_id("app-id").await.unwrap(),
            AppLookup::Found(_)
        ));
        // Ok(NotFound), not Err — unknown keys are a normal answer.
        assert!(matches!(
            m.by_key("nope").await.unwrap(),
            AppLookup::NotFound
        ));
    }

    /// R1: a disabled app is `Disabled`, NOT `NotFound` — the REST layer maps
    /// Disabled to 403 and NotFound to 401.
    #[tokio::test]
    async fn disabled_app_resolves_to_disabled_not_not_found() {
        let raw = r#"[{"name":"X","id":"a","key":"k","secret":"s","enabled":false}]"#;
        let m = StaticFileAppManager::from_json(raw).unwrap();
        assert!(matches!(m.by_id("a").await.unwrap(), AppLookup::Disabled));
        assert!(matches!(m.by_key("k").await.unwrap(), AppLookup::Disabled));
        // The pre-R1 collapse is still available for WS-side callers.
        assert!(m.by_id("a").await.unwrap().into_enabled().is_none());
    }

    #[tokio::test]
    async fn app_without_enabled_field_defaults_enabled() {
        let m = StaticFileAppManager::from_json(SAMPLE).unwrap(); // SAMPLE has no "enabled"
        assert!(matches!(
            m.by_id("app-id").await.unwrap(),
            AppLookup::Found(_)
        ));
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
        let AppLookup::Found(app) = m.by_id("a").await.unwrap() else {
            panic!("expected Found");
        };
        assert!(app.has_channel_occupied_webhooks);
    }

    #[tokio::test]
    async fn by_id_and_by_key_share_one_arc() {
        let m = StaticFileAppManager::from_json(SAMPLE).unwrap();
        let (AppLookup::Found(a1), AppLookup::Found(a2)) = (
            m.by_id("app-id").await.unwrap(),
            m.by_id("app-id").await.unwrap(),
        ) else {
            panic!("expected Found");
        };
        // Two lookups of the same app return the SAME backing Arc — no per-lookup clone.
        assert!(
            std::sync::Arc::ptr_eq(&a1, &a2),
            "by_id must share one Arc<App>"
        );
        let AppLookup::Found(k1) = m.by_key("app-key").await.unwrap() else {
            panic!("expected Found");
        };
        assert!(
            std::sync::Arc::ptr_eq(&a1, &k1),
            "by_key/by_id must share the same Arc<App>"
        );
    }

    #[tokio::test]
    async fn by_key_cached_is_instant_and_matches_by_key() {
        let m = StaticFileAppManager::from_json(SAMPLE).unwrap();
        // Hit: returns Some(Ok(Found(app))) without any I/O.
        let probed = m.by_key_cached("app-key").expect("static always resolves");
        assert!(matches!(probed.unwrap(), AppLookup::Found(_)));
        // Miss-on-unknown: static resolves it as Some(Ok(NotFound)), never None.
        let unknown = m.by_key_cached("nope").expect("static always resolves");
        assert!(matches!(unknown.unwrap(), AppLookup::NotFound));
    }

    #[tokio::test]
    async fn by_key_cached_distinguishes_disabled() {
        let raw = r#"[{"name":"X","id":"a","key":"k","secret":"s","enabled":false}]"#;
        let m = StaticFileAppManager::from_json(raw).unwrap();
        let probed = m.by_key_cached("k").expect("static always resolves");
        assert!(
            matches!(probed.unwrap(), AppLookup::Disabled),
            "disabled app probes as Disabled (WS maps it to the same 4001)"
        );
    }
}
