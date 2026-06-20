use super::{App, AppLookupError};
use fred::prelude::*;
use std::sync::Arc;

/// Best-effort shared L2 cache for resolved apps, backed by Redis (fred).
/// Cheap to clone (the fred `Pool` is internally an Arc).
#[derive(Clone)]
pub struct RedisAppCache {
    pool: Pool,
    ttl_secs: i64,
}

impl RedisAppCache {
    pub async fn connect(url: &str, pool_size: usize, ttl_secs: u64) -> anyhow::Result<Self> {
        let config = Config::from_url(url)?;
        let pool = Builder::from_config(config).build_pool(pool_size.max(1))?;
        pool.init().await?;
        Ok(Self {
            pool,
            ttl_secs: ttl_secs as i64,
        })
    }

    fn id_key(id: &str) -> String {
        format!("pylon:app:id:{id}")
    }
    fn key_key(key: &str) -> String {
        format!("pylon:app:key:{key}")
    }

    pub async fn get_id(&self, id: &str) -> Result<Option<Arc<App>>, AppLookupError> {
        self.get(&Self::id_key(id)).await
    }
    pub async fn get_key(&self, key: &str) -> Result<Option<Arc<App>>, AppLookupError> {
        self.get(&Self::key_key(key)).await
    }

    async fn get(&self, k: &str) -> Result<Option<Arc<App>>, AppLookupError> {
        let raw: Option<String> = self
            .pool
            .get(k)
            .await
            .map_err(|e| AppLookupError::Backend(e.to_string()))?;
        match raw {
            None => Ok(None),
            Some(s) => {
                let mut app: App = serde_json::from_str(&s)
                    .map_err(|e| AppLookupError::Decode(e.to_string()))?;
                app.recompute_has_flags();
                Ok(Some(Arc::new(app)))
            }
        }
    }

    /// Store the app under both its id- and key-keyed entries with the configured TTL.
    pub async fn put(&self, app: &App) -> Result<(), AppLookupError> {
        let s =
            serde_json::to_string(app).map_err(|e| AppLookupError::Decode(e.to_string()))?;
        let exp = Some(Expiration::EX(self.ttl_secs));
        let _: () = self
            .pool
            .set(Self::id_key(&app.id), s.clone(), exp.clone(), None, false)
            .await
            .map_err(|e| AppLookupError::Backend(e.to_string()))?;
        let _: () = self
            .pool
            .set(Self::key_key(&app.key), s, exp, None, false)
            .await
            .map_err(|e| AppLookupError::Backend(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::App;

    fn redis_url() -> String {
        std::env::var("PYLON_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6390".into())
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

    #[tokio::test]
    async fn put_then_get_by_id_and_key_round_trips() {
        let c = RedisAppCache::connect(&redis_url(), 2, 60).await.unwrap();
        let app = uniq_app();
        c.put(&app).await.unwrap();
        let by_id = c.get_id(&app.id).await.unwrap().expect("get_id hit");
        assert_eq!(by_id.key, app.key);
        assert!(by_id.has_channel_occupied_webhooks); // recompute ran on read-back
        let by_key = c.get_key(&app.key).await.unwrap().expect("get_key hit");
        assert_eq!(by_key.id, app.id);
    }

    #[tokio::test]
    async fn get_miss_is_ok_none() {
        let c = RedisAppCache::connect(&redis_url(), 2, 60).await.unwrap();
        let n = uuid::Uuid::new_v4();
        assert!(c.get_id(&format!("absent-{n}")).await.unwrap().is_none());
    }
}
