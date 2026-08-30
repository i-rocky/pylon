use super::{App, AppLookupError};
use fred::prelude::*;
use std::sync::Arc;

/// An L2 hit: either a resolved (enabled) app or the "this id/key belongs to a
/// disabled app" marker written by [`RedisAppCache::put_disabled_id`] /
/// [`RedisAppCache::put_disabled_key`]. A miss is the outer `None`.
#[derive(Debug)]
pub enum L2Hit {
    Found(Arc<App>),
    Disabled,
}

/// The stored value marking a disabled app. Deliberately NOT valid JSON (an
/// `App` always serializes to a `{...}` object), so it can never collide with
/// or be mistaken for a cached app document.
const DISABLED_MARKER: &str = "__PYLON_APP_DISABLED__";

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

    pub async fn get_id(&self, id: &str) -> Result<Option<L2Hit>, AppLookupError> {
        self.get(&Self::id_key(id)).await
    }
    pub async fn get_key(&self, key: &str) -> Result<Option<L2Hit>, AppLookupError> {
        self.get(&Self::key_key(key)).await
    }

    async fn get(&self, k: &str) -> Result<Option<L2Hit>, AppLookupError> {
        let raw: Option<String> = self
            .pool
            .get(k)
            .await
            .map_err(|e| AppLookupError::Backend(e.to_string()))?;
        match raw {
            None => Ok(None),
            Some(s) if s == DISABLED_MARKER => Ok(Some(L2Hit::Disabled)),
            Some(s) => {
                let mut app: App =
                    serde_json::from_str(&s).map_err(|e| AppLookupError::Decode(e.to_string()))?;
                app.recompute_has_flags();
                Ok(Some(L2Hit::Found(Arc::new(app))))
            }
        }
    }

    /// Delete both the id- and key-keyed entries for an app (a disabled marker
    /// lives under the same keys, so it is evicted by the same `del`).
    pub async fn del(&self, id: &str, key: &str) -> Result<(), AppLookupError> {
        let _: () = self
            .pool
            .del((Self::id_key(id), Self::key_key(key)))
            .await
            .map_err(|e| AppLookupError::Backend(e.to_string()))?;
        Ok(())
    }

    /// Store the app under both its id- and key-keyed entries with the configured TTL.
    pub async fn put(&self, app: &App) -> Result<(), AppLookupError> {
        let s = serde_json::to_string(app).map_err(|e| AppLookupError::Decode(e.to_string()))?;
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

    /// R1: persist the disabled marker under the id-keyed entry (positive TTL,
    /// like a Found app). The caller only knows the alias it probed with, so
    /// the key alias is marked by its own lookup path.
    pub async fn put_disabled_id(&self, id: &str) -> Result<(), AppLookupError> {
        self.put_disabled_at(&Self::id_key(id)).await
    }

    /// R1: persist the disabled marker under the key-keyed entry.
    pub async fn put_disabled_key(&self, key: &str) -> Result<(), AppLookupError> {
        self.put_disabled_at(&Self::key_key(key)).await
    }

    async fn put_disabled_at(&self, k: &str) -> Result<(), AppLookupError> {
        let exp = Some(Expiration::EX(self.ttl_secs));
        let _: () = self
            .pool
            .set(k, DISABLED_MARKER, exp, None, false)
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
}
