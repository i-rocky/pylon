use super::{App, AppLookup, AppLookupError, AppManager, WebhookConfig};
use sqlx::any::{AnyPoolOptions, AnyRow};
use sqlx::{AnyPool, Row};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Dialect {
    Sqlite,
    MySql,
    Postgres,
}

impl Dialect {
    fn from_dsn(dsn: &str) -> Self {
        let scheme = dsn.split(':').next().unwrap_or("").to_ascii_lowercase();
        if scheme.starts_with("mysql") || scheme.starts_with("mariadb") {
            Dialect::MySql
        } else if scheme.starts_with("postgres") {
            Dialect::Postgres
        } else {
            Dialect::Sqlite
        }
    }
    /// `key` is reserved in MySQL (needs backticks); non-reserved in SQLite/Postgres.
    fn key_ident(&self) -> &'static str {
        match self {
            Dialect::MySql => "`key`",
            _ => "key",
        }
    }
    /// Bind-parameter placeholder for the single lookup value. Postgres uses $N.
    fn placeholder(&self) -> &'static str {
        match self {
            Dialect::Postgres => "$1",
            _ => "?",
        }
    }
}

/// App store backed by a SQL database via sqlx `Any` (SQLite now; MySQL/Postgres
/// added later by enabling their sqlx features — same code, DSN-selected).
#[derive(Debug)]
pub struct SqlAppManager {
    pub(crate) pool: AnyPool,
    dialect: Dialect,
}

/// Typed column for app lookup to prevent SQL injection via caller-controlled column names.
enum LookupCol {
    Id,
    Key,
}

impl SqlAppManager {
    pub async fn connect(dsn: &str) -> anyhow::Result<Self> {
        sqlx::any::install_default_drivers();
        let dialect = Dialect::from_dsn(dsn);
        let pool = AnyPoolOptions::new()
            .max_connections(8)
            .connect(dsn)
            .await?;
        Ok(Self { pool, dialect })
    }

    fn select_sql(&self) -> String {
        format!(
            "SELECT id, {}, secret, name, capacity, client_messages_enabled, \
                 subscription_count_enabled, enabled, webhooks FROM apps",
            self.dialect.key_ident()
        )
    }

    /// Fetch the row by id/key WITHOUT filtering `enabled` in the WHERE, so a
    /// found-but-disabled row maps to `AppLookup::Disabled` (REST 403) rather
    /// than being erased into `NotFound` (REST 401). R1.
    async fn fetch(&self, col: LookupCol, val: &str) -> Result<AppLookup, AppLookupError> {
        let where_col = match col {
            LookupCol::Id => "id",
            LookupCol::Key => self.dialect.key_ident(),
        };
        let sql = format!(
            "{} WHERE {} = {} LIMIT 1",
            self.select_sql(),
            where_col,
            self.dialect.placeholder()
        );
        let row = sqlx::query(&sql)
            .bind(val)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| AppLookupError::Backend(e.to_string()))?;
        match row {
            None => Ok(AppLookup::NotFound),
            Some(r) => {
                let app = row_to_app(&r)?;
                if app.enabled {
                    Ok(AppLookup::Found(Arc::new(app)))
                } else {
                    Ok(AppLookup::Disabled)
                }
            }
        }
    }
}

fn get_bool(r: &AnyRow, col: &str) -> Result<bool, AppLookupError> {
    r.try_get::<i64, _>(col)
        .map(|v| v != 0)
        .map_err(|e| AppLookupError::Decode(format!("{col}: {e}")))
}

/// Read a column that may come back as `String` (SQLite/Postgres VARCHAR) or
/// `Vec<u8>` (MySQL TEXT mapped to BLOB by sqlx Any) — both cases valid UTF-8.
fn get_text(r: &AnyRow, col: &str) -> Result<String, AppLookupError> {
    if let Ok(s) = r.try_get::<String, _>(col) {
        return Ok(s);
    }
    let bytes: Vec<u8> = r
        .try_get(col)
        .map_err(|e| AppLookupError::Decode(format!("{col} column: {e}")))?;
    String::from_utf8(bytes).map_err(|e| AppLookupError::Decode(format!("{col} utf8: {e}")))
}

fn row_to_app(r: &AnyRow) -> Result<App, AppLookupError> {
    let webhooks_json = get_text(r, "webhooks")?;
    let webhooks: Vec<WebhookConfig> = serde_json::from_str(&webhooks_json)
        .map_err(|e| AppLookupError::Decode(format!("webhooks json: {e}")))?;
    let dec = |e: sqlx::Error| AppLookupError::Decode(e.to_string());
    let mut app = App {
        name: r.try_get("name").map_err(dec)?,
        id: r.try_get("id").map_err(dec)?,
        key: r.try_get("key").map_err(dec)?,
        secret: r.try_get("secret").map_err(dec)?,
        client_messages_enabled: get_bool(r, "client_messages_enabled")?,
        capacity: r.try_get::<i64, _>("capacity").map_err(dec)? as u32,
        subscription_count_enabled: get_bool(r, "subscription_count_enabled")?,
        enabled: get_bool(r, "enabled")?,
        webhooks,
        has_channel_occupied_webhooks: false,
        has_channel_vacated_webhooks: false,
        has_member_added_webhooks: false,
        has_member_removed_webhooks: false,
        has_client_event_webhooks: false,
        has_cache_miss_webhooks: false,
        has_subscription_count_webhooks: false,
    };
    app.recompute_has_flags();
    app.validate().map_err(AppLookupError::Decode)?;
    Ok(app)
}

#[async_trait::async_trait]
impl AppManager for SqlAppManager {
    async fn by_key(&self, key: &str) -> Result<AppLookup, AppLookupError> {
        self.fetch(LookupCol::Key, key).await
    }
    async fn by_id(&self, id: &str) -> Result<AppLookup, AppLookupError> {
        self.fetch(LookupCol::Id, id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a temp-file SQLite DB, seeds the apps table, and returns
    /// (manager, _tmp) — caller must keep `_tmp` alive or the file is deleted.
    async fn seed() -> (SqlAppManager, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let dsn = format!("sqlite://{}?mode=rwc", tmp.path().join("apps.db").display());
        let m = SqlAppManager::connect(&dsn).await.unwrap();
        sqlx::query(include_str!("../../deploy/db/sqlite/001_apps.sql"))
            .execute(&m.pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO apps (id,key,secret,name,capacity,client_messages_enabled,\
             subscription_count_enabled,enabled,webhooks) VALUES \
             ('app-id','app-key','app-secret','Example',2,1,1,1,\
              '[{\"url\":\"https://e.test\",\"event_types\":[\"channel_occupied\"]}]'),\
             ('off-id','off-key','s','Disabled',0,0,0,0,'[]')",
        )
        .execute(&m.pool)
        .await
        .unwrap();
        (m, tmp)
    }

    #[tokio::test]
    async fn by_id_and_by_key_return_the_app() {
        let (m, _tmp) = seed().await;
        let AppLookup::Found(a) = m.by_id("app-id").await.unwrap() else {
            panic!("expected Found");
        };
        assert_eq!(a.key, "app-key");
        assert_eq!(a.capacity, 2);
        assert!(a.client_messages_enabled);
        assert!(a.has_channel_occupied_webhooks); // recompute_has_flags ran
        let AppLookup::Found(k) = m.by_key("app-key").await.unwrap() else {
            panic!("expected Found");
        };
        assert_eq!(k.id, "app-id");
    }

    #[tokio::test]
    async fn missing_app_is_not_found() {
        let (m, _tmp) = seed().await;
        assert!(matches!(
            m.by_id("nope").await.unwrap(),
            AppLookup::NotFound
        ));
        assert!(matches!(
            m.by_key("nope").await.unwrap(),
            AppLookup::NotFound
        ));
    }

    /// R1: an `enabled = 0` row is `Disabled` (REST 403), not `NotFound` (401).
    #[tokio::test]
    async fn disabled_app_is_disabled_not_not_found() {
        let (m, _tmp) = seed().await;
        assert!(matches!(
            m.by_id("off-id").await.unwrap(),
            AppLookup::Disabled
        ));
        assert!(matches!(
            m.by_key("off-key").await.unwrap(),
            AppLookup::Disabled
        ));
    }

    #[tokio::test]
    async fn backend_failure_is_err_not_none() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dsn = format!("sqlite://{}?mode=rwc", tmp.path().join("apps.db").display());
        let m = SqlAppManager::connect(&dsn).await.unwrap(); // no table created
        assert!(matches!(
            m.by_id("x").await,
            Err(AppLookupError::Backend(_))
        ));
    }
}
