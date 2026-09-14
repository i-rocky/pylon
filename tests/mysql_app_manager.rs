//! MySQL-backed SqlAppManager integration test. Fail-loud (assumes a MySQL at
//! PYLON_TEST_MYSQL_URL or 127.0.0.1:3307), per the repo's redis_cluster.rs convention.
use pylon::app::{sql::SqlAppManager, AppLookup, AppLookupError, AppManager};
use sqlx::any::AnyPoolOptions;

fn url() -> String {
    std::env::var("PYLON_TEST_MYSQL_URL")
        .unwrap_or_else(|_| "mysql://root:pylon@127.0.0.1:3307/pylon_test".into())
}

const DDL: &str = include_str!("../deploy/db/mysql/001_apps.sql");

#[tokio::test]
async fn mysql_resolves_by_id_and_key_and_filters_disabled() {
    sqlx::any::install_default_drivers();
    let setup = AnyPoolOptions::new()
        .max_connections(2)
        .connect(&url())
        .await
        .expect("connect MySQL (is pylon-test-mysql up on 3307?)");
    sqlx::query(DDL).execute(&setup).await.unwrap();

    let n = uuid::Uuid::new_v4().to_string();
    let (id, key, off_id, off_key) = (
        format!("id-{n}"),
        format!("key-{n}"),
        format!("offid-{n}"),
        format!("offkey-{n}"),
    );
    sqlx::query(
        "INSERT INTO apps (id,`key`,secret,name,capacity,client_messages_enabled,\
         subscription_count_enabled,enabled,webhooks) VALUES (?,?,?,?,?,?,?,?,?),(?,?,?,?,?,?,?,?,?)")
        .bind(&id).bind(&key).bind("sec").bind("M").bind(7_i64).bind(1_i64).bind(1_i64).bind(1_i64)
        .bind("[{\"url\":\"https://e.test\",\"event_types\":[\"channel_occupied\"]}]")
        .bind(&off_id).bind(&off_key).bind("s").bind("Off").bind(0_i64).bind(0_i64).bind(0_i64).bind(0_i64).bind("[]")
        .execute(&setup).await.unwrap();
    sqlx::query("FLUSH PRIVILEGES")
        .execute(&setup)
        .await
        .unwrap();

    let m = SqlAppManager::connect(&url()).await.unwrap();
    let AppLookup::Found(a) = m.by_id(&id).await.unwrap() else {
        panic!("by_id hit");
    };
    assert_eq!(a.key, key);
    assert_eq!(a.capacity, 7);
    assert!(a.client_messages_enabled);
    assert!(a.has_channel_occupied_webhooks); // recompute ran
    let AppLookup::Found(k) = m.by_key(&key).await.unwrap() else {
        panic!("by_key hit");
    };
    assert_eq!(k.id, id);
    // R1: missing -> NotFound (REST 401) ...
    assert!(matches!(
        m.by_id("nope-xyz").await.unwrap(),
        AppLookup::NotFound
    ));
    // ... while disabled -> Disabled (REST 403) — the row exists, enabled=0.
    assert!(matches!(
        m.by_id(&off_id).await.unwrap(),
        AppLookup::Disabled
    ));
    assert!(matches!(
        m.by_key(&off_key).await.unwrap(),
        AppLookup::Disabled
    ));
}

const LEGACY_DDL: &str = "CREATE TABLE apps (\
     id VARCHAR(255) NOT NULL PRIMARY KEY, `key` VARCHAR(255) NOT NULL UNIQUE, \
     secret VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL DEFAULT '', \
     capacity BIGINT NOT NULL DEFAULT 0, client_messages_enabled BIGINT NOT NULL DEFAULT 0, \
     subscription_count_enabled BIGINT NOT NULL DEFAULT 0, enabled BIGINT NOT NULL DEFAULT 1, \
     webhooks TEXT NOT NULL)";

#[tokio::test]
async fn mysql_loads_the_rate_overrides_and_tolerates_a_legacy_table() {
    sqlx::any::install_default_drivers();
    let setup = AnyPoolOptions::new()
        .max_connections(2)
        .connect(&url())
        .await
        .expect("connect MySQL (is pylon-test-mysql up on 3307?)");
    sqlx::query("CREATE DATABASE IF NOT EXISTS pylon_test_ratelimit")
        .execute(&setup)
        .await
        .expect("create the isolated rate-limit database");
    let dsn = swap_database(&url(), "pylon_test_ratelimit");
    let db = AnyPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect the isolated rate-limit database");

    sqlx::query("DROP TABLE IF EXISTS apps")
        .execute(&db)
        .await
        .unwrap();
    sqlx::query(LEGACY_DDL).execute(&db).await.unwrap();
    sqlx::query(
        "INSERT INTO apps (id,`key`,secret,name,capacity,client_messages_enabled,\
         subscription_count_enabled,enabled,webhooks) VALUES \
         ('legacy','legacy-key','sec','Legacy',0,0,0,1,'[]')",
    )
    .execute(&db)
    .await
    .unwrap();
    let m = SqlAppManager::connect(&dsn)
        .await
        .expect("a legacy apps table must still produce a manager");
    let AppLookup::Found(a) = m.by_id("legacy").await.unwrap() else {
        panic!("an apps table without the two override columns must still resolve its apps");
    };
    assert_eq!(a.max_backend_events_per_second, None);
    assert_eq!(a.max_read_requests_per_second, None);

    sqlx::query("DROP TABLE apps").execute(&db).await.unwrap();
    sqlx::query(DDL).execute(&db).await.unwrap();
    sqlx::query(
        "INSERT INTO apps (id,`key`,secret,name,capacity,client_messages_enabled,\
         subscription_count_enabled,enabled,webhooks,max_backend_events_per_second,\
         max_read_requests_per_second) VALUES \
         ('capped','capped-key','sec','Capped',0,0,0,1,'[]',500,NULL),\
         ('free','free-key','sec','Free',0,0,0,1,'[]',0,0)",
    )
    .execute(&db)
    .await
    .unwrap();
    let m = SqlAppManager::connect(&dsn).await.unwrap();
    let AppLookup::Found(capped) = m.by_id("capped").await.unwrap() else {
        panic!("by_id hit");
    };
    assert_eq!(capped.max_backend_events_per_second, Some(500));
    assert_eq!(
        capped.max_read_requests_per_second, None,
        "a NULL column is an absent override, not a zero one"
    );
    let AppLookup::Found(free) = m.by_id("free").await.unwrap() else {
        panic!("by_id hit");
    };
    assert_eq!(free.max_backend_events_per_second, Some(0));
    assert_eq!(free.max_read_requests_per_second, Some(0));

    sqlx::query(
        "INSERT INTO apps (id,`key`,secret,name,capacity,client_messages_enabled,\
         subscription_count_enabled,enabled,webhooks,max_backend_events_per_second,\
         max_read_requests_per_second) VALUES \
         ('negative','negative-key','sec','Negative',0,0,0,1,'[]',-1,NULL)",
    )
    .execute(&db)
    .await
    .unwrap();
    match m.by_id("negative").await {
        Err(AppLookupError::Decode(msg)) => assert!(
            msg.contains("max_backend_events_per_second"),
            "the decode error must name the offending column, got: {msg}"
        ),
        other => {
            panic!("a negative limit is an invalid row, not silently unlimited, got: {other:?}")
        }
    }
}

fn swap_database(url: &str, name: &str) -> String {
    let (prefix, _db) = url
        .rsplit_once('/')
        .expect("the test URL carries a database name to swap");
    format!("{prefix}/{name}")
}
