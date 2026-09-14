//! MongoDB-backed MongoAppManager integration test. Fail-loud (assumes a Mongo at
//! PYLON_TEST_MONGO_URL or 127.0.0.1:27018), per the repo's redis_cluster.rs convention.
use mongodb::{bson::doc, bson::Document, Client};
use pylon::app::{mongo::MongoAppManager, AppLookup, AppLookupError, AppManager};

fn uri() -> String {
    std::env::var("PYLON_TEST_MONGO_URL")
        .unwrap_or_else(|_| "mongodb://127.0.0.1:27018/pylon_test".into())
}

#[tokio::test]
async fn mongo_resolves_by_id_and_key_and_filters_disabled() {
    let client = Client::with_uri_str(&uri())
        .await
        .expect("connect Mongo (is pylon-test-mongo up on 27018?)");
    let coll = client
        .default_database()
        .expect("uri has db")
        .collection::<Document>("apps");

    let n = uuid::Uuid::new_v4().to_string();
    let (id, key, off_id, off_key) = (
        format!("id-{n}"),
        format!("key-{n}"),
        format!("offid-{n}"),
        format!("offkey-{n}"),
    );
    coll.insert_many(vec![
        doc! { "id": &id, "key": &key, "secret": "sec", "name": "Mo", "capacity": 7_i32,
        "client_messages_enabled": true, "subscription_count_enabled": true, "enabled": true,
        "webhooks": [ { "url": "https://e.test", "event_types": ["channel_occupied"] } ] },
        doc! { "id": &off_id, "key": &off_key, "secret": "s", "name": "Off", "capacity": 0_i32,
        "client_messages_enabled": false, "subscription_count_enabled": false, "enabled": false,
        "webhooks": [] },
    ])
    .await
    .unwrap();

    let m = MongoAppManager::connect(&uri()).await.unwrap();
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
    // ... while disabled -> Disabled (REST 403) — the document exists, enabled:false.
    assert!(matches!(
        m.by_id(&off_id).await.unwrap(),
        AppLookup::Disabled
    ));
    assert!(matches!(
        m.by_key(&off_key).await.unwrap(),
        AppLookup::Disabled
    ));
}

/// The URI must name a database. Without one there is no `apps` collection to
/// read, so `connect` fails at startup with an actionable message rather than
/// building a manager whose every lookup would fail at request time.
#[tokio::test]
async fn mongo_uri_without_a_database_name_is_rejected_at_connect() {
    let host_only = uri()
        .rsplit_once('/')
        .map(|(host, _db)| host.to_string())
        .expect("the test URI carries a database name to strip");
    let Err(err) = MongoAppManager::connect(&host_only).await else {
        panic!("a URI with no database name must not produce a manager");
    };
    assert!(
        err.to_string().contains("must include a database name"),
        "the error must say what is wrong with the URI, got: {err}"
    );
}

/// A stored document that fails `App::validate` is a Decode error on EVERY
/// lookup against it — not a `NotFound` (which the REST plane renders 401, as
/// though the caller's credentials were wrong) and not a silently accepted app.
/// Here the secret is blank: a zero-length HMAC key anyone holding the public
/// app key could forge signatures with.
#[tokio::test]
async fn mongo_document_failing_validation_surfaces_as_a_decode_error() {
    let client = Client::with_uri_str(&uri())
        .await
        .expect("connect Mongo (is pylon-test-mongo up on 27018?)");
    let coll = client
        .default_database()
        .expect("uri has db")
        .collection::<Document>("apps");

    let n = uuid::Uuid::new_v4().to_string();
    let (id, key) = (format!("blank-{n}"), format!("blankkey-{n}"));
    coll.insert_one(
        doc! { "id": &id, "key": &key, "secret": "", "name": "Blank", "capacity": 0_i32,
        "client_messages_enabled": false, "subscription_count_enabled": false,
        "enabled": true, "webhooks": [] },
    )
    .await
    .unwrap();

    let m = MongoAppManager::connect(&uri()).await.unwrap();
    for err in [
        m.by_id(&id).await.expect_err("by_id must reject the row"),
        m.by_key(&key)
            .await
            .expect_err("by_key must reject the row"),
    ] {
        match err {
            AppLookupError::Decode(msg) => assert!(
                msg.contains("secret is empty"),
                "the decode error must name the failed rule, got: {msg}"
            ),
            other => panic!("a row that fails validation must be a Decode error, got {other:?}"),
        }
    }
}
