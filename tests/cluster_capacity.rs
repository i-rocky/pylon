//! Cluster-wide per-app connection capacity (Task 4.2, audit finding D2).
//!
//! The docs (`website/docs/user-guide/applications.md`) promise that `capacity`
//! is "enforced cluster-wide when using the Redis adapter". Before this task
//! only the NODE-LOCAL `conn_counts` check existed, so N nodes each admitted up
//! to `capacity` connections. These tests prove the real cluster semantics:
//!
//! * Script-level bounds — `ADMIT_APP_LUA` admits below the cap, rejects at it;
//!   `RELEASE_APP_LUA` floors at 0 and a "phantom" release (this node holds no
//!   recorded unit) never steals a unit another node legitimately holds.
//! * Sweeper reclaim — a dead node's per-app counts are subtracted from the
//!   cluster totals (floored at 0) once its node heartbeat has expired.
//! * Cross-node enforcement — two in-process nodes sharing Redis, capacity 1:
//!   the first connection (node A) is admitted, the second (node B, same app)
//!   is rejected with the SAME 4004 the local check sends; closing the first
//!   frees the slot cluster-wide (bounded wait for the release to land).
//! * Dead-node recovery — a node that dies without releases (its bridge is
//!   dropped) has its counts reclaimed by the sweeper within a short-heartbeat
//!   window, after which a new connection on the survivor succeeds.
//! * Fail-open — when the bridge is unavailable, admission still succeeds
//!   locally (a degraded bridge must not lock clients out of the node).
//!
//! Like `percore_cluster.rs`, these talk to a REAL Redis (`PYLON_TEST_REDIS_URL`,
//! default `redis://127.0.0.1:6390`) behind a random key prefix — NEVER
//! FLUSHALL/FLUSHDB — and FAIL LOUD if Redis is unreachable.

mod common;

use common::{connect, established_socket_id, spawn_percore_cluster_with_apps, wait_until, Ws};
use futures_util::StreamExt;
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::redis::keys::Keys;
use pylon::adapter::redis::RedisAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::cluster::adapter::ClusterAdapter;
use pylon::cluster::bridge;
use pylon::server::config::ServerConfig;
use pylon::webhook::WebhookHandle;
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

/// The standard app with capacity **1** — the smallest cap that can be hit
/// across two nodes. Both nodes of a test resolve THIS app.
const CAP1_APPS: &str = r#"[
    {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
     "capacity":1,"client_messages_enabled":true}
]"#;

/// Test Redis URL: `PYLON_TEST_REDIS_URL` or the documented default (port 6390).
fn test_redis_url() -> String {
    std::env::var("PYLON_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6390".to_string())
}

/// A random, run-unique key prefix (never FLUSHALL/FLUSHDB).
fn random_prefix() -> String {
    format!("pylontest:{}", Uuid::new_v4())
}

/// A Redis-adapter config against the test Redis under `prefix`.
fn redis_test_config(prefix: &str) -> ServerConfig {
    ServerConfig {
        adapter: "redis".into(),
        redis_url: test_redis_url(),
        redis_prefix: prefix.into(),
        ..ServerConfig::default()
    }
}

/// One connected fred client for direct hash reads/writes (fabricating cluster
/// state and asserting on it). Fails loud when Redis is unreachable.
async fn fred_client() -> fred::clients::SubscriberClient {
    use fred::interfaces::ClientLike;
    use fred::prelude::Builder;
    let config =
        fred::prelude::Config::from_url(test_redis_url().as_str()).expect("redis url parses");
    let client = Builder::from_config(config)
        .build_subscriber_client()
        .expect("fred client builds");
    client.init().await.expect("test Redis must be reachable");
    client
}

/// `HGET key field` as i64 (0 when the field/key is absent).
async fn hget_i64(client: &fred::clients::SubscriberClient, key: &str, field: &str) -> i64 {
    use fred::interfaces::HashesInterface;
    client
        .hget::<Option<i64>, _, _>(key, field)
        .await
        .expect("HGET must not error")
        .unwrap_or(0)
}

/// Whether `key` exists in Redis.
async fn key_exists(client: &fred::clients::SubscriberClient, key: &str) -> bool {
    use fred::interfaces::KeysInterface;
    client
        .exists::<i64, _>(key)
        .await
        .expect("EXISTS must not error")
        != 0
}

// ── 1. Script-level bounds ──────────────────────────────────────────────────

/// `ADMIT_APP_LUA` / `RELEASE_APP_LUA` bounds, driven through the adapter's own
/// script call sites against the real Redis: below-cap admits, at-cap rejects,
/// release floors at 0 (never negative), and a phantom release (this node holds
/// no recorded unit) never decrements the cluster total.
#[tokio::test]
async fn admit_release_script_bounds() {
    let prefix = random_prefix();
    let keys = Keys::new(&prefix);
    let adapter = RedisAdapter::new(&redis_test_config(&prefix))
        .await
        .expect("adapter must connect to the test Redis");
    let client = fred_client().await;

    // Below the cap: both admissions succeed and each takes exactly one unit.
    assert_eq!(adapter.cluster_admit_app("t1", 2).await, Some(true));
    assert_eq!(adapter.cluster_admit_app("t1", 2).await, Some(true));
    assert_eq!(
        hget_i64(&client, &keys.appconns(), "t1").await,
        2,
        "two admissions must take two cluster units"
    );

    // At the cap: rejected, and the reject must NOT take a unit.
    assert_eq!(adapter.cluster_admit_app("t1", 2).await, Some(false));
    assert_eq!(
        hget_i64(&client, &keys.appconns(), "t1").await,
        2,
        "a rejected admission must not change the cluster count"
    );

    // A release frees exactly one unit, so the next admission is accepted again.
    adapter.cluster_release_app("t1").await;
    assert_eq!(
        hget_i64(&client, &keys.appconns(), "t1").await,
        1,
        "one release must give back one unit"
    );
    assert_eq!(adapter.cluster_admit_app("t1", 2).await, Some(true));

    // Releases past zero floor at 0 — never negative.
    for _ in 0..5 {
        adapter.cluster_release_app("t1").await;
    }
    assert_eq!(
        hget_i64(&client, &keys.appconns(), "t1").await,
        0,
        "releasing past zero must floor at 0"
    );
    // …and the floor leaves no residue: a fresh capacity-1 admission succeeds.
    assert_eq!(adapter.cluster_admit_app("t1", 1).await, Some(true));

    // Phantom-release guard: a unit held by ANOTHER node (fabricated directly in
    // Redis) is never stolen by this node releasing more than it holds.
    use fred::interfaces::HashesInterface;
    let _: () = client
        .hset(&keys.appconns(), ("t2", 1))
        .await
        .expect("HSET must not error");
    adapter.cluster_release_app("t2").await; // this node holds no t2 unit
    assert_eq!(
        hget_i64(&client, &keys.appconns(), "t2").await,
        1,
        "a phantom release must not steal another node's unit"
    );

    // Balance the books: give back the units this test took.
    adapter.cluster_release_app("t1").await;
    adapter.cluster_release_app("t2").await;

    use fred::interfaces::ClientLike;
    let _ = client.quit().await;
}

/// The sweeper's dead-node reclaim: a node whose `node:{id}` liveness key is
/// gone (TTL-expired) has its per-app counts subtracted from the cluster totals
/// (floored at 0), its `nodeconns` hash deleted, and itself pruned from the
/// `nodes` set — in ONE sweep pass.
#[tokio::test]
async fn sweeper_reclaims_dead_node_connection_counts() {
    let prefix = random_prefix();
    let keys = Keys::new(&prefix);
    let adapter = RedisAdapter::new(&redis_test_config(&prefix))
        .await
        .expect("adapter must connect to the test Redis");
    let client = fred_client().await;

    // Fabricate a dead node: listed in `nodes`, its `node:{id}` key GONE (the
    // sweeper's dead-node test), holding 2 connections of app "t4" whose 2
    // cluster units are all its own.
    use fred::interfaces::{HashesInterface, SetsInterface};
    let _: () = client.sadd(keys.nodes(), "ghost").await.unwrap();
    let _: () = client.hset(&keys.appconns(), ("t4", 2)).await.unwrap();
    let _: () = client
        .hset(keys.nodeconns("ghost"), ("t4", 2))
        .await
        .unwrap();

    // A second dead node whose claim EXCEEDS the cluster total (1): the reclaim
    // must floor at 0, not go negative.
    let _: () = client.sadd(keys.nodes(), "ghost2").await.unwrap();
    let _: () = client.hset(&keys.appconns(), ("t5", 1)).await.unwrap();
    let _: () = client
        .hset(keys.nodeconns("ghost2"), ("t5", 5))
        .await
        .unwrap();

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let _ = adapter.sweep_now(&WebhookHandle::null(), now_ms).await;

    // Both apps' cluster totals were reclaimed, floored at 0.
    assert_eq!(
        hget_i64(&client, &keys.appconns(), "t4").await,
        0,
        "the dead node's units must be subtracted from the cluster total"
    );
    assert_eq!(
        hget_i64(&client, &keys.appconns(), "t5").await,
        0,
        "an over-claiming dead node must floor the total at 0, never negative"
    );
    // The dead nodes' per-node hashes are deleted and they are pruned from `nodes`.
    assert!(
        !key_exists(&client, &keys.nodeconns("ghost")).await,
        "the dead node's nodeconns hash must be deleted"
    );
    assert!(
        !key_exists(&client, &keys.nodeconns("ghost2")).await,
        "the second dead node's nodeconns hash must be deleted"
    );
    let members: Vec<String> = client.smembers(keys.nodes()).await.unwrap();
    assert!(
        !members.contains(&"ghost".to_string()) && !members.contains(&"ghost2".to_string()),
        "dead nodes must be pruned from the nodes set (got {members:?})"
    );
    // …and the reclaimed capacity is immediately usable again.
    assert_eq!(
        adapter.cluster_admit_app("t4", 2).await,
        Some(true),
        "capacity must be usable immediately after the reclaim"
    );
    adapter.cluster_release_app("t4").await;

    use fred::interfaces::ClientLike;
    let _ = client.quit().await;
}

// ── 2. Cross-node enforcement on two real in-process nodes ──────────────────

/// Read frames from a JUST-CONNECTED client until the capacity rejection
/// arrives: the `pusher:error` frame carrying code 4004 (sent before the WS
/// Close). Fails the test on any other terminal event.
async fn expect_capacity_reject(ws: &mut Ws) {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                if let Ok(v) = serde_json::from_str::<Value>(&t) {
                    if v["event"] == "pusher:error" {
                        assert_eq!(
                            v["data"]["code"], 4004,
                            "the cluster capacity reject must use the same 4004 code as the local check (got {v})"
                        );
                        return;
                    }
                    // connection_established or anything else: keep reading.
                }
            }
            // The WS Close(4004) follows the error frame — either terminal event
            // without an error frame means the node did NOT reject: fall through
            // to the panic below.
            Ok(Some(Ok(Message::Close(_)))) => {
                panic!("connection closed without a pusher:error 4004 frame")
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => panic!("ws error while awaiting the capacity reject: {e}"),
            Ok(None) => panic!("stream ended without a pusher:error 4004 frame"),
            Err(_) => panic!("timed out awaiting the capacity reject"),
        }
    }
}

/// Two in-process percore nodes sharing one Redis, app capacity 1: the first
/// connection (node A) is admitted; the second (node B, same app) is rejected
/// with 4004; closing the first releases the slot cluster-wide and a NEW
/// connection on node B then succeeds (bounded wait for the release to land).
#[tokio::test]
async fn cluster_capacity_enforced_across_nodes() {
    let prefix = random_prefix();
    let (addr_a, _guard_a) = spawn_percore_cluster_with_apps(&prefix, CAP1_APPS, |_| {}).await;
    let (addr_b, _guard_b) = spawn_percore_cluster_with_apps(&prefix, CAP1_APPS, |_| {}).await;

    // 1. The first connection, on node A, is admitted.
    let mut ws_a = connect(addr_a, "?protocol=7").await;
    let _sid_a = established_socket_id(&mut ws_a).await;

    // 2. The second connection, on node B for the SAME app, is rejected 4004 —
    //    B's node-local count is 0, so only the CLUSTER check can reject it.
    let mut ws_b = connect(addr_b, "?protocol=7").await;
    expect_capacity_reject(&mut ws_b).await;

    // 3. Close node A's connection. Its worker fires the cluster release at the
    //    bridge; bounded-wait on the observable (the Redis cluster count == 0).
    drop(ws_a);
    let keys = Keys::new(&prefix);
    let client = fred_client().await;
    let released = wait_until(Duration::from_secs(10), || async {
        hget_i64(&client, &keys.appconns(), "app").await == 0
    })
    .await;
    assert!(
        released,
        "closing the admitted connection must release the cluster unit within 10s"
    );

    // 4. A NEW connection on node B now succeeds.
    let mut ws_b2 = connect(addr_b, "?protocol=7").await;
    let _sid_b2 = established_socket_id(&mut ws_b2).await;
    drop(ws_b2);

    use fred::interfaces::ClientLike;
    let _ = client.quit().await;
}

// ── 3. Dead-node reclaim on two real nodes (short heartbeat) ────────────────

/// A node that dies WITHOUT releasing (its bridge is dropped, so its close-time
/// releases fire into a dead channel and vanish) does not leak capacity
/// forever: once its node heartbeat lapses, the sweeper on the survivor
/// reclaims its counts and a new connection succeeds (bounded wait; short
/// heartbeat + sweep interval so the reclaim happens within seconds).
#[tokio::test]
async fn dead_node_capacity_reclaimed_by_sweeper() {
    let prefix = random_prefix();
    // Short heartbeat: the dead node's `node:{id}` key expires after 3s and the
    // sweeper ticks every 1s → the reclaim lands within a few seconds.
    let tune = |c: &mut ServerConfig| {
        c.redis_node_heartbeat_secs = 1;
        c.redis_sweep_interval_secs = 1;
    };
    let (addr_a, _guard_a) = spawn_percore_cluster_with_apps(&prefix, CAP1_APPS, tune).await;
    let (addr_b, guard_b) = spawn_percore_cluster_with_apps(&prefix, CAP1_APPS, tune).await;

    // Node B holds the only slot.
    let mut ws_b = connect(addr_b, "?protocol=7").await;
    let _sid_b = established_socket_id(&mut ws_b).await;
    // Sanity: while B's connection is live, node A is rejected.
    let mut rej = connect(addr_a, "?protocol=7").await;
    expect_capacity_reject(&mut rej).await;

    // Kill node B without releases: dropping the guard tears down its bridge
    // (heartbeats stop; its worker's releases fire at a closed channel → dropped).
    drop(ws_b);
    drop(guard_b);

    // Bounded wait on the observable: the sweeper on node A reclaims B's counts
    // (the Redis cluster count returns to 0).
    let keys = Keys::new(&prefix);
    let client = fred_client().await;
    let reclaimed = wait_until(Duration::from_secs(20), || async {
        hget_i64(&client, &keys.appconns(), "app").await == 0
    })
    .await;
    assert!(
        reclaimed,
        "the sweeper must reclaim the dead node's counts within 20s (short heartbeat)"
    );

    // The freed slot is immediately usable on the survivor.
    let mut ws_a = connect(addr_a, "?protocol=7").await;
    let _sid_a = established_socket_id(&mut ws_a).await;
    drop(ws_a);

    use fred::interfaces::ClientLike;
    let _ = client.quit().await;
}

// ── 4. Fail-open when the bridge is unavailable ─────────────────────────────

/// When the bridge is unavailable, `ClusterHandle::admit_app` returns `None`
/// and the worker FAILS OPEN: a connection whose local checks pass is admitted
/// even though the cluster count (fabricated at capacity in Redis) would have
/// rejected it. A degraded bridge must not lock clients out of the node.
#[tokio::test]
async fn admission_fails_open_when_bridge_unavailable() {
    let prefix = random_prefix();
    let keys = Keys::new(&prefix);

    // A real bridge is started (proving the connect path), its handle cloned,
    // then the bridge DROPPED: the handle's command channel is closed — the
    // exact state a worker sees when the bridge runtime is gone.
    let local = Arc::new(LocalAdapter::new(
        Arc::new(Registry::new()),
        Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
    ));
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(CAP1_APPS).unwrap());
    let bridge = bridge::start(&redis_test_config(&prefix), local.clone(), apps.clone())
        .expect("bridge must start against the test Redis");
    let stale_handle = bridge.handle();
    drop(bridge);

    // The None path itself: the closed channel cannot carry the admission, so
    // the verdict is "unavailable" — never a silent reject.
    assert_eq!(
        stale_handle.admit_app("app", 1),
        None,
        "a dropped bridge must yield None (fail-open), not a rejection"
    );

    // Fabricate a cluster already AT capacity in Redis (another node's unit).
    let client = fred_client().await;
    use fred::interfaces::HashesInterface;
    let _: () = client.hset(&keys.appconns(), ("app", 1)).await.unwrap();

    // Hand-roll ONE clustered percore node whose only bridge handle is the
    // stale one: `clustered = true` + `cluster = Some(stale)`, mirroring the
    // harness wiring minus the live bridge.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let mut config = redis_test_config(&prefix);
    config.bind = "127.0.0.1".into();
    config.port = port;
    config.workers = 1;

    let worker_adapter: Arc<dyn Adapter> =
        Arc::new(ClusterAdapter::new(local.clone(), stale_handle.clone()));
    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = shutdown.clone();
    let worker_config = config.clone();
    let worker_apps = apps.clone();
    let worker_webhooks = WebhookHandle::null();
    let worker_runtime = tokio::runtime::Handle::current();
    let worker = std::thread::spawn(move || {
        let _ = pylon::transport::run_percore(
            worker_config,
            worker_apps,
            worker_adapter,
            Arc::new(Default::default()),
            Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
            Arc::new(AtomicUsize::new(0)),
            worker_webhooks,
            None,
            worker_shutdown,
            Some(local),
            true,
            Some(stale_handle),
            None,
            worker_runtime,
        );
    });

    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let ready = wait_until(Duration::from_secs(5), || async {
        tokio::net::TcpStream::connect(addr).await.is_ok()
    })
    .await;
    assert!(ready, "the fail-open node must bind {addr} within 5s");

    // The connection is ADMITTED despite the cluster being at capacity: the
    // node-local check passed and the unavailable cluster check fails open.
    let mut ws = connect(addr, "?protocol=7").await;
    let _sid = established_socket_id(&mut ws).await;
    drop(ws);

    // Stop the worker; join it so the test leaves no spinning thread behind.
    shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = worker.join();

    use fred::interfaces::ClientLike;
    let _ = client.quit().await;
}
