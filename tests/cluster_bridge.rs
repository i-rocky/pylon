//! Integration test for the percore [`ClusterBridge`].
//!
//! The bridge owns a DEDICATED tokio runtime (on its own OS thread) hosting a
//! `RedisAdapter`; the percore workers fire fire-and-forget commands at it over a cheap-
//! clone, `Send` [`ClusterHandle`]. This test proves the runtime starts (a real connect to
//! the test Redis), the handle clones and crosses a thread boundary, a smoke `publish`
//! returns immediately without panicking, and dropping the bridge tears it down cleanly.
//!
//! Like `redis_cluster.rs` it talks to a REAL Redis (`PYLON_TEST_REDIS_URL`, default
//! `redis://127.0.0.1:6390`) and isolates every run behind a random key prefix — it NEVER
//! issues FLUSHALL/FLUSHDB. It FAILS LOUD if Redis is unreachable (an explicit assert, the
//! same convention as `redis_cluster.rs`) — there is no silent skip: a dead test Redis
//! must fail the gate, not quietly void this suite.
//!
//! [`ClusterBridge`]: pylon::cluster::bridge::ClusterBridge
//! [`ClusterHandle`]: pylon::cluster::bridge::ClusterHandle

use pylon::adapter::local::LocalAdapter;
use pylon::adapter::redis::client::RedisClients;
use pylon::adapter::redis::keys::Keys;
use pylon::adapter::redis::RedisAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::cluster::bridge;
use pylon::connection::handle::{ConnectionHandle, Mailbox};
use pylon::presence::member::PresenceMember;
use pylon::protocol::event::ServerEvent;
use pylon::protocol::socket_id::SocketId;
use pylon::server::config::ServerConfig;
use pylon::webhook::dispatcher::SystemClock;
use pylon::webhook::transport::{RecordingTransport, WebhookTransport};
use pylon::webhook::WebhookHandle;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Test Redis URL: `PYLON_TEST_REDIS_URL` or the documented test default (port 6390).
fn test_redis_url() -> String {
    std::env::var("PYLON_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6390".to_string())
}

/// A random, run-unique key prefix for isolation on a shared Redis.
fn random_prefix() -> String {
    format!("pylontest:{}", Uuid::new_v4())
}

/// Build a `ServerConfig` for the Redis adapter against the test Redis with a random prefix.
fn redis_test_config(prefix: &str) -> ServerConfig {
    ServerConfig {
        adapter: "redis".into(),
        redis_url: test_redis_url(),
        redis_prefix: prefix.into(),
        ..ServerConfig::default()
    }
}

/// `host:port` of the test Redis URL, for a cheap reachability probe.
fn redis_host_port() -> String {
    test_redis_url()
        .strip_prefix("redis://")
        .unwrap_or("127.0.0.1:6390")
        .trim_end_matches('/')
        .to_string()
}

/// Whether the test Redis accepts a TCP connection. Used to FAIL LOUD (assert) when
/// Redis is unreachable — matching `redis_cluster.rs`, there is no silent skip.
fn redis_reachable() -> bool {
    use std::net::ToSocketAddrs;
    match redis_host_port()
        .to_socket_addrs()
        .ok()
        .and_then(|mut it| it.next())
    {
        Some(sa) => TcpStream::connect_timeout(&sa, Duration::from_millis(500)).is_ok(),
        None => false,
    }
}

#[tokio::test]
async fn cluster_bridge_starts_clones_publishes_and_drops_cleanly() {
    assert!(
        redis_reachable(),
        "cluster_bridge requires Redis; set PYLON_TEST_REDIS_URL (default redis://127.0.0.1:6390) — refusing to silently pass"
    );

    // The whole body is bounded so a wedged Redis or a hung shutdown fails the test fast
    // instead of stalling CI. The bridge runs on its OWN runtime thread, so this outer
    // tokio runtime only hosts the `WebhookHandle::null()` drainer and this timeout.
    tokio::time::timeout(Duration::from_secs(8), async {
        let cfg = redis_test_config(&random_prefix());
        // The SAME `LocalAdapter` the percore workers would broadcast through.
        let local = Arc::new(LocalAdapter::new(
            Arc::new(Registry::new()),
            Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
        ));
        let webhooks = WebhookHandle::null();
        // The bridge resolves per-app flags itself; a single app is enough for the smoke.
        let apps: Arc<dyn AppManager> = Arc::new(
            StaticFileAppManager::from_json(r#"[{"name":"T","id":"app","key":"k","secret":"s"}]"#)
                .expect("apps json must parse"),
        );

        // 1. Start: a real connect to the test Redis must succeed. Webhooks are attached
        //    AFTER start (mirroring `main.rs`'s deferred-webhooks wiring); here the null
        //    sink is fine — the smoke publish below fires no webhook-bearing command.
        //    No worker fleet backs this bridge, so an empty per-app counter map is the
        //    correct `conn_counts` (the heartbeat's outage re-seed then has nothing to
        //    re-seed, which is exactly right for a fleet-less bridge).
        let bridge = bridge::start(&cfg, local, apps, Arc::new(Default::default()))
            .expect("ClusterBridge::start must connect to the test Redis and report ready");
        bridge.attach_webhooks(webhooks);

        // The live node id is non-empty (a UUID minted by the adapter).
        assert!(
            !bridge.handle().node_id().is_empty(),
            "the bridge handle must carry the real cluster node id"
        );

        // 2. The handle clones and is `Send`: move a clone into a plain OS thread and use
        //    it there (node_id + a smoke publish), exactly as a percore worker would.
        let worker_handle = bridge.handle();
        let node_id_from_thread = std::thread::spawn(move || {
            let id = worker_handle.node_id().to_string();
            // 3. Smoke publish: `try_send`s and returns immediately — must not panic.
            worker_handle.publish(
                Arc::from("app"),
                Arc::from("chan"),
                "{\"event\":\"x\"}".to_string(),
                None,
            );
            id
        })
        .join()
        .expect("worker thread must not panic");

        assert_eq!(
            node_id_from_thread,
            bridge.handle().node_id(),
            "the node id seen on the worker thread must match the bridge's"
        );

        // 4. A publish on THIS task's clone is likewise immediate and panic-free.
        bridge.handle().publish(
            Arc::from("app"),
            Arc::from("chan2"),
            "{\"event\":\"y\"}".to_string(),
            None,
        );

        // 5. Dropping the bridge signals shutdown and joins the runtime thread — it must
        //    not hang (the surrounding timeout would catch a hang and fail the test).
        drop(bridge);
    })
    .await
    .expect("cluster_bridge test must not hang (Redis up? shutdown clean?)");
}

/// A presence join rejected by the cluster cap must not swallow the node's 0→1 Redis
/// `SUBSCRIBE`.
///
/// `node_first` is a one-shot token: exactly one in-flight command carries it for a given
/// node-local 0→1 edge on a channel. This drives the exact interleaving the bridge sees —
/// the rejected joiner's command carrying the edge, drained BEFORE the admitted joiner's
/// command that carries `node_first = false` — by firing both at the handle in order,
/// which the bridge drains FIFO. If the reject arm returns before taking the pub/sub
/// edge, the node holds a live presence member of a channel it is not a Redis subscriber
/// of, and every cross-node frame for that channel is lost.
///
/// The membership reconciler would re-subscribe on its next tick and mask the defect, so
/// it is stepped out of the way rather than raced: a warm-up channel this node holds a
/// member of before the bridge starts gives the reconciler's immediate first tick
/// something observable, and that tick's `NUMSUB` gates the rest of the test. Its member
/// snapshot is therefore taken before the presence connections below exist, and with
/// `redis_presence_heartbeat_secs` at an hour there is no second tick. What remains
/// asserts the join path itself, not the repair loop behind it.
#[tokio::test]
async fn capacity_rejected_presence_join_keeps_the_node_subscribed() {
    assert!(
        redis_reachable(),
        "cluster_bridge requires Redis; set PYLON_TEST_REDIS_URL (default redis://127.0.0.1:6390) — refusing to silently pass"
    );

    tokio::time::timeout(Duration::from_secs(15), async {
        let prefix = random_prefix();
        let mut cfg = redis_test_config(&prefix);
        cfg.max_presence_members = 1;
        cfg.redis_presence_heartbeat_secs = 3600;
        let channel = "presence-deaf";
        let keys = Keys::new(&prefix);
        let msg_key = keys.msg("app", channel);
        let warmup_key = keys.msg("app", "warmup-gate");

        // Node A fills the cluster roster to the cap with `u1`, so on node B `u1` is an
        // existing member (admitted) and `u2` is a new distinct user (rejected).
        let node_a = RedisAdapter::new(&cfg)
            .await
            .expect("node A's RedisAdapter must connect to the test Redis");
        let (a_sid, _a_handle, u1) = presence_conn("u1");
        node_a
            .cluster_presence_join("app", channel, &u1, &a_sid, Some(1))
            .await
            .expect("seeding the roster on node A must reach Redis")
            .expect("u1 is the first member and must be admitted");

        let local = Arc::new(LocalAdapter::new(
            Arc::new(Registry::new()),
            Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
        ));
        let apps: Arc<dyn AppManager> = Arc::new(
            StaticFileAppManager::from_json(r#"[{"name":"T","id":"app","key":"k","secret":"s"}]"#)
                .expect("apps json must parse"),
        );
        let (_warmup_sid, warmup_handle, _) = presence_conn("warmup");
        local
            .subscribe("app", "warmup-gate", warmup_handle, None)
            .await;
        let bridge = bridge::start(&cfg, local.clone(), apps, Arc::new(Default::default()))
            .expect("ClusterBridge::start must connect to the test Redis and report ready");
        bridge.attach_webhooks(WebhookHandle::null());
        require_numsub(&warmup_key, 1, Duration::from_secs(5)).await;

        // Node B's worker half: `u2` takes the node-local 0→1 edge, `u1` follows on the
        // same channel, so the node still holds a member when `u2` is rejected.
        let (rejected_sid, rejected_handle, u2) = presence_conn("u2");
        let (mut rejected_rx, rejected_mailbox) = mailbox_pair();
        let (admitted_sid, admitted_handle, u1_on_b) = presence_conn("u1");
        let (_admitted_rx, admitted_mailbox) = mailbox_pair();
        let first = local
            .subscribe("app", channel, rejected_handle, Some(u2.clone()))
            .await;
        assert_eq!(
            first.subscription_count, 1,
            "the rejected joiner must be the node-local 0→1 edge"
        );
        let second = local
            .subscribe("app", channel, admitted_handle, Some(u1_on_b.clone()))
            .await;
        assert_eq!(
            second.subscription_count, 2,
            "the admitted joiner must NOT carry the node-local 0→1 edge"
        );

        let handle = bridge.handle();
        handle.presence_subscribe(
            Arc::from("app"),
            Arc::from(channel),
            u2,
            rejected_sid,
            rejected_mailbox,
            true,
        );
        handle.presence_subscribe(
            Arc::from("app"),
            Arc::from(channel),
            u1_on_b,
            admitted_sid,
            admitted_mailbox,
            false,
        );

        let rejection = tokio::time::timeout(Duration::from_secs(5), rejected_rx.recv())
            .await
            .expect("the cap rejection must arrive")
            .expect("the rejected joiner's mailbox must stay open");
        match *rejection {
            ServerEvent::SubscriptionError { status, .. } => {
                assert_eq!(status, 4004, "the cap rejection is a 4004")
            }
            other => panic!("expected a 4004 subscription_error, got {other:?}"),
        }

        require_numsub(&msg_key, 1, Duration::from_secs(5)).await;

        drop(bridge);
    })
    .await
    .expect("capacity-reject subscription test must not hang (Redis up?)");
}

/// A presence connection: its socket id, a `ConnectionHandle` for the node-local join, and
/// the member the bridge command carries.
fn presence_conn(user_id: &str) -> (SocketId, ConnectionHandle, PresenceMember) {
    let socket_id = SocketId::generate();
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let handle = ConnectionHandle {
        socket_id,
        mailbox: Mailbox::new(tx, None, None),
    };
    let member = PresenceMember {
        user_id: user_id.into(),
        user_info: serde_json::json!({ "name": user_id }),
    };
    (socket_id, handle, member)
}

/// A readable mailbox for the frames the BRIDGE sends a joining connection.
fn mailbox_pair() -> (tokio::sync::mpsc::Receiver<Box<ServerEvent>>, Mailbox) {
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    (rx, Mailbox::new(tx, None, None))
}

/// Bounded wait for Redis to report at least `want` subscribers on `channel`. The server's
/// own `PUBSUB NUMSUB` is the only thing that settles whether a SUBSCRIBE is really live,
/// so this gates on it rather than sleeping. Polled from a plain command connection —
/// RESP2 forbids ordinary commands on a connection in subscribe context.
async fn require_numsub(channel: &str, want: i64, bound: Duration) {
    use fred::interfaces::PubsubInterface;
    let clients = RedisClients::connect(&test_redis_url(), 1)
        .await
        .expect("fred clients must connect to the test Redis");
    let mut seen = -1;
    let deadline = tokio::time::Instant::now() + bound;
    while tokio::time::Instant::now() < deadline {
        let counts: std::collections::HashMap<String, i64> = clients
            .pool
            .next()
            .pubsub_numsub(channel)
            .await
            .unwrap_or_default();
        seen = counts.get(channel).copied().unwrap_or(0);
        if seen >= want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "Redis never reported >= {want} subscriber(s) on {channel} within {bound:?} \
         (last saw {seen}) — the node is deaf to a channel it holds members for"
    );
}

/// How many subscribers Redis currently reports on `channel`.
async fn numsub(channel: &str) -> i64 {
    use fred::interfaces::PubsubInterface;
    let clients = RedisClients::connect(&test_redis_url(), 1)
        .await
        .expect("fred clients must connect to the test Redis");
    let counts: std::collections::HashMap<String, i64> = clients
        .pool
        .next()
        .pubsub_numsub(channel)
        .await
        .unwrap_or_default();
    counts.get(channel).copied().unwrap_or(0)
}

/// The app the webhook-bearing tests below resolve, subscribed to every cluster
/// edge the drain loop can emit EXCEPT `subscription_count` — leaving that one
/// out keeps the recorded list to the edges each test is actually about.
const WEBHOOK_APPS: &str = r#"[{"name":"T","id":"app","key":"k","secret":"s",
    "webhooks":[{"url":"http://127.0.0.1:1/pusher/webhooks",
                 "event_types":["channel_occupied","channel_vacated",
                                "member_added","member_removed","cache_miss"]}]}]"#;

fn webhook_apps() -> Arc<dyn AppManager> {
    Arc::new(StaticFileAppManager::from_json(WEBHOOK_APPS).expect("apps json must parse"))
}

/// A real webhook dispatcher backed by a `RecordingTransport`: the drain loop's
/// `enqueue` is batched and signed exactly as in production, captured in memory.
/// `vacated_grace_ms` is 0 so `channel_vacated` / `member_removed` deliver inline
/// in enqueue order rather than through the debounced grace path.
fn recording_webhooks() -> (WebhookHandle, RecordingTransport) {
    let transport = RecordingTransport::new();
    let recorded = transport.clone();
    let handle = pylon::webhook::spawn(
        webhook_apps(),
        move |_metrics| {
            Ok::<_, std::convert::Infallible>(Arc::new(recorded) as Arc<dyn WebhookTransport>)
        },
        Arc::new(SystemClock),
        10,
        1024,
        0,
        None,
    )
    .expect("recording transport factory is infallible");
    (handle, transport)
}

/// The webhook event names recorded so far, in delivery order.
async fn recorded_names(transport: &RecordingTransport) -> Vec<String> {
    let mut out = Vec::new();
    for d in transport.recorded().await {
        let v: serde_json::Value =
            serde_json::from_str(&d.body).expect("webhook body must be JSON");
        for e in v["events"].as_array().into_iter().flatten() {
            out.push(e["name"].as_str().unwrap_or_default().to_string());
        }
    }
    out
}

/// Bounded wait until `want` webhook events have been delivered, then return
/// them SORTED. Ordering within a batch is not the property these tests pin —
/// which edges fire, and that no extra one does, is.
async fn await_sorted_webhooks(transport: &RecordingTransport, want: usize) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        let names = recorded_names(transport).await;
        if names.len() >= want {
            let mut names = names;
            names.sort();
            return names;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "only {:?} webhook(s) delivered within 10s; expected {want}",
        recorded_names(transport).await
    );
}

/// A malformed `PYLON_REDIS_URL` must surface as an `Err` from `start` — the
/// startup handshake exists so a bridge that cannot come up fails the process
/// with the real error instead of hanging or panicking on a runtime thread.
#[tokio::test]
async fn bridge_start_surfaces_an_unusable_redis_url_as_an_error() {
    let mut cfg = redis_test_config(&random_prefix());
    cfg.redis_url = "definitely-not-a-redis-url".to_string();
    let local = Arc::new(LocalAdapter::new(
        Arc::new(Registry::new()),
        Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
    ));

    let Err(err) = bridge::start(&cfg, local, webhook_apps(), Arc::new(Default::default())) else {
        panic!("an unusable redis url must not produce a running bridge");
    };
    assert!(
        !err.to_string().is_empty(),
        "the connect failure must carry the underlying error, not an empty message"
    );
}

/// `/metrics` reports `pylon_redis_connected` from the node heartbeat, not from
/// a hopeful default: the gauge starts false and is only raised once a tick has
/// actually reached Redis.
#[tokio::test]
async fn bridge_metrics_report_redis_connected_once_the_heartbeat_ticks() {
    assert!(
        redis_reachable(),
        "cluster_bridge requires Redis; set PYLON_TEST_REDIS_URL (default redis://127.0.0.1:6390) — refusing to silently pass"
    );

    let mut cfg = redis_test_config(&random_prefix());
    cfg.redis_node_heartbeat_secs = 1;
    let local = Arc::new(LocalAdapter::new(
        Arc::new(Registry::new()),
        Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
    ));
    let bridge = bridge::start(&cfg, local, webhook_apps(), Arc::new(Default::default()))
        .expect("ClusterBridge::start must connect to the test Redis");
    bridge.attach_webhooks(WebhookHandle::null());

    let metrics = bridge.metrics();
    assert_eq!(
        metrics
            .cmd_dropped
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "a fresh bridge has dropped nothing"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if metrics
            .redis_connected
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            drop(bridge);
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the heartbeat never raised redis_connected against a live Redis");
}

/// The CLUSTER cache replay is the bridge's job: the worker's node-local replay
/// is suppressed in cluster mode, so a subscriber here must be handed the event
/// published on ANY node — or told it is a miss, with the `cache_miss` webhook
/// the operator subscribed to.
#[tokio::test]
async fn cluster_subscribe_on_a_cache_channel_replays_or_signals_a_miss() {
    assert!(
        redis_reachable(),
        "cluster_bridge requires Redis; set PYLON_TEST_REDIS_URL (default redis://127.0.0.1:6390) — refusing to silently pass"
    );

    tokio::time::timeout(Duration::from_secs(15), async {
        let cfg = redis_test_config(&random_prefix());
        let local = Arc::new(LocalAdapter::new(
            Arc::new(Registry::new()),
            Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
        ));
        let (webhooks, recorder) = recording_webhooks();
        let bridge = bridge::start(&cfg, local, webhook_apps(), Arc::new(Default::default()))
            .expect("ClusterBridge::start must connect to the test Redis");
        bridge.attach_webhooks(webhooks);
        let handle = bridge.handle();

        // MISS: nothing in the cluster cache yet.
        let (mut miss_rx, miss_mailbox) = mailbox_pair();
        handle.subscribe(
            Arc::from("app"),
            Arc::from("cache-room"),
            SocketId::generate(),
            miss_mailbox,
            true,
        );
        let frame = tokio::time::timeout(Duration::from_secs(5), miss_rx.recv())
            .await
            .expect("the joining connection must be told about the cache")
            .expect("the mailbox must stay open");
        match *frame {
            ServerEvent::CacheMiss { ref channel } => assert_eq!(channel, "cache-room"),
            ref other => panic!("an empty cluster cache must send cache_miss, got {other:?}"),
        }

        // HIT: the cluster (Redis) cache is written through the node's single
        // adapter, exactly as the REST publish path does on any node.
        bridge
            .adapter()
            .cache_set(
                "app",
                "cache-room",
                pylon::channel::cache::CachedEvent {
                    event: "ev".to_string(),
                    data: "{\"v\":1}".to_string(),
                },
                Duration::from_secs(60),
            )
            .await;
        let (mut hit_rx, hit_mailbox) = mailbox_pair();
        handle.subscribe(
            Arc::from("app"),
            Arc::from("cache-room"),
            SocketId::generate(),
            hit_mailbox,
            false,
        );
        let frame = tokio::time::timeout(Duration::from_secs(5), hit_rx.recv())
            .await
            .expect("the joining connection must get the cluster replay")
            .expect("the mailbox must stay open");
        match *frame {
            ServerEvent::ChannelEvent {
                ref channel,
                ref event,
                ref data,
                ..
            } => {
                assert_eq!(channel, "cache-room");
                assert_eq!(event, "ev");
                assert_eq!(data, &serde_json::Value::String("{\"v\":1}".to_string()));
            }
            ref other => panic!("a populated cluster cache must replay the event, got {other:?}"),
        }

        // The miss — and only the miss — produced a webhook, alongside the single
        // cluster-wide channel_occupied for the 0→1 edge.
        assert_eq!(
            await_sorted_webhooks(&recorder, 2).await,
            ["cache_miss", "channel_occupied"],
            "the cache HIT must not fire a second cache_miss"
        );

        drop(bridge);
    })
    .await
    .expect("cluster cache replay test must not hang (Redis up?)");
}

/// A cluster edge whose app has vanished from the store mid-flight is DROPPED,
/// not emitted against an app whose webhook configuration is unknown. Proven by
/// ordering: the ghost app's four edges are fired first, and once the known
/// app's four have all been delivered the ghost's have demonstrably been drained
/// — the bridge runs one FIFO loop.
#[tokio::test]
async fn cluster_edges_for_an_app_that_vanished_fire_no_webhooks() {
    assert!(
        redis_reachable(),
        "cluster_bridge requires Redis; set PYLON_TEST_REDIS_URL (default redis://127.0.0.1:6390) — refusing to silently pass"
    );

    tokio::time::timeout(Duration::from_secs(20), async {
        let cfg = redis_test_config(&random_prefix());
        let local = Arc::new(LocalAdapter::new(
            Arc::new(Registry::new()),
            Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
        ));
        let (webhooks, recorder) = recording_webhooks();
        let bridge = bridge::start(
            &cfg,
            local.clone(),
            webhook_apps(),
            Arc::new(Default::default()),
        )
        .expect("ClusterBridge::start must connect to the test Redis");
        bridge.attach_webhooks(webhooks);
        let handle = bridge.handle();

        // ── The ghost app: every edge, none of which may reach a webhook. ──
        let ghost_sid = SocketId::generate();
        let (_ghost_rx, ghost_mailbox) = mailbox_pair();
        handle.subscribe(
            Arc::from("ghost"),
            Arc::from("public-g"),
            ghost_sid,
            ghost_mailbox,
            true,
        );
        handle.unsubscribe(Arc::from("ghost"), Arc::from("public-g"), ghost_sid, true);
        let (_, _, ghost_member) = presence_conn("gu");
        let (mut ghost_pres_rx, ghost_pres_mailbox) = mailbox_pair();
        handle.presence_subscribe(
            Arc::from("ghost"),
            Arc::from("presence-g"),
            ghost_member,
            ghost_sid,
            ghost_pres_mailbox,
            true,
        );
        handle.presence_leave(
            Arc::from("ghost"),
            Arc::from("presence-g"),
            "gu".to_string(),
            ghost_sid,
            true,
        );
        // The presence ack is sent BEFORE the app lookup, so it still arrives —
        // which also proves the ghost commands really were executed, not skipped.
        let acked = tokio::time::timeout(Duration::from_secs(5), ghost_pres_rx.recv())
            .await
            .expect("the ghost app's joiner must still be acked")
            .expect("the mailbox must stay open");
        assert!(
            matches!(*acked, ServerEvent::SubscriptionSucceeded { .. }),
            "the presence ack precedes the app lookup and must still be sent"
        );

        // ── The known app: the same four edges, all of which must fire. ──
        let sid = SocketId::generate();
        let (_rx, mailbox) = mailbox_pair();
        handle.subscribe(Arc::from("app"), Arc::from("public-k"), sid, mailbox, true);
        handle.unsubscribe(Arc::from("app"), Arc::from("public-k"), sid, true);
        let (pres_sid, _pres_handle, member) = presence_conn("ku");
        let (_pres_rx, pres_mailbox) = mailbox_pair();
        handle.presence_subscribe(
            Arc::from("app"),
            Arc::from("presence-k"),
            member,
            pres_sid,
            pres_mailbox,
            true,
        );
        handle.presence_leave(
            Arc::from("app"),
            Arc::from("presence-k"),
            "ku".to_string(),
            pres_sid,
            true,
        );

        assert_eq!(
            await_sorted_webhooks(&recorder, 6).await,
            [
                "channel_occupied",
                "channel_occupied",
                "channel_vacated",
                "channel_vacated",
                "member_added",
                "member_removed",
            ],
            "exactly the known app's six cluster edges — the ghost app, drained \
             first, contributed none"
        );

        drop(bridge);
    })
    .await
    .expect("vanished-app edge test must not hang (Redis up?)");
}

/// The cluster online/offline edge notifies THIS node's local watchers directly:
/// the `WatchOnline` publish self-dedups on the origin node, so without this the
/// signing-in user's own node would never tell its watchers.
#[tokio::test]
async fn cluster_signin_and_signout_notify_this_nodes_local_watchers() {
    assert!(
        redis_reachable(),
        "cluster_bridge requires Redis; set PYLON_TEST_REDIS_URL (default redis://127.0.0.1:6390) — refusing to silently pass"
    );

    tokio::time::timeout(Duration::from_secs(15), async {
        let cfg = redis_test_config(&random_prefix());
        let local = Arc::new(LocalAdapter::new(
            Arc::new(Registry::new()),
            Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
        ));
        let bridge = bridge::start(
            &cfg,
            local.clone(),
            webhook_apps(),
            Arc::new(Default::default()),
        )
        .expect("ClusterBridge::start must connect to the test Redis");
        bridge.attach_webhooks(WebhookHandle::null());

        // A watcher on THIS node, registered node-locally exactly as the worker's
        // `ClusterAdapter::watch` does before firing the bridge command.
        let (mut watcher_rx, watcher_mailbox) = mailbox_pair();
        let watcher = ConnectionHandle {
            socket_id: SocketId::generate(),
            mailbox: watcher_mailbox,
        };
        local.watch_edges("app", watcher, vec!["u1".to_string()]);

        let handle = bridge.handle();
        let signed_in = SocketId::generate();
        handle.signin(Arc::from("app"), "u1".to_string(), signed_in, true);
        let online = tokio::time::timeout(Duration::from_secs(5), watcher_rx.recv())
            .await
            .expect("the local watcher must be told the user came online")
            .expect("the mailbox must stay open");
        assert_eq!(
            *online,
            ServerEvent::WatchlistEvents {
                events: vec![pylon::protocol::event::WatchlistChange {
                    name: "online".to_string(),
                    user_ids: vec!["u1".to_string()],
                }],
            }
        );

        handle.signout(Arc::from("app"), "u1".to_string(), signed_in, true);
        let offline = tokio::time::timeout(Duration::from_secs(5), watcher_rx.recv())
            .await
            .expect("the local watcher must be told the user went offline")
            .expect("the mailbox must stay open");
        assert_eq!(
            *offline,
            ServerEvent::WatchlistEvents {
                events: vec![pylon::protocol::event::WatchlistChange {
                    name: "offline".to_string(),
                    user_ids: vec!["u1".to_string()],
                }],
            }
        );

        drop(bridge);
    })
    .await
    .expect("local watcher notify test must not hang (Redis up?)");
}

/// A capacity-rejected presence join must not leave the node subscribed to a
/// channel it holds no members of. The positive control in the same test — a
/// node-first subscribe on a second channel reaching `NUMSUB 1` — is what makes
/// the negative meaningful: it proves this bridge does take the pub/sub edge, so
/// the rejected channel's zero is a hand-back rather than an edge never taken.
#[tokio::test]
async fn a_rejected_presence_join_hands_back_the_pubsub_edge_when_the_node_empties() {
    assert!(
        redis_reachable(),
        "cluster_bridge requires Redis; set PYLON_TEST_REDIS_URL (default redis://127.0.0.1:6390) — refusing to silently pass"
    );

    tokio::time::timeout(Duration::from_secs(20), async {
        let prefix = random_prefix();
        let mut cfg = redis_test_config(&prefix);
        cfg.max_presence_members = 1;
        cfg.redis_presence_heartbeat_secs = 3600;
        let keys = Keys::new(&prefix);
        let rejected_key = keys.msg("app", "presence-full");
        let control_key = keys.msg("app", "public-control");

        // Another node fills the cluster roster to the cap, so a DIFFERENT user
        // joining here is rejected.
        let node_a = RedisAdapter::new(&cfg)
            .await
            .expect("node A's RedisAdapter must connect to the test Redis");
        let (a_sid, _a_handle, u1) = presence_conn("u1");
        node_a
            .cluster_presence_join("app", "presence-full", &u1, &a_sid, Some(1))
            .await
            .expect("seeding the roster on node A must reach Redis")
            .expect("u1 is the first member and must be admitted");

        let local = Arc::new(LocalAdapter::new(
            Arc::new(Registry::new()),
            Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
        ));
        let bridge = bridge::start(
            &cfg,
            local.clone(),
            webhook_apps(),
            Arc::new(Default::default()),
        )
        .expect("ClusterBridge::start must connect to the test Redis");
        bridge.attach_webhooks(WebhookHandle::null());
        let handle = bridge.handle();

        // Positive control: a node-first subscribe DOES make this node a Redis
        // subscriber of the channel's msg key.
        let (_control_rx, control_mailbox) = mailbox_pair();
        handle.subscribe(
            Arc::from("app"),
            Arc::from("public-control"),
            SocketId::generate(),
            control_mailbox,
            true,
        );
        require_numsub(&control_key, 1, Duration::from_secs(5)).await;

        // The rejected joiner is the node's ONLY member of its channel, so the
        // reject arm's local unsubscribe empties the node and the edge is
        // handed back.
        let (rejected_sid, rejected_handle, u2) = presence_conn("u2");
        let (mut rejected_rx, rejected_mailbox) = mailbox_pair();
        let first = local
            .subscribe("app", "presence-full", rejected_handle, Some(u2.clone()))
            .await;
        assert_eq!(
            first.subscription_count, 1,
            "the rejected joiner must be this node's only member of the channel"
        );
        handle.presence_subscribe(
            Arc::from("app"),
            Arc::from("presence-full"),
            u2,
            rejected_sid,
            rejected_mailbox,
            true,
        );
        let rejection = tokio::time::timeout(Duration::from_secs(5), rejected_rx.recv())
            .await
            .expect("the cap rejection must arrive")
            .expect("the rejected joiner's mailbox must stay open");
        match *rejection {
            ServerEvent::SubscriptionError { status, .. } => {
                assert_eq!(status, 4004, "the cap rejection is a 4004")
            }
            ref other => panic!("expected a 4004 subscription_error, got {other:?}"),
        }

        assert_eq!(
            numsub(&rejected_key).await,
            0,
            "a node with no members of the channel must not stay subscribed to it"
        );

        drop(bridge);
    })
    .await
    .expect("rejected-join edge hand-back test must not hang (Redis up?)");
}
