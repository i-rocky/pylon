//! Integration tests for the Redis scaling adapter (SP7a).
//!
//! These talk to a REAL Redis. Point `PYLON_TEST_REDIS_URL` at a throwaway
//! instance (default `redis://127.0.0.1:6379`). Each run uses a random key/channel
//! prefix (`pylontest:<uuid>`) so a shared Redis is never clobbered — we NEVER
//! issue FLUSHALL/FLUSHDB or any unscoped destructive command.
//!
//! They FAIL LOUD if Redis is unreachable (the connect error propagates) — there
//! is no silent skip.

use fred::prelude::*;
use pylon::adapter::redis::keys::Keys;
use pylon::adapter::redis::{client::RedisClients, RedisAdapter};
use pylon::adapter::Adapter;
use pylon::connection::handle::ConnectionHandle;
use pylon::protocol::event::ServerEvent;
use pylon::protocol::socket_id::SocketId;
use pylon::server::config::ServerConfig;
use std::time::Duration;
use uuid::Uuid;

/// Fixed app id used by the cluster lifecycle tests. Channel/app ids are plain
/// string args to the adapter; they don't come from `ServerConfig`.
const TEST_APP: &str = "app1";

/// Test Redis URL: `PYLON_TEST_REDIS_URL` or the documented default.
fn test_redis_url() -> String {
    std::env::var("PYLON_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

/// A random, run-unique key/channel prefix for isolation on a shared Redis.
fn random_prefix() -> String {
    format!("pylontest:{}", Uuid::new_v4())
}

/// Build a `ServerConfig` configured for the Redis adapter against the test Redis
/// with a random prefix.
fn redis_test_config(prefix: &str) -> ServerConfig {
    ServerConfig {
        adapter: "redis".into(),
        redis_url: test_redis_url(),
        redis_prefix: prefix.into(),
        ..ServerConfig::default()
    }
}

/// Build a `ServerConfig` for the Redis adapter with explicit, short membership TTL
/// and heartbeat cadence. Lets the heartbeat test prove a live node keeps its members
/// alive past a TTL that would otherwise have elapsed.
fn redis_test_config_with_ttl(prefix: &str, ttl_secs: u64, heartbeat_secs: u64) -> ServerConfig {
    ServerConfig {
        adapter: "redis".into(),
        redis_url: test_redis_url(),
        redis_prefix: prefix.into(),
        redis_membership_ttl_secs: ttl_secs,
        redis_presence_heartbeat_secs: heartbeat_secs,
        ..ServerConfig::default()
    }
}

/// Build a connected `RedisAdapter` against the test Redis. Fails loud if Redis
/// is down.
async fn connect_adapter() -> RedisAdapter {
    let cfg = redis_test_config(&random_prefix());
    RedisAdapter::new(&cfg)
        .await
        .expect("RedisAdapter::new must connect to the test Redis")
}

/// Build a connected `RedisAdapter` sharing an explicit `prefix` — used to form a
/// multi-node cluster (several adapters) over one Redis, all seeing the same keys.
async fn connect_adapter_with_prefix(prefix: &str) -> RedisAdapter {
    let cfg = redis_test_config(prefix);
    RedisAdapter::new(&cfg)
        .await
        .expect("RedisAdapter::new must connect to the test Redis")
}

#[tokio::test]
async fn smoke_connectivity() {
    // 1. The adapter connects (proves new() + fred wiring works end-to-end).
    let _adapter = connect_adapter().await;

    // 2. Build a dedicated pair of fred clients for a raw PUBLISH -> SUBSCRIBE
    //    round-trip. (We use a fresh pair rather than the adapter's private
    //    clients so the test exercises the same `connect()` path the adapter uses.)
    let clients = RedisClients::connect(&test_redis_url(), 2)
        .await
        .expect("fred clients must connect to the test Redis");

    // PING via the command pool.
    let pong: String = clients
        .pool
        .ping(None)
        .await
        .expect("PING must succeed on the command pool");
    assert_eq!(pong, "PONG");

    // 3. PUBLISH (pool) -> SUBSCRIBE (subscriber) round-trip on a random channel.
    let channel = format!("pylontest:{}:smoke", Uuid::new_v4());
    let payload = format!("hello-{}", Uuid::new_v4());

    // Take the message stream BEFORE subscribing so we cannot miss the message.
    let mut rx = clients.sub.message_rx();
    clients
        .sub
        .subscribe(channel.clone())
        .await
        .expect("SUBSCRIBE must succeed");

    // Publish from the pool side. `Pool` itself is not a `PubsubInterface`;
    // pub/sub commands go through a pooled `Client` (`pool.next()`).
    let _: i64 = clients
        .pool
        .next()
        .publish(channel.clone(), payload.clone())
        .await
        .expect("PUBLISH must succeed");

    // Receive, with a hard timeout so a broken stream fails instead of hanging.
    let msg = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("must receive the published message within 2s")
        .expect("broadcast receiver must yield a message");

    assert_eq!(msg.channel.to_string(), channel);
    assert_eq!(
        msg.value.into_string(),
        Some(payload),
        "received payload must match what was published"
    );

    // Clean shutdown of the test clients (the adapter drops on scope exit).
    let _ = clients.sub.quit().await;
    let _ = clients.pool.quit().await;
}

/// B1: the per-(app,channel) Redis-subscription lifecycle. A node's SubscriberClient
/// must track the `keys.msg(app, channel)` pub/sub channel exactly while it has at
/// least one node-local subscriber on that channel — subscribe on the 0→1 edge,
/// unsubscribe on the 1→0 edge.
#[tokio::test]
async fn redis_sub_lifecycle_tracks_channels() {
    // Two adapters (A and B) form a 2-node cluster on one Redis via a shared prefix.
    let prefix = random_prefix();
    let _node_a = connect_adapter_with_prefix(&prefix).await;
    let node_b = connect_adapter_with_prefix(&prefix).await;

    let keys = Keys::new(&prefix);
    let msg_key = keys.msg(TEST_APP, "public-room");

    // A fake connection handle — `ConnectionHandle`'s fields are `pub`, so it is
    // constructible directly from an integration test.
    let socket_id = SocketId::generate();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = ConnectionHandle {
        socket_id: socket_id.clone(),
        mailbox: tx,
    };

    // Before any subscribe, B must NOT be tracking the msg channel.
    assert!(
        !tracked_contains(&node_b, &msg_key),
        "B must not track {msg_key} before any local subscriber"
    );

    // Subscribe the fake socket on B → node-local 0→1 edge → B SUBSCRIBEs to Redis.
    let out = tokio::time::timeout(
        Duration::from_secs(2),
        node_b.subscribe(TEST_APP, "public-room", handle, None),
    )
    .await
    .expect("subscribe must not hang (Redis up?)");
    assert_eq!(
        out.subscription_count, 1,
        "first local subscriber → count 1"
    );

    assert!(
        tracked_contains(&node_b, &msg_key),
        "B must track {msg_key} after the node-local 0→1 edge"
    );

    // Unsubscribe that socket on B → node-local 1→0 edge → B UNSUBSCRIBEs from Redis.
    let out = tokio::time::timeout(
        Duration::from_secs(2),
        node_b.unsubscribe(TEST_APP, "public-room", &socket_id),
    )
    .await
    .expect("unsubscribe must not hang (Redis up?)");
    assert_eq!(
        out.subscription_count, 0,
        "last local subscriber gone → count 0"
    );

    assert!(
        !tracked_contains(&node_b, &msg_key),
        "B must no longer track {msg_key} after the node-local 1→0 edge"
    );
}

/// Whether `adapter`'s SubscriberClient currently tracks `key` as a subscription.
fn tracked_contains(adapter: &RedisAdapter, key: &str) -> bool {
    adapter.tracked_redis_channels().iter().any(|c| c == key)
}

/// Subscribe a fresh fake socket to `(TEST_APP, channel)` on `adapter`, returning
/// its `SocketId` and the receiving half of its mailbox. The connection task would
/// normally drain the mailbox; here the test owns the rx so it can assert delivery.
async fn subscribe_socket(
    adapter: &RedisAdapter,
    channel: &str,
) -> (SocketId, tokio::sync::mpsc::UnboundedReceiver<ServerEvent>) {
    let socket_id = SocketId::generate();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = ConnectionHandle {
        socket_id: socket_id.clone(),
        mailbox: tx,
    };
    adapter.subscribe(TEST_APP, channel, handle, None).await;
    (socket_id, rx)
}

/// Poll `adapter.tracked_redis_channels()` until it contains `key` or the deadline
/// elapses. Returns whether the channel showed up — lets the test wait for a Redis
/// SUBSCRIBE to take effect without a blind fixed sleep.
async fn await_tracked(adapter: &RedisAdapter, key: &str, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tracked_contains(adapter, key) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// B2: a `broadcast` on node A must (1) deliver locally honouring `except`, (2) fan
/// out across Redis to subscribers on node B as a pre-encoded v7 frame, and (3) NOT
/// loop back to A's own local sockets a second time (self-dedup via `node_id`).
#[tokio::test]
async fn cross_node_broadcast_fans_out_with_dedup_and_exclusion() {
    let prefix = random_prefix();
    let adapter_a = connect_adapter_with_prefix(&prefix).await;
    let adapter_b = connect_adapter_with_prefix(&prefix).await;

    let keys = Keys::new(&prefix);
    let msg_key = keys.msg(TEST_APP, "public-room");

    // On A: the sender (excepted) and another local subscriber.
    let (sender_a_id, mut sender_a_rx) = subscribe_socket(&adapter_a, "public-room").await;
    let (_other_a_id, mut other_a_rx) = subscribe_socket(&adapter_a, "public-room").await;

    // On B: one remote subscriber that should receive the event via Redis.
    let (_recv_b_id, mut recv_b_rx) = subscribe_socket(&adapter_b, "public-room").await;

    // Wait for B's Redis SUBSCRIBE to take effect so the published message isn't lost.
    assert!(
        await_tracked(&adapter_b, &msg_key, Duration::from_secs(2)).await,
        "B must track {msg_key} before A publishes"
    );

    // A broadcasts, excepting the sender socket on A.
    adapter_a
        .broadcast(
            TEST_APP,
            "public-room",
            ServerEvent::ChannelEvent {
                channel: "public-room".into(),
                event: "my-event".into(),
                data: serde_json::json!({ "hello": "world" }),
                user_id: None,
            },
            Some(sender_a_id.clone()),
        )
        .await;

    // other_a receives EXACTLY ONE typed event via local delivery.
    let got = tokio::time::timeout(Duration::from_secs(2), other_a_rx.recv())
        .await
        .expect("other_a must receive the local broadcast within 2s")
        .expect("other_a mailbox must yield an event");
    match got {
        ServerEvent::ChannelEvent {
            channel,
            event,
            data,
            ..
        } => {
            assert_eq!(channel, "public-room");
            assert_eq!(event, "my-event");
            assert_eq!(data, serde_json::json!({ "hello": "world" }));
        }
        other => panic!("other_a expected ChannelEvent, got {other:?}"),
    }

    // recv_b receives the event via Redis as a pre-encoded Raw frame.
    let got_b = tokio::time::timeout(Duration::from_secs(2), recv_b_rx.recv())
        .await
        .expect("recv_b must receive the cross-node broadcast within 2s")
        .expect("recv_b mailbox must yield an event");
    let frame = match got_b {
        ServerEvent::Raw(s) => s,
        other => panic!("recv_b expected Raw frame from Redis, got {other:?}"),
    };
    assert!(
        frame.contains("my-event") && frame.contains("hello"),
        "Raw frame must carry the event payload: {frame}"
    );
    let parsed: serde_json::Value =
        serde_json::from_str(&frame).expect("Raw frame must be valid JSON");
    assert_eq!(parsed["event"], "my-event");
    assert_eq!(parsed["channel"], "public-room");
    assert_eq!(parsed["data"]["hello"], "world");

    // Drain window: confirm self-dedup (other_a gets NO second copy from A's own
    // Redis echo) and exclusion (sender_a gets NOTHING at all).
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        other_a_rx.try_recv().is_err(),
        "other_a must NOT receive a duplicate from A's own Redis echo (self-dedup)"
    );
    assert!(
        sender_a_rx.try_recv().is_err(),
        "sender_a was excepted and must receive nothing"
    );
}

/// Build a fresh fake `ConnectionHandle` (its fields are `pub`) and return it with
/// its `SocketId`. The mailbox rx is dropped — these tests only assert membership
/// counts, not delivery.
fn fake_handle() -> (SocketId, ConnectionHandle) {
    let socket_id = SocketId::generate();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = ConnectionHandle {
        socket_id: socket_id.clone(),
        mailbox: tx,
    };
    (socket_id, handle)
}

/// Short timeout wrapper so a wedged Redis fails loud instead of hanging the suite.
async fn with_timeout<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(Duration::from_secs(2), fut)
        .await
        .expect("redis op must not hang (Redis up?)")
}

/// C1: membership (`HLEN` of the occ hash) is the AUTHORITATIVE, cluster-wide
/// `subscription_count`, and its 0→1 / 1→0 transitions are the exactly-once cluster
/// occupied/vacated edges — across nodes, for ALL channel kinds. `channel` and
/// `channels` report that same cluster count, not the node-local one.
#[tokio::test]
async fn cluster_membership_count_and_occupancy() {
    let prefix = random_prefix();
    let adapter_a = connect_adapter_with_prefix(&prefix).await;
    let adapter_b = connect_adapter_with_prefix(&prefix).await;

    // 1. First subscriber (on A): cluster count 1, this is the cluster occupied edge.
    let (sock_a, handle_a) = fake_handle();
    let out_a = with_timeout(adapter_a.subscribe(TEST_APP, "public-room", handle_a, None)).await;
    assert_eq!(
        out_a.subscription_count, 1,
        "first cluster subscriber → cluster count 1"
    );
    assert!(out_a.occupied, "0→1 cluster edge must report occupied");

    // 2. Second subscriber on a DIFFERENT node (B): cluster count 2, NOT occupied
    //    (this is the assertion that fails while `subscribe` delegates to local — A
    //    and B would each see their own node-local count of 1).
    let (sock_b, handle_b) = fake_handle();
    let out_b = with_timeout(adapter_b.subscribe(TEST_APP, "public-room", handle_b, None)).await;
    assert_eq!(
        out_b.subscription_count, 2,
        "second cluster subscriber on another node → cluster count 2"
    );
    assert!(
        !out_b.occupied,
        "a non-0→1 subscribe must NOT report occupied"
    );

    // 3. `channel` on A reports the cluster count (2), occupied true.
    let summary = with_timeout(adapter_a.channel(TEST_APP, "public-room")).await;
    assert_eq!(
        summary.subscription_count, 2,
        "channel() must report the cluster-wide count"
    );
    assert!(
        summary.occupied,
        "channel() must report occupied while members exist"
    );

    // 4. `channels` on A lists public-room with the cluster count.
    let all = with_timeout(adapter_a.channels(TEST_APP, None)).await;
    let pr = all
        .iter()
        .find(|c| c.name == "public-room")
        .expect("channels() must list public-room while it is occupied");
    assert_eq!(
        pr.subscription_count, 2,
        "channels() must report the cluster-wide count"
    );

    // 5. Unsubscribe B's socket → cluster count 1, NOT vacated; then A's → 0, vacated.
    let un_b = with_timeout(adapter_b.unsubscribe(TEST_APP, "public-room", &sock_b)).await;
    assert_eq!(
        un_b.subscription_count, 1,
        "one cluster member remains → count 1"
    );
    assert!(
        !un_b.vacated,
        "a non-1→0 unsubscribe must NOT report vacated"
    );

    let un_a = with_timeout(adapter_a.unsubscribe(TEST_APP, "public-room", &sock_a)).await;
    assert_eq!(
        un_a.subscription_count, 0,
        "last cluster member gone → count 0"
    );
    assert!(un_a.vacated, "1→0 cluster edge must report vacated");

    // 6. After both leave: channel reports 0/!occupied and channels no longer lists it.
    let summary = with_timeout(adapter_a.channel(TEST_APP, "public-room")).await;
    assert_eq!(
        summary.subscription_count, 0,
        "empty channel → cluster count 0"
    );
    assert!(!summary.occupied, "empty channel must not be occupied");

    let all = with_timeout(adapter_a.channels(TEST_APP, None)).await;
    assert!(
        !all.iter().any(|c| c.name == "public-room"),
        "channels() must not list a vacated channel"
    );
}

/// C2: the membership TTL heartbeat. A node spawns a task that, every
/// `redis_presence_heartbeat_secs`, re-stamps every LOCAL member's `expireAt` and
/// bumps the occ-hash whole-key TTL. With a 2s TTL and a 1s heartbeat, a member
/// subscribed once must STILL be present after 2.5s — proving the refresh ran.
/// Without the heartbeat the `EXPIRE 2` would have elapsed and the count would be 0.
#[tokio::test]
async fn membership_heartbeat_keeps_member_alive_past_ttl() {
    tokio::time::timeout(Duration::from_secs(6), async {
        let prefix = random_prefix();
        // Short TTL (2s) + faster heartbeat (1s): re-stamps at ~1s and ~2s.
        let cfg = redis_test_config_with_ttl(&prefix, 2, 1);
        let adapter = RedisAdapter::new(&cfg)
            .await
            .expect("RedisAdapter::new must connect to the test Redis");

        // One local subscriber on a public channel.
        let (_sock, handle) = fake_handle();
        let out = adapter
            .subscribe(TEST_APP, "public-room", handle, None)
            .await;
        assert_eq!(
            out.subscription_count, 1,
            "first subscriber → cluster count 1"
        );

        // Sleep past the 2s TTL. The 1s heartbeat must have re-stamped the member
        // (and bumped the key TTL) at ~1s and ~2s, so it is still alive.
        tokio::time::sleep(Duration::from_millis(2500)).await;

        let summary = adapter.channel(TEST_APP, "public-room").await;
        assert_eq!(
            summary.subscription_count, 1,
            "heartbeat must keep the member alive past the {}s TTL (got {})",
            cfg.redis_membership_ttl_secs, summary.subscription_count
        );
    })
    .await
    .expect("heartbeat test must not hang (Redis up?)");
}
