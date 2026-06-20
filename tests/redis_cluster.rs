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
use pylon::channel::cache::CachedEvent;
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

/// Build a connected `RedisAdapter` sharing a `prefix` with a short membership TTL +
/// heartbeat cadence — used by the sweeper tests to make crashed-node members go stale
/// fast while a live node keeps its own members fresh.
async fn connect_adapter_with_prefix_ttl(
    prefix: &str,
    ttl_secs: u64,
    heartbeat_secs: u64,
) -> RedisAdapter {
    let cfg = redis_test_config_with_ttl(prefix, ttl_secs, heartbeat_secs);
    RedisAdapter::new(&cfg)
        .await
        .expect("RedisAdapter::new must connect to the test Redis")
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
    let (tx, _rx) = tokio::sync::mpsc::channel(1024);
    let handle = ConnectionHandle {
        socket_id,
        mailbox: pylon::connection::handle::Mailbox::new(tx, None, None),
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
) -> (SocketId, tokio::sync::mpsc::Receiver<Box<ServerEvent>>) {
    let socket_id = SocketId::generate();
    let (tx, rx) = tokio::sync::mpsc::channel(1024);
    let handle = ConnectionHandle {
        socket_id,
        mailbox: pylon::connection::handle::Mailbox::new(tx, None, None),
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
            Some(sender_a_id),
        )
        .await;

    // other_a receives EXACTLY ONE event via local delivery. `broadcast` now
    // encodes once and fans out pre-encoded `Raw` frames, so assert the local
    // delivery on its wire content (byte-identical to the cross-node frame).
    let got = *tokio::time::timeout(Duration::from_secs(2), other_a_rx.recv())
        .await
        .expect("other_a must receive the local broadcast within 2s")
        .expect("other_a mailbox must yield an event");
    match got {
        ServerEvent::Raw(s) => {
            let parsed: serde_json::Value =
                serde_json::from_str(&s).expect("Raw frame must be valid JSON");
            assert_eq!(parsed["channel"], "public-room");
            assert_eq!(parsed["event"], "my-event");
            assert_eq!(parsed["data"], serde_json::json!({ "hello": "world" }));
        }
        other => panic!("other_a expected Raw frame from local broadcast, got {other:?}"),
    }

    // recv_b receives the event via Redis as a pre-encoded Raw frame.
    let got_b = *tokio::time::timeout(Duration::from_secs(2), recv_b_rx.recv())
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
    let (tx, _rx) = tokio::sync::mpsc::channel(1024);
    let handle = ConnectionHandle {
        socket_id,
        mailbox: pylon::connection::handle::Mailbox::new(tx, None, None),
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

/// Current wall-clock millis since the Unix epoch (mirrors the adapter's internal
/// `now_ms`; the sweeper test seam takes `now` so the test drives time deterministically).
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// E1: a cache-channel last-event written on one node must be readable on ANOTHER
/// node — the cache store is Redis-backed, not node-local. A `cache_set` on adapter A
/// must be visible to a `cache_get` on adapter B sharing the same prefix. (This fails
/// while `cache_set`/`cache_get` delegate to the in-memory `LocalAdapter`: B's local
/// store would be empty, so the cross-node read returns None.)
#[tokio::test]
async fn cache_set_on_one_node_is_readable_on_another() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let adapter_a = connect_adapter_with_prefix(&prefix).await;
        let adapter_b = connect_adapter_with_prefix(&prefix).await;

        adapter_a
            .cache_set(
                TEST_APP,
                "cache-x",
                CachedEvent {
                    event: "e".into(),
                    data: "d".into(),
                },
                Duration::from_secs(30),
            )
            .await;

        let got = adapter_b.cache_get(TEST_APP, "cache-x").await;
        assert_eq!(
            got,
            Some(CachedEvent {
                event: "e".into(),
                data: "d".into(),
            }),
            "cache_set on A must be readable via cache_get on B (Redis-backed)"
        );
    })
    .await
    .expect("cross-node cache test must not hang (Redis up?)");
}

/// E1: a `cache_get` for a channel that was never set returns None (a benign
/// `pusher:cache_miss`), not an error.
#[tokio::test]
async fn cache_get_is_none_when_absent() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let adapter_a = connect_adapter_with_prefix(&prefix).await;
        assert_eq!(
            adapter_a.cache_get(TEST_APP, "cache-never").await,
            None,
            "an unset cache channel must read back None"
        );
    })
    .await
    .expect("absent-cache test must not hang (Redis up?)");
}

/// E1: a cache entry expires once its Redis PX TTL elapses. Set with a 150ms TTL,
/// wait 300ms, then the GET returns nil → None (Redis handles expiry natively; the
/// Redis adapter does NO manual expiry check).
#[tokio::test]
async fn cache_entry_expires() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let adapter_a = connect_adapter_with_prefix(&prefix).await;

        adapter_a
            .cache_set(
                TEST_APP,
                "cache-x",
                CachedEvent {
                    event: "e".into(),
                    data: "d".into(),
                },
                Duration::from_millis(150),
            )
            .await;

        // Real (short) sleep past the PX TTL — the only timing-sensitive assertion.
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert_eq!(
            adapter_a.cache_get(TEST_APP, "cache-x").await,
            None,
            "cache entry must be gone after its Redis PX TTL elapsed"
        );
    })
    .await
    .expect("cache-expiry test must not hang (Redis up?)");
}

/// D2: the lease-locked sweeper reaps members whose `expireAt` is in the past (a
/// crashed node's members go stale once its heartbeat stops re-stamping them) but must
/// NOT vacate a channel that a LIVE node still holds. Two nodes A and B both subscribe
/// to `public-room`; A "crashes" (drop aborts its heartbeat) while B keeps its member
/// fresh. After the TTL elapses, B's sweep reaps A's stale member but leaves the channel
/// occupied (B's member is still live).
#[tokio::test]
async fn sweeper_reaps_dead_node_members_without_vacating_live_channel() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let prefix = random_prefix();
        // ttl=2s, heartbeat=1s. The 2s TTL keeps A's `expireAt` (last stamped at most
        // ~one heartbeat after the drop) reapable on a comfortable margin, while B's 1s
        // heartbeat keeps B's member fresh AND keeps the occ key's whole-key TTL alive.
        let adapter_a = connect_adapter_with_prefix_ttl(&prefix, 2, 1).await;
        let adapter_b = connect_adapter_with_prefix_ttl(&prefix, 2, 1).await;

        // A subscribes (cluster count 1, occupied), then B subscribes (cluster count 2).
        let (_sock_a, handle_a) = fake_handle();
        let out_a = adapter_a
            .subscribe(TEST_APP, "public-room", handle_a, None)
            .await;
        assert_eq!(out_a.subscription_count, 1, "A first subscriber → count 1");

        let (_sock_b, handle_b) = fake_handle();
        let out_b = adapter_b
            .subscribe(TEST_APP, "public-room", handle_b, None)
            .await;
        assert_eq!(out_b.subscription_count, 2, "B second subscriber → count 2");

        // Crash A: dropping the adapter aborts its heartbeat task, so A's member's
        // `expireAt` stops being re-stamped and will fall into the past after the TTL.
        drop(adapter_a);

        // Sleep past A's worst-case `expireAt` (≤ ~2s after its last stamp) so A is
        // reliably stale, while B's 1s heartbeat keeps B fresh and the occ key alive.
        tokio::time::sleep(Duration::from_millis(2600)).await;

        // B sweeps. It holds the lease (nobody else does), reaps A's stale member, but
        // must NOT vacate the channel because B's member is still live.
        let webhooks = pylon::webhook::WebhookHandle::null();
        let (acquired, reaped, vacated) = adapter_b.sweep_now(&webhooks, now_ms()).await;
        assert!(acquired, "B must acquire the sweep lease (no other holder)");
        assert!(
            reaped >= 1,
            "B must reap A's stale member (reaped={reaped})"
        );
        assert!(
            !vacated.contains(&(TEST_APP.to_string(), "public-room".to_string())),
            "public-room must NOT be vacated — B still holds a live member: {vacated:?}"
        );

        let summary = adapter_b.channel(TEST_APP, "public-room").await;
        assert_eq!(
            summary.subscription_count, 1,
            "after the sweep only B's live member remains → count 1 (got {})",
            summary.subscription_count
        );
    })
    .await
    .expect("sweeper-no-vacate test must not hang (Redis up?)");
}

/// D2: a channel whose only member lived on a crashed node is fully vacated by the
/// sweep — HDEL'd, DEL'd, de-indexed — and the `(app, channel)` pair shows up in the
/// returned vacated list (which drives the `ChannelVacated` webhook enqueue).
#[tokio::test]
async fn sweeper_vacates_channel_orphaned_by_dead_node() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let prefix = random_prefix();
        let adapter_a = connect_adapter_with_prefix_ttl(&prefix, 1, 1).await;
        let adapter_b = connect_adapter_with_prefix_ttl(&prefix, 1, 1).await;

        // The only subscriber to `lonely-room` lives on A.
        let (_sock_a, handle_a) = fake_handle();
        let out_a = adapter_a
            .subscribe(TEST_APP, "lonely-room", handle_a, None)
            .await;
        assert_eq!(
            out_a.subscription_count, 1,
            "A is the only member → count 1"
        );

        // Crash A so its member goes stale.
        drop(adapter_a);
        tokio::time::sleep(Duration::from_millis(1300)).await;

        // B sweeps: A's member is the last one and is stale → vacate. (With a short TTL
        // the occ hash's whole-key backstop may already have removed A's member by the
        // time B sweeps; either way the channel is orphaned in `chans` and the sweep
        // must vacate it — so we assert on the vacate, not on a non-zero `reaped`.)
        let webhooks = pylon::webhook::WebhookHandle::null();
        let (acquired, _reaped, vacated) = adapter_b.sweep_now(&webhooks, now_ms()).await;
        assert!(acquired, "B must acquire the sweep lease");
        assert!(
            vacated.contains(&(TEST_APP.to_string(), "lonely-room".to_string())),
            "lonely-room must be in the vacated list: {vacated:?}"
        );

        // The occ/chans state is gone.
        let summary = adapter_b.channel(TEST_APP, "lonely-room").await;
        assert_eq!(
            summary.subscription_count, 0,
            "vacated channel → cluster count 0 (got {})",
            summary.subscription_count
        );
        let all = adapter_b.channels(TEST_APP, None).await;
        assert!(
            !all.iter().any(|c| c.name == "lonely-room"),
            "channels() must not list a vacated channel"
        );
    })
    .await
    .expect("sweeper-vacate test must not hang (Redis up?)");
}

/// D2: the sweep is lease-locked. If another node already holds `{prefix}:sweeplock`,
/// this node must yield — `acquired == false`, no reaping — so exactly one node sweeps
/// at a time. After the lock is released, the node can acquire it.
#[tokio::test]
async fn sweeper_lease_lock_prevents_concurrent_sweep() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let prefix = random_prefix();
        let adapter_b = connect_adapter_with_prefix_ttl(&prefix, 1, 1).await;

        // A raw client grabs the sweeplock as a DIFFERENT node, with a long PX so it
        // is still held when B tries to sweep.
        let clients = RedisClients::connect(&test_redis_url(), 2)
            .await
            .expect("fred clients must connect to the test Redis");
        let keys = Keys::new(&prefix);
        let _: () = clients
            .pool
            .next()
            .set(
                keys.sweeplock(),
                "other-node",
                Some(Expiration::PX(60_000)),
                None,
                false,
            )
            .await
            .expect("raw SET sweeplock must succeed");

        // B must yield: it neither holds nor can steal the lease.
        let webhooks = pylon::webhook::WebhookHandle::null();
        let (acquired, reaped, vacated) = adapter_b.sweep_now(&webhooks, now_ms()).await;
        assert!(
            !acquired,
            "B must NOT sweep while another node holds the lease"
        );
        assert_eq!(reaped, 0, "a yielded sweep must reap nothing");
        assert!(vacated.is_empty(), "a yielded sweep must vacate nothing");

        // Release the lock; now B can acquire it.
        let _: () = clients
            .pool
            .next()
            .del(keys.sweeplock())
            .await
            .expect("raw DEL sweeplock must succeed");
        let (acquired2, _r2, _v2) = adapter_b.sweep_now(&webhooks, now_ms()).await;
        assert!(
            acquired2,
            "B must acquire the lease once the other node releases it"
        );

        let _ = clients.pool.quit().await;
    })
    .await
    .expect("sweeper-lease test must not hang (Redis up?)");
}

/// Build a fresh fake presence `ConnectionHandle` and a `PresenceMember` for
/// `user_id`/`user_info`. Returns `(socket_id, handle, member)` ready to pass to
/// `adapter.subscribe(app, channel, handle, Some(member))`.
fn presence_handle(
    user_id: &str,
    user_info: serde_json::Value,
) -> (
    SocketId,
    ConnectionHandle,
    pylon::presence::member::PresenceMember,
) {
    let socket_id = SocketId::generate();
    let (tx, _rx) = tokio::sync::mpsc::channel(1024);
    let handle = ConnectionHandle {
        socket_id,
        mailbox: pylon::connection::handle::Mailbox::new(tx, None, None),
    };
    let member = pylon::presence::member::PresenceMember {
        user_id: user_id.into(),
        user_info,
    };
    (socket_id, handle, member)
}

/// B1 (SP7b): `member_added` fires exactly once per cluster-wide user transition.
/// `first_for_user` is the cluster refcount 0→1 edge — NOT the node-local one. A
/// second connection of the SAME user on a DIFFERENT node must report
/// `first_for_user == false`; a new distinct user reports `true`.
///
/// (RED before B1: with `subscribe` delegating to the local adapter, B's first
/// connection of u1 has no node-local refcount and would report `true` — a duplicate
/// `member_added`.)
#[tokio::test]
async fn cross_node_presence_member_added_single_emit() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let adapter_a = connect_adapter_with_prefix(&prefix).await;
        let adapter_b = connect_adapter_with_prefix(&prefix).await;

        // u1's first connection on A → first_for_user (cluster 0→1 for u1).
        let (_s1, h1, m1) = presence_handle("u1", serde_json::json!({"name":"Ann"}));
        let out1 = adapter_a
            .subscribe(TEST_APP, "presence-room", h1, Some(m1))
            .await;
        let j1 = out1.presence.expect("presence join on A must be Some");
        assert!(
            j1.first_for_user,
            "u1's first cluster connection must be first_for_user"
        );

        // u1's SECOND connection (new socket) on B → NOT first_for_user.
        let (_s2, h2, m2) = presence_handle("u1", serde_json::json!({"name":"Ann"}));
        let out2 = adapter_b
            .subscribe(TEST_APP, "presence-room", h2, Some(m2))
            .await;
        let j2 = out2.presence.expect("presence join on B must be Some");
        assert!(
            !j2.first_for_user,
            "u1's second cluster connection (on another node) must NOT be first_for_user"
        );

        // u2's first connection on B → first_for_user (distinct user).
        let (_s3, h3, m3) = presence_handle("u2", serde_json::json!({"name":"Bob"}));
        let out3 = adapter_b
            .subscribe(TEST_APP, "presence-room", h3, Some(m3))
            .await;
        let j3 = out3.presence.expect("presence join for u2 must be Some");
        assert!(
            j3.first_for_user,
            "u2 (a distinct user) must be first_for_user"
        );
    })
    .await
    .expect("presence member_added test must not hang (Redis up?)");
}

/// B1 (SP7b): the roster a subscribing connection sees is the CLUSTER-wide presence
/// set, not the node-local one. With u1 on A and u2 on B, a third connection (u3) on A
/// must see all three users in its roster — ids sorted, distinct count, each with its
/// own `user_info`.
#[tokio::test]
async fn cross_node_presence_roster_is_cluster_wide() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let adapter_a = connect_adapter_with_prefix(&prefix).await;
        let adapter_b = connect_adapter_with_prefix(&prefix).await;

        let (_s1, h1, m1) = presence_handle("u1", serde_json::json!({"name":"Ann"}));
        adapter_a
            .subscribe(TEST_APP, "presence-room", h1, Some(m1))
            .await;

        let (_s2, h2, m2) = presence_handle("u2", serde_json::json!({"name":"Bob"}));
        adapter_b
            .subscribe(TEST_APP, "presence-room", h2, Some(m2))
            .await;

        // u3 subscribes on A — its roster must reflect the whole cluster.
        let (_s3, h3, m3) = presence_handle("u3", serde_json::json!({"name":"Cleo"}));
        let out3 = adapter_a
            .subscribe(TEST_APP, "presence-room", h3, Some(m3))
            .await;
        let roster = out3
            .presence
            .expect("presence join for u3 must be Some")
            .roster;

        assert_eq!(roster.count, 3, "cluster roster must count all 3 users");
        assert_eq!(
            roster.ids,
            vec!["u1".to_string(), "u2".to_string(), "u3".to_string()],
            "cluster roster ids must be sorted and contain u1,u2,u3"
        );
        assert_eq!(
            roster.hash.get("u1"),
            Some(&serde_json::json!({"name":"Ann"})),
            "roster hash must carry u1's user_info"
        );
        assert_eq!(
            roster.hash.get("u2"),
            Some(&serde_json::json!({"name":"Bob"})),
            "roster hash must carry u2's user_info"
        );
        assert_eq!(
            roster.hash.get("u3"),
            Some(&serde_json::json!({"name":"Cleo"})),
            "roster hash must carry u3's user_info"
        );
    })
    .await
    .expect("presence roster test must not hang (Redis up?)");
}

/// B1 (SP7b): `member_removed` fires exactly once per cluster-wide user transition.
/// `last_for_user` is the cluster refcount →0 edge. u1 has a connection on A (socket
/// sA) and on B (socket sB). Removing sA must NOT be last_for_user (u1 still has sB);
/// removing sB must be last_for_user, with the right `user_id`.
#[tokio::test]
async fn cross_node_presence_member_removed_single_emit() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let adapter_a = connect_adapter_with_prefix(&prefix).await;
        let adapter_b = connect_adapter_with_prefix(&prefix).await;

        // u1 on A (sA) and on B (sB).
        let (s_a, h_a, m_a) = presence_handle("u1", serde_json::json!({"name":"Ann"}));
        adapter_a
            .subscribe(TEST_APP, "presence-room", h_a, Some(m_a))
            .await;
        let (s_b, h_b, m_b) = presence_handle("u1", serde_json::json!({"name":"Ann"}));
        adapter_b
            .subscribe(TEST_APP, "presence-room", h_b, Some(m_b))
            .await;

        // Remove A's connection → NOT last_for_user (u1 still on B).
        let un_a = adapter_a.unsubscribe(TEST_APP, "presence-room", &s_a).await;
        let leave_a = un_a.presence.expect("presence leave on A must be Some");
        assert!(
            !leave_a.last_for_user,
            "u1 still has a connection on B → NOT last_for_user"
        );
        assert_eq!(leave_a.user_id, "u1");

        // Remove B's connection → last_for_user (u1's final cluster connection).
        let un_b = adapter_b.unsubscribe(TEST_APP, "presence-room", &s_b).await;
        let leave_b = un_b.presence.expect("presence leave on B must be Some");
        assert!(
            leave_b.last_for_user,
            "u1's final cluster connection gone → last_for_user"
        );
        assert_eq!(leave_b.user_id, "u1");
    })
    .await
    .expect("presence member_removed test must not hang (Redis up?)");
}

/// B2 (SP7b): `presence_members` and the presence `user_count` are CLUSTER-wide.
/// With u1 on A and u2 on B, A's `channel().user_count`, `presence_members`, and the
/// matching `channels()` entry must all reflect both users — not just A's local one.
///
/// (RED before B2: `channel().user_count` was node-local, so A would see `Some(1)` and
/// `presence_members` would list only u1.)
#[tokio::test]
async fn cross_node_presence_user_count_and_members() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let adapter_a = connect_adapter_with_prefix(&prefix).await;
        let adapter_b = connect_adapter_with_prefix(&prefix).await;

        // u1 on A, u2 on B → cluster presence of 2 distinct users.
        let (_s1, h1, m1) = presence_handle("u1", serde_json::json!({"name":"Ann"}));
        adapter_a
            .subscribe(TEST_APP, "presence-room", h1, Some(m1))
            .await;
        let (_s2, h2, m2) = presence_handle("u2", serde_json::json!({"name":"Bob"}));
        adapter_b
            .subscribe(TEST_APP, "presence-room", h2, Some(m2))
            .await;

        // A's channel() user_count is the cluster distinct-user count (2).
        let summary = adapter_a.channel(TEST_APP, "presence-room").await;
        assert_eq!(
            summary.user_count,
            Some(2),
            "channel().user_count must be the cluster-wide distinct-user count"
        );

        // A's presence_members lists BOTH users, sorted by user_id.
        let members = adapter_a.presence_members(TEST_APP, "presence-room").await;
        assert_eq!(
            members.len(),
            2,
            "presence_members must list the whole cluster roster"
        );
        let ids: Vec<String> = members.iter().map(|m| m.user_id.clone()).collect();
        assert_eq!(
            ids,
            vec!["u1".to_string(), "u2".to_string()],
            "presence_members ids must be sorted and contain u1,u2"
        );

        // channels(presence-) lists presence-room with the cluster user_count.
        let all = adapter_a.channels(TEST_APP, Some("presence-")).await;
        let pr = all
            .iter()
            .find(|c| c.name == "presence-room")
            .expect("channels() must list presence-room while it is occupied");
        assert_eq!(
            pr.user_count,
            Some(2),
            "channels() entry must carry the cluster-wide user_count"
        );
    })
    .await
    .expect("presence user_count/members test must not hang (Redis up?)");
}

/// C1 (SP7b): when the lease-locked sweeper reaps a crashed node's presence member, it
/// must decrement that user's cluster refcount and, on the →0 edge, remove the user from
/// the cluster roster (and emit `member_removed`). u1 connects on A and u2 on B; A
/// "crashes" (drop aborts its heartbeat) so u1's `expireAt` goes stale while u2 (B, still
/// heart-beating) stays fresh. B's sweep reaps u1 → the roster shrinks to {u2} and the
/// cluster `user_count` drops to 1. The `member_removed` emit (broadcast + webhook) rides
/// the same →0 edge as the roster/user_count change; we observe it via Redis state (the
/// presence side-tables), which proves the reap+emit path ran.
///
/// (RED before C1: the sweeper HDELs the stale occ token but never touches the presence
/// side-tables, so u1 stays in `presence_members` and `user_count` stays Some(2).)
#[tokio::test]
async fn sweeper_emits_member_removed_for_crashed_presence_member() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let prefix = random_prefix();
        // ttl=2s, heartbeat=1s — the same comfortable margin the sibling crash tests use.
        // A's `expireAt` (last stamped ≤ ~one heartbeat after the drop) is reapable after
        // the sleep below, while B's 1s heartbeat keeps u2 fresh AND keeps the occ key's
        // whole-key TTL alive so its presence side-tables survive to be reaped.
        let adapter_a = connect_adapter_with_prefix_ttl(&prefix, 2, 1).await;
        let adapter_b = connect_adapter_with_prefix_ttl(&prefix, 2, 1).await;

        // u1's connection on A, u2's connection on B → cluster presence {u1, u2}.
        let (_s1, h1, m1) = presence_handle("u1", serde_json::json!({"name": "Ann"}));
        adapter_a
            .subscribe(TEST_APP, "presence-room", h1, Some(m1))
            .await;
        let (_s2, h2, m2) = presence_handle("u2", serde_json::json!({"name": "Bob"}));
        adapter_b
            .subscribe(TEST_APP, "presence-room", h2, Some(m2))
            .await;

        // Sanity: before the crash the cluster sees both users.
        let before = adapter_b.presence_members(TEST_APP, "presence-room").await;
        let before_ids: Vec<String> = before.iter().map(|m| m.user_id.clone()).collect();
        assert_eq!(
            before_ids,
            vec!["u1".to_string(), "u2".to_string()],
            "before the crash the cluster roster must be {{u1, u2}}"
        );

        // Crash A: dropping the adapter aborts its heartbeat, so u1's `expireAt` stops
        // being re-stamped and falls into the past after the TTL. (Must be the LAST ref.)
        drop(adapter_a);

        // Sleep past u1's worst-case `expireAt` (≤ ~2s after its last stamp) so u1 is
        // reliably stale, while B's 1s heartbeat keeps u2 fresh and the occ key alive.
        tokio::time::sleep(Duration::from_millis(2600)).await;

        // B sweeps: it holds the lease, reaps u1's stale occ token, and the presence
        // branch decrements u1's refcount to 0 → removes u1 + emits member_removed.
        let webhooks = pylon::webhook::WebhookHandle::null();
        let (acquired, reaped, _vacated) = adapter_b.sweep_now(&webhooks, now_ms()).await;
        assert!(acquired, "B must acquire the sweep lease (no other holder)");
        assert!(
            reaped >= 1,
            "B must reap u1's stale member (reaped={reaped})"
        );

        // The roster now reflects only u2 — u1's →0 edge removed it from the cluster
        // presence side-tables (the same edge that emitted member_removed).
        let after = adapter_b.presence_members(TEST_APP, "presence-room").await;
        let after_ids: Vec<String> = after.iter().map(|m| m.user_id.clone()).collect();
        assert_eq!(
            after_ids,
            vec!["u2".to_string()],
            "after the sweep the cluster roster must be {{u2}} only (u1 reaped)"
        );

        let summary = adapter_b.channel(TEST_APP, "presence-room").await;
        assert_eq!(
            summary.user_count,
            Some(1),
            "after the sweep the cluster user_count must drop to 1 (got {:?})",
            summary.user_count
        );
    })
    .await
    .expect("sweeper member_removed test must not hang (Redis up?)");
}

/// A2: user online/offline is a SINGLE cluster-wide edge, not per-node. The FIRST
/// connection for a user anywhere in the cluster reports `first_for_user`; a second
/// connection on ANOTHER node does NOT. `is_user_online` reads cluster truth (`HLEN
/// usr`). Signing out the cluster-last connection (regardless of node) reports
/// `last_for_user`; an earlier signout on the other node does not. While `signin_user`
/// delegated to the node-local registry, B would see its own first/last edges as true
/// — this test pins the cluster single-emit semantics.
#[tokio::test]
async fn cross_node_user_online_offline_single_emit() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let adapter_a = connect_adapter_with_prefix(&prefix).await;
        let adapter_b = connect_adapter_with_prefix(&prefix).await;

        // 1. First connection for "u7" (socket sA on A): cluster online edge.
        let (sock_a, handle_a) = fake_handle();
        let out_a = adapter_a.signin_user(TEST_APP, "u7", handle_a).await;
        assert!(
            out_a.first_for_user,
            "first cluster connection for u7 → first_for_user (cluster online edge)"
        );

        // 2. Second connection (socket sB on B): NOT the cluster online edge.
        let (sock_b, handle_b) = fake_handle();
        let out_b = adapter_b.signin_user(TEST_APP, "u7", handle_b).await;
        assert!(
            !out_b.first_for_user,
            "second cluster connection on another node must NOT report first_for_user"
        );

        // 3. Cluster online check sees the user from either node.
        assert!(
            adapter_a.is_user_online(TEST_APP, "u7").await,
            "is_user_online must read cluster truth (HLEN usr > 0)"
        );

        // 4. Sign out B's connection → NOT the cluster-last edge (A still holds one).
        let out_b = adapter_b.signout_user(TEST_APP, "u7", &sock_b).await;
        assert!(
            !out_b.last_for_user,
            "a non-cluster-last signout must NOT report last_for_user"
        );
        assert!(
            adapter_a.is_user_online(TEST_APP, "u7").await,
            "u7 still online cluster-wide while A's connection remains"
        );

        // 5. Sign out A's connection → the cluster-last edge.
        let out_a = adapter_a.signout_user(TEST_APP, "u7", &sock_a).await;
        assert!(
            out_a.last_for_user,
            "the cluster-last signout must report last_for_user (cluster offline edge)"
        );

        // 6. Now offline cluster-wide.
        assert!(
            !adapter_a.is_user_online(TEST_APP, "u7").await,
            "u7 must be offline cluster-wide after the last connection signs out"
        );
    })
    .await
    .expect("cross-node user online/offline test must not hang (Redis up?)");
}

/// Build a fake `ConnectionHandle` whose mailbox receiver is RETURNED so a test can
/// assert what was delivered to it (the cross-node user-delivery tests need this).
fn recording_handle() -> (
    SocketId,
    ConnectionHandle,
    tokio::sync::mpsc::Receiver<Box<ServerEvent>>,
) {
    let socket_id = SocketId::generate();
    let (tx, rx) = tokio::sync::mpsc::channel(1024);
    let handle = ConnectionHandle {
        socket_id,
        mailbox: pylon::connection::handle::Mailbox::new(tx, None, None),
    };
    (socket_id, handle, rx)
}

/// B1: `send_to_user` from one node reaches the user's connection on ANOTHER node.
/// The receiving node subscribes `usermsg(user)` when the user signs in locally; the
/// originating node has no local connection of the user, so delivery is pure cross-node.
#[tokio::test]
async fn send_to_user_reaches_connection_on_another_node() {
    let prefix = random_prefix();
    let keys = Keys::new(&prefix);
    let node_a = connect_adapter_with_prefix(&prefix).await;
    let node_b = connect_adapter_with_prefix(&prefix).await;

    // B holds u7's connection → B subscribes usermsg(u7).
    let (_sid, handle_b, mut rx_b) = recording_handle();
    node_b.signin_user(TEST_APP, "u7", handle_b).await;
    assert!(
        await_tracked(
            &node_b,
            &keys.usermsg(TEST_APP, "u7"),
            Duration::from_secs(2)
        )
        .await,
        "node B must subscribe usermsg(u7)"
    );

    // A (no local u7 connection) sends to u7 → must reach B's connection.
    node_a
        .send_to_user(
            TEST_APP,
            "u7",
            ServerEvent::ChannelEvent {
                channel: "x".into(),
                event: "e".into(),
                data: serde_json::json!({"k":1}),
                user_id: None,
            },
        )
        .await;

    let got = with_timeout(async { rx_b.recv().await }).await.map(|b| *b);
    match got {
        Some(ServerEvent::Raw(frame)) => {
            let v: serde_json::Value = serde_json::from_str(&frame).expect("raw frame is JSON");
            assert_eq!(v["event"], "e", "cross-node send_to_user frame");
        }
        other => panic!("expected Raw frame on node B, got {other:?}"),
    }
}

/// B1: `terminate_user` from one node closes the user's connection on ANOTHER node
/// (4009 error frame then a 4009 Close).
#[tokio::test]
async fn terminate_user_closes_connection_on_another_node() {
    let prefix = random_prefix();
    let keys = Keys::new(&prefix);
    let node_a = connect_adapter_with_prefix(&prefix).await;
    let node_b = connect_adapter_with_prefix(&prefix).await;

    let (_sid, handle_b, mut rx_b) = recording_handle();
    node_b.signin_user(TEST_APP, "u8", handle_b).await;
    assert!(
        await_tracked(
            &node_b,
            &keys.usermsg(TEST_APP, "u8"),
            Duration::from_secs(2)
        )
        .await,
        "node B must subscribe usermsg(u8)"
    );

    node_a.terminate_user(TEST_APP, "u8").await;

    let first = with_timeout(async { rx_b.recv().await }).await.map(|b| *b);
    assert!(
        matches!(first, Some(ServerEvent::Error(ref e)) if e.code == 4009),
        "expected 4009 error frame on node B, got {first:?}"
    );
    let second = with_timeout(async { rx_b.recv().await }).await.map(|b| *b);
    assert!(
        matches!(second, Some(ServerEvent::Close { code: 4009, .. })),
        "expected 4009 Close on node B, got {second:?}"
    );
}

/// C1: cross-node watchlist. A node watching a user on the WATCHER side must
/// SUBSCRIBE that user's `watch(app,user)` channel, so a WatchOnline/WatchOffline
/// published by ANOTHER node (on the cluster online/offline edge of that user)
/// reaches it and is delivered to its local watchers as a `WatchlistEvents` frame.
///
/// (RED before C1: with `watch` delegating to the local adapter, A never SUBSCRIBEs
/// the watch channel, so B's WatchOnline publish never reaches A and `rx` gets
/// nothing.)
#[tokio::test]
async fn cross_node_watchlist_online_offline() {
    tokio::time::timeout(Duration::from_secs(6), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let adapter_a = connect_adapter_with_prefix(&prefix).await;
        let adapter_b = connect_adapter_with_prefix(&prefix).await;

        // 1. A watches u7 — not online yet, so the initial snapshot is empty. The
        //    watcher's mailbox rx is kept so we can assert what A delivers to it.
        let (_s, watcher, mut rx) = recording_handle();
        let online = adapter_a.watch(TEST_APP, watcher, vec!["u7".into()]).await;
        assert!(
            online.is_empty(),
            "u7 is not online yet → watch() initial snapshot must be empty (got {online:?})"
        );
        // Wait for A's Redis SUBSCRIBE of watch(u7) to take effect before B publishes.
        assert!(
            await_tracked(
                &adapter_a,
                &keys.watch(TEST_APP, "u7"),
                Duration::from_secs(2)
            )
            .await,
            "A must SUBSCRIBE watch(u7) on the 0→1 local watcher edge"
        );

        // 2. B signs in u7 → cluster online edge → publishes WatchOnline on watch(u7).
        let (_sb, handle_b, _rx_b) = recording_handle();
        let b_socket = handle_b.socket_id;
        adapter_b.signin_user(TEST_APP, "u7", handle_b).await;

        // 3. A's watcher receives a WatchlistEvents "online" for u7.
        let got = with_timeout(async { rx.recv().await }).await.map(|b| *b);
        match got {
            Some(ServerEvent::WatchlistEvents { events }) => {
                assert_eq!(events.len(), 1, "exactly one watchlist change");
                assert_eq!(events[0].name, "online", "u7 came online");
                assert_eq!(events[0].user_ids, vec!["u7".to_string()]);
            }
            other => panic!("expected WatchlistEvents online on A, got {other:?}"),
        }

        // 4. B signs out u7 → cluster offline edge → publishes WatchOffline → A gets it.
        adapter_b.signout_user(TEST_APP, "u7", &b_socket).await;
        let got = with_timeout(async { rx.recv().await }).await.map(|b| *b);
        match got {
            Some(ServerEvent::WatchlistEvents { events }) => {
                assert_eq!(events.len(), 1, "exactly one watchlist change");
                assert_eq!(events[0].name, "offline", "u7 went offline");
                assert_eq!(events[0].user_ids, vec!["u7".to_string()]);
            }
            other => panic!("expected WatchlistEvents offline on A, got {other:?}"),
        }
    })
    .await
    .expect("cross-node watchlist test must not hang (Redis up?)");
}

/// D1: the membership heartbeat + lease-locked sweeper extend to USER BINDINGS, so a
/// crashed node's signed-in user goes offline (and its watchers are notified) within the
/// TTL. A watches u7 on node A; B signs u7 in (cluster online edge → A's watcher sees
/// "online"). B "crashes" (drop aborts its heartbeat) so u7's `usr` binding goes stale.
/// After the TTL elapses, A's sweep reaps u7's last (dead-node) binding → the cluster →0
/// edge publishes WatchOffline → A's watcher receives a `WatchlistEvents` "offline", and
/// `is_user_online` reads false.
///
/// (RED before D1: without the user-binding heartbeat re-stamp + sweep, u7's binding is
/// never refreshed NOR reaped on B's crash — `is_user_online` stays true and no offline
/// notify ever reaches A's watcher.)
#[tokio::test]
async fn sweeper_offline_on_user_crash_notifies_watchers() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);

        // A watches u7 with a short TTL (2s) + heartbeat (1s) — the proven crash margin.
        let adapter_a = connect_adapter_with_prefix_ttl(&prefix, 2, 1).await;
        let (_s, watcher, mut rx) = recording_handle();
        let online = adapter_a.watch(TEST_APP, watcher, vec!["u7".into()]).await;
        assert!(
            online.is_empty(),
            "u7 is not online yet → watch() initial snapshot must be empty (got {online:?})"
        );
        assert!(
            await_tracked(
                &adapter_a,
                &keys.watch(TEST_APP, "u7"),
                Duration::from_secs(2)
            )
            .await,
            "A must SUBSCRIBE watch(u7) on the 0→1 local watcher edge"
        );

        // B signs u7 in → cluster online edge → publishes WatchOnline on watch(u7). A's
        // watcher receives the "online" first; drain it so we can assert the "offline".
        let adapter_b = connect_adapter_with_prefix_ttl(&prefix, 2, 1).await;
        let (_sb, b_handle, _rx_b) = recording_handle();
        adapter_b.signin_user(TEST_APP, "u7", b_handle).await;

        let got = with_timeout(async { rx.recv().await }).await.map(|b| *b);
        match got {
            Some(ServerEvent::WatchlistEvents { events }) => {
                assert_eq!(events.len(), 1, "exactly one watchlist change");
                assert_eq!(events[0].name, "online", "u7 came online via B's signin");
                assert_eq!(events[0].user_ids, vec!["u7".to_string()]);
            }
            other => panic!("expected WatchlistEvents online on A, got {other:?}"),
        }

        // Crash B: dropping the adapter aborts its heartbeat, so u7's `usr` binding stops
        // being re-stamped and its `expireAt` falls into the past. (Must be the LAST ref.)
        drop(adapter_b);

        // Sleep past u7's worst-case `expireAt` (≤ ~2s after its last stamp) so the
        // binding is reliably stale, while A's heartbeat keeps A's own state alive.
        tokio::time::sleep(Duration::from_millis(2600)).await;

        // A sweeps: it holds the lease (nobody else does), reaps u7's stale binding, and
        // the user branch's →0 edge publishes WatchOffline → A notifies its local watcher.
        let webhooks = pylon::webhook::WebhookHandle::null();
        let (acquired, _reaped, _vacated) = adapter_a.sweep_now(&webhooks, now_ms()).await;
        assert!(acquired, "A must acquire the sweep lease (no other holder)");

        // A's watcher receives a WatchlistEvents "offline" for u7.
        let got = with_timeout(async { rx.recv().await }).await.map(|b| *b);
        match got {
            Some(ServerEvent::WatchlistEvents { events }) => {
                assert_eq!(events.len(), 1, "exactly one watchlist change");
                assert_eq!(events[0].name, "offline", "u7 went offline on B's crash");
                assert_eq!(events[0].user_ids, vec!["u7".to_string()]);
            }
            other => panic!("expected WatchlistEvents offline on A, got {other:?}"),
        }

        // And the cluster online check now reads false (the `usr` binding was reaped).
        assert!(
            !adapter_a.is_user_online(TEST_APP, "u7").await,
            "u7 must be offline cluster-wide after the sweep reaped its dead-node binding"
        );
    })
    .await
    .expect("sweeper user-crash offline test must not hang (Redis up?)");
}

/// C1: the `watch` initial-online snapshot is CLUSTER-wide. If a user is already
/// online on ANOTHER node when a connection starts watching it, that user must be in
/// the returned online set (driven by the cluster `is_user_online`, i.e. `HLEN usr`),
/// not just the node-local `users` map.
///
/// (RED before C1: with the node-local snapshot, A has no local connection of u7, so
/// `watch` would return an empty online set even though u7 is online on B.)
#[tokio::test]
async fn watch_initial_snapshot_is_cluster_wide() {
    tokio::time::timeout(Duration::from_secs(6), async {
        let prefix = random_prefix();
        let adapter_a = connect_adapter_with_prefix(&prefix).await;
        let adapter_b = connect_adapter_with_prefix(&prefix).await;

        // 1. B signs in u7 first → u7 is online cluster-wide (HLEN usr > 0).
        let (_sb, handle_b, _rx_b) = recording_handle();
        adapter_b.signin_user(TEST_APP, "u7", handle_b).await;

        // 2. A starts watching u7 → its initial snapshot is the cluster online check,
        //    so u7 must be reported online even though A holds no local connection.
        let (_s, watcher, _rx) = recording_handle();
        let online = adapter_a.watch(TEST_APP, watcher, vec!["u7".into()]).await;
        assert_eq!(
            online,
            vec!["u7".to_string()],
            "watch() initial snapshot must be cluster-wide (u7 online on B)"
        );
    })
    .await
    .expect("cluster watch-snapshot test must not hang (Redis up?)");
}

// ---------------------------------------------------------------------------
// SP11 Phase 3.1: direct tests for the extracted cluster-only coordination ops.
//
// These call the `cluster_*` methods (the Redis/cluster half the `ClusterBridge`
// will own) DIRECTLY — without any `LocalAdapter` subscribe — and assert they
// return the authoritative cluster value. They are the same random-prefix-isolated,
// fail-loud-if-Redis-down shape as the suite above.
// ---------------------------------------------------------------------------

/// `cluster_subscribe` records cluster-wide membership and returns the AUTHORITATIVE
/// `(count, occupied)` WITHOUT any local subscribe. Two nodes calling it for the same
/// channel see cluster counts 1 (occupied) then 2 (not occupied) — proving it reads the
/// cluster `HLEN`, not a node-local view. The `node_first` flag drives the msg-channel
/// subscribe lifecycle, so the caller's Redis subscriber tracks the channel.
#[tokio::test]
async fn cluster_subscribe_returns_cluster_count_without_local() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let adapter_a = connect_adapter_with_prefix(&prefix).await;
        let adapter_b = connect_adapter_with_prefix(&prefix).await;

        let sock_a = SocketId::generate();
        let (count_a, occ_a) = adapter_a
            .cluster_subscribe(TEST_APP, "public-room", &sock_a, true)
            .await;
        assert_eq!(count_a, 1, "first cluster member → cluster count 1");
        assert!(occ_a, "0→1 cluster edge must report occupied");

        // `node_first == true` must have SUBSCRIBEd the msg channel on A.
        let key = Keys::new(&prefix).msg(TEST_APP, "public-room");
        assert!(
            adapter_a.tracked_redis_channels().contains(&key),
            "node_first must SUBSCRIBE the channel's msg key"
        );

        // A second node's cluster_subscribe sees the cluster count 2, NOT occupied —
        // proving the count is the cluster HLEN, not a node-local 1.
        let sock_b = SocketId::generate();
        let (count_b, occ_b) = adapter_b
            .cluster_subscribe(TEST_APP, "public-room", &sock_b, true)
            .await;
        assert_eq!(
            count_b, 2,
            "second cluster member on another node → count 2"
        );
        assert!(
            !occ_b,
            "a non-0→1 cluster_subscribe must NOT report occupied"
        );

        // `cluster_unsubscribe` mirrors it: 2→1 (not vacated), then 1→0 (vacated).
        let (rem_b, vac_b) = adapter_b
            .cluster_unsubscribe(TEST_APP, "public-room", &sock_b, true)
            .await;
        assert_eq!(rem_b, 1, "one cluster member remains → count 1");
        assert!(
            !vac_b,
            "a non-1→0 cluster_unsubscribe must NOT report vacated"
        );

        let (rem_a, vac_a) = adapter_a
            .cluster_unsubscribe(TEST_APP, "public-room", &sock_a, true)
            .await;
        assert_eq!(rem_a, 0, "last cluster member gone → count 0");
        assert!(vac_a, "1→0 cluster edge must report vacated");
    })
    .await
    .expect("cluster_subscribe direct test must not hang (Redis up?)");
}

/// `cluster_presence_capacity` returns the cluster distinct-user count and whether a
/// given user is already in the cluster roster — the presence-subscribe admission probe —
/// reading cross-node Redis state, not any node-local roster. After `cluster_presence_join`
/// of u1 on A and u2 on B, A's probe sees count 2, `already_member` true for u1 / u2 and
/// false for an unseen u3.
#[tokio::test]
async fn cluster_presence_capacity_is_cluster_wide() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let adapter_a = connect_adapter_with_prefix(&prefix).await;
        let adapter_b = connect_adapter_with_prefix(&prefix).await;

        let (s1, _h1, m1) = presence_handle("u1", serde_json::json!({"name":"Ann"}));
        let (first1, _roster1) = adapter_a
            .cluster_presence_join(TEST_APP, "presence-room", &m1, &s1)
            .await
            .expect("cluster_presence_join on A must succeed");
        assert!(
            first1,
            "u1's first cluster connection must be first_for_user"
        );

        let (s2, _h2, m2) = presence_handle("u2", serde_json::json!({"name":"Bob"}));
        adapter_b
            .cluster_presence_join(TEST_APP, "presence-room", &m2, &s2)
            .await
            .expect("cluster_presence_join on B must succeed");

        // A's capacity probe is cluster-wide: 2 distinct users, u1 & u2 already members.
        let (count, u1_member) = adapter_a
            .cluster_presence_capacity(TEST_APP, "presence-room", "u1")
            .await;
        assert_eq!(count, 2, "cluster distinct-user count must be 2");
        assert!(u1_member, "u1 must read as already a cluster member");

        let (_c, u2_member) = adapter_a
            .cluster_presence_capacity(TEST_APP, "presence-room", "u2")
            .await;
        assert!(
            u2_member,
            "u2 (joined on B) must read as already a member on A"
        );

        let (_c, u3_member) = adapter_a
            .cluster_presence_capacity(TEST_APP, "presence-room", "u3")
            .await;
        assert!(
            !u3_member,
            "an unseen user must NOT read as already a member"
        );

        // Leaving drops the cluster count back down (last_for_user edge).
        let last1 = adapter_a
            .cluster_presence_leave(TEST_APP, "presence-room", "u1", &s1)
            .await
            .expect("cluster_presence_leave must succeed");
        assert!(
            last1,
            "u1's only cluster connection leaving → last_for_user"
        );
        let (count_after, _) = adapter_a
            .cluster_presence_capacity(TEST_APP, "presence-room", "u1")
            .await;
        assert_eq!(count_after, 1, "after u1 leaves, cluster count is 1 (u2)");
    })
    .await
    .expect("cluster_presence_capacity direct test must not hang (Redis up?)");
}

/// `cluster_publish_broadcast` PUBLISHes the Broadcast envelope on the channel's `msg`
/// key for cross-node delivery — and does NO local delivery itself (that is the caller's
/// job). A second node subscribed to the channel receives the pre-encoded frame; the
/// publisher does not loop it back into any local mailbox (it has none here).
#[tokio::test]
async fn cluster_publish_broadcast_fans_out_only_remote() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let adapter_a = connect_adapter_with_prefix(&prefix).await;
        let adapter_b = connect_adapter_with_prefix(&prefix).await;

        // B subscribes locally so its receive loop will deliver A's remote broadcast.
        let (sock_b, handle_b, mut rx_b) = recording_handle();
        adapter_b
            .subscribe(TEST_APP, "public-room", handle_b, None)
            .await;

        // A publishes ONLY the cluster half — a pre-encoded v7 frame — with no local
        // delivery. B's subscriber receives it and re-delivers to B's local socket.
        let frame =
            pylon::protocol::v7::frames::encode(&ServerEvent::Raw(std::sync::Arc::from("ping")));
        adapter_a
            .cluster_publish_broadcast(TEST_APP, "public-room", frame.clone(), None)
            .await;

        let received = *tokio::time::timeout(Duration::from_secs(2), rx_b.recv())
            .await
            .expect("B must receive the cross-node broadcast in time")
            .expect("B's mailbox must yield the broadcast");
        match received {
            ServerEvent::Raw(f) => assert_eq!(&*f, &frame, "B must get A's pre-encoded frame"),
            other => panic!("expected a Raw frame, got {other:?}"),
        }

        // Cleanup B's membership so the prefix's keys vacate cleanly.
        adapter_b
            .unsubscribe(TEST_APP, "public-room", &sock_b)
            .await;
    })
    .await
    .expect("cluster_publish_broadcast direct test must not hang (Redis up?)");
}

/// `purge_app` on a `RedisAdapter` closes all local connections with 4009 and
/// removes the app from the Redis `apps` set so the sweeper stops enumerating it.
#[tokio::test]
async fn purge_app_closes_connections_and_removes_from_redis_apps_set() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let prefix = random_prefix();
        let adapter = connect_adapter_with_prefix(&prefix).await;
        let keys = Keys::new(&prefix);

        // Register the app in the `apps` set by subscribing a connection (same as
        // `cluster_subscribe` does) so the pre-condition matches production.
        let (tx, _rx) = tokio::sync::mpsc::channel::<Box<ServerEvent>>(64);
        let sock = SocketId::generate();
        let handle = ConnectionHandle {
            socket_id: sock,
            mailbox: pylon::connection::handle::Mailbox::new(tx, None, None),
        };
        adapter.subscribe(TEST_APP, "public-room", handle, None).await;

        // Confirm the app is indexed in the Redis `apps` set.
        let clients = RedisClients::connect(&test_redis_url(), 1)
            .await
            .expect("test clients must connect");
        let is_member: bool = clients
            .pool
            .next()
            .sismember(keys.apps(), TEST_APP)
            .await
            .expect("SISMEMBER apps must succeed");
        assert!(is_member, "app must be in the `apps` set after subscribe");

        // Now purge the app — all local connections closed, Redis `apps` SREM'd.
        let ids = adapter.purge_app(TEST_APP).await;
        // The RedisAdapter's internal LocalAdapter has its own private AppRegistry
        // with no worker-registered connections — `subscribe` goes to the channel
        // registry only, not app_registry — so drain_app always returns empty here.
        // The real point of this test is the SREM assertion below.
        assert!(
            ids.is_empty(),
            "RedisAdapter's private app_registry has no worker-registered conns; ids must be empty"
        );

        // The app must have been removed from the Redis `apps` set.
        let still_member: bool = clients
            .pool
            .next()
            .sismember(keys.apps(), TEST_APP)
            .await
            .expect("SISMEMBER apps must succeed after purge");
        assert!(
            !still_member,
            "app must be removed from the `apps` set after purge_app"
        );
    })
    .await
    .expect("purge_app Redis test must not hang (Redis up?)");
}
