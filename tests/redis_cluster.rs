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
