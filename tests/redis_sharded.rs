//! Integration tests for `PYLON_REDIS_SHARDED_PUBSUB` (Redis 7 sharded pub/sub).
//!
//! These talk to a REAL Redis 7+ (SSUBSCRIBE/SPUBLISH need 7.0). Same shape as
//! `redis_cluster.rs`: random per-run key/channel prefix so a shared Redis is
//! never clobbered (no FLUSHALL/FLUSHDB or any unscoped destructive command),
//! and they FAIL LOUD if Redis is unreachable — there is no silent skip.
//!
//! What these pin beyond `redis_cluster.rs`: with the flag ON, subscriptions
//! are genuinely SHARDED — visible to `PUBSUB SHARDCHANNELS` on the server and
//! invisible to `PUBSUB CHANNELS` — while SPUBLISH-driven cross-node delivery
//! keeps working (broadcast, per-user send, watchlist). With the flag OFF the
//! ordinary pub/sub path is untouched.

use fred::prelude::*;
use pylon::adapter::redis::client::RedisClients;
use pylon::adapter::redis::keys::Keys;
use pylon::adapter::redis::RedisAdapter;
use pylon::adapter::Adapter;
use pylon::connection::handle::ConnectionHandle;
use pylon::protocol::event::ServerEvent;
use pylon::protocol::socket_id::SocketId;
use pylon::server::config::ServerConfig;
use std::time::Duration;
use uuid::Uuid;

/// Fixed app id used by these tests (plain string arg to the adapter).
const TEST_APP: &str = "app1";

/// Test Redis URL: `PYLON_TEST_REDIS_URL` or the documented default.
fn test_redis_url() -> String {
    std::env::var("PYLON_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

/// A random, run-unique key/channel prefix for isolation on a shared Redis.
fn random_prefix() -> String {
    format!("pylontest:{}", Uuid::new_v4())
}

/// Build a `ServerConfig` for the Redis adapter with `redis_sharded_pubsub`
/// set explicitly — the knob these tests exercise.
fn sharded_test_config(prefix: &str, sharded: bool) -> ServerConfig {
    ServerConfig {
        adapter: "redis".into(),
        redis_url: test_redis_url(),
        redis_prefix: prefix.into(),
        redis_sharded_pubsub: sharded,
        ..ServerConfig::default()
    }
}

/// Build a connected `RedisAdapter` sharing `prefix` with the sharded flag as
/// given. Fails loud if Redis is down.
async fn connect_sharded(prefix: &str, sharded: bool) -> RedisAdapter {
    let cfg = sharded_test_config(prefix, sharded);
    RedisAdapter::new(&cfg)
        .await
        .expect("RedisAdapter::new must connect to the test Redis")
}

/// A raw probe client pair for SERVER-side pub/sub introspection
/// (`PUBSUB SHARDCHANNELS` / `PUBSUB CHANNELS`) — the authoritative view of
/// which subscribe mode the adapter actually used.
async fn probe_clients() -> RedisClients {
    RedisClients::connect(&test_redis_url(), 1)
        .await
        .expect("fred probe clients must connect to the test Redis")
}

/// Whether the server's sharded-channel index lists `channel`. Uses the full
/// literal channel name as the glob pattern (Redis glob has no special braces,
/// and the random prefix keeps it run-unique anyway).
async fn shardchannels_contain(clients: &RedisClients, channel: &str) -> bool {
    let list: Vec<String> = clients
        .pool
        .next()
        .pubsub_shardchannels(channel.to_string())
        .await
        .unwrap_or_default();
    list.iter().any(|c| c == channel)
}

/// Whether the server's ORDINARY channel index lists `channel` (i.e. the
/// subscription went through plain SUBSCRIBE, not SSUBSCRIBE).
async fn channels_contain(clients: &RedisClients, channel: &str) -> bool {
    let list: Vec<String> = clients
        .pool
        .next()
        .pubsub_channels(channel.to_string())
        .await
        .unwrap_or_default();
    list.iter().any(|c| c == channel)
}

/// Poll the server-side sharded-channel index until `channel` appears or the
/// deadline elapses. The publisher waits on this so the first SPUBLISH cannot
/// race the SSUBSCRIBE landing on the shard.
async fn await_shardchannel(clients: &RedisClients, channel: &str, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if shardchannels_contain(clients, channel).await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Poll until the server-side sharded-channel index NO LONGER lists `channel`
/// (or the deadline elapses) — the teardown-side twin of [`await_shardchannel`].
async fn await_shardchannel_gone(clients: &RedisClients, channel: &str, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if !shardchannels_contain(clients, channel).await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Poll the server-side ORDINARY channel index until `channel` appears or the
/// deadline elapses (the flag-off twin of [`await_shardchannel`]).
async fn await_channel(clients: &RedisClients, channel: &str, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if channels_contain(clients, channel).await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Subscribe a fresh fake socket to `(TEST_APP, channel)` on `adapter`,
/// returning its `SocketId` and the receiving half of its mailbox.
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

/// Build a fake `ConnectionHandle` whose mailbox receiver is RETURNED so a
/// test can assert what was delivered to it (user-delivery paths).
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

/// Short timeout wrapper so a wedged Redis fails loud instead of hanging.
async fn with_timeout<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(Duration::from_secs(2), fut)
        .await
        .expect("redis op must not hang (Redis up?)")
}

/// D1/4.1: with `PYLON_REDIS_SHARDED_PUBSUB=1`, a channel subscription is a
/// genuine SSUBSCRIBE — the server's `PUBSUB SHARDCHANNELS` index lists the
/// msg channel while `PUBSUB CHANNELS` does not — and an A-side broadcast
/// (SPUBLISH) still reaches B's subscriber cross-node. Teardown on the node
/// 1→0 edge removes the shard channel again.
#[tokio::test]
async fn sharded_broadcast_cross_node_with_shardchannels_proof() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let probe = probe_clients().await;
        let adapter_a = connect_sharded(&prefix, true).await;
        let adapter_b = connect_sharded(&prefix, true).await;

        let msg_key = keys.msg(TEST_APP, "public-room");

        // B subscribes → node 0→1 edge → SSUBSCRIBE. Wait for the SHARDED
        // index to list the channel before publishing.
        let (sock_b, mut rx_b) = subscribe_socket(&adapter_b, "public-room").await;
        assert!(
            await_shardchannel(&probe, &msg_key, Duration::from_secs(2)).await,
            "with the flag ON the msg channel must appear in PUBSUB SHARDCHANNELS"
        );
        assert!(
            !channels_contain(&probe, &msg_key).await,
            "a sharded subscription must NOT appear in the ordinary PUBSUB CHANNELS index"
        );

        // A broadcasts → SPUBLISH → B's subscriber receives the pre-encoded frame.
        adapter_a
            .broadcast(
                TEST_APP,
                "public-room",
                ServerEvent::ChannelEvent {
                    channel: "public-room".into(),
                    event: "sharded-event".into(),
                    data: serde_json::json!({ "hello": "sharded" }),
                    user_id: None,
                },
                None,
            )
            .await;

        let got = with_timeout(async { rx_b.recv().await })
            .await
            .map(|b| *b)
            .expect("B's subscriber must receive the cross-node sharded broadcast");
        match got {
            ServerEvent::Raw(frame) => {
                let v: serde_json::Value =
                    serde_json::from_str(&frame).expect("Raw frame must be valid JSON");
                assert_eq!(v["event"], "sharded-event");
                assert_eq!(v["channel"], "public-room");
                assert_eq!(v["data"]["hello"], "sharded");
            }
            other => panic!("expected Raw frame on node B, got {other:?}"),
        }

        // Teardown: the node-local 1→0 edge must SUNSUBSCRIBE — the shard-channel
        // index drops the channel again.
        adapter_b
            .unsubscribe(TEST_APP, "public-room", &sock_b)
            .await;
        assert!(
            await_shardchannel_gone(&probe, &msg_key, Duration::from_secs(2)).await,
            "the node 1→0 edge must SUNSUBSCRIBE — SHARDCHANNELS must drop the channel"
        );
    })
    .await
    .expect("sharded broadcast test must not hang (Redis up?)");
}

/// D1/4.1: the per-user fan-out follows the flag too — B's signin SSUBSCRIBEs
/// the `usermsg` channel (SHARDCHANNELS proof) and A's `send_to_user`
/// (SPUBLISH) reaches B's connection cross-node.
#[tokio::test]
async fn sharded_send_to_user_cross_node() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let probe = probe_clients().await;
        let node_a = connect_sharded(&prefix, true).await;
        let node_b = connect_sharded(&prefix, true).await;

        // B holds u7's connection → B SSUBSCRIBEs usermsg(u7).
        let (_sid, handle_b, mut rx_b) = recording_handle();
        node_b.signin_user(TEST_APP, "u7", handle_b).await;
        let usermsg = keys.usermsg(TEST_APP, "u7");
        assert!(
            await_shardchannel(&probe, &usermsg, Duration::from_secs(2)).await,
            "with the flag ON the usermsg channel must appear in PUBSUB SHARDCHANNELS"
        );

        // A (no local u7 connection) sends to u7 → SPUBLISH → must reach B.
        node_a
            .send_to_user(
                TEST_APP,
                "u7",
                ServerEvent::ChannelEvent {
                    channel: "x".into(),
                    event: "sharded-user-event".into(),
                    data: serde_json::json!({"k":1}),
                    user_id: None,
                },
            )
            .await;

        let got = with_timeout(async { rx_b.recv().await })
            .await
            .map(|b| *b)
            .expect("B's u7 connection must receive the sharded user send");
        match got {
            ServerEvent::Raw(frame) => {
                let v: serde_json::Value =
                    serde_json::from_str(&frame).expect("Raw frame must be valid JSON");
                assert_eq!(v["event"], "sharded-user-event");
            }
            other => panic!("expected Raw frame on node B, got {other:?}"),
        }
    })
    .await
    .expect("sharded send_to_user test must not hang (Redis up?)");
}

/// D1/4.1: the watchlist fan-out follows the flag — A's watch SSUBSCRIBEs the
/// per-user `watch` channel (SHARDCHANNELS proof) and B's cluster online edge
/// (SPUBLISH) delivers the WatchlistEvents frame to A's watcher.
#[tokio::test]
async fn sharded_watchlist_cross_node() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let probe = probe_clients().await;
        let adapter_a = connect_sharded(&prefix, true).await;
        let adapter_b = connect_sharded(&prefix, true).await;

        // A watches u7 (not online yet → empty initial snapshot).
        let (_s, watcher, mut rx) = recording_handle();
        let online = adapter_a.watch(TEST_APP, watcher, vec!["u7".into()]).await;
        assert!(
            online.is_empty(),
            "u7 is not online yet → watch() initial snapshot must be empty (got {online:?})"
        );
        let watch_key = keys.watch(TEST_APP, "u7");
        assert!(
            await_shardchannel(&probe, &watch_key, Duration::from_secs(2)).await,
            "with the flag ON the watch channel must appear in PUBSUB SHARDCHANNELS"
        );

        // B signs in u7 → cluster online edge → SPUBLISH on watch(u7) → A's watcher.
        let (_sb, handle_b, _rx_b) = recording_handle();
        let b_socket = handle_b.socket_id;
        adapter_b.signin_user(TEST_APP, "u7", handle_b).await;

        let got = with_timeout(async { rx.recv().await })
            .await
            .map(|b| *b)
            .expect("A's watcher must receive the sharded WatchOnline");
        match got {
            ServerEvent::WatchlistEvents { events } => {
                assert_eq!(events.len(), 1, "exactly one watchlist change");
                assert_eq!(events[0].name, "online", "u7 came online");
                assert_eq!(events[0].user_ids, vec!["u7".to_string()]);
            }
            other => panic!("expected WatchlistEvents online on A, got {other:?}"),
        }

        // Sign-out edge: the sharded WatchOffline reaches A too.
        adapter_b.signout_user(TEST_APP, "u7", &b_socket).await;
        let got = with_timeout(async { rx.recv().await })
            .await
            .map(|b| *b)
            .expect("A's watcher must receive the sharded WatchOffline");
        match got {
            ServerEvent::WatchlistEvents { events } => {
                assert_eq!(events.len(), 1, "exactly one watchlist change");
                assert_eq!(events[0].name, "offline", "u7 went offline");
                assert_eq!(events[0].user_ids, vec!["u7".to_string()]);
            }
            other => panic!("expected WatchlistEvents offline on A, got {other:?}"),
        }
    })
    .await
    .expect("sharded watchlist test must not hang (Redis up?)");
}

/// D1/4.1: flag OFF (the default) keeps the ORDINARY pub/sub path — the msg
/// channel shows up in `PUBSUB CHANNELS` and NOT in `PUBSUB SHARDCHANNELS`,
/// and cross-node broadcast still delivers. (The full ordinary-path behavior
/// matrix is pinned by `redis_cluster.rs`; this is the mode proof.)
#[tokio::test]
async fn flag_off_keeps_ordinary_pubsub() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let probe = probe_clients().await;
        let adapter_a = connect_sharded(&prefix, false).await;
        let adapter_b = connect_sharded(&prefix, false).await;

        let msg_key = keys.msg(TEST_APP, "public-room");

        let (_sock_b, mut rx_b) = subscribe_socket(&adapter_b, "public-room").await;
        assert!(
            await_channel(&probe, &msg_key, Duration::from_secs(2)).await,
            "with the flag OFF the msg channel must appear in ordinary PUBSUB CHANNELS"
        );
        assert!(
            !shardchannels_contain(&probe, &msg_key).await,
            "with the flag OFF the msg channel must NOT appear in PUBSUB SHARDCHANNELS"
        );

        adapter_a
            .broadcast(
                TEST_APP,
                "public-room",
                ServerEvent::ChannelEvent {
                    channel: "public-room".into(),
                    event: "ordinary-event".into(),
                    data: serde_json::json!({ "hello": "ordinary" }),
                    user_id: None,
                },
                None,
            )
            .await;

        let got = with_timeout(async { rx_b.recv().await })
            .await
            .map(|b| *b)
            .expect("B's subscriber must receive the ordinary cross-node broadcast");
        match got {
            ServerEvent::Raw(frame) => {
                let v: serde_json::Value =
                    serde_json::from_str(&frame).expect("Raw frame must be valid JSON");
                assert_eq!(v["event"], "ordinary-event");
            }
            other => panic!("expected Raw frame on node B, got {other:?}"),
        }
    })
    .await
    .expect("flag-off ordinary pubsub test must not hang (Redis up?)");
}
