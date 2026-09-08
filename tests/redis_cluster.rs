//! Integration tests for the Redis scaling adapter (SP7a).
//!
//! These talk to a REAL Redis. Point `PYLON_TEST_REDIS_URL` at a throwaway
//! instance (default `redis://127.0.0.1:6390`). Each run uses a random key/channel
//! prefix (`pylontest:<uuid>`) so a shared Redis is never clobbered — we NEVER
//! issue FLUSHALL/FLUSHDB or any unscoped destructive command.
//!
//! They FAIL LOUD if Redis is unreachable (the connect error propagates) — there
//! is no silent skip.

use fred::prelude::*;
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::redis::keys::Keys;
use pylon::adapter::redis::{client::RedisClients, client::Scripts, RedisAdapter};
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::cache::CachedEvent;
use pylon::channel::registry::Registry;
use pylon::connection::handle::ConnectionHandle;
use pylon::protocol::event::ServerEvent;
use pylon::protocol::socket_id::SocketId;
use pylon::server::config::ServerConfig;
use pylon::webhook::dispatcher::SystemClock;
use pylon::webhook::transport::{RecordingTransport, WebhookTransport};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Fixed app id used by the cluster lifecycle tests. Channel/app ids are plain
/// string args to the adapter; they don't come from `ServerConfig`.
const TEST_APP: &str = "app1";

/// Test Redis URL: `PYLON_TEST_REDIS_URL` or the documented default.
fn test_redis_url() -> String {
    std::env::var("PYLON_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6390".to_string())
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
    // PUBLISH returns the number of subscribers the SERVER delivered to, and
    // that count is authoritative and immediate — Redis pub/sub does not
    // buffer for late subscribers, so a copy dispatched to zero connections
    // is gone for good, not merely slow. A zero here is the SAME
    // single-copy-loss class `recover_lost_copy` below tolerates on a
    // recv-side timeout (subscription gone, e.g. a subscriber reconnect
    // between the SUBSCRIBE confirmation and the PUBLISH); it's just
    // observed at publish time rather than recv time, and gets the same
    // recovery instead of a bare, unrecoverable assert.
    let first_count: i64 = clients
        .pool
        .next()
        .publish(channel.clone(), payload.clone())
        .await
        .expect("PUBLISH must succeed");

    // Receive, event-bound on arrival (`broadcast::recv` is cancel-safe: a
    // dropped pending recv consumes nothing), with a hard PER-COPY bound so a
    // broken stream fails instead of hanging.
    //
    // Flake history (PR #14 CI, 2-core runner, job 100255097741): the 5s bound
    // expired once — with SUBSCRIBE confirmed, PUBLISH returned, every
    // neighboring pub/sub round trip 10-15ms, and the runner healthy directly
    // before AND after (the next suite ran 4 round trips in 60ms). The failure
    // is an ISOLATED loss/delay of exactly one message on one connection —
    // the environmental tail (docker-bridge segment loss + TCP RTO backoff,
    // hypervisor steal stalling the runtime, or a subscriber blip) — not a
    // wall-clock settle window that a bigger deadline fixes: two of those
    // classes only ever DELIVER LATE, the blip class NEVER delivers.
    //
    // A second flake (issue #23) put the same loss class on the OTHER side of
    // the round trip: `subscribe().await` resolved `Ok` and the very next
    // PUBLISH still reported zero subscribers server-side. `subscribe()`
    // resolving means the server acknowledged the SUBSCRIBE command, not that
    // every later subscriber-count read (this PUBLISH's return value
    // included) has already caught up — so a zero first-publish count isn't
    // a different failure mode, it's the identical class caught one step
    // earlier. Both faces route through the same `recover_lost_copy`: gate on
    // the observable server state (PUBSUB NUMSUB) and re-publish ONE second
    // copy with the SAME payload, only failing loud if that gate itself says
    // the subscriber is genuinely gone. Redis pub/sub is best-effort by
    // contract; the smoke's purpose is "the round trip works", which one
    // retry preserves without masking real breakage.
    let msg = if first_count >= 1 {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Ok(msg)) => msg,
            Ok(Err(e)) => panic!("broadcast receiver must yield a message: {e:?}"),
            Err(_) => recover_lost_copy(&clients, &channel, &payload, &mut rx, first_count).await,
        }
    } else {
        recover_lost_copy(&clients, &channel, &payload, &mut rx, first_count).await
    };

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

/// Recover from a lost first pub/sub copy in `smoke_connectivity` — shared by
/// its two faces of the same single-copy-loss class: a PUBLISH that reported
/// zero subscribers, and a copy that reported >=1 subscriber but never
/// arrived within its recv bound. Gates the retry on the server's own
/// observable state (`PUBSUB NUMSUB`, via `require_numsub_at_least`) rather
/// than a bare retry loop, re-publishes ONE second copy with the SAME
/// payload, and waits for it (or a still-arriving first copy — `recv` is
/// cancel-safe, so neither can fall between recvs). Fails loud — never masks
/// a genuinely broken stream — if the gate itself finds no subscriber, or if
/// neither copy arrives within its bound.
async fn recover_lost_copy(
    clients: &RedisClients,
    channel: &str,
    payload: &str,
    rx: &mut tokio::sync::broadcast::Receiver<fred::types::Message>,
    first_count: i64,
) -> fred::types::Message {
    require_numsub_at_least(
        clients.pool.next(),
        channel,
        1,
        Duration::from_secs(2),
        &format!(
            "first copy lost (PUBLISH reported {first_count} subscriber(s) at publish time); \
             tracked: {:?}",
            clients.sub.tracked_channels()
        ),
    )
    .await;

    eprintln!(
        "smoke_connectivity: first publish not delivered (PUBLISH reported {first_count} \
         subscriber(s) at publish time) — re-publishing one retry copy"
    );
    let retry_count: i64 = clients
        .pool
        .next()
        .publish(channel.to_string(), payload.to_string())
        .await
        .expect("retry PUBLISH must succeed");
    assert!(
        retry_count >= 1,
        "retry PUBLISH delivered to {retry_count} subscribers"
    );

    // Wait for EITHER copy, then the caller's assert holds (same
    // payload/channel on both).
    match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
        Ok(Ok(msg)) => msg,
        Ok(Err(e)) => panic!("broadcast receiver must yield a message: {e:?}"),
        Err(_) => panic!(
            "neither the first publish nor a retry (confirmed delivered to >=1 subscriber \
             server-side) arrived within 5s per copy on {channel} (tracked: {:?}) — the \
             subscriber stream is broken",
            clients.sub.tracked_channels()
        ),
    }
}

/// Poll Redis `PUBSUB NUMSUB <channel>` until the server reports at least
/// `want` subscribers attached to the pub/sub channel. The observable "the
/// SUBSCRIBE really is live server-side" — used to gate a re-publish, or an
/// upcoming publish a probe needs to see, after a completed `subscribe()`
/// that doesn't yet guarantee delivery.
///
/// `client` must NOT itself hold any active subscription: RESP2 forbids
/// ordinary commands — `PUBSUB NUMSUB` included — on a connection that is in
/// subscribe context (confirmed against a live server: Redis rejects it
/// outright with "only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT /
/// RESET are allowed in this context"), so a `SubscriberClient` can never
/// poll its OWN attachment this way. Pass a plain command client instead — a
/// command-pool `Client` works, and so does a second, unsubscribed
/// connection built solely for this check.
///
/// `last_seen` is stamped with every count this function reads (including
/// ones below `want`), so a caller whose OUTER timeout cancels this loop
/// mid-poll can still report the last count it actually observed — this
/// function only ever returns on success, so that's the one channel a
/// canceling caller has to that history.
///
/// Only ever resolves `true`; the caller's timeout supplies the `false`.
/// Never panics.
async fn poll_numsub_at_least<C: PubsubInterface>(
    client: &C,
    channel: &str,
    want: i64,
    last_seen: &AtomicI64,
) -> bool {
    loop {
        let counts: std::collections::HashMap<String, i64> =
            client.pubsub_numsub(channel).await.unwrap_or_default();
        let count = counts.get(channel).copied().unwrap_or(0);
        last_seen.store(count, Ordering::Relaxed);
        if count >= want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Bounded wait for the server to confirm `>= want` subscribers on `channel`
/// (via [`poll_numsub_at_least`]), panicking with `context` folded into the
/// message if the server never confirms within `bound`. This is the single
/// readiness/recovery gate every call site needing "is the subscription
/// really live server-side" shares: a completed `subscribe()` resolving only
/// means the server acknowledged the command, not that every code path that
/// later consults subscriber state (a PUBLISH's delivery count, another
/// client's own NUMSUB read) has caught up — only the server's own NUMSUB
/// count settles that. Do not re-derive this bound-and-assert shape at a new
/// call site; route it through here instead.
///
/// `client` carries the same constraint as [`poll_numsub_at_least`]: it must
/// not itself be the subscribed connection whose attachment you're checking.
///
/// On timeout the panic states both `want` and the last NUMSUB count this
/// call actually observed, so a future reader of a failed run can tell "saw
/// 0, nothing ever attached" from "saw want-1, one subscriber short" without
/// re-running anything.
async fn require_numsub_at_least<C: PubsubInterface>(
    client: &C,
    channel: &str,
    want: i64,
    bound: Duration,
    context: &str,
) {
    let last_seen = AtomicI64::new(-1);
    let attached = tokio::time::timeout(
        bound,
        poll_numsub_at_least(client, channel, want, &last_seen),
    )
    .await
    .unwrap_or(false);
    let observed = match last_seen.load(Ordering::Relaxed) {
        -1 => "no NUMSUB read completed in time".to_string(),
        n => format!("last saw {n}"),
    };
    assert!(
        attached,
        "server never reported >= {want} subscriber(s) on {channel} within {bound:?} \
         ({observed}) — the pub/sub round trip is broken, not slow ({context})"
    );
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

/// Issue #66: a `node_last` teardown token is computed on a percore worker under the
/// shared registry lock but APPLIED later, in bridge-queue order — so a re-join can
/// land between the two. Applying the stale token blind unsubscribes this node from a
/// channel it still has local subscribers on, leaving it deaf to that channel's
/// cross-node traffic until the reconciler's next tick.
#[tokio::test]
async fn stale_node_last_keeps_a_channel_this_node_still_subscribes() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let prefix = random_prefix();
        let adapter = connect_adapter_with_prefix(&prefix).await;
        let msg_key = Keys::new(&prefix).msg(TEST_APP, "public-room");

        let (leaving, leaving_handle) = fake_handle();
        adapter
            .subscribe(TEST_APP, "public-room", leaving_handle, None)
            .await;
        let (_staying, staying_handle) = fake_handle();
        adapter
            .subscribe(TEST_APP, "public-room", staying_handle, None)
            .await;
        assert!(
            await_tracked(&adapter, &msg_key, Duration::from_secs(2)).await,
            "precondition: the node must be subscribed to the msg key after the 0→1 edge"
        );

        // The token a worker computed while `leaving` was this node's last subscriber,
        // applied after the re-join `staying` stands for.
        adapter
            .cluster_unsubscribe(TEST_APP, "public-room", &leaving, true)
            .await;

        assert!(
            tracked_contains(&adapter, &msg_key),
            "a stale node_last must not unsubscribe a channel with live local subscribers"
        );
    })
    .await
    .expect("stale node_last test must not hang (Redis up?)");
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

/// Issue #49: the occ hash's whole-key TTL is a BACKSTOP, not a second deadline. It
/// must OUTLIVE the per-member `expireAt` stamps it carries — a hash that lapsed at
/// its own members' deadline would take a crashed node's tokens with it at the very
/// instant they went stale, leaving the sweeper nothing to resolve to the presence
/// user it owes a `member_removed`. Both writers of that TTL (the subscribe script and
/// the heartbeat's re-arm) must honour it, so the remaining TTL is checked after each.
#[tokio::test]
async fn occ_key_ttl_outlives_its_member_stamps() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let prefix = random_prefix();
        let stamp_ttl_secs = 2;
        let adapter = connect_adapter_with_prefix_ttl(&prefix, stamp_ttl_secs, 1).await;
        let clients = RedisClients::connect(&test_redis_url(), 2)
            .await
            .expect("fred clients must connect to the test Redis");
        let occ = Keys::new(&prefix).occ(TEST_APP, "public-room");

        let (_sock, handle) = fake_handle();
        adapter
            .subscribe(TEST_APP, "public-room", handle, None)
            .await;

        let after_subscribe: i64 = clients.pool.next().ttl(&occ).await.expect("ttl occ");
        assert!(
            after_subscribe > stamp_ttl_secs as i64,
            "the subscribe script must arm a backstop longer than the {stamp_ttl_secs}s stamp horizon (got {after_subscribe}s)"
        );

        // Past the stamp horizon: the heartbeat has re-armed the key at least twice,
        // and its re-arm must use the same backstop rather than the stamp horizon.
        tokio::time::sleep(Duration::from_millis(2500)).await;
        let after_heartbeat: i64 = clients.pool.next().ttl(&occ).await.expect("ttl occ");
        assert!(
            after_heartbeat > stamp_ttl_secs as i64,
            "the heartbeat re-arm must keep the backstop longer than the {stamp_ttl_secs}s stamp horizon (got {after_heartbeat}s)"
        );

        let _ = clients.pool.quit().await;
    })
    .await
    .expect("occ TTL backstop test must not hang (Redis up?)");
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
        // F-5: the join carries the PRE-ENCODED `subscription_succeeded` frame
        // (the Redis overwrite path encoded the cluster-truth roster through
        // `wire::encode`), so the assertions decode the frame's double-encoded
        // `data` string.
        let (_s3, h3, m3) = presence_handle("u3", serde_json::json!({"name":"Cleo"}));
        let out3 = adapter_a
            .subscribe(TEST_APP, "presence-room", h3, Some(m3))
            .await;
        let frame = out3
            .presence
            .expect("presence join for u3 must be Some")
            .roster_frame;
        let j: serde_json::Value = serde_json::from_str(&frame).expect("frame must be JSON");
        assert_eq!(j["event"], "pusher_internal:subscription_succeeded");
        assert_eq!(j["channel"], "presence-room");
        let roster: serde_json::Value =
            serde_json::from_str(j["data"].as_str().expect("data is a JSON string"))
                .expect("roster data must be JSON");
        let presence = &roster["presence"];

        assert_eq!(
            presence["count"], 3,
            "cluster roster must count all 3 users"
        );
        assert_eq!(
            presence["ids"],
            serde_json::json!(["u1", "u2", "u3"]),
            "cluster roster ids must be sorted and contain u1,u2,u3"
        );
        assert_eq!(
            presence["hash"]["u1"],
            serde_json::json!({"name":"Ann"}),
            "roster hash must carry u1's user_info"
        );
        assert_eq!(
            presence["hash"]["u2"],
            serde_json::json!({"name":"Bob"}),
            "roster hash must carry u2's user_info"
        );
        assert_eq!(
            presence["hash"]["u3"],
            serde_json::json!({"name":"Cleo"}),
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

/// Issue #79: the CLUSTER roster follows the user's oldest LIVE connection, the way
/// the node-local one has since #63. u1 opens a connection on A presenting one
/// `user_info` and a second on B presenting another; when A's — the connection that
/// seeded the roster — leaves, everything reading cluster truth must report B's
/// value, not the departed one.
///
/// (RED before this: `presinfo` was written once, on the cluster 0→1 user edge, and
/// nothing ever re-derived it — so a clustered deployment kept serving "Old" while a
/// single-node one served "New", the divergence #79 exists to close.)
#[tokio::test]
async fn cross_node_presence_roster_reseats_when_the_seeding_connection_leaves() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let channel = "presence-reseat";
        let adapter_a = connect_adapter_with_prefix(&prefix).await;
        let adapter_b = connect_adapter_with_prefix(&prefix).await;

        let (s_a, h_a, m_a) = presence_handle("u1", serde_json::json!({"name":"Old"}));
        adapter_a.subscribe(TEST_APP, channel, h_a, Some(m_a)).await;
        let (_s_b, h_b, m_b) = presence_handle("u1", serde_json::json!({"name":"New"}));
        adapter_b.subscribe(TEST_APP, channel, h_b, Some(m_b)).await;

        let seeded = adapter_b.presence_members(TEST_APP, channel).await;
        assert_eq!(
            seeded.first().map(|m| &m.user_info),
            Some(&serde_json::json!({"name":"Old"})),
            "the second connection must not displace the first's user_info"
        );

        let un_a = adapter_a.unsubscribe(TEST_APP, channel, &s_a).await;
        assert!(
            !un_a
                .presence
                .expect("presence leave on A must be Some")
                .last_for_user,
            "u1 still has a connection on B → NOT last_for_user"
        );

        let after = adapter_b.presence_members(TEST_APP, channel).await;
        assert_eq!(
            after.first().map(|m| &m.user_info),
            Some(&serde_json::json!({"name":"New"})),
            "the cluster roster must carry the surviving connection's user_info"
        );

        // The same value has to reach a joiner's `subscription_succeeded` roster —
        // that frame is built from cluster truth, not from the REST view.
        let (_s_c, h_c, m_c) = presence_handle("u2", serde_json::json!({"name":"Cleo"}));
        let out_c = adapter_a.subscribe(TEST_APP, channel, h_c, Some(m_c)).await;
        let frame = out_c
            .presence
            .expect("presence join for u2 must be Some")
            .roster_frame;
        let j: serde_json::Value = serde_json::from_str(&frame).expect("frame must be JSON");
        let roster: serde_json::Value =
            serde_json::from_str(j["data"].as_str().expect("data is a JSON string"))
                .expect("roster data must be JSON");
        assert_eq!(
            roster["presence"]["hash"]["u1"],
            serde_json::json!({"name":"New"}),
            "a new subscriber's roster must carry the re-seated user_info"
        );
    })
    .await
    .expect("presence re-seat test must not hang (Redis up?)");
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

/// Issue #66: a node whose own membership stamps went stale keeps holding the sweep
/// lease, so the token it reaps can carry its OWN node id. Stamping the compensating
/// `member_removed` with that id makes the sweeper's own receive loop drop the frame
/// as a self-echo — the member then vanishes for every node except the one that
/// reaped it. The sweeper delivers to no local socket itself, so its emission belongs
/// to no publisher and no node may dedup it.
#[tokio::test]
async fn self_reaped_presence_member_reaches_the_reaping_node() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let prefix = random_prefix();
        // A heartbeat far longer than the test: the reconciler re-stamps once at
        // startup and never again, so the stale stamp written below stays stale.
        let adapter = connect_adapter_with_prefix_ttl(&prefix, 60, 60).await;
        let keys = Keys::new(&prefix);
        let msg_key = keys.msg(TEST_APP, "presence-room");

        let (reaped_socket, reaped_handle, reaped_member) =
            presence_handle("u1", serde_json::json!({"name": "Ann"}));
        adapter
            .subscribe(
                TEST_APP,
                "presence-room",
                reaped_handle,
                Some(reaped_member),
            )
            .await;

        let observer_socket = SocketId::generate();
        let (observer_tx, mut observer_rx) = tokio::sync::mpsc::channel(1024);
        adapter
            .subscribe(
                TEST_APP,
                "presence-room",
                ConnectionHandle {
                    socket_id: observer_socket,
                    mailbox: pylon::connection::handle::Mailbox::new(observer_tx, None, None),
                },
                Some(pylon::presence::member::PresenceMember {
                    user_id: "u2".into(),
                    user_info: serde_json::json!({"name": "Bob"}),
                }),
            )
            .await;
        assert!(
            await_tracked(&adapter, &msg_key, Duration::from_secs(2)).await,
            "precondition: the node must be subscribed to the presence channel's msg key"
        );

        // Age u1's OWN stamp past `now` without touching u2's, exactly as a stalled
        // reconciler on this node would.
        let clients = RedisClients::connect(&test_redis_url(), 1)
            .await
            .expect("test clients must connect");
        let stale_token =
            pylon::adapter::redis::keys::member_token(adapter.node_id(), reaped_socket.as_str());
        let _: i64 = clients
            .pool
            .next()
            .hset(
                keys.occ(TEST_APP, "presence-room"),
                vec![(stale_token, "1".to_string())],
            )
            .await
            .expect("HSET stale expireAt must succeed");

        let webhooks = pylon::webhook::WebhookHandle::null();
        let (acquired, reaped, vacated) = adapter.sweep_now(&webhooks, now_ms()).await;
        assert!(acquired, "precondition: the only node must take the lease");
        assert_eq!(
            reaped, 1,
            "precondition: exactly u1's own token must be reaped"
        );
        assert!(
            vacated.is_empty(),
            "u2 is still fresh, so the channel must not be vacated"
        );

        let frame = tokio::time::timeout(Duration::from_secs(5), observer_rx.recv())
            .await
            .expect("u2 must receive the self-reaped member_removed within 5s")
            .expect("u2's mailbox must yield an event");
        match *frame {
            ServerEvent::Raw(s) => {
                let parsed: serde_json::Value =
                    serde_json::from_str(&s).expect("Raw frame must be valid JSON");
                assert_eq!(parsed["event"], "pusher_internal:member_removed");
                assert_eq!(parsed["channel"], "presence-room");
                assert!(
                    s.contains("u1"),
                    "the removal must name the reaped user: {s}"
                );
            }
            other => panic!("u2 expected a Raw member_removed frame, got {other:?}"),
        }
    })
    .await
    .expect("self-reap delivery test must not hang (Redis up?)");
}

/// A real webhook dispatcher backed by a `RecordingTransport`, so a sweep's
/// `webhooks.enqueue(...)` is batched and signed exactly as in production but captured
/// in memory. `vacated_grace_ms` is 0, so `member_removed` / `channel_vacated` deliver
/// inline in enqueue order rather than through the debounced grace path.
fn recording_webhooks() -> (pylon::webhook::WebhookHandle, RecordingTransport) {
    let apps: Arc<dyn AppManager> = Arc::new(
        StaticFileAppManager::from_json(
            r#"[{"name":"Test","id":"app1","key":"app1-key","secret":"app1-secret",
             "webhooks":[{"url":"http://127.0.0.1:1/pusher/webhooks",
                          "event_types":["member_removed","channel_vacated"]}]}]"#,
        )
        .expect("apps json must parse"),
    );
    let transport = RecordingTransport::new();
    let recorded = transport.clone();
    let handle = pylon::webhook::spawn(
        apps,
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

/// The webhook event names recorded by a `RecordingTransport`, in delivery order,
/// paired with each event's `user_id` when it carries one.
async fn recorded_events(transport: &RecordingTransport) -> Vec<(String, Option<String>)> {
    let mut out = Vec::new();
    for d in transport.recorded().await {
        let v: serde_json::Value =
            serde_json::from_str(&d.body).expect("webhook body must be JSON");
        for e in v["events"].as_array().into_iter().flatten() {
            out.push((
                e["name"].as_str().unwrap_or_default().to_string(),
                e.get("user_id")
                    .and_then(|u| u.as_str())
                    .map(str::to_string),
            ));
        }
    }
    out
}

/// Poll `transport` until it has recorded at least `want` events or the deadline
/// elapses — the dispatcher's batch window is asynchronous, so a bare read races it.
async fn await_recorded(
    transport: &RecordingTransport,
    want: usize,
    timeout: Duration,
) -> Vec<(String, Option<String>)> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let events = recorded_events(transport).await;
        if events.len() >= want || tokio::time::Instant::now() >= deadline {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Issue #49: a node dying while it holds the LAST presence members of a channel. Its
/// `occ` entries go stale AND — with no other live node re-arming the hash — the whole
/// `occ` key eventually lapses with them, taking the member tokens the per-token reap
/// needs. The roster is then reachable only through the un-expiring `chans` index, and
/// the sweep's vacate must drain it: exactly one `member_removed` for u1, BEFORE
/// `channel_vacated`, with the three presence hashes gone. Proving the corruption is
/// really cleared, u1's next join on that channel must again report `first_for_user`.
///
/// The `occ` key's own lapse is applied directly (a raw `DEL` standing in for the
/// whole-key backstop firing) rather than slept through: the backstop is deliberately
/// sized to outlive the member stamps by several sweep intervals, which is exactly the
/// wait this test must not take.
///
/// (RED before the fix: the sweep finds an empty `occ`, reaps nothing, vacates, and
/// leaves `presusers`/`presinfo`/`presmembers` in Redis forever — zero `member_removed`,
/// a `channel_vacated` with no preceding member removal, and a u1 whose every later join
/// is silently suppressed.)
#[tokio::test]
async fn sweeper_drains_presence_roster_of_channel_orphaned_by_dead_node() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let prefix = random_prefix();
        let channel = "presence-lonely";
        let adapter_a = connect_adapter_with_prefix_ttl(&prefix, 2, 1).await;
        let adapter_b = connect_adapter_with_prefix_ttl(&prefix, 2, 1).await;

        // u1 on A is the channel's ONLY member cluster-wide.
        let (_s1, h1, m1) = presence_handle("u1", serde_json::json!({"name": "Ann"}));
        adapter_a.subscribe(TEST_APP, channel, h1, Some(m1)).await;
        let before = adapter_b.presence_members(TEST_APP, channel).await;
        assert_eq!(before.len(), 1, "u1 must be the only cluster roster entry");

        // Crash A (drop aborts its heartbeats), then apply the occ hash's whole-key
        // lapse: no live node holds this channel, so nothing would re-arm it.
        drop(adapter_a);
        let clients = RedisClients::connect(&test_redis_url(), 2)
            .await
            .expect("fred clients must connect to the test Redis");
        let keys = Keys::new(&prefix);
        let _: () = clients
            .pool
            .next()
            .del(keys.occ(TEST_APP, channel))
            .await
            .expect("raw DEL occ must succeed");

        // B sweeps: no tokens to reap, so the vacate is the only thing that can still
        // find u1 — and it must announce the removal before vacating the channel.
        let (webhooks, transport) = recording_webhooks();
        let (acquired, _reaped, vacated) = adapter_b.sweep_now(&webhooks, now_ms()).await;
        assert!(acquired, "B must acquire the sweep lease");
        assert!(
            vacated.contains(&(TEST_APP.to_string(), channel.to_string())),
            "the orphaned presence channel must be vacated: {vacated:?}"
        );

        let events = await_recorded(&transport, 2, Duration::from_secs(3)).await;
        assert_eq!(
            events,
            vec![
                ("member_removed".to_string(), Some("u1".to_string())),
                ("channel_vacated".to_string(), None),
            ],
            "the sweep must fire exactly one member_removed for u1 and then channel_vacated"
        );

        // The roster is clean: no ghost in presinfo, no ghost refcount in presusers.
        let after = adapter_b.presence_members(TEST_APP, channel).await;
        assert!(
            after.is_empty(),
            "the cluster roster must be empty: {after:?}"
        );
        let summary = adapter_b.channel(TEST_APP, channel).await;
        assert_eq!(
            summary.user_count,
            Some(0),
            "the cluster user_count must be 0 (got {:?})",
            summary.user_count
        );

        // The compounding half: u1's next join must be a genuine first_for_user again,
        // not a duplicate suppressed by a leaked refcount.
        let (_s2, h2, m2) = presence_handle("u1", serde_json::json!({"name": "Ann"}));
        let out = adapter_b.subscribe(TEST_APP, channel, h2, Some(m2)).await;
        assert!(
            out.presence.expect("presence join outcome").first_for_user,
            "u1 rejoining a drained channel must report first_for_user"
        );

        let _ = clients.pool.quit().await;
    })
    .await
    .expect("presence-drain sweep test must not hang (Redis up?)");
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

/// F-2 encode-once pin: `RedisAdapter::send_to_user` must encode the frame ONCE
/// and feed the SAME bytes to BOTH halves — the node-local delivery (a `Raw`
/// frame, byte-identical to a fresh `wire::encode` of the event) and the
/// usermsg publish's `event` string — so the encode-once construction can never
/// let the two halves diverge (the same contract `ClusterAdapter::broadcast`'s
/// F17 pin enforces). A caller-supplied pre-encoded `Raw` payload must reach
/// both halves VERBATIM (no re-encode, no mutation).
#[tokio::test]
async fn send_to_user_feeds_the_same_encoded_bytes_to_local_and_publish_halves() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let adapter = connect_adapter_with_prefix(&prefix).await;

        // A local connection of the user on THIS node (the local half's target).
        let (_sid, handle, mut rx) = recording_handle();
        adapter.signin_user(TEST_APP, "u-half", handle).await;

        // A raw probe subscriber sniffs the published envelope bytes. A
        // completed `subscribe()` only means the server acknowledged the
        // SUBSCRIBE command — NOT that the server's own subscriber-count
        // bookkeeping (what the very next PUBLISH's delivery depends on) has
        // caught up. That false "subscribe cannot race the publish" assumption
        // is exactly the issue #23 flake class (see `poll_numsub_at_least` /
        // `require_numsub_at_least` above, shared with `smoke_connectivity`
        // and the envelope-compat test): gate on the observable state (PUBSUB
        // NUMSUB) before trusting the probe to see the first publish below.
        let probe = fred::prelude::Builder::from_config(
            fred::prelude::Config::from_url(test_redis_url().as_str()).unwrap(),
        )
        .build_subscriber_client()
        .unwrap();
        probe.init().await.expect("probe must connect");
        let mut probe_rx = probe.message_rx();
        let usermsg = keys.usermsg(TEST_APP, "u-half");
        probe
            .subscribe(usermsg.clone())
            .await
            .expect("probe SUBSCRIBE");
        // A plain command client for the NUMSUB readiness gate — NOT `probe`
        // itself: RESP2 forbids ordinary commands (PUBSUB included) on a
        // connection that has active subscriptions, so the gate needs its own
        // connection to ask "is `probe` attached?" from the outside.
        let numsub = fred::prelude::Builder::from_config(
            fred::prelude::Config::from_url(test_redis_url().as_str()).unwrap(),
        )
        .build()
        .unwrap();
        numsub.init().await.expect("numsub client must connect");
        // want=2, not 1: `adapter.signin_user` above already SUBSCRIBEd its OWN
        // `SubscriberClient` to this exact `usermsg` channel (the node-local
        // 0->1 edge — see redis/mod.rs), so NUMSUB is never 0 here even before
        // the probe catches up. Gating on >=1 would pass on the adapter's own
        // subscriber alone and let the publish below race the probe's — the
        // same flake this whole gate exists to close, just one subscriber
        // short of catching it. Only >=2 (adapter + probe) is the observable
        // fact that the probe itself is attached.
        require_numsub_at_least(
            &numsub,
            &usermsg,
            2,
            Duration::from_secs(2),
            "probe readiness before the first send_to_user publish",
        )
        .await;

        // Typed event → ONE encode shared by both halves.
        let event = ServerEvent::ChannelEvent {
            channel: "x".into(),
            event: "e".into(),
            data: serde_json::json!({"k":1}),
            user_id: None,
        };
        adapter
            .send_to_user(TEST_APP, "u-half", event.clone())
            .await;

        let expected =
            pylon::protocol::wire::encode(pylon::protocol::wire::ACTIVE_VERSIONS[0], &event);
        let local = with_timeout(async { rx.recv().await }).await.map(|b| *b);
        match local {
            Some(ServerEvent::Raw(f)) => assert_eq!(
                &*f, &expected,
                "local half must get the shared frame (byte-identical to a fresh encode)"
            ),
            other => panic!("expected Raw frame locally, got {other:?}"),
        }
        let wire = next_probe_json(&mut probe_rx).await;
        assert_eq!(
            wire["kind"], "UserSend",
            "the probed envelope is the user send"
        );
        assert_eq!(
            wire["event"].as_str(),
            Some(expected.as_str()),
            "publish half must relay the SAME bytes the local half delivered, got: {wire}"
        );

        // `Raw` passthrough: a caller-supplied pre-encoded payload reaches both
        // halves VERBATIM.
        let raw: Arc<str> = Arc::from("{\"event\":\"pre\"}");
        adapter
            .send_to_user(TEST_APP, "u-half", ServerEvent::Raw(raw.clone()))
            .await;
        let local = with_timeout(async { rx.recv().await }).await.map(|b| *b);
        match local {
            Some(ServerEvent::Raw(f)) => assert_eq!(&*f, &*raw, "Raw payload verbatim locally"),
            other => panic!("expected the verbatim Raw frame locally, got {other:?}"),
        }
        let wire = next_probe_json(&mut probe_rx).await;
        assert_eq!(
            wire["event"].as_str(),
            Some(&*raw),
            "publish half must relay the Raw payload verbatim, got: {wire}"
        );

        let _ = probe.quit().await;
        let _ = numsub.quit().await;
    })
    .await
    .expect("send_to_user encode-once pin must not hang (Redis up?)");
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

/// The cluster presence cap is decided INSIDE `PRESENCE_JOIN_LUA`, so it holds across
/// nodes admitting at the same instant. The roster is pre-filled to `CAP - 1`, then eight
/// distinct new users race the last slot — half through node A, half through node B, all
/// in flight together, which is exactly the interleaving a probe-then-commit gate loses:
/// every racer reads room and every racer commits, overshooting by up to one member per
/// node. Exactly ONE may be admitted and `HLEN presusers` must land exactly on `CAP`.
///
/// A second connection for a user ALREADY on the roster is not a new distinct user and
/// stays admitted with the channel full.
#[tokio::test]
async fn cluster_presence_cap_is_atomic_across_nodes() {
    const CAP: usize = 4;
    const RACERS: usize = 8;

    tokio::time::timeout(Duration::from_secs(10), async {
        let prefix = random_prefix();
        let adapter_a = connect_adapter_with_prefix(&prefix).await;
        let adapter_b = connect_adapter_with_prefix(&prefix).await;
        let keys = Keys::new(&prefix);

        // Fill the roster to CAP - 1 across BOTH nodes, so the contested slot is the last.
        for i in 0..CAP - 1 {
            let node = if i % 2 == 0 { &adapter_a } else { &adapter_b };
            let (sid, _h, m) = presence_handle(&format!("seed{i}"), serde_json::json!({"i": i}));
            let admitted = node
                .cluster_presence_join(TEST_APP, "presence-cap", &m, &sid, Some(CAP))
                .await
                .expect("seeding join must reach Redis");
            assert!(
                admitted.is_some(),
                "seed{i} is below the cap and must be admitted"
            );
        }

        // Race the final slot: every racer's EVALSHA is in flight before any completes.
        let racers: Vec<_> = (0..RACERS)
            .map(|i| {
                let (sid, _h, m) =
                    presence_handle(&format!("race{i}"), serde_json::json!({"i": i}));
                (if i % 2 == 0 { &adapter_a } else { &adapter_b }, sid, m)
            })
            .collect();
        let verdicts = futures_util::future::join_all(racers.iter().map(|(node, sid, m)| {
            node.cluster_presence_join(TEST_APP, "presence-cap", m, sid, Some(CAP))
        }))
        .await;

        let admitted = verdicts
            .iter()
            .filter(|v| v.as_ref().expect("every racer must reach Redis").is_some())
            .count();
        assert_eq!(
            admitted, 1,
            "exactly one of {RACERS} concurrent racers may take the last slot"
        );

        let clients = RedisClients::connect(&test_redis_url(), 1)
            .await
            .expect("fred clients must connect to the test Redis");
        let occupants: i64 = clients
            .pool
            .next()
            .hlen(keys.presusers(TEST_APP, "presence-cap"))
            .await
            .expect("HLEN presusers must succeed");
        assert_eq!(
            occupants as usize, CAP,
            "the cluster roster must land exactly on the cap, never above it"
        );

        // A second connection for a user already on the FULL roster is still admitted:
        // it adds no distinct user.
        let (_sid0, _h0, m0) = presence_handle("seed0", serde_json::json!({"i": 0}));
        let second_sid = SocketId::generate();
        let rejoin = adapter_b
            .cluster_presence_join(TEST_APP, "presence-cap", &m0, &second_sid, Some(CAP))
            .await
            .expect("the rejoin must reach Redis")
            .expect("an existing roster member must be admitted at the cap");
        assert!(
            !rejoin.0,
            "a second connection of an existing user is not first_for_user"
        );
    })
    .await
    .expect("cluster presence cap race must not hang (Redis up?)");
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
        let frame = pylon::protocol::wire::encode(
            pylon::protocol::wire::ACTIVE_VERSIONS[0],
            &ServerEvent::Raw(std::sync::Arc::from("ping")),
        );
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

/// F-1 (`PYLON_CLUSTER_ENVELOPE_COMPAT`): the knob's end-to-end behavior, sniffed
/// on the actual Redis bus. The cluster suites boot adapters from struct-literal
/// configs (they never read `PYLON_*` env), so the compat=false SERVER path is
/// covered HERE: the test exports `PYLON_CLUSTER_ENVELOPE_COMPAT=0`, boots the
/// publisher via `ServerConfig::from_env()` (proving the env plumbing), and a raw
/// probe subscriber on the `msg`/`usermsg` channels pins the PUBLISHED envelope
/// bytes — no `event` member, `frame_b64` the sole carrier — while a default
/// (compat=on) publisher keeps the double-carry shape. Cross-node relay of the
/// frame_b64-only envelope is proven end-to-end by B (a DEFAULT compat receiver —
/// receivers are knob-independent) delivering A's compat=off frames locally.
#[tokio::test]
async fn cluster_envelope_compat_knob_shapes_the_wire_and_relay_still_works() {
    tokio::time::timeout(Duration::from_secs(10), async {
        // ── Part 1: DEFAULT (compat on) — the bus carries BOTH fields. ─────────
        let prefix_on = random_prefix();
        let adapter_on = connect_adapter_with_prefix(&prefix_on).await;
        let msg_on = Keys::new(&prefix_on).msg(TEST_APP, "compat-room");

        // A raw probe subscriber sniffs the published envelope bytes.
        let probe = fred::prelude::Builder::from_config(
            fred::prelude::Config::from_url(test_redis_url().as_str()).unwrap(),
        )
        .build_subscriber_client()
        .unwrap();
        probe.init().await.expect("probe must connect");
        let mut probe_rx = probe.message_rx();
        // A plain command client for the NUMSUB readiness gate below — NOT
        // `probe` itself: RESP2 forbids ordinary commands (PUBSUB included)
        // on a connection that has active subscriptions, so the gate needs
        // its own connection to ask "is `probe` attached?" from the outside.
        let numsub = fred::prelude::Builder::from_config(
            fred::prelude::Config::from_url(test_redis_url().as_str()).unwrap(),
        )
        .build()
        .unwrap();
        numsub.init().await.expect("numsub client must connect");
        probe
            .subscribe(msg_on.clone())
            .await
            .expect("probe SUBSCRIBE");
        // A completed `subscribe()` only means the server acknowledged the
        // SUBSCRIBE command — not that the server's own subscriber-count
        // bookkeeping (what the very next broadcast's delivery depends on)
        // has caught up. That's the same single-copy-loss class
        // `smoke_connectivity` hits from the PUBLISH side of the round trip;
        // gate on the observable state (PUBSUB NUMSUB) before trusting the
        // probe to see the first broadcast.
        require_numsub_at_least(
            &numsub,
            &msg_on,
            1,
            Duration::from_secs(2),
            "probe readiness before the compat=on broadcast",
        )
        .await;

        adapter_on
            .broadcast(
                TEST_APP,
                "compat-room",
                ServerEvent::Raw(std::sync::Arc::from("ping")),
                None,
            )
            .await;
        let wire = next_probe_json(&mut probe_rx).await;
        assert!(
            wire.get("event").is_some() && wire.get("frame_b64").is_some(),
            "compat=on must double-carry on the wire, got: {wire}"
        );

        // ── Part 2: compat OFF via the exported env var. ───────────────────────
        // The guard restores the var's PRIOR value on scope exit — even when a
        // later assertion panics — so a leaked `=0` can never re-knob the rest
        // of this test binary (the lib suite's ENV_LOCK discipline, scoped to
        // the one variable this test owns).
        let _env_guard = EnvCompatGuard::capture();
        std::env::set_var("PYLON_CLUSTER_ENVELOPE_COMPAT", "0");
        let prefix_off = random_prefix();
        // Boot the publisher from the ENVIRONMENT (the env knob's whole point) —
        // then stamp the test Redis wiring on top, mirroring `redis_test_config`.
        let mut cfg = ServerConfig::from_env();
        assert!(
            !cfg.cluster_envelope_compat,
            "PYLON_CLUSTER_ENVELOPE_COMPAT=0 must reach ServerConfig"
        );
        cfg.adapter = "redis".into();
        cfg.redis_url = test_redis_url();
        cfg.redis_prefix = prefix_off.clone();
        let adapter_a = RedisAdapter::new(&cfg)
            .await
            .expect("compat=off adapter must connect");
        // B is a DEFAULT-config receiver: receivers decode both envelope shapes
        // regardless of their own knob — the mixed-fleet guarantee.
        let adapter_b = connect_adapter_with_prefix(&prefix_off).await;
        let keys_off = Keys::new(&prefix_off);
        let msg_off = keys_off.msg(TEST_APP, "compat-room");
        let usermsg_off = keys_off.usermsg(TEST_APP, "u1");

        // B subscribes locally (broadcast leg) and signs in u1 (user-send leg) so
        // its receive loop re-delivers A's compat=off envelopes.
        let (sock_b, handle_b, mut rx_b) = recording_handle();
        adapter_b
            .subscribe(TEST_APP, "compat-room", handle_b, None)
            .await;
        let (user_sock, user_handle, mut user_rx) = recording_handle();
        adapter_b.signin_user(TEST_APP, "u1", user_handle).await;

        probe
            .subscribe(msg_off.clone())
            .await
            .expect("probe SUBSCRIBE");
        probe
            .subscribe(usermsg_off.clone())
            .await
            .expect("probe SUBSCRIBE");
        // want=2, not 1: `adapter_b.subscribe` above already SUBSCRIBEd its OWN
        // `SubscriberClient` to this exact `msg_off` channel (the node-local
        // 0->1 edge — see the B1 lifecycle test), so NUMSUB is never 0 here
        // even before the probe catches up. Gating on >=1 would pass on
        // adapter_b's own subscriber alone and let the broadcast below race
        // the probe's SUBSCRIBE — the same flake this gate exists to close,
        // just one subscriber short of catching it. Only >=2 (adapter_b +
        // probe) is the observable fact that the probe itself is attached.
        require_numsub_at_least(
            &numsub,
            &msg_off,
            2,
            Duration::from_secs(2),
            "probe readiness before the compat=off broadcast",
        )
        .await;

        // A (compat=off) broadcasts: the bus envelope must omit `event`.
        adapter_a
            .broadcast(
                TEST_APP,
                "compat-room",
                ServerEvent::Raw(std::sync::Arc::from("ping")),
                None,
            )
            .await;
        let wire = next_probe_json(&mut probe_rx).await;
        assert_eq!(wire.get("kind").and_then(|v| v.as_str()), Some("Broadcast"));
        assert!(
            wire.get("event").is_none(),
            "compat=off must omit the legacy event member on the wire, got: {wire}"
        );
        assert!(
            wire.get("frame_b64").is_some(),
            "compat=off still carries frame_b64, got: {wire}"
        );
        // And B (default receiver) relays the frame_b64-only envelope locally.
        // (`Raw` frames pass through the relay verbatim — see `wire::encode`.)
        let received = *tokio::time::timeout(Duration::from_secs(2), rx_b.recv())
            .await
            .expect("B must relay A's compat=off broadcast")
            .expect("B's mailbox must yield the broadcast");
        match received {
            ServerEvent::Raw(f) => assert_eq!(&*f, "ping"),
            other => panic!("expected a Raw frame, got {other:?}"),
        }

        // A (compat=off) sends to u1: the UserSend envelope also omits `event`,
        // and B still delivers the frame to its local u1 connection.
        //
        // want=2, not 1: `adapter_b.signin_user` above already SUBSCRIBEd its
        // OWN `SubscriberClient` to this exact `usermsg_off` channel (the same
        // node-local 0->1 edge `send_to_user_feeds_the_same_encoded_bytes_...`
        // relies on), so NUMSUB is never 0 here even before the probe catches
        // up. Gating on >=1 would pass on adapter_b's own subscriber alone and
        // prove only "someone is subscribed" — already true before this gate
        // ever ran — letting the send below race the probe's SUBSCRIBE. Only
        // >=2 (adapter_b + probe) is the observable fact that the probe itself
        // is attached.
        require_numsub_at_least(
            &numsub,
            &usermsg_off,
            2,
            Duration::from_secs(2),
            "probe readiness before the compat=off user send",
        )
        .await;
        adapter_a
            .send_to_user(
                TEST_APP,
                "u1",
                ServerEvent::Raw(std::sync::Arc::from("pong")),
            )
            .await;
        let wire = next_probe_json(&mut probe_rx).await;
        assert_eq!(wire.get("kind").and_then(|v| v.as_str()), Some("UserSend"));
        assert!(
            wire.get("event").is_none(),
            "compat=off UserSend must omit event on the wire, got: {wire}"
        );
        assert!(wire.get("frame_b64").is_some());
        let received = *tokio::time::timeout(Duration::from_secs(2), user_rx.recv())
            .await
            .expect("B must relay A's compat=off user send")
            .expect("u1's mailbox must yield the frame");
        match received {
            ServerEvent::Raw(f) => assert_eq!(&*f, "pong"),
            other => panic!("expected a Raw frame, got {other:?}"),
        }

        // Cleanup: drop the memberships. The env override is restored by
        // `_env_guard`'s Drop (which also runs on a panic unwind).
        adapter_b
            .unsubscribe(TEST_APP, "compat-room", &sock_b)
            .await;
        adapter_b.signout_user(TEST_APP, "u1", &user_sock).await;
        let _ = probe.quit().await;
        let _ = numsub.quit().await;
    })
    .await
    .expect("compat knob test must not hang (Redis up?)");
}

/// Restores `PYLON_CLUSTER_ENVELOPE_COMPAT` to its pre-test value when dropped
/// (removed when it was unset) — panic-safe env hygiene for the knob test: a
/// leaked `=0` would silently re-knob any later env-reading code in this
/// binary. Mirrors the lib suite's `ENV_LOCK` discipline, scoped to the one
/// variable that test owns (the binary's tests run `--test-threads=1`).
struct EnvCompatGuard(Option<String>);

impl EnvCompatGuard {
    fn capture() -> Self {
        Self(std::env::var("PYLON_CLUSTER_ENVELOPE_COMPAT").ok())
    }
}

impl Drop for EnvCompatGuard {
    fn drop(&mut self) {
        match self.0.take() {
            Some(prior) => std::env::set_var("PYLON_CLUSTER_ENVELOPE_COMPAT", prior),
            None => std::env::remove_var("PYLON_CLUSTER_ENVELOPE_COMPAT"),
        }
    }
}

/// Read the probe's next pub/sub message as JSON (fails loud on stall/close).
async fn next_probe_json(
    rx: &mut tokio::sync::broadcast::Receiver<fred::types::Message>,
) -> serde_json::Value {
    match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
        Ok(Ok(msg)) => {
            let payload = msg
                .value
                .into_string()
                .expect("envelope payload must be a string");
            serde_json::from_str(&payload).expect("envelope must be JSON")
        }
        Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
            panic!("probe stream lagged by {n} envelopes while awaiting one")
        }
        Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
            panic!("probe stream closed while awaiting an envelope")
        }
        Err(_) => panic!("timed out awaiting an envelope on the probe"),
    }
}

/// `purge_app` on a `RedisAdapter` closes all local connections with 4009 and
/// removes the app from the Redis `apps` set so the sweeper stops enumerating it.
#[tokio::test]
async fn purge_app_closes_connections_and_removes_from_redis_apps_set() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let prefix = random_prefix();
        let cfg = redis_test_config(&prefix);
        let keys = Keys::new(&prefix);

        // Build the RedisAdapter over a LocalAdapter whose AppRegistry we control, so
        // we can register a live connection (as the percore worker does at establish)
        // and prove purge_app force-closes it with 4009 — the close half — in addition
        // to the Redis SREM half.
        let app_registry = Arc::new(pylon::adapter::app_registry::AppRegistry::new());
        let local = Arc::new(pylon::adapter::local::LocalAdapter::new(
            Arc::new(pylon::channel::registry::Registry::new()),
            app_registry.clone(),
        ));
        // No worker fleet backs this standalone adapter → `None` conn_counts (the
        // heartbeat's outage re-seed has nothing to re-seed, which is correct).
        let adapter = RedisAdapter::with_local(&cfg, local, None, None)
            .await
            .expect("with_local must connect to the test Redis");

        // (1) A live connection registered in the app's AppRegistry (mirrors the
        //     worker's establish-time insert) — purge_app must force-close THIS.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Box<ServerEvent>>(64);
        let sock = SocketId::generate();
        app_registry.insert(
            TEST_APP,
            ConnectionHandle {
                socket_id: sock,
                mailbox: pylon::connection::handle::Mailbox::new(tx, None, None),
            },
        );

        // (2) Index the app in the Redis `apps` set the way `cluster_subscribe` does
        //     (a separate handle), so the SREM half has something to remove.
        let (tx2, _rx2) = tokio::sync::mpsc::channel::<Box<ServerEvent>>(64);
        let sub_handle = ConnectionHandle {
            socket_id: SocketId::generate(),
            mailbox: pylon::connection::handle::Mailbox::new(tx2, None, None),
        };
        adapter
            .subscribe(TEST_APP, "public-room", sub_handle, None)
            .await;

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

        // Purge: close every local connection (4009) AND SREM the app index.
        let ids = adapter.purge_app(TEST_APP).await;

        // CLOSE HALF: the registered connection is returned and force-closed 4009
        // (Error frame then Close frame), exactly like terminate_user.
        assert_eq!(
            ids,
            vec![sock],
            "purge_app must return the app's registered connection"
        );
        assert!(
            matches!(rx.try_recv().map(|b| *b), Ok(ServerEvent::Error(e)) if e.code == 4009),
            "registered connection must receive Error(4009)"
        );
        assert!(
            matches!(
                rx.try_recv().map(|b| *b),
                Ok(ServerEvent::Close { code: 4009, .. })
            ),
            "registered connection must then receive Close(4009)"
        );
        // The app's AppRegistry entry is fully drained.
        assert!(app_registry.connected_app_ids().is_empty());

        // SREM HALF: the app is removed from the Redis `apps` set.
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

// ---------------------------------------------------------------------------
// Vacate CAS (duplicate channel_vacated regression).
//
// The `channel_vacated` emission right belongs to whichever caller's atomic
// operation actually removed the channel from the `chans` index (its SREM
// returned 1). Two writers can reach the vacate decision concurrently — the
// last-unsubscribe (UNSUBSCRIBE_LUA, driven by the bridge) and the sweeper's
// orphan reclaim (VACATE_LUA) — and before the CAS they could BOTH fire the
// webhook for one vacancy (observed as a flaky double `channel_vacated` in
// `cluster_subscribe::cross_node_vacated_single_emit`). These tests drive the
// two scripts DIRECTLY, in BOTH orderings of the race, and assert exactly one
// winner each — deterministic where the full-system straddle is not.
// ---------------------------------------------------------------------------

/// Run the sweeper's VACATE_LUA for `channel`; returns `(won, drained_user_ids)`
/// (`won == 1` iff THIS call's SREM removed the channel from the `chans` index, in
/// which case the ids are the presence roster it drained).
async fn run_vacate(
    scripts: &Scripts,
    pool: &fred::clients::Pool,
    keys: &Keys,
    channel: &str,
) -> (i64, Vec<String>) {
    scripts
        .vacate
        .evalsha_with_reload::<(i64, Vec<String>), _, _>(
            pool.next(),
            vec![
                keys.occ(TEST_APP, channel),
                keys.chans(TEST_APP),
                keys.presusers(TEST_APP, channel),
                keys.presinfo(TEST_APP, channel),
                keys.presmembers(TEST_APP, channel),
                keys.presseats(TEST_APP, channel),
            ],
            vec![channel.to_string()],
        )
        .await
        .expect("VACATE_LUA must eval")
}

/// Run the bridge's UNSUBSCRIBE_LUA for `token`; returns `(remaining, won)`
/// (`won` iff THIS call removed the channel from the `chans` index).
async fn run_unsubscribe(
    scripts: &Scripts,
    pool: &fred::clients::Pool,
    keys: &Keys,
    channel: &str,
    token: &str,
) -> (i64, i64) {
    scripts
        .unsubscribe
        .evalsha_with_reload::<(i64, i64), _, _>(
            pool.next(),
            vec![keys.occ(TEST_APP, channel), keys.chans(TEST_APP)],
            vec![token.to_string(), channel.to_string()],
        )
        .await
        .expect("UNSUBSCRIBE_LUA must eval")
}

/// Ordering 1 — the last-unsubscribe wins, the sweeper's later orphan reclaim
/// must stay silent. Two members in `occ`, the channel indexed in `chans`; the
/// last unsubscribe empties `occ` and its SREM removes the `chans` entry
/// (`won == 1`); the sweeper's VACATE_LUA then finds the entry already gone
/// (`won == 0`) — exactly one emission right in total.
#[tokio::test]
async fn vacate_cas_unsubscribe_first_sweep_silent() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let scripts = Scripts::new();
        let clients = RedisClients::connect(&test_redis_url(), 2)
            .await
            .expect("fred clients must connect to the test Redis");
        let channel = format!("cas-room-{}", Uuid::new_v4());

        // Seed the state two subscribed members produce: occ with 2 tokens,
        // the channel indexed in chans (SUBSCRIBE_LUA SADDs on the 0→1 edge).
        let occ = keys.occ(TEST_APP, &channel);
        let chans = keys.chans(TEST_APP);
        let pool = clients.pool.next();
        let _: () = pool.hset(occ.clone(), ("n1:s1", 1)).await.expect("hset");
        let _: () = pool.hset(occ.clone(), ("n2:s2", 1)).await.expect("hset");
        let _: () = pool
            .sadd(chans.clone(), channel.clone())
            .await
            .expect("sadd");

        // Not-last unsubscribe: one member remains → no vacate branch at all.
        let (count, won) = run_unsubscribe(&scripts, &clients.pool, &keys, &channel, "n1:s1").await;
        assert_eq!(
            (count, won),
            (1, 0),
            "non-last unsubscribe: count 1, no emission right"
        );

        // LAST unsubscribe: empties occ, SREM removes the chans entry → WINS.
        let (count, won) = run_unsubscribe(&scripts, &clients.pool, &keys, &channel, "n2:s2").await;
        assert_eq!(
            (count, won),
            (0, 1),
            "last unsubscribe must WIN the vacate emission right"
        );

        // The sweeper's later orphan reclaim finds the chans entry gone → silent.
        let (won, drained) = run_vacate(&scripts, &clients.pool, &keys, &channel).await;
        assert_eq!(
            won, 0,
            "the sweeper must NOT win after the unsubscribe already vacated"
        );
        assert!(
            drained.is_empty(),
            "a losing vacate must drain nothing: {drained:?}"
        );

        // And the Redis state is fully reclaimed either way.
        let remaining: i64 = clients.pool.next().hlen(&occ).await.expect("hlen");
        assert_eq!(remaining, 0, "occ must be gone");
        let member: bool = clients
            .pool
            .next()
            .sismember(chans.clone(), channel.clone())
            .await
            .expect("sismember");
        assert!(!member, "chans must not list the vacated channel");
    })
    .await
    .expect("vacate CAS ordering-1 test must not hang (Redis up?)");
}

/// Ordering 2 — the sweeper wins the straddle, the later last-unsubscribe must
/// stay silent. The sweeper observes the post-unsubscribe straddle state
/// (`chans` still lists the channel, `occ` already empty/gone) and its VACATE_LUA
/// SREM removes the entry (`won == 1`); the bridge's late UNSUBSCRIBE_LUA —
/// whose HDEL no-ops on the gone hash — finds `SREM == 0` and reports
/// `won == 0`, so the bridge suppresses its `channel_vacated`. Exactly one
/// emission right in total, in the ordering that used to double-fire.
#[tokio::test]
async fn vacate_cas_sweep_first_unsubscribe_silent() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let scripts = Scripts::new();
        let clients = RedisClients::connect(&test_redis_url(), 2)
            .await
            .expect("fred clients must connect to the test Redis");
        let channel = format!("cas-room-{}", Uuid::new_v4());

        // Seed the STRADDLE state: the channel still indexed in chans, but occ
        // already emptied by the (atomic) unsubscribe — exactly what the
        // sweeper's non-atomic SMEMBERS→HGETALL view can observe mid-vacate.
        let chans = keys.chans(TEST_APP);
        let _: () = clients
            .pool
            .next()
            .sadd(chans.clone(), channel.clone())
            .await
            .expect("sadd");

        // The sweeper's atomic vacate: occ empty/gone → DEL (no-op) → SREM
        // removes the entry → the sweeper WINS the emission right.
        let (won, _drained) = run_vacate(&scripts, &clients.pool, &keys, &channel).await;
        assert_eq!(
            won, 1,
            "the sweeper must WIN the vacate emission right in the straddle"
        );

        // The bridge's late last-unsubscribe: HDEL no-ops, HLEN 0, but its SREM
        // removes nothing → won == 0 → the bridge stays silent.
        let (count, won) = run_unsubscribe(&scripts, &clients.pool, &keys, &channel, "n1:s1").await;
        assert_eq!(
            (count, won),
            (0, 0),
            "a late unsubscribe must NOT win after the sweeper already vacated"
        );
    })
    .await
    .expect("vacate CAS ordering-2 test must not hang (Redis up?)");
}

/// Issue #49 — the winning vacate DRAINS the presence side-tables. Seed exactly what
/// a node death leaves behind once the `occ` hash lapses with the whole-key backstop
/// that carried its member tokens: `chans` still indexes the channel, `occ` is gone,
/// and `presusers` / `presinfo` / `presmembers` still hold u1. `chans` is the only
/// structure with no TTL, so this vacate is the last moment anything can reach that
/// roster — it must hand u1 back (one owed `member_removed`) and leave the three
/// hashes empty. The racing second vacate wins nothing and drains nothing, so the
/// emission cannot double-fire.
///
/// (RED before the fix: the vacate SREMs `chans` and returns, so u1 stays in all three
/// hashes forever — `member_removed` never fires, and every later join of u1 on this
/// channel is silently suppressed by the ghost refcount.)
#[tokio::test]
async fn vacate_drains_presence_roster_left_by_a_dead_node() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let scripts = Scripts::new();
        let clients = RedisClients::connect(&test_redis_url(), 2)
            .await
            .expect("fred clients must connect to the test Redis");
        let channel = format!("presence-orphan-{}", Uuid::new_v4());

        seed_presence_user(&clients, &keys, &channel, "u1", &["deadnode:s1"], 1).await;
        let _: () = clients
            .pool
            .next()
            .sadd(keys.chans(TEST_APP), channel.clone())
            .await
            .expect("sadd chans");

        let (won, drained) = run_vacate(&scripts, &clients.pool, &keys, &channel).await;
        assert_eq!(won, 1, "the orphaned channel's vacate must win");
        assert_eq!(
            drained,
            vec!["u1".to_string()],
            "the winning vacate must drain the surviving roster (got {drained:?})"
        );
        assert_presence_fully_reclaimed(&clients, &keys, &channel).await;

        // A racing second sweeper finds the chans entry gone: no win, no second drain,
        // so the drained user can never be announced twice.
        let (won, drained) = run_vacate(&scripts, &clients.pool, &keys, &channel).await;
        assert_eq!(
            (won, drained),
            (0, Vec::new()),
            "a losing vacate must neither win nor drain"
        );
    })
    .await
    .expect("vacate presence-drain test must not hang (Redis up?)");
}

// ---------------------------------------------------------------------------
// Reap-member CAS (duplicate member_removed regression — F-6).
//
// The `member_removed` emission right belongs to whichever caller's atomic
// operation takes the user's refcount to EXACTLY 0 (the member analog of the
// vacate CAS above). Two writers can reach the user-removal edge concurrently —
// the live leave (PRESENCE_LEAVE_LUA, driven by the bridge, emitting iff the
// script returns `== 0`) and the sweeper's stale-token reap (REAP_MEMBER_LUA,
// emitting iff `won == 1`) — and before the CAS the sweeper's separate
// HGET→HDEL→HINCRBY commands could double-decrement a refcount the live path
// had already zeroed, its `<= 0` gate firing a second `member_removed` for one
// user removal. These tests drive the two scripts DIRECTLY, in BOTH orderings
// of the race, and assert exactly one winner each — deterministic where the
// full-system straddle is not.
// ---------------------------------------------------------------------------

/// Run the bridge's PRESENCE_JOIN_LUA uncapped for `user_id`/`token` presenting
/// `user_info`; returns the user's new cluster-wide connection refcount.
async fn run_presence_join(
    scripts: &Scripts,
    pool: &fred::clients::Pool,
    keys: &Keys,
    channel: &str,
    user_id: &str,
    token: &str,
    user_info: &str,
) -> i64 {
    scripts
        .presence_join
        .evalsha_with_reload::<i64, _, _>(
            pool.next(),
            vec![
                keys.presusers(TEST_APP, channel),
                keys.presinfo(TEST_APP, channel),
                keys.presmembers(TEST_APP, channel),
                keys.presseats(TEST_APP, channel),
            ],
            vec![
                user_id.to_string(),
                user_info.to_string(),
                token.to_string(),
                "-1".to_string(),
            ],
        )
        .await
        .expect("PRESENCE_JOIN_LUA must eval")
}

/// The `user_info` the cluster roster currently advertises for `user_id`.
async fn roster_user_info(
    clients: &RedisClients,
    keys: &Keys,
    channel: &str,
    user_id: &str,
) -> Option<String> {
    clients
        .pool
        .next()
        .hget(keys.presinfo(TEST_APP, channel), user_id)
        .await
        .expect("hget presinfo")
}

/// The seats recorded for `user_id`, as the packed `token\nuser_info\n` blob.
async fn seats_of(
    clients: &RedisClients,
    keys: &Keys,
    channel: &str,
    user_id: &str,
) -> Option<String> {
    clients
        .pool
        .next()
        .hget(keys.presseats(TEST_APP, channel), user_id)
        .await
        .expect("hget presseats")
}

/// Run the bridge's PRESENCE_LEAVE_LUA for `user_id`/`token`; returns the
/// remaining refcount (the live path emits `member_removed` iff it is `== 0`).
async fn run_presence_leave(
    scripts: &Scripts,
    pool: &fred::clients::Pool,
    keys: &Keys,
    channel: &str,
    user_id: &str,
    token: &str,
) -> i64 {
    scripts
        .presence_leave
        .evalsha_with_reload::<i64, _, _>(
            pool.next(),
            vec![
                keys.presusers(TEST_APP, channel),
                keys.presinfo(TEST_APP, channel),
                keys.presmembers(TEST_APP, channel),
                keys.presseats(TEST_APP, channel),
            ],
            vec![user_id.to_string(), token.to_string()],
        )
        .await
        .expect("PRESENCE_LEAVE_LUA must eval")
}

/// Run the sweeper's REAP_MEMBER_LUA for `token`; returns `(user_id, remaining,
/// won)` (`won == 1` iff THIS call took the user's refcount to exactly 0 and
/// owns the single `member_removed` emission right).
async fn run_reap_member(
    scripts: &Scripts,
    pool: &fred::clients::Pool,
    keys: &Keys,
    channel: &str,
    token: &str,
) -> (String, i64, i64) {
    scripts
        .reap_member
        .evalsha_with_reload::<(String, i64, i64), _, _>(
            pool.next(),
            vec![
                keys.presusers(TEST_APP, channel),
                keys.presinfo(TEST_APP, channel),
                keys.presmembers(TEST_APP, channel),
                keys.presseats(TEST_APP, channel),
            ],
            vec![token.to_string()],
        )
        .await
        .expect("REAP_MEMBER_LUA must eval")
}

/// Seed the presence state `user_id`'s connections produce: `presmembers`
/// token→user_id for each token, `presseats` user_id→each token's seat in join
/// order, `presusers` user_id→`count`, `presinfo` user_id→user_info (what
/// PRESENCE_JOIN_LUA leaves behind).
async fn seed_presence_user(
    clients: &RedisClients,
    keys: &Keys,
    channel: &str,
    user_id: &str,
    tokens: &[&str],
    count: i64,
) {
    let pool = clients.pool.next();
    let info = format!(r#"{{"name":"{user_id}"}}"#);
    let mut seats = String::new();
    for token in tokens {
        let _: () = pool
            .hset(
                keys.presmembers(TEST_APP, channel),
                (token.to_string(), user_id.to_string()),
            )
            .await
            .expect("hset presmembers");
        seats.push_str(&format!("{token}\n{info}\n"));
    }
    let _: () = pool
        .hset(
            keys.presseats(TEST_APP, channel),
            (user_id.to_string(), seats),
        )
        .await
        .expect("hset presseats");
    let _: () = pool
        .hset(
            keys.presusers(TEST_APP, channel),
            (user_id.to_string(), count),
        )
        .await
        .expect("hset presusers");
    let _: () = pool
        .hset(
            keys.presinfo(TEST_APP, channel),
            (user_id.to_string(), info),
        )
        .await
        .expect("hset presinfo");
}

/// Assert the channel's three presence hashes hold nothing (user fully
/// reclaimed, no ghost token or refcount residue).
async fn assert_presence_fully_reclaimed(clients: &RedisClients, keys: &Keys, channel: &str) {
    for key in [
        keys.presusers(TEST_APP, channel),
        keys.presinfo(TEST_APP, channel),
        keys.presmembers(TEST_APP, channel),
        keys.presseats(TEST_APP, channel),
    ] {
        let len: i64 = clients.pool.next().hlen(&key).await.expect("hlen");
        assert_eq!(len, 0, "presence hash {key} must be empty");
    }
}

/// Ordering 1 — the live leave wins, the sweeper's later stale-token reap must
/// stay silent. u1 has one connection whose node's heartbeat went stale; the
/// socket then closes orderly. The live PRESENCE_LEAVE_LUA takes the refcount
/// to exactly 0 (returns 0 → the bridge emits the single `member_removed`);
/// the sweeper's later REAP_MEMBER_LUA finds the token already gone
/// (`won == 0`) — exactly one emission right in total, in the ordering that
/// used to double-fire via the `<= 0` gate on a double-decrement.
#[tokio::test]
async fn reap_cas_live_leave_first_reap_silent() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let scripts = Scripts::new();
        let clients = RedisClients::connect(&test_redis_url(), 2)
            .await
            .expect("fred clients must connect to the test Redis");
        let channel = format!("presence-cas-{}", Uuid::new_v4());

        seed_presence_user(&clients, &keys, &channel, "u1", &["n1:s1"], 1).await;

        // The live leave: takes the refcount 1→0 → returns 0 → the bridge
        // owns the single member_removed emission.
        let left =
            run_presence_leave(&scripts, &clients.pool, &keys, &channel, "u1", "n1:s1").await;
        assert_eq!(left, 0, "the live leave must own the →0 emission right");

        // The sweeper's late reap of the same stale token: the token is
        // already gone → won == 0 → silent.
        let (user_id, left, won) =
            run_reap_member(&scripts, &clients.pool, &keys, &channel, "n1:s1").await;
        assert_eq!(
            (user_id.as_str(), left, won),
            ("", 0, 0),
            "a reap after the live leave already won must NOT win the emission right"
        );

        assert_presence_fully_reclaimed(&clients, &keys, &channel).await;
    })
    .await
    .expect("reap CAS ordering-1 test must not hang (Redis up?)");
}

/// Ordering 2 — the sweeper wins the straddle, the later live leave must stay
/// silent. The sweeper's REAP_MEMBER_LUA observes the refcount at exactly 1 and
/// atomically removes the token AND the user (`won == 1` → the sweeper emits
/// the single `member_removed`); the bridge's late PRESENCE_LEAVE_LUA — whose
/// HDEL no-ops on the gone token — HINCRBYs the HDEL'd field to −1 (not 0),
/// so the live path's `== 0` emit gate suppresses it. Exactly one emission
/// right in total, in the ordering that used to double-fire.
#[tokio::test]
async fn reap_cas_reap_first_live_leave_silent() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let scripts = Scripts::new();
        let clients = RedisClients::connect(&test_redis_url(), 2)
            .await
            .expect("fred clients must connect to the test Redis");
        let channel = format!("presence-cas-{}", Uuid::new_v4());

        seed_presence_user(&clients, &keys, &channel, "u1", &["n1:s1"], 1).await;

        // The sweeper's atomic reap: resolves the token, sees the refcount at
        // exactly 1, removes token + user → WINS the emission right.
        let (user_id, left, won) =
            run_reap_member(&scripts, &clients.pool, &keys, &channel, "n1:s1").await;
        assert_eq!(
            (user_id.as_str(), left, won),
            ("u1", 0, 1),
            "the reap of a 1-connection user must WIN the emission right"
        );

        // The bridge's late live leave: HDEL no-ops, HINCRBY on the HDEL'd
        // field yields −1 — NOT 0, so the live path stays silent (and the
        // script's `<= 0` cleanup leaves no −1 residue behind).
        let left =
            run_presence_leave(&scripts, &clients.pool, &keys, &channel, "u1", "n1:s1").await;
        assert_eq!(
            left, -1,
            "a late live leave must NOT see == 0 after the reap already won"
        );

        assert_presence_fully_reclaimed(&clients, &keys, &channel).await;
    })
    .await
    .expect("reap CAS ordering-2 test must not hang (Redis up?)");
}

/// Shared user, both writers legitimate: u1 holds one stale token (crashed
/// node) and one live connection. The stale reap is a PLAIN DECREMENT
/// (`won == 0`, refcount 2→1, no emission — the user is still present); the
/// live connection's leave then takes 1→0 and owns the single emission. A
/// further late reap of the live token stays silent.
#[tokio::test]
async fn reap_cas_shared_user_reap_decrements_leave_wins() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let scripts = Scripts::new();
        let clients = RedisClients::connect(&test_redis_url(), 2)
            .await
            .expect("fred clients must connect to the test Redis");
        let channel = format!("presence-cas-{}", Uuid::new_v4());

        seed_presence_user(&clients, &keys, &channel, "u1", &["n1:s1", "n2:s2"], 2).await;

        // Reap the crashed node's token: the user still has a live connection,
        // so this is a decrement, not the →0 edge — no emission right.
        let (user_id, left, won) =
            run_reap_member(&scripts, &clients.pool, &keys, &channel, "n1:s1").await;
        assert_eq!(
            (user_id.as_str(), left, won),
            ("u1", 1, 0),
            "a reap while the user still holds a connection must NOT win"
        );

        // The live connection's leave takes 1→0 → it owns the emission.
        let left =
            run_presence_leave(&scripts, &clients.pool, &keys, &channel, "u1", "n2:s2").await;
        assert_eq!(
            left, 0,
            "the last live leave must own the →0 emission right"
        );

        // A late reap of the already-left live token stays silent.
        let (user_id, left, won) =
            run_reap_member(&scripts, &clients.pool, &keys, &channel, "n2:s2").await;
        assert_eq!(
            (user_id.as_str(), left, won),
            ("", 0, 0),
            "a late reap must NOT win after the live leave already won"
        );

        assert_presence_fully_reclaimed(&clients, &keys, &channel).await;
    })
    .await
    .expect("reap CAS shared-user test must not hang (Redis up?)");
}

/// Ghost token: the token lingers in `presmembers` but the user's refcount
/// field is already gone (the →0 edge was taken — and emitted — by another
/// writer). The reap still garbage-collects the stale token but returns
/// `won == 0` — no second `member_removed` for a removal that already fired.
#[tokio::test]
async fn reap_cas_ghost_token_reaped_without_emission() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let scripts = Scripts::new();
        let clients = RedisClients::connect(&test_redis_url(), 2)
            .await
            .expect("fred clients must connect to the test Redis");
        let channel = format!("presence-cas-{}", Uuid::new_v4());

        // Seed ONLY the token — no presusers/presinfo entries for u1.
        let _: () = clients
            .pool
            .next()
            .hset(
                keys.presmembers(TEST_APP, &channel),
                ("n1:s1".to_string(), "u1".to_string()),
            )
            .await
            .expect("hset presmembers");

        let (user_id, left, won) =
            run_reap_member(&scripts, &clients.pool, &keys, &channel, "n1:s1").await;
        assert_eq!(
            (user_id.as_str(), left, won),
            ("u1", 0, 0),
            "a ghost token must be reaped WITHOUT a second emission right"
        );

        assert_presence_fully_reclaimed(&clients, &keys, &channel).await;
    })
    .await
    .expect("reap CAS ghost-token test must not hang (Redis up?)");
}

// ---------------------------------------------------------------------------
// Issue #79 — the cluster roster re-seat. `presinfo` kept whichever connection
// wrote it first, cluster-wide, forever, so a clustered deployment advertised
// metadata no live connection had presented while the node-local roster (#63)
// re-seated. These drive the scripts directly, in the orderings the adapter
// cannot make deterministic.
// ---------------------------------------------------------------------------

/// The roster entry follows the user's OLDEST LIVE connection, exactly as
/// `ChannelState` does node-locally: a later connection never displaces the seat,
/// removing a later connection leaves it alone, and the seeding connection's
/// departure hands it to the next-oldest survivor — not to the newest, and not to
/// the departed value.
#[tokio::test]
async fn presence_leave_reseats_the_roster_on_the_oldest_surviving_connection() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let scripts = Scripts::new();
        let clients = RedisClients::connect(&test_redis_url(), 2)
            .await
            .expect("fred clients must connect to the test Redis");
        let channel = format!("presence-reseat-{}", Uuid::new_v4());

        for (token, info) in [
            ("n1:s1", r#"{"name":"Old"}"#),
            ("n2:s2", r#"{"name":"Middle"}"#),
            ("n3:s3", r#"{"name":"New"}"#),
        ] {
            run_presence_join(&scripts, &clients.pool, &keys, &channel, "u1", token, info).await;
        }
        assert_eq!(
            roster_user_info(&clients, &keys, &channel, "u1")
                .await
                .as_deref(),
            Some(r#"{"name":"Old"}"#),
            "a later connection must not displace the oldest connection's seat"
        );

        let left =
            run_presence_leave(&scripts, &clients.pool, &keys, &channel, "u1", "n2:s2").await;
        assert_eq!(left, 2, "a middle connection leaving is not the →0 edge");
        assert_eq!(
            roster_user_info(&clients, &keys, &channel, "u1")
                .await
                .as_deref(),
            Some(r#"{"name":"Old"}"#),
            "removing a connection that never held the seat must not move it"
        );

        let left =
            run_presence_leave(&scripts, &clients.pool, &keys, &channel, "u1", "n1:s1").await;
        assert_eq!(left, 1, "the user still holds its newest connection");
        assert_eq!(
            roster_user_info(&clients, &keys, &channel, "u1")
                .await
                .as_deref(),
            Some(r#"{"name":"New"}"#),
            "with the seeding connection gone the roster must carry the survivor's value"
        );

        let left =
            run_presence_leave(&scripts, &clients.pool, &keys, &channel, "u1", "n3:s3").await;
        assert_eq!(left, 0, "the last leave owns the member_removed edge");
        assert_presence_fully_reclaimed(&clients, &keys, &channel).await;
    })
    .await
    .expect("presence re-seat test must not hang (Redis up?)");
}

/// The seeding connection can also vanish without a leave — its node crashed and
/// the sweeper reaps its token. That path re-seats the roster too, or a crash would
/// pin the roster on the dead connection's value for as long as the user survives.
#[tokio::test]
async fn reap_member_reseats_the_roster_when_the_seeding_connection_crashed() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let scripts = Scripts::new();
        let clients = RedisClients::connect(&test_redis_url(), 2)
            .await
            .expect("fred clients must connect to the test Redis");
        let channel = format!("presence-reseat-{}", Uuid::new_v4());

        run_presence_join(
            &scripts,
            &clients.pool,
            &keys,
            &channel,
            "u1",
            "deadnode:s1",
            r#"{"name":"Old"}"#,
        )
        .await;
        run_presence_join(
            &scripts,
            &clients.pool,
            &keys,
            &channel,
            "u1",
            "n2:s2",
            r#"{"name":"New"}"#,
        )
        .await;

        let (user_id, left, won) =
            run_reap_member(&scripts, &clients.pool, &keys, &channel, "deadnode:s1").await;
        assert_eq!(
            (user_id.as_str(), left, won),
            ("u1", 1, 0),
            "reaping one connection of a still-present user is a plain decrement"
        );
        assert_eq!(
            roster_user_info(&clients, &keys, &channel, "u1")
                .await
                .as_deref(),
            Some(r#"{"name":"New"}"#),
            "the reap must re-seat the roster on the surviving connection"
        );
    })
    .await
    .expect("presence reap re-seat test must not hang (Redis up?)");
}

/// Mixed fleet, old writer → new reader: a user whose connections were all recorded
/// by a node that predates `presseats` has no seat to re-seat from. The leave must
/// leave `presinfo` exactly as it found it — the pre-`presseats` behaviour — rather
/// than blanking the roster entry of a user who is still present.
#[tokio::test]
async fn a_roster_with_no_recorded_seats_keeps_its_value_through_a_leave() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let scripts = Scripts::new();
        let clients = RedisClients::connect(&test_redis_url(), 2)
            .await
            .expect("fred clients must connect to the test Redis");
        let channel = format!("presence-reseat-{}", Uuid::new_v4());

        seed_presence_user(&clients, &keys, &channel, "u1", &["n1:s1", "n2:s2"], 2).await;
        let _: () = clients
            .pool
            .next()
            .del(keys.presseats(TEST_APP, &channel))
            .await
            .expect("raw DEL presseats must succeed");

        let left =
            run_presence_leave(&scripts, &clients.pool, &keys, &channel, "u1", "n1:s1").await;
        assert_eq!(left, 1, "the user still holds its other connection");
        assert_eq!(
            roster_user_info(&clients, &keys, &channel, "u1")
                .await
                .as_deref(),
            Some(r#"{"name":"u1"}"#),
            "with no seat recorded the roster value must be left untouched"
        );
        assert_eq!(
            seats_of(&clients, &keys, &channel, "u1").await,
            None,
            "a leave must not invent a seat for a connection it never saw join"
        );
    })
    .await
    .expect("seatless roster test must not hang (Redis up?)");
}

/// Mixed fleet, new writer → old leaver: a node that predates `presseats` removes a
/// connection from `presmembers` and decrements the refcount, leaving that
/// connection's seat behind. `presmembers` is the liveness truth, so the orphaned
/// seat must never be seated — and must be collected on the way past.
#[tokio::test]
async fn a_seat_orphaned_by_a_writer_that_records_none_is_pruned_not_seated() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let scripts = Scripts::new();
        let clients = RedisClients::connect(&test_redis_url(), 2)
            .await
            .expect("fred clients must connect to the test Redis");
        let channel = format!("presence-reseat-{}", Uuid::new_v4());

        for (token, info) in [
            ("n1:s1", r#"{"name":"Old"}"#),
            ("n2:s2", r#"{"name":"New"}"#),
        ] {
            run_presence_join(&scripts, &clients.pool, &keys, &channel, "u1", token, info).await;
        }

        // The older node's leave of n1:s1, verbatim: its script's two writes, and
        // no seat removal because its script knows of no seats.
        let pool = clients.pool.next();
        let _: () = pool
            .hdel(keys.presmembers(TEST_APP, &channel), "n1:s1")
            .await
            .expect("raw HDEL presmembers must succeed");
        let _: i64 = pool
            .hincrby(keys.presusers(TEST_APP, &channel), "u1", -1)
            .await
            .expect("raw HINCRBY presusers must succeed");

        run_presence_join(
            &scripts,
            &clients.pool,
            &keys,
            &channel,
            "u1",
            "n3:s3",
            r#"{"name":"Third"}"#,
        )
        .await;
        let left =
            run_presence_leave(&scripts, &clients.pool, &keys, &channel, "u1", "n3:s3").await;
        assert_eq!(left, 1, "only the departed node's connection is gone");
        assert_eq!(
            roster_user_info(&clients, &keys, &channel, "u1")
                .await
                .as_deref(),
            Some(r#"{"name":"New"}"#),
            "the orphaned seat must not be chosen over the oldest LIVE connection"
        );
        assert_eq!(
            seats_of(&clients, &keys, &channel, "u1").await.as_deref(),
            Some("n2:s2\n{\"name\":\"New\"}\n"),
            "the orphaned seat must be collected, leaving only live connections"
        );
    })
    .await
    .expect("orphaned seat test must not hang (Redis up?)");
}

/// Poll `SISMEMBER key member` until Redis reports membership or `timeout` elapses.
/// The event-based wait for a reconciler repair: poll the observable rather than
/// sleeping for a guessed number of ticks.
async fn await_set_contains(
    clients: &RedisClients,
    key: &str,
    member: &str,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let present: bool = clients
            .pool
            .next()
            .sismember(key, member)
            .await
            .unwrap_or(false);
        if present {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Issue #50: `apps` / `chans` / `users` are the sweeper's ONLY enumeration of
/// occupied channels and signed-in users, and the `chans` entry is the CAS the
/// single cluster-wide `channel_vacated` is won on — yet `chans` / `users` were
/// written only on the cluster 0→1 edge and never re-seeded, while the membership
/// refresh unconditionally re-created `occ` / `usr`. Any divergence therefore left
/// the channel functionally occupied and structurally orphaned FOREVER: no
/// `channel_vacated`, no sweeper reach, and a `GET /channels` that under-reports.
///
/// The divergence is reproduced directly — the live entries are removed from all
/// three indexes, which is the state a Redis restart or a dropped bridge command
/// leaves once the membership refresh has restored `occ` / `usr` and nothing has
/// restored the indexes. The reconciler must put all three back from node-local
/// truth, and the last unsubscribe must then still win the vacate CAS.
#[tokio::test]
async fn reconciler_reseeds_the_enumeration_indexes_after_a_divergence() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        // A 1s reconcile cadence keeps the repair inside the test's budget.
        let adapter = connect_adapter_with_prefix_ttl(&prefix, 60, 1).await;
        let clients = RedisClients::connect(&test_redis_url(), 2)
            .await
            .expect("fred clients must connect to the test Redis");

        let (sock, handle) = fake_handle();
        adapter
            .subscribe(TEST_APP, "public-room", handle, None)
            .await;
        let (_user_sock, user_handle) = fake_handle();
        adapter.signin_user(TEST_APP, "u5", user_handle).await;

        let indexed = [
            (keys.apps(), TEST_APP.to_string()),
            (keys.chans(TEST_APP), "public-room".to_string()),
            (keys.users(TEST_APP), "u5".to_string()),
        ];
        for (key, member) in &indexed {
            let removed: i64 = clients
                .pool
                .next()
                .srem(key, member.clone())
                .await
                .expect("SREM must succeed");
            assert_eq!(
                removed, 1,
                "{member} must be indexed in {key} before the divergence is staged"
            );
        }

        for (key, member) in &indexed {
            assert!(
                await_set_contains(&clients, key, member, Duration::from_secs(10)).await,
                "the reconciler must re-seed {member} into {key} from node-local truth"
            );
        }

        // The REST channel listing reads `chans`, so it recovers with the index.
        let listed = adapter.channels(TEST_APP, None).await;
        assert!(
            listed.iter().any(|c| c.name == "public-room"),
            "channels() must list the re-indexed channel again, got {listed:?}"
        );

        // And the vacate CAS — the SREM that carries the single cluster-wide
        // `channel_vacated` emission right — can be won again.
        let out = adapter.unsubscribe(TEST_APP, "public-room", &sock).await;
        assert!(
            out.vacated,
            "the last unsubscribe must win the vacate CAS on the re-seeded index"
        );

        let _ = clients.pool.quit().await;
    })
    .await
    .expect("index re-seed test must not hang (Redis up?)");
}

/// Issue #50, second half: the index write inside `SUBSCRIBE_LUA` / `USER_SIGNIN_LUA`
/// must be unconditional, not gated on the cluster 0→1 edge. A subscriber that
/// arrives while the channel is ALREADY occupied elsewhere (`HLEN != 1`) used to
/// leave a lost index entry lost — the channel could never be re-indexed for as
/// long as it stayed occupied.
#[tokio::test]
async fn subscribe_and_signin_reindex_without_the_cluster_first_edge() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let node_a = connect_adapter_with_prefix(&prefix).await;
        let node_b = connect_adapter_with_prefix(&prefix).await;
        let clients = RedisClients::connect(&test_redis_url(), 2)
            .await
            .expect("fred clients must connect to the test Redis");

        let (_sock_a, handle_a) = fake_handle();
        node_a
            .subscribe(TEST_APP, "public-room", handle_a, None)
            .await;
        let (_user_a, user_handle_a) = fake_handle();
        node_a.signin_user(TEST_APP, "u5", user_handle_a).await;

        // Stage the divergence while both stay occupied cluster-wide.
        for (key, member) in [
            (keys.chans(TEST_APP), "public-room"),
            (keys.users(TEST_APP), "u5"),
        ] {
            let removed: i64 = clients
                .pool
                .next()
                .srem(&key, member)
                .await
                .expect("SREM must succeed");
            assert_eq!(removed, 1, "{member} must be indexed in {key} first");
        }

        // A second member / connection on another node: HLEN goes 1→2, so this is
        // NOT the cluster-first edge, and it must still re-index.
        let (_sock_b, handle_b) = fake_handle();
        let out = node_b
            .subscribe(TEST_APP, "public-room", handle_b, None)
            .await;
        assert_eq!(
            out.subscription_count, 2,
            "the second cluster subscriber must see cluster count 2"
        );
        let (_user_b, user_handle_b) = fake_handle();
        node_b.signin_user(TEST_APP, "u5", user_handle_b).await;

        for (key, member) in [
            (keys.chans(TEST_APP), "public-room"),
            (keys.users(TEST_APP), "u5"),
        ] {
            let present: bool = clients
                .pool
                .next()
                .sismember(&key, member)
                .await
                .expect("SISMEMBER must succeed");
            assert!(
                present,
                "a non-first subscribe/signin must still index {member} in {key}"
            );
        }

        let _ = clients.pool.quit().await;
    })
    .await
    .expect("non-first-edge reindex test must not hang (Redis up?)");
}

/// Issue #52: the Redis `SUBSCRIBE` of a channel's `msg` key (and a user's
/// `usermsg` key) is derived from node-local LEVEL truth but was applied only on
/// the EDGE the bridge received. That command rides a bounded, drop-on-full
/// channel, so one dropped node-first command left the node deaf to ALL of that
/// channel's cross-node traffic — indefinitely, while `redis_connected` stayed
/// true and only a debug line recorded the drop.
///
/// The drop is reproduced directly: node B's shared `LocalAdapter` gains the
/// subscriber and the signed-in user while `cluster_subscribe` / `cluster_signin`
/// never run for them — exactly the state a dropped `ClusterCmd::Subscribe` /
/// `Signin` leaves behind. The reconciler must restore both subscriptions, and a
/// broadcast and a `send_to_user` from node A must then reach node B.
#[tokio::test]
async fn reconciler_resubscribes_pubsub_keys_after_a_dropped_bridge_command() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let channel = "deafened-room";
        let node_a = connect_adapter_with_prefix(&prefix).await;

        // Node B's adapter shares the LocalAdapter the workers would drive, so the
        // test can commit node-local membership WITHOUT the bridge command that
        // normally carries the Redis SUBSCRIBE with it.
        let local_b = Arc::new(LocalAdapter::new(
            Arc::new(Registry::new()),
            Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
        ));
        let node_b = RedisAdapter::with_local(
            &redis_test_config_with_ttl(&prefix, 60, 1),
            local_b.clone(),
            None,
            None,
        )
        .await
        .expect("node B's RedisAdapter must connect to the test Redis");

        let (_sock, handle, mut rx) = recording_handle();
        local_b.subscribe(TEST_APP, channel, handle, None).await;
        let (_user_sock, user_handle, mut user_rx) = recording_handle();
        local_b.signin_user(TEST_APP, "u6", user_handle).await;

        let msg_key = keys.msg(TEST_APP, channel);
        let usermsg_key = keys.usermsg(TEST_APP, "u6");
        assert!(
            await_tracked(&node_b, &msg_key, Duration::from_secs(10)).await,
            "the reconciler must SUBSCRIBE {msg_key} for a channel node B has local members on"
        );
        assert!(
            await_tracked(&node_b, &usermsg_key, Duration::from_secs(10)).await,
            "the reconciler must SUBSCRIBE {usermsg_key} for a user signed in on node B"
        );

        // A completed SUBSCRIBE is not yet an observable attachment, and a publish
        // that races one is lost outright — gate on the server's own NUMSUB, the
        // readiness gate this suite shares.
        let gate = RedisClients::connect(&test_redis_url(), 1)
            .await
            .expect("fred clients must connect to the test Redis");
        require_numsub_at_least(
            gate.pool.next(),
            &msg_key,
            1,
            Duration::from_secs(5),
            "node B's reconciled msg subscription",
        )
        .await;
        require_numsub_at_least(
            gate.pool.next(),
            &usermsg_key,
            1,
            Duration::from_secs(5),
            "node B's reconciled usermsg subscription",
        )
        .await;

        node_a
            .broadcast(
                TEST_APP,
                channel,
                ServerEvent::ChannelEvent {
                    channel: channel.into(),
                    event: "reconciled".into(),
                    data: serde_json::json!({"k": 1}),
                    user_id: None,
                },
                None,
            )
            .await;
        match with_timeout(async { rx.recv().await }).await.map(|b| *b) {
            Some(ServerEvent::Raw(frame)) => {
                let v: serde_json::Value = serde_json::from_str(&frame).expect("raw frame is JSON");
                assert_eq!(
                    v["event"], "reconciled",
                    "node B must receive the cross-node broadcast again"
                );
            }
            other => panic!("expected the cross-node broadcast on node B, got {other:?}"),
        }

        node_a
            .send_to_user(
                TEST_APP,
                "u6",
                ServerEvent::ChannelEvent {
                    channel: "x".into(),
                    event: "direct".into(),
                    data: serde_json::json!({"k": 2}),
                    user_id: None,
                },
            )
            .await;
        match with_timeout(async { user_rx.recv().await })
            .await
            .map(|b| *b)
        {
            Some(ServerEvent::Raw(frame)) => {
                let v: serde_json::Value = serde_json::from_str(&frame).expect("raw frame is JSON");
                assert_eq!(
                    v["event"], "direct",
                    "node B must receive the cross-node send_to_user again"
                );
            }
            other => panic!("expected the cross-node send_to_user on node B, got {other:?}"),
        }

        let _ = gate.pool.quit().await;
    })
    .await
    .expect("pub/sub reconcile test must not hang (Redis up?)");
}

// ---------------------------------------------------------------------------
// User-reap CAS (live-binding wipe + spurious WatchOffline regression).
//
// The user reap is the twin of the member reap above: it resolves stale tokens,
// decides the cluster →0 offline edge, and emits. Split across separate
// round trips, its decision could straddle a signin — the reap reading an empty
// hash, a signin on another node writing a live binding and indexing the user,
// and the reap's later DEL + SREM then wiping BOTH while publishing an offline
// for a user that is online. As one script, Redis serialises it against
// MEMBERSHIP_JOIN_LUA and the straddle has nowhere to land.
// ---------------------------------------------------------------------------

/// Run the signin half of MEMBERSHIP_JOIN_LUA for `user_id`/`token`, stamping a
/// binding that stays fresh for a minute. Returns the cluster connection count.
async fn run_user_signin(
    scripts: &Scripts,
    pool: &fred::clients::Pool,
    keys: &Keys,
    user_id: &str,
    token: &str,
    now: u64,
) -> i64 {
    scripts
        .membership_join
        .evalsha_with_reload::<i64, _, _>(
            pool.next(),
            vec![keys.usr(TEST_APP, user_id), keys.users(TEST_APP)],
            vec![
                token.to_string(),
                (now + 60_000).to_string(),
                "60".to_string(),
                user_id.to_string(),
            ],
        )
        .await
        .expect("MEMBERSHIP_JOIN_LUA must eval")
}

/// A signin racing the sweeper's reap of the SAME user must survive it, whichever
/// way the two land: the reap either finds the fresh binding and declines, or wins
/// the offline edge outright and the signin re-establishes the user behind it.
/// What must never happen is the reap's decision being made before the signin and
/// its writes landing after — wiping a live binding, de-indexing an online user
/// from `users(app)` for good, and publishing an offline for them.
///
/// The race is driven directly: each round arms one stale binding on a dead node,
/// starts a sweep, and fires the signin the instant the reap's own HDEL empties the
/// hash — the window the pre-CAS reap left between its `HLEN` guard and its `DEL`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_reap_cannot_wipe_a_signin_it_straddles() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let adapter = connect_adapter_with_prefix(&prefix).await;
        let clients = RedisClients::connect(&test_redis_url(), 4)
            .await
            .expect("fred clients must connect to the test Redis");
        let webhooks = pylon::webhook::WebhookHandle::null();
        let _: () = clients
            .pool
            .next()
            .sadd(keys.apps(), TEST_APP)
            .await
            .expect("sadd apps");

        for _ in 0..40 {
            let user_id = format!("u-{}", Uuid::new_v4());
            let usr = keys.usr(TEST_APP, &user_id);
            let now = now_ms();

            // One binding from a node that stopped heart-beating: stale, and the only
            // record of the user besides the `users(app)` index entry.
            let _: () = clients
                .pool
                .next()
                .hset(&usr, ("deadnode:s1", (now - 1_000).to_string()))
                .await
                .expect("hset stale binding");
            let _: () = clients
                .pool
                .next()
                .sadd(keys.users(TEST_APP), user_id.clone())
                .await
                .expect("sadd users");

            let signin = {
                let pool = clients.pool.clone();
                let scripts = Scripts::new();
                let keys = keys.clone();
                let usr = usr.clone();
                let user_id = user_id.clone();
                tokio::spawn(async move {
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                    while tokio::time::Instant::now() < deadline {
                        let live: i64 = pool.next().exists(&usr).await.unwrap_or(1);
                        if live == 0 {
                            break;
                        }
                    }
                    run_user_signin(&scripts, &pool, &keys, &user_id, "nodec:s2", now).await
                })
            };

            let (_acquired, _reaped, _vacated) = adapter.sweep_now(&webhooks, now).await;
            let conns = signin.await.expect("signin task must not panic");
            assert_eq!(conns, 1, "the signin must record the user's only binding");

            let bound: Option<String> = clients
                .pool
                .next()
                .hget(&usr, "nodec:s2")
                .await
                .expect("hget binding");
            assert!(
                bound.is_some(),
                "the reap must not wipe a binding a signin established after its decision"
            );
            let indexed: bool = clients
                .pool
                .next()
                .sismember(keys.users(TEST_APP), user_id.clone())
                .await
                .expect("sismember users");
            assert!(
                indexed,
                "the reap must not de-index a user a signin brought back online"
            );

            let _: i64 = clients.pool.next().del(&usr).await.expect("del usr");
            let _: i64 = clients
                .pool
                .next()
                .srem(keys.users(TEST_APP), user_id)
                .await
                .expect("srem users");
        }
    })
    .await
    .expect("user reap CAS race test must not hang (Redis up?)");
}

/// A member whose `expireAt` stamp is not a number can never be re-stamped into
/// a valid future value by a live node, so the sweeper must treat it as STALE
/// and reap it. Left alone it would pin the channel occupied forever.
///
/// No waiting and no TTL: the sweep runs with `now = 0`, at which instant every
/// well-formed stamp is in the future and therefore fresh. The corrupt stamp is
/// the ONLY thing that can be reaped, so a pass that reaps it proves the
/// unparseable branch ran — and the surviving member proves the sweep did not
/// simply reap everything.
#[tokio::test]
async fn sweeper_reaps_a_member_whose_expire_at_stamp_is_corrupt() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let prefix = random_prefix();
        let adapter = connect_adapter_with_prefix(&prefix).await;
        let keys = Keys::new(&prefix);

        let (corrupt_sock, corrupt_handle) = fake_handle();
        adapter
            .subscribe(TEST_APP, "public-corrupt", corrupt_handle, None)
            .await;
        let (_live_sock, live_handle) = fake_handle();
        let out = adapter
            .subscribe(TEST_APP, "public-corrupt", live_handle, None)
            .await;
        assert_eq!(out.subscription_count, 2, "both members must be recorded");

        // Corrupt exactly one member's stamp, in place.
        let clients = RedisClients::connect(&test_redis_url(), 1)
            .await
            .expect("fred clients must connect to the test Redis");
        let occ = keys.occ(TEST_APP, "public-corrupt");
        let token =
            pylon::adapter::redis::keys::member_token(adapter.node_id(), corrupt_sock.as_str());
        let _: i64 = clients
            .pool
            .next()
            .hset(&occ, (token.clone(), "not-a-timestamp"))
            .await
            .expect("the corrupt stamp must be written");

        let webhooks = pylon::webhook::WebhookHandle::null();
        let (acquired, reaped, vacated) = adapter.sweep_now(&webhooks, 0).await;
        assert!(acquired, "this node must hold the sweep lease");
        assert_eq!(
            reaped, 1,
            "only the corrupt stamp is stale at now=0; every parseable stamp is in the future"
        );
        assert!(
            !vacated.contains(&(TEST_APP.to_string(), "public-corrupt".to_string())),
            "the channel still has a live member and must not be vacated: {vacated:?}"
        );
        assert_eq!(
            adapter
                .channel(TEST_APP, "public-corrupt")
                .await
                .subscription_count,
            1,
            "the reap must remove the corrupt member and only the corrupt member"
        );

        let _: i64 = clients.pool.next().del(&occ).await.expect("del occ");
    })
    .await
    .expect("corrupt-stamp reap test must not hang (Redis up?)");
}

/// `unwatch` on the node's `RedisAdapter` drops the connection's node-local watch
/// state AND — once no local watcher of that user remains — UNSUBSCRIBEs the
/// per-user Redis watch channel. Without the second half the node keeps
/// receiving cross-node transitions for a user nobody here is watching, forever.
#[tokio::test]
async fn unwatch_drops_the_local_watcher_and_unsubscribes_the_watch_channel() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let prefix = random_prefix();
        let keys = Keys::new(&prefix);
        let adapter = connect_adapter_with_prefix(&prefix).await;
        let watch_key = keys.watch(TEST_APP, "u7");

        let (socket_id, watcher, _rx) = recording_handle();
        adapter.watch(TEST_APP, watcher, vec!["u7".into()]).await;
        assert!(
            await_tracked(&adapter, &watch_key, Duration::from_secs(2)).await,
            "precondition: the 0→1 watcher edge must SUBSCRIBE the watch channel"
        );
        assert_eq!(
            adapter.watchers_of(TEST_APP, "u7").await.len(),
            1,
            "precondition: the watcher must be recorded node-locally"
        );

        adapter.unwatch(TEST_APP, &socket_id).await;

        assert!(
            adapter.watchers_of(TEST_APP, "u7").await.is_empty(),
            "the connection's watch state must be dropped"
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            if !tracked_contains(&adapter, &watch_key) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("the 1→0 watcher edge must UNSUBSCRIBE {watch_key}");
    })
    .await
    .expect("unwatch test must not hang (Redis up?)");
}

/// The BACKGROUND sweeper — the loop `start_sweeper` spawns, not the `sweep_now`
/// test seam — reaps on its own timer and fires `channel_vacated` through the
/// dispatcher it was handed. Everything else in this file drives `sweep_once`
/// directly, so nothing else proves the loop is really wired to it.
///
/// The stale member is MADE stale by stamping its `expireAt` into the past
/// rather than by waiting out a TTL, and the wait is on the webhook actually
/// being delivered — never on a fixed sleep.
#[tokio::test]
async fn the_background_sweeper_vacates_an_orphaned_channel_and_fires_the_webhook() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let prefix = random_prefix();
        let cfg = ServerConfig {
            adapter: "redis".into(),
            redis_url: test_redis_url(),
            redis_prefix: prefix.clone(),
            redis_sweep_interval_secs: 1,
            ..ServerConfig::default()
        };
        let adapter = RedisAdapter::new(&cfg)
            .await
            .expect("RedisAdapter::new must connect to the test Redis");
        let keys = Keys::new(&prefix);

        let (socket_id, handle) = fake_handle();
        adapter
            .subscribe(TEST_APP, "public-orphan", handle, None)
            .await;

        // Stamp the member as long expired — the state a node that stopped
        // heartbeating leaves behind. The membership heartbeat's next tick is a
        // full `redis_presence_heartbeat_secs` away, so this stamp stands.
        let clients = RedisClients::connect(&test_redis_url(), 1)
            .await
            .expect("fred clients must connect to the test Redis");
        let occ = keys.occ(TEST_APP, "public-orphan");
        let token =
            pylon::adapter::redis::keys::member_token(adapter.node_id(), socket_id.as_str());
        let _: i64 = clients
            .pool
            .next()
            .hset(&occ, (token, "1"))
            .await
            .expect("the expired stamp must be written");

        let (webhooks, transport) = recording_webhooks();
        adapter.start_sweeper(webhooks);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
        while tokio::time::Instant::now() < deadline {
            let vacated = transport.recorded().await.iter().any(|d| {
                let v: serde_json::Value =
                    serde_json::from_str(&d.body).expect("webhook body must be JSON");
                v["events"].as_array().is_some_and(|events| {
                    events
                        .iter()
                        .any(|e| e["name"] == "channel_vacated" && e["channel"] == "public-orphan")
                })
            });
            if vacated {
                assert_eq!(
                    adapter
                        .channel(TEST_APP, "public-orphan")
                        .await
                        .subscription_count,
                    0,
                    "the vacate must have cleared the channel's members, not just fired a webhook"
                );
                let _: i64 = clients.pool.next().del(&occ).await.expect("del occ");
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("the background sweeper never vacated the orphaned channel");
    })
    .await
    .expect("background sweeper test must not hang (Redis up?)");
}

// ─────────────────────────────────────────────────────────────────────────────
// Sweeper fault injection.
//
// `sweeper.rs` states a contract: "Every Redis error is logged and skipped; one
// failure must never abort the whole sweep. Nothing here panics or unwraps."
// The tests below hold it to that WITHOUT mocking Redis: overwriting a key with
// a plain STRING makes the exact command the sweeper issues against it fail with
// WRONGTYPE, deterministically and on demand. Each test then shows the pass
// degraded in the documented way — and, where the degradation is a no-op, that
// the SAME state sweeps successfully once the key is restored, so the negative
// cannot pass for the wrong reason.
// ─────────────────────────────────────────────────────────────────────────────

/// A fresh prefix holding one channel whose only member is stamped long expired
/// — the state a crashed node leaves behind, and exactly what a healthy sweep
/// reaps and vacates. Returns the adapter, its key builder, and a raw client.
async fn orphaned_channel(channel: &str) -> (RedisAdapter, Keys, RedisClients) {
    let prefix = random_prefix();
    let adapter = connect_adapter_with_prefix(&prefix).await;
    let keys = Keys::new(&prefix);
    let (socket_id, handle) = fake_handle();
    adapter.subscribe(TEST_APP, channel, handle, None).await;
    let clients = RedisClients::connect(&test_redis_url(), 1)
        .await
        .expect("fred clients must connect to the test Redis");
    let token = pylon::adapter::redis::keys::member_token(adapter.node_id(), socket_id.as_str());
    let _: i64 = clients
        .pool
        .next()
        .hset(&keys.occ(TEST_APP, channel), (token, "1"))
        .await
        .expect("the expired stamp must be written");
    (adapter, keys, clients)
}

/// Overwrite `key` with a plain STRING, so the hash/set command the sweeper
/// issues against it fails with WRONGTYPE.
async fn poison(clients: &RedisClients, key: &str) {
    let _: () = clients
        .pool
        .next()
        .set(key, "poisoned", None, None, false)
        .await
        .expect("the poison must be written");
}

/// An unreadable `apps` index costs the pass its enumeration source, so nothing
/// is reaped — but the pass still completes and holds the lease. Restoring the
/// index and sweeping again reaps the very member the poisoned pass left alone,
/// which is what makes the first assertion mean something.
#[tokio::test]
async fn a_corrupt_apps_index_makes_the_sweep_a_safe_no_op() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (adapter, keys, clients) = orphaned_channel("public-noapps").await;
        let webhooks = pylon::webhook::WebhookHandle::null();

        poison(&clients, &keys.apps()).await;
        let (acquired, reaped, vacated) = adapter.sweep_now(&webhooks, now_ms()).await;
        assert!(acquired, "a broken index must not cost the node the lease");
        assert_eq!(reaped, 0, "nothing is enumerable, so nothing is reaped");
        assert!(
            vacated.is_empty(),
            "and nothing may be vacated: {vacated:?}"
        );

        let _: i64 = clients
            .pool
            .next()
            .del(&keys.apps())
            .await
            .expect("del poisoned apps");
        let _: i64 = clients
            .pool
            .next()
            .sadd(&keys.apps(), TEST_APP)
            .await
            .expect("restore apps");
        let (_, reaped, vacated) = adapter.sweep_now(&webhooks, now_ms()).await;
        assert_eq!(
            reaped, 1,
            "the restored pass reaps the member the poisoned pass skipped"
        );
        assert!(
            vacated.contains(&(TEST_APP.to_string(), "public-noapps".to_string())),
            "and vacates the channel it emptied: {vacated:?}"
        );
    })
    .await
    .expect("corrupt-apps sweep test must not hang (Redis up?)");
}

/// An unreadable per-app `chans` index skips THAT app and moves on. Same
/// before/after shape: restore the index and the identical state sweeps clean.
#[tokio::test]
async fn a_corrupt_channel_index_skips_the_app_and_the_pass_continues() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (adapter, keys, clients) = orphaned_channel("public-nochans").await;
        let webhooks = pylon::webhook::WebhookHandle::null();
        let chans = keys.chans(TEST_APP);

        poison(&clients, &chans).await;
        let (acquired, reaped, vacated) = adapter.sweep_now(&webhooks, now_ms()).await;
        assert!(acquired, "the pass still holds the lease");
        assert_eq!(
            reaped, 0,
            "the app whose channel index is unreadable is skipped"
        );
        assert!(vacated.is_empty(), "{vacated:?}");

        let _: i64 = clients
            .pool
            .next()
            .del(&chans)
            .await
            .expect("del poisoned chans");
        let _: i64 = clients
            .pool
            .next()
            .sadd(&chans, "public-nochans")
            .await
            .expect("restore chans");
        let (_, reaped, _) = adapter.sweep_now(&webhooks, now_ms()).await;
        assert_eq!(reaped, 1, "the restored pass reaps the skipped member");
    })
    .await
    .expect("corrupt-chans sweep test must not hang (Redis up?)");
}

/// An unreadable occupancy hash must SKIP the channel, never vacate it. This is
/// the dangerous one: a sweep that treated the unreadable hash as "no members"
/// would de-index a channel that is, for all it knows, still occupied — and fire
/// a `channel_vacated` for it.
#[tokio::test]
async fn a_corrupt_occupancy_hash_skips_the_channel_instead_of_vacating_it() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (adapter, keys, clients) = orphaned_channel("public-noocc").await;
        let webhooks = pylon::webhook::WebhookHandle::null();

        poison(&clients, &keys.occ(TEST_APP, "public-noocc")).await;
        let (acquired, reaped, vacated) = adapter.sweep_now(&webhooks, now_ms()).await;
        assert!(acquired);
        assert_eq!(reaped, 0);
        assert!(
            vacated.is_empty(),
            "a channel whose membership cannot be read must not be declared vacant: {vacated:?}"
        );
        let still_indexed: bool = clients
            .pool
            .next()
            .sismember(keys.chans(TEST_APP), "public-noocc")
            .await
            .expect("sismember chans");
        assert!(
            still_indexed,
            "the channel must stay indexed so a later pass can retry it"
        );
    })
    .await
    .expect("corrupt-occ sweep test must not hang (Redis up?)");
}

/// A vacate whose CAS script errors claims NO emission right: the sweep reports
/// no vacated channel, so no `channel_vacated` webhook is fired off the back of
/// a script that did not return a verdict.
#[tokio::test]
async fn a_failing_vacate_script_claims_no_emission_right() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (adapter, keys, clients) = orphaned_channel("presence-novacate").await;
        let webhooks = pylon::webhook::WebhookHandle::null();

        // VACATE_LUA reads the presence roster with HKEYS; a STRING there makes
        // the script fail after the member reap has already happened, so the
        // pass reaches the vacate and then loses it.
        poison(&clients, &keys.presusers(TEST_APP, "presence-novacate")).await;
        let (acquired, reaped, vacated) = adapter.sweep_now(&webhooks, now_ms()).await;
        assert!(acquired);
        assert_eq!(
            reaped, 1,
            "the stale member is still reaped before the vacate"
        );
        assert!(
            vacated.is_empty(),
            "a failed vacate must claim no emission right: {vacated:?}"
        );
    })
    .await
    .expect("failing-vacate sweep test must not hang (Redis up?)");
}

/// A failure in the user-binding half or the dead-node half must not abort the
/// pass: the channel half, which ran first, still did its work.
#[tokio::test]
async fn a_failure_in_a_later_sweep_phase_does_not_abort_the_earlier_one() {
    tokio::time::timeout(Duration::from_secs(15), async {
        for poisoned in ["users", "nodes"] {
            let channel = format!("public-late-{poisoned}");
            let (adapter, keys, clients) = orphaned_channel(&channel).await;
            let webhooks = pylon::webhook::WebhookHandle::null();
            let key = match poisoned {
                "users" => keys.users(TEST_APP),
                _ => keys.nodes(),
            };
            poison(&clients, &key).await;

            let (acquired, reaped, vacated) = adapter.sweep_now(&webhooks, now_ms()).await;
            assert!(acquired, "the {poisoned} failure must not cost the lease");
            assert_eq!(
                reaped, 1,
                "the channel phase runs before the {poisoned} phase and must still complete"
            );
            assert!(
                vacated.contains(&(TEST_APP.to_string(), channel.clone())),
                "and must still vacate the channel it emptied: {vacated:?}"
            );
        }
    })
    .await
    .expect("late-phase failure sweep test must not hang (Redis up?)");
}

/// A sweep lock that exists but cannot be READ is not this node's to claim, so
/// the pass yields rather than sweeping on an unknown lease. The contrast makes
/// it concrete: with the lock removed, the identical state sweeps.
#[tokio::test]
async fn an_unreadable_sweep_lock_yields_the_pass() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (adapter, keys, clients) = orphaned_channel("public-nolock").await;
        let webhooks = pylon::webhook::WebhookHandle::null();
        let lock = keys.sweeplock();

        // A LIST at the lock key: `SET … NX` finds the key present and declines,
        // and the ownership `GET` that follows fails with WRONGTYPE.
        let _: i64 = clients
            .pool
            .next()
            .lpush(&lock, "not-a-node-id")
            .await
            .expect("the lock poison must be written");

        let (acquired, reaped, vacated) = adapter.sweep_now(&webhooks, now_ms()).await;
        assert!(
            !acquired,
            "a lock whose owner cannot be read must not be swept under"
        );
        assert_eq!(reaped, 0);
        assert!(vacated.is_empty(), "{vacated:?}");

        let _: i64 = clients
            .pool
            .next()
            .del(&lock)
            .await
            .expect("del poisoned lock");
        let (acquired, reaped, _) = adapter.sweep_now(&webhooks, now_ms()).await;
        assert!(acquired, "with the lock gone the node takes it");
        assert_eq!(
            reaped, 1,
            "and reaps the member the yielded pass left alone"
        );
    })
    .await
    .expect("unreadable-lock sweep test must not hang (Redis up?)");
}
