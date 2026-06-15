//! Integration tests for SP11 Task 3.3a: non-presence channel clustering at the
//! adapter + bridge layer — the clustered `subscription_count` broadcast and the
//! single-emit `channel_occupied` / `channel_vacated` edges fired BY THE BRIDGE
//! (not the connection handler) when a percore worker fires a fire-and-forget
//! `ClusterCmd::Subscribe` / `Unsubscribe` at it.
//!
//! Like `redis_cluster.rs` / `cluster_bridge.rs` these talk to a REAL Redis
//! (`PYLON_TEST_REDIS_URL`, default `redis://127.0.0.1:6390`) and isolate every run
//! behind a random key prefix — they NEVER issue FLUSHALL/FLUSHDB. Two
//! `ClusterBridge`es sharing one prefix simulate a 2-node cluster.
//!
//! Observation without a transport:
//! - The clustered `subscription_count` broadcast lands as a `SubscriptionCount`
//!   frame in a fake subscriber's mailbox registered on a node's `LocalAdapter`
//!   (no sink installed → registry mailbox path → `ServerEvent::Raw`).
//! - The occupied/vacated webhooks are captured by a `RecordingTransport` behind a
//!   real `webhook::spawn` dispatcher (one per node); the test parses the recorded
//!   signed envelopes and counts the named events across both nodes.

use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::cluster::bridge::{self, ClusterBridge};
use pylon::connection::handle::ConnectionHandle;
use pylon::protocol::socket_id::SocketId;
use pylon::server::config::ServerConfig;
use pylon::webhook::dispatcher::SystemClock;
use pylon::webhook::transport::{RecordingTransport, WebhookTransport};
use pylon::webhook::WebhookHandle;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Fixed app id used by these tests. Channel/app ids are plain string args to the
/// adapter; they don't come from `ServerConfig`.
const TEST_APP: &str = "app";

/// Test Redis URL: `PYLON_TEST_REDIS_URL` or the documented test default (port 6390).
fn test_redis_url() -> String {
    std::env::var("PYLON_TEST_REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6390".to_string())
}

/// A random, run-unique key prefix for isolation on a shared Redis.
fn random_prefix() -> String {
    format!("pylontest:{}", Uuid::new_v4())
}

/// Build a `ServerConfig` for the Redis adapter against the test Redis with a shared
/// `prefix` (so the two bridges form a 2-node cluster over the same keys).
fn redis_test_config(prefix: &str) -> ServerConfig {
    ServerConfig {
        adapter: "redis".into(),
        redis_url: test_redis_url(),
        redis_prefix: prefix.into(),
        ..ServerConfig::default()
    }
}

/// An `AppManager` whose single app enables `subscription_count` and the
/// occupied/vacated webhooks — the exact per-app flags the bridge resolves to decide
/// whether to broadcast the count and fire the webhooks.
fn apps_manager() -> Arc<dyn AppManager> {
    let raw = r#"[
        {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
         "subscription_count_enabled":true,
         "webhooks":[{"url":"http://127.0.0.1:1/pusher/webhooks",
                      "event_types":["channel_occupied","channel_vacated"]}]}
    ]"#;
    Arc::new(StaticFileAppManager::from_json(raw).expect("apps json must parse"))
}

/// A real webhook dispatcher backed by a `RecordingTransport`, so the bridge's
/// `webhooks.enqueue(...)` is signed/batched exactly as in production but captured in
/// memory. Returns the `WebhookHandle` (handed to the bridge) and the transport (to
/// read back the recorded deliveries). A tiny batch window keeps the test fast.
fn recording_webhooks(apps: Arc<dyn AppManager>) -> (WebhookHandle, RecordingTransport) {
    let transport = RecordingTransport::new();
    let dyn_transport: Arc<dyn WebhookTransport> = Arc::new(transport.clone());
    let handle = pylon::webhook::spawn(
        apps,
        dyn_transport,
        Arc::new(SystemClock),
        10,   // 10ms batch window
        1024, // mailbox capacity
        0,    // vacated fires immediately (no cluster grace in this test)
        None, // no cluster occupancy source
    );
    (handle, transport)
}

/// Count the named webhook events across a `RecordingTransport`'s recorded signed
/// envelopes. Each delivery body is `{ "time_ms", "events": [ { "name", ... } ] }`.
async fn count_webhook(transport: &RecordingTransport, name: &str) -> usize {
    let mut n = 0;
    for d in transport.recorded().await {
        let v: Value = serde_json::from_str(&d.body).expect("webhook body must be JSON");
        if let Some(events) = v.get("events").and_then(|e| e.as_array()) {
            n += events
                .iter()
                .filter(|e| e.get("name").and_then(|x| x.as_str()) == Some(name))
                .count();
        }
    }
    n
}

/// Register a fake subscriber for `(TEST_APP, channel)` on `local` (no sink installed
/// → registry mailbox path) and return its mailbox receiver so the test can observe
/// the bridge's `redis.broadcast(SubscriptionCount)`. Also returns the local
/// `subscription_count` so the caller can pass the right `node_first` edge to the
/// `ClusterHandle::subscribe` it fires next.
async fn fake_subscriber(
    local: &LocalAdapter,
    channel: &str,
) -> (
    SocketId,
    usize,
    tokio::sync::mpsc::UnboundedReceiver<pylon::protocol::event::ServerEvent>,
) {
    let socket_id = SocketId::generate();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = ConnectionHandle {
        socket_id: socket_id.clone(),
        mailbox: tx,
    };
    let out = local.subscribe(TEST_APP, channel, handle, None).await;
    (socket_id, out.subscription_count, rx)
}

/// Drain `rx` until a `SubscriptionCount` frame for `channel` is observed (parsing the
/// registry-mailbox `Raw` frame), returning its `count`, or `None` within `timeout`.
async fn await_subscription_count(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<pylon::protocol::event::ServerEvent>,
    channel: &str,
    timeout: Duration,
) -> Option<u64> {
    let fut = async {
        loop {
            match rx.recv().await {
                Some(pylon::protocol::event::ServerEvent::Raw(frame)) => {
                    let v: Value = match serde_json::from_str(&frame) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    // v7 shape: { "event": "...:subscription_count", "channel": "<ch>",
                    //             "data": "{\"subscription_count\":<n>}" } — channel is at
                    // the TOP level; `data` is a double-encoded JSON string.
                    if v.get("event").and_then(|e| e.as_str())
                        == Some("pusher_internal:subscription_count")
                        && v.get("channel").and_then(|c| c.as_str()) == Some(channel)
                    {
                        let inner: Value = match v.get("data") {
                            Some(Value::String(s)) => {
                                serde_json::from_str(s).unwrap_or(Value::Null)
                            }
                            Some(other) => other.clone(),
                            None => Value::Null,
                        };
                        return inner.get("subscription_count").and_then(|c| c.as_u64());
                    }
                }
                Some(_) => continue,
                None => return None,
            }
        }
    };
    tokio::time::timeout(timeout, fut).await.ok().flatten()
}

/// Short timeout wrapper so a wedged Redis fails loud instead of hanging the suite.
async fn with_timeout<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(Duration::from_secs(4), fut)
        .await
        .expect("op must not hang (Redis up?)")
}

/// Spin up one cluster "node": its own shared `LocalAdapter`, a recording webhook
/// dispatcher, and a `ClusterBridge` sharing `prefix`. Returns the pieces the test
/// drives. The bridge owns the node's single `RedisAdapter`.
struct Node {
    bridge: ClusterBridge,
    local: Arc<LocalAdapter>,
    transport: RecordingTransport,
}

fn start_node(prefix: &str, apps: Arc<dyn AppManager>) -> Node {
    let cfg = redis_test_config(prefix);
    let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));
    let (webhooks, transport) = recording_webhooks(apps.clone());
    let bridge = bridge::start(&cfg, local.clone(), webhooks, apps)
        .expect("ClusterBridge::start must connect to the test Redis and report ready");
    Node {
        bridge,
        local,
        transport,
    }
}

/// Test A — clustered count + occupied, single node. After a node-local subscribe on a
/// public channel, firing `handle.subscribe(.., node_first=true)` must make the bridge
/// (1) broadcast `subscription_count == 1` to the node-local fake subscriber and (2)
/// fire `channel_occupied` exactly once.
#[tokio::test]
async fn clustered_count_and_occupied_single_node() {
    with_timeout(async {
        let prefix = random_prefix();
        let apps = apps_manager();
        let node = start_node(&prefix, apps);

        let channel = "my-chan";
        // A node-local subscriber: drives the registry-mailbox delivery AND gives us
        // the node_first edge to pass to the bridge.
        let (sid, local_count, mut rx) = fake_subscriber(&node.local, channel).await;
        assert_eq!(local_count, 1, "first node-local subscriber → local count 1");

        // Fire the fire-and-forget Subscribe the percore ClusterAdapter would fire.
        node.bridge
            .handle()
            .subscribe(Arc::from(TEST_APP), Arc::from(channel), sid, true);

        // The bridge broadcasts the cluster subscription_count to the fake subscriber.
        let count = await_subscription_count(&mut rx, channel, Duration::from_secs(3)).await;
        assert_eq!(
            count,
            Some(1),
            "bridge must broadcast cluster subscription_count == 1"
        );

        // And channel_occupied fired exactly once (settle the batch window first).
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            count_webhook(&node.transport, "channel_occupied").await,
            1,
            "channel_occupied must fire exactly once on the cluster 0→1 edge"
        );
        assert_eq!(
            count_webhook(&node.transport, "channel_vacated").await,
            0,
            "no vacated yet"
        );

        drop(node);
    })
    .await;
}

/// Test B — cross-node count + single occupied emit. Node A subscribes a member
/// (cluster count 1, occupied once); node B subscribes another (its bridge broadcasts
/// cluster count 2 to ITS local subscriber). `channel_occupied` must fire EXACTLY ONCE
/// across both nodes' webhook sinks (single cluster-wide emit), and the cluster count
/// must reach 2.
#[tokio::test]
async fn cross_node_count_and_single_occupied_emit() {
    with_timeout(async {
        let prefix = random_prefix();
        let apps = apps_manager();
        let node_a = start_node(&prefix, apps.clone());
        let node_b = start_node(&prefix, apps.clone());

        let channel = "my-chan";

        // Node A: first cluster subscriber → count 1, occupied once.
        let (sid_a, ca, mut rx_a) = fake_subscriber(&node_a.local, channel).await;
        assert_eq!(ca, 1, "A first node-local subscriber → local count 1");
        node_a
            .bridge
            .handle()
            .subscribe(Arc::from(TEST_APP), Arc::from(channel), sid_a, true);
        let count_a = await_subscription_count(&mut rx_a, channel, Duration::from_secs(3)).await;
        assert_eq!(count_a, Some(1), "A's bridge broadcasts cluster count 1");

        // Node B: a SECOND cluster subscriber on a DIFFERENT node → cluster count 2,
        // and NOT a 0→1 cluster edge (occupied must NOT fire again).
        let (sid_b, cb, mut rx_b) = fake_subscriber(&node_b.local, channel).await;
        assert_eq!(cb, 1, "B first node-local subscriber → its local count 1");
        node_b
            .bridge
            .handle()
            .subscribe(Arc::from(TEST_APP), Arc::from(channel), sid_b, true);
        let count_b = await_subscription_count(&mut rx_b, channel, Duration::from_secs(3)).await;
        assert_eq!(
            count_b,
            Some(2),
            "B's bridge broadcasts the CLUSTER count 2 (not B's node-local 1)"
        );

        // Settle the batch windows, then assert occupied fired exactly once across BOTH
        // nodes' sinks (single cluster-wide emit on the cluster 0→1 edge).
        tokio::time::sleep(Duration::from_millis(200)).await;
        let occ_a = count_webhook(&node_a.transport, "channel_occupied").await;
        let occ_b = count_webhook(&node_b.transport, "channel_occupied").await;
        assert_eq!(
            occ_a + occ_b,
            1,
            "channel_occupied must fire EXACTLY ONCE cluster-wide (A={occ_a}, B={occ_b})"
        );

        drop(node_a);
        drop(node_b);
    })
    .await;
}

/// Test C — vacated single-emit. With one member on each node, unsubscribe both: the
/// non-cluster-last unsubscribe must NOT vacate; the cluster-last (count → 0) must fire
/// `channel_vacated` exactly once across both nodes.
#[tokio::test]
async fn cross_node_vacated_single_emit() {
    with_timeout(async {
        let prefix = random_prefix();
        let apps = apps_manager();
        let node_a = start_node(&prefix, apps.clone());
        let node_b = start_node(&prefix, apps.clone());

        let channel = "my-chan";

        // Bring the channel to cluster count 2 (one member per node).
        let (sid_a, _ca, mut rx_a) = fake_subscriber(&node_a.local, channel).await;
        node_a
            .bridge
            .handle()
            .subscribe(Arc::from(TEST_APP), Arc::from(channel), sid_a.clone(), true);
        assert_eq!(
            await_subscription_count(&mut rx_a, channel, Duration::from_secs(3)).await,
            Some(1)
        );

        let (sid_b, _cb, mut rx_b) = fake_subscriber(&node_b.local, channel).await;
        node_b
            .bridge
            .handle()
            .subscribe(Arc::from(TEST_APP), Arc::from(channel), sid_b.clone(), true);
        assert_eq!(
            await_subscription_count(&mut rx_b, channel, Duration::from_secs(3)).await,
            Some(2)
        );

        // Unsubscribe A's member → node_last=true locally, but cluster count → 1, NOT
        // vacated. The bridge broadcasts count 1 to A's local subscriber? No — A's fake
        // subscriber was removed from the local registry below; we observe the count on
        // B's subscriber after B's own unsubscribe instead. Here we just drive the edge.
        let un_a = node_a.local.unsubscribe(TEST_APP, channel, &sid_a).await;
        node_a.bridge.handle().unsubscribe(
            Arc::from(TEST_APP),
            Arc::from(channel),
            sid_a,
            un_a.subscription_count == 0,
        );

        // Unsubscribe B's member → cluster count → 0 → vacated.
        let un_b = node_b.local.unsubscribe(TEST_APP, channel, &sid_b).await;
        node_b.bridge.handle().unsubscribe(
            Arc::from(TEST_APP),
            Arc::from(channel),
            sid_b,
            un_b.subscription_count == 0,
        );

        // Settle the batch windows, then assert vacated fired EXACTLY ONCE cluster-wide.
        tokio::time::sleep(Duration::from_millis(250)).await;
        let vac_a = count_webhook(&node_a.transport, "channel_vacated").await;
        let vac_b = count_webhook(&node_b.transport, "channel_vacated").await;
        assert_eq!(
            vac_a + vac_b,
            1,
            "channel_vacated must fire EXACTLY ONCE cluster-wide (A={vac_a}, B={vac_b})"
        );

        drop(node_a);
        drop(node_b);
    })
    .await;
}
