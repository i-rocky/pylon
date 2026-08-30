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
    std::env::var("PYLON_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6390".to_string())
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
    apps_manager_with_event_types(r#"["channel_occupied","channel_vacated"]"#)
}

/// Like [`apps_manager`] but with the caller's `event_types` JSON array — the
/// count-webhook test adds `subscription_count` to the operator opt-in.
fn apps_manager_with_event_types(event_types: &str) -> Arc<dyn AppManager> {
    let raw = format!(
        r#"[{{"name":"Test","id":"app","key":"app-key","secret":"app-secret",
         "subscription_count_enabled":true,
         "webhooks":[{{"url":"http://127.0.0.1:1/pusher/webhooks",
                      "event_types":{event_types}}}]}}]"#
    );
    Arc::new(StaticFileAppManager::from_json(&raw).expect("apps json must parse"))
}

/// A real webhook dispatcher backed by a `RecordingTransport`, so the bridge's
/// `webhooks.enqueue(...)` is signed/batched exactly as in production but captured in
/// memory. Returns the `WebhookHandle` (handed to the bridge) and the transport (to
/// read back the recorded deliveries). A tiny batch window keeps the test fast.
fn recording_webhooks(apps: Arc<dyn AppManager>) -> (WebhookHandle, RecordingTransport) {
    let transport = RecordingTransport::new();
    let recorded = transport.clone();
    let handle = pylon::webhook::spawn(
        apps,
        // RecordingTransport doesn't count outcomes; it ignores the metrics.
        move |_metrics| {
            Ok::<_, std::convert::Infallible>(Arc::new(recorded) as Arc<dyn WebhookTransport>)
        },
        Arc::new(SystemClock),
        10,   // 10ms batch window
        1024, // mailbox capacity
        0,    // vacated fires immediately (no cluster grace in this test)
        None, // no cluster occupancy source
    )
    .expect("recording transport factory is infallible");
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

/// The `subscription_count` VALUES (in delivery order) recorded on a transport —
/// the per-node observation for the count-webhook test below.
async fn subscription_count_values(transport: &RecordingTransport) -> Vec<u64> {
    let mut out = Vec::new();
    for d in transport.recorded().await {
        let v: Value = serde_json::from_str(&d.body).expect("webhook body must be JSON");
        if let Some(events) = v.get("events").and_then(|e| e.as_array()) {
            for e in events {
                if e.get("name").and_then(|x| x.as_str()) == Some("subscription_count") {
                    out.push(
                        e.get("subscription_count")
                            .and_then(|c| c.as_u64())
                            .expect("subscription_count must be a JSON number"),
                    );
                }
            }
        }
    }
    out
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
    pylon::connection::handle::Mailbox,
    tokio::sync::mpsc::Receiver<Box<pylon::protocol::event::ServerEvent>>,
) {
    let socket_id = SocketId::generate();
    let (tx, rx) = tokio::sync::mpsc::channel(1024);
    // No notifier in these bridge tests: they `try_recv` the `rx` directly, so the
    // `Mailbox` just forwards `send` (no wake).
    let mailbox = pylon::connection::handle::Mailbox::new(tx, None, None);
    let handle = ConnectionHandle {
        socket_id,
        mailbox: mailbox.clone(),
    };
    let out = local.subscribe(TEST_APP, channel, handle, None).await;
    (socket_id, out.subscription_count, mailbox, rx)
}

/// Poll `pred` every ~10ms until it returns `true` or `timeout` elapses. The
/// event-based wait for this suite's webhook assertions: poll the observable
/// (the recorded webhook count) instead of sleeping for a guessed settle time.
/// (The WS-driving suites' equivalent lives in `tests/common/mod.rs::wait_until`;
/// this suite is self-contained by design.)
async fn wait_until<F, Fut>(timeout: Duration, mut pred: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if pred().await {
            return true;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return false;
        }
        tokio::time::sleep(remaining.min(Duration::from_millis(10))).await;
    }
}

/// Drain `rx` until a `SubscriptionCount` frame for `channel` reporting `want` is
/// observed (parsing the registry-mailbox `Raw` frame), skipping earlier/smaller
/// counts; bounded by `timeout`. Returns whether the wanted count arrived.
///
/// Skipping earlier counts is REQUIRED, not a convenience: the bridge's count
/// broadcast fans out node-locally AND via Redis, so a remote node's EARLIER
/// count (e.g. node A's 1, delivered to B's mailbox through B's recv loop) can
/// arrive before B's own local count-2 delivery — the order is nondeterministic,
/// and asserting on the FIRST frame raced (observed as a flaky `Some(1)` vs
/// `Some(2)`). If B only ever broadcast its node-local count, `want` never
/// arrives and the assert still fails — identical semantics.
async fn await_subscription_count(
    rx: &mut tokio::sync::mpsc::Receiver<Box<pylon::protocol::event::ServerEvent>>,
    channel: &str,
    want: u64,
    timeout: Duration,
) -> bool {
    let fut = async {
        loop {
            match rx.recv().await.map(|b| *b) {
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
                        if inner.get("subscription_count").and_then(|c| c.as_u64()) == Some(want) {
                            return true;
                        }
                    }
                }
                Some(_) => continue,
                None => return false,
            }
        }
    };
    tokio::time::timeout(timeout, fut).await.unwrap_or(false)
}

/// Short timeout wrapper so a wedged Redis fails loud instead of hanging the suite.
/// Sized ABOVE the sum of the slowest path's per-stage budgets (Test C: two 10s
/// count awaits + 10s occupied-delivery gate + 10s vacated settle + 1s
/// duplicate-exposure window ≈ 41s) so the anti-hang bound never clips a
/// legitimately slow (shared-runner) delivery chain — it exists to fail loud on
/// a WEDGED Redis, not to pace the test. A stage that exhausts its own budget
/// fails its assert immediately; only passing (or slowly-passing) runs reach
/// this bound.
async fn with_timeout<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(Duration::from_secs(60), fut)
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
    let local = Arc::new(LocalAdapter::new(
        Arc::new(Registry::new()),
        Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
    ));
    let (webhooks, transport) = recording_webhooks(apps.clone());
    // No worker fleet backs these bridge-only nodes → an empty counter map is the
    // correct `conn_counts` (see the Task 4.2 heartbeat outage re-seed).
    let bridge = bridge::start(&cfg, local.clone(), apps, Arc::new(Default::default()))
        .expect("ClusterBridge::start must connect to the test Redis and report ready");
    bridge.attach_webhooks(webhooks);
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
        let (sid, local_count, mailbox, mut rx) = fake_subscriber(&node.local, channel).await;
        assert_eq!(
            local_count, 1,
            "first node-local subscriber → local count 1"
        );

        // Fire the fire-and-forget Subscribe the percore ClusterAdapter would fire.
        node.bridge
            .handle()
            .subscribe(Arc::from(TEST_APP), Arc::from(channel), sid, mailbox, true);

        // The bridge broadcasts the cluster subscription_count to the fake subscriber.
        assert!(
            await_subscription_count(&mut rx, channel, 1, WEBHOOK_CHAIN_BUDGET).await,
            "bridge must broadcast cluster subscription_count == 1"
        );

        // And channel_occupied fired exactly once: poll the recording transport
        // until the webhook is DELIVERED (the 10ms batch window + flush are
        // asynchronous) instead of sleeping for a guessed settle time.
        assert!(
            wait_until(WEBHOOK_CHAIN_BUDGET, || async {
                count_webhook(&node.transport, "channel_occupied").await >= 1
            })
            .await,
            "channel_occupied must fire on the cluster 0→1 edge"
        );
        // Duplicate-exposure window: hold for a further bounded 1s AFTER the
        // first delivery so an illegal second emit (same chain, observed p99
        // ≤75ms even under 10-core starvation) would be caught — then assert.
        tokio::time::sleep(DUPLICATE_EXPOSURE_WINDOW).await;
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
        let (sid_a, ca, mailbox_a, mut rx_a) = fake_subscriber(&node_a.local, channel).await;
        assert_eq!(ca, 1, "A first node-local subscriber → local count 1");
        node_a.bridge.handle().subscribe(
            Arc::from(TEST_APP),
            Arc::from(channel),
            sid_a,
            mailbox_a,
            true,
        );
        assert!(
            await_subscription_count(&mut rx_a, channel, 1, WEBHOOK_CHAIN_BUDGET).await,
            "A's bridge broadcasts cluster count 1"
        );

        // Node B: a SECOND cluster subscriber on a DIFFERENT node → cluster count 2,
        // and NOT a 0→1 cluster edge (occupied must NOT fire again).
        let (sid_b, cb, mailbox_b, mut rx_b) = fake_subscriber(&node_b.local, channel).await;
        assert_eq!(cb, 1, "B first node-local subscriber → its local count 1");
        node_b.bridge.handle().subscribe(
            Arc::from(TEST_APP),
            Arc::from(channel),
            sid_b,
            mailbox_b,
            true,
        );
        assert!(
            await_subscription_count(&mut rx_b, channel, 2, WEBHOOK_CHAIN_BUDGET).await,
            "B's bridge broadcasts the CLUSTER count 2 (not B's node-local 1)"
        );

        // Poll until occupied is delivered somewhere, then assert EXACTLY once
        // across BOTH nodes' sinks (single cluster-wide emit on the 0→1 edge).
        assert!(
            wait_until(WEBHOOK_CHAIN_BUDGET, || async {
                count_webhook(&node_a.transport, "channel_occupied").await
                    + count_webhook(&node_b.transport, "channel_occupied").await
                    >= 1
            })
            .await,
            "channel_occupied must fire somewhere cluster-wide"
        );
        // Duplicate-exposure window (see Test A): catch an illegal second emit.
        tokio::time::sleep(DUPLICATE_EXPOSURE_WINDOW).await;
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

/// Budget for one hop of the fire-and-forget delivery chain this suite observes
/// (bridge cmd → Redis script → webhook enqueue → dispatcher 10ms batch → flush →
/// recording transport, or the analogous mailbox path for subscription_count).
/// Locally the chain measures 22-75ms end-to-end even under 10-core CPU
/// starvation (p99 ≤75ms), but CI's shared 2-vCPU runner + service-container
/// Redis stretches every hop; 10s is ≥130× the observed starved p99 —
/// generous-but-bounded, and small next to the anti-hang `with_timeout`.
///
/// NOTE this budget alone does NOT make a `channel_vacated` "arrive eventually":
/// per webhook spec §5, a `channel_occupied` and a `channel_vacated` for the same
/// channel that land in ONE dispatcher batch window CANCEL 1:1 and are never
/// delivered (proven deterministic: see the occupied-delivery gate in Test C and
/// CI run 33303526290, where zero vacated webhooks appeared in the whole budget).
const WEBHOOK_CHAIN_BUDGET: Duration = Duration::from_secs(10);

/// How long to keep observing AFTER the first webhook delivery before asserting
/// exactly-once — a deliberate duplicate-EXPOSURE window (an illegal second emit
/// rides the same chain, observed p99 ≤75ms, so 1s ≈ 13× p99), not a settle
/// sleep racing an async producer.
const DUPLICATE_EXPOSURE_WINDOW: Duration = Duration::from_secs(1);

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
        let (sid_a, _ca, mailbox_a, mut rx_a) = fake_subscriber(&node_a.local, channel).await;
        node_a.bridge.handle().subscribe(
            Arc::from(TEST_APP),
            Arc::from(channel),
            sid_a,
            mailbox_a,
            true,
        );
        assert!(
            await_subscription_count(&mut rx_a, channel, 1, WEBHOOK_CHAIN_BUDGET).await,
            "A's bridge broadcasts cluster count 1"
        );

        let (sid_b, _cb, mailbox_b, mut rx_b) = fake_subscriber(&node_b.local, channel).await;
        node_b.bridge.handle().subscribe(
            Arc::from(TEST_APP),
            Arc::from(channel),
            sid_b,
            mailbox_b,
            true,
        );
        assert!(
            await_subscription_count(&mut rx_b, channel, 2, WEBHOOK_CHAIN_BUDGET).await,
            "B's bridge broadcasts the CLUSTER count 2"
        );

        // PRECONDITION GATE (the actual fix for CI run 33303526290's zero-delivery
        // failure): wait until A's `channel_occupied` webhook (fired by A's
        // subscribe, the cluster 0→1 edge) has been DELIVERED by A's dispatcher
        // before firing any unsubscribe. Historically this gated against the
        // batch coalescer cancelling an occupied+vacated pair sharing one
        // 10ms window; that coalescing was removed for Pusher parity (audit
        // R12a — the doc's vacate delay + reconnect-only suppression implies
        // BOTH sides of a create-and-vacate are delivered), so the cancellation
        // hazard is gone. The gate is kept as belt-and-suspenders: with the
        // occupied DELIVERED, A's batch is flushed and empty, so whichever node
        // later wins the vacate lands in a fresh batch — isolating the
        // exactly-once assertion from dispatcher window timing entirely. This
        // gates the test's precondition; it changes no assertion.
        assert!(
            wait_until(WEBHOOK_CHAIN_BUDGET, || async {
                count_webhook(&node_a.transport, "channel_occupied").await >= 1
            })
            .await,
            "A's channel_occupied (cluster 0→1 edge) must be delivered before the unsubscribes"
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

        // Poll until vacated is delivered somewhere (the batch window + flush are
        // asynchronous; generous slow-runner budget — see WEBHOOK_CHAIN_BUDGET),
        // then assert EXACTLY once cluster-wide — the vacate-CAS guarantee: only
        // the atomic SREM winner emits.
        assert!(
            wait_until(WEBHOOK_CHAIN_BUDGET, || async {
                count_webhook(&node_a.transport, "channel_vacated").await
                    + count_webhook(&node_b.transport, "channel_vacated").await
                    >= 1
            })
            .await,
            "channel_vacated must fire somewhere cluster-wide"
        );
        // Duplicate-exposure window (see Test A): a second, CAS-losing emit would
        // ride the same chain; hold 1s to expose it before asserting exactly-once.
        tokio::time::sleep(DUPLICATE_EXPOSURE_WINDOW).await;
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

/// Test D — `subscription_count` WEBHOOK on the cluster path (Task 2.5 / audit
/// R6; verified against https://pusher.com/docs/channels/server_api/webhooks/,
/// 2026-08-30: "Channels will send a subscription_count webhook whenever a new
/// client subscribes or unsubscribes to a channel"). The bridge owns the
/// cluster-authoritative count (Task 0.2), so the webhook fires from the SAME
/// `ClusterCmd::Subscribe` / `Unsubscribe` arms that broadcast the count —
/// emitted on the node whose bridge computed the count, never duplicated. With
/// one member per node: A subscribes → A's transport sees count 1; B subscribes
/// → B's transport sees the CLUSTER count 2; B unsubscribes → B sees 1; A
/// unsubscribes (cluster 1→0) → NO count webhook anywhere (the arm's `count > 0`
/// guard — `channel_vacated` is the vacancy signal), and `channel_vacated`
/// fires exactly once cluster-wide.
#[tokio::test]
async fn cluster_subscription_count_webhook_follows_bridge_owned_counts() {
    with_timeout(async {
        let prefix = random_prefix();
        let apps = apps_manager_with_event_types(
            r#"["channel_occupied","channel_vacated","subscription_count"]"#,
        );
        let node_a = start_node(&prefix, apps.clone());
        let node_b = start_node(&prefix, apps.clone());

        let channel = "count-webhook-chan";

        // A subscribes (cluster 0→1): A's bridge owns the edge → A's transport
        // gets subscription_count == 1 (B's gets nothing).
        let (sid_a, ca, mailbox_a, _rx_a) = fake_subscriber(&node_a.local, channel).await;
        assert_eq!(ca, 1);
        node_a.bridge.handle().subscribe(
            Arc::from(TEST_APP),
            Arc::from(channel),
            sid_a,
            mailbox_a,
            true,
        );
        assert!(
            wait_until(WEBHOOK_CHAIN_BUDGET, || async {
                subscription_count_values(&node_a.transport).await == vec![1]
            })
            .await,
            "A's bridge must emit the subscription_count webhook with the cluster count 1"
        );
        assert_eq!(
            subscription_count_values(&node_b.transport).await,
            Vec::<u64>::new(),
            "no count webhook on B for A's edge"
        );

        // B subscribes (cluster 1→2): B's bridge computed the authoritative
        // CLUSTER count → B's transport gets 2 (not B's node-local 1).
        let (sid_b, cb, mailbox_b, _rx_b) = fake_subscriber(&node_b.local, channel).await;
        assert_eq!(cb, 1, "B first node-local subscriber → its local count 1");
        node_b.bridge.handle().subscribe(
            Arc::from(TEST_APP),
            Arc::from(channel),
            sid_b,
            mailbox_b,
            true,
        );
        assert!(
            wait_until(WEBHOOK_CHAIN_BUDGET, || async {
                subscription_count_values(&node_b.transport).await == vec![2]
            })
            .await,
            "B's bridge must emit the subscription_count webhook with the CLUSTER count 2"
        );

        // B unsubscribes (cluster 2→1): B's Unsubscribe arm owns the remaining
        // count → B's transport appends 1.
        let un_b = node_b.local.unsubscribe(TEST_APP, channel, &sid_b).await;
        node_b.bridge.handle().unsubscribe(
            Arc::from(TEST_APP),
            Arc::from(channel),
            sid_b,
            un_b.subscription_count == 0,
        );
        assert!(
            wait_until(WEBHOOK_CHAIN_BUDGET, || async {
                subscription_count_values(&node_b.transport).await == vec![2, 1]
            })
            .await,
            "B's bridge must emit the remaining cluster count 1 on unsubscribe"
        );

        // A unsubscribes (cluster 1→0): count 0 → NO count webhook anywhere; the
        // vacate-CAS winner (A) fires channel_vacated exactly once instead.
        let un_a = node_a.local.unsubscribe(TEST_APP, channel, &sid_a).await;
        node_a.bridge.handle().unsubscribe(
            Arc::from(TEST_APP),
            Arc::from(channel),
            sid_a,
            un_a.subscription_count == 0,
        );
        assert!(
            wait_until(WEBHOOK_CHAIN_BUDGET, || async {
                count_webhook(&node_a.transport, "channel_vacated").await
                    + count_webhook(&node_b.transport, "channel_vacated").await
                    >= 1
            })
            .await,
            "channel_vacated must fire on the cluster 1→0 edge"
        );
        // Duplicate-exposure window: an illegal extra count emit (or a second
        // vacated) rides the same chain — hold, then pin the final picture.
        tokio::time::sleep(DUPLICATE_EXPOSURE_WINDOW).await;
        assert_eq!(
            subscription_count_values(&node_a.transport).await,
            vec![1],
            "A's count webhooks: exactly one, with the cluster count 1"
        );
        assert_eq!(
            subscription_count_values(&node_b.transport).await,
            vec![2, 1],
            "B's count webhooks: cluster counts 2 then 1"
        );
        assert!(
            !subscription_count_values(&node_a.transport)
                .await
                .into_iter()
                .chain(subscription_count_values(&node_b.transport).await)
                .any(|c| c == 0),
            "no zero-count webhook on the vacate edge (count > 0 guard)"
        );
        let vac_a = count_webhook(&node_a.transport, "channel_vacated").await;
        let vac_b = count_webhook(&node_b.transport, "channel_vacated").await;
        assert_eq!(
            vac_a + vac_b,
            1,
            "channel_vacated exactly once cluster-wide"
        );

        drop(node_a);
        drop(node_b);
    })
    .await;
}
