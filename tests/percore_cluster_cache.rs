//! End-to-end 2-node clustered-percore proof for CACHE channels (SP11).
//!
//! Cache channels (`cache-*`) retain their last published event so a new subscriber
//! is replayed it (or told `pusher:cache_miss` when empty). In cluster mode the cache
//! is Redis-backed: a REST publish on ANY node writes the cluster cache via that
//! node's `RedisAdapter::cache_set`. A percore worker, however, drives a
//! `ClusterAdapter` whose `cache_get` is node-LOCAL — so the cluster cache replay for
//! a subscribing connection is done by the bridge's `ClusterCmd::Subscribe` arm
//! (which reads the node's `RedisAdapter` and delivers the replay / miss to the
//! connection's mailbox). These tests prove that cross-node behaviour with two REAL
//! percore nodes sharing one Redis key prefix.
//!
//! Like `percore_cluster.rs` / `redis_cluster.rs`, they talk to a REAL Redis
//! (`PYLON_TEST_REDIS_URL`, default `redis://127.0.0.1:6390`) and isolate every run
//! behind a random key prefix — they NEVER issue FLUSHALL/FLUSHDB. Cross-node delivery
//! rides Redis pub/sub latency, so every assertion is BOUNDED by the WS helpers'
//! per-frame timeout (never masked by a long unconditional sleep).

mod common;

use common::{
    connect, established_socket_id, next_event_named, next_json, send_json,
    spawn_percore_cluster, Ws, KEY, SECRET,
};
use pylon::auth::signature::{hmac_sha256_hex, md5_hex};
use serde_json::json;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use uuid::Uuid;

/// The common harness `APPS` app id (key `app-key`, secret `app-secret`).
const APP_ID: &str = "app";

/// A random, run-unique key prefix so the two nodes share one cluster namespace
/// without ever clobbering a shared Redis.
fn random_prefix() -> String {
    format!("pylontest:{}", Uuid::new_v4())
}

/// Build the signed Pusher REST query string for a request (HMAC-SHA256 over
/// `METHOD\npath\ncanonical`, with a `body_md5`). Mirrors `percore_cluster.rs`.
fn signed_query(method: &str, path: &str, body: &[u8]) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut p: BTreeMap<String, String> = BTreeMap::new();
    p.insert("auth_key".into(), KEY.into());
    p.insert("auth_timestamp".into(), now.to_string());
    p.insert("auth_version".into(), "1.0".into());
    if !body.is_empty() {
        p.insert("body_md5".into(), md5_hex(body));
    }
    let canon = p
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let signed = format!("{}\n{}\n{}", method.to_uppercase(), path, canon);
    let sig = hmac_sha256_hex(SECRET, &signed);
    format!("{canon}&auth_signature={sig}")
}

/// POST a signed Pusher event to `addr`'s REST `/apps/{APP_ID}/events`, asserting a
/// 200. `data` is the event payload as a JSON STRING (the Pusher REST `data` field) —
/// exactly what a cache channel retains and later replays verbatim.
async fn publish_event(addr: SocketAddr, name: &str, channel: &str, data: &str) {
    let path = format!("/apps/{APP_ID}/events");
    let body = json!({ "name": name, "data": data, "channels": [channel] }).to_string();
    let q = signed_query("POST", &path, body.as_bytes());
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}{path}?{q}"))
        .body(body)
        .send()
        .await
        .expect("REST publish request must reach the node");
    assert_eq!(
        resp.status(),
        200,
        "REST publish must be accepted (200); got {}",
        resp.status()
    );
}

/// Connect a WS client to `addr`, drain its `connection_established`, then subscribe
/// to the public cache `channel`. Returns the live socket positioned right AFTER the
/// subscribe send, so the test can assert the exact post-subscribe frame ORDER
/// (`subscription_succeeded` THEN the cache replay / miss).
async fn connect_and_subscribe_cache(addr: SocketAddr, channel: &str) -> Ws {
    let mut ws = connect(addr, "?protocol=7").await;
    let _socket_id = established_socket_id(&mut ws).await;
    send_json(
        &mut ws,
        json!({ "event": "pusher:subscribe", "data": { "channel": channel } }),
    )
    .await;
    ws
}

/// A cache event published on node A is replayed (cross-node) to a subscriber that
/// joins the SAME cache channel on node B. Pusher ordering: the subscriber must see
/// `subscription_succeeded` FIRST, THEN the replayed `ChannelEvent` (the bridge reads
/// the Redis cache and delivers it to the connection's mailbox). The replayed frame
/// carries the published event name + verbatim data.
#[tokio::test]
async fn cross_node_cache_replay() {
    let prefix = random_prefix();
    // Two real percore nodes on one shared Redis prefix → a 2-node cluster.
    let (addr_a, _guard_a) = spawn_percore_cluster(&prefix).await;
    let (addr_b, _guard_b) = spawn_percore_cluster(&prefix).await;

    let channel = "cache-news";
    let payload = "{\"headline\":\"cluster\"}";

    // Publish a cache event via node A's REST endpoint → writes the Redis cache that
    // every node (and its bridge) reads. No subscriber needed for the cache write.
    publish_event(addr_a, "breaking", channel, payload).await;

    // A subscriber joins the SAME cache channel on node B. The handler sends
    // `subscription_succeeded` inline; the bridge then reads the CLUSTER cache and
    // delivers the replay to this connection's mailbox.
    let mut ws_b = connect_and_subscribe_cache(addr_b, channel).await;

    // Ordering: subscription_succeeded MUST precede the cache replay (the inline frame
    // is drained before the async bridge mailbox frame).
    let succeeded = next_json(&mut ws_b).await;
    assert_eq!(
        succeeded["event"], "pusher_internal:subscription_succeeded",
        "first post-subscribe frame must be subscription_succeeded"
    );
    assert_eq!(succeeded["channel"], channel, "succeeded names the channel");

    // Then the cross-node cache replay: the bridge read the Redis cache populated by
    // node A's publish and delivered it to B's subscriber. `next_event_named` is
    // timeout-bounded by the helper, so a missing replay fails loud (not a hang).
    let replay = next_event_named(&mut ws_b, "breaking").await;
    assert_eq!(replay["channel"], channel, "replay names the channel");
    assert_eq!(
        replay["data"], payload,
        "replay must carry the cross-node published payload verbatim"
    );
}

/// Subscribing a FRESH cache channel cluster-wide (nothing published anywhere) yields
/// `subscription_succeeded` THEN `pusher:cache_miss` — the bridge read the empty Redis
/// cache and delivered the miss to the connection's mailbox.
#[tokio::test]
async fn cross_node_cache_miss() {
    let prefix = random_prefix();
    let (addr_a, _guard_a) = spawn_percore_cluster(&prefix).await;
    let (addr_b, _guard_b) = spawn_percore_cluster(&prefix).await;

    // A run-unique fresh cache channel → guaranteed nothing cached cluster-wide. The
    // `_addr_a` node is part of the cluster but unused here (proves the miss is real,
    // not a node-isolation artefact).
    let _ = addr_a;
    let channel = format!("cache-empty-{}", Uuid::new_v4());

    let mut ws_b = connect_and_subscribe_cache(addr_b, &channel).await;

    let succeeded = next_json(&mut ws_b).await;
    assert_eq!(
        succeeded["event"], "pusher_internal:subscription_succeeded",
        "first post-subscribe frame must be subscription_succeeded"
    );
    assert_eq!(succeeded["channel"], channel, "succeeded names the channel");

    // Then the cache miss (the bridge read an empty cluster cache).
    let miss = next_event_named(&mut ws_b, "pusher:cache_miss").await;
    assert_eq!(
        miss["channel"], channel,
        "cache_miss must name the subscribed channel"
    );
}
