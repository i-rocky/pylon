//! End-to-end 2-node clustered-percore PRESENCE proof (SP11 §3.4a gate).
//!
//! Two REAL percore nodes (each a single-worker `mio` fleet + a [`ClusterBridge`]
//! owning that node's `RedisAdapter`) are spawned on ONE shared Redis key prefix,
//! forming a 2-node cluster. The tests connect real WS presence clients to each
//! node and prove the cluster-wide presence happy path rides through Redis:
//!
//! * `cross_node_presence_roster_is_cluster_wide` — a presence subscribe on node B
//!   sees a `subscription_succeeded` roster that contains BOTH the member on node A
//!   and itself (the bridge's `cluster_presence_join` returns the cluster roster).
//! * `cross_node_presence_member_added_single_emit` — when a presence member joins
//!   on node B, an already-subscribed member on node A receives EXACTLY ONE
//!   `pusher_internal:member_added` (cross-node, single-emit on the cluster
//!   first-for-user edge — never duplicated).
//! * `cross_node_presence_member_removed_single_emit` — when the member on node B
//!   leaves, the member on node A receives EXACTLY ONE `member_removed`.
//!
//! Like `percore_cluster.rs`, these talk to a REAL Redis (`PYLON_TEST_REDIS_URL`,
//! default `redis://127.0.0.1:6390`) and isolate every run behind a random key
//! prefix — they NEVER issue FLUSHALL/FLUSHDB. They FAIL LOUD if Redis is
//! unreachable (the bridge `start` panics with a clear message).
//!
//! Cross-node delivery has Redis pub/sub latency, so every cross-node assertion is
//! BOUNDED by a timeout (the shared WS helpers' built-in per-frame timeout plus an
//! explicit poll loop), never masked by a long unconditional sleep.

mod common;

use common::{
    auth_token, connect, established_socket_id, next_event_named, next_json, send_json,
    spawn_percore_cluster, Ws,
};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::time::Duration;
use uuid::Uuid;

/// A random, run-unique key prefix so the two nodes share one cluster namespace
/// without ever clobbering a shared Redis.
fn random_prefix() -> String {
    format!("pylontest:{}", Uuid::new_v4())
}

/// Connect a WS presence client to `addr` as `user_id` (with `user_info`), drain its
/// `connection_established`, subscribe it to `channel`, and return the live socket
/// plus its own `subscription_succeeded` frame (already read off the wire). The
/// caller inspects the returned frame's roster and then reads further frames.
async fn connect_presence(
    addr: SocketAddr,
    channel: &str,
    user_id: &str,
    user_info: Value,
) -> (Ws, Value) {
    let mut ws = connect(addr, "?protocol=7").await;
    let sid = established_socket_id(&mut ws).await;
    let channel_data = json!({ "user_id": user_id, "user_info": user_info }).to_string();
    send_json(
        &mut ws,
        json!({
            "event": "pusher:subscribe",
            "data": {
                "channel": channel,
                "auth": auth_token(&sid, channel, Some(&channel_data)),
                "channel_data": channel_data
            }
        }),
    )
    .await;
    // The clustered presence path delivers `subscription_succeeded` from the bridge
    // (carrying the CLUSTER roster) — bounded by `next_event_named`'s per-frame timeout.
    let succeeded = next_event_named(&mut ws, "pusher_internal:subscription_succeeded").await;
    (ws, succeeded)
}

/// Decode a presence `subscription_succeeded`'s double-encoded `data` STRING into the
/// inner `presence` object `{ ids, hash, count }`.
fn roster_of(succeeded: &Value) -> Value {
    let data: Value = serde_json::from_str(succeeded["data"].as_str().unwrap())
        .expect("subscription_succeeded data is a JSON string");
    data["presence"].clone()
}

/// The roster a subscribing connection sees is the CLUSTER-wide presence set. With
/// u1 on node A, a u2 subscribe on node B must observe a roster of BOTH u1 and u2.
#[tokio::test]
async fn cross_node_presence_roster_is_cluster_wide() {
    let prefix = random_prefix();
    let (addr_a, _guard_a) = spawn_percore_cluster(&prefix).await;
    let (addr_b, _guard_b) = spawn_percore_cluster(&prefix).await;

    let channel = "presence-room";

    // u1 joins on node A first; await its own subscription_succeeded (roster = just u1).
    let (_ws_a, succ_a) = connect_presence(addr_a, channel, "u1", json!({"name":"Ann"})).await;
    let roster_a = roster_of(&succ_a);
    assert_eq!(roster_a["count"], 1, "u1's own roster on A is just itself");

    // u2 joins on node B; its subscription_succeeded roster must be CLUSTER-wide (u1+u2).
    // u1's membership is already committed to Redis (we awaited its succeeded above), so a
    // single bounded read of u2's succeeded carries the full cluster roster.
    let (_ws_b, succ_b) = connect_presence(addr_b, channel, "u2", json!({"name":"Bob"})).await;
    let roster_b = roster_of(&succ_b);

    assert_eq!(
        roster_b["count"], 2,
        "u2's cluster roster must count BOTH users (u1 on A, u2 on B)"
    );
    let ids = roster_b["ids"].as_array().expect("roster ids is an array");
    let id_strs: Vec<&str> = ids.iter().filter_map(|v| v.as_str()).collect();
    assert!(
        id_strs.contains(&"u1") && id_strs.contains(&"u2"),
        "cluster roster ids must contain BOTH u1 (on A) and u2 (on B); got {id_strs:?}"
    );
    assert_eq!(
        roster_b["hash"]["u1"]["name"], "Ann",
        "roster hash must carry u1's cross-node user_info"
    );
    assert_eq!(
        roster_b["hash"]["u2"]["name"], "Bob",
        "roster hash must carry u2's own user_info"
    );
}

/// Read frames from `ws` (timeout-bounded per frame) until a `member_added` for
/// `want_user` arrives, or `deadline` elapses; returns the COUNT of matching frames
/// seen. We keep reading for the whole budget AFTER the first match to prove the
/// emit is SINGLE (a duplicate cross-node emit would surface a second frame).
async fn count_member_added(ws: &mut Ws, want_user: &str, deadline: Duration) -> usize {
    count_member_frames(ws, "pusher_internal:member_added", want_user, deadline).await
}

/// As [`count_member_added`] but for `member_removed`.
async fn count_member_removed(ws: &mut Ws, want_user: &str, deadline: Duration) -> usize {
    count_member_frames(ws, "pusher_internal:member_removed", want_user, deadline).await
}

/// Count `event`-named frames whose double-encoded `data.user_id` equals `want_user`,
/// reading until `deadline`. Bounded per read (raced against the remaining budget);
/// an elapsed budget just stops the loop (it never masks a failure — a missing frame
/// returns 0, a duplicate returns ≥2).
async fn count_member_frames(
    ws: &mut Ws,
    event: &str,
    want_user: &str,
    deadline: Duration,
) -> usize {
    let stop = tokio::time::Instant::now() + deadline;
    let mut seen = 0usize;
    while tokio::time::Instant::now() < stop {
        let remaining = stop.saturating_duration_since(tokio::time::Instant::now());
        let frame = match tokio::time::timeout(remaining, next_json(ws)).await {
            Ok(f) => f,
            Err(_) => break,
        };
        if frame["event"] == event {
            if let Some(s) = frame["data"].as_str() {
                if let Ok(inner) = serde_json::from_str::<Value>(s) {
                    if inner["user_id"] == want_user {
                        seen += 1;
                    }
                }
            }
        }
    }
    seen
}

/// When u2 joins on node B, an already-subscribed u1 on node A receives EXACTLY ONE
/// `member_added` for u2 — cross-node, single-emit on the cluster first-for-user edge.
#[tokio::test]
async fn cross_node_presence_member_added_single_emit() {
    let prefix = random_prefix();
    let (addr_a, _guard_a) = spawn_percore_cluster(&prefix).await;
    let (addr_b, _guard_b) = spawn_percore_cluster(&prefix).await;

    let channel = "presence-room";

    // u1 subscribes on A and settles (its own succeeded carries just u1).
    let (mut ws_a, succ_a) = connect_presence(addr_a, channel, "u1", json!({"name":"Ann"})).await;
    assert_eq!(roster_of(&succ_a)["count"], 1, "u1 starts alone");

    // u2 subscribes on B → the bridge on B fires the single cluster-wide member_added,
    // which fans to A via Redis. u1 on A must receive EXACTLY ONE member_added for u2.
    let (_ws_b, _succ_b) = connect_presence(addr_b, channel, "u2", json!({"name":"Bob"})).await;

    let count = count_member_added(&mut ws_a, "u2", Duration::from_secs(5)).await;
    assert_eq!(
        count, 1,
        "u1 on A must receive EXACTLY ONE cross-node member_added for u2 (got {count})"
    );
}

/// When u2 leaves node B, an already-subscribed u1 on node A receives EXACTLY ONE
/// `member_removed` for u2 — cross-node, single-emit on the cluster last-for-user edge.
#[tokio::test]
async fn cross_node_presence_member_removed_single_emit() {
    let prefix = random_prefix();
    let (addr_a, _guard_a) = spawn_percore_cluster(&prefix).await;
    let (addr_b, _guard_b) = spawn_percore_cluster(&prefix).await;

    let channel = "presence-room";

    // u1 on A and u2 on B both subscribed.
    let (mut ws_a, _succ_a) = connect_presence(addr_a, channel, "u1", json!({"name":"Ann"})).await;
    let (ws_b, _succ_b) = connect_presence(addr_b, channel, "u2", json!({"name":"Bob"})).await;

    // Drain u1's member_added for u2 first so it can't be confused for a later frame.
    let added = count_member_added(&mut ws_a, "u2", Duration::from_secs(5)).await;
    assert_eq!(added, 1, "u1 must first see u2's single member_added");

    // u2 disconnects on B → the bridge on B fires the single cluster-wide member_removed
    // on the cluster last-for-user edge, which fans to A. u1 must receive EXACTLY ONE.
    drop(ws_b);

    let removed = count_member_removed(&mut ws_a, "u2", Duration::from_secs(5)).await;
    assert_eq!(
        removed, 1,
        "u1 on A must receive EXACTLY ONE cross-node member_removed for u2 (got {removed})"
    );
}
