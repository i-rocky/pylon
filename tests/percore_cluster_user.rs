//! End-to-end 2-node clustered-percore signin / watchlist / user-send / terminate
//! proof (SP11 §3.5 gate).
//!
//! Two REAL percore nodes (each a single-worker `mio` fleet + a [`ClusterBridge`]
//! owning that node's `RedisAdapter`) are spawned on ONE shared Redis key prefix,
//! forming a 2-node cluster. The tests connect real WS clients to each node and
//! prove the SIGNED-IN cross-node edges ride through Redis:
//!
//! * `cross_node_watchlist_online` — a watcher on node A is notified when the
//!   watched user signs in on node B (the bridge's `cluster_signin` publishes
//!   `WatchOnline`, which node A's recv loop turns into a `watchlist_events`).
//! * `cross_node_watchlist_offline` — symmetric: disconnecting the watched user on
//!   node B notifies the watcher on node A (`WatchOffline`).
//! * `cross_node_watchlist_initial_online_snapshot` — a watcher signing in on node A
//!   sees the watched user (already signed in on node B) in its INITIAL online
//!   snapshot (the cluster online subset from the bridge's `cluster_watch`).
//! * `cross_node_send_to_user` — a server-to-user REST trigger on node A reaches the
//!   user's connection on node B (the `RedisAdapter` publishes `UserSend`, node B's
//!   recv loop delivers it to the user's mailbox).
//! * `cross_node_terminate_user` — a terminate REST call on node A closes the user's
//!   connection on node B with 4009 (the `RedisAdapter` publishes `UserTerminate`).
//!
//! Like the rest of the cluster suite, these talk to a REAL Redis
//! (`PYLON_TEST_REDIS_URL`, default `redis://127.0.0.1:6390`) and isolate every run
//! behind a random key prefix — they NEVER issue FLUSHALL/FLUSHDB, and FAIL LOUD if
//! Redis is unreachable (the bridge `start` panics with a clear message).
//!
//! Cross-node delivery has Redis pub/sub latency, so every cross-node assertion is
//! BOUNDED by a timeout. The single-emit watchlist tests keep reading the budget
//! AFTER the first match to assert exactly one event arrives (no cross-node dup).

mod common;

use common::{
    connect, established_socket_id, next_json, next_json_within, send_json, spawn_percore_cluster,
    wait_pubsub_subscribers, wait_redis_hlen_ge, Ws, KEY, SECRET,
};
use futures_util::StreamExt;
use pylon::adapter::redis::keys::Keys;
use pylon::auth::signature::{hmac_sha256_hex, md5_hex, user_signature};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

/// The common harness `APPS` app id (key `app-key`, secret `app-secret`).
const APP_ID: &str = "app";

/// A random, run-unique key prefix so the two nodes share one cluster namespace
/// without ever clobbering a shared Redis.
fn random_prefix() -> String {
    format!("pylontest:{}", Uuid::new_v4())
}

/// Build the signed Pusher REST query string for a request, mirroring
/// `tests/percore_cluster.rs::signed_query` (HMAC-SHA256 over
/// `METHOD\npath\ncanonical`, with a `body_md5`).
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

/// Connect a WS client to `addr` and drain its `connection_established`, returning
/// the live socket and its assigned socket_id.
async fn connect_established(addr: SocketAddr) -> (Ws, String) {
    let mut ws = connect(addr, "?protocol=7").await;
    let socket_id = established_socket_id(&mut ws).await;
    (ws, socket_id)
}

/// Sign and send a `pusher:signin` for the EXACT `user_data` string, then read and
/// assert the `pusher:signin_success` ack.
async fn signin(ws: &mut Ws, socket_id: &str, user_data: &str) {
    let auth = format!("{KEY}:{}", user_signature(SECRET, socket_id, user_data));
    send_json(
        ws,
        json!({
            "event": "pusher:signin",
            "data": { "auth": auth, "user_data": user_data }
        }),
    )
    .await;
    let ack = next_json(ws).await;
    assert_eq!(ack["event"], "pusher:signin_success");
}

/// Wait (bounded) for the next `pusher_internal:watchlist_events` frame, returning
/// its single change's `(name, user_ids)`. Skips any interleaved non-watchlist
/// frames. Returns `None` if `deadline` elapses with no watchlist frame. Each read
/// is bounded by the REMAINING budget via `next_json_within` (non-panicking —
/// racing `next_json` itself against an equal-length outer timeout let its
/// INTERNAL 5s panic win the tie).
async fn next_watchlist(ws: &mut Ws, deadline: Duration) -> Option<(String, Vec<String>)> {
    let stop = tokio::time::Instant::now() + deadline;
    loop {
        let remaining = stop.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        let frame = next_json_within(ws, remaining).await?;
        if frame["event"] == "pusher_internal:watchlist_events" {
            let ev = &frame["data"]["events"][0];
            let name = ev["name"].as_str().unwrap_or_default().to_string();
            let ids = ev["user_ids"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            return Some((name, ids));
        }
    }
}

/// Assert NO further `watchlist_events` frame arrives within `window` — used to
/// prove a cross-node single-emit edge delivered exactly once (no Redis dup).
async fn assert_no_more_watchlist(ws: &mut Ws, window: Duration) {
    if let Some((name, ids)) = next_watchlist(ws, window).await {
        panic!("unexpected duplicate watchlist_events: name={name} user_ids={ids:?}");
    }
}

/// A watcher `w` on node A is watching user `U`. When `u` signs in as `U` on node B,
/// the bridge's cluster_signin publishes `WatchOnline`; node A's recv loop turns it
/// into a single `watchlist_events { online: [U] }` for `w`.
#[tokio::test]
async fn cross_node_watchlist_online() {
    let prefix = random_prefix();
    let (addr_a, _guard_a) = spawn_percore_cluster(&prefix).await;
    let (addr_b, _guard_b) = spawn_percore_cluster(&prefix).await;

    // w signs in on A watching U. U is offline cluster-wide → NO initial snapshot.
    let (mut w, sid_w) = connect_established(addr_a).await;
    signin(&mut w, &sid_w, r#"{"id":"W","watchlist":["U"]}"#).await;
    assert!(
        next_watchlist(&mut w, Duration::from_millis(500))
            .await
            .is_none(),
        "w must receive no watchlist snapshot while U is offline"
    );

    // Gate u's signin on the OBSERVABLE "A's bridge finished its Redis SUBSCRIBE of
    // U's watch channel" (PUBSUB NUMSUB >= 1). The cluster online edge PUBLISHES on
    // that channel, and a publish racing a still-in-flight SUBSCRIBE is silently
    // lost by pub/sub — the old fixed 300ms sleep was a guess at this.
    let watch_key = Keys::new(&prefix).watch(APP_ID, "U");
    assert!(
        wait_pubsub_subscribers(&watch_key, 1, Duration::from_secs(5)).await,
        "A's bridge must SUBSCRIBE {watch_key} before U signs in on B"
    );

    // u signs in as U on B → cluster online edge → w gets ONE online event for U.
    let (mut u, sid_u) = connect_established(addr_b).await;
    signin(&mut u, &sid_u, r#"{"id":"U"}"#).await;

    let (name, ids) = next_watchlist(&mut w, Duration::from_secs(5))
        .await
        .expect("w must receive a cross-node online watchlist_events for U");
    assert_eq!(name, "online");
    assert_eq!(ids, vec!["U".to_string()]);
    // Exactly one — the origin self-dedups its publish; A delivers it once.
    assert_no_more_watchlist(&mut w, Duration::from_millis(600)).await;
}

/// With `w`@A watching U and `u`@B signed in as U, disconnecting `u` notifies `w`
/// with ONE `offline` watchlist event for U (the bridge's cluster_signout publishes
/// `WatchOffline` on the cluster last-for-user edge).
#[tokio::test]
async fn cross_node_watchlist_offline() {
    let prefix = random_prefix();
    let (addr_a, _guard_a) = spawn_percore_cluster(&prefix).await;
    let (addr_b, _guard_b) = spawn_percore_cluster(&prefix).await;

    // w signs in on A watching U (U offline → no snapshot).
    let (mut w, sid_w) = connect_established(addr_a).await;
    signin(&mut w, &sid_w, r#"{"id":"W","watchlist":["U"]}"#).await;
    let _ = next_watchlist(&mut w, Duration::from_millis(300)).await;
    // Same gate as the online test: A's watch-channel SUBSCRIBE must land before
    // u's signin publishes the WatchOnline on it (the observable, not a sleep).
    let watch_key = Keys::new(&prefix).watch(APP_ID, "U");
    assert!(
        wait_pubsub_subscribers(&watch_key, 1, Duration::from_secs(5)).await,
        "A's bridge must SUBSCRIBE {watch_key} before U signs in on B"
    );

    // u signs in as U on B → w sees online; drain it.
    let (u, sid_u) = connect_established(addr_b).await;
    let mut u = u;
    signin(&mut u, &sid_u, r#"{"id":"U"}"#).await;
    let online = next_watchlist(&mut w, Duration::from_secs(5)).await;
    assert_eq!(online, Some(("online".to_string(), vec!["U".to_string()])));

    // Drop u's socket so B observes the disconnect and runs on_close → cluster offline.
    drop(u);

    let (name, ids) = next_watchlist(&mut w, Duration::from_secs(5))
        .await
        .expect("w must receive a cross-node offline watchlist_events for U");
    assert_eq!(name, "offline");
    assert_eq!(ids, vec!["U".to_string()]);
    assert_no_more_watchlist(&mut w, Duration::from_millis(600)).await;
}

/// `u`@B signs in as U FIRST; THEN `w`@A signs in watching U. `w`'s INITIAL online
/// snapshot must include U (the cluster online subset returned by the bridge's
/// `cluster_watch` and sent back via the Watch cmd's mailbox).
#[tokio::test]
async fn cross_node_watchlist_initial_online_snapshot() {
    let prefix = random_prefix();
    let (addr_a, _guard_a) = spawn_percore_cluster(&prefix).await;
    let (addr_b, _guard_b) = spawn_percore_cluster(&prefix).await;

    // u signs in as U on B first → U is online cluster-wide.
    let (mut u, sid_u) = connect_established(addr_b).await;
    signin(&mut u, &sid_u, r#"{"id":"U"}"#).await;
    // Gate w's signin on the OBSERVABLE the snapshot reads: U's cluster binding
    // hash (`usr`) must be non-empty in Redis. The signin ack does NOT prove the
    // bridge's async USER_SIGNIN write landed — the old 300ms sleep guessed.
    let usr_key = Keys::new(&prefix).usr(APP_ID, "U");
    assert!(
        wait_redis_hlen_ge(&usr_key, 1, Duration::from_secs(5)).await,
        "U's usr binding must land in Redis before w's snapshot read"
    );

    // w signs in on A watching U → initial snapshot must report U online.
    let (mut w, sid_w) = connect_established(addr_a).await;
    signin(&mut w, &sid_w, r#"{"id":"W","watchlist":["U"]}"#).await;

    let (name, ids) = next_watchlist(&mut w, Duration::from_secs(5))
        .await
        .expect("w's initial snapshot must include U as online");
    assert_eq!(name, "online");
    assert_eq!(ids, vec!["U".to_string()]);
    // The keep-`u`-alive binding stays in scope until the assertions complete.
    let _ = &u;
}

/// A server-to-user REST trigger on node A reaches the user's signed-in connection
/// on node B (the node's `RedisAdapter::send_to_user` publishes `UserSend`; node B's
/// recv loop delivers it to U's mailbox).
#[tokio::test]
async fn cross_node_send_to_user() {
    let prefix = random_prefix();
    let (addr_a, _guard_a) = spawn_percore_cluster(&prefix).await;
    let (addr_b, _guard_b) = spawn_percore_cluster(&prefix).await;

    // u signs in as U on node B (no channel subscribe — server-to-user routes via
    // the user registry, not a channel).
    let (mut u, sid_u) = connect_established(addr_b).await;
    signin(&mut u, &sid_u, r#"{"id":"U"}"#).await;
    // Gate the REST trigger on the OBSERVABLE "B's bridge finished its Redis
    // SUBSCRIBE of U's usermsg channel": the trigger PUBLISHES on that channel,
    // and a publish racing a still-in-flight SUBSCRIBE is silently lost by
    // pub/sub — the old fixed 300ms sleep was a guess at this.
    let usermsg_key = Keys::new(&prefix).usermsg(APP_ID, "U");
    assert!(
        wait_pubsub_subscribers(&usermsg_key, 1, Duration::from_secs(5)).await,
        "B's bridge must SUBSCRIBE {usermsg_key} before the REST trigger fires on A"
    );

    // Server-to-user trigger on node A's REST: `data` is a JSON-encoded STRING.
    let body = json!({
        "name": "notif",
        "channel": "#server-to-user-U",
        "data": "{\"msg\":\"hi\"}"
    })
    .to_string();
    let path = format!("/apps/{APP_ID}/events");
    let q = signed_query("POST", &path, body.as_bytes());
    let resp = reqwest::Client::new()
        .post(format!("http://{addr_a}{path}?{q}"))
        .body(body)
        .send()
        .await
        .expect("REST server-to-user request must reach node A");
    assert_eq!(resp.status(), 200);

    // u (on B) receives the cross-node user-directed event.
    let stop = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = stop.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "u must receive the cross-node server-to-user event"
        );
        let frame = match tokio::time::timeout(remaining, next_json(&mut u)).await {
            Ok(f) => f,
            Err(_) => panic!("u must receive the cross-node server-to-user event"),
        };
        if frame["event"] == "notif" {
            assert_eq!(frame["channel"], "#server-to-user-U");
            assert_eq!(frame["data"], "{\"msg\":\"hi\"}");
            break;
        }
    }
}

/// A terminate REST call on node A closes the user's connection on node B with 4009
/// (the node's `RedisAdapter::terminate_user` publishes `UserTerminate`; node B's
/// recv loop runs `local.terminate_user`, sending 4009 + a close).
#[tokio::test]
async fn cross_node_terminate_user() {
    let prefix = random_prefix();
    let (addr_a, _guard_a) = spawn_percore_cluster(&prefix).await;
    let (addr_b, _guard_b) = spawn_percore_cluster(&prefix).await;

    // u signs in as U on node B.
    let (mut u, sid_u) = connect_established(addr_b).await;
    signin(&mut u, &sid_u, r#"{"id":"U"}"#).await;
    // Same gate as send_to_user: the terminate PUBLISHES on U's usermsg channel,
    // so B's SUBSCRIBE of it must land first (the observable, not a sleep).
    let usermsg_key = Keys::new(&prefix).usermsg(APP_ID, "U");
    assert!(
        wait_pubsub_subscribers(&usermsg_key, 1, Duration::from_secs(5)).await,
        "B's bridge must SUBSCRIBE {usermsg_key} before the REST terminate fires on A"
    );

    // Terminate U via node A's REST terminate_connections route (body `{}`).
    let path = format!("/apps/{APP_ID}/users/U/terminate_connections");
    let body = b"{}";
    let q = signed_query("POST", &path, body);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr_a}{path}?{q}"))
        .body(body.as_slice())
        .send()
        .await
        .expect("REST terminate request must reach node A");
    assert_eq!(resp.status(), 200);

    // u (on B) must receive pusher:error 4009 (then a close). Tolerate an interleaved
    // frame before the error, but the connection MUST surface 4009 / close within
    // the budget.
    let stop = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut saw_error = false;
    loop {
        let remaining = stop.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, u.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["event"] == "pusher:error" && v["data"]["code"] == 4009 {
                    saw_error = true;
                    break;
                }
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                if let Some(cf) = frame {
                    assert_eq!(u16::from(cf.code), 4009, "close frame should carry 4009");
                }
                saw_error = true;
                break;
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => break,
        }
    }
    assert!(
        saw_error,
        "u on node B must receive the cross-node terminate (4009 error or 4009 close)"
    );
}
