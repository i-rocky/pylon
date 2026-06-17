//! Opt-in Redis failover regression test.
//!
//! Gate: the entire test body is skipped unless `PYLON_TEST_REDIS_FAILOVER=1`
//! is set. This test bounces the shared `pylon-test-redis` Docker container,
//! which would disrupt parallel cluster tests — so it MUST NOT run in the
//! normal suite.
//!
//! What it proves: after a Docker `restart` of the Redis container, cross-node
//! delivery (node A REST-publish → Redis pub/sub → node B WS subscriber)
//! **resumes** automatically — Fred reconnects and resubscribes, the receive
//! loop on B resumes, and the next publish reaches B.
//!
//! Run:
//! ```sh
//! docker start pylon-test-redis
//! PYLON_TEST_REDIS_FAILOVER=1 PYLON_TEST_REDIS_URL=redis://127.0.0.1:6390 \
//!   cargo test --test redis_failover -- --nocapture
//! ```

mod common;

use common::{connect, established_socket_id, next_event_named, send_json, spawn_percore_cluster};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;
use uuid::Uuid;

/// The common harness app id.
const APP_ID: &str = "app";

fn random_prefix() -> String {
    format!("pylontest:{}", Uuid::new_v4())
}

/// Sign a Pusher REST request. Mirrors `tests/percore_cluster.rs::signed_query`.
fn signed_query(method: &str, path: &str, body: &[u8]) -> String {
    use common::{KEY, SECRET};
    use pylon::auth::signature::{hmac_sha256_hex, md5_hex};

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

/// POST a signed Pusher event to `addr` and assert HTTP 200.
async fn publish_event(addr: SocketAddr, event_name: &str, channel: &str, data: &str) {
    let path = format!("/apps/{APP_ID}/events");
    let body =
        json!({ "name": event_name, "data": data, "channels": [channel] }).to_string();
    let q = signed_query("POST", &path, body.as_bytes());
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}{path}?{q}"))
        .body(body)
        .send()
        .await
        .expect("REST publish must reach the node");
    assert_eq!(
        resp.status(),
        200,
        "REST publish must be accepted (200); got {}",
        resp.status()
    );
}

/// Connect a WS client and subscribe it to `channel`. Returns the live socket.
async fn connect_and_subscribe(addr: SocketAddr, channel: &str) -> common::Ws {
    let mut ws = connect(addr, "?protocol=7").await;
    let _socket_id = established_socket_id(&mut ws).await;
    send_json(
        &mut ws,
        json!({ "event": "pusher:subscribe", "data": { "channel": channel } }),
    )
    .await;
    let succeeded =
        next_event_named(&mut ws, "pusher_internal:subscription_succeeded").await;
    assert_eq!(
        succeeded["channel"], channel,
        "subscription_succeeded must name the channel"
    );
    ws
}

/// Read the next `event_name` frame from `ws` within `timeout_secs`. Panics on
/// timeout or an unexpected close. Skips unrelated frames (like subscription_count).
async fn recv_event(ws: &mut common::Ws, event_name: &str, timeout_secs: u64) -> Value {
    let deadline = Duration::from_secs(timeout_secs);
    tokio::time::timeout(deadline, next_event_named(ws, event_name))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "timed out ({timeout_secs}s) waiting for event '{event_name}' after Redis bounce"
            )
        })
}

/// Poll until `docker exec pylon-test-redis redis-cli ping` returns "PONG" or
/// `max_wait` elapses. Returns whether Redis responded in time.
fn wait_for_redis_ping(max_wait: Duration) -> bool {
    let start = std::time::Instant::now();
    loop {
        let out = std::process::Command::new("docker")
            .args(["exec", "pylon-test-redis", "redis-cli", "ping"])
            .output();
        if let Ok(o) = out {
            if o.status.success() {
                let stdout = String::from_utf8_lossy(&o.stdout);
                if stdout.trim() == "PONG" {
                    return true;
                }
            }
        }
        if start.elapsed() >= max_wait {
            return false;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Opt-in Redis failover regression test.
///
/// Proves that cross-node delivery (A → Redis → B) **resumes** automatically
/// after a `docker restart` of the Redis container.
#[tokio::test]
async fn redis_failover_cross_node_resumes() {
    // ── Gate: skip unless the opt-in env var is set ──────────────────────────
    if std::env::var("PYLON_TEST_REDIS_FAILOVER").is_err() {
        eprintln!("skipping: set PYLON_TEST_REDIS_FAILOVER=1 to run");
        return;
    }

    // ── 1. Spawn a 2-node cluster ─────────────────────────────────────────────
    let prefix = random_prefix();
    let (addr_a, _guard_a) = spawn_percore_cluster(&prefix).await;
    let (addr_b, _guard_b) = spawn_percore_cluster(&prefix).await;

    let channel = "failover-chan";

    // ── 2. Subscribe a WS client on node B ───────────────────────────────────
    let mut ws_b = connect_and_subscribe(addr_b, channel).await;

    // Give B's bridge a moment to issue the Redis SUBSCRIBE before the first
    // publish so the baseline message is not lost.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // ── 3. Baseline: publish on A, assert B receives cross-node ──────────────
    let baseline_payload = "{\"step\":\"baseline\"}";
    publish_event(addr_a, "step-event", channel, baseline_payload).await;

    let frame = recv_event(&mut ws_b, "step-event", 10).await;
    assert_eq!(
        frame["data"], baseline_payload,
        "baseline: B must receive the cross-node event before the Redis bounce"
    );
    eprintln!("[redis_failover] baseline cross-node delivery: PASS");

    // ── 4. Bounce Redis ───────────────────────────────────────────────────────
    eprintln!("[redis_failover] bouncing pylon-test-redis …");
    let bounce = std::process::Command::new("docker")
        .args(["restart", "pylon-test-redis"])
        .status()
        .expect("docker restart must be accessible");
    assert!(bounce.success(), "docker restart pylon-test-redis must succeed");
    eprintln!("[redis_failover] docker restart exited OK; waiting for PONG …");

    // Poll until Redis is back (bounded to 30 s).
    assert!(
        wait_for_redis_ping(Duration::from_secs(30)),
        "Redis must respond to PING within 30s after the bounce"
    );
    eprintln!("[redis_failover] Redis is back (PONG received)");

    // ── 5. Give Fred time to reconnect + resubscribe ─────────────────────────
    // Fred starts reconnecting immediately; give the async runtime a few seconds
    // to complete the handshake and re-issue SUBSCRIBE. We poll rather than
    // sleeping a fixed duration so the test is fast when Redis is quick.
    //
    // Wait up to 10 s for Fred to resubscribe. We'll confirm delivery in step 6;
    // this sleep just gives the receive loop time to re-arm before the publish.
    let resubscribe_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    tokio::time::sleep_until(resubscribe_deadline).await;
    eprintln!("[redis_failover] waited for Fred reconnect+resubscribe window");

    // ── 6. Publish again on A; assert B receives (cross-node resumed) ─────────
    let post_bounce_payload = "{\"step\":\"post-bounce\"}";
    publish_event(addr_a, "step-event", channel, post_bounce_payload).await;

    // Generous timeout: Fred may still be mid-resubscribe so the frame may arrive
    // with a short lag. 20 s is far above any realistic reconnect time.
    let frame = recv_event(&mut ws_b, "step-event", 20).await;
    assert_eq!(
        frame["data"], post_bounce_payload,
        "post-bounce: B must receive cross-node event after the Redis bounce — the receive loop must survive"
    );
    eprintln!("[redis_failover] post-bounce cross-node delivery: PASS");
    eprintln!("[redis_failover] TEST PASSED — cross-node delivery resumed after Redis bounce");
}
