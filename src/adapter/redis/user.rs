//! Cross-node user/identity operations: the atomic signin/signout connection
//! refcount, the cluster online check, and the per-user pub/sub publish. All
//! return `anyhow::Result` (or are best-effort with logging); callers fall back
//! to the node-local adapter on error.

use super::client;
use super::client::Scripts;
use super::envelope::{Envelope, EnvelopeKind};
use super::keys::{member_token, Keys};
use crate::protocol::socket_id::SocketId;
use fred::clients::Pool;
use fred::interfaces::{HashesInterface, KeysInterface, SetsInterface};
use serde_json::Value;

/// Run USER_SIGNIN. Returns the cluster `first_for_user` edge (HLEN == 1 → the user
/// came online cluster-wide via this connection).
#[allow(clippy::too_many_arguments)]
pub(super) async fn signin(
    scripts: &Scripts,
    pool: &Pool,
    keys: &Keys,
    node_id: &str,
    app: &str,
    user_id: &str,
    socket_id: &SocketId,
    ttl_secs: u64,
) -> anyhow::Result<bool> {
    let token = member_token(node_id, socket_id.as_str());
    let conn: i64 = scripts
        .user_signin
        .evalsha_with_reload::<i64, _, _>(
            pool.next(),
            vec![keys.usr(app, user_id), keys.users(app)],
            vec![
                token,
                (super::now_ms() + ttl_secs * 1000).to_string(),
                ttl_secs.to_string(),
                user_id.to_string(),
            ],
        )
        .await?;
    Ok(conn == 1)
}

/// Run USER_SIGNOUT. Returns the cluster `last_for_user` edge (HLEN == 0 → the
/// user's last cluster connection just dropped).
pub(super) async fn signout(
    scripts: &Scripts,
    pool: &Pool,
    keys: &Keys,
    node_id: &str,
    app: &str,
    user_id: &str,
    socket_id: &SocketId,
) -> anyhow::Result<bool> {
    let token = member_token(node_id, socket_id.as_str());
    let conn: i64 = scripts
        .user_signout
        .evalsha_with_reload::<i64, _, _>(
            pool.next(),
            vec![keys.usr(app, user_id), keys.users(app)],
            vec![token, user_id.to_string()],
        )
        .await?;
    Ok(conn == 0)
}

/// Cluster online check: `HLEN usr > 0`.
pub(super) async fn is_online(
    pool: &Pool,
    keys: &Keys,
    app: &str,
    user_id: &str,
) -> anyhow::Result<bool> {
    let n: i64 = pool.next().hlen(keys.usr(app, user_id)).await?;
    Ok(n > 0)
}

/// Sweeper crash-time reap of ONE indexed user's stale bindings. A live node re-stamps
/// its own bindings' `expireAt`; a crashed node stops, so its bindings go stale. This
/// HDELs every stale token of `user_id`; when no fresh binding remains — the user's last
/// cluster connection was on the dead node, OR the `usr` hash already TTL-expired while
/// still indexed in `users(app)` (the orphan case, mirroring the channel sweep) — it is
/// the cluster offline edge: DEL the (now-empty) hash, de-index, and publish WatchOffline
/// so every live node notifies its local watchers. Best-effort; logs + continues.
///
/// The WatchOffline envelope's publisher `node_id` is the DEAD node (the stale token's
/// prefix, or an empty sentinel for an already-gone hash) so this sweeper's OWN receive
/// loop does NOT self-dedup it — A must still notify its local watchers of u7's offline.
/// `compat` is the cluster-wide `PYLON_CLUSTER_ENVELOPE_COMPAT` setting (the
/// WatchOffline control envelope keeps its shape either way; threaded for
/// uniformity with the frame-carrying reap paths).
pub(super) async fn reap_user(
    pool: &Pool,
    keys: &Keys,
    app: &str,
    user_id: &str,
    sharded: bool,
    compat: bool,
    now: u64,
) {
    let usr = keys.usr(app, user_id);
    let members: Vec<(String, String)> = match pool.next().hgetall(&usr).await {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(error = %e, app, user_id, "sweeper: HGETALL usr failed");
            return;
        }
    };

    // Partition into stale tokens (to HDEL) and a dead-node attribution for the offline
    // publish. A fresh (future-`expireAt`) binding means a live node still holds the user.
    let mut stale: Vec<String> = Vec::new();
    let mut had_fresh = false;
    let mut dead_node = String::new();
    for (token, expire_at) in &members {
        let is_stale = expire_at
            .parse::<u64>()
            .map(|exp| exp < now)
            .unwrap_or(true);
        if is_stale {
            if dead_node.is_empty() {
                if let Some((n, _)) = token.split_once(':') {
                    dead_node = n.to_string();
                }
            }
            stale.push(token.clone());
        } else {
            had_fresh = true;
        }
    }

    if !stale.is_empty() {
        if let Err(e) = pool.next().hdel::<i64, _, _>(&usr, stale.clone()).await {
            tracing::warn!(error = %e, app, user_id, "sweeper: HDEL usr stale failed");
            return;
        }
    }

    // A user with any fresh binding is still online cluster-wide — leave it.
    if had_fresh {
        return;
    }

    // No fresh binding remains. `HLEN usr` is the authoritative post-reap count (also 0
    // when the hash already TTL-expired). A non-zero count means a concurrent signin
    // landed between our HGETALL and now — skip the offline edge for it.
    let remaining: i64 = match pool.next().hlen(&usr).await {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(error = %e, app, user_id, "sweeper: HLEN usr failed");
            return;
        }
    };
    if remaining != 0 {
        return;
    }

    // Cluster offline edge: DEL the (now-empty) hash, de-index, publish WatchOffline.
    // Both stay best-effort (log-only on error): a failed SREM leaves the user indexed,
    // so the next sweep's reap retries the DEL/SREM — and the empty `usr` hash is
    // bounded by its whole-key TTL backstop from signin — so repeated failures
    // self-heal on the next sweep.
    if let Err(e) = pool.next().del::<i64, _>(&usr).await {
        tracing::warn!(error = %e, app, user_id, "sweeper: DEL usr failed");
    }
    if let Err(e) = pool
        .next()
        .srem::<i64, _, _>(keys.users(app), user_id.to_string())
        .await
    {
        tracing::warn!(error = %e, app, user_id, "sweeper: SREM users failed");
    }
    publish(
        pool,
        &keys.watch(app, user_id),
        &dead_node,
        app,
        user_id,
        super::envelope::EnvelopeKind::WatchOffline,
        serde_json::Value::Null,
        sharded,
        compat,
    )
    .await;
}

/// Publish a control/notify envelope on a per-user channel. `frame` is the
/// pre-encoded v7 frame for `UserSend`; `Null` for the other kinds. `node_id` is
/// the publisher (self) for live paths, or the DEAD node (token prefix) from the
/// sweeper so every live node — including the sweeper's own — acts on it.
/// `sharded` routes the publish through SPUBLISH vs PUBLISH (the cluster-wide
/// `PYLON_REDIS_SHARDED_PUBSUB` setting). `compat` is the cluster-wide
/// `PYLON_CLUSTER_ENVELOPE_COMPAT` setting: with compat off a `UserSend`
/// envelope omits the legacy `event` member (frame_b64 is the sole carrier);
/// the `Null` control kinds keep their shape either way. Best-effort: logs +
/// continues on any Redis error.
#[allow(clippy::too_many_arguments)]
pub(super) async fn publish(
    pool: &Pool,
    channel: &str,
    node_id: &str,
    app: &str,
    user_id: &str,
    kind: EnvelopeKind,
    frame: Value,
    sharded: bool,
    compat: bool,
) {
    let env = Envelope {
        node_id: node_id.to_string(),
        app: app.to_string(),
        kind,
        channel: user_id.to_string(),
        // Additive (F16): when the envelope carries a frame (`UserSend`), emit
        // its raw bytes as base64 alongside the legacy `event` JSON string so
        // mixed old/new nodes relay either shape. Non-frame kinds (`Null`)
        // omit the field.
        frame_b64: frame.as_str().map(Envelope::encode_frame_b64),
        event: frame,
        except: None,
    };
    if let Ok(payload) = String::from_utf8(env.encode_with(compat)) {
        if let Err(e) = client::publish_channel(pool, channel, payload, sharded).await {
            tracing::warn!(error = %e, app, user_id, "redis user publish failed");
        }
    }
}
