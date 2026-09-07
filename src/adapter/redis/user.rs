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
use fred::interfaces::HashesInterface;
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
        .membership_join
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

/// Sweeper crash-time reap of ONE indexed user's stale bindings, via the atomic
/// USER_REAP CAS. A live node re-stamps its own bindings' `expireAt`; a crashed node
/// stops, so its bindings go stale. Winning the CAS (`won == 1`) means this call took
/// the user to no bindings at all and de-indexed them — the cluster offline edge, and
/// the single cluster-wide `WatchOffline` emission right. Best-effort: a failed script
/// leaves the user indexed and the next sweep retries.
///
/// The WatchOffline envelope's publisher `node_id` is the DEAD node the script resolved
/// (or an empty sentinel for an already-lapsed hash), so this sweeper's OWN receive loop
/// does NOT self-dedup it — it must still notify its local watchers.
#[allow(clippy::too_many_arguments)]
pub(super) async fn reap_user(
    scripts: &Scripts,
    pool: &Pool,
    keys: &Keys,
    app: &str,
    user_id: &str,
    sharded: bool,
    compat: bool,
    now: u64,
) {
    let (won, dead_node): (i64, String) = match scripts
        .user_reap
        .evalsha_with_reload(
            pool.next(),
            vec![keys.usr(app, user_id), keys.users(app)],
            vec![now.to_string(), user_id.to_string()],
        )
        .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, app, user_id, "sweeper: user reap CAS failed");
            return;
        }
    };
    if won != 1 {
        return;
    }
    publish(
        pool,
        &keys.watch(app, user_id),
        &dead_node,
        app,
        user_id,
        EnvelopeKind::WatchOffline,
        Value::Null,
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
