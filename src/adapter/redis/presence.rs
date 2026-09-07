//! Cross-node presence operations: the atomic join/leave refcount and the cluster
//! roster read. All return `anyhow::Result`; callers fall back to the node-local
//! adapter on error.

use super::client;
use super::client::Scripts;
use super::envelope::{Envelope, EnvelopeKind};
use super::keys::{member_token, Keys};
use crate::presence::member::PresenceMember;
use crate::protocol::event::{PresencePayload, ServerEvent};
use crate::protocol::socket_id::SocketId;
use crate::webhook::event::WebhookEvent;
use crate::webhook::WebhookHandle;
use fred::clients::Pool;
use fred::interfaces::HashesInterface;
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Run PRESENCE_JOIN under `max_members` and read the cluster roster. `Ok(None)` means the
/// cluster-wide distinct-user cap rejected the join and nothing was written.
#[allow(clippy::too_many_arguments)]
pub(super) async fn join(
    scripts: &Scripts,
    pool: &Pool,
    keys: &Keys,
    node_id: &str,
    app: &str,
    channel: &str,
    member: &PresenceMember,
    socket_id: &SocketId,
    max_members: Option<usize>,
) -> anyhow::Result<Option<(bool, PresencePayload)>> {
    let token = member_token(node_id, socket_id.as_str());
    let info = serde_json::to_string(&member.user_info)?;
    let cap = max_members.map_or(-1, |n| i64::try_from(n).unwrap_or(i64::MAX));
    let conn: i64 = scripts
        .presence_join
        .evalsha_with_reload::<i64, _, _>(
            pool.next(),
            vec![
                keys.presusers(app, channel),
                keys.presinfo(app, channel),
                keys.presmembers(app, channel),
                keys.presseats(app, channel),
            ],
            vec![member.user_id.clone(), info, token, cap.to_string()],
        )
        .await?;
    if conn < 0 {
        return Ok(None);
    }
    let roster = roster(pool, keys, app, channel).await?;
    Ok(Some((conn == 1, roster)))
}

/// Run PRESENCE_LEAVE. Returns `last_for_user`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn leave(
    scripts: &Scripts,
    pool: &Pool,
    keys: &Keys,
    node_id: &str,
    app: &str,
    channel: &str,
    user_id: &str,
    socket_id: &SocketId,
) -> anyhow::Result<bool> {
    let token = member_token(node_id, socket_id.as_str());
    let conn: i64 = scripts
        .presence_leave
        .evalsha_with_reload::<i64, _, _>(
            pool.next(),
            vec![
                keys.presusers(app, channel),
                keys.presinfo(app, channel),
                keys.presmembers(app, channel),
                keys.presseats(app, channel),
            ],
            vec![user_id.to_string(), token],
        )
        .await?;
    Ok(conn == 0)
}

/// Presence channels are exactly `presence-*` (cache or not).
pub(super) fn is_presence(channel: &str) -> bool {
    channel.starts_with("presence-")
}

/// Cluster roster as `Vec<PresenceMember>` (sorted by user_id) — for `presence_members`.
pub(super) async fn members(
    pool: &Pool,
    keys: &Keys,
    app: &str,
    channel: &str,
) -> anyhow::Result<Vec<PresenceMember>> {
    let entries: Vec<(String, String)> = pool.next().hgetall(keys.presinfo(app, channel)).await?;
    let mut members: Vec<PresenceMember> = entries
        .into_iter()
        .map(|(user_id, info)| PresenceMember {
            user_info: serde_json::from_str(&info).unwrap_or(Value::Null),
            user_id,
        })
        .collect();
    members.sort_by(|a, b| a.user_id.cmp(&b.user_id));
    Ok(members)
}

/// Cluster distinct-user count = `HLEN presusers`.
pub(super) async fn user_count(
    pool: &Pool,
    keys: &Keys,
    app: &str,
    channel: &str,
) -> anyhow::Result<usize> {
    let n: i64 = pool.next().hlen(keys.presusers(app, channel)).await?;
    Ok(n.max(0) as usize)
}

/// Sweeper crash-time reap of ONE stale presence member token — the atomic CAS
/// (the member analog of the vacate CAS `VACATE_LUA` in `client.rs`). One
/// `REAP_MEMBER_LUA` invocation resolves the token to its user, decrements the
/// user's cluster refcount (or removes the user on the 1→0 edge), and returns the
/// CAS verdict: `won == 1` iff THIS call took the refcount to EXACTLY 0 — the
/// single cluster-wide `member_removed` emission right. Best-effort: logs +
/// returns on any Redis error, never panics.
#[allow(clippy::too_many_arguments)]
pub(super) async fn reap_member(
    scripts: &Scripts,
    pool: &Pool,
    keys: &Keys,
    app: &str,
    channel: &str,
    token: &str,
    sharded: bool,
    compat: bool,
    webhooks: &WebhookHandle,
) {
    let (user_id, _remaining, won): (String, i64, i64) = match scripts
        .reap_member
        .evalsha_with_reload::<(String, i64, i64), _, _>(
            pool.next(),
            vec![
                keys.presusers(app, channel),
                keys.presinfo(app, channel),
                keys.presmembers(app, channel),
                keys.presseats(app, channel),
            ],
            vec![token.to_string()],
        )
        .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, app, channel, token, "sweeper: REAP_MEMBER cas failed");
            return;
        }
    };
    if won != 1 {
        return;
    }
    let dead_node = token.split_once(':').map(|(n, _)| n).unwrap_or_default();
    emit_member_removed(
        pool, keys, app, channel, &user_id, dead_node, sharded, compat, webhooks,
    )
    .await;
}

/// Announce the users a won `VACATE_LUA` drained from the roster of a channel that
/// has just become unreachable — the compensating `member_removed` for members whose
/// node died holding them, which no live leave and no per-token reap can still emit.
/// Emitted before the caller's `channel_vacated`, preserving the documented order.
#[allow(clippy::too_many_arguments)]
pub(super) async fn emit_drained_members(
    pool: &Pool,
    keys: &Keys,
    app: &str,
    channel: &str,
    users: &[String],
    sharded: bool,
    compat: bool,
    webhooks: &WebhookHandle,
) {
    for user_id in users {
        emit_member_removed(
            pool,
            keys,
            app,
            channel,
            user_id,
            NO_ORIGIN_NODE,
            sharded,
            compat,
            webhooks,
        )
        .await;
    }
}

/// Envelope publisher for an emission that belongs to no surviving node. Node ids are
/// UUIDs, so no live node's receive loop can mistake this for its own echo and drop it.
const NO_ORIGIN_NODE: &str = "";

/// Broadcast one `member_removed` cluster-wide on the channel's msg pub/sub, plus its
/// webhook. `origin_node` is the node the departed member belonged to (the sweeper is
/// never it), so every LIVE node — including the sweeper's own — delivers the frame.
/// `compat` is the cluster-wide `PYLON_CLUSTER_ENVELOPE_COMPAT` setting: with compat
/// off the envelope omits the legacy `event` member and `frame_b64` is the sole
/// carrier. One frame is shared cluster-wide, so it encodes at `ACTIVE_VERSIONS[0]`.
#[allow(clippy::too_many_arguments)]
async fn emit_member_removed(
    pool: &Pool,
    keys: &Keys,
    app: &str,
    channel: &str,
    user_id: &str,
    origin_node: &str,
    sharded: bool,
    compat: bool,
    webhooks: &WebhookHandle,
) {
    let frame = crate::protocol::wire::encode(
        crate::protocol::wire::ACTIVE_VERSIONS[0],
        &ServerEvent::MemberRemoved {
            channel: channel.to_string(),
            user_id: user_id.to_string(),
        },
    );
    let env = Envelope {
        node_id: origin_node.to_string(),
        app: app.to_string(),
        kind: EnvelopeKind::Broadcast,
        channel: channel.to_string(),
        event: Value::String(frame.clone()),
        except: None,
        frame_b64: Some(Envelope::encode_frame_b64(&frame)),
    };
    if let Ok(payload) = String::from_utf8(env.encode_with(compat)) {
        if let Err(e) =
            client::publish_channel(pool, &keys.msg(app, channel), payload, sharded).await
        {
            tracing::warn!(error = %e, app, channel, "sweeper: PUBLISH member_removed failed");
        }
    }
    webhooks.enqueue(WebhookEvent::MemberRemoved {
        app: app.to_string(),
        channel: channel.to_string(),
        user_id: user_id.to_string(),
    });
}

/// Cluster roster from `presinfo`: sorted ids, id→user_info hash, distinct count.
pub(super) async fn roster(
    pool: &Pool,
    keys: &Keys,
    app: &str,
    channel: &str,
) -> anyhow::Result<PresencePayload> {
    let entries: Vec<(String, String)> = pool.next().hgetall(keys.presinfo(app, channel)).await?;
    let map: HashMap<String, String> = entries.into_iter().collect();
    let mut ids: Vec<String> = map.keys().cloned().collect();
    ids.sort();
    let mut hash = Map::new();
    for id in &ids {
        let info = map
            .get(id)
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or(Value::Null);
        hash.insert(id.clone(), info);
    }
    Ok(PresencePayload {
        count: ids.len(),
        ids,
        hash,
    })
}
