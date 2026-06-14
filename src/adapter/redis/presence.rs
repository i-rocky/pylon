//! Cross-node presence operations: the atomic join/leave refcount and the cluster
//! roster read. All return `anyhow::Result`; callers fall back to the node-local
//! adapter on error. (members/user_count/reap land in later SP7b tasks.)

use super::client::Scripts;
use super::envelope::Envelope;
use super::keys::{member_token, Keys};
use crate::presence::member::PresenceMember;
use crate::protocol::event::{PresencePayload, ServerEvent};
use crate::protocol::socket_id::SocketId;
use crate::webhook::event::WebhookEvent;
use crate::webhook::WebhookHandle;
use fred::clients::Pool;
use fred::interfaces::{HashesInterface, PubsubInterface};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Run PRESENCE_JOIN and read the cluster roster. Returns `(first_for_user, roster)`.
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
) -> anyhow::Result<(bool, PresencePayload)> {
    let token = member_token(node_id, socket_id.as_str());
    let info = serde_json::to_string(&member.user_info)?;
    let conn: i64 = scripts
        .presence_join
        .evalsha_with_reload::<i64, _, _>(
            pool.next(),
            vec![
                keys.presusers(app, channel),
                keys.presinfo(app, channel),
                keys.presmembers(app, channel),
            ],
            vec![member.user_id.clone(), info, token],
        )
        .await?;
    let roster = roster(pool, keys, app, channel).await?;
    Ok((conn == 1, roster))
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

/// Sweeper crash-time reap of ONE stale presence member token. Decrements the user's
/// cluster refcount; on the →0 edge removes the user + emits `member_removed` (broadcast
/// cross-node via the channel's msg pub/sub, and a webhook). Best-effort: logs + returns
/// on any Redis error, never panics. The broadcast envelope's `node_id` is the DEAD node
/// (the token prefix) so every LIVE node — including this sweeper's — delivers it.
pub(super) async fn reap_member(
    pool: &Pool,
    keys: &Keys,
    app: &str,
    channel: &str,
    token: &str,
    webhooks: &WebhookHandle,
) {
    let presmembers = keys.presmembers(app, channel);
    let user_id: Option<String> = match pool.next().hget(&presmembers, token).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, app, channel, token, "sweeper: HGET presmembers failed");
            return;
        }
    };
    let Some(user_id) = user_id else { return };
    if let Err(e) = pool
        .next()
        .hdel::<i64, _, _>(&presmembers, token.to_string())
        .await
    {
        tracing::warn!(error = %e, app, channel, token, "sweeper: HDEL presmembers failed");
    }
    let conn: i64 = match pool
        .next()
        .hincrby(keys.presusers(app, channel), &user_id, -1)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, app, channel, user_id, "sweeper: HINCRBY presusers failed");
            return;
        }
    };
    if conn <= 0 {
        let _ = pool
            .next()
            .hdel::<i64, _, _>(keys.presusers(app, channel), user_id.clone())
            .await;
        let _ = pool
            .next()
            .hdel::<i64, _, _>(keys.presinfo(app, channel), user_id.clone())
            .await;
        let dead_node = token
            .split_once(':')
            .map(|(n, _)| n.to_string())
            .unwrap_or_default();
        let frame = crate::protocol::v7::frames::encode(&ServerEvent::MemberRemoved {
            channel: channel.to_string(),
            user_id: user_id.clone(),
        });
        let env = Envelope {
            node_id: dead_node,
            app: app.to_string(),
            channel: channel.to_string(),
            event: Value::String(frame),
            except: None,
        };
        if let Ok(payload) = String::from_utf8(env.encode()) {
            if let Err(e) = pool
                .next()
                .publish::<(), _, _>(keys.msg(app, channel), payload)
                .await
            {
                tracing::warn!(error = %e, app, channel, "sweeper: PUBLISH member_removed failed");
            }
        }
        webhooks.enqueue(WebhookEvent::MemberRemoved {
            app: app.to_string(),
            channel: channel.to_string(),
            user_id,
        });
    }
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
