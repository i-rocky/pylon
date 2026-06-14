//! POST /apps/{app_id}/events and /batch_events.

use crate::channel::cache::CachedEvent;
use crate::channel::kind::{AuthKind, ChannelInfo};
use crate::http::error::RestError;
use crate::http::rest::auth::authenticate;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use crate::server::router::AppState;
use axum::body::Bytes;
use axum::extract::{OriginalUri, Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Deserialize)]
struct TriggerBody {
    name: String,
    data: String,
    #[serde(default)]
    channels: Option<Vec<String>>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    socket_id: Option<String>,
    #[serde(default)]
    info: Option<String>,
}

#[derive(Deserialize)]
struct BatchBody {
    batch: Vec<BatchItem>,
}

#[derive(Deserialize)]
struct BatchItem {
    name: String,
    data: String,
    channel: String,
    #[serde(default)]
    socket_id: Option<String>,
    #[serde(default)]
    info: Option<String>,
}

fn wants(info: Option<&str>, attr: &str) -> bool {
    info.is_some_and(|s| s.split(',').any(|a| a.trim() == attr))
}

/// Broadcast one event string to a channel, excluding `socket_id` if present.
async fn deliver(
    state: &AppState,
    app_id: &str,
    channel: &str,
    name: &str,
    data: &str,
    socket_id: Option<&str>,
) {
    // Server-to-user: a `sendToUser` REST trigger targets `#server-to-user-<id>`,
    // which is never a registry channel. Route it to the user's live connections
    // via the user registry instead of broadcasting (and never cache it). The
    // delivered frame is byte-identical to a normal channel event so pusher-js's
    // `#server-to-user-<id>` handler processes it.
    if let Some(user_id) = channel.strip_prefix(crate::channel::kind::SERVER_TO_USER_PREFIX) {
        // Reject a malformed empty user id (e.g. exactly "#server-to-user-"):
        // deliver to nobody rather than returning a misleading 200-with-no-effect.
        if user_id.is_empty() {
            return;
        }
        // NB: `socket_id` exclusion is intentionally NOT applied to server-to-user
        // delivery — there is no "originating socket" among the user's connections
        // (the trigger comes from the server via REST). Matches soketi's user-channel path.
        state
            .adapter
            .send_to_user(
                app_id,
                user_id,
                ServerEvent::ChannelEvent {
                    channel: channel.to_string(),
                    event: name.to_string(),
                    data: Value::String(data.to_string()),
                    user_id: None,
                },
            )
            .await;
        return;
    }
    let except = socket_id.map(|s| SocketId::from_raw(s.to_string()));
    state
        .adapter
        .broadcast(
            app_id,
            channel,
            ServerEvent::ChannelEvent {
                channel: channel.to_string(),
                event: name.to_string(),
                data: Value::String(data.to_string()),
                user_id: None,
            },
            except,
        )
        .await;
    // Cache channels retain their last event for replay to new subscribers.
    if ChannelInfo::of(channel).cache {
        state
            .adapter
            .cache_set(
                app_id,
                channel,
                CachedEvent {
                    event: name.to_string(),
                    data: data.to_string(),
                },
                Duration::from_secs(state.config.cache_ttl_secs),
            )
            .await;
    }
}

/// Build the per-channel `info` attributes object (empty if nothing requested).
async fn channel_attrs(
    state: &AppState,
    app_id: &str,
    channel: &str,
    info: Option<&str>,
) -> Map<String, Value> {
    let mut attrs = Map::new();
    if wants(info, "subscription_count") || wants(info, "user_count") {
        let s = state.adapter.channel(app_id, channel).await;
        if wants(info, "subscription_count") {
            attrs.insert("subscription_count".into(), s.subscription_count.into());
        }
        if wants(info, "user_count") {
            if let Some(uc) = s.user_count {
                attrs.insert("user_count".into(), uc.into());
            }
        }
    }
    attrs
}

pub async fn post_events(
    State(state): State<AppState>,
    Path(app_id): Path<String>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Json<Value>, RestError> {
    let app = authenticate(&state, &app_id, "POST", uri.path(), &params, &body).await?;
    let t: TriggerBody = serde_json::from_slice(&body)
        .map_err(|_| RestError::bad_request("invalid request body"))?;
    if t.data.len() > state.config.max_event_payload_bytes {
        return Err(RestError::bad_request("event data too large"));
    }
    let channels = match (&t.channels, &t.channel) {
        (Some(list), _) => list.clone(),
        (None, Some(c)) => vec![c.clone()],
        (None, None) => return Err(RestError::bad_request("must provide channel or channels")),
    };
    if channels.is_empty() || channels.len() > state.config.max_channels_per_publish {
        return Err(RestError::bad_request("invalid channel count"));
    }
    // Hosted Pusher forbids triggering to more than one encrypted channel at once.
    let encrypted = channels
        .iter()
        .filter(|c| ChannelInfo::of(c).auth == AuthKind::PrivateEncrypted)
        .count();
    if encrypted > 1 {
        return Err(RestError::bad_request(
            "cannot trigger to more than one encrypted channel",
        ));
    }
    for ch in &channels {
        deliver(
            &state,
            &app.id,
            ch,
            &t.name,
            &t.data,
            t.socket_id.as_deref(),
        )
        .await;
    }
    let mut out = Map::new();
    if t.info.is_some() {
        let mut chans = Map::new();
        for ch in &channels {
            chans.insert(
                ch.clone(),
                Value::Object(channel_attrs(&state, &app.id, ch, t.info.as_deref()).await),
            );
        }
        out.insert("channels".into(), Value::Object(chans));
    }
    Ok(Json(Value::Object(out)))
}

pub async fn post_batch(
    State(state): State<AppState>,
    Path(app_id): Path<String>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Json<Value>, RestError> {
    let app = authenticate(&state, &app_id, "POST", uri.path(), &params, &body).await?;
    let b: BatchBody = serde_json::from_slice(&body)
        .map_err(|_| RestError::bad_request("invalid request body"))?;
    if b.batch.is_empty() || b.batch.len() > state.config.max_batch_events {
        return Err(RestError::bad_request("invalid batch size"));
    }
    for item in &b.batch {
        if item.data.len() > state.config.max_event_payload_bytes {
            return Err(RestError::bad_request("event data too large"));
        }
    }
    for item in &b.batch {
        deliver(
            &state,
            &app.id,
            &item.channel,
            &item.name,
            &item.data,
            item.socket_id.as_deref(),
        )
        .await;
    }
    let any_info = b.batch.iter().any(|i| i.info.is_some());
    let mut out = Map::new();
    if any_info {
        let mut arr = Vec::new();
        for item in &b.batch {
            arr.push(Value::Object(
                channel_attrs(&state, &app.id, &item.channel, item.info.as_deref()).await,
            ));
        }
        out.insert("batch".into(), Value::Array(arr));
    }
    Ok(Json(Value::Object(out)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wants_parses_csv() {
        assert!(wants(Some("user_count,subscription_count"), "user_count"));
        assert!(wants(Some("subscription_count"), "subscription_count"));
        assert!(!wants(Some("user_count"), "subscription_count"));
        assert!(!wants(None, "user_count"));
    }
}
