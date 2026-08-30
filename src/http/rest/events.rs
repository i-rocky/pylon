//! POST /apps/{app_id}/events and /batch_events.

use crate::channel::cache::CachedEvent;
use crate::channel::kind::{validate_channel_name, AuthKind, ChannelInfo};
use crate::http::error::RestError;
use crate::http::rest::auth::authenticate;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use crate::server::router::AppState;
use axum::body::Bytes;
use axum::extract::rejection::{BytesRejection, QueryRejection};
use axum::extract::{OriginalUri, Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::time::Duration;

/// Unwrap the `Result<Query<..>, QueryRejection>` extractor: a query-string
/// rejection (R15) renders the same JSON `{"error","status"}` body as every
/// other REST error instead of axum's plain text.
fn query_params(
    q: Result<Query<HashMap<String, String>>, QueryRejection>,
) -> Result<HashMap<String, String>, RestError> {
    q.map(|Query(p)| p)
        .map_err(|e| RestError::from_rejection(e.status(), e.body_text()))
}

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

/// R9 — Pusher REST doc (General): "For POST requests, parameters MAY be
/// submitted in the query string but SHOULD be submitted in the POST body as a
/// JSON hash". After parsing the JSON body, any top-level trigger field the
/// body does NOT carry falls back to the query map (already URL-decoded by
/// axum's `Query` extractor); on conflict the body always wins.
///
/// `data` is byte-identical either way: the query value `data=%22hi%22` decodes
/// to the string `"hi"` (quotes included) — exactly the string the body form
/// `{"data":"\"hi\""}` carries — so both sources feed the same
/// [`TriggerBody::data`] and downstream validation/delivery unchanged.
///
/// Scope notes:
/// - `channels` accepts a single plain `channels=<name>` query value as a
///   one-element list. The doc's repeated `channels[]=a&channels[]=b` form is
///   NOT supported: the query map collapses duplicate keys (last wins), which
///   would silently DROP channels — a multi-channel trigger must use the body.
/// - `/batch_events` deliberately has NO fallback (see `post_batch`): its sole
///   parameter `batch` is an array of event objects with no documented
///   query-string representation — the doc's arrays-in-query note
///   (`channels[]=…`) appears only under the single trigger endpoint.
fn merged_trigger_body(
    body: &[u8],
    params: &HashMap<String, String>,
) -> Result<TriggerBody, RestError> {
    // An all-query trigger sends an empty body; treat it as `{}` so every field
    // can come from the query map.
    let mut root: Value = if body.is_empty() {
        Value::Object(Map::new())
    } else {
        serde_json::from_slice(body).map_err(|_| RestError::bad_request("invalid request body"))?
    };
    {
        let Some(obj) = root.as_object_mut() else {
            return Err(RestError::bad_request("invalid request body"));
        };
        for key in ["name", "data", "channel", "socket_id", "info"] {
            if !obj.contains_key(key) {
                if let Some(v) = params.get(key) {
                    obj.insert(key.to_string(), Value::String(v.clone()));
                }
            }
        }
        if !obj.contains_key("channels") {
            if let Some(c) = params.get("channels") {
                obj.insert(
                    "channels".to_string(),
                    Value::Array(vec![Value::String(c.clone())]),
                );
            }
        }
    }
    serde_json::from_value(root).map_err(|_| RestError::bad_request("invalid request body"))
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
    let except = socket_id.map(SocketId::from_raw);
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
    subscription_count_enabled: bool,
) -> Map<String, Value> {
    let mut attrs = Map::new();
    let want_sub = wants(info, "subscription_count") && subscription_count_enabled;
    let want_uc = wants(info, "user_count");
    if want_sub || want_uc {
        let s = state.adapter.channel(app_id, channel).await;
        if want_sub {
            attrs.insert("subscription_count".into(), s.subscription_count.into());
        }
        if want_uc {
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
    query: Result<Query<HashMap<String, String>>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<Value>, RestError> {
    // Map the body-limit rejection (413) into a RestError so it renders the
    // same JSON error body as every other REST error.
    let body = body.map_err(|e| RestError::from_rejection(e.status(), e.body_text()))?;
    let params = query_params(query)?;
    let app = authenticate(&state, &app_id, "POST", uri.path(), &params, &body).await?;
    // SP10 admission control: under sustained overload the percore broadcast
    // pipeline is saturated — reject the publish (503 + Retry-After) instead of
    // broadcasting, so the publisher backs off and the fast path is never
    // throttled. Off-percore `is_saturated()` is always false (no-op).
    if state.is_saturated() {
        return Err(RestError::service_unavailable("Server overloaded"));
    }
    let t: TriggerBody = merged_trigger_body(&body, &params)?;
    if t.data.len() > state.config.max_event_payload_bytes {
        return Err(RestError::payload_too_large("Event message over 10k"));
    }
    // P9: enforce event-name length cap.
    if t.name.len() > state.config.max_event_name_length {
        return Err(RestError::bad_request("Event name too long"));
    }
    let channels = match (&t.channels, &t.channel) {
        (Some(list), _) => list.clone(),
        (None, Some(c)) => vec![c.clone()],
        (None, None) => return Err(RestError::bad_request("must provide channel or channels")),
    };
    if channels.is_empty() || channels.len() > state.config.max_channels_per_publish {
        return Err(RestError::bad_request("invalid channel count"));
    }
    // P8: validate every channel name (length + charset).
    // `#server-to-user-` channels are a special reserved namespace handled by
    // `deliver()` and are exempt from the normal charset check (they start with `#`).
    for ch in &channels {
        if !ch.starts_with(crate::channel::kind::SERVER_TO_USER_PREFIX)
            && !validate_channel_name(ch, state.config.max_channel_name_length)
        {
            return Err(RestError::bad_request("Invalid channel name"));
        }
    }
    // Encrypted channels must be triggered solo — no mixing with any other channel.
    let encrypted = channels
        .iter()
        .filter(|c| ChannelInfo::of(c).auth == AuthKind::PrivateEncrypted)
        .count();
    if encrypted >= 1 && channels.len() > 1 {
        return Err(RestError::bad_request(
            "Cannot trigger to multiple channels when using encrypted channels",
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
                Value::Object(
                    channel_attrs(
                        &state,
                        &app.id,
                        ch,
                        t.info.as_deref(),
                        app.subscription_count_enabled,
                    )
                    .await,
                ),
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
    query: Result<Query<HashMap<String, String>>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<Value>, RestError> {
    // Map the body-limit rejection (413) into a RestError (see `post_events`).
    let body = body.map_err(|e| RestError::from_rejection(e.status(), e.body_text()))?;
    let params = query_params(query)?;
    let app = authenticate(&state, &app_id, "POST", uri.path(), &params, &body).await?;
    // SP10 admission control: reject under saturation (see `post_events`).
    if state.is_saturated() {
        return Err(RestError::service_unavailable("Server overloaded"));
    }
    // NB (R9): unlike `post_events`, there is NO query-string fallback here —
    // `batch` is an array of event objects with no documented query encoding
    // (the doc's `channels[]=` note covers only the single trigger endpoint),
    // so the JSON body is the sole source. Pinned by
    // `rest_batch_events_empty_body_still_400`.
    let b: BatchBody = serde_json::from_slice(&body)
        .map_err(|_| RestError::bad_request("invalid request body"))?;
    if b.batch.is_empty() || b.batch.len() > state.config.max_batch_events {
        return Err(RestError::bad_request("invalid batch size"));
    }
    for item in &b.batch {
        if item.data.len() > state.config.max_event_payload_bytes {
            return Err(RestError::payload_too_large("Event message over 10k"));
        }
    }
    // P9: enforce event-name length cap for every batch item.
    for item in &b.batch {
        if item.name.len() > state.config.max_event_name_length {
            return Err(RestError::bad_request("Event name too long"));
        }
    }
    // P8: validate every channel name (length + charset).
    // `#server-to-user-` channels are exempt (handled as special reserved namespace).
    for item in &b.batch {
        if !item
            .channel
            .starts_with(crate::channel::kind::SERVER_TO_USER_PREFIX)
            && !validate_channel_name(&item.channel, state.config.max_channel_name_length)
        {
            return Err(RestError::bad_request("Invalid channel name"));
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
                channel_attrs(
                    &state,
                    &app.id,
                    &item.channel,
                    item.info.as_deref(),
                    app.subscription_count_enabled,
                )
                .await,
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

    fn q(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    /// R9: empty body + trigger fields in the query map parses fully from the
    /// query (the `%22hi%22` wire form is ALREADY decoded by the extractor, so
    /// `data` arrives as the quoted string, matching the body form).
    #[test]
    fn merged_trigger_body_fills_from_query_when_body_empty() {
        let params = q(&[
            ("name", "qse"),
            ("channel", "qc"),
            ("data", "\"hi\""),
            ("socket_id", "1234.5678"),
            ("info", "user_count"),
        ]);
        let t = merged_trigger_body(b"", &params).unwrap();
        assert_eq!(t.name, "qse");
        assert_eq!(t.channel.as_deref(), Some("qc"));
        assert_eq!(t.data, "\"hi\"");
        assert_eq!(t.socket_id.as_deref(), Some("1234.5678"));
        assert_eq!(t.info.as_deref(), Some("user_count"));
        assert!(t.channels.is_none());
    }

    /// R9 precedence: fields present in the body win; the query fills only the
    /// absent ones.
    #[test]
    fn merged_trigger_body_body_wins_on_conflict() {
        let body = br#"{"name":"ev","data":"{}","channel":"body-ch"}"#;
        let params = q(&[("channel", "query-ch"), ("name", "query-name")]);
        let t = merged_trigger_body(body, &params).unwrap();
        assert_eq!(t.name, "ev");
        assert_eq!(t.channel.as_deref(), Some("body-ch"));
    }

    /// A plain `channels=<name>` query value becomes a one-element list; the
    /// doc's repeated `channels[]=` form is unsupported (duplicate keys collapse
    /// in the query map — see the handler note).
    #[test]
    fn merged_trigger_body_query_channels_becomes_single_element_list() {
        let params = q(&[("name", "e"), ("data", "1"), ("channels", "solo")]);
        let t = merged_trigger_body(b"", &params).unwrap();
        assert_eq!(t.channels.as_deref(), Some(&["solo".to_string()][..]));
    }

    /// Neither body nor query carries the required fields → the same 400 the
    /// invalid-body path has always produced.
    #[test]
    fn merged_trigger_body_missing_fields_is_400() {
        assert!(merged_trigger_body(b"", &HashMap::new()).is_err());
        assert!(merged_trigger_body(b"definitely not json", &HashMap::new()).is_err());
        // Valid JSON but not an object (and no query rescue possible).
        assert!(merged_trigger_body(b"[1,2]", &HashMap::new()).is_err());
    }
}
