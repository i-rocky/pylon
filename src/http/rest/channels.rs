//! GET /apps/{app_id}/channels and /channels/{name}.

use crate::channel::kind::ChannelInfo;
use crate::http::error::RestError;
use crate::http::rest::auth::authenticate;
use crate::server::router::AppState;
use axum::extract::rejection::QueryRejection;
use axum::extract::{OriginalUri, Path, Query, State};
use axum::Json;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

/// Unwrap the `Result<Query<..>, QueryRejection>` extractor: a query-string
/// rejection (R15) renders the same JSON `{"error","status"}` body as every
/// other REST error instead of axum's plain text.
fn query_params(
    q: Result<Query<HashMap<String, String>>, QueryRejection>,
) -> Result<HashMap<String, String>, RestError> {
    q.map(|Query(p)| p)
        .map_err(|e| RestError::from_rejection(e.status(), e.body_text()))
}

fn wants(params: &HashMap<String, String>, attr: &str) -> bool {
    params
        .get("info")
        .is_some_and(|s| s.split(',').any(|a| a.trim() == attr))
}

pub async fn get_channels(
    State(state): State<AppState>,
    Path(app_id): Path<String>,
    OriginalUri(uri): OriginalUri,
    query: Result<Query<HashMap<String, String>>, QueryRejection>,
) -> Result<Json<Value>, RestError> {
    let params = query_params(query)?;
    let app = authenticate(&state, &app_id, "GET", uri.path(), &params, &[]).await?;
    let prefix = params.get("filter_by_prefix").map(String::as_str);
    let want_user_count = wants(&params, "user_count");
    let want_subscription_count = wants(&params, "subscription_count");
    // Pusher: "If user_count is requested and the request is not limited to
    // presence channels, the API returns 400."
    if want_user_count && !prefix.is_some_and(|p| p.starts_with("presence-")) {
        return Err(RestError::bad_request(
            "user_count is only allowed when filtering by presence channels",
        ));
    }
    let summaries = state.adapter.channels(&app.id, prefix).await;
    let mut chans = Map::new();
    for s in summaries {
        let mut attrs = Map::new();
        if want_user_count {
            if let Some(uc) = s.user_count {
                attrs.insert("user_count".into(), uc.into());
            }
        }
        if want_subscription_count && app.subscription_count_enabled {
            attrs.insert("subscription_count".into(), s.subscription_count.into());
        }
        chans.insert(s.name, Value::Object(attrs));
    }
    Ok(Json(json!({ "channels": chans })))
}

pub async fn get_channel(
    State(state): State<AppState>,
    Path((app_id, channel)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    query: Result<Query<HashMap<String, String>>, QueryRejection>,
) -> Result<Json<Value>, RestError> {
    let params = query_params(query)?;
    let app = authenticate(&state, &app_id, "GET", uri.path(), &params, &[]).await?;
    let s = state.adapter.channel(&app.id, &channel).await;
    let mut out = Map::new();
    out.insert("occupied".into(), Value::Bool(s.occupied));
    if wants(&params, "subscription_count") && app.subscription_count_enabled {
        out.insert("subscription_count".into(), s.subscription_count.into());
    }
    if wants(&params, "user_count") {
        if let Some(uc) = s.user_count {
            out.insert("user_count".into(), uc.into());
        }
    }
    // Pusher info-attributes table: `cache` — Cache channels only — "Cached
    // data and TTL (in seconds) for this channel or null in case the cache is
    // empty." `cache_get` is TTL-aware (local: lazy expiry check; redis: PX
    // key expiry), so an expired entry already reads as the doc's empty case.
    // The reported `ttl` is the channel's cache TTL (`cache_ttl_secs`), the
    // same value the REST trigger cached the event with.
    //
    // Requesting `cache` on a non-cache channel is the inapplicable-attribute
    // 400 (Task 2.7); until that lands the attribute is simply omitted.
    if wants(&params, "cache") && ChannelInfo::of(&channel).cache {
        let cached = state.adapter.cache_get(&app.id, &channel).await;
        let v = match cached {
            Some(e) => json!({ "data": e.data, "ttl": state.config.cache_ttl_secs }),
            None => Value::Null,
        };
        out.insert("cache".into(), v);
    }
    Ok(Json(Value::Object(out)))
}
