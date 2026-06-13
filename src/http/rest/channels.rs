//! GET /apps/{app_id}/channels and /channels/{name}.

use crate::http::error::RestError;
use crate::http::rest::auth::authenticate;
use crate::server::router::AppState;
use axum::extract::{OriginalUri, Path, Query, State};
use axum::Json;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

fn wants(params: &HashMap<String, String>, attr: &str) -> bool {
    params
        .get("info")
        .is_some_and(|s| s.split(',').any(|a| a.trim() == attr))
}

pub async fn get_channels(
    State(state): State<AppState>,
    Path(app_id): Path<String>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, RestError> {
    let app = authenticate(&state, &app_id, "GET", uri.path(), &params, &[]).await?;
    let prefix = params.get("filter_by_prefix").map(String::as_str);
    let want_user_count = wants(&params, "user_count");
    let summaries = state.adapter.channels(&app.id, prefix).await;
    let mut chans = Map::new();
    for s in summaries {
        let mut attrs = Map::new();
        if want_user_count {
            if let Some(uc) = s.user_count {
                attrs.insert("user_count".into(), uc.into());
            }
        }
        chans.insert(s.name, Value::Object(attrs));
    }
    Ok(Json(json!({ "channels": chans })))
}

pub async fn get_channel(
    State(state): State<AppState>,
    Path((app_id, channel)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, RestError> {
    let app = authenticate(&state, &app_id, "GET", uri.path(), &params, &[]).await?;
    let s = state.adapter.channel(&app.id, &channel).await;
    let mut out = Map::new();
    out.insert("occupied".into(), Value::Bool(s.occupied));
    if wants(&params, "subscription_count") {
        out.insert("subscription_count".into(), s.subscription_count.into());
    }
    if wants(&params, "user_count") {
        if let Some(uc) = s.user_count {
            out.insert("user_count".into(), uc.into());
        }
    }
    Ok(Json(Value::Object(out)))
}
