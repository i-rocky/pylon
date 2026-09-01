//! The `User` domain: a connection's signed-in identity, parsed from the
//! `user_data` JSON string a client sends with `pusher:signin`.

use serde_json::Value;

pub mod registry;

/// A connection's authenticated user, parsed from `user_data`. `user_info` is
/// intentionally not split out — it stays inside `user_data_raw`.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthenticatedUser {
    pub id: String,
    /// The exact JSON string the client signed — echoed verbatim in `signin_success`.
    pub user_data_raw: String,
    pub watchlist: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserDataError {
    NotJson,
    MissingId,
}

/// Did this connection bring the user online (offline -> online)?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserJoinOutcome {
    pub first_for_user: bool,
}

/// Did this connection take the user offline (online -> offline)?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserLeaveOutcome {
    pub last_for_user: bool,
}

/// Parse the `user_data` string. Pusher requires `id` to be a NON-EMPTY STRING
/// (pusher-js rejects a non-string id, user.ts:108). `watchlist` is optional;
/// non-string entries are ignored. The 100-entry cap is applied at signin time.
pub fn parse_user_data(raw: &str) -> Result<AuthenticatedUser, UserDataError> {
    let v: Value = serde_json::from_str(raw).map_err(|_| UserDataError::NotJson)?;
    let id = match v.get("id") {
        Some(Value::String(s)) if !s.is_empty() => s.clone(),
        _ => return Err(UserDataError::MissingId),
    };
    let watchlist = v
        .get("watchlist")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    Ok(AuthenticatedUser {
        id,
        user_data_raw: raw.to_string(),
        watchlist,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_user_data() {
        let u = parse_user_data(r#"{"id":"42"}"#).unwrap();
        assert_eq!(u.id, "42");
        assert_eq!(u.user_data_raw, r#"{"id":"42"}"#);
        assert!(u.watchlist.is_empty());
    }

    #[test]
    fn parses_watchlist_of_string_ids() {
        let u = parse_user_data(r#"{"id":"a","watchlist":["b","c"]}"#).unwrap();
        assert_eq!(u.watchlist, vec!["b".to_string(), "c".to_string()]);
    }

    #[test]
    fn rejects_missing_or_non_string_id() {
        // pusher-js requires `typeof id === 'string'` (user.ts:108); numeric id is invalid.
        assert_eq!(
            parse_user_data(r#"{"watchlist":[]}"#),
            Err(UserDataError::MissingId)
        );
        assert_eq!(
            parse_user_data(r#"{"id":42}"#),
            Err(UserDataError::MissingId)
        );
        assert_eq!(
            parse_user_data(r#"{"id":""}"#),
            Err(UserDataError::MissingId)
        );
    }

    #[test]
    fn rejects_non_json() {
        assert_eq!(parse_user_data("nope"), Err(UserDataError::NotJson));
    }
}
