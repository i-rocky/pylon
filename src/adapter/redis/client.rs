//! fred v10 client wiring for the Redis adapter.
//!
//! Holds the command pool (one [`Pool`] of `pool_size` connections) used for all
//! ordinary commands + PUBLISH, and a dedicated [`SubscriberClient`] for the
//! pub/sub side. The subscriber's resubscribe task ([`SubscriberClient::manage_subscriptions`])
//! is kept alive by storing its [`JoinHandle`] — dropping it would stop the
//! automatic re-subscribe on reconnect.

use fred::clients::{Pool, SubscriberClient};
use fred::error::Error as FredError;
use fred::prelude::*;
use fred::types::scripts::Script;
use tokio::task::JoinHandle;

/// Publish `payload` on a Pylon pub/sub channel (`msg` / `usermsg` / `watch`),
/// honoring the `PYLON_REDIS_SHARDED_PUBSUB` flag: Redis 7 `SPUBLISH` when set,
/// ordinary `PUBLISH` otherwise. The two commands address SEPARATE namespaces —
/// `SPUBLISH` reaches only `SSUBSCRIBE`rs, `PUBLISH` only `SUBSCRIBE`rs — so
/// every node of a cluster must run with the same flag. Returns the underlying
/// result; the per-site error handling (log + keep going, never fatal) stays at
/// the call sites.
pub(crate) async fn publish_channel(
    pool: &Pool,
    channel: &str,
    payload: String,
    sharded: bool,
) -> Result<(), FredError> {
    if sharded {
        pool.next()
            .spublish::<(), _, _>(channel.to_string(), payload)
            .await
    } else {
        pool.next()
            .publish::<(), _, _>(channel.to_string(), payload)
            .await
    }
}

/// The connected fred clients for one Redis adapter instance.
pub struct RedisClients {
    /// Connection pool for ordinary commands and PUBLISH.
    pub pool: Pool,
    /// Dedicated subscriber client for the pub/sub fan-in.
    pub sub: SubscriberClient,
    /// Background task that re-subscribes the subscriber after a reconnect.
    /// Kept alive for the lifetime of the adapter — never `.await`ed.
    pub sub_manager: JoinHandle<()>,
}

impl RedisClients {
    /// Connect to Redis at `redis_url` with a pool of `pool_size` connections.
    ///
    /// Uses an exponential reconnect policy (min 100ms, max 30s, base 2,
    /// unlimited attempts). Initializes both the pool and the subscriber, and
    /// spawns the subscriber's resubscribe-on-reconnect task.
    pub async fn connect(redis_url: &str, pool_size: u32) -> anyhow::Result<RedisClients> {
        let config = Config::from_url(redis_url)?;
        // `max_attempts = 0` means retry forever; min 100ms, max 30s, base 2.
        let policy = ReconnectPolicy::new_exponential(0, 100, 30_000, 2);

        let mut builder = Builder::from_config(config);
        builder.set_policy(policy);

        let pool = builder.build_pool(pool_size as usize)?;
        let sub = builder.build_subscriber_client()?;

        pool.init().await?;
        sub.init().await?;

        // Keep the resubscribe task handle so it isn't dropped (which would stop it).
        let sub_manager = sub.manage_subscriptions();

        Ok(RedisClients {
            pool,
            sub,
            sub_manager,
        })
    }
}

/// SUBSCRIBE membership script. Records this member in the channel's occupancy
/// hash, refreshes the whole-key TTL backstop, and — on the cluster 0→1 edge —
/// indexes the channel in the app's active-channels set. Returns the new `HLEN`
/// (the authoritative cluster-wide subscription count).
///
/// `KEYS[1]` = occ hash, `KEYS[2]` = chans set.
/// `ARGV[1]` = member_token, `ARGV[2]` = expire_at_ms, `ARGV[3]` = ttl_secs,
/// `ARGV[4]` = channel.
const SUBSCRIBE_LUA: &str = r#"
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
local count = redis.call('HLEN', KEYS[1])
if count == 1 then redis.call('SADD', KEYS[2], ARGV[4]) end
return count
"#;

/// UNSUBSCRIBE membership script. Removes this member from the occupancy hash and
/// — on the cluster 1→0 edge — deletes the now-empty hash and de-indexes the
/// channel. Returns `{ remaining_count, won }`: the remaining `HLEN` (the
/// authoritative cluster-wide count) plus the VACATE CAS flag — `won == 1` iff
/// THIS call's `SREM` actually removed the channel from the `chans` index, i.e.
/// this caller owns the single cluster-wide `channel_vacated` emission right
/// (see [`VACATE_LUA`]; Redis serializes scripts, so exactly one of the two
/// vacating writers can ever observe `SREM == 1`).
///
/// `KEYS[1]` = occ hash, `KEYS[2]` = chans set.
/// `ARGV[1]` = member_token, `ARGV[2]` = channel.
const UNSUBSCRIBE_LUA: &str = r#"
redis.call('HDEL', KEYS[1], ARGV[1])
local count = redis.call('HLEN', KEYS[1])
local won = 0
if count <= 0 then
  redis.call('DEL', KEYS[1])
  won = redis.call('SREM', KEYS[2], ARGV[2])
end
return {count, won}
"#;

/// VACATE CAS script (the sweeper's orphan reclaim). Atomically decides AND
/// performs the vacate: if the occ hash holds no members (or is already gone),
/// DEL it and `SREM` the channel from the `chans` index, returning `1` — the
/// single cluster-wide `channel_vacated` emission right — ONLY if THIS call's
/// SREM actually removed the entry. Returns `0` when a member (re-)appeared, or
/// when another writer (the last-unsubscribe's [`UNSUBSCRIBE_LUA`]) already
/// removed the entry and therefore already owns the emission. Together the two
/// scripts guarantee exactly one `channel_vacated` per vacancy in every
/// interleaving.
///
/// `KEYS[1]` = occ hash, `KEYS[2]` = chans set.
/// `ARGV[1]` = channel.
const VACATE_LUA: &str = r#"
if redis.call('HLEN', KEYS[1]) == 0 then
  redis.call('DEL', KEYS[1])
  return redis.call('SREM', KEYS[2], ARGV[1])
end
return 0
"#;

/// PRESENCE_JOIN. Records this connection's member, bumps the user's cluster-wide
/// connection refcount, and on the 0→1 user edge stores the user_info for the roster.
/// Returns the new refcount (== 1 means first_for_user → emit member_added).
/// KEYS\[1\]=presusers KEYS\[2\]=presinfo KEYS\[3\]=presmembers
/// ARGV\[1\]=user_id ARGV\[2\]=user_info ARGV\[3\]=member_token
const PRESENCE_JOIN_LUA: &str = r#"
redis.call('HSET', KEYS[3], ARGV[3], ARGV[1])
local conn = redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
if conn == 1 then redis.call('HSET', KEYS[2], ARGV[1], ARGV[2]) end
return conn
"#;

/// PRESENCE_LEAVE. Drops this connection's member and decrements the user's refcount;
/// on the →0 user edge removes the user from presusers + presinfo. Returns the
/// remaining refcount (== 0 means last_for_user → emit member_removed).
/// KEYS\[1\]=presusers KEYS\[2\]=presinfo KEYS\[3\]=presmembers
/// ARGV\[1\]=user_id ARGV\[2\]=member_token
const PRESENCE_LEAVE_LUA: &str = r#"
redis.call('HDEL', KEYS[3], ARGV[2])
local conn = redis.call('HINCRBY', KEYS[1], ARGV[1], -1)
if conn <= 0 then redis.call('HDEL', KEYS[1], ARGV[1]); redis.call('HDEL', KEYS[2], ARGV[1]) end
return conn
"#;

/// USER_SIGNIN. Records this connection's binding token, refreshes the whole-key
/// TTL backstop, and — on the cluster 0→1 user edge (HLEN == 1) — indexes the user
/// in the app's `users` set. Returns the new `HLEN` (cluster-wide connection count).
///
/// `KEYS[1]` = usr hash, `KEYS[2]` = users set.
/// `ARGV[1]` = member_token, `ARGV[2]` = expire_at_ms, `ARGV[3]` = ttl_secs,
/// `ARGV[4]` = user_id.
const USER_SIGNIN_LUA: &str = r#"
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
local conn = redis.call('HLEN', KEYS[1])
if conn == 1 then redis.call('SADD', KEYS[2], ARGV[4]) end
return conn
"#;

/// USER_SIGNOUT. Removes this connection's binding token and — on the cluster 1→0
/// user edge — deletes the now-empty hash and de-indexes the user. Returns the
/// remaining `HLEN` (authoritative cluster-wide connection count).
///
/// `KEYS[1]` = usr hash, `KEYS[2]` = users set.
/// `ARGV[1]` = member_token, `ARGV[2]` = user_id.
const USER_SIGNOUT_LUA: &str = r#"
redis.call('HDEL', KEYS[1], ARGV[1])
local conn = redis.call('HLEN', KEYS[1])
if conn <= 0 then redis.call('DEL', KEYS[1]); redis.call('SREM', KEYS[2], ARGV[2]) end
return conn
"#;

/// APP ADMIT (Task 4.2 / finding D2): the cluster-wide per-app capacity gate.
/// Atomically checks the CLUSTER count (`appconns`) against the app's capacity
/// and, when there is room, takes one unit there AND on the admitting node's
/// own per-app hash (which also re-arms that hash's TTL backstop). Returns `1`
/// when admitted, `0` when the app is at capacity (no state changed).
/// `capacity <= 0` means unlimited: the check is skipped but the unit is still
/// recorded, so every admitted connection has exactly one matching release.
///
/// `KEYS[1]` = appconns hash, `KEYS[2]` = nodeconns:{node} hash.
/// `ARGV[1]` = app_id, `ARGV[2]` = capacity, `ARGV[3]` = nodeconns ttl_secs.
const ADMIT_APP_LUA: &str = r#"
local cap = tonumber(ARGV[2])
if cap ~= nil and cap > 0 then
  local cur = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0')
  if cur >= cap then return 0 end
end
redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
redis.call('HINCRBY', KEYS[2], ARGV[1], 1)
redis.call('EXPIRE', KEYS[2], ARGV[3])
return 1
"#;

/// APP RELEASE (Task 4.2 / finding D2): floor-0 give-back of one unit on both
/// the node's per-app hash and the cluster total — never negative. NODE-GUARDED:
/// the cluster total is decremented only when this node actually held a unit, so
/// a phantom release (e.g. an admission that failed open, or a capacity config
/// that changed between establish and close) can never steal a unit another node
/// legitimately holds. Fields that reach 0 are HDEL'd so the hashes stay tidy.
/// Returns the remaining cluster total for the app.
///
/// `KEYS[1]` = appconns hash, `KEYS[2]` = nodeconns:{node} hash.
/// `ARGV[1]` = app_id.
const RELEASE_APP_LUA: &str = r#"
local node = redis.call('HINCRBY', KEYS[2], ARGV[1], -1)
if node < 0 then
  redis.call('HDEL', KEYS[2], ARGV[1])
  return 0
end
if node == 0 then redis.call('HDEL', KEYS[2], ARGV[1]) end
local total = redis.call('HINCRBY', KEYS[1], ARGV[1], -1)
if total <= 0 then redis.call('HDEL', KEYS[1], ARGV[1]) end
return total
"#;

/// DEAD-NODE RECLAIM (Task 4.2 / finding D2, run by the sweeper): subtract a
/// dead node's per-app counts from the cluster totals, floored at 0 per app
/// (never negative), then delete the dead node's hash. One script = the whole
/// read-subtract-delete decision is atomic, so it cannot straddle a concurrent
/// admission. Returns the number of apps reclaimed.
///
/// `KEYS[1]` = appconns hash, `KEYS[2]` = nodeconns:{dead_node} hash.
const RECLAIM_NODE_LUA: &str = r#"
local counts = redis.call('HGETALL', KEYS[2])
for i = 1, #counts, 2 do
  local app = counts[i]
  local n = tonumber(counts[i + 1]) or 0
  local total = tonumber(redis.call('HGET', KEYS[1], app) or '0')
  if total <= n then
    redis.call('HDEL', KEYS[1], app)
  else
    redis.call('HINCRBY', KEYS[1], app, -n)
  end
end
redis.call('DEL', KEYS[2])
return math.floor(#counts / 2)
"#;

/// The membership/presence Lua scripts, compiled (SHA-1 hashed) at adapter build
/// time. `Script::from_lua` is purely local — no Redis round-trip — and the scripts
/// are loaded lazily on first use via `evalsha_with_reload`'s NOSCRIPT fallback.
pub struct Scripts {
    /// Records a member and returns the new cluster-wide subscription count.
    pub subscribe: Script,
    /// Removes a member and returns `{remaining cluster-wide count, vacate-CAS won}`.
    pub unsubscribe: Script,
    /// The sweeper's atomic vacate: returns 1 iff THIS call won the
    /// `channel_vacated` emission right (its SREM removed the chans entry).
    pub vacate: Script,
    /// Records a presence join and returns the user's new connection refcount.
    pub presence_join: Script,
    /// Records a presence leave and returns the user's remaining connection refcount.
    pub presence_leave: Script,
    /// Records a user signin and returns the user's new cluster connection count.
    pub user_signin: Script,
    /// Records a user signout and returns the user's remaining cluster connection count.
    pub user_signout: Script,
    /// Cluster-wide per-app capacity gate: returns 1 when admitted (unit taken
    /// on both hashes), 0 when the app is at capacity.
    pub admit_app: Script,
    /// Floor-0, node-guarded give-back of one per-app unit on both hashes.
    pub release_app: Script,
    /// Sweeper's dead-node reclaim: subtracts a dead node's per-app counts from
    /// the cluster totals (floored at 0) and deletes its hash.
    pub reclaim_node: Script,
}

impl Scripts {
    /// Compile the membership scripts. No Redis access — just SHA-1 hashing.
    pub fn new() -> Self {
        Self {
            subscribe: Script::from_lua(SUBSCRIBE_LUA),
            unsubscribe: Script::from_lua(UNSUBSCRIBE_LUA),
            vacate: Script::from_lua(VACATE_LUA),
            presence_join: Script::from_lua(PRESENCE_JOIN_LUA),
            presence_leave: Script::from_lua(PRESENCE_LEAVE_LUA),
            user_signin: Script::from_lua(USER_SIGNIN_LUA),
            user_signout: Script::from_lua(USER_SIGNOUT_LUA),
            admit_app: Script::from_lua(ADMIT_APP_LUA),
            release_app: Script::from_lua(RELEASE_APP_LUA),
            reclaim_node: Script::from_lua(RECLAIM_NODE_LUA),
        }
    }
}

impl Default for Scripts {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scripts_compile_including_presence() {
        let s = Scripts::new();
        assert_ne!(s.presence_join.sha1(), s.presence_leave.sha1());
        assert_ne!(s.subscribe.sha1(), s.presence_join.sha1());
    }

    #[test]
    fn scripts_compile_including_user() {
        let s = Scripts::new();
        assert_ne!(s.user_signin.sha1(), s.user_signout.sha1());
        assert_ne!(s.user_signin.sha1(), s.subscribe.sha1());
    }

    #[test]
    fn scripts_compile_including_vacate() {
        let s = Scripts::new();
        assert_ne!(s.vacate.sha1(), s.unsubscribe.sha1());
        assert_ne!(s.vacate.sha1(), s.subscribe.sha1());
    }

    #[test]
    fn scripts_compile_including_app_capacity() {
        let s = Scripts::new();
        assert_ne!(s.admit_app.sha1(), s.release_app.sha1());
        assert_ne!(s.admit_app.sha1(), s.reclaim_node.sha1());
        assert_ne!(s.admit_app.sha1(), s.subscribe.sha1());
    }
}
