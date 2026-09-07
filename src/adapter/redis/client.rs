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

/// MEMBERSHIP JOIN script, shared by a channel subscribe (`occ` + `chans`) and a
/// user signin (`usr` + `users`) — the two membership hashes have the same shape,
/// the same TTL backstop and the same app-level enumeration index, so they take
/// the same program. Records the connection's token, refreshes the hash's
/// whole-key TTL backstop, indexes the hash's subject, and returns the new `HLEN`
/// (the authoritative cluster-wide count).
///
/// The `SADD` is unconditional rather than gated on the cluster 0→1 edge. For a
/// channel that index entry IS the CAS the single cluster-wide `channel_vacated`
/// is won on ([`UNSUBSCRIBE_LUA`]), and for a user it is the sweeper's only
/// enumeration of who is signed in; an entry lost while the subject stayed
/// occupied — a Redis restart, a dropped bridge command, a sweeper false-reap —
/// would silence both for good. Every join re-asserts it instead.
///
/// `KEYS[1]` = membership hash (`occ` / `usr`), `KEYS[2]` = index set
/// (`chans` / `users`). `ARGV[1]` = member_token, `ARGV[2]` = expire_at_ms,
/// `ARGV[3]` = key_ttl_secs, `ARGV[4]` = index member (channel / user_id).
const MEMBERSHIP_JOIN_LUA: &str = r#"
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('SADD', KEYS[2], ARGV[4])
return redis.call('HLEN', KEYS[1])
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

/// VACATE CAS script (the sweeper's orphan reclaim). With the occ hash empty (or
/// already gone) it DELs the hash and `SREM`s the channel from the `chans` index;
/// the caller whose SREM actually removed the entry owns the single cluster-wide
/// `channel_vacated`. A member that (re-)appeared, or an entry another writer
/// (the last-unsubscribe's [`UNSUBSCRIBE_LUA`]) already removed, yields `won == 0`.
///
/// That same winner DRAINS the channel's presence side-tables in the same script,
/// returning every user still on the roster — each owed one `member_removed`.
/// `chans` is the only index without a TTL, so the SREM that de-indexes the channel
/// is the last instant anything can still reach those hashes; doing both under one
/// script is what stops them outliving the membership they describe, and keeps the
/// drain on the single winner rather than every racing sweeper.
///
/// `KEYS[1]` = occ hash, `KEYS[2]` = chans set, `KEYS[3]` = presusers,
/// `KEYS[4]` = presinfo, `KEYS[5]` = presmembers. `ARGV[1]` = channel.
/// Returns `{won, drained_user_ids}`; a non-presence channel drains empty.
const VACATE_LUA: &str = r#"
if redis.call('HLEN', KEYS[1]) ~= 0 then return {0, {}} end
redis.call('DEL', KEYS[1])
if redis.call('SREM', KEYS[2], ARGV[1]) == 0 then return {0, {}} end
local roster = redis.call('HKEYS', KEYS[3])
redis.call('DEL', KEYS[3], KEYS[4], KEYS[5])
return {1, roster}
"#;

/// PRESENCE_JOIN. Decides the cluster-wide distinct-user cap and, when the join is
/// admitted, records this connection's member, bumps the user's cluster-wide connection
/// refcount and on the 0→1 user edge stores the user_info for the roster.
///
/// Deciding the cap HERE is what makes it a real ceiling: Redis serializes scripts, so a
/// new distinct user is weighed against `HLEN presusers` and committed in one indivisible
/// step. Split across a probe and a later write, two nodes admitting concurrently both
/// read room and both commit.
///
/// Returns `-1` when the cap rejected the join — nothing was written — else the user's new
/// refcount (`1` means first_for_user → emit member_added). Negative `ARGV[4]` = uncapped.
/// KEYS\[1\]=presusers KEYS\[2\]=presinfo KEYS\[3\]=presmembers
/// ARGV\[1\]=user_id ARGV\[2\]=user_info ARGV\[3\]=member_token ARGV\[4\]=max_members
const PRESENCE_JOIN_LUA: &str = r#"
local cap = tonumber(ARGV[4])
if cap >= 0
   and redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0
   and redis.call('HLEN', KEYS[1]) >= cap then
  return -1
end
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

/// REAP_MEMBER CAS script (the sweeper's stale-connection reap). Resolves the stale
/// `member_token` to its user and either takes that user's refcount to EXACTLY 0 —
/// de-indexing it from `presusers` + `presinfo` and returning `won == 1`, the single
/// cluster-wide `member_removed` emission right — or applies a plain decrement and
/// returns `won == 0`. A token already gone, or a refcount already at/below 0, is
/// still garbage-collected but never re-emits: that edge was taken and announced by
/// another writer. Redis serializes scripts, so exactly one of this and the live
/// [`PRESENCE_LEAVE_LUA`] can observe the 1→0 edge — and after a reap win the racing
/// live leave sees −1, not 0.
///
/// Returns `{user_id, remaining, won}` (`user_id` is `''` when the token was absent).
/// KEYS\[1\]=presusers KEYS\[2\]=presinfo KEYS\[3\]=presmembers
/// ARGV\[1\]=member_token
const REAP_MEMBER_LUA: &str = r#"
local user_id = redis.call('HGET', KEYS[3], ARGV[1])
if not user_id then return {'', 0, 0} end
redis.call('HDEL', KEYS[3], ARGV[1])
local conn = tonumber(redis.call('HGET', KEYS[1], user_id) or '0') or 0
if conn == 1 then
  redis.call('HDEL', KEYS[1], user_id)
  redis.call('HDEL', KEYS[2], user_id)
  return {user_id, 0, 1}
end
if conn <= 0 then
  redis.call('HDEL', KEYS[1], user_id)
  redis.call('HDEL', KEYS[2], user_id)
  return {user_id, 0, 0}
end
local left = redis.call('HINCRBY', KEYS[1], user_id, -1)
return {user_id, left, 0}
"#;

/// USER_REAP CAS script (the sweeper's stale-binding reap), the user twin of
/// [`REAP_MEMBER_LUA`]. HDELs every binding whose `expireAt` is in the past (an
/// unparseable stamp counts as stale — no live node can ever re-stamp it to a valid
/// future value) and, when that leaves the user with none, DELs the hash and
/// de-indexes them. `won == 1` iff THIS call's `SREM` removed the `users` entry: the
/// single cluster-wide `WatchOffline` emission right, so a concurrent signout that
/// already de-indexed the user leaves this reap silent.
///
/// Decision and writes are one script, so a signin landing anywhere near it is
/// serialised either wholly before (this call then sees a fresh binding and
/// declines) or wholly after (it re-establishes the user behind a reap that won).
/// Split across round trips, the decision could be made before the signin and the
/// `DEL` + `SREM` land after it — wiping a live binding, dropping an online user out
/// of `users(app)` for good, and publishing an offline for them.
///
/// Returns `{won, dead_node}` — `dead_node` is the first stale token's node prefix
/// (the publisher stamped on the `WatchOffline` so no live node self-dedups it), or
/// `''` when the hash had already TTL-lapsed while still indexed.
///
/// `KEYS[1]` = usr hash, `KEYS[2]` = users set.
/// `ARGV[1]` = now_ms, `ARGV[2]` = user_id.
const USER_REAP_LUA: &str = r#"
local now = tonumber(ARGV[1])
local bindings = redis.call('HGETALL', KEYS[1])
local dead_node = ''
local fresh = 0
for i = 1, #bindings, 2 do
  local expire_at = tonumber(bindings[i + 1])
  if expire_at ~= nil and expire_at >= now then
    fresh = fresh + 1
  else
    if dead_node == '' then
      local node = string.match(bindings[i], '^([^:]+):')
      if node then dead_node = node end
    end
    redis.call('HDEL', KEYS[1], bindings[i])
  end
end
if fresh > 0 then return {0, ''} end
redis.call('DEL', KEYS[1])
if redis.call('SREM', KEYS[2], ARGV[2]) == 0 then return {0, ''} end
return {1, dead_node}
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

/// APP ADMIT: the cluster-wide per-app capacity gate.
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

/// APP RELEASE: floor-0 give-back of one unit on both the node's per-app hash and
/// the cluster total — never negative. The node guard is an aggregate backstop, not
/// per-connection: it stops this node's releases from driving the cluster total
/// below the units this node holds in total, but on a node holding units for the
/// app it cannot recognise a release that matches no admission. Matching release to
/// admission is the caller's job (`Session::cluster_admitted`). Fields that reach 0
/// are HDEL'd so the hashes stay tidy. Returns the remaining cluster total.
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

/// DEAD-NODE RECLAIM (run by the sweeper): subtract a dead node's per-app counts
/// from the cluster totals, floored at 0 per app (never negative), then delete the
/// dead node's hash. One script = the whole read-subtract-delete decision is atomic,
/// so it cannot straddle a concurrent admission. Returns the number of apps
/// reclaimed, or `-1` when the node's liveness key is back — the sweeper's own
/// `EXISTS` probe and this call are separate round trips, and a node that
/// re-advertised in between is alive and still holds every unit on its hash.
///
/// `KEYS[1]` = appconns hash, `KEYS[2]` = nodeconns:{dead_node} hash,
/// `KEYS[3]` = node:{dead_node} liveness key.
const RECLAIM_NODE_LUA: &str = r#"
if redis.call('EXISTS', KEYS[3]) == 1 then return -1 end
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

/// NODE CAPACITY RE-SEED (run by the node heartbeat when its `nodeconns` hash has
/// gone): write this node's live per-app counts back onto its hash and rebuild each
/// of those apps' cluster total as `Σ nodeconns[node][app]` over the `nodes` set.
///
/// Recomputing the sum — rather than adding the live counts back — is what makes the
/// repair correct for BOTH ways the hash can vanish. A plain TTL lapse leaves this
/// node's units in `appconns` (adding them again would double-count); a dead-node
/// reclaim subtracted them (leaving them out under-counts forever). The sum is the
/// invariant both cases must land on, and Redis serializes the script, so it cannot
/// straddle a concurrent admission on another node.
///
/// `KEYS[1]` = appconns hash, `KEYS[2]` = this node's nodeconns hash, `KEYS[3]` = nodes
/// set. `ARGV[1]` = nodeconns key prefix, `ARGV[2]` = this node id, `ARGV[3]` = nodeconns
/// ttl_secs, `ARGV[4..]` = app/count pairs. Returns the number of apps re-seeded.
const RESEED_NODE_CAPACITY_LUA: &str = r#"
for i = 4, #ARGV, 2 do
  redis.call('HSET', KEYS[2], ARGV[i], ARGV[i + 1])
end
redis.call('EXPIRE', KEYS[2], ARGV[3])
local nodes = redis.call('SMEMBERS', KEYS[3])
for i = 4, #ARGV, 2 do
  local total = tonumber(ARGV[i + 1])
  for _, node in ipairs(nodes) do
    if node ~= ARGV[2] then
      total = total + (tonumber(redis.call('HGET', ARGV[1] .. node, ARGV[i])) or 0)
    end
  end
  redis.call('HSET', KEYS[1], ARGV[i], total)
end
return math.floor((#ARGV - 3) / 2)
"#;

/// The membership/presence Lua scripts, compiled (SHA-1 hashed) at adapter build
/// time. `Script::from_lua` is purely local — no Redis round-trip — and the scripts
/// are loaded lazily on first use via `evalsha_with_reload`'s NOSCRIPT fallback.
pub struct Scripts {
    /// Records one connection in a membership hash, re-arms that hash's TTL
    /// backstop, indexes its subject, and returns the new cluster-wide count.
    /// Drives BOTH the channel subscribe and the user signin.
    pub membership_join: Script,
    /// Removes a member and returns `{remaining cluster-wide count, vacate-CAS won}`.
    pub unsubscribe: Script,
    /// The sweeper's atomic vacate: returns `{won, drained_user_ids}` — `won == 1`
    /// iff THIS call's SREM removed the chans entry, in which case it also drained
    /// the presence side-tables and each returned user is owed a `member_removed`.
    pub vacate: Script,
    /// Decides the cluster-wide presence cap and, on admission, records the join.
    pub presence_join: Script,
    /// Records a presence leave and returns the user's remaining connection refcount.
    pub presence_leave: Script,
    /// The sweeper's atomic member reap: returns `{user_id, remaining, won}` —
    /// `won == 1` iff THIS call took the user's refcount to exactly 0 and owns
    /// the single `member_removed` emission right.
    pub reap_member: Script,
    /// The sweeper's atomic user-binding reap: returns `{won, dead_node}` — `won == 1`
    /// iff THIS call de-indexed the user and owns the single `WatchOffline` emission
    /// right.
    pub user_reap: Script,
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
    /// Heartbeat's capacity self-heal: re-seeds this node's per-app hash from the
    /// live counts and rebuilds each app's cluster total from every node's hash.
    pub reseed_node_capacity: Script,
}

impl Scripts {
    /// Compile the membership scripts. No Redis access — just SHA-1 hashing.
    pub fn new() -> Self {
        Self {
            membership_join: Script::from_lua(MEMBERSHIP_JOIN_LUA),
            unsubscribe: Script::from_lua(UNSUBSCRIBE_LUA),
            vacate: Script::from_lua(VACATE_LUA),
            presence_join: Script::from_lua(PRESENCE_JOIN_LUA),
            presence_leave: Script::from_lua(PRESENCE_LEAVE_LUA),
            reap_member: Script::from_lua(REAP_MEMBER_LUA),
            user_reap: Script::from_lua(USER_REAP_LUA),
            user_signout: Script::from_lua(USER_SIGNOUT_LUA),
            admit_app: Script::from_lua(ADMIT_APP_LUA),
            release_app: Script::from_lua(RELEASE_APP_LUA),
            reclaim_node: Script::from_lua(RECLAIM_NODE_LUA),
            reseed_node_capacity: Script::from_lua(RESEED_NODE_CAPACITY_LUA),
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
        assert_ne!(s.membership_join.sha1(), s.presence_join.sha1());
        assert_ne!(s.reap_member.sha1(), s.presence_leave.sha1());
        assert_ne!(s.reap_member.sha1(), s.vacate.sha1());
    }

    #[test]
    fn scripts_compile_including_user() {
        let s = Scripts::new();
        assert_ne!(s.membership_join.sha1(), s.user_signout.sha1());
        assert_ne!(s.user_signout.sha1(), s.unsubscribe.sha1());
        assert_ne!(s.user_reap.sha1(), s.user_signout.sha1());
        assert_ne!(s.user_reap.sha1(), s.reap_member.sha1());
    }

    #[test]
    fn scripts_compile_including_vacate() {
        let s = Scripts::new();
        assert_ne!(s.vacate.sha1(), s.unsubscribe.sha1());
        assert_ne!(s.vacate.sha1(), s.membership_join.sha1());
    }

    #[test]
    fn scripts_compile_including_app_capacity() {
        let s = Scripts::new();
        assert_ne!(s.admit_app.sha1(), s.release_app.sha1());
        assert_ne!(s.admit_app.sha1(), s.reclaim_node.sha1());
        assert_ne!(s.admit_app.sha1(), s.membership_join.sha1());
        assert_ne!(s.reseed_node_capacity.sha1(), s.reclaim_node.sha1());
    }
}
