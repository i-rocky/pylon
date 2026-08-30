//! Redis key schema for Pylon. Channel-scoped keys (`msg`/`occ`/`cache`) wrap the
//! channel in a hash-tag (`{channel}`) so all keys for one channel co-locate on a
//! single Redis Cluster slot.
//!
//! SP7a targets a single Redis instance. The membership Lua intentionally spans the
//! per-channel `occ` hash and the app-level `chans` index set, which live on
//! different slots — fine on a single instance, but a known constraint to resolve
//! (e.g. co-locating or de-atomizing the `chans` index write) when Cluster mode
//! lands. Do not assume cross-key scripts here are CROSSSLOT-safe under Cluster.
//! The per-app capacity scripts span the SAME constraint: `ADMIT_APP_LUA` /
//! `RELEASE_APP_LUA` touch `appconns` + `nodeconns:{node}` in one script.

/// Builds Redis keys for all Pylon data structures under a given prefix.
#[derive(Clone)]
pub struct Keys {
    prefix: String,
}

impl Keys {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_owned(),
        }
    }

    /// PubSub channel for broadcasting events on a specific app channel.
    pub fn msg(&self, app: &str, channel: &str) -> String {
        format!("{}:msg:{}:{{{}}}", self.prefix, app, channel)
    }

    /// Hash key storing occupancy (presence members) for a channel.
    pub fn occ(&self, app: &str, channel: &str) -> String {
        format!("{}:occ:{}:{{{}}}", self.prefix, app, channel)
    }

    /// String key holding the last cached event for a cache channel.
    pub fn cache(&self, app: &str, channel: &str) -> String {
        format!("{}:cache:{}:{{{}}}", self.prefix, app, channel)
    }

    /// Set key of active channels for an app.
    pub fn chans(&self, app: &str) -> String {
        format!("{}:chans:{}", self.prefix, app)
    }

    /// Hash key holding per-node metadata.
    pub fn node(&self, node_id: &str) -> String {
        format!("{}:node:{}", self.prefix, node_id)
    }

    /// Set key of all known node IDs.
    pub fn nodes(&self) -> String {
        format!("{}:nodes", self.prefix)
    }

    /// Set key indexing every app that has ever had an occupied channel. The sweeper
    /// enumerates this set to find each app's `chans` index. Bounded by the configured
    /// apps, so it needs no cleanup.
    pub fn apps(&self) -> String {
        format!("{}:apps", self.prefix)
    }

    /// Distributed lock key for the sweep (expiry cleanup) job.
    pub fn sweeplock(&self) -> String {
        format!("{}:sweeplock", self.prefix)
    }

    /// Presence: HASH user_id -> live connection count (HINCRBY; HLEN = user_count).
    pub fn presusers(&self, app: &str, channel: &str) -> String {
        format!("{}:presusers:{}:{{{}}}", self.prefix, app, channel)
    }

    /// Presence: HASH user_id -> user_info JSON (the roster `hash`).
    pub fn presinfo(&self, app: &str, channel: &str) -> String {
        format!("{}:presinfo:{}:{{{}}}", self.prefix, app, channel)
    }

    /// Presence: HASH member_token -> user_id (lets the sweeper map a crashed socket to its user).
    pub fn presmembers(&self, app: &str, channel: &str) -> String {
        format!("{}:presmembers:{}:{{{}}}", self.prefix, app, channel)
    }

    /// User binding: HASH member_token -> expireAt (HLEN = cluster connection count).
    pub fn usr(&self, app: &str, user_id: &str) -> String {
        format!("{}:usr:{}:{{{}}}", self.prefix, app, user_id)
    }

    /// Per-app SET of users with >=1 cluster connection (sweeper enumerates this).
    pub fn users(&self, app: &str) -> String {
        format!("{}:users:{}", self.prefix, app)
    }

    /// Per-user pub/sub channel for send_to_user / terminate_user fan-out.
    pub fn usermsg(&self, app: &str, user_id: &str) -> String {
        format!("{}:usermsg:{}:{{{}}}", self.prefix, app, user_id)
    }

    /// Per-user pub/sub channel for watchlist online/offline notifications.
    pub fn watch(&self, app: &str, user_id: &str) -> String {
        format!("{}:watch:{}:{{{}}}", self.prefix, app, user_id)
    }

    /// Per-app CLUSTER connection counts: HASH app → count of live connections
    /// across every node. The `ADMIT_APP_LUA` capacity check reads this hash;
    /// admissions/releases keep it exact. No TTL — correctness is maintained by
    /// the admit/release scripts and the sweeper's dead-node reclaim.
    pub fn appconns(&self) -> String {
        format!("{}:appconns", self.prefix)
    }

    /// Per-NODE per-app connection counts: HASH app → count of THIS node's live
    /// connections. Lets `RELEASE_APP_LUA` floor-0 without stealing another
    /// node's units, and the sweeper subtract a dead node's exact contribution
    /// from `appconns`. Whole-key TTL refreshed by the node heartbeat + each
    /// admission (see [`RedisConfig::node_conns_ttl_secs`]), sized to outlive the
    /// node-liveness key so the sweeper always reclaims before the backstop
    /// expires the hash.
    ///
    /// [`RedisConfig::node_conns_ttl_secs`]: crate::adapter::redis::RedisConfig::node_conns_ttl_secs
    pub fn nodeconns(&self, node_id: &str) -> String {
        format!("{}:nodeconns:{}", self.prefix, node_id)
    }
}

/// Composite token uniquely identifying one socket connection across the cluster.
/// Stored in presence hashes; `"node_id:socket_id"`.
pub fn member_token(node_id: &str, socket_id: &str) -> String {
    format!("{}:{}", node_id, socket_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn channel_keys_use_prefix_and_hash_tag() {
        let k = Keys::new("pylon");
        assert_eq!(k.msg("app1", "public-room"), "pylon:msg:app1:{public-room}");
        assert_eq!(k.occ("app1", "public-room"), "pylon:occ:app1:{public-room}");
        assert_eq!(k.cache("app1", "cache-x"), "pylon:cache:app1:{cache-x}");
        assert_eq!(k.chans("app1"), "pylon:chans:app1");
        assert_eq!(k.node("n1"), "pylon:node:n1");
        assert_eq!(k.nodes(), "pylon:nodes");
        assert_eq!(k.apps(), "pylon:apps");
        assert_eq!(k.sweeplock(), "pylon:sweeplock");
        assert_eq!(
            k.presusers("app1", "presence-room"),
            "pylon:presusers:app1:{presence-room}"
        );
        assert_eq!(
            k.presinfo("app1", "presence-room"),
            "pylon:presinfo:app1:{presence-room}"
        );
        assert_eq!(
            k.presmembers("app1", "presence-room"),
            "pylon:presmembers:app1:{presence-room}"
        );
        assert_eq!(k.usr("app1", "u7"), "pylon:usr:app1:{u7}");
        assert_eq!(k.users("app1"), "pylon:users:app1");
        assert_eq!(k.usermsg("app1", "u7"), "pylon:usermsg:app1:{u7}");
        assert_eq!(k.watch("app1", "u7"), "pylon:watch:app1:{u7}");
        assert_eq!(k.appconns(), "pylon:appconns");
        assert_eq!(k.nodeconns("n1"), "pylon:nodeconns:n1");
    }
    #[test]
    fn member_token_is_node_and_socket() {
        assert_eq!(member_token("n1", "123.456"), "n1:123.456");
    }
}
