//! Redis key schema for Pylon. All channel-scoped keys use hash-tags (`{channel}`)
//! so that multi-key Lua scripts over one channel are Redis Cluster slot-safe.

/// Builds Redis keys for all Pylon data structures under a given prefix.
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

    /// Distributed lock key for the sweep (expiry cleanup) job.
    pub fn sweeplock(&self) -> String {
        format!("{}:sweeplock", self.prefix)
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
        assert_eq!(k.sweeplock(), "pylon:sweeplock");
    }
    #[test]
    fn member_token_is_node_and_socket() {
        assert_eq!(member_token("n1", "123.456"), "n1:123.456");
    }
}
