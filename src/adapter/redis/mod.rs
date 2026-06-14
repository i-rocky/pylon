//! Redis scaling adapter — key schema, broadcast envelope, fred client wiring,
//! and the `RedisAdapter` itself.
//!
//! A3 ships a *skeleton*: every [`Adapter`] method delegates to a private
//! [`LocalAdapter`] so a `redis`-configured node behaves exactly like a `local`
//! node. Real cross-node behavior (PUBLISH/SUBSCRIBE broadcast, Redis-backed
//! presence/cache/users) is layered on in later phases (B–E) without changing
//! handler code.

pub mod client;
pub mod envelope;
pub mod keys;
pub mod pubsub;

use super::Adapter;
use crate::adapter::local::LocalAdapter;
use crate::channel::cache::CachedEvent;
use crate::channel::outcome::{ChannelSummary, SubscribeOutcome, UnsubscribeOutcome};
use crate::channel::registry::Registry;
use crate::connection::handle::ConnectionHandle;
use crate::presence::member::PresenceMember;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use crate::server::config::ServerConfig;
use crate::user::{UserJoinOutcome, UserLeaveOutcome};
use async_trait::async_trait;
use fred::interfaces::{EventInterface, PubsubInterface};
use std::sync::Arc;
use std::time::Duration;

/// The few `ServerConfig` knobs the Redis adapter needs to keep around for the
/// later phases (TTLs, heartbeat cadence, grace window). Cheap `Copy` struct so
/// it can be read on any task without locking.
#[derive(Clone, Copy, Debug)]
pub struct RedisConfig {
    pub membership_ttl_secs: u64,
    pub presence_heartbeat_secs: u64,
    pub node_heartbeat_secs: u64,
    pub sweep_interval_secs: u64,
    pub webhook_vacated_grace_ms: u64,
    pub sharded_pubsub: bool,
}

impl RedisConfig {
    fn from_server_config(cfg: &ServerConfig) -> Self {
        Self {
            membership_ttl_secs: cfg.redis_membership_ttl_secs,
            presence_heartbeat_secs: cfg.redis_presence_heartbeat_secs,
            node_heartbeat_secs: cfg.redis_node_heartbeat_secs,
            sweep_interval_secs: cfg.redis_sweep_interval_secs,
            webhook_vacated_grace_ms: cfg.webhook_vacated_grace_ms,
            sharded_pubsub: cfg.redis_sharded_pubsub,
        }
    }
}

/// Cross-node adapter backed by Redis. Broadcasts deliver locally and fan out over
/// Redis pub/sub; a spawned receive loop re-delivers remote broadcasts to this
/// node's local sockets. Everything else still delegates to the local adapter.
pub struct RedisAdapter {
    /// Shared with the receive loop so it can deliver remote broadcasts locally.
    local: Arc<LocalAdapter>,
    clients: client::RedisClients,
    keys: keys::Keys,
    node_id: String,
    #[allow(dead_code)] // wired in C/D/E
    cfg: RedisConfig,
    /// The pub/sub receive loop. Kept alive for the adapter's lifetime — dropping
    /// it would abort cross-node delivery on this node.
    #[allow(dead_code)]
    recv_handle: tokio::task::JoinHandle<()>,
}

impl RedisAdapter {
    /// Connect to Redis (per `cfg.redis_url` / `cfg.redis_pool_size`) and build
    /// the adapter. Fails loud if Redis is unreachable.
    pub async fn new(cfg: &ServerConfig) -> anyhow::Result<Self> {
        let node_id = uuid::Uuid::new_v4().to_string();
        let keys = keys::Keys::new(&cfg.redis_prefix);
        let clients = client::RedisClients::connect(&cfg.redis_url, cfg.redis_pool_size).await?;
        let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));

        // Spawn the pub/sub receive loop. It shares the local adapter so remote
        // broadcasts land on this node's sockets. The handle is stored on the
        // struct so the task is not dropped (which would stop cross-node delivery).
        let rx = clients.sub.message_rx();
        let recv_local = local.clone();
        let recv_node = node_id.clone();
        let recv_handle =
            tokio::spawn(async move { pubsub::receive_loop(rx, recv_local, recv_node).await });

        Ok(Self {
            local,
            clients,
            keys,
            node_id,
            cfg: RedisConfig::from_server_config(cfg),
            recv_handle,
        })
    }

    /// Test-support accessor: the set of Redis pub/sub channels this node's
    /// SubscriberClient is currently tracking. Used by the cluster integration
    /// tests to assert the per-(app,channel) subscription lifecycle.
    #[doc(hidden)]
    pub fn tracked_redis_channels(&self) -> Vec<String> {
        self.clients
            .sub
            .tracked_channels()
            .into_iter()
            .map(|c| c.to_string())
            .collect()
    }
}

#[async_trait]
impl Adapter for RedisAdapter {
    async fn subscribe(
        &self,
        app: &str,
        channel: &str,
        handle: ConnectionHandle,
        member: Option<PresenceMember>,
    ) -> SubscribeOutcome {
        let out = self.local.subscribe(app, channel, handle, member).await;

        // The Redis-subscription lifecycle is keyed on the node-LOCAL subscriber
        // edge: subscribe to the msg channel when this node goes 0 → 1. We capture
        // the local count now because C1 will overwrite `out.subscription_count`
        // with the *cluster*-wide count — the lifecycle must stay on the local edge.
        let local_count = out.subscription_count;
        if local_count == 1 {
            let msg_key = self.keys.msg(app, channel);
            if let Err(e) = self.clients.sub.subscribe(msg_key.clone()).await {
                // The local subscription already succeeded; a Redis SUBSCRIBE
                // failure only costs cross-node delivery for this channel on this
                // node. Log loudly but never panic the connection task.
                tracing::warn!(
                    error = %e,
                    channel = %msg_key,
                    "failed to SUBSCRIBE to Redis msg channel on 0→1 edge"
                );
            }
        }

        out
    }

    async fn unsubscribe(
        &self,
        app: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> UnsubscribeOutcome {
        let out = self.local.unsubscribe(app, channel, socket_id).await;

        // Mirror of `subscribe`: tear down the Redis subscription on the node-LOCAL
        // 1 → 0 edge. Keyed on the local count (see note in `subscribe`): C1 will
        // overwrite `out.subscription_count` with the cluster count, so the
        // lifecycle decision must read the node-local count captured here.
        let local_count = out.subscription_count;
        if local_count == 0 {
            let msg_key = self.keys.msg(app, channel);
            if let Err(e) = self.clients.sub.unsubscribe(msg_key.clone()).await {
                tracing::warn!(
                    error = %e,
                    channel = %msg_key,
                    "failed to UNSUBSCRIBE from Redis msg channel on 1→0 edge"
                );
            }
        }

        out
    }

    async fn broadcast(
        &self,
        app: &str,
        channel: &str,
        event: ServerEvent,
        except: Option<SocketId>,
    ) {
        // 1. Local delivery on THIS node — typed event, honouring `except`.
        self.local
            .broadcast(app, channel, event.clone(), except.clone())
            .await;

        // 2. Fan out to the rest of the cluster. Publish the *pre-encoded* v7 frame
        //    so remote nodes deliver it verbatim (no re-encoding). Always publish —
        //    even with no local subscribers — because a REST trigger may land on a
        //    node where the channel is only subscribed elsewhere.
        let frame = crate::protocol::v7::frames::encode(&event);
        let env = envelope::Envelope {
            node_id: self.node_id.clone(),
            app: app.to_string(),
            channel: channel.to_string(),
            event: serde_json::Value::String(frame),
            except: except.as_ref().map(|s| s.as_str().to_string()),
        };
        // Publish as a UTF-8 string (the envelope JSON is valid UTF-8); the receive
        // loop reads it back with `Value::into_string()` — a proven round-trip.
        let payload = match String::from_utf8(env.encode()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, app, channel, "envelope was not valid UTF-8");
                return;
            }
        };
        let key = self.keys.msg(app, channel);
        if let Err(e) = self
            .clients
            .pool
            .next()
            .publish::<(), _, _>(key, payload)
            .await
        {
            tracing::warn!(error = %e, app, channel, "redis publish failed");
        }
    }

    async fn channels(&self, app: &str, prefix: Option<&str>) -> Vec<ChannelSummary> {
        self.local.channels(app, prefix).await
    }

    async fn channel(&self, app: &str, channel: &str) -> ChannelSummary {
        self.local.channel(app, channel).await
    }

    async fn presence_members(&self, app: &str, channel: &str) -> Vec<PresenceMember> {
        self.local.presence_members(app, channel).await
    }

    async fn cache_set(&self, app: &str, channel: &str, event: CachedEvent, ttl: Duration) {
        self.local.cache_set(app, channel, event, ttl).await
    }

    async fn cache_get(&self, app: &str, channel: &str) -> Option<CachedEvent> {
        self.local.cache_get(app, channel).await
    }

    async fn signin_user(
        &self,
        app: &str,
        user_id: &str,
        handle: ConnectionHandle,
    ) -> UserJoinOutcome {
        self.local.signin_user(app, user_id, handle).await
    }

    async fn signout_user(
        &self,
        app: &str,
        user_id: &str,
        socket_id: &SocketId,
    ) -> UserLeaveOutcome {
        self.local.signout_user(app, user_id, socket_id).await
    }

    async fn is_user_online(&self, app: &str, user_id: &str) -> bool {
        self.local.is_user_online(app, user_id).await
    }

    async fn send_to_user(&self, app: &str, user_id: &str, event: ServerEvent) {
        self.local.send_to_user(app, user_id, event).await
    }

    async fn terminate_user(&self, app: &str, user_id: &str) -> Vec<SocketId> {
        self.local.terminate_user(app, user_id).await
    }

    async fn watch(
        &self,
        app: &str,
        handle: ConnectionHandle,
        watched: Vec<String>,
    ) -> Vec<String> {
        self.local.watch(app, handle, watched).await
    }

    async fn unwatch(&self, app: &str, socket_id: &SocketId) {
        self.local.unwatch(app, socket_id).await
    }

    async fn watchers_of(&self, app: &str, user_id: &str) -> Vec<ConnectionHandle> {
        self.local.watchers_of(app, user_id).await
    }
}
