use super::Adapter;
use crate::channel::cache::{CacheStore, CachedEvent};
use crate::channel::outcome::{ChannelSummary, SubscribeOutcome, UnsubscribeOutcome};
use crate::channel::registry::Registry;
use crate::connection::handle::ConnectionHandle;
use crate::presence::member::PresenceMember;
use crate::protocol::error::PusherError;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use crate::user::registry::UserRegistry;
use crate::user::{UserJoinOutcome, UserLeaveOutcome};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct LocalAdapter {
    registry: Arc<Registry>,
    cache: CacheStore,
    users: UserRegistry,
}

impl LocalAdapter {
    pub fn new(registry: Arc<Registry>) -> Self {
        Self {
            registry,
            cache: CacheStore::new(),
            users: UserRegistry::new(),
        }
    }

    /// Every local subscription as `(app, channel, socket_id)`. Exposed so the Redis
    /// adapter's membership heartbeat can re-stamp each local member without reaching
    /// into the private registry.
    pub fn local_members(&self) -> Vec<(String, String, SocketId)> {
        self.registry.local_members()
    }

    /// Every local user binding as `(app, user_id, socket_id)`. Exposed so the Redis
    /// adapter's membership heartbeat can re-stamp each local user binding's `expireAt`
    /// without reaching into the private user registry.
    pub fn local_user_bindings(&self) -> Vec<(String, String, SocketId)> {
        self.users.local_bindings()
    }

    /// Record watchers locally and report the per-user LOCAL watcher edges. Returns
    /// `(online, newly_watched)` — the node-local online subset plus the users whose
    /// LOCAL watcher set went 0→1 here. The composing `RedisAdapter` calls this (not
    /// via the `Adapter` trait) to drive the per-user `watch` Redis-sub lifecycle.
    pub fn watch_edges(
        &self,
        app: &str,
        handle: ConnectionHandle,
        watched: Vec<String>,
    ) -> (Vec<String>, Vec<String>) {
        self.users.watch(app, handle, watched)
    }

    /// Drop this connection's watch state and report the users whose LOCAL watcher set
    /// dropped to empty here (1→0). The composing `RedisAdapter` uses these to
    /// UNSUBSCRIBE the per-user `watch` Redis channel.
    pub fn unwatch_edges(&self, app: &str, socket_id: &SocketId) -> Vec<String> {
        self.users.unwatch(app, socket_id)
    }
}

#[async_trait]
impl Adapter for LocalAdapter {
    async fn subscribe(
        &self,
        app: &str,
        channel: &str,
        handle: ConnectionHandle,
        member: Option<PresenceMember>,
    ) -> SubscribeOutcome {
        self.registry.subscribe(app, channel, handle, member)
    }

    async fn unsubscribe(
        &self,
        app: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> UnsubscribeOutcome {
        self.registry.unsubscribe(app, channel, socket_id)
    }

    async fn broadcast(
        &self,
        app: &str,
        channel: &str,
        event: ServerEvent,
        except: Option<SocketId>,
    ) {
        self.registry
            .broadcast(app, channel, &event, except.as_ref());
    }

    async fn channels(&self, app: &str, prefix: Option<&str>) -> Vec<ChannelSummary> {
        self.registry.channels(app, prefix)
    }

    async fn channel(&self, app: &str, channel: &str) -> ChannelSummary {
        self.registry.channel_summary(app, channel)
    }

    async fn presence_members(&self, app: &str, channel: &str) -> Vec<PresenceMember> {
        self.registry.presence_members(app, channel)
    }

    async fn cache_set(&self, app: &str, channel: &str, event: CachedEvent, ttl: Duration) {
        let expiry = Instant::now() + ttl;
        self.cache
            .insert((app.to_string(), channel.to_string()), (event, expiry));
    }

    async fn cache_get(&self, app: &str, channel: &str) -> Option<CachedEvent> {
        let key = (app.to_string(), channel.to_string());
        {
            // Hold the shard read-guard only inside this block. On the live path
            // we return the clone while still holding it (safe); the expired path
            // falls through, dropping the guard BEFORE the remove() write-lock
            // below so DashMap cannot self-deadlock on the same shard.
            let entry = self.cache.get(&key)?;
            // `<` (not `<=`): an entry whose expiry instant has been reached is
            // treated as expired — a ttl of 0 is therefore immediately expired.
            if Instant::now() < entry.1 {
                return Some(entry.0.clone());
            }
        }
        self.cache.remove(&key);
        None
    }

    async fn signin_user(
        &self,
        app: &str,
        user_id: &str,
        handle: ConnectionHandle,
    ) -> UserJoinOutcome {
        self.users.signin(app, user_id, handle)
    }

    async fn signout_user(
        &self,
        app: &str,
        user_id: &str,
        socket_id: &SocketId,
    ) -> UserLeaveOutcome {
        self.users.signout(app, user_id, socket_id)
    }

    async fn is_user_online(&self, app: &str, user_id: &str) -> bool {
        self.users.is_online(app, user_id)
    }

    async fn send_to_user(&self, app: &str, user_id: &str, event: ServerEvent) {
        for h in self.users.handles(app, user_id) {
            let _ = h.mailbox.send(event.clone());
        }
    }

    async fn watch(
        &self,
        app: &str,
        handle: ConnectionHandle,
        watched: Vec<String>,
    ) -> Vec<String> {
        // Trait contract: return only the (node-local) online subset.
        self.users.watch(app, handle, watched).0
    }

    async fn unwatch(&self, app: &str, socket_id: &SocketId) {
        let _ = self.users.unwatch(app, socket_id);
    }

    async fn watchers_of(&self, app: &str, user_id: &str) -> Vec<ConnectionHandle> {
        self.users.watchers_of(app, user_id)
    }

    async fn terminate_user(&self, app: &str, user_id: &str) -> Vec<SocketId> {
        let handles = self.users.handles(app, user_id);
        let ids = handles.iter().map(|h| h.socket_id.clone()).collect();
        for h in handles {
            // Mirror soketi namespace.ts:179-188 — error frame then close, both 4009.
            let _ = h.mailbox.send(ServerEvent::Error(PusherError::new(
                4009,
                "You got disconnected by the app.",
            )));
            let _ = h.mailbox.send(ServerEvent::Close {
                code: 4009,
                reason: "You got disconnected by the app.".to_string(),
            });
        }
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn subscribe_then_broadcast_delegates_to_registry() {
        let reg = Arc::new(Registry::new());
        let adapter = LocalAdapter::new(reg.clone());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let out = adapter
            .subscribe(
                "app",
                "c",
                ConnectionHandle {
                    socket_id: SocketId::generate(),
                    mailbox: tx,
                },
                None,
            )
            .await;
        assert_eq!(out.subscription_count, 1);
        adapter.broadcast("app", "c", ServerEvent::Pong, None).await;
        // `broadcast` now encodes once and fans out `Raw` frames; assert the wire
        // bytes match a freshly-encoded `Pong` rather than the structured variant.
        match rx.try_recv() {
            Ok(ServerEvent::Raw(f)) => {
                assert_eq!(&*f, crate::protocol::v7::frames::encode(&ServerEvent::Pong))
            }
            other => panic!("expected Raw(Pong), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn presence_members_round_trip() {
        let reg = Arc::new(Registry::new());
        let adapter = LocalAdapter::new(reg.clone());
        let (tx, _rx) = mpsc::unbounded_channel();
        adapter
            .subscribe(
                "app",
                "presence-x",
                ConnectionHandle {
                    socket_id: SocketId::generate(),
                    mailbox: tx,
                },
                Some(PresenceMember {
                    user_id: "u1".into(),
                    user_info: serde_json::json!({}),
                }),
            )
            .await;
        let members = adapter.presence_members("app", "presence-x").await;
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].user_id, "u1");
        assert_eq!(
            adapter.channel("app", "presence-x").await.user_count,
            Some(1)
        );
    }

    #[tokio::test]
    async fn cache_set_then_get_round_trips() {
        let adapter = LocalAdapter::new(Arc::new(Registry::new()));
        adapter
            .cache_set(
                "app",
                "cache-x",
                crate::channel::cache::CachedEvent {
                    event: "e".into(),
                    data: "d".into(),
                },
                std::time::Duration::from_secs(60),
            )
            .await;
        let got = adapter.cache_get("app", "cache-x").await;
        assert_eq!(
            got,
            Some(crate::channel::cache::CachedEvent {
                event: "e".into(),
                data: "d".into()
            })
        );
    }

    #[tokio::test]
    async fn cache_set_overwrites_last_event() {
        let adapter = LocalAdapter::new(Arc::new(Registry::new()));
        for data in ["one", "two"] {
            adapter
                .cache_set(
                    "app",
                    "cache-x",
                    crate::channel::cache::CachedEvent {
                        event: "e".into(),
                        data: data.into(),
                    },
                    std::time::Duration::from_secs(60),
                )
                .await;
        }
        assert_eq!(
            adapter.cache_get("app", "cache-x").await.unwrap().data,
            "two"
        );
    }

    #[tokio::test]
    async fn cache_get_is_none_when_absent() {
        let adapter = LocalAdapter::new(Arc::new(Registry::new()));
        assert_eq!(adapter.cache_get("app", "cache-missing").await, None);
    }

    #[tokio::test]
    async fn cache_entry_expires_after_ttl() {
        let adapter = LocalAdapter::new(Arc::new(Registry::new()));
        adapter
            .cache_set(
                "app",
                "cache-x",
                crate::channel::cache::CachedEvent {
                    event: "e".into(),
                    data: "d".into(),
                },
                std::time::Duration::from_millis(0), // already expired
            )
            .await;
        assert_eq!(adapter.cache_get("app", "cache-x").await, None);
    }

    #[tokio::test]
    async fn send_to_user_fans_out_to_all_connections() {
        let adapter = LocalAdapter::new(Arc::new(Registry::new()));
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        let s1 = SocketId::generate();
        let s2 = SocketId::generate();
        adapter
            .signin_user(
                "app",
                "u",
                ConnectionHandle {
                    socket_id: s1,
                    mailbox: tx1,
                },
            )
            .await;
        adapter
            .signin_user(
                "app",
                "u",
                ConnectionHandle {
                    socket_id: s2,
                    mailbox: tx2,
                },
            )
            .await;
        adapter.send_to_user("app", "u", ServerEvent::Pong).await;
        assert!(matches!(rx1.try_recv(), Ok(ServerEvent::Pong)));
        assert!(matches!(rx2.try_recv(), Ok(ServerEvent::Pong)));
    }

    #[tokio::test]
    async fn terminate_user_pushes_error_then_close_and_returns_ids() {
        let adapter = LocalAdapter::new(Arc::new(Registry::new()));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let s = SocketId::generate();
        adapter
            .signin_user(
                "app",
                "u",
                ConnectionHandle {
                    socket_id: s.clone(),
                    mailbox: tx,
                },
            )
            .await;
        let ids = adapter.terminate_user("app", "u").await;
        assert_eq!(ids, vec![s]);
        assert!(matches!(rx.try_recv(), Ok(ServerEvent::Error(e)) if e.code == 4009));
        assert!(matches!(
            rx.try_recv(),
            Ok(ServerEvent::Close { code: 4009, .. })
        ));
    }
}
