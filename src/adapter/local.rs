use super::Adapter;
use crate::adapter::app_registry::AppRegistry;
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

/// Send error (4009) + close (4009) frames to every handle in `handles` and
/// return their socket ids. Shared by `terminate_user` and `purge_app` so the
/// 4009 eviction logic lives in exactly one place.
fn close_handles_4009(handles: Vec<ConnectionHandle>) -> Vec<SocketId> {
    let ids = handles.iter().map(|h| h.socket_id).collect();
    for h in handles {
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

pub struct LocalAdapter {
    registry: Arc<Registry>,
    cache: CacheStore,
    users: UserRegistry,
    /// Per-app connection index for `purge_app`. Shared with the percore worker
    /// fleet (inserted on establish, removed on close) so `purge_app` can drain
    /// ALL connections of a removed app — including idle (unsubscribed) ones that
    /// would be invisible to the channel registry.
    app_registry: Arc<AppRegistry>,
    /// Per-core SHARDED broadcast sink. Set by `run_percore` BEFORE any worker
    /// spawns when the per-core transport is active; `None` for the legacy
    /// (axum) transport and standalone tests. When present, channel broadcasts
    /// are routed to the workers (each fans out to its own local subscribers,
    /// no per-connection mpsc); when absent, the legacy registry mailbox path is
    /// used. `OnceLock` because the sink is installed exactly once at startup.
    bcast_sink: std::sync::OnceLock<crate::transport::fanout::BroadcastSink>,
    /// SP10 admission control: the percore broadcast pipeline's saturation flag.
    /// Owned here (created at construction) so it can be shared with the REST
    /// `AppState` (which reads it to 503) AND with the sink/workers (which set and
    /// clear it) — `run_percore` installs a sink that points at this same flag.
    /// Off-percore it simply stays `false`, so the 503 path is a no-op.
    saturated: Arc<std::sync::atomic::AtomicBool>,
}

impl LocalAdapter {
    pub fn new(registry: Arc<Registry>, app_registry: Arc<AppRegistry>) -> Self {
        Self {
            registry,
            cache: CacheStore::new(),
            users: UserRegistry::new(),
            app_registry,
            bcast_sink: std::sync::OnceLock::new(),
            saturated: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Install the per-core sharded broadcast sink. Called once by `run_percore`
    /// before spawning workers; idempotent (a second call is ignored).
    pub fn set_broadcast_sink(&self, sink: crate::transport::fanout::BroadcastSink) {
        let _ = self.bcast_sink.set(sink);
    }

    /// The shared saturation flag (SP10). `run_percore` builds the broadcast sink
    /// around this very `Arc` so the publishers' "saturated" signal and the REST
    /// admission check observe the same bit. Cloned into `AppState` for the 503
    /// gate.
    pub fn saturation_flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.saturated.clone()
    }

    /// Cheap admission-control check (SP10): whether the percore broadcast
    /// pipeline is currently saturated. Off-percore this stays `false`.
    pub fn is_saturated(&self) -> bool {
        self.saturated.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The installed per-core broadcast sink, if any (percore active).
    fn broadcast_sink(&self) -> Option<&crate::transport::fanout::BroadcastSink> {
        self.bcast_sink.get()
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
        if let Some(sink) = self.broadcast_sink() {
            // Per-core active: encode the v7 JSON once, WS-frame it once, and
            // route the shared frame to every worker. Each worker fans it out to
            // its own local subscribers by direct slab-enqueue.
            let json: Arc<str> = match &event {
                ServerEvent::Raw(f) => f.clone(),
                other => Arc::from(crate::protocol::v7::frames::encode(other).as_str()),
            };
            let mut buf = bytes::BytesMut::new();
            crate::transport::frame::encode_text(&mut buf, json.as_bytes());
            sink.broadcast(
                Arc::from(app),
                Arc::from(channel),
                Arc::from(&buf[..]),
                except,
            );
        } else {
            // Legacy mailbox path (axum transport / tests): UNCHANGED.
            self.registry
                .broadcast(app, channel, &event, except.as_ref());
        }
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
        // Mirror soketi namespace.ts:179-188 — error frame then close, both 4009.
        close_handles_4009(self.users.handles(app, user_id))
    }

    async fn purge_app(&self, app_id: &str) -> Vec<SocketId> {
        // Drain all connections for this app from the per-app index, then 4009-close
        // each one. Each connection's on_close will self-clean its channel/user/presence
        // registries, so no explicit registry teardown is needed here.
        close_handles_4009(self.app_registry.drain_app(app_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn subscribe_then_broadcast_delegates_to_registry() {
        let reg = Arc::new(Registry::new());
        let adapter = LocalAdapter::new(reg.clone(), Arc::new(crate::adapter::app_registry::AppRegistry::new()));
        let (tx, mut rx) = mpsc::channel(1024);
        let out = adapter
            .subscribe(
                "app",
                "c",
                ConnectionHandle {
                    socket_id: SocketId::generate(),
                    mailbox: crate::connection::handle::Mailbox::new(tx, None, None),
                },
                None,
            )
            .await;
        assert_eq!(out.subscription_count, 1);
        adapter.broadcast("app", "c", ServerEvent::Pong, None).await;
        // `broadcast` now encodes once and fans out `Raw` frames; assert the wire
        // bytes match a freshly-encoded `Pong` rather than the structured variant.
        match rx.try_recv().map(|b| *b) {
            Ok(ServerEvent::Raw(f)) => {
                assert_eq!(&*f, crate::protocol::v7::frames::encode(&ServerEvent::Pong))
            }
            other => panic!("expected Raw(Pong), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn presence_members_round_trip() {
        let reg = Arc::new(Registry::new());
        let adapter = LocalAdapter::new(reg.clone(), Arc::new(crate::adapter::app_registry::AppRegistry::new()));
        let (tx, _rx) = mpsc::channel(1024);
        adapter
            .subscribe(
                "app",
                "presence-x",
                ConnectionHandle {
                    socket_id: SocketId::generate(),
                    mailbox: crate::connection::handle::Mailbox::new(tx, None, None),
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
        let adapter = LocalAdapter::new(Arc::new(Registry::new()), Arc::new(crate::adapter::app_registry::AppRegistry::new()));
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
        let adapter = LocalAdapter::new(Arc::new(Registry::new()), Arc::new(crate::adapter::app_registry::AppRegistry::new()));
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
        let adapter = LocalAdapter::new(Arc::new(Registry::new()), Arc::new(crate::adapter::app_registry::AppRegistry::new()));
        assert_eq!(adapter.cache_get("app", "cache-missing").await, None);
    }

    #[tokio::test]
    async fn cache_entry_expires_after_ttl() {
        let adapter = LocalAdapter::new(Arc::new(Registry::new()), Arc::new(crate::adapter::app_registry::AppRegistry::new()));
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
        let adapter = LocalAdapter::new(Arc::new(Registry::new()), Arc::new(crate::adapter::app_registry::AppRegistry::new()));
        let (tx1, mut rx1) = mpsc::channel(1024);
        let (tx2, mut rx2) = mpsc::channel(1024);
        let s1 = SocketId::generate();
        let s2 = SocketId::generate();
        adapter
            .signin_user(
                "app",
                "u",
                ConnectionHandle {
                    socket_id: s1,
                    mailbox: crate::connection::handle::Mailbox::new(tx1, None, None),
                },
            )
            .await;
        adapter
            .signin_user(
                "app",
                "u",
                ConnectionHandle {
                    socket_id: s2,
                    mailbox: crate::connection::handle::Mailbox::new(tx2, None, None),
                },
            )
            .await;
        adapter.send_to_user("app", "u", ServerEvent::Pong).await;
        assert!(matches!(rx1.try_recv().map(|b| *b), Ok(ServerEvent::Pong)));
        assert!(matches!(rx2.try_recv().map(|b| *b), Ok(ServerEvent::Pong)));
    }

    #[tokio::test]
    async fn terminate_user_pushes_error_then_close_and_returns_ids() {
        let adapter = LocalAdapter::new(Arc::new(Registry::new()), Arc::new(crate::adapter::app_registry::AppRegistry::new()));
        let (tx, mut rx) = mpsc::channel(1024);
        let s = SocketId::generate();
        adapter
            .signin_user(
                "app",
                "u",
                ConnectionHandle {
                    socket_id: s,
                    mailbox: crate::connection::handle::Mailbox::new(tx, None, None),
                },
            )
            .await;
        let ids = adapter.terminate_user("app", "u").await;
        assert_eq!(ids, vec![s]);
        assert!(matches!(rx.try_recv().map(|b| *b), Ok(ServerEvent::Error(e)) if e.code == 4009));
        assert!(matches!(
            rx.try_recv().map(|b| *b),
            Ok(ServerEvent::Close { code: 4009, .. })
        ));
    }

    #[tokio::test]
    async fn purge_app_closes_subscribed_and_idle_connections_with_4009() {
        let app_registry = Arc::new(crate::adapter::app_registry::AppRegistry::new());
        let adapter = LocalAdapter::new(Arc::new(Registry::new()), app_registry.clone());

        // An IDLE (registered-but-unsubscribed) connection.
        let (tx_idle, mut rx_idle) = mpsc::channel(1024);
        let s_idle = SocketId::generate();
        app_registry.insert(
            "app",
            ConnectionHandle {
                socket_id: s_idle,
                mailbox: crate::connection::handle::Mailbox::new(tx_idle, None, None),
            },
        );
        // A SUBSCRIBED connection (also registered in app_registry).
        let (tx_sub, mut rx_sub) = mpsc::channel(1024);
        let s_sub = SocketId::generate();
        let sub_handle = ConnectionHandle {
            socket_id: s_sub,
            mailbox: crate::connection::handle::Mailbox::new(tx_sub, None, None),
        };
        app_registry.insert("app", sub_handle.clone());
        adapter.subscribe("app", "c", sub_handle, None).await;

        // purge_app enumerates via app_registry (complete), not the channel registry
        // (which would miss the idle connection).
        let mut ids = adapter.purge_app("app").await;
        ids.sort();
        let mut want = vec![s_idle, s_sub];
        want.sort();
        assert_eq!(ids, want, "purge must return BOTH the idle and subscribed socket ids");

        // BOTH connections received Error(4009) then Close(4009).
        for rx in [&mut rx_idle, &mut rx_sub] {
            assert!(matches!(rx.try_recv().map(|b| *b), Ok(ServerEvent::Error(e)) if e.code == 4009));
            assert!(matches!(
                rx.try_recv().map(|b| *b),
                Ok(ServerEvent::Close { code: 4009, .. })
            ));
        }
        // The registry entry is fully drained.
        assert!(app_registry.connected_app_ids().is_empty());
        // A second purge is a no-op (no handles).
        assert!(adapter.purge_app("app").await.is_empty());
    }
}
