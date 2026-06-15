//! Per-connection command dispatch. Version-agnostic: operates on domain types.

use crate::adapter::Adapter;
use crate::app::App;
use crate::connection::handle::ConnectionHandle;
use crate::protocol::command::ClientCommand;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

pub struct ConnectionContext {
    pub app: App,
    pub socket_id: SocketId,
    pub self_tx: UnboundedSender<ServerEvent>,
    pub adapter: Arc<dyn Adapter>,
    pub limits: crate::server::config::Limits,
    pub subscribed: HashSet<String>,
    pub user: Option<crate::user::AuthenticatedUser>,
    pub webhooks: crate::webhook::WebhookHandle,
    /// presence channel → this connection's member user_id (for client_event.user_id).
    pub presence_membership: std::collections::HashMap<String, String>,
    /// Per-connection client-event rate limiter (Pusher: 10 events/sec/connection → 4301).
    pub client_event_rate: crate::ws::rate::RateWindow,
    /// SP10 admission control: the percore broadcast pipeline's saturation flag.
    /// When set and saturated, a WS `client-*` event is dropped at ingress
    /// (mirroring the rate-limit drop) instead of broadcasting — the WS analogue
    /// of the REST 503. `None` off-percore (legacy transport / tests), so the
    /// drop never fires and behaviour is unchanged.
    pub saturated: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    /// SP11: whether this connection runs on the clustered percore path. When `true`
    /// the cluster `ClusterBridge` owns the single cluster-wide channel-edge emits — the
    /// clustered `subscription_count` broadcast, `channel_occupied`, and
    /// `channel_vacated` — so the handler MUST NOT emit the node-local versions (they
    /// would duplicate/wrong-count across nodes). `false` everywhere off-cluster (legacy
    /// transport, tests, and the not-yet-clustered percore path), so behaviour is
    /// byte-identical until Task 3.6 flips it true.
    pub clustered: bool,
}

impl ConnectionContext {
    /// Whether the percore broadcast pipeline is currently saturated (SP10).
    /// `false` when no flag is wired (off-percore).
    pub(in crate::ws) fn is_saturated(&self) -> bool {
        self.saturated
            .as_ref()
            .is_some_and(|s| s.load(std::sync::atomic::Ordering::Relaxed))
    }

    pub(in crate::ws) fn handle(&self) -> ConnectionHandle {
        ConnectionHandle {
            socket_id: self.socket_id.clone(),
            mailbox: self.self_tx.clone(),
        }
    }

    pub(in crate::ws) fn send_self(&self, event: ServerEvent) {
        let _ = self.self_tx.send(event);
    }

    /// Enqueue a webhook trigger (non-blocking; dropped if the mailbox is full).
    pub(in crate::ws) fn emit_webhook(&self, event: crate::webhook::event::WebhookEvent) {
        self.webhooks.enqueue(event);
    }

    /// Push a one-change `watchlist_events` frame to every connection watching `user_id`.
    pub(in crate::ws) async fn notify_watchers(&self, user_id: &str, name: &str) {
        let watchers = self.adapter.watchers_of(&self.app.id, user_id).await;
        if watchers.is_empty() {
            return;
        }
        let ev = ServerEvent::WatchlistEvents {
            events: vec![crate::protocol::event::WatchlistChange {
                name: name.to_string(),
                user_ids: vec![user_id.to_string()],
            }],
        };
        for h in watchers {
            let _ = h.mailbox.send(ev.clone());
        }
    }

    pub async fn dispatch(&mut self, cmd: ClientCommand) {
        match cmd {
            ClientCommand::Ping => self.send_self(ServerEvent::Pong),
            ClientCommand::Subscribe {
                channel,
                auth,
                channel_data,
            } => self.subscribe(channel, auth, channel_data).await,
            ClientCommand::Unsubscribe { channel } => self.unsubscribe(channel).await,
            ClientCommand::ClientEvent {
                event,
                channel,
                data,
            } => self.client_event(event, channel, data).await,
            ClientCommand::Signin { auth, user_data } => self.signin(auth, user_data).await,
            ClientCommand::Unknown(_) => {}
        }
    }

    async fn unsubscribe(&mut self, channel: String) {
        if self.subscribed.remove(&channel) {
            self.presence_membership.remove(&channel);
            let out = self
                .adapter
                .unsubscribe(&self.app.id, &channel, &self.socket_id)
                .await;
            // Clustered: the bridge fires the single cluster-wide `member_removed` + its
            // webhook on the cluster-wide last-for-user edge (`PresenceLeave`). The handler
            // must NOT emit the node-local versions — they would double/wrong-fire across
            // nodes. The node-local unsubscribe above still ran (the connection is
            // de-indexed locally); only the cluster-wide wire output is deferred.
            if !self.clustered {
                if let Some(leave) = out.presence {
                    if leave.last_for_user {
                        let uid = leave.user_id.clone();
                        self.adapter
                            .broadcast(
                                &self.app.id,
                                &channel,
                                ServerEvent::MemberRemoved {
                                    channel: channel.clone(),
                                    user_id: leave.user_id,
                                },
                                None,
                            )
                            .await;
                        if self.app.has_member_removed_webhooks {
                            self.emit_webhook(
                                crate::webhook::event::WebhookEvent::MemberRemoved {
                                    app: self.app.id.clone(),
                                    channel: channel.clone(),
                                    user_id: uid,
                                },
                            );
                        }
                    }
                }
            }
            // Clustered: the bridge fires the single cluster-wide channel_vacated on the
            // cluster 1→0 edge. The handler must NOT fire the node-local one.
            if !self.clustered && out.vacated && self.app.has_channel_vacated_webhooks {
                self.emit_webhook(crate::webhook::event::WebhookEvent::ChannelVacated {
                    app: self.app.id.clone(),
                    channel: channel.clone(),
                });
            }
            self.maybe_emit_count(&channel, out.subscription_count)
                .await;
        }
    }

    pub(in crate::ws) async fn maybe_emit_count(&self, channel: &str, count: usize) {
        // Clustered: the bridge broadcasts the cluster-wide subscription_count (a single
        // emit on the node's RedisAdapter). The handler must NOT broadcast the node-local
        // count — it would be wrong cross-node and double-counted.
        if self.clustered {
            return;
        }
        // Presence channels communicate membership via member_added/member_removed;
        // pusher_internal:subscription_count must not be emitted for them (Pusher parity P4).
        if crate::channel::kind::ChannelInfo::of(channel).auth
            == crate::channel::kind::AuthKind::Presence
        {
            return;
        }
        if self.app.subscription_count_enabled {
            self.adapter
                .broadcast(
                    &self.app.id,
                    channel,
                    ServerEvent::SubscriptionCount {
                        channel: channel.to_string(),
                        count,
                    },
                    None,
                )
                .await;
        }
    }

    /// On disconnect: leave channels, sign out + emit watchlist offline if this
    /// was the user's last connection, and drop this connection's own watches.
    pub async fn on_close(&mut self) {
        let channels: Vec<String> = self.subscribed.iter().cloned().collect();
        for channel in channels {
            self.unsubscribe(channel).await;
        }
        if let Some(user) = self.user.take() {
            let outcome = self
                .adapter
                .signout_user(&self.app.id, &user.id, &self.socket_id)
                .await;
            if outcome.last_for_user {
                self.notify_watchers(&user.id, "offline").await;
            }
        }
        self.adapter.unwatch(&self.app.id, &self.socket_id).await;
    }
}

#[cfg(test)]
#[path = "handler_tests.rs"]
mod tests;
