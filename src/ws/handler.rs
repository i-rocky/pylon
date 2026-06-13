//! Per-connection command dispatch. Version-agnostic: operates on domain types.

use crate::adapter::Adapter;
use crate::app::App;
use crate::channel::kind::ChannelKind;
use crate::connection::handle::ConnectionHandle;
use crate::protocol::command::ClientCommand;
use crate::protocol::error::PusherError;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use serde_json::Value;
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
}

impl ConnectionContext {
    fn handle(&self) -> ConnectionHandle {
        ConnectionHandle {
            socket_id: self.socket_id.clone(),
            mailbox: self.self_tx.clone(),
        }
    }

    fn send_self(&self, event: ServerEvent) {
        let _ = self.self_tx.send(event);
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
            ClientCommand::Unknown(_) => {}
        }
    }

    async fn subscribe(
        &mut self,
        channel: String,
        auth: Option<String>,
        channel_data: Option<String>,
    ) {
        match ChannelKind::of(&channel) {
            ChannelKind::Public => {
                let out = self
                    .adapter
                    .subscribe(&self.app.id, &channel, self.handle(), None)
                    .await;
                self.subscribed.insert(channel.clone());
                self.send_self(ServerEvent::SubscriptionSucceeded {
                    channel: channel.clone(),
                    presence: None,
                });
                self.maybe_emit_count(&channel, out.subscription_count)
                    .await;
            }
            ChannelKind::Private => {
                let token = match auth.as_deref() {
                    Some(t) => t,
                    None => {
                        return self.send_subscription_error(
                            &channel,
                            "AuthError",
                            "Auth signature required",
                            401,
                        )
                    }
                };
                if let Err(e) = crate::auth::channel::verify(
                    &self.app.key,
                    &self.app.secret,
                    self.socket_id.as_str(),
                    &channel,
                    None,
                    token,
                ) {
                    return self.send_subscription_error(&channel, "AuthError", e.message(), 401);
                }
                let out = self
                    .adapter
                    .subscribe(&self.app.id, &channel, self.handle(), None)
                    .await;
                self.subscribed.insert(channel.clone());
                self.send_self(ServerEvent::SubscriptionSucceeded {
                    channel: channel.clone(),
                    presence: None,
                });
                self.maybe_emit_count(&channel, out.subscription_count)
                    .await;
            }
            ChannelKind::Presence => {
                let token = match auth.as_deref() {
                    Some(t) => t,
                    None => {
                        return self.send_subscription_error(
                            &channel,
                            "AuthError",
                            "Auth signature required",
                            401,
                        )
                    }
                };
                let raw = match channel_data.as_deref() {
                    Some(d) => d,
                    None => {
                        return self.send_subscription_error(
                            &channel,
                            "AuthError",
                            "Presence requires channel_data",
                            401,
                        )
                    }
                };
                if let Err(e) = crate::auth::channel::verify(
                    &self.app.key,
                    &self.app.secret,
                    self.socket_id.as_str(),
                    &channel,
                    Some(raw),
                    token,
                ) {
                    return self.send_subscription_error(&channel, "AuthError", e.message(), 401);
                }
                let member = match crate::presence::member::parse_channel_data(raw) {
                    Ok(m) => m,
                    Err(_) => {
                        return self.send_subscription_error(
                            &channel,
                            "AuthError",
                            "Invalid channel_data",
                            401,
                        )
                    }
                };
                // Enforce the configurable presence member cap (pylon-chosen rejection shape).
                let current = self
                    .adapter
                    .channel(&self.app.id, &channel)
                    .await
                    .user_count
                    .unwrap_or(0);
                let already_member = self
                    .adapter
                    .presence_members(&self.app.id, &channel)
                    .await
                    .iter()
                    .any(|m| m.user_id == member.user_id);
                if !already_member && current >= self.limits.max_presence_members {
                    return self.send_subscription_error(
                        &channel,
                        "LimitReached",
                        "Presence channel is full",
                        4004,
                    );
                }
                let out = self
                    .adapter
                    .subscribe(&self.app.id, &channel, self.handle(), Some(member))
                    .await;
                self.subscribed.insert(channel.clone());
                if let Some(join) = out.presence {
                    self.send_self(ServerEvent::SubscriptionSucceeded {
                        channel: channel.clone(),
                        presence: Some(join.roster),
                    });
                    if join.first_for_user {
                        self.adapter
                            .broadcast(
                                &self.app.id,
                                &channel,
                                ServerEvent::MemberAdded {
                                    channel: channel.clone(),
                                    user_id: join.member.user_id,
                                    user_info: join.member.user_info,
                                },
                                Some(self.socket_id.clone()),
                            )
                            .await;
                    }
                }
                self.maybe_emit_count(&channel, out.subscription_count)
                    .await;
            }
            // encrypted / cache require auth — SP3.
            _ => self.send_self(ServerEvent::Error(PusherError::unauthorized())),
        }
    }

    fn send_subscription_error(&self, channel: &str, error_type: &str, error: &str, status: u16) {
        self.send_self(ServerEvent::SubscriptionError {
            channel: channel.to_string(),
            error_type: error_type.to_string(),
            error: error.to_string(),
            status,
        });
    }

    async fn client_event(&self, event: String, channel: String, data: Value) {
        if !self.app.client_messages_enabled {
            self.send_self(ServerEvent::Error(PusherError::new(
                4301,
                "The app does not have client messaging enabled.",
            )));
            return;
        }
        // Client events are valid only on private/presence channels the sender joined.
        let kind = ChannelKind::of(&channel);
        let allowed = matches!(
            kind,
            ChannelKind::Private | ChannelKind::Presence | ChannelKind::PrivateEncrypted
        );
        if !allowed || !self.subscribed.contains(&channel) {
            return; // silently dropped, matching Soketi/Pusher
        }
        // Oversize client-event payloads are silently dropped.
        if serde_json::to_string(&data).map_or(0, |s| s.len()) > self.limits.max_event_payload_bytes
        {
            return;
        }
        self.adapter
            .broadcast(
                &self.app.id,
                &channel,
                ServerEvent::ChannelEvent {
                    channel: channel.clone(),
                    event,
                    data,
                },
                Some(self.socket_id.clone()),
            )
            .await;
    }

    async fn unsubscribe(&mut self, channel: String) {
        if self.subscribed.remove(&channel) {
            let out = self
                .adapter
                .unsubscribe(&self.app.id, &channel, &self.socket_id)
                .await;
            if let Some(leave) = out.presence {
                if leave.last_for_user {
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
                }
            }
            self.maybe_emit_count(&channel, out.subscription_count)
                .await;
        }
    }

    async fn maybe_emit_count(&self, channel: &str, count: usize) {
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

    /// On disconnect: leave every channel (emitting count updates).
    pub async fn on_close(&mut self) {
        let channels: Vec<String> = self.subscribed.iter().cloned().collect();
        for channel in channels {
            self.unsubscribe(channel).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::local::LocalAdapter;
    use crate::channel::registry::Registry;
    use tokio::sync::mpsc;

    fn app(sub_count: bool) -> App {
        serde_json::from_value::<App>(serde_json::json!({
            "name": "t", "id": "app", "key": "k", "secret": "s",
            "client_messages_enabled": true,
            "subscription_count_enabled": sub_count
        }))
        .unwrap()
    }

    fn app_with_client_messages(enabled: bool) -> App {
        serde_json::from_value::<App>(serde_json::json!({
            "name": "t", "id": "app", "key": "k", "secret": "s",
            "client_messages_enabled": enabled, "subscription_count_enabled": false
        }))
        .unwrap()
    }

    fn ctx(app: App) -> (ConnectionContext, mpsc::UnboundedReceiver<ServerEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let registry = Arc::new(Registry::new());
        let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry));
        let c = ConnectionContext {
            app,
            socket_id: SocketId::generate(),
            self_tx: tx,
            adapter,
            limits: crate::server::config::ServerConfig::default().limits(),
            subscribed: HashSet::new(),
        };
        (c, rx)
    }

    #[tokio::test]
    async fn ping_enqueues_pong() {
        let (mut c, mut rx) = ctx(app(false));
        c.dispatch(ClientCommand::Ping).await;
        assert!(matches!(rx.try_recv(), Ok(ServerEvent::Pong)));
    }

    #[tokio::test]
    async fn public_subscribe_succeeds_and_registers() {
        let (mut c, mut rx) = ctx(app(false));
        c.dispatch(ClientCommand::Subscribe {
            channel: "room".into(),
            auth: None,
            channel_data: None,
        })
        .await;
        assert!(matches!(
            rx.try_recv(),
            Ok(ServerEvent::SubscriptionSucceeded { .. })
        ));
        assert_eq!(c.adapter.channel("app", "room").await.subscription_count, 1);
    }

    #[tokio::test]
    async fn subscription_count_emitted_when_enabled() {
        let (mut c, mut rx) = ctx(app(true));
        c.dispatch(ClientCommand::Subscribe {
            channel: "room".into(),
            auth: None,
            channel_data: None,
        })
        .await;
        assert!(matches!(
            rx.try_recv(),
            Ok(ServerEvent::SubscriptionSucceeded { .. })
        ));
        match rx.try_recv() {
            Ok(ServerEvent::SubscriptionCount { count, .. }) => assert_eq!(count, 1),
            other => panic!("expected SubscriptionCount, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn private_subscribe_without_auth_errors_non_fatally() {
        let (mut c, mut rx) = ctx(app(false));
        c.dispatch(ClientCommand::Subscribe {
            channel: "private-x".into(),
            auth: None,
            channel_data: None,
        })
        .await;
        match rx.try_recv() {
            Ok(ServerEvent::SubscriptionError {
                channel, status, ..
            }) => {
                assert_eq!(channel, "private-x");
                assert_eq!(status, 401);
            }
            other => panic!("expected SubscriptionError, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn private_subscribe_with_valid_auth_succeeds() {
        let (mut c, mut rx) = ctx(app(false));
        let sid = c.socket_id.as_str().to_string();
        let sig = crate::auth::signature::channel_signature("s", &sid, "private-x", None);
        let token = format!("k:{sig}"); // app key "k", secret "s" from the `app()` helper
        c.dispatch(ClientCommand::Subscribe {
            channel: "private-x".into(),
            auth: Some(token),
            channel_data: None,
        })
        .await;
        assert!(matches!(
            rx.try_recv(),
            Ok(ServerEvent::SubscriptionSucceeded { .. })
        ));
    }

    #[tokio::test]
    async fn presence_subscribe_returns_roster_and_broadcasts_member_added() {
        let (mut c, mut rx) = ctx(app(false));
        let sid = c.socket_id.as_str().to_string();
        let cd = r#"{"user_id":"u1","user_info":{"name":"Ann"}}"#;
        let sig = crate::auth::signature::channel_signature("s", &sid, "presence-x", Some(cd));
        c.dispatch(ClientCommand::Subscribe {
            channel: "presence-x".into(),
            auth: Some(format!("k:{sig}")),
            channel_data: Some(cd.into()),
        })
        .await;
        match rx.try_recv() {
            Ok(ServerEvent::SubscriptionSucceeded {
                presence: Some(p), ..
            }) => {
                assert_eq!(p.count, 1);
                assert_eq!(p.ids, vec!["u1".to_string()]);
            }
            other => panic!("expected presence SubscriptionSucceeded, got {other:?}"),
        }
        // Self is excluded from its own member_added, so no further self-delivered event.
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn presence_subscribe_with_bad_auth_errors() {
        let (mut c, mut rx) = ctx(app(false));
        c.dispatch(ClientCommand::Subscribe {
            channel: "presence-x".into(),
            auth: Some("k:bad".into()),
            channel_data: Some(r#"{"user_id":"u1"}"#.into()),
        })
        .await;
        assert!(matches!(
            rx.try_recv(),
            Ok(ServerEvent::SubscriptionError { .. })
        ));
    }

    #[tokio::test]
    async fn presence_unsubscribe_broadcasts_member_removed_to_others() {
        // Shared adapter so two contexts see the same channel.
        let registry = Arc::new(Registry::new());
        let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry));
        let mk = |adapter: Arc<dyn Adapter>| {
            let (tx, rx) = mpsc::unbounded_channel();
            let c = ConnectionContext {
                app: app(false),
                socket_id: SocketId::generate(),
                self_tx: tx,
                adapter,
                limits: crate::server::config::ServerConfig::default().limits(),
                subscribed: HashSet::new(),
            };
            (c, rx)
        };
        let (mut a, mut rxa) = mk(adapter.clone());
        let (mut b, _rxb) = mk(adapter.clone());

        for (c, who) in [(&mut a, "ua"), (&mut b, "ub")] {
            let sid = c.socket_id.as_str().to_string();
            let cd = format!(r#"{{"user_id":"{who}"}}"#);
            let sig = crate::auth::signature::channel_signature("s", &sid, "presence-x", Some(&cd));
            c.dispatch(ClientCommand::Subscribe {
                channel: "presence-x".into(),
                auth: Some(format!("k:{sig}")),
                channel_data: Some(cd),
            })
            .await;
        }
        // Drain a's queued frames (its own subscription_succeeded + member_added for ub).
        while rxa.try_recv().is_ok() {}

        b.unsubscribe("presence-x".into()).await;
        // a should now see member_removed for ub.
        let mut saw = false;
        while let Ok(ev) = rxa.try_recv() {
            if let ServerEvent::MemberRemoved { user_id, .. } = ev {
                assert_eq!(user_id, "ub");
                saw = true;
            }
        }
        assert!(saw, "remaining member should receive member_removed");
    }

    #[tokio::test]
    async fn client_event_rejected_when_messaging_disabled() {
        let (mut c, mut rx) = ctx(app_with_client_messages(false));
        c.dispatch(ClientCommand::ClientEvent {
            event: "client-x".into(),
            channel: "private-x".into(),
            data: serde_json::json!({}),
        })
        .await;
        match rx.try_recv() {
            Ok(ServerEvent::Error(e)) => assert_eq!(e.code, 4301),
            other => panic!("expected 4301, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn client_event_dropped_when_not_subscribed() {
        let (mut c, mut rx) = ctx(app_with_client_messages(true));
        c.dispatch(ClientCommand::ClientEvent {
            event: "client-x".into(),
            channel: "private-x".into(),
            data: serde_json::json!({}),
        })
        .await;
        assert!(
            rx.try_recv().is_err(),
            "unsubscribed client event is silently dropped"
        );
    }
}
