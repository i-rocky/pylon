//! Subscribe/unsubscribe and client-event handling, split from `ws::handler`.

use super::handler::ConnectionContext;
use crate::channel::kind::{AuthKind, ChannelInfo};
use crate::protocol::error::PusherError;
use crate::protocol::event::ServerEvent;
use serde_json::Value;

impl ConnectionContext {
    pub(in crate::ws) async fn subscribe(
        &mut self,
        channel: String,
        auth: Option<String>,
        channel_data: Option<String>,
    ) {
        // Idempotent per spec §5.1: ignore a duplicate subscribe to an already-joined
        // channel (prevents presence conn_count corruption / ghost members).
        if self.subscribed.contains(&channel) {
            return;
        }
        let info = ChannelInfo::of(&channel);
        match info.auth {
            AuthKind::Public => {
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
            // Encrypted channels authenticate exactly like private channels
            // (HMAC over `socket_id:channel`, no channel_data) — pure relay.
            AuthKind::Private | AuthKind::PrivateEncrypted => {
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
            AuthKind::Presence => {
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
                if let Some(join) = out.presence {
                    self.subscribed.insert(channel.clone());
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
        }

        // Cache channels: after subscription_succeeded, replay the last event to
        // this new subscriber only — or signal a miss. `subscribed` contains the
        // channel iff the subscribe above succeeded (auth failures returned early).
        if info.cache && self.subscribed.contains(&channel) {
            let event = match self.adapter.cache_get(&self.app.id, &channel).await {
                Some(cached) => ServerEvent::ChannelEvent {
                    channel,
                    event: cached.event,
                    data: Value::String(cached.data),
                },
                None => ServerEvent::CacheMiss { channel },
            };
            self.send_self(event);
        }
    }

    pub(in crate::ws) fn send_subscription_error(
        &self,
        channel: &str,
        error_type: &str,
        error: &str,
        status: u16,
    ) {
        self.send_self(ServerEvent::SubscriptionError {
            channel: channel.to_string(),
            error_type: error_type.to_string(),
            error: error.to_string(),
            status,
        });
    }

    pub(in crate::ws) async fn client_event(&self, event: String, channel: String, data: Value) {
        if !self.app.client_messages_enabled {
            self.send_self(ServerEvent::Error(PusherError::new(
                4301,
                "The app does not have client messaging enabled.",
            )));
            return;
        }
        // Client events are valid only on private/presence channels the sender joined.
        let auth = ChannelInfo::of(&channel).auth;
        // Client libraries cannot trigger events to encrypted channels.
        let allowed = matches!(auth, AuthKind::Private | AuthKind::Presence);
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
}
