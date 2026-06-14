//! Subscribe/unsubscribe and client-event handling, split from `ws::handler`.

use super::handler::ConnectionContext;
use crate::channel::kind::{validate_channel_name, AuthKind, ChannelInfo, SERVER_TO_USER_PREFIX};
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

        // Reserved `#` channels are server-managed, never normal subscriptions.
        if let Some(uid) = channel.strip_prefix(SERVER_TO_USER_PREFIX) {
            let ok = self.user.as_ref().is_some_and(|u| u.id == uid);
            if ok {
                // Delivery is via the user registry (set up at signin), NOT the
                // channel registry — just acknowledge so the client's User
                // channel settles. Do not register or broadcast a count.
                return self.send_self(ServerEvent::SubscriptionSucceeded {
                    channel,
                    presence: None,
                });
            }
            return self.send_subscription_error(&channel, "AuthError", "Unauthorized", 401);
        }
        if channel.starts_with('#') {
            return self.send_subscription_error(&channel, "AuthError", "Unknown channel", 401);
        }

        // P8: enforce channel name length + charset before any auth or registry work.
        if !validate_channel_name(&channel, self.limits.max_channel_name_length) {
            return self.send_subscription_error(
                &channel,
                "InvalidChannel",
                "Invalid channel name",
                4009,
            );
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
                self.emit_occupied_if_edge(&channel, out.occupied);
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
                self.emit_occupied_if_edge(&channel, out.occupied);
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
                // P10: enforce presence user_id length and user_info byte limits.
                // Order: verify (done above) → parse (done above) → size check → cap check → add.
                if member.user_id.chars().count() > self.limits.max_presence_user_id_length {
                    return self.send_subscription_error(
                        &channel,
                        "InvalidPresenceData",
                        "user_id exceeds maximum length",
                        401,
                    );
                }
                if !member.user_info.is_null() {
                    let info_bytes =
                        serde_json::to_string(&member.user_info).map_or(0, |s| s.len());
                    if info_bytes > self.limits.max_presence_user_info_bytes {
                        return self.send_subscription_error(
                            &channel,
                            "InvalidPresenceData",
                            "user_info exceeds maximum size",
                            401,
                        );
                    }
                }
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
                let occupied = out.occupied;
                if let Some(join) = out.presence {
                    self.subscribed.insert(channel.clone());
                    // Record this socket's presence member id so a later
                    // `client_event` on this channel can attach `user_id`. Clone
                    // before the `first_for_user` block moves `join.member.user_id`.
                    self.presence_membership
                        .insert(channel.clone(), join.member.user_id.clone());
                    self.send_self(ServerEvent::SubscriptionSucceeded {
                        channel: channel.clone(),
                        presence: Some(join.roster),
                    });
                    if join.first_for_user {
                        let uid = join.member.user_id.clone();
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
                        if self.app.has_member_added_webhooks {
                            self.emit_webhook(crate::webhook::event::WebhookEvent::MemberAdded {
                                app: self.app.id.clone(),
                                channel: channel.clone(),
                                user_id: uid,
                            });
                        }
                    }
                }
                self.maybe_emit_count(&channel, out.subscription_count)
                    .await;
                self.emit_occupied_if_edge(&channel, occupied);
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
                    user_id: None,
                },
                None => {
                    if self.app.has_cache_miss_webhooks {
                        self.emit_webhook(crate::webhook::event::WebhookEvent::CacheMiss {
                            app: self.app.id.clone(),
                            channel: channel.clone(),
                        });
                    }
                    ServerEvent::CacheMiss { channel }
                }
            };
            self.send_self(event);
        }
    }

    /// Emit `channel_occupied` if this subscribe was the 0→1 edge and the app
    /// wants it. Called once per successful subscribe.
    pub(in crate::ws) fn emit_occupied_if_edge(&self, channel: &str, occupied: bool) {
        if occupied && self.app.has_channel_occupied_webhooks {
            self.emit_webhook(crate::webhook::event::WebhookEvent::ChannelOccupied {
                app: self.app.id.clone(),
                channel: channel.to_string(),
            });
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

    pub(in crate::ws) async fn client_event(
        &mut self,
        event: String,
        channel: String,
        data: Value,
    ) {
        if !self.app.client_messages_enabled {
            self.send_self(ServerEvent::ClientEventError {
                channel,
                code: 4301,
                message: "The app does not have client messaging enabled.".into(),
            });
            return;
        }
        // Client events are valid only on private/presence channels the sender joined.
        let auth = ChannelInfo::of(&channel).auth;
        // Client libraries cannot trigger events to encrypted channels.
        let allowed = matches!(auth, AuthKind::Private | AuthKind::Presence);
        if !allowed || !self.subscribed.contains(&channel) {
            return; // silently dropped, matching Soketi/Pusher
        }
        // P16: client-events with an oversized name emit in-band 4301 (soketi parity).
        // This gate must remain BEFORE the rate-limit check so oversized-name events
        // do NOT consume rate budget.
        if event.len() > self.limits.max_event_name_length {
            let max = self.limits.max_event_name_length;
            self.send_self(ServerEvent::ClientEventError {
                channel,
                code: 4301,
                message: format!("Event name is too long. Maximum allowed size is {max}."),
            });
            return;
        }
        // Oversize client-event payloads return pusher:error 4301 (soketi parity).
        if serde_json::to_string(&data).map_or(0, |s| s.len()) > self.limits.max_event_payload_bytes
        {
            self.send_self(ServerEvent::ClientEventError {
                channel,
                code: 4301,
                message: "Client event rejected - the data is too large".into(),
            });
            return;
        }
        // Rate-limit client events: drop + send in-band 4301 if the per-second
        // window is exhausted (Pusher parity: 10 client events/sec/connection).
        if !self.client_event_rate.check() {
            self.send_self(ServerEvent::ClientEventError {
                channel,
                code: 4301,
                message: "Client event rejected due to rate limit".to_string(),
            });
            return;
        }
        // Capture clones for the webhook before the broadcast moves `event`/`data`.
        // `user_id` is present only if this socket joined `channel` as a presence
        // member (recorded in `presence_membership` at subscribe).
        let user_id = self.presence_membership.get(&channel).cloned();
        let wh_event = event.clone();
        let wh_data = data.clone();
        self.adapter
            .broadcast(
                &self.app.id,
                &channel,
                ServerEvent::ChannelEvent {
                    channel: channel.clone(),
                    event,
                    data,
                    // Presence members broadcast their `user_id`; private has none.
                    user_id: user_id.clone(),
                },
                Some(self.socket_id.clone()),
            )
            .await;
        if self.app.has_client_event_webhooks {
            self.emit_webhook(crate::webhook::event::WebhookEvent::ClientEvent {
                app: self.app.id.clone(),
                channel,
                event: wh_event,
                data: wh_data,
                socket_id: self.socket_id.as_str().to_string(),
                user_id,
            });
        }
    }
}
