use super::{Adapter, BroadcastError};
use crate::channel::cache::CachedEvent;
use crate::channel::outcome::{ChannelSummary, SubscribeOutcome, UnsubscribeOutcome};
use crate::connection::handle::{ConnectionHandle, Mailbox};
use crate::presence::member::PresenceMember;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use crate::user::{UserJoinOutcome, UserLeaveOutcome};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

pub struct FailingBroadcastAdapter {
    inner: Arc<dyn Adapter>,
}

impl FailingBroadcastAdapter {
    pub fn new(inner: Arc<dyn Adapter>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl Adapter for FailingBroadcastAdapter {
    async fn broadcast(
        &self,
        _app: &str,
        _channel: &str,
        _event: ServerEvent,
        _except: Option<SocketId>,
    ) -> Result<(), BroadcastError> {
        Err(BroadcastError::Publish("injected publish failure".into()))
    }

    async fn subscribe(
        &self,
        app: &str,
        channel: &str,
        handle: ConnectionHandle,
        member: Option<PresenceMember>,
    ) -> SubscribeOutcome {
        self.inner.subscribe(app, channel, handle, member).await
    }

    async fn unsubscribe(
        &self,
        app: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> UnsubscribeOutcome {
        self.inner.unsubscribe(app, channel, socket_id).await
    }

    async fn channels(&self, app: &str, prefix: Option<&str>) -> Vec<ChannelSummary> {
        self.inner.channels(app, prefix).await
    }

    async fn channel(&self, app: &str, channel: &str) -> ChannelSummary {
        self.inner.channel(app, channel).await
    }

    async fn presence_members(&self, app: &str, channel: &str) -> Vec<PresenceMember> {
        self.inner.presence_members(app, channel).await
    }

    async fn resend_presence_ack(&self, app: &str, channel: &str, mailbox: Mailbox) {
        self.inner.resend_presence_ack(app, channel, mailbox).await
    }

    async fn cache_set(&self, app: &str, channel: &str, event: CachedEvent, ttl: Duration) {
        self.inner.cache_set(app, channel, event, ttl).await
    }

    async fn cache_get(&self, app: &str, channel: &str) -> Option<CachedEvent> {
        self.inner.cache_get(app, channel).await
    }

    async fn signin_user(
        &self,
        app: &str,
        user_id: &str,
        handle: ConnectionHandle,
    ) -> UserJoinOutcome {
        self.inner.signin_user(app, user_id, handle).await
    }

    async fn signout_user(
        &self,
        app: &str,
        user_id: &str,
        socket_id: &SocketId,
    ) -> UserLeaveOutcome {
        self.inner.signout_user(app, user_id, socket_id).await
    }

    async fn is_user_online(&self, app: &str, user_id: &str) -> bool {
        self.inner.is_user_online(app, user_id).await
    }

    async fn send_to_user(&self, app: &str, user_id: &str, event: ServerEvent) {
        self.inner.send_to_user(app, user_id, event).await
    }

    async fn terminate_user(&self, app: &str, user_id: &str) -> Vec<SocketId> {
        self.inner.terminate_user(app, user_id).await
    }

    async fn purge_app(&self, app_id: &str) -> Vec<SocketId> {
        self.inner.purge_app(app_id).await
    }

    async fn watch(
        &self,
        app: &str,
        handle: ConnectionHandle,
        watched: Vec<String>,
    ) -> Vec<String> {
        self.inner.watch(app, handle, watched).await
    }

    async fn unwatch(&self, app: &str, socket_id: &SocketId) {
        self.inner.unwatch(app, socket_id).await
    }

    async fn watchers_of(&self, app: &str, user_id: &str) -> Vec<ConnectionHandle> {
        self.inner.watchers_of(app, user_id).await
    }
}
