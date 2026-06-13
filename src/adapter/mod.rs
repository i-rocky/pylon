//! Cross-node channel-state seam. SP2a ships only the in-process `Local` impl;
//! a Redis impl lands in SP7 behind this same trait — no handler changes.

pub mod local;

use crate::channel::cache::CachedEvent;
use crate::channel::outcome::{ChannelSummary, SubscribeOutcome, UnsubscribeOutcome};
use crate::connection::handle::ConnectionHandle;
use crate::presence::member::PresenceMember;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use crate::user::{UserJoinOutcome, UserLeaveOutcome};
use async_trait::async_trait;
use std::time::Duration;

#[async_trait]
pub trait Adapter: Send + Sync {
    async fn subscribe(
        &self,
        app: &str,
        channel: &str,
        handle: ConnectionHandle,
        member: Option<PresenceMember>,
    ) -> SubscribeOutcome;

    async fn unsubscribe(
        &self,
        app: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> UnsubscribeOutcome;

    async fn broadcast(
        &self,
        app: &str,
        channel: &str,
        event: ServerEvent,
        except: Option<SocketId>,
    );

    async fn channels(&self, app: &str, prefix: Option<&str>) -> Vec<ChannelSummary>;

    async fn channel(&self, app: &str, channel: &str) -> ChannelSummary;

    async fn presence_members(&self, app: &str, channel: &str) -> Vec<PresenceMember>;

    /// Store the last event for a cache channel with the given TTL. Overwrites
    /// any previous entry for `(app, channel)`.
    async fn cache_set(&self, app: &str, channel: &str, event: CachedEvent, ttl: Duration);

    /// Fetch the last cached event for a cache channel, or `None` if there is
    /// none or it has expired.
    async fn cache_get(&self, app: &str, channel: &str) -> Option<CachedEvent>;

    /// Bind a connection to a user. `first_for_user` is true when this is the
    /// user's first live connection (offline -> online transition).
    async fn signin_user(
        &self,
        app: &str,
        user_id: &str,
        handle: ConnectionHandle,
    ) -> UserJoinOutcome;

    /// Unbind a connection from a user. `last_for_user` is true when the user
    /// has no remaining connections (online -> offline transition).
    async fn signout_user(
        &self,
        app: &str,
        user_id: &str,
        socket_id: &SocketId,
    ) -> UserLeaveOutcome;

    /// True while `user_id` has at least one live signed-in connection.
    async fn is_user_online(&self, app: &str, user_id: &str) -> bool;
}
