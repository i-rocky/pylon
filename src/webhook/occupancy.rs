//! Occupancy/presence lookup for the webhook reconnect-grace window (Task D1,
//! extended by re-audit R12b). Before a debounced `channel_vacated` fires, the
//! dispatcher re-checks the cluster subscription_count for the channel; before
//! a debounced `member_removed` fires, it re-checks whether the user is a
//! presence member again. A re-occupied channel / re-joined member suppresses
//! the webhook (the hosted doc's "if the client reconnects within this delay,
//! no webhooks will be sent" — scoped to BOTH events).

use async_trait::async_trait;
use std::sync::Arc;

/// Source of the current cluster-wide subscription_count for a channel and of
/// user presence. The dispatcher calls this at vacated/member_removed FIRE time
/// to decide whether the client reconnected during the grace window.
#[async_trait]
pub trait OccupancySource: Send + Sync {
    /// Current cluster-wide subscription_count for (app, channel).
    async fn subscription_count(&self, app: &str, channel: &str) -> usize;
    /// Whether `user_id` is currently a presence member of (app, channel) —
    /// the member analog of [`Self::subscription_count`], used by the
    /// `member_removed` grace re-check.
    async fn is_member_present(&self, app: &str, channel: &str, user_id: &str) -> bool;
}

/// Adapter-backed occupancy source: defers to `Adapter::channel(...).subscription_count`
/// and `Adapter::presence_members`.
pub struct AdapterOccupancy(pub Arc<dyn crate::adapter::Adapter>);

#[async_trait]
impl OccupancySource for AdapterOccupancy {
    async fn subscription_count(&self, app: &str, channel: &str) -> usize {
        self.0.channel(app, channel).await.subscription_count
    }

    async fn is_member_present(&self, app: &str, channel: &str, user_id: &str) -> bool {
        self.0
            .presence_members(app, channel)
            .await
            .iter()
            .any(|m| m.user_id == user_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::app_registry::AppRegistry;
    use crate::adapter::local::LocalAdapter;
    use crate::adapter::Adapter;
    use crate::channel::registry::Registry;
    use crate::connection::handle::{ConnectionHandle, Mailbox};
    use crate::protocol::event::ServerEvent;
    use crate::protocol::socket_id::SocketId;
    use tokio::sync::mpsc;

    /// `AdapterOccupancy` must surface the adapter's real channel subscription_count
    /// (the value the vacated-suppression grace check reads). Uses a real
    /// `LocalAdapter` so the delegation + the adapter's `channel()` path are exercised.
    #[tokio::test]
    async fn adapter_occupancy_reports_channel_subscription_count() {
        let local = Arc::new(LocalAdapter::new(
            Arc::new(Registry::new()),
            Arc::new(AppRegistry::new()),
        ));
        let occ = AdapterOccupancy(local.clone() as Arc<dyn Adapter>);

        // No subscribers yet → 0.
        assert_eq!(occ.subscription_count("app", "ch").await, 0);

        // Subscribe one connection → the occupancy source reports 1.
        let (tx, _rx) = mpsc::channel::<Box<ServerEvent>>(8);
        local
            .subscribe(
                "app",
                "ch",
                ConnectionHandle {
                    socket_id: SocketId::generate(),
                    mailbox: Mailbox::new(tx, None, None),
                },
                None,
            )
            .await;
        assert_eq!(
            occ.subscription_count("app", "ch").await,
            1,
            "AdapterOccupancy must report the adapter's channel subscription_count"
        );
    }

    /// `AdapterOccupancy::is_member_present` must answer from the adapter's real
    /// presence roster (the member_removed grace re-check reads it): true for a
    /// subscribed user, false for anyone else. Uses a real `LocalAdapter` with a
    /// presence subscription so the delegation + roster path are exercised.
    #[tokio::test]
    async fn adapter_occupancy_reports_member_presence() {
        use crate::presence::member::PresenceMember;

        let local = Arc::new(LocalAdapter::new(
            Arc::new(Registry::new()),
            Arc::new(AppRegistry::new()),
        ));
        let occ = AdapterOccupancy(local.clone() as Arc<dyn Adapter>);

        // Nobody home yet.
        assert!(!occ.is_member_present("app", "presence-x", "7").await);

        // Subscribe one connection as presence user "7".
        let (tx, _rx) = mpsc::channel::<Box<ServerEvent>>(8);
        local
            .subscribe(
                "app",
                "presence-x",
                ConnectionHandle {
                    socket_id: SocketId::generate(),
                    mailbox: Mailbox::new(tx, None, None),
                },
                Some(PresenceMember {
                    user_id: "7".into(),
                    user_info: serde_json::json!({}),
                }),
            )
            .await;
        assert!(
            occ.is_member_present("app", "presence-x", "7").await,
            "subscribed user must read as present"
        );
        assert!(
            !occ.is_member_present("app", "presence-x", "8").await,
            "a different user must read as absent"
        );
    }
}
