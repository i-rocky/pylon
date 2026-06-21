//! Cluster-wide occupancy lookup for the cluster-aware `channel_vacated` grace
//! window (Task D1). Before a debounced vacated webhook fires, the dispatcher
//! re-checks the cluster subscription_count for the channel; if it is occupied
//! again anywhere in the cluster, the vacated webhook is suppressed.

use async_trait::async_trait;
use std::sync::Arc;

/// Source of the current cluster-wide subscription_count for a channel. The
/// dispatcher calls this at vacated FIRE time to decide whether the channel has
/// been re-occupied during the grace window.
#[async_trait]
pub trait OccupancySource: Send + Sync {
    /// Current cluster-wide subscription_count for (app, channel).
    async fn subscription_count(&self, app: &str, channel: &str) -> usize;
}

/// Adapter-backed occupancy source: defers to `Adapter::channel(...).subscription_count`.
pub struct AdapterOccupancy(pub Arc<dyn crate::adapter::Adapter>);

#[async_trait]
impl OccupancySource for AdapterOccupancy {
    async fn subscription_count(&self, app: &str, channel: &str) -> usize {
        self.0.channel(app, channel).await.subscription_count
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
}
