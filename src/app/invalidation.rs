use super::purger::AppPurger;
use fred::interfaces::PubsubInterface;
use fred::prelude::*;
use std::sync::Arc;

/// Cross-node app-cache invalidation over Redis pub/sub.
pub struct AppInvalidator {
    publish_pool: Pool,
}

pub const INVALIDATE_CHANNEL: &str = "pylon:app:invalidate";

/// The invalidation action. `Refresh` (config/secret change) evicts cache only;
/// `Remove` (disabled/deleted) additionally force-closes connections + reclaims
/// the per-app counter. `#[serde(default)]` on the field + `#[default] Refresh`
/// means any legacy/blank message degrades to the SAFE (non-destructive) action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InvalidateAction {
    #[default]
    Refresh,
    Remove,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct InvalidateMsg {
    id: String,
    key: String,
    #[serde(default)]
    action: InvalidateAction,
}

impl AppInvalidator {
    /// Connect to `url`, subscribe to the invalidation channel (dispatching each
    /// message through `purger`), and return a handle that can publish
    /// invalidations.
    pub async fn spawn(url: &str, purger: Arc<AppPurger>) -> anyhow::Result<Arc<Self>> {
        // `max_attempts = 0` means retry forever; min 100ms, max 30s, base 2.
        let policy = ReconnectPolicy::new_exponential(0, 100, 30_000, 2);
        let mut builder = Builder::from_config(Config::from_url(url)?);
        builder.set_policy(policy);

        let publish_pool = builder.build_pool(2)?;
        publish_pool.init().await?;

        let sub = builder.build_subscriber_client()?;
        sub.init().await?;
        // Keep the resubscribe task handle so it isn't dropped (which would stop it).
        let _mgr = sub.manage_subscriptions();
        sub.subscribe(INVALIDATE_CHANNEL).await?;
        let mut rx = sub.message_rx();
        tokio::spawn(async move {
            // hold `sub` and `_mgr` for the task's lifetime so the subscription stays open
            let _sub = sub;
            let _sub_mgr = _mgr;
            loop {
                match rx.recv().await {
                    Ok(msg) => match msg.value.into_string() {
                        Some(s) => {
                            if let Ok(m) = serde_json::from_str::<InvalidateMsg>(&s) {
                                match m.action {
                                    InvalidateAction::Refresh => {
                                        purger.refresh(&m.id, &m.key).await
                                    }
                                    InvalidateAction::Remove => purger.purge(&m.id, &m.key).await,
                                }
                            } else {
                                tracing::warn!(payload = %s, "bad app-invalidate message");
                            }
                        }
                        None => {
                            tracing::warn!("dropped non-UTF8 app-invalidate payload");
                        }
                    },
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            skipped = n,
                            "app-invalidate sub stream lagged; dropped messages"
                        );
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        Ok(Arc::new(Self { publish_pool }))
    }

    pub async fn publish(
        &self,
        id: &str,
        key: &str,
        action: InvalidateAction,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_string(&InvalidateMsg {
            id: id.into(),
            key: key.into(),
            action,
        })?;
        let _: () = self
            .publish_pool
            .next()
            .publish(INVALIDATE_CHANNEL, payload)
            .await?;
        Ok(())
    }
}
