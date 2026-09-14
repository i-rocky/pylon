use super::AppManager;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub async fn probe_once(manager: &dyn AppManager, timeout: Duration) -> bool {
    match tokio::time::timeout(timeout, manager.probe()).await {
        Ok(Ok(())) => true,
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "app store probe failed");
            false
        }
        Err(_) => {
            tracing::warn!(
                timeout_ms = timeout.as_millis(),
                "app store probe timed out"
            );
            false
        }
    }
}

pub fn spawn_probe(
    manager: Arc<dyn AppManager>,
    up: Arc<AtomicBool>,
    interval_secs: u64,
    timeout_ms: u64,
) -> tokio::task::JoinHandle<()> {
    let timeout = Duration::from_millis(timeout_ms);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            up.store(
                probe_once(manager.as_ref(), timeout).await,
                Ordering::Relaxed,
            );
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::static_file::StaticFileAppManager;
    use crate::app::{AppLookup, AppLookupError};

    const SAMPLE: &str = r#"[{"name":"X","id":"a","key":"k","secret":"s"}]"#;

    #[tokio::test]
    async fn a_static_store_always_probes_up() {
        let m = StaticFileAppManager::from_json(SAMPLE).unwrap();
        assert!(probe_once(&m, std::time::Duration::from_millis(50)).await);
    }

    #[tokio::test]
    async fn a_probe_that_outlives_its_timeout_is_down() {
        struct Slow;
        #[async_trait::async_trait]
        impl AppManager for Slow {
            async fn by_id(&self, _: &str) -> Result<AppLookup, AppLookupError> {
                Ok(AppLookup::NotFound)
            }
            async fn by_key(&self, _: &str) -> Result<AppLookup, AppLookupError> {
                Ok(AppLookup::NotFound)
            }
            async fn probe(&self) -> Result<(), AppLookupError> {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                Ok(())
            }
        }
        tokio::time::pause();
        let probing =
            tokio::spawn(async { probe_once(&Slow, std::time::Duration::from_millis(2000)).await });
        tokio::time::advance(std::time::Duration::from_millis(2001)).await;
        assert!(!probing.await.unwrap(), "a timed-out probe reads down");
    }

    #[tokio::test]
    async fn spawn_probe_reflects_probe_verdict_over_time() {
        struct Flaky(Arc<AtomicBool>);
        #[async_trait::async_trait]
        impl AppManager for Flaky {
            async fn by_id(&self, _: &str) -> Result<AppLookup, AppLookupError> {
                Ok(AppLookup::NotFound)
            }
            async fn by_key(&self, _: &str) -> Result<AppLookup, AppLookupError> {
                Ok(AppLookup::NotFound)
            }
            async fn probe(&self) -> Result<(), AppLookupError> {
                if self.0.load(Ordering::Relaxed) {
                    Ok(())
                } else {
                    Err(AppLookupError::Backend("down".to_string()))
                }
            }
        }

        tokio::time::pause();
        let healthy = Arc::new(AtomicBool::new(false));
        let manager: Arc<dyn AppManager> = Arc::new(Flaky(healthy.clone()));
        let up = Arc::new(AtomicBool::new(true));
        let handle = spawn_probe(manager, up.clone(), 1, 500);

        for _ in 0..50 {
            tokio::time::advance(Duration::from_millis(50)).await;
            tokio::task::yield_now().await;
            if !up.load(Ordering::Relaxed) {
                break;
            }
        }
        assert!(
            !up.load(Ordering::Relaxed),
            "a failing probe must flip the flag to false"
        );

        healthy.store(true, Ordering::Relaxed);
        for _ in 0..50 {
            tokio::time::advance(Duration::from_millis(50)).await;
            tokio::task::yield_now().await;
            if up.load(Ordering::Relaxed) {
                break;
            }
        }
        assert!(
            up.load(Ordering::Relaxed),
            "a recovered probe must flip the flag back to true"
        );

        handle.abort();
    }
}
