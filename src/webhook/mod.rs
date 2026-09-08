//! Webhooks (SP5): WS-lifecycle-driven, signed, batched HTTP notifications.
//!
//! `WebhookEvent` (the trigger) → `WebhookHandle` (cheap-clone mpsc sender) →
//! `WebhookDispatcher` (actor: window + sign) → `WebhookTransport`.

pub mod dispatcher;
pub mod event;
pub mod occupancy;
pub mod transport;

use crate::app::AppManager;
use dispatcher::{Clock, WebhookDispatcher};
use event::WebhookEvent;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use transport::WebhookTransport;

pub use occupancy::{AdapterOccupancy, OccupancySource};

/// Shared counters for the webhook pipeline, exposed via `/metrics`.
/// All fields are `AtomicU64`; hold as `Arc<WebhookMetrics>` to share between
/// the `WebhookHandle`, the dispatcher, and the metrics handler.
pub struct WebhookMetrics {
    /// Total events successfully enqueued via `WebhookHandle::enqueue`.
    pub enqueued: AtomicU64,
    /// Total events dropped on a full or closed mailbox.
    pub dropped: AtomicU64,
    /// Total webhook deliveries that resolved with a 2xx response.
    pub delivered_ok: AtomicU64,
    /// Total webhook deliveries that exhausted all retries without success.
    pub delivered_failed: AtomicU64,
    /// Maximum mailbox capacity (for queue-depth gauge: depth = max - remaining).
    pub max_capacity: usize,
}

impl WebhookMetrics {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            enqueued: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            delivered_ok: AtomicU64::new(0),
            delivered_failed: AtomicU64::new(0),
            max_capacity,
        }
    }
}

/// Cheap-clone enqueue handle held in `AppState` and `ConnectionContext`. The
/// WS path NEVER blocks on it: `enqueue` is a non-awaiting `try_send` that drops
/// (and logs) on a full mailbox (spec §8).
#[derive(Clone)]
pub struct WebhookHandle {
    tx: mpsc::Sender<WebhookEvent>,
    metrics: Arc<WebhookMetrics>,
}

impl WebhookHandle {
    /// A handle whose dispatcher is a draining sink (no deliveries). Used by tests
    /// and by any caller that wants webhooks disabled. Spawns a task that drains the
    /// receiver so enqueues never error; must run inside a tokio runtime.
    pub fn null() -> Self {
        let (tx, mut rx) = mpsc::channel(1024);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        WebhookHandle {
            tx,
            metrics: Arc::new(WebhookMetrics::new(1024)),
        }
    }

    /// The shared metrics for this webhook pipeline.
    pub fn metrics(&self) -> Arc<WebhookMetrics> {
        self.metrics.clone()
    }

    /// Current webhook mailbox depth: `max_capacity − remaining permits` (spec
    /// §8). `Sender::capacity()` returns the remaining permits, so this is the
    /// number of events queued but not yet drained by the dispatcher.
    pub fn queue_depth(&self) -> u64 {
        self.metrics.max_capacity.saturating_sub(self.tx.capacity()) as u64
    }

    /// Non-blocking enqueue. Drops + logs on a full or closed mailbox.
    pub fn enqueue(&self, event: WebhookEvent) {
        match self.tx.try_send(event) {
            Ok(()) => {
                self.metrics.enqueued.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(e)) => {
                self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    name = e.name(),
                    app = e.app(),
                    "webhook mailbox full; dropping"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                tracing::warn!("webhook dispatcher gone; dropping trigger");
            }
        }
    }
}

/// Spawn the dispatcher actor and return the enqueue handle. `mailbox_capacity`
/// sizes the bounded channel (the §8 backpressure safety valve).
///
/// `make_transport` is a factory handed the freshly-built `Arc<WebhookMetrics>`
/// so the transport (e.g. `HttpTransport`) shares the SAME counters as the
/// returned `WebhookHandle`: the handle owns `enqueued` / `dropped` /
/// `queue_depth`, the transport owns `delivered_ok` / `delivered_failed`. A
/// transport that doesn't count (e.g. `RecordingTransport`) simply ignores it.
///
/// `vacated_grace_ms` + `occupancy` enable the reconnect grace window (Task D1;
/// extended to `member_removed` by re-audit R12b): when a grace window is
/// configured, a surviving `channel_vacated` / `member_removed` is debounced by
/// it and — if an occupancy source is also supplied — re-checked before firing
/// (the cluster subscription_count for vacated, the user's presence for
/// member_removed), suppressed if the channel re-occupied / the user re-joined
/// within the window. Without an occupancy source the re-check is skipped and
/// the event fires after the grace (logged as an error). With `0` both fire
/// immediately, as before.
///
/// The transport factory is fallible: a build failure (e.g. `HttpTransport`'s
/// reqwest/TLS initialization) is returned as `Err` — BEFORE the dispatcher is
/// spawned — so startup fails cleanly with the real error instead of a panic
/// (G9).
pub fn spawn<F, E>(
    apps: Arc<dyn AppManager>,
    make_transport: F,
    clock: Arc<dyn Clock>,
    batch_ms: u64,
    mailbox_capacity: usize,
    vacated_grace_ms: u64,
    occupancy: Option<Arc<dyn OccupancySource>>,
) -> Result<WebhookHandle, E>
where
    F: FnOnce(Arc<WebhookMetrics>) -> Result<Arc<dyn WebhookTransport>, E>,
{
    let (tx, rx) = mpsc::channel(mailbox_capacity);
    let metrics = Arc::new(WebhookMetrics::new(mailbox_capacity));
    let transport = make_transport(metrics.clone())?;
    let dispatcher = WebhookDispatcher::new(
        rx,
        apps,
        transport,
        clock,
        batch_ms,
        vacated_grace_ms,
        occupancy,
    );
    tokio::spawn(dispatcher.run());
    Ok(WebhookHandle { tx, metrics })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::static_file::StaticFileAppManager;

    /// The single app the dispatcher tests below resolve, with one endpoint
    /// subscribed to `channel_occupied`.
    const APPS: &str = r#"[
        {"name":"T","id":"app1","key":"k","secret":"s",
         "webhooks":[{"url":"https://hook.test","event_types":["channel_occupied"]}]}
    ]"#;

    fn apps() -> Arc<dyn AppManager> {
        Arc::new(StaticFileAppManager::from_json(APPS).expect("apps json must parse"))
    }

    fn occupied(app: &str, channel: &str) -> WebhookEvent {
        WebhookEvent::ChannelOccupied {
            app: app.to_string(),
            channel: channel.to_string(),
        }
    }

    /// G9: a transport factory failure must propagate out of `spawn` as an
    /// `Err` — startup fails cleanly with the real error — instead of being
    /// forced into a panic inside the factory (the old `HttpTransport::new`
    /// `.expect`). Uses an injected error: reqwest's builder cannot be made
    /// to fail deterministically without mocking reqwest (deliberately not
    /// done), so the reqwest-specific failure MODE is not testable here —
    /// this pins the Result plumbing end to end.
    #[tokio::test]
    async fn spawn_propagates_transport_factory_error() {
        let result: Result<WebhookHandle, ()> = spawn(
            apps(),
            |_metrics| Err(()),
            Arc::new(dispatcher::FixedClock(0)),
            10,
            16,
            0,
            None,
        );
        assert!(result.is_err(), "factory failure must surface from spawn");
    }

    /// §8 backpressure: the WS path must never block on a lagging webhook
    /// dispatcher. A full mailbox DROPS the trigger and counts it, and — the
    /// part that matters — the events already queued are untouched, so the
    /// dispatcher still delivers everything it accepted.
    #[tokio::test]
    async fn a_full_mailbox_drops_the_trigger_and_counts_it() {
        let (tx, mut rx) = mpsc::channel(1);
        let handle = WebhookHandle {
            tx,
            metrics: Arc::new(WebhookMetrics::new(1)),
        };

        handle.enqueue(occupied("app1", "first"));
        assert_eq!(handle.queue_depth(), 1, "the accepted event is queued");
        assert_eq!(handle.metrics().enqueued.load(Ordering::Relaxed), 1);
        assert_eq!(handle.metrics().dropped.load(Ordering::Relaxed), 0);

        handle.enqueue(occupied("app1", "overflow"));
        assert_eq!(
            handle.metrics().dropped.load(Ordering::Relaxed),
            1,
            "the trigger that did not fit must be counted as dropped"
        );
        assert_eq!(
            handle.metrics().enqueued.load(Ordering::Relaxed),
            1,
            "a dropped trigger must NOT be counted as enqueued"
        );

        assert_eq!(
            rx.try_recv().map(|e| e.name()).ok(),
            Some("channel_occupied"),
            "the queued event must survive the overflow"
        );
        assert!(
            rx.try_recv().is_err(),
            "the dropped trigger must not have been queued"
        );
    }

    /// A trigger naming an app the store does not know has no endpoints to go
    /// to, so it is discarded rather than posted somewhere arbitrary.
    ///
    /// The negative is gated, not timed: the unknown-app trigger is enqueued
    /// FIRST and a known-app trigger behind it. The dispatcher is one actor
    /// draining one mailbox in order, so once the known app's delivery is
    /// recorded the unknown one has demonstrably already been processed — and
    /// the recording must hold that one delivery and nothing else.
    #[tokio::test]
    async fn a_trigger_for_an_unknown_app_delivers_nowhere() {
        let recorder = Arc::new(transport::RecordingTransport::new());
        let sink = recorder.clone();
        let handle: WebhookHandle = spawn::<_, std::convert::Infallible>(
            apps(),
            move |_metrics| Ok(sink as Arc<dyn WebhookTransport>),
            Arc::new(dispatcher::FixedClock(0)),
            1,
            16,
            0,
            None,
        )
        .expect("the recording transport factory cannot fail");

        handle.enqueue(occupied("no-such-app", "public-c"));
        handle.enqueue(occupied("app1", "public-c"));

        let recorded = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let recorded = recorder.recorded().await;
                if !recorded.is_empty() {
                    return recorded;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the known app's webhook must be delivered");

        assert_eq!(
            recorded.len(),
            1,
            "exactly one delivery — the unknown app's trigger, drained first, produced none"
        );
        assert_eq!(recorded[0].url, "https://hook.test");
    }
}
