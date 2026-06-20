//! The dispatcher actor: a spawned task draining a bounded mpsc, running a
//! trailing batch window, coalescing per app, filtering per endpoint by
//! `event_types`, signing, and handing deliveries to a `WebhookTransport`.

use crate::app::AppManager;
use crate::webhook::batch::coalesce;
use crate::webhook::event::WebhookEvent;
use crate::webhook::occupancy::OccupancySource;
use crate::webhook::transport::{build_signed_delivery, WebhookTransport};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Injectable wall clock so `time_ms` is deterministic under test.
pub trait Clock: Send + Sync {
    /// Unix epoch milliseconds at flush.
    fn now_ms(&self) -> u64;
}

/// Production clock: `SystemTime::now()`.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// Fixed clock for tests.
pub struct FixedClock(pub u64);

impl Clock for FixedClock {
    fn now_ms(&self) -> u64 {
        self.0
    }
}

/// The actor. Owns the mailbox, the window, the apps source, the clock, and the
/// transport. `run` consumes it.
pub struct WebhookDispatcher {
    rx: mpsc::Receiver<WebhookEvent>,
    apps: Arc<dyn AppManager>,
    transport: Arc<dyn WebhookTransport>,
    clock: Arc<dyn Clock>,
    batch_ms: u64,
    /// Grace window before a cluster `channel_vacated` fires (Task D1). `0`
    /// (with `occupancy = None`) means fire immediately (local-adapter path).
    vacated_grace_ms: u64,
    /// Cluster occupancy lookup used to re-check the subscription_count before a
    /// debounced vacated fires. `None` on the local-adapter path.
    occupancy: Option<Arc<dyn OccupancySource>>,
}

impl WebhookDispatcher {
    pub fn new(
        rx: mpsc::Receiver<WebhookEvent>,
        apps: Arc<dyn AppManager>,
        transport: Arc<dyn WebhookTransport>,
        clock: Arc<dyn Clock>,
        batch_ms: u64,
        vacated_grace_ms: u64,
        occupancy: Option<Arc<dyn OccupancySource>>,
    ) -> Self {
        Self {
            rx,
            apps,
            transport,
            clock,
            batch_ms,
            vacated_grace_ms,
            occupancy,
        }
    }

    /// Drain the mailbox forever. On the first event into an empty batch, start a
    /// trailing `batch_ms` timer; keep accumulating until it fires, then flush.
    pub async fn run(mut self) {
        loop {
            // Block until the first event of a new batch (or shutdown).
            let first = match self.rx.recv().await {
                Some(e) => e,
                None => return, // all senders dropped
            };
            let mut batch = vec![first];
            let deadline = tokio::time::Instant::now() + Duration::from_millis(self.batch_ms);

            // Accumulate until the trailing window elapses.
            loop {
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(deadline) => break,
                    maybe = self.rx.recv() => match maybe {
                        Some(e) => batch.push(e),
                        None => break, // senders dropped: flush what we have, then exit after
                    },
                }
            }

            self.flush(batch).await;
        }
    }

    /// Partition by app, coalesce, then per configured endpoint filter by
    /// `event_types`, build+sign one envelope, and deliver.
    ///
    /// On the cluster path (`vacated_grace_ms > 0` and `occupancy.is_some()`)
    /// each surviving `channel_vacated` is NOT delivered inline; instead a
    /// detached task sleeps `vacated_grace_ms`, re-checks the cluster
    /// subscription_count, and fires only if the channel is still empty
    /// (Task D1). All other survivors deliver inline exactly as before.
    async fn flush(&self, batch: Vec<WebhookEvent>) {
        use std::collections::HashMap;
        let mut by_app: HashMap<String, Vec<WebhookEvent>> = HashMap::new();
        for e in batch {
            by_app.entry(e.app().to_string()).or_default().push(e);
        }

        let cluster = self.vacated_grace_ms > 0 && self.occupancy.is_some();

        for (app_id, events) in by_app {
            let survivors = coalesce(events);
            if survivors.is_empty() {
                continue;
            }

            // On the cluster path, peel surviving vacated events off for the
            // debounced grace+recheck; everything else delivers inline now.
            let (deferred_vacated, immediate): (Vec<WebhookEvent>, Vec<WebhookEvent>) = if cluster {
                survivors
                    .into_iter()
                    .partition(|e| matches!(e, WebhookEvent::ChannelVacated { .. }))
            } else {
                (Vec::new(), survivors)
            };

            if !immediate.is_empty() {
                let app = match self.apps.by_id(&app_id).await {
                    Ok(Some(a)) => a,
                    Ok(None) => continue, // app vanished (hot-reload race): drop
                    Err(e) => { tracing::warn!(error = %e, "webhook app lookup failed; skipping cycle"); continue }
                };
                if !app.webhooks.is_empty() {
                    Self::deliver_app_events(
                        self.transport.as_ref(),
                        &app,
                        self.clock.now_ms(),
                        &immediate,
                    )
                    .await;
                }
            }

            // Cluster path: spawn one detached grace+recheck task per surviving
            // vacated event. It re-fetches the app at FIRE time (config may have
            // changed) and re-times the envelope with the fire-time clock.
            if !deferred_vacated.is_empty() {
                let occupancy = self
                    .occupancy
                    .clone()
                    .expect("cluster path implies occupancy is Some");
                for event in deferred_vacated {
                    let (app, channel) = match &event {
                        WebhookEvent::ChannelVacated { app, channel } => {
                            (app.clone(), channel.clone())
                        }
                        _ => unreachable!("partitioned to ChannelVacated only"),
                    };
                    let apps = self.apps.clone();
                    let transport = self.transport.clone();
                    let clock = self.clock.clone();
                    let occupancy = occupancy.clone();
                    let grace = self.vacated_grace_ms;
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(grace)).await;
                        let count = occupancy.subscription_count(&app, &channel).await;
                        if count != 0 {
                            tracing::trace!(
                                app = %app,
                                channel = %channel,
                                count,
                                "channel re-occupied within grace; suppressing channel_vacated"
                            );
                            return;
                        }
                        let resolved = match apps.by_id(&app).await {
                            Ok(Some(a)) => a,
                            Ok(None) => return, // app vanished: drop
                            Err(e) => { tracing::warn!(error = %e, "webhook app lookup failed; skipping cycle"); return }
                        };
                        if resolved.webhooks.is_empty() {
                            return;
                        }
                        Self::deliver_app_events(
                            transport.as_ref(),
                            &resolved,
                            clock.now_ms(),
                            std::slice::from_ref(&event),
                        )
                        .await;
                    });
                }
            }
        }
    }

    /// Per-endpoint filter (`event_types`) + build/sign + deliver for one app's
    /// surviving events. Shared by the immediate flush path and the deferred
    /// vacated firing so the loop is written once (DRY). `deliver` spawns the
    /// attempt loop and returns immediately; the `delivered_ok` / `delivered_failed`
    /// counters are bumped inside that spawned task (in the transport), so the
    /// dispatcher never blocks on a slow/failing endpoint.
    async fn deliver_app_events(
        transport: &dyn WebhookTransport,
        app: &crate::app::App,
        time_ms: u64,
        events: &[WebhookEvent],
    ) {
        for endpoint in &app.webhooks {
            let selected: Vec<serde_json::Value> = events
                .iter()
                .filter(|e| endpoint.event_types.iter().any(|t| t == e.name()))
                .map(|e| e.to_json())
                .collect();
            if selected.is_empty() {
                continue;
            }
            let custom: BTreeMap<String, String> = endpoint.headers.clone().into_iter().collect();
            let delivery = build_signed_delivery(
                &endpoint.url,
                &app.key,
                &app.secret,
                time_ms,
                &selected,
                &custom,
            );
            transport.deliver(delivery).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::AppLookupError;
    use crate::app::AppManager;
    use crate::app::{App, WebhookConfig};
    use crate::webhook::occupancy::OccupancySource;
    use crate::webhook::transport::{RecordingTransport, WebhookDelivery};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // A tiny single-app AppManager for the dispatcher test.
    struct OneApp(App);

    #[async_trait]
    impl AppManager for OneApp {
        async fn by_key(&self, key: &str) -> Result<Option<std::sync::Arc<App>>, AppLookupError> {
            Ok((self.0.key == key).then(|| std::sync::Arc::new(self.0.clone())))
        }
        async fn by_id(&self, id: &str) -> Result<Option<std::sync::Arc<App>>, AppLookupError> {
            Ok((self.0.id == id).then(|| std::sync::Arc::new(self.0.clone())))
        }
    }

    // A fake cluster-occupancy source: returns the stored count at fire time.
    struct FakeOccupancy(Arc<AtomicUsize>);

    #[async_trait]
    impl OccupancySource for FakeOccupancy {
        async fn subscription_count(&self, _app: &str, _channel: &str) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn app_with(webhooks: Vec<WebhookConfig>) -> App {
        let mut a = serde_json::from_value::<App>(serde_json::json!({
            "name": "t", "id": "app", "key": "app-key", "secret": "app-secret"
        }))
        .unwrap();
        a.webhooks = webhooks;
        a.recompute_has_flags();
        a
    }

    fn occ() -> WebhookEvent {
        WebhookEvent::ChannelOccupied {
            app: "app".into(),
            channel: "c".into(),
        }
    }
    fn vac() -> WebhookEvent {
        WebhookEvent::ChannelVacated {
            app: "app".into(),
            channel: "c".into(),
        }
    }
    fn miss() -> WebhookEvent {
        WebhookEvent::CacheMiss {
            app: "app".into(),
            channel: "cache-x".into(),
        }
    }

    /// Deterministically wait (under paused time) for the dispatcher task to
    /// finish its flush. After `advance` wakes the trailing-window timer, the
    /// spawned task still has several `.await` points (`by_id`, then `deliver`
    /// per endpoint) before deliveries land; a single `yield_now` is not enough
    /// to guarantee it ran to completion. Yield until the expected count is
    /// recorded (bounded, so a real regression still fails fast rather than
    /// hanging). This touches only the harness, not dispatcher semantics.
    async fn wait_for(transport: &RecordingTransport, expected: usize) -> Vec<WebhookDelivery> {
        for _ in 0..1000 {
            let recorded = transport.recorded().await;
            if recorded.len() >= expected {
                return recorded;
            }
            tokio::task::yield_now().await;
        }
        transport.recorded().await
    }

    #[tokio::test(start_paused = true)]
    async fn one_window_batches_and_coalesces_into_one_delivery() {
        let app = app_with(vec![WebhookConfig {
            url: "https://e.test/all".into(),
            event_types: vec![
                "channel_occupied".into(),
                "channel_vacated".into(),
                "cache_miss".into(),
            ],
            headers: Default::default(),
        }]);
        let apps: Arc<dyn AppManager> = Arc::new(OneApp(app));
        let transport = Arc::new(RecordingTransport::new());

        let (tx, rx) = mpsc::channel(64);
        let dispatcher = WebhookDispatcher {
            rx,
            apps,
            transport: transport.clone(),
            clock: Arc::new(FixedClock(1700000000000)),
            batch_ms: 50,
            vacated_grace_ms: 0,
            occupancy: None,
        };
        let task = tokio::spawn(dispatcher.run());

        // Three triggers inside ONE window: occ + vac (cancel) + miss (survives).
        tx.send(occ()).await.unwrap();
        tx.send(vac()).await.unwrap();
        tx.send(miss()).await.unwrap();

        // Let the dispatcher drain the mailbox and arm its trailing-window timer
        // BEFORE advancing time; otherwise `advance` would move the clock past the
        // not-yet-computed deadline and the window would never fire under paused
        // time. (Harness ordering only — dispatcher semantics unchanged.)
        tokio::task::yield_now().await;

        // Advance past the trailing window → exactly one flush.
        tokio::time::advance(Duration::from_millis(60)).await;

        let recorded = wait_for(&transport, 1).await;
        assert_eq!(recorded.len(), 1, "one endpoint, one delivery this window");
        let env: serde_json::Value = serde_json::from_str(&recorded[0].body).unwrap();
        assert_eq!(env["time_ms"], 1700000000000u64);
        let events = env["events"].as_array().unwrap();
        assert_eq!(
            events.len(),
            1,
            "occ+vac coalesced away; only cache_miss left"
        );
        assert_eq!(events[0]["name"], "cache_miss");

        drop(tx);
        let _ = task.await;
    }

    #[tokio::test(start_paused = true)]
    async fn event_types_filter_routes_per_endpoint() {
        let app = app_with(vec![
            WebhookConfig {
                url: "https://e.test/occ".into(),
                event_types: vec!["channel_occupied".into()],
                headers: Default::default(),
            },
            WebhookConfig {
                url: "https://e.test/miss".into(),
                event_types: vec!["cache_miss".into()],
                headers: Default::default(),
            },
        ]);
        let apps: Arc<dyn AppManager> = Arc::new(OneApp(app));
        let transport = Arc::new(RecordingTransport::new());
        let (tx, rx) = mpsc::channel(64);
        let dispatcher = WebhookDispatcher {
            rx,
            apps,
            transport: transport.clone(),
            clock: Arc::new(FixedClock(1)),
            batch_ms: 50,
            vacated_grace_ms: 0,
            occupancy: None,
        };
        let task = tokio::spawn(dispatcher.run());

        tx.send(occ()).await.unwrap();
        tx.send(miss()).await.unwrap();
        // Let the dispatcher arm its window before advancing time (see the other
        // test for why). Harness ordering only.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(60)).await;

        let recorded = wait_for(&transport, 2).await;
        assert_eq!(recorded.len(), 2, "one delivery per matching endpoint");
        // /occ endpoint got only channel_occupied; /miss got only cache_miss.
        let occ_ep = recorded.iter().find(|d| d.url.ends_with("/occ")).unwrap();
        let miss_ep = recorded.iter().find(|d| d.url.ends_with("/miss")).unwrap();
        let occ_env: serde_json::Value = serde_json::from_str(&occ_ep.body).unwrap();
        let miss_env: serde_json::Value = serde_json::from_str(&miss_ep.body).unwrap();
        assert_eq!(occ_env["events"][0]["name"], "channel_occupied");
        assert_eq!(occ_env["events"].as_array().unwrap().len(), 1);
        assert_eq!(miss_env["events"][0]["name"], "cache_miss");
        assert_eq!(miss_env["events"].as_array().unwrap().len(), 1);

        drop(tx);
        let _ = task.await;
    }

    fn vacated_app() -> App {
        app_with(vec![WebhookConfig {
            url: "https://e.test/vac".into(),
            event_types: vec!["channel_vacated".into()],
            headers: Default::default(),
        }])
    }

    /// Cluster path: grace window elapses, the channel is STILL empty at fire
    /// time → the debounced `channel_vacated` fires.
    #[tokio::test(start_paused = true)]
    async fn vacated_fires_after_grace_when_still_empty() {
        let apps: Arc<dyn AppManager> = Arc::new(OneApp(vacated_app()));
        let transport = Arc::new(RecordingTransport::new());
        let count = Arc::new(AtomicUsize::new(0)); // still empty at recheck
        let occupancy: Arc<dyn OccupancySource> = Arc::new(FakeOccupancy(count.clone()));

        let (tx, rx) = mpsc::channel(64);
        let dispatcher = WebhookDispatcher {
            rx,
            apps,
            transport: transport.clone(),
            clock: Arc::new(FixedClock(1700000000000)),
            batch_ms: 50,
            vacated_grace_ms: 3000,
            occupancy: Some(occupancy),
        };
        let task = tokio::spawn(dispatcher.run());

        tx.send(vac()).await.unwrap();
        // Arm the trailing window before advancing time (harness ordering only).
        tokio::task::yield_now().await;
        // Past the 50ms batch window → flush runs, deferred grace task spawned.
        tokio::time::advance(Duration::from_millis(60)).await;
        tokio::task::yield_now().await;
        // Elapse the 3000ms grace → the deferred recheck fires.
        tokio::time::advance(Duration::from_millis(3001)).await;

        let recorded = wait_for(&transport, 1).await;
        assert_eq!(
            recorded.len(),
            1,
            "vacated fires after grace when still empty"
        );
        let env: serde_json::Value = serde_json::from_str(&recorded[0].body).unwrap();
        let events = env["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["name"], "channel_vacated");

        drop(tx);
        let _ = task.await;
    }

    /// Cluster path: the channel is re-occupied somewhere in the cluster during
    /// the grace window (recheck count > 0) → the vacated webhook is suppressed.
    #[tokio::test(start_paused = true)]
    async fn vacated_suppressed_when_reoccupied_within_grace() {
        let apps: Arc<dyn AppManager> = Arc::new(OneApp(vacated_app()));
        let transport = Arc::new(RecordingTransport::new());
        let count = Arc::new(AtomicUsize::new(1)); // re-occupied at recheck
        let occupancy: Arc<dyn OccupancySource> = Arc::new(FakeOccupancy(count.clone()));

        let (tx, rx) = mpsc::channel(64);
        let dispatcher = WebhookDispatcher {
            rx,
            apps,
            transport: transport.clone(),
            clock: Arc::new(FixedClock(1700000000000)),
            batch_ms: 50,
            vacated_grace_ms: 3000,
            occupancy: Some(occupancy),
        };
        let task = tokio::spawn(dispatcher.run());

        tx.send(vac()).await.unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(60)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(3001)).await;

        // Give the deferred task ample scheduling slots; it must NOT deliver.
        for _ in 0..1000 {
            tokio::task::yield_now().await;
        }
        let recorded = transport.recorded().await;
        assert_eq!(
            recorded.len(),
            0,
            "vacated suppressed when re-occupied within grace"
        );

        drop(tx);
        let _ = task.await;
    }

    /// Local path: grace == 0 and no occupancy source → vacated fires immediately
    /// (no grace, no recheck), preserving the SP5 local-adapter behavior.
    #[tokio::test(start_paused = true)]
    async fn local_path_fires_vacated_immediately() {
        let apps: Arc<dyn AppManager> = Arc::new(OneApp(vacated_app()));
        let transport = Arc::new(RecordingTransport::new());

        let (tx, rx) = mpsc::channel(64);
        let dispatcher = WebhookDispatcher {
            rx,
            apps,
            transport: transport.clone(),
            clock: Arc::new(FixedClock(1700000000000)),
            batch_ms: 50,
            vacated_grace_ms: 0,
            occupancy: None,
        };
        let task = tokio::spawn(dispatcher.run());

        tx.send(vac()).await.unwrap();
        tokio::task::yield_now().await;
        // Only advance past the batch window — no grace needed on the local path.
        tokio::time::advance(Duration::from_millis(60)).await;

        let recorded = wait_for(&transport, 1).await;
        assert_eq!(recorded.len(), 1, "local path delivers vacated immediately");
        let env: serde_json::Value = serde_json::from_str(&recorded[0].body).unwrap();
        assert_eq!(env["events"][0]["name"], "channel_vacated");

        drop(tx);
        let _ = task.await;
    }
}
