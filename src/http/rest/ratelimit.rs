use crate::app::App;
use crate::rate::TokenBucket;
use crate::server::config::ServerConfig;
use moka::sync::Cache;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateHeaders {
    pub limit: u32,
    pub remaining: u32,
    pub retry_after_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateDecision {
    Allowed,
    Limited(RateHeaders),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RestRateLimitedCounts {
    pub node: u64,
    pub app_events: u64,
    pub app_reads: u64,
}

const BUCKET_IDLE: Duration = Duration::from_secs(300);
const BUCKET_CAPACITY: u64 = 100_000;

type SharedBucket = Arc<Mutex<TokenBucket>>;

pub struct RestRateLimits {
    origin: Instant,
    node_rate: u32,
    node: Mutex<TokenBucket>,
    default_events: u32,
    default_reads: u32,
    events: Cache<String, SharedBucket>,
    reads: Cache<String, SharedBucket>,
    limited_node: AtomicU64,
    limited_app_events: AtomicU64,
    limited_app_reads: AtomicU64,
}

fn fresh_bucket(rate_per_second: u32) -> SharedBucket {
    Arc::new(Mutex::new(TokenBucket::new(
        rate_per_second,
        rate_per_second,
    )))
}

fn bucket_cache() -> Cache<String, SharedBucket> {
    Cache::builder()
        .max_capacity(BUCKET_CAPACITY)
        .time_to_idle(BUCKET_IDLE)
        .build()
}

impl RestRateLimits {
    pub fn new(config: &ServerConfig) -> Self {
        Self {
            origin: Instant::now(),
            node_rate: config.max_rest_requests_per_second,
            node: Mutex::new(TokenBucket::new(
                config.max_rest_requests_per_second,
                config.max_rest_requests_per_second,
            )),
            default_events: config.max_backend_events_per_second,
            default_reads: config.max_read_requests_per_second,
            events: bucket_cache(),
            reads: bucket_cache(),
            limited_node: AtomicU64::new(0),
            limited_app_events: AtomicU64::new(0),
            limited_app_reads: AtomicU64::new(0),
        }
    }

    fn now_ns(&self) -> u64 {
        self.origin.elapsed().as_nanos() as u64
    }

    fn decide(bucket: &Mutex<TokenBucket>, now_ns: u64, cost: u32) -> RateDecision {
        let mut b = bucket.lock().unwrap_or_else(|e| e.into_inner());
        if b.take_at_ns(now_ns, cost) {
            return RateDecision::Allowed;
        }
        RateDecision::Limited(RateHeaders {
            limit: b.rate_per_second(),
            remaining: b.remaining_at_ns(now_ns),
            retry_after_secs: b.retry_after_secs_at_ns(now_ns, cost).max(1),
        })
    }

    pub fn check_node(&self) -> RateDecision {
        if self.node_rate == 0 {
            return RateDecision::Allowed;
        }
        let decision = Self::decide(&self.node, self.now_ns(), 1);
        if matches!(decision, RateDecision::Limited(_)) {
            self.limited_node.fetch_add(1, Ordering::Relaxed);
        }
        decision
    }

    fn app_decision(
        &self,
        cache: &Cache<String, SharedBucket>,
        app: &App,
        configured: u32,
        cost: u32,
    ) -> RateDecision {
        if configured == 0 {
            return RateDecision::Allowed;
        }
        let cached = cache.get_with(app.id.clone(), || fresh_bucket(configured));
        let built_for = {
            let b = cached.lock().unwrap_or_else(|e| e.into_inner());
            b.rate_per_second()
        };
        let bucket = if built_for == configured {
            cached
        } else {
            let replacement = fresh_bucket(configured);
            cache.insert(app.id.clone(), Arc::clone(&replacement));
            replacement
        };
        Self::decide(&bucket, self.now_ns(), cost)
    }

    pub fn check_app_events(&self, app: &App, cost: u32) -> RateDecision {
        let configured = app
            .max_backend_events_per_second
            .unwrap_or(self.default_events);
        let decision = self.app_decision(&self.events, app, configured, cost);
        if matches!(decision, RateDecision::Limited(_)) {
            self.limited_app_events.fetch_add(1, Ordering::Relaxed);
        }
        decision
    }

    pub fn check_app_reads(&self, app: &App) -> RateDecision {
        let configured = app
            .max_read_requests_per_second
            .unwrap_or(self.default_reads);
        let decision = self.app_decision(&self.reads, app, configured, 1);
        if matches!(decision, RateDecision::Limited(_)) {
            self.limited_app_reads.fetch_add(1, Ordering::Relaxed);
        }
        decision
    }

    pub fn counts(&self) -> RestRateLimitedCounts {
        RestRateLimitedCounts {
            node: self.limited_node.load(Ordering::Relaxed),
            app_events: self.limited_app_events.load(Ordering::Relaxed),
            app_reads: self.limited_app_reads.load(Ordering::Relaxed),
        }
    }
}

pub async fn node_rate_limit(
    axum::extract::State(state): axum::extract::State<crate::server::router::AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if let RateDecision::Limited(rate) = state.rest_limits.check_node() {
        return crate::http::error::RestError::too_many_requests("Rate limit exceeded", rate)
            .into_response();
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::App;
    use crate::server::config::ServerConfig;

    fn config(node: u32, events: u32, reads: u32) -> ServerConfig {
        ServerConfig {
            max_rest_requests_per_second: node,
            max_backend_events_per_second: events,
            max_read_requests_per_second: reads,
            ..ServerConfig::default()
        }
    }

    fn app_with(id: &str, events: Option<u32>, reads: Option<u32>) -> App {
        let mut app: App = serde_json::from_value(serde_json::json!({
            "name": "t", "id": id, "key": id, "secret": "s"
        }))
        .unwrap();
        app.max_backend_events_per_second = events;
        app.max_read_requests_per_second = reads;
        app
    }

    #[test]
    fn a_zero_node_cap_never_limits_and_never_touches_the_bucket() {
        let limits = RestRateLimits::new(&config(0, 0, 0));
        for i in 0..1_000 {
            assert_eq!(limits.check_node(), RateDecision::Allowed, "request {i}");
        }
        assert_eq!(limits.counts(), RestRateLimitedCounts::default());
        let node = limits
            .node
            .try_lock()
            .expect("the unlimited path must leave the node bucket free");
        assert_eq!(
            node.remaining_at_ns(0),
            u32::MAX,
            "an unconfigured node cap must spend nothing"
        );
    }

    #[test]
    fn the_node_cap_limits_past_its_budget_and_reports_the_headers() {
        let limits = RestRateLimits::new(&config(2, 0, 0));
        assert_eq!(limits.check_node(), RateDecision::Allowed);
        assert_eq!(limits.check_node(), RateDecision::Allowed);
        let RateDecision::Limited(headers) = limits.check_node() else {
            panic!("the third request inside the window must be limited");
        };
        assert_eq!(headers.limit, 2);
        assert_eq!(headers.remaining, 0);
        assert!(
            headers.retry_after_secs >= 1,
            "a limited request must never be told to retry after 0 seconds"
        );
        assert_eq!(
            limits.counts(),
            RestRateLimitedCounts {
                node: 1,
                app_events: 0,
                app_reads: 0
            }
        );
    }

    #[test]
    fn an_explicit_zero_override_is_unlimited_despite_the_server_default() {
        let limits = RestRateLimits::new(&config(0, 1, 1));
        let app = app_with("a", Some(0), Some(0));
        for i in 0..50 {
            assert_eq!(
                limits.check_app_events(&app, 10),
                RateDecision::Allowed,
                "publish {i}"
            );
            assert_eq!(
                limits.check_app_reads(&app),
                RateDecision::Allowed,
                "read {i}"
            );
        }
        assert_eq!(limits.counts(), RestRateLimitedCounts::default());
    }

    #[test]
    fn an_absent_override_falls_back_to_the_server_default() {
        let limits = RestRateLimits::new(&config(0, 2, 0));
        let app = app_with("a", None, None);
        assert_eq!(limits.check_app_events(&app, 2), RateDecision::Allowed);
        let RateDecision::Limited(headers) = limits.check_app_events(&app, 1) else {
            panic!("the server default must bound an app carrying no override");
        };
        assert_eq!(headers.limit, 2);
        assert_eq!(limits.counts().app_events, 1);
    }

    #[test]
    fn an_override_replaces_the_server_default_in_both_directions() {
        let limits = RestRateLimits::new(&config(0, 1, 0));
        let raised = app_with("raised", Some(3), None);
        for i in 0..3 {
            assert_eq!(
                limits.check_app_events(&raised, 1),
                RateDecision::Allowed,
                "publish {i} is inside the raised override"
            );
        }
        assert!(matches!(
            limits.check_app_events(&raised, 1),
            RateDecision::Limited(_)
        ));
        let limits = RestRateLimits::new(&config(0, 10, 0));
        let lowered = app_with("lowered", Some(1), None);
        assert_eq!(limits.check_app_events(&lowered, 1), RateDecision::Allowed);
        assert!(matches!(
            limits.check_app_events(&lowered, 1),
            RateDecision::Limited(_)
        ));
    }

    #[test]
    fn a_changed_limit_replaces_the_cached_bucket_without_a_restart() {
        let limits = RestRateLimits::new(&config(0, 0, 0));
        let mut app = app_with("a", Some(2), None);
        assert_eq!(limits.check_app_events(&app, 1), RateDecision::Allowed);
        assert_eq!(
            limits.check_app_events(&app, 1),
            RateDecision::Allowed,
            "a budget of 2 covers two publishes"
        );
        app.max_backend_events_per_second = Some(1);
        assert_eq!(
            limits.check_app_events(&app, 1),
            RateDecision::Allowed,
            "the changed limit must build a fresh bucket instead of reusing the spent one"
        );
        let RateDecision::Limited(headers) = limits.check_app_events(&app, 1) else {
            panic!("the fresh bucket holds 1 token, so the publish after it is over the limit");
        };
        assert_eq!(
            headers.limit, 1,
            "the headers must report the new limit, not the one the bucket was built with"
        );
    }

    #[test]
    fn a_batch_spends_one_token_per_event() {
        let limits = RestRateLimits::new(&config(0, 5, 0));
        let app = app_with("a", None, None);
        assert_eq!(limits.check_app_events(&app, 3), RateDecision::Allowed);
        assert_eq!(limits.check_app_events(&app, 2), RateDecision::Allowed);
        assert!(
            matches!(limits.check_app_events(&app, 1), RateDecision::Limited(_)),
            "3 + 2 events exactly spend a budget of 5"
        );
    }

    #[test]
    fn a_batch_larger_than_the_whole_budget_is_refused_without_spending_it() {
        let limits = RestRateLimits::new(&config(0, 3, 0));
        let app = app_with("a", None, None);
        let RateDecision::Limited(headers) = limits.check_app_events(&app, 4) else {
            panic!("a batch larger than the per-second budget can never be afforded");
        };
        assert_eq!(headers.limit, 3);
        assert!(headers.retry_after_secs >= 1);
        assert_eq!(
            limits.check_app_events(&app, 3),
            RateDecision::Allowed,
            "the refusal must not have spent any of the budget"
        );
    }

    #[test]
    fn reads_and_events_draw_on_separate_budgets() {
        let limits = RestRateLimits::new(&config(0, 1, 1));
        let app = app_with("a", None, None);
        assert_eq!(limits.check_app_events(&app, 1), RateDecision::Allowed);
        assert_eq!(
            limits.check_app_reads(&app),
            RateDecision::Allowed,
            "a read must not spend the publish budget"
        );
        assert!(matches!(
            limits.check_app_events(&app, 1),
            RateDecision::Limited(_)
        ));
        assert!(matches!(
            limits.check_app_reads(&app),
            RateDecision::Limited(_)
        ));
        assert_eq!(
            limits.counts(),
            RestRateLimitedCounts {
                node: 0,
                app_events: 1,
                app_reads: 1
            }
        );
    }

    #[test]
    fn each_app_gets_its_own_bucket() {
        let limits = RestRateLimits::new(&config(0, 1, 0));
        let noisy = app_with("noisy", None, None);
        let quiet = app_with("quiet", None, None);
        assert_eq!(limits.check_app_events(&noisy, 1), RateDecision::Allowed);
        assert_eq!(
            limits.check_app_events(&quiet, 1),
            RateDecision::Allowed,
            "one tenant's flood must not spend another tenant's budget"
        );
        assert!(matches!(
            limits.check_app_events(&noisy, 1),
            RateDecision::Limited(_)
        ));
    }
}
