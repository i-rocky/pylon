//! GET /metrics — Prometheus text exposition format v0.0.4.

use crate::cluster::bridge::ClusterMetrics;
use crate::server::router::AppState;
use crate::transport::percore_metrics_snapshot;
use crate::webhook::WebhookMetrics;
use axum::extract::State;
use axum::http::{header, HeaderValue};
use axum::response::IntoResponse;
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

/// Prometheus-escape a label value per the text format spec:
/// `\` → `\\`, `"` → `\"`, newline → `\n`.
fn escape_label(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// Per-app metric data collected before encoding.
pub struct AppMetrics {
    pub connections: u64,
    pub channels_occupied: u64,
    pub subscriptions: u64,
}

/// Full snapshot passed to the encoder.
pub struct MetricsSnapshot {
    pub apps: HashMap<String, AppMetrics>,
    pub saturation: Option<bool>,
    pub percore: Option<crate::transport::PercoreMetricsSnapshot>,
    /// Phase-2 B2: webhook pipeline counters.
    pub webhook: Option<Arc<WebhookMetrics>>,
    /// Phase-2 B2: current webhook queue depth (max_capacity - remaining permits).
    /// Pre-computed by the handler so the pure encoder doesn't need the Sender.
    pub webhook_queue_depth: Option<u64>,
    /// Phase-2 B3: cluster bridge counters (only present on the Redis path).
    pub cluster: Option<Arc<ClusterMetrics>>,
}

/// Pure encoder: given a snapshot, return the Prometheus text body.
pub fn encode(snapshot: &MetricsSnapshot) -> String {
    let mut out = String::new();

    // pylon_up
    out.push_str("# HELP pylon_up Pylon process is up (liveness/scrape check)\n");
    out.push_str("# TYPE pylon_up gauge\n");
    out.push_str("pylon_up 1\n");

    // pylon_saturation_flag (omit if None)
    if let Some(sat) = snapshot.saturation {
        out.push_str(
            "# HELP pylon_saturation_flag Broadcast pipeline saturation (1 = saturated)\n",
        );
        out.push_str("# TYPE pylon_saturation_flag gauge\n");
        let _ = writeln!(out, "pylon_saturation_flag {}", if sat { 1 } else { 0 });
    }

    // Per-app metrics
    let mut app_ids: Vec<&String> = snapshot.apps.keys().collect();
    app_ids.sort();

    if !app_ids.is_empty() {
        out.push_str("# HELP pylon_connections Number of live WebSocket connections per app\n");
        out.push_str("# TYPE pylon_connections gauge\n");
        for app_id in &app_ids {
            let m = &snapshot.apps[*app_id];
            let _ = writeln!(
                out,
                "pylon_connections{{app=\"{}\"}} {}",
                escape_label(app_id),
                m.connections
            );
        }

        out.push_str("# HELP pylon_channels_occupied Number of occupied channels per app\n");
        out.push_str("# TYPE pylon_channels_occupied gauge\n");
        for app_id in &app_ids {
            let m = &snapshot.apps[*app_id];
            let _ = writeln!(
                out,
                "pylon_channels_occupied{{app=\"{}\"}} {}",
                escape_label(app_id),
                m.channels_occupied
            );
        }

        out.push_str("# HELP pylon_subscriptions Total channel subscriptions per app\n");
        out.push_str("# TYPE pylon_subscriptions gauge\n");
        for app_id in &app_ids {
            let m = &snapshot.apps[*app_id];
            let _ = writeln!(
                out,
                "pylon_subscriptions{{app=\"{}\"}} {}",
                escape_label(app_id),
                m.subscriptions
            );
        }
    }

    // Per-core worker metrics (omit entirely if no percore fleet ran)
    if let Some(ref pc) = snapshot.percore {
        out.push_str("# HELP pylon_broadcast_dropped_total Broadcasts dropped due to full worker hand-off channel (cumulative)\n");
        out.push_str("# TYPE pylon_broadcast_dropped_total counter\n");
        for (i, &dropped) in pc.dropped.iter().enumerate() {
            let _ = writeln!(
                out,
                "pylon_broadcast_dropped_total{{worker=\"{i}\"}} {dropped}"
            );
        }

        out.push_str("# HELP pylon_inflight_bytes Bytes queued in per-worker outbound buffers\n");
        out.push_str("# TYPE pylon_inflight_bytes gauge\n");
        for (i, &inflight) in pc.inflight.iter().enumerate() {
            let _ = writeln!(out, "pylon_inflight_bytes{{worker=\"{i}\"}} {inflight}");
        }

        // `_sum` not `_total`: this is a gauge (sum of per-worker gauges), and the
        // `_total` suffix conventionally signals a counter to Prometheus tooling.
        out.push_str("# HELP pylon_inflight_bytes_sum Total bytes queued across all workers\n");
        out.push_str("# TYPE pylon_inflight_bytes_sum gauge\n");
        let _ = writeln!(out, "pylon_inflight_bytes_sum {}", pc.inflight_total);

        out.push_str("# HELP pylon_worker_budget_bytes Per-worker memory budget in bytes\n");
        out.push_str("# TYPE pylon_worker_budget_bytes gauge\n");
        let _ = writeln!(out, "pylon_worker_budget_bytes {}", pc.worker_budget_bytes);

        out.push_str("# HELP pylon_budget_factor PSI memory-pressure budget factor (0.0–1.0)\n");
        out.push_str("# TYPE pylon_budget_factor gauge\n");
        let _ = writeln!(out, "pylon_budget_factor {:.3}", pc.budget_factor);

        out.push_str(
            "# HELP pylon_accepted_connections_total Cumulative connections accepted per worker\n",
        );
        out.push_str("# TYPE pylon_accepted_connections_total counter\n");
        for (i, &accepted) in pc.accepted.iter().enumerate() {
            let _ = writeln!(
                out,
                "pylon_accepted_connections_total{{worker=\"{i}\"}} {accepted}"
            );
        }

        out.push_str("# HELP pylon_codel_dropped_total Frames dropped by CoDel staleness check per worker (cumulative)\n");
        out.push_str("# TYPE pylon_codel_dropped_total counter\n");
        for (i, &codel) in pc.codel_dropped.iter().enumerate() {
            let _ = writeln!(out, "pylon_codel_dropped_total{{worker=\"{i}\"}} {codel}");
        }

        out.push_str("# HELP pylon_drophead_dropped_total Frames evicted by the per-connection drop-head queue per worker (cumulative)\n");
        out.push_str("# TYPE pylon_drophead_dropped_total counter\n");
        for (i, &drophead) in pc.drophead_dropped.iter().enumerate() {
            let _ = writeln!(
                out,
                "pylon_drophead_dropped_total{{worker=\"{i}\"}} {drophead}"
            );
        }

        out.push_str("# HELP pylon_mailbox_dropped_total Frames dropped due to per-connection mailbox full per worker (cumulative)\n");
        out.push_str("# TYPE pylon_mailbox_dropped_total counter\n");
        for (i, &mb) in pc.mailbox_dropped.iter().enumerate() {
            let _ = writeln!(out, "pylon_mailbox_dropped_total{{worker=\"{i}\"}} {mb}");
        }
    }

    // Phase-2 B2: webhook pipeline metrics.
    if let Some(ref wh) = snapshot.webhook {
        use std::sync::atomic::Ordering;
        let enqueued = wh.enqueued.load(Ordering::Relaxed);
        let dropped = wh.dropped.load(Ordering::Relaxed);
        let ok = wh.delivered_ok.load(Ordering::Relaxed);
        let failed = wh.delivered_failed.load(Ordering::Relaxed);

        out.push_str(
            "# HELP pylon_webhook_enqueued_total Total webhook events successfully enqueued\n",
        );
        out.push_str("# TYPE pylon_webhook_enqueued_total counter\n");
        let _ = writeln!(out, "pylon_webhook_enqueued_total {enqueued}");

        out.push_str("# HELP pylon_webhook_dropped_total Total webhook events dropped on full/closed mailbox\n");
        out.push_str("# TYPE pylon_webhook_dropped_total counter\n");
        let _ = writeln!(out, "pylon_webhook_dropped_total {dropped}");

        out.push_str("# HELP pylon_webhook_delivered_total Total webhook deliveries by outcome\n");
        out.push_str("# TYPE pylon_webhook_delivered_total counter\n");
        let _ = writeln!(out, "pylon_webhook_delivered_total{{status=\"ok\"}} {ok}");
        let _ = writeln!(
            out,
            "pylon_webhook_delivered_total{{status=\"failed\"}} {failed}"
        );

        // Queue depth gauge: max_capacity − remaining permits, computed by the
        // handler from `WebhookHandle::queue_depth()` (it holds the Sender).
        if let Some(queue_depth) = snapshot.webhook_queue_depth {
            out.push_str("# HELP pylon_webhook_queue_depth Current number of events in the webhook mailbox\n");
            out.push_str("# TYPE pylon_webhook_queue_depth gauge\n");
            let _ = writeln!(out, "pylon_webhook_queue_depth {queue_depth}");
        }
    }

    // Phase-2 B3: cluster bridge metrics (only when Some — Redis path only).
    if let Some(ref cm) = snapshot.cluster {
        use std::sync::atomic::Ordering;
        let dropped = cm.cmd_dropped.load(Ordering::Relaxed);
        let connected = if cm.redis_connected.load(Ordering::Relaxed) {
            1u64
        } else {
            0u64
        };

        out.push_str("# HELP pylon_cluster_cmd_dropped_total Total ClusterCmds dropped on a full bridge channel\n");
        out.push_str("# TYPE pylon_cluster_cmd_dropped_total counter\n");
        let _ = writeln!(out, "pylon_cluster_cmd_dropped_total {dropped}");

        out.push_str(
            "# HELP pylon_redis_connected Redis connection health (1=connected, 0=error)\n",
        );
        out.push_str("# TYPE pylon_redis_connected gauge\n");
        let _ = writeln!(out, "pylon_redis_connected {connected}");
    }

    out
}

/// Extract the bearer token from `Authorization: Bearer <token>`.
/// Returns `""` when the header is absent, malformed, or not a Bearer scheme —
/// the caller compares it against the configured token, so a missing token is
/// simply "wrong" (and gets the same 404 as a wrong token).
fn bearer_token(headers: &axum::http::HeaderMap) -> &str {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
}

/// Constant-time bearer-token compare with **no early exit**: the length
/// difference folds into the same bytewise accumulation as content
/// differences, so the compare always walks `max(len)` bytes regardless of
/// where (or whether) the inputs diverge. Token LENGTHS are not secret (the
/// operator picks them), but the discipline is kept anyway: no short-circuit
/// on the first differing byte or on a length mismatch.
fn token_matches(presented: &str, expected: &str) -> bool {
    use subtle::ConstantTimeEq;
    let (p, e) = (presented.as_bytes(), expected.as_bytes());
    let max = p.len().max(e.len());
    // AND-fold: start from the identity (1 = "equal so far") with the length
    // equality folded in as a Choice (0/1), not a branch; out-of-range bytes
    // (only possible on a length mismatch) read as 0.
    let mut eq = subtle::Choice::from(u8::from(p.len() == e.len()));
    for i in 0..max {
        let a = p.get(i).copied().unwrap_or(0);
        let b = e.get(i).copied().unwrap_or(0);
        eq &= a.ct_eq(&b);
    }
    eq.into()
}

pub async fn get_metrics(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    // S1: optional bearer gate. When `PYLON_METRICS_TOKEN` is set, a scrape
    // must present it. Missing/wrong token → 404 (NOT 401): an
    // unauthenticated prober must not distinguish "metrics exist but are
    // protected" from "no such route" — the same existence-non-disclosure
    // posture as the disabled admin API. `/health` + `/ready` stay open.
    if let Some(expected) = state.config.metrics_token.as_deref() {
        if !token_matches(bearer_token(&headers), expected) {
            return crate::http::error::RestError::not_found("Not found").into_response();
        }
    }

    use std::sync::atomic::Ordering;

    // Collect per-app metrics.
    let mut apps: HashMap<String, AppMetrics> = HashMap::new();
    for entry in state.conn_counts.iter() {
        let app_id = entry.key().clone();
        let connections = entry.value().load(Ordering::Relaxed) as u64;
        // Get channel info from the adapter.
        let summaries = state.adapter.channels(&app_id, None).await;
        let channels_occupied = summaries.iter().filter(|s| s.occupied).count() as u64;
        let subscriptions: u64 = summaries.iter().map(|s| s.subscription_count as u64).sum();
        apps.insert(
            app_id,
            AppMetrics {
                connections,
                channels_occupied,
                subscriptions,
            },
        );
    }

    let saturation = state.saturated.as_ref().map(|s| s.load(Ordering::Relaxed));
    let percore = percore_metrics_snapshot();

    // Phase-2 B2: webhook metrics + queue depth. The depth is the spec formula
    // `max_capacity − remaining permits`, read straight off the handle's Sender.
    let webhook = Some(state.webhooks.metrics());
    let webhook_queue_depth = Some(state.webhooks.queue_depth());

    // Phase-2 B3: cluster metrics (Some on Redis path, None on local path).
    let cluster = state.cluster_metrics.clone();

    let body = encode(&MetricsSnapshot {
        apps,
        saturation,
        percore,
        webhook,
        webhook_queue_depth,
        cluster,
    });

    let mut response = axum::response::Response::new(axum::body::Body::from(body));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot_with_one_app(
        app_id: &str,
        conns: u64,
        channels: u64,
        subs: u64,
    ) -> MetricsSnapshot {
        let mut apps = HashMap::new();
        apps.insert(
            app_id.to_string(),
            AppMetrics {
                connections: conns,
                channels_occupied: channels,
                subscriptions: subs,
            },
        );
        MetricsSnapshot {
            apps,
            saturation: None,
            percore: None,
            webhook: None,
            webhook_queue_depth: None,
            cluster: None,
        }
    }

    #[test]
    fn encode_pylon_up_always_present() {
        let s = snapshot_with_one_app("app1", 0, 0, 0);
        let text = encode(&s);
        assert!(text.contains("# HELP pylon_up "), "missing HELP pylon_up");
        assert!(
            text.contains("# TYPE pylon_up gauge"),
            "missing TYPE pylon_up"
        );
        assert!(text.contains("pylon_up 1\n"), "missing pylon_up 1");
    }

    #[test]
    fn encode_per_app_metrics() {
        let s = snapshot_with_one_app("myapp", 42, 7, 15);
        let text = encode(&s);
        assert!(
            text.contains("pylon_connections{app=\"myapp\"} 42\n"),
            "connections missing: {text}"
        );
        assert!(
            text.contains("pylon_channels_occupied{app=\"myapp\"} 7\n"),
            "channels missing: {text}"
        );
        assert!(
            text.contains("pylon_subscriptions{app=\"myapp\"} 15\n"),
            "subscriptions missing: {text}"
        );
        assert!(
            text.contains("# HELP pylon_connections "),
            "missing HELP connections"
        );
        assert!(
            text.contains("# TYPE pylon_connections gauge"),
            "missing TYPE connections"
        );
    }

    #[test]
    fn encode_saturation_flag_present_when_some() {
        let s = MetricsSnapshot {
            apps: HashMap::new(),
            saturation: Some(true),
            percore: None,
            webhook: None,
            webhook_queue_depth: None,
            cluster: None,
        };
        let text = encode(&s);
        assert!(
            text.contains("pylon_saturation_flag 1\n"),
            "saturation flag missing: {text}"
        );

        let s2 = MetricsSnapshot {
            apps: HashMap::new(),
            saturation: Some(false),
            percore: None,
            webhook: None,
            webhook_queue_depth: None,
            cluster: None,
        };
        let text2 = encode(&s2);
        assert!(
            text2.contains("pylon_saturation_flag 0\n"),
            "saturation=0 missing: {text2}"
        );
    }

    #[test]
    fn encode_saturation_flag_omitted_when_none() {
        let s = MetricsSnapshot {
            apps: HashMap::new(),
            saturation: None,
            percore: None,
            webhook: None,
            webhook_queue_depth: None,
            cluster: None,
        };
        let text = encode(&s);
        assert!(
            !text.contains("pylon_saturation_flag"),
            "saturation flag must be absent: {text}"
        );
    }

    #[test]
    fn encode_label_escaping() {
        // Label value with backslash, double-quote, and newline.
        let app_id = "my\\app\"with\nnewline";
        let s = snapshot_with_one_app(app_id, 1, 0, 0);
        let text = encode(&s);
        // Expect escaping: \ → \\, " → \", newline → \n
        assert!(
            text.contains(r#"pylon_connections{app="my\\app\"with\nnewline"} 1"#),
            "label escaping incorrect:\n{text}"
        );
    }

    #[test]
    fn encode_percore_metrics_present_when_some() {
        use crate::transport::PercoreMetricsSnapshot;
        let pc = PercoreMetricsSnapshot {
            inflight: vec![100, 200],
            dropped: vec![5, 3],
            accepted: vec![10, 20],
            codel_dropped: vec![1, 2],
            drophead_dropped: vec![0, 0],
            mailbox_dropped: vec![0, 0],
            inflight_total: 300,
            budget_factor: 0.9,
            worker_budget_bytes: 1024 * 1024 * 512,
        };
        let s = MetricsSnapshot {
            apps: HashMap::new(),
            saturation: None,
            percore: Some(pc),
            webhook: None,
            webhook_queue_depth: None,
            cluster: None,
        };
        let text = encode(&s);
        assert!(
            text.contains("pylon_inflight_bytes{worker=\"0\"} 100\n"),
            "worker 0 inflight: {text}"
        );
        assert!(
            text.contains("pylon_inflight_bytes{worker=\"1\"} 200\n"),
            "worker 1 inflight: {text}"
        );
        assert!(
            text.contains("pylon_broadcast_dropped_total{worker=\"0\"} 5\n"),
            "worker 0 dropped: {text}"
        );
        assert!(
            text.contains("pylon_broadcast_dropped_total{worker=\"1\"} 3\n"),
            "worker 1 dropped: {text}"
        );
        assert!(
            text.contains("pylon_inflight_bytes_sum 300\n"),
            "inflight_total: {text}"
        );
        assert!(
            text.contains("pylon_budget_factor 0.900\n"),
            "budget_factor: {text}"
        );
        assert!(
            text.contains(&format!(
                "pylon_worker_budget_bytes {}\n",
                1024 * 1024 * 512
            )),
            "budget_bytes: {text}"
        );
        assert!(
            text.contains("# TYPE pylon_broadcast_dropped_total counter"),
            "type counter: {text}"
        );
        assert!(
            text.contains("# TYPE pylon_inflight_bytes gauge"),
            "type gauge: {text}"
        );
    }

    #[test]
    fn encode_percore_metrics_absent_when_none() {
        let s = MetricsSnapshot {
            apps: HashMap::new(),
            saturation: None,
            percore: None,
            webhook: None,
            webhook_queue_depth: None,
            cluster: None,
        };
        let text = encode(&s);
        assert!(
            !text.contains("pylon_inflight_bytes"),
            "percore must be absent: {text}"
        );
        assert!(
            !text.contains("pylon_broadcast_dropped_total"),
            "percore must be absent: {text}"
        );
    }

    #[test]
    fn encode_help_type_before_series() {
        let s = snapshot_with_one_app("a", 1, 0, 0);
        let text = encode(&s);
        let help_pos = text.find("# HELP pylon_connections").unwrap();
        let type_pos = text.find("# TYPE pylon_connections").unwrap();
        let series_pos = text.find("pylon_connections{").unwrap();
        assert!(help_pos < type_pos, "HELP must precede TYPE");
        assert!(type_pos < series_pos, "TYPE must precede series");
    }

    #[test]
    fn encode_percore_accepted_and_codel_present_when_some() {
        use crate::transport::PercoreMetricsSnapshot;
        let pc = PercoreMetricsSnapshot {
            inflight: vec![0, 0],
            dropped: vec![0, 0],
            accepted: vec![42, 17],
            codel_dropped: vec![3, 0],
            drophead_dropped: vec![0, 0],
            mailbox_dropped: vec![0, 0],
            inflight_total: 0,
            budget_factor: 1.0,
            worker_budget_bytes: 1,
        };
        let s = MetricsSnapshot {
            apps: HashMap::new(),
            saturation: None,
            percore: Some(pc),
            webhook: None,
            webhook_queue_depth: None,
            cluster: None,
        };
        let text = encode(&s);
        assert!(
            text.contains("pylon_accepted_connections_total{worker=\"0\"} 42\n"),
            "accepted w0: {text}"
        );
        assert!(
            text.contains("pylon_accepted_connections_total{worker=\"1\"} 17\n"),
            "accepted w1: {text}"
        );
        assert!(
            text.contains("pylon_codel_dropped_total{worker=\"0\"} 3\n"),
            "codel_dropped w0: {text}"
        );
        assert!(
            text.contains("pylon_codel_dropped_total{worker=\"1\"} 0\n"),
            "codel_dropped w1: {text}"
        );
        assert!(
            text.contains("# TYPE pylon_accepted_connections_total counter"),
            "type counter accepted: {text}"
        );
        assert!(
            text.contains("# TYPE pylon_codel_dropped_total counter"),
            "type counter codel: {text}"
        );
    }

    #[test]
    fn encode_percore_drophead_present_when_some() {
        use crate::transport::PercoreMetricsSnapshot;
        let pc = PercoreMetricsSnapshot {
            inflight: vec![0, 0],
            dropped: vec![0, 0],
            accepted: vec![0, 0],
            codel_dropped: vec![0, 0],
            drophead_dropped: vec![7, 0],
            mailbox_dropped: vec![0, 0],
            inflight_total: 0,
            budget_factor: 1.0,
            worker_budget_bytes: 1,
        };
        let s = MetricsSnapshot {
            apps: HashMap::new(),
            saturation: None,
            percore: Some(pc),
            webhook: None,
            webhook_queue_depth: None,
            cluster: None,
        };
        let text = encode(&s);
        assert!(
            text.contains("pylon_drophead_dropped_total{worker=\"0\"} 7\n"),
            "drophead_dropped w0: {text}"
        );
        assert!(
            text.contains("pylon_drophead_dropped_total{worker=\"1\"} 0\n"),
            "drophead_dropped w1 must be present at 0 (quiescent convention, like codel): {text}"
        );
        assert!(
            text.contains("# HELP pylon_drophead_dropped_total "),
            "missing HELP drophead: {text}"
        );
        assert!(
            text.contains("# TYPE pylon_drophead_dropped_total counter"),
            "type counter drophead: {text}"
        );
        // HELP/TYPE precede the series (format invariant).
        let help_pos = text.find("# HELP pylon_drophead_dropped_total").unwrap();
        let type_pos = text.find("# TYPE pylon_drophead_dropped_total").unwrap();
        let series_pos = text.find("pylon_drophead_dropped_total{").unwrap();
        assert!(help_pos < type_pos && type_pos < series_pos);
    }

    #[test]
    fn encode_webhook_metrics_present_when_some() {
        use crate::webhook::WebhookMetrics;
        use std::sync::atomic::Ordering;
        let wm = Arc::new(WebhookMetrics::new(100));
        wm.enqueued.store(10, Ordering::Relaxed);
        wm.dropped.store(2, Ordering::Relaxed);
        wm.delivered_ok.store(7, Ordering::Relaxed);
        wm.delivered_failed.store(1, Ordering::Relaxed);
        let s = MetricsSnapshot {
            apps: HashMap::new(),
            saturation: None,
            percore: None,
            webhook: Some(wm),
            webhook_queue_depth: Some(3),
            cluster: None,
        };
        let text = encode(&s);
        assert!(
            text.contains("pylon_webhook_enqueued_total 10\n"),
            "enqueued: {text}"
        );
        assert!(
            text.contains("pylon_webhook_dropped_total 2\n"),
            "dropped: {text}"
        );
        assert!(
            text.contains("pylon_webhook_delivered_total{status=\"ok\"} 7\n"),
            "ok: {text}"
        );
        assert!(
            text.contains("pylon_webhook_delivered_total{status=\"failed\"} 1\n"),
            "failed: {text}"
        );
        assert!(
            text.contains("pylon_webhook_queue_depth 3\n"),
            "queue_depth: {text}"
        );
        assert!(
            text.contains("# TYPE pylon_webhook_enqueued_total counter"),
            "type enqueued: {text}"
        );
        assert!(
            text.contains("# TYPE pylon_webhook_delivered_total counter"),
            "type delivered: {text}"
        );
    }

    #[test]
    fn encode_webhook_metrics_absent_when_none() {
        let s = MetricsSnapshot {
            apps: HashMap::new(),
            saturation: None,
            percore: None,
            webhook: None,
            webhook_queue_depth: None,
            cluster: None,
        };
        let text = encode(&s);
        assert!(
            !text.contains("pylon_webhook_enqueued_total"),
            "webhook must be absent: {text}"
        );
        assert!(
            !text.contains("pylon_webhook_queue_depth"),
            "queue_depth must be absent: {text}"
        );
    }

    #[test]
    fn encode_cluster_metrics_present_when_some() {
        use crate::cluster::bridge::ClusterMetrics;
        use std::sync::atomic::Ordering;
        let cm = Arc::new(ClusterMetrics::new());
        cm.cmd_dropped.store(5, Ordering::Relaxed);
        cm.redis_connected.store(true, Ordering::Relaxed);
        let s = MetricsSnapshot {
            apps: HashMap::new(),
            saturation: None,
            percore: None,
            webhook: None,
            webhook_queue_depth: None,
            cluster: Some(cm),
        };
        let text = encode(&s);
        assert!(
            text.contains("pylon_cluster_cmd_dropped_total 5\n"),
            "cmd_dropped: {text}"
        );
        assert!(
            text.contains("pylon_redis_connected 1\n"),
            "redis_connected 1: {text}"
        );
        assert!(
            text.contains("# TYPE pylon_cluster_cmd_dropped_total counter"),
            "type counter cluster: {text}"
        );
        assert!(
            text.contains("# TYPE pylon_redis_connected gauge"),
            "type gauge redis: {text}"
        );
    }

    #[test]
    fn encode_cluster_metrics_absent_when_none() {
        let s = MetricsSnapshot {
            apps: HashMap::new(),
            saturation: None,
            percore: None,
            webhook: None,
            webhook_queue_depth: None,
            cluster: None,
        };
        let text = encode(&s);
        assert!(
            !text.contains("pylon_cluster_cmd_dropped_total"),
            "cluster must be absent: {text}"
        );
        assert!(
            !text.contains("pylon_redis_connected"),
            "redis_connected must be absent: {text}"
        );
    }

    // ── S1: optional bearer-token gate on /metrics ────────────────────────────

    /// Minimal `AppState` driving the handler directly, with an optional
    /// `metrics_token` (mirrors admin.rs's `test_state` harness).
    fn gate_state(token: Option<&str>) -> AppState {
        let config = crate::server::config::ServerConfig {
            metrics_token: token.map(|t| t.to_string()),
            ..crate::server::config::ServerConfig::default()
        };
        AppState {
            config,
            apps: Arc::new(crate::app::static_file::StaticFileAppManager::from_json("[]").unwrap()),
            adapter: Arc::new(crate::adapter::local::LocalAdapter::new(
                Arc::new(crate::channel::registry::Registry::new()),
                Arc::new(crate::adapter::app_registry::AppRegistry::new()),
            )),
            conn_counts: Arc::new(Default::default()),
            webhooks: crate::webhook::WebhookHandle::null(),
            saturated: None,
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            cluster_metrics: None,
            invalidator: None,
        }
    }

    fn bearer(token: &str) -> axum::http::HeaderMap {
        let mut h = axum::http::HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        h
    }

    /// Token unset → metrics open (back-compat: the gate must be inert).
    #[tokio::test]
    async fn token_unset_metrics_open() {
        let resp = get_metrics(State(gate_state(None)), axum::http::HeaderMap::new())
            .await
            .into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    /// Token set + no Authorization header → 404 (not 401: no existence disclosure).
    #[tokio::test]
    async fn token_set_missing_header_is_404_not_401() {
        let resp = get_metrics(
            State(gate_state(Some("sekrit"))),
            axum::http::HeaderMap::new(),
        )
        .await
        .into_response();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::NOT_FOUND,
            "missing token must 404, not 401"
        );
    }

    /// Token set + wrong token (same length) → 404.
    #[tokio::test]
    async fn token_set_wrong_token_is_404() {
        let resp = get_metrics(
            State(gate_state(Some("sekret"))),
            bearer("sekrit"), // same length, differs in the last two bytes
        )
        .await
        .into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::NOT_FOUND);
    }

    /// Token set + correct token → 200 with the real metrics body.
    #[tokio::test]
    async fn token_set_correct_token_is_200_with_body() {
        let resp = get_metrics(State(gate_state(Some("sekrit"))), bearer("sekrit"))
            .await
            .into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("pylon_up 1"), "body: {text}");
    }

    /// A non-Bearer Authorization scheme is treated as no token at all.
    #[tokio::test]
    async fn token_set_basic_scheme_is_404() {
        let mut h = axum::http::HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            "Basic c2Vrcml0OnNla3JpdA==".parse().unwrap(),
        );
        let resp = get_metrics(State(gate_state(Some("sekrit"))), h)
            .await
            .into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::NOT_FOUND);
    }

    // ── S1: the constant-time token compare (pure function) ───────────────────

    #[test]
    fn token_matches_accepts_identical() {
        assert!(token_matches("sekrit", "sekrit"));
        assert!(token_matches("", ""));
    }

    #[test]
    fn token_matches_rejects_same_length_difference() {
        assert!(!token_matches(
            "sekrit",
            "sekrit".replace('t', "x").as_str()
        ));
        // Differ only in the FIRST byte (an early-exit compare would return
        // fastest here; the fold must behave identically for every position).
        assert!(!token_matches("aekrit", "sekrit"));
        // Differ only in the LAST byte.
        assert!(!token_matches("sekris", "sekrit"));
    }

    #[test]
    fn token_matches_rejects_length_mismatch_both_ways() {
        assert!(!token_matches("", "sekrit"));
        assert!(!token_matches("sekrit", ""));
        assert!(!token_matches("sekrit-with-a-much-longer-length", "sekrit"));
        assert!(!token_matches("s", "sekrit"));
    }
}
