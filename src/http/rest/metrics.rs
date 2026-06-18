//! GET /metrics — Prometheus text exposition format v0.0.4.

use crate::server::router::AppState;
use crate::transport::percore_metrics_snapshot;
use axum::extract::State;
use axum::http::{HeaderValue, header};
use axum::response::IntoResponse;
use std::collections::HashMap;
use std::fmt::Write;

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
        out.push_str("# HELP pylon_saturation_flag Broadcast pipeline saturation (1 = saturated)\n");
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
            let _ = writeln!(out, "pylon_connections{{app=\"{}\"}} {}", escape_label(app_id), m.connections);
        }

        out.push_str("# HELP pylon_channels_occupied Number of occupied channels per app\n");
        out.push_str("# TYPE pylon_channels_occupied gauge\n");
        for app_id in &app_ids {
            let m = &snapshot.apps[*app_id];
            let _ = writeln!(out, "pylon_channels_occupied{{app=\"{}\"}} {}", escape_label(app_id), m.channels_occupied);
        }

        out.push_str("# HELP pylon_subscriptions Total channel subscriptions per app\n");
        out.push_str("# TYPE pylon_subscriptions gauge\n");
        for app_id in &app_ids {
            let m = &snapshot.apps[*app_id];
            let _ = writeln!(out, "pylon_subscriptions{{app=\"{}\"}} {}", escape_label(app_id), m.subscriptions);
        }
    }

    // Per-core worker metrics (omit entirely if no percore fleet ran)
    if let Some(ref pc) = snapshot.percore {
        out.push_str("# HELP pylon_broadcast_dropped_total Broadcasts dropped due to full worker hand-off channel (cumulative)\n");
        out.push_str("# TYPE pylon_broadcast_dropped_total counter\n");
        for (i, &dropped) in pc.dropped.iter().enumerate() {
            let _ = writeln!(out, "pylon_broadcast_dropped_total{{worker=\"{i}\"}} {dropped}");
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
    }

    out
}

pub async fn get_metrics(State(state): State<AppState>) -> impl IntoResponse {
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
        apps.insert(app_id, AppMetrics { connections, channels_occupied, subscriptions });
    }

    let saturation = state.saturated.as_ref().map(|s| s.load(Ordering::Relaxed));
    let percore = percore_metrics_snapshot();

    let body = encode(&MetricsSnapshot { apps, saturation, percore });

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

    fn snapshot_with_one_app(app_id: &str, conns: u64, channels: u64, subs: u64) -> MetricsSnapshot {
        let mut apps = HashMap::new();
        apps.insert(app_id.to_string(), AppMetrics {
            connections: conns,
            channels_occupied: channels,
            subscriptions: subs,
        });
        MetricsSnapshot { apps, saturation: None, percore: None }
    }

    #[test]
    fn encode_pylon_up_always_present() {
        let s = snapshot_with_one_app("app1", 0, 0, 0);
        let text = encode(&s);
        assert!(text.contains("# HELP pylon_up "), "missing HELP pylon_up");
        assert!(text.contains("# TYPE pylon_up gauge"), "missing TYPE pylon_up");
        assert!(text.contains("pylon_up 1\n"), "missing pylon_up 1");
    }

    #[test]
    fn encode_per_app_metrics() {
        let s = snapshot_with_one_app("myapp", 42, 7, 15);
        let text = encode(&s);
        assert!(text.contains("pylon_connections{app=\"myapp\"} 42\n"), "connections missing: {text}");
        assert!(text.contains("pylon_channels_occupied{app=\"myapp\"} 7\n"), "channels missing: {text}");
        assert!(text.contains("pylon_subscriptions{app=\"myapp\"} 15\n"), "subscriptions missing: {text}");
        assert!(text.contains("# HELP pylon_connections "), "missing HELP connections");
        assert!(text.contains("# TYPE pylon_connections gauge"), "missing TYPE connections");
    }

    #[test]
    fn encode_saturation_flag_present_when_some() {
        let s = MetricsSnapshot {
            apps: HashMap::new(),
            saturation: Some(true),
            percore: None,
        };
        let text = encode(&s);
        assert!(text.contains("pylon_saturation_flag 1\n"), "saturation flag missing: {text}");

        let s2 = MetricsSnapshot {
            apps: HashMap::new(),
            saturation: Some(false),
            percore: None,
        };
        let text2 = encode(&s2);
        assert!(text2.contains("pylon_saturation_flag 0\n"), "saturation=0 missing: {text2}");
    }

    #[test]
    fn encode_saturation_flag_omitted_when_none() {
        let s = MetricsSnapshot {
            apps: HashMap::new(),
            saturation: None,
            percore: None,
        };
        let text = encode(&s);
        assert!(!text.contains("pylon_saturation_flag"), "saturation flag must be absent: {text}");
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
            inflight_total: 300,
            budget_factor: 0.9,
            worker_budget_bytes: 1024 * 1024 * 512,
        };
        let s = MetricsSnapshot {
            apps: HashMap::new(),
            saturation: None,
            percore: Some(pc),
        };
        let text = encode(&s);
        assert!(text.contains("pylon_inflight_bytes{worker=\"0\"} 100\n"), "worker 0 inflight: {text}");
        assert!(text.contains("pylon_inflight_bytes{worker=\"1\"} 200\n"), "worker 1 inflight: {text}");
        assert!(text.contains("pylon_broadcast_dropped_total{worker=\"0\"} 5\n"), "worker 0 dropped: {text}");
        assert!(text.contains("pylon_broadcast_dropped_total{worker=\"1\"} 3\n"), "worker 1 dropped: {text}");
        assert!(text.contains("pylon_inflight_bytes_sum 300\n"), "inflight_total: {text}");
        assert!(text.contains("pylon_budget_factor 0.900\n"), "budget_factor: {text}");
        assert!(text.contains(&format!("pylon_worker_budget_bytes {}\n", 1024 * 1024 * 512)), "budget_bytes: {text}");
        assert!(text.contains("# TYPE pylon_broadcast_dropped_total counter"), "type counter: {text}");
        assert!(text.contains("# TYPE pylon_inflight_bytes gauge"), "type gauge: {text}");
    }

    #[test]
    fn encode_percore_metrics_absent_when_none() {
        let s = MetricsSnapshot {
            apps: HashMap::new(),
            saturation: None,
            percore: None,
        };
        let text = encode(&s);
        assert!(!text.contains("pylon_inflight_bytes"), "percore must be absent: {text}");
        assert!(!text.contains("pylon_broadcast_dropped_total"), "percore must be absent: {text}");
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
}
