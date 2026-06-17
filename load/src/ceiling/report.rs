//! Ceiling report: combine ConnCeiling + TputCeiling into an Envelope,
//! format human-readable and JSON output, and compute a sizing Recommendation.

use super::conn_ramp::ConnCeiling;
use super::rate_ramp::TputCeiling;

// ── Public types ─────────────────────────────────────────────────────────────

/// Snapshot of the box spec + measured ceilings.
pub struct Envelope {
    pub logical_cores: usize,
    pub physical_cores: usize,
    pub total_ram_bytes: u64,
    pub kernel: String,
    pub conn: Option<ConnCeiling>,
    pub tput: Option<TputCeiling>,
}

/// Sizing recommendation for a target workload.
pub struct Recommendation {
    pub ram_bytes: u64,
    pub cores: u64,
    pub binding: &'static str,
}

// ── recommend ────────────────────────────────────────────────────────────────

/// Compute RAM + core sizing for `target_conns` connections and
/// `target_rate` msgs/s, given empirical ceilings from this box.
///
/// `box_ram_bytes`: total physical RAM of the measured box.
pub fn recommend(
    conn: &ConnCeiling,
    tput: &TputCeiling,
    physical_cores: usize,
    box_ram_bytes: u64,
    target_conns: u64,
    target_rate: u64,
) -> Recommendation {
    // RAM: target_conns × bytes_per_conn × 1.3 safety factor (integer: ×13/10).
    let ram_bytes = target_conns
        .saturating_mul(conn.bytes_per_conn)
        .saturating_mul(13)
        / 10;

    // Throughput capacity per core on the measured box.
    let per_core = tput.best.delivered_per_s / (physical_cores.max(1) as u64);

    // Cores: ceil(target_rate × 1.5 / per_core), min 1.
    let need = target_rate.saturating_mul(15) / 10;
    let cores = ((need + per_core.max(1) - 1) / per_core.max(1)).max(1);

    // Compare RAM pressure per core vs what the box provides per core.
    let box_ram_per_core = box_ram_bytes / (physical_cores.max(1) as u64);
    let ram_per_core_needed = ram_bytes / cores;

    let binding = if ram_per_core_needed > box_ram_per_core { "memory" } else { "cpu" };

    Recommendation { ram_bytes, cores, binding }
}

// ── human ────────────────────────────────────────────────────────────────────

fn fmt_thousands(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

fn bytes_to_gb(b: u64) -> f64 {
    b as f64 / (1u64 << 30) as f64
}

/// Format a human-readable multi-line report.
pub fn human(env: &Envelope, rec: Option<&Recommendation>) -> String {
    let mut lines: Vec<String> = Vec::new();

    lines.push(format!(
        "=== pylon-ceiling report ===\nBox:  {} logical / {} physical cores  |  RAM {:.1} GB  |  kernel {}",
        env.logical_cores,
        env.physical_cores,
        bytes_to_gb(env.total_ram_bytes),
        env.kernel,
    ));

    if let Some(c) = &env.conn {
        lines.push(format!(
            "Connections:  max {}  |  RSS {:.2} GB  |  {} bytes/conn  |  {} conns/GB  |  stop: {:?}",
            fmt_thousands(c.max_conns),
            bytes_to_gb(c.rss_bytes_at_max),
            fmt_thousands(c.bytes_per_conn),
            fmt_thousands(c.conns_per_gb),
            c.stop_reason,
        ));
    }

    if let Some(t) = &env.tput {
        let b = &t.best;
        let per_core = if env.physical_cores > 0 {
            b.delivered_per_s / env.physical_cores as u64
        } else {
            b.delivered_per_s
        };
        lines.push(format!(
            "Throughput:   best {} msg/s  |  p50 {} ms  |  p99 {} ms  |  CPU {:.1}%  |  drop {:.2}%  |  {} msg/s/core  |  stop: {:?}",
            fmt_thousands(b.delivered_per_s),
            b.p50_ms,
            b.p99_ms,
            b.cpu_busy_pct,
            b.drop_pct,
            fmt_thousands(per_core),
            t.stop_reason,
        ));
        if !b.per_core_busy.is_empty() {
            // Show the first physical_cores entries (or all if fewer).
            let show = if env.physical_cores > 0 {
                b.per_core_busy.len().min(env.physical_cores)
            } else {
                b.per_core_busy.len()
            };
            let cores_str: Vec<String> =
                b.per_core_busy[..show].iter().map(|v| format!("{}%", v.round() as u64)).collect();
            lines.push(format!("              per-core busy: [{}]", cores_str.join(" ")));
        }
    }

    if let Some(r) = rec {
        let binding_note = if r.binding == "memory" {
            "memory-bound — buy RAM"
        } else {
            "cpu-bound — buy cores"
        };
        lines.push(format!(
            "RECOMMEND:    RAM {:.1} GB  |  {} core(s)  |  {}",
            bytes_to_gb(r.ram_bytes),
            r.cores,
            binding_note,
        ));
    }

    lines.join("\n")
}

// ── json ─────────────────────────────────────────────────────────────────────

/// Emit the report as a JSON string.
pub fn json(env: &Envelope, rec: Option<&Recommendation>) -> String {
    let conn_val = match &env.conn {
        Some(c) => serde_json::json!({
            "max_conns": c.max_conns,
            "rss_bytes_at_max": c.rss_bytes_at_max,
            "bytes_per_conn": c.bytes_per_conn,
            "conns_per_gb": c.conns_per_gb,
            "stop_reason": format!("{:?}", c.stop_reason),
        }),
        None => serde_json::Value::Null,
    };

    let tput_val = match &env.tput {
        Some(t) => {
            let b = &t.best;
            let mut best_obj = serde_json::json!({
                "rate": b.rate,
                "delivered_per_s": b.delivered_per_s,
                "drop_pct": b.drop_pct,
                "p50_ms": b.p50_ms,
                "p99_ms": b.p99_ms,
                "cpu_busy_pct": b.cpu_busy_pct,
            });
            if !b.per_core_busy.is_empty() {
                best_obj["per_core_busy"] = serde_json::json!(b.per_core_busy);
            }
            serde_json::json!({
                "best": best_obj,
                "stop_reason": format!("{:?}", t.stop_reason),
            })
        }
        None => serde_json::Value::Null,
    };

    let rec_val = match rec {
        Some(r) => serde_json::json!({
            "ram_bytes": r.ram_bytes,
            "cores": r.cores,
            "binding": r.binding,
        }),
        None => serde_json::Value::Null,
    };

    serde_json::json!({
        "box": {
            "logical_cores": env.logical_cores,
            "physical_cores": env.physical_cores,
            "total_ram_bytes": env.total_ram_bytes,
            "kernel": env.kernel,
        },
        "conn_ceiling": conn_val,
        "tput_ceiling": tput_val,
        "recommendation": rec_val,
    })
    .to_string()
}

// ── unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::conn_ramp::StopReason;
    use super::super::rate_ramp::{TputStep, TputStop};

    #[test]
    fn memory_bound_recommends_ram_and_flags_binding() {
        let conn = ConnCeiling {
            max_conns: 1_000_000,
            rss_bytes_at_max: 6_144_000_000,
            bytes_per_conn: 6144,
            conns_per_gb: 174762,
            stop_reason: StopReason::MemCeiling,
        };
        let best = TputStep {
            rate: 1000,
            delivered_per_s: 500_000,
            drop_pct: 0.0,
            p50_ms: 5,
            p99_ms: 40,
            cpu_busy_pct: 90.0,
            per_core_busy: vec![],
        };
        let tput = TputCeiling {
            best: best.clone(),
            steps: vec![best],
            stop_reason: TputStop::CpuSaturated,
        };
        // box ~4 GB/core; target needs ~8 GB/core → memory-bound
        let r = recommend(&conn, &tput, 1, 4_294_967_296, 1_000_000, 300_000);
        assert!(r.ram_bytes > 7_000_000_000 && r.ram_bytes < 9_000_000_000); // ~8 GB
        assert_eq!(r.cores, 1); // ceil(300k*1.5/500k)=ceil(0.9)=1
        assert_eq!(r.binding, "memory");
    }

    #[test]
    fn cpu_bound_flags_cpu() {
        let conn = ConnCeiling {
            max_conns: 1_000_000,
            rss_bytes_at_max: 1_000_000_000,
            bytes_per_conn: 1000,
            conns_per_gb: 1_073_741,
            stop_reason: StopReason::MemCeiling,
        };
        let best = TputStep {
            rate: 1000,
            delivered_per_s: 100_000,
            drop_pct: 0.0,
            p50_ms: 5,
            p99_ms: 40,
            cpu_busy_pct: 95.0,
            per_core_busy: vec![],
        };
        let tput = TputCeiling {
            best: best.clone(),
            steps: vec![best],
            stop_reason: TputStop::CpuSaturated,
        };
        // tiny bytes/conn + 16 GB/core → cpu-bound
        let r = recommend(&conn, &tput, 1, 17_179_869_184, 1_000_000, 1_000_000);
        assert_eq!(r.binding, "cpu");
    }
}
