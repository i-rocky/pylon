#![deny(unsafe_code)]
//! pylon-ceiling: empirical capacity-envelope finder.
//!
//! Spawns a pylon child process (core-pinned), sweeps connections and/or
//! publish rate to their ceilings, then prints a human or JSON report.
//!
//! # Ctrl-C / signal note
//! The ctrl-c handler kills the child's process group before exiting, ensuring
//! proper teardown even though `std::process::exit(130)` bypasses Rust's drop glue.
//! The PylonChild Drop guard won't run on Ctrl-C, so we explicitly kill the group in
//! the signal handler.

use anyhow::Result;
use clap::{Parser, ValueEnum};

use pylon_load::ceiling::{
    child::{default_pylon_bin, AppsFile, ChildOpts, PylonChild},
    conn_ramp::{self, ConnRampOpts},
    rate_ramp::{self, RateRampOpts},
    report::{human, json, recommend, Envelope},
    spec,
};

// ── Phase selector ─────────────────────────────────────────────────────────

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Conn,
    Throughput,
    Both,
}

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "pylon-ceiling",
    about = "Empirical capacity-envelope finder for pylon.\n\
             Sweeps connections and/or publish rate to their ceilings, then \
             prints a sizing report."
)]
struct Args {
    /// Which phase(s) to run: conn | throughput | both
    #[arg(long, value_enum, default_value_t = Phase::Both)]
    phase: Phase,

    /// Path to the pylon binary (default: auto-detect next to this binary)
    #[arg(long, default_value_t = default_pylon_bin())]
    pylon_bin: String,

    /// Number of server worker threads (default: logical_cores / 2, min 1)
    #[arg(long, default_value_t = 0)]
    server_cores: usize,

    /// Number of tokio worker threads for the server child (default: = server-cores)
    #[arg(long, default_value_t = 0)]
    workers: usize,

    /// Path to the apps JSON file (default: write a temp file)
    #[arg(long, default_value = "")]
    apps_path: String,

    /// Port for the pylon child to listen on
    #[arg(long, default_value_t = 7000)]
    port: u16,

    /// Hard connection limit (0 = unlimited, stop on memory)
    #[arg(long, default_value_t = 0)]
    max_conns: usize,

    /// Stop the connection ramp when server RSS exceeds this % of total RAM
    #[arg(long, default_value_t = 80)]
    mem_ceiling_pct: u8,

    /// Number of connections to add per ramp batch
    #[arg(long, default_value_t = 20000)]
    conn_batch: usize,

    /// Comma-separated list of client source IPs (spreads ephemeral port space)
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5,127.0.0.6,127.0.0.7,127.0.0.8"
    )]
    client_ips: Vec<String>,

    /// Subscriber connections for the throughput phase (0 = auto)
    #[arg(long, default_value_t = 0)]
    tput_conns: usize,

    /// Number of channels for the throughput phase
    #[arg(long, default_value_t = 200)]
    channels: usize,

    /// Starting publish rate (msgs/s) for the throughput ramp
    #[arg(long, default_value_t = 200)]
    rate_start: u64,

    /// Rate increment per step (msgs/s)
    #[arg(long, default_value_t = 200)]
    rate_step: u64,

    /// Maximum publish rate to attempt (msgs/s)
    #[arg(long, default_value_t = 5000)]
    max_rate: u64,

    /// p99 latency budget in milliseconds; exceeding this stops the ramp
    #[arg(long, default_value_t = 100)]
    p99_budget_ms: u64,

    /// Max in-flight publish requests (open-loop back-pressure)
    #[arg(long, default_value_t = 256)]
    max_inflight: usize,

    /// Duration of each rate-ramp step in seconds
    #[arg(long, default_value_t = 15)]
    step_secs: u64,

    /// Target connection count for the sizing recommendation (optional)
    #[arg(long)]
    target_conns: Option<u64>,

    /// Target publish rate (msgs/s) for the sizing recommendation (optional)
    #[arg(long)]
    target_rate: Option<u64>,

    /// Emit JSON output instead of human-readable text
    #[arg(long, default_value_t = false)]
    json: bool,
}

// ── entry point ─────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // Initialise tracing so child-module tracing::info! calls appear.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let mut args = Args::parse();

    // 1. Detect box.
    let spec = spec::detect();

    // 2. Resolve server_cores default (logical_cores / 2, min 1).
    if args.server_cores == 0 {
        args.server_cores = (spec.logical_cores / 2).max(1);
    }
    // workers defaults to server_cores.
    if args.workers == 0 {
        args.workers = args.server_cores;
    }

    let server_cores = args.server_cores;

    // Build taskset CPU list: e.g. server_cores=4 → "0-3"; server_cores=1 → "0-0".
    let cores_list = format!("0-{}", server_cores.saturating_sub(1));

    // 3. Resolve apps path.
    let apps_file = if args.apps_path.is_empty() {
        AppsFile::create_temp()?
    } else {
        AppsFile::use_existing(&args.apps_path)?
    };

    // 4. Spawn pylon child.
    let child_opts = ChildOpts {
        pylon_bin: args.pylon_bin.clone(),
        port: args.port,
        workers: args.workers,
        cores: cores_list,
        apps_path: apps_file.path().to_owned(),
    };
    let child = PylonChild::spawn(&child_opts).await?;

    // 5. Build URLs.
    let url = format!(
        "ws://127.0.0.1:{}/app/{}",
        args.port,
        apps_file.credentials().key
    );
    let rest = format!("http://127.0.0.1:{}", args.port);

    // 6. Install Ctrl-C handler; kill the child's process group before exiting.
    let pgid = child.pid();
    let temp_apps = apps_file.owned_path().map(str::to_owned);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("interrupted — tearing down pylon child");
        // Kill the child's process group (negative pid). Drop won't run under process::exit,
        // so do the teardown explicitly here.
        let _ = std::process::Command::new("kill")
            .args(["-TERM", &format!("-{pgid}")])
            .status();
        if let Some(path) = &temp_apps {
            AppsFile::remove_temp(path);
        }
        std::process::exit(130);
    });

    // 7. Phase storage.
    let mut conn: Option<pylon_load::ceiling::conn_ramp::ConnCeiling> = None;
    let mut tput: Option<pylon_load::ceiling::rate_ramp::TputCeiling> = None;

    // 8. Connection-ceiling phase.
    if args.phase == Phase::Conn || args.phase == Phase::Both {
        let opts = ConnRampOpts {
            url: url.clone(),
            key: apps_file.credentials().key.clone(),
            secret: apps_file.credentials().secret.clone(),
            conn_batch: args.conn_batch,
            max_conns: args.max_conns,
            mem_ceiling_pct: args.mem_ceiling_pct,
            client_ips: args.client_ips.clone(),
            fail_threshold: 200,
        };
        conn = Some(conn_ramp::run(&child, &spec, &opts).await);
    }

    // 9. Throughput-ceiling phase.
    if args.phase == Phase::Throughput || args.phase == Phase::Both {
        let tput_conns = if args.tput_conns > 0 {
            args.tput_conns
        } else {
            conn.as_ref()
                .map(|c| (c.max_conns as usize).min(50_000))
                .unwrap_or(10_000)
        };

        let opts = RateRampOpts {
            url: url.clone(),
            app_id: apps_file.app_id().to_owned(),
            rest: rest.clone(),
            key: apps_file.credentials().key.clone(),
            secret: apps_file.credentials().secret.clone(),
            tput_conns,
            channels: args.channels,
            rate_start: args.rate_start,
            rate_step: args.rate_step,
            max_rate: args.max_rate,
            p99_budget_ms: args.p99_budget_ms,
            max_inflight: args.max_inflight,
            step_secs: args.step_secs,
            server_cores,
            client_ips: args.client_ips.clone(),
        };
        tput = Some(rate_ramp::run(&child, &opts).await);
    }

    // 10. Build envelope.
    let env = Envelope {
        logical_cores: spec.logical_cores,
        physical_cores: spec.physical_cores,
        total_ram_bytes: spec.total_ram_bytes,
        kernel: spec.kernel.clone(),
        conn,
        tput,
    };

    // 11. Compute recommendation iff both targets supplied and both phases ran.
    let rec = match (
        args.target_conns,
        args.target_rate,
        env.conn.as_ref(),
        env.tput.as_ref(),
    ) {
        (Some(tc), Some(tr), Some(c), Some(t)) => Some(recommend(
            c,
            t,
            spec.physical_cores,
            spec.total_ram_bytes,
            tc,
            tr,
        )),
        _ => None,
    };

    // 12. Print report.
    if args.json {
        println!("{}", json(&env, rec.as_ref()));
    } else {
        println!("{}", human(&env, rec.as_ref()));
    }

    // 13. Return; `child` drops here → PylonChild::drop() sends SIGTERM to the group.
    Ok(())
}
