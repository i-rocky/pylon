//! The conformance harness entrypoint: parse the command line, then either
//! list the catalog, audit the binding table, or orchestrate a whole run.
//!
//! A run wires the landed pieces together (spec §6 flow):
//!
//! 1. resolve the pylon binary (`--pylon-bin`, else `<repo>/target/release/pylon`)
//! 2. bind the plumbing — auth endpoint on `port_base + 0`, webhook receiver
//!    on `port_base + 1` — and spawn pylon itself on `port_base + 2`
//! 3. write ONE `AdapterEnv` for the whole run (same server, same app) to a
//!    scratch `env.json`, and hand its path to both the scenario runners and
//!    the signing-mode [`SignerFn`]
//! 4. select scenarios (catalog order; `--sdk` / `--scenario` / `--smoke`)
//! 5. per scenario, `adapter::run` under the catalog budget — AWAITED
//!    DIRECTLY, never wrapped in an outer timeout: budgets are enforced
//!    inside `run` via a process-group kill, and cancelling its future would
//!    orphan the group (cancellation never signals anything)
//! 6. shut pylon down, render + write the report, print the human matrix,
//!    exit with `report::exit_code`

mod adapter;
mod args;
mod catalog;
mod plumbing;
mod report;
mod server;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use args::Command;

use crate::adapter::AdapterEnv;
use crate::catalog::Scenario;
use crate::plumbing::SignerFn;
use crate::server::AppSpec;

/// How long the (outer, hard-stop) `tokio::time::timeout` around
/// `PylonServer::wait_ready` waits — the same duration is passed in, so the
/// wrapper is the deadline: `wait_ready`'s internal poll has no per-attempt
/// cap.
const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// The `--smoke` subset: one client-plane liveness scenario plus the two
/// server-plane scenarios that witness pylon's HTTP and webhook planes.
const SMOKE_IDS: [&str; 3] = ["C-ESTABLISH", "S-TRIGGER", "S-WEBHOOK-VERIFY"];

/// Wall-clock bound on one `--sign` child. Signing is pure crypto (no server
/// I/O), so a healthy runner finishes in well under a second; a hung one must
/// not pin the auth endpoint forever. Cancelling the `wait_with_output` future
/// drops the child, and `kill_on_drop` (set below) kills it — so the timeout
/// cannot orphan the process.
const SIGNER_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::main]
async fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let refs: Vec<&str> = argv.iter().map(String::as_str).collect();
    let code = match args::parse(&refs) {
        Ok(parsed) => match parsed.command {
            Command::List => {
                list_scenarios();
                0
            }
            Command::Audit => audit_bindings(),
            Command::Run {
                sdk,
                scenario,
                smoke,
                report,
                port_base,
                pylon_bin,
            } => {
                match run_scenarios(&sdk, &scenario, smoke, &report, port_base, &pylon_bin).await {
                    Ok(code) => code,
                    Err(e) => {
                        eprintln!("error: {e:#}");
                        2
                    }
                }
            }
        },
        Err(e) => {
            eprintln!("error: {e}\n\n{}", args::USAGE);
            2
        }
    };
    std::process::exit(code);
}

/// `--list`: one row per catalog scenario, in catalog order.
fn list_scenarios() {
    for s in catalog::CATALOG {
        println!(
            "{:<17} {:<17} {:<6} {:>6}ms  {}",
            s.id,
            s.sdk,
            match s.plane {
                catalog::Plane::Client => "client",
                catalog::Plane::Server => "server",
            },
            s.budget_ms,
            s.summary,
        );
    }
}

/// `--audit`: the binding table's honesty gate.
///
/// Two problem families, all printed, any of which exits nonzero:
///
/// - per binding, `adapters/<sdk>/runner.js` must EXIST on disk — with no
///   runners committed yet this fails for all 26 bindings, which is the
///   correct scaffolding state (it is what keeps `--audit` from lying green
///   before Tasks 7-9 land the runners). The deeper check — that each runner
///   actually DECLARES the bound id in its `--list` output — is wired by
///   Tasks 7/8 together with the runner `--list` mode itself.
/// - `catalog::audit` cross-checks the table against the catalog (missing /
///   orphaned / wrong-sdk / duplicate bindings).
fn audit_bindings() -> i32 {
    let impls = catalog_impls();
    let mut problems = Vec::new();
    for (sdk, id) in &impls {
        let runner = adapters_dir().join(sdk).join("runner.js");
        if !runner.is_file() {
            problems.push(format!(
                "runner missing: {} (binding {})",
                runner.display(),
                id
            ));
        }
    }
    problems.extend(catalog::audit(&impls));

    if problems.is_empty() {
        println!("audit: OK — {} bindings, all runners present", impls.len());
        0
    } else {
        for p in &problems {
            eprintln!("audit: {p}");
        }
        eprintln!("audit: {} problem(s)", problems.len());
        1
    }
}

/// The binding table: all 26 `(sdk, scenario id)` pairs the harness claims
/// are implemented by an adapter's `runner.js` (spec §5, one implementation
/// per scenario, exactly one SDK each). `--audit` holds this table — and the
/// adapter directories — to that claim.
fn catalog_impls() -> Vec<(String, String)> {
    catalog::CATALOG
        .iter()
        .map(|s| (s.sdk.to_string(), s.id.to_string()))
        .collect()
}

/// `run`: the whole orchestrated flow. Returns the process exit code (0/1);
/// harness-level failures bubble up as `Err` (exit 2 in `main`).
async fn run_scenarios(
    sdk: &Option<String>,
    scenario: &Option<String>,
    smoke: bool,
    report_path: &Path,
    port_base: u16,
    pylon_bin: &Option<PathBuf>,
) -> Result<i32> {
    let bin = resolve_pylon_bin(pylon_bin)?;
    let selected = select_scenarios(sdk.as_deref(), scenario.as_deref(), smoke)?;
    if adapter::which_node().is_none() {
        bail!("no usable `node` on PATH — adapters are node runners");
    }

    // One env for the whole run: every scenario talks to the same servers and
    // the same app, so one scratch file serves the runners and the signer.
    let scratch =
        std::env::temp_dir().join(format!("pylon-conformance-env-{}", std::process::id()));
    std::fs::create_dir_all(&scratch).with_context(|| format!("create {}", scratch.display()))?;
    let env_path = scratch.join("env.json");

    // Signing mode: the auth endpoint delegates ALL crypto to the official
    // pusher-http-node runner. Every auth hit spawns
    // `node runner.js --sign --env <env.json>` with the request body on
    // STDIN and answers with the runner's stdout. The runner's `--sign` mode
    // lands in Task 7 — until then this closure is fully wired but fails at
    // call time (a spawn error surfacing as the auth endpoint's 500), which
    // is the expected scaffolding state.
    let signer = signer_fn(&adapters_dir().join("pusher-http-node"), &env_path);

    let auth = plumbing::AuthServer::spawn(port_base, signer)
        .await
        .context("spawn auth endpoint")?;
    let hooks = plumbing::WebhookReceiver::spawn(port_base + 1)
        .await
        .context("spawn webhook receiver")?;

    let apps = vec![
        AppSpec::conformance_main(&format!("{}/hooks", hooks.base_url())),
        AppSpec::conformance_disabled(),
    ];
    let mut pylon = server::spawn_pylon(&bin.to_string_lossy(), port_base + 2, &apps)
        .await
        .context("spawn pylon")?;
    match tokio::time::timeout(READY_TIMEOUT, pylon.wait_ready(READY_TIMEOUT)).await {
        Ok(inner) => inner.context("pylon health check")?,
        Err(_) => bail!("pylon not healthy within {READY_TIMEOUT:?}"),
    }
    eprintln!(
        "harness up: auth on port {}, webhook receiver on port {}, pylon on port {}",
        auth.port(),
        hooks.port(),
        pylon.port
    );

    let main_app = &apps[0];
    let env = AdapterEnv {
        ws_url: format!("ws://127.0.0.1:{}/app/{}", pylon.port, main_app.key),
        http_host: "127.0.0.1".to_string(),
        http_port: pylon.port,
        app_id: main_app.id.clone(),
        app_key: main_app.key.clone(),
        app_secret: main_app.secret.clone(),
        auth_endpoint: format!("http://127.0.0.1:{}/auth", auth.port()),
        webhook_receiver: hooks.base_url(),
    };
    env.write_to(&env_path)?;

    let pylon_version = pylon_version(&bin).await;
    let mut results = Vec::new();
    for s in &selected {
        let adapter_dir = adapters_dir().join(s.sdk);
        if !adapter_dir.join("runner.js").is_file() {
            eprintln!(
                "note: {} has no runner.js yet — expecting a failure verdict",
                adapter_dir.display()
            );
        }
        eprintln!(
            "running {} ({}) with {}ms budget ...",
            s.id, s.sdk, s.budget_ms
        );
        // Direct await, deliberately NOT wrapped in an outer timeout: see the
        // module docs (budget enforcement is the process-group kill inside
        // `adapter::run`; cancelling this future would orphan the group).
        results.push(adapter::run(&adapter_dir, s.id, &env_path, s.budget_ms).await);
    }

    pylon.shutdown().await;
    let _ = std::fs::remove_dir_all(&scratch);
    eprintln!(
        "webhook receiver captured {} envelope(s) this run",
        hooks.envelopes().len()
    );

    // SDK versions come from each adapter's `--version` mode (Task 7); the
    // table is empty until those runners exist.
    let report = report::Report {
        run: report::RunMeta {
            timestamp: rfc3339_now(),
            pylon_version,
            sdk_versions: Vec::new(),
            target: "local".to_string(),
        },
        results,
    };
    report::write_json(&report, report_path)?;
    print!("{}", report::render_human(&report));
    Ok(report::exit_code(&report))
}

/// `--pylon-bin` if given, else `<repo>/target/release/pylon` where `<repo>`
/// is this crate's parent directory (resolved from `CARGO_MANIFEST_DIR`, so
/// the harness works from any CWD). A missing binary is a build hint, not a
/// mystery: point at `cargo build --release` in the repo root.
fn resolve_pylon_bin(pylon_bin: &Option<PathBuf>) -> Result<PathBuf> {
    let bin = pylon_bin
        .clone()
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/release/pylon"));
    if !bin.is_file() {
        bail!(
            "pylon binary not found at {} — build it first: `cargo build --release` in the repo root (or pass --pylon-bin)",
            bin.display()
        );
    }
    Ok(bin)
}

/// `conformance/adapters` — where `adapters/<sdk>/runner.js` lives.
fn adapters_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("adapters")
}

/// Filter the catalog into the run's scenario list, preserving catalog order.
///
/// `--smoke` intersects with the [`SMOKE_IDS`] subset; `--sdk` and
/// `--scenario` intersect with each other and with it. Unknown scenario ids
/// and unknown SDKs fail fast with pointers to `--list`; an empty result is
/// an error rather than a silent green run.
fn select_scenarios(
    sdk: Option<&str>,
    scenario: Option<&str>,
    smoke: bool,
) -> Result<Vec<&'static Scenario>> {
    if let Some(id) = scenario {
        if catalog::find(id).is_none() {
            bail!("unknown scenario {id:?} — see `--list` for the catalog");
        }
    }
    if let Some(sdk) = sdk {
        if !catalog::CATALOG.iter().any(|s| s.sdk == sdk) {
            let known: Vec<&str> = {
                let mut sdks: Vec<&str> = catalog::CATALOG.iter().map(|s| s.sdk).collect();
                sdks.sort_unstable();
                sdks.dedup();
                sdks
            };
            bail!("unknown sdk {sdk:?} — known sdks: {}", known.join(", "));
        }
    }

    let mut selected: Vec<&Scenario> = catalog::CATALOG.iter().collect();
    if smoke {
        selected.retain(|s| SMOKE_IDS.contains(&s.id));
    }
    if let Some(sdk) = sdk {
        selected.retain(|s| s.sdk == sdk);
    }
    if let Some(id) = scenario {
        selected.retain(|s| s.id == id);
    }
    if selected.is_empty() {
        bail!("no scenarios selected (--sdk/--scenario/--smoke filters exclude everything)");
    }
    Ok(selected)
}

/// Build the auth endpoint's [`SignerFn`]: spawn the official pusher-http-node
/// runner in `--sign` mode per auth hit, request body on STDIN, and return
/// its trimmed stdout as the auth response string. The child is bounded by
/// [`SIGNER_TIMEOUT`] — safe to cancel because `kill_on_drop` kills it.
///
/// The outer closure must stay `Fn` (the endpoint may answer concurrently),
/// so the captured paths are cloned into each invocation's future.
fn signer_fn(http_adapter_dir: &Path, env_path: &Path) -> SignerFn {
    let adapter_dir = http_adapter_dir.to_path_buf();
    let env_path = env_path.to_path_buf();
    Arc::new(move |body| {
        let adapter_dir = adapter_dir.clone();
        let env_path = env_path.clone();
        Box::pin(async move {
            use std::process::Stdio;
            use tokio::io::AsyncWriteExt as _;

            let mut child = tokio::process::Command::new("node")
                .arg("runner.js")
                .arg("--sign")
                .arg("--env")
                .arg(&env_path)
                .current_dir(&adapter_dir)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .kill_on_drop(true)
                .spawn()
                .with_context(|| {
                    format!("spawn `node runner.js --sign` in {}", adapter_dir.display())
                })?;

            // Request body in, then close the pipe: the runner reads STDIN to
            // EOF, answers on stdout, exits.
            if let Some(mut stdin) = child.stdin.take() {
                stdin.write_all(&body).await.context("write signer stdin")?;
                drop(stdin);
            }

            // Bounded wait: on timeout the wait future (which owns the child)
            // is dropped, and kill_on_drop above kills the process.
            let output = tokio::time::timeout(SIGNER_TIMEOUT, child.wait_with_output())
                .await
                .context("signer runner timed out")?
                .context("wait for signer runner")?;
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !output.status.success() {
                bail!(
                    "signer runner failed: {status}: {stdout}",
                    status = output.status
                );
            }
            Ok(stdout)
        })
    })
}

/// The pylon server's version: `<bin> --version`, trimmed stdout (e.g.
/// `pylon 0.3.0`). Any failure degrades to `"unknown"` — a version probe
/// must not kill a run.
async fn pylon_version(bin: &Path) -> String {
    match tokio::process::Command::new(bin)
        .arg("--version")
        .output()
        .await
    {
        Ok(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        }
        _ => "unknown".to_string(),
    }
}

/// RFC 3339 UTC timestamp of right now, e.g. `2026-09-01T12:34:56Z`.
///
/// No chrono: epoch seconds → civil date via Howard Hinnant's
/// `civil_from_days` (the standard days-from-epoch to y/m/d algorithm).
fn rfc3339_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (hour, min, sec) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);

    let days = (secs / 86_400) as i64;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 {
        yoe + era * 400 + 1
    } else {
        yoe + era * 400
    };

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::args::{self, Command};
    use crate::catalog;
    use crate::server::{self, render_apps_json, AppSpec};

    #[test]
    fn args_parse_run_list_audit_and_smoke_selects_three() {
        let a = args::parse(&["run", "--smoke"]).unwrap();
        match &a.command {
            Command::Run { smoke: true, .. } => {}
            _ => panic!(),
        }
        assert!(matches!(
            args::parse(&["--list"]).unwrap().command,
            Command::List
        ));
        assert!(matches!(
            args::parse(&["--audit"]).unwrap().command,
            Command::Audit
        ));
    }

    #[test]
    fn smoke_selects_exactly_three_in_catalog_order() {
        let selected =
            super::select_scenarios(None, None, true).expect("smoke subset always selects");
        let ids: Vec<&str> = selected.iter().map(|s| s.id).collect();
        assert_eq!(ids, ["C-ESTABLISH", "S-TRIGGER", "S-WEBHOOK-VERIFY"]);
    }

    #[test]
    fn selection_filters_and_validates() {
        let all = super::select_scenarios(None, None, false).unwrap();
        assert_eq!(all.len(), 26, "no filters means the whole catalog");

        let server_only = super::select_scenarios(Some("pusher-http-node"), None, false).unwrap();
        assert_eq!(server_only.len(), 8);
        assert!(server_only.iter().all(|s| s.sdk == "pusher-http-node"));

        let one = super::select_scenarios(None, Some("C-PING"), false).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].id, "C-PING");

        assert!(super::select_scenarios(None, Some("C-NOPE"), false).is_err());
        assert!(super::select_scenarios(Some("pusher-swift"), None, false).is_err());
        assert!(super::select_scenarios(None, Some("C-PING"), true)
            .unwrap_err()
            .to_string()
            .contains("no scenarios selected"));
    }

    #[test]
    fn binding_table_covers_the_whole_catalog() {
        let impls = super::catalog_impls();
        assert_eq!(impls.len(), 26);
        assert!(
            catalog::audit(&impls).is_empty(),
            "the hardcoded binding table must satisfy catalog::audit"
        );
    }

    #[test]
    fn rfc3339_timestamp_has_the_right_shape() {
        let ts = super::rfc3339_now();
        // e.g. 2026-09-01T12:34:56Z — fixed-width fields, Z suffix.
        assert_eq!(ts.len(), 20);
        assert_eq!(&ts[4..5], "-");
        assert_eq!(&ts[10..11], "T");
        assert_eq!(&ts[19..], "Z");
        assert!(ts.starts_with("20"));
    }

    #[test]
    fn apps_json_renders_both_apps_with_webhook() {
        let apps = vec![
            AppSpec::conformance_main("http://127.0.0.1:9902/hooks"),
            AppSpec::conformance_disabled(),
        ];
        let json = render_apps_json(&apps);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v.as_array().unwrap().len(), 2);
        let main = &v[0];
        assert_eq!(main["key"], "cf-key-main");
        assert_eq!(main["enabled"], true);
        assert_eq!(main["client_messages_enabled"], true);
        assert_eq!(main["subscription_count_enabled"], true);
        assert_eq!(main["webhooks"][0]["url"], "http://127.0.0.1:9902/hooks");
        assert!(
            main["webhooks"][0]["event_types"]
                .as_array()
                .unwrap()
                .iter()
                .count()
                >= 1
        );
        assert_eq!(v[1]["enabled"], false);
        assert_eq!(v[1]["key"], "cf-key-disabled");
    }

    #[tokio::test]
    #[ignore = "needs ../target/release/pylon; run in CI or after cargo build --release"]
    async fn spawns_and_shuts_down_real_pylon() {
        let mut s = server::spawn_pylon(
            "../target/release/pylon",
            19801,
            &[AppSpec::conformance_main("http://127.0.0.1:1/hooks")],
        )
        .await
        .unwrap();
        // Same hard-stop wrapper as the orchestrator: wait_ready's internal
        // poll has no per-attempt cap, so the call site supplies the deadline.
        tokio::time::timeout(
            Duration::from_secs(30),
            s.wait_ready(Duration::from_secs(30)),
        )
        .await
        .expect("health wait must respect the outer timeout")
        .expect("pylon must become healthy");
        s.shutdown().await;
    }
}
