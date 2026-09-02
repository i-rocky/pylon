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
//!    run the observation-normalization scan (spec §7's third family), and
//!    exit with `report::exit_code` — or 1 when the scan flags raw values

mod adapter;
mod args;
mod catalog;
mod normalization;
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

/// The `--smoke` subset: C-PUB-SUB's subscribe/unsubscribe records
/// occupied/vacated webhook envelopes, so the two server-plane scenarios
/// that follow (S-TRIGGER's HTTP plane, S-WEBHOOK-VERIFY's `/last` +
/// SDK-verifier path) have something to consume — all three members
/// actually exercise instead of S-WEBHOOK-VERIFY skipping on an empty
/// receiver.
const SMOKE_IDS: [&str; 3] = ["C-PUB-SUB", "S-TRIGGER", "S-WEBHOOK-VERIFY"];

/// Wall-clock bound on one `--sign` child. Signing is pure crypto (no server
/// I/O), so a healthy runner finishes in well under a second; a hung one must
/// not pin the auth endpoint forever. Cancelling the `wait_with_output` future
/// drops the child, and `kill_on_drop` (set below) kills it — so the timeout
/// cannot orphan the process.
const SIGNER_TIMEOUT: Duration = Duration::from_secs(15);

/// Wall-clock bound on one audit `--list` child — same pattern as the signer:
/// `kill_on_drop` makes the timeout cancellation-safe, so a runner that hangs
/// instead of listing cannot stall `--audit` indefinitely.
const LISTING_TIMEOUT: Duration = Duration::from_secs(10);

/// Wall-clock bound on one `--version` probe — metadata must never stall a
/// run (same cancellation-safe pattern as the listing and signer bounds).
const VERSION_TIMEOUT: Duration = Duration::from_secs(10);

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
            Command::Audit => audit_bindings().await,
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
/// Problem families, all printed, any of which exits nonzero:
///
/// - per binding, `adapters/<sdk>/runner.js` must EXIST on disk;
/// - per sdk, `node runner.js --list` must RUN and list ids, and every
///   binding's id must appear in its runner's listing — a runner whose
///   `--list` omits a bound id means the harness would spawn a scenario the
///   adapter cannot run (the Task 7/8 note deferred this to the runners'
///   `--list` mode; resolved here). The check is BIDIRECTIONAL: a listing
///   id the catalog does not bind is equally a problem (a scenario nobody
///   selected or audited would run). Each sdk's listing is fetched ONCE and
///   reused for all its bindings. Without a usable `node` on PATH this leg
///   cannot run at all, which is itself a problem line (audit exits nonzero);
/// - `catalog::audit` cross-checks the table against the catalog (missing /
///   orphaned / wrong-sdk / duplicate bindings).
///
/// Plus one ADVISORY family (never changes the exit code): the
/// observation-normalization scan (spec §7's third family) over the last
/// report artifact, if one exists — advisory because the artifact may
/// predate a runner fix; the in-run scan is the mandatory gate.
async fn audit_bindings() -> i32 {
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

    // The --list leg needs node; without it the audit cannot be honest.
    if adapter::which_node().is_none() {
        problems
            .push("no usable `node` on PATH — cannot cross-check runner --list output".to_string());
    } else {
        // One `--list` invocation per sdk, cached across its bindings.
        let mut listings: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for (sdk, _) in &impls {
            if listings.contains_key(sdk) {
                continue;
            }
            let dir = adapters_dir().join(sdk);
            if !dir.join("runner.js").is_file() {
                continue; // already flagged as a missing runner above
            }
            match runner_listing(&dir).await {
                Ok(ids) => {
                    listings.insert(sdk.clone(), ids);
                }
                Err(e) => problems.push(format!("runner --list failed for {sdk}: {e}")),
            }
        }
        problems.extend(listing_problems(&listings, &impls));
    }

    problems.extend(catalog::audit(&impls));

    // Advisory family: scan the LAST report artifact (default path, resolved
    // from the crate dir so CWD does not matter) for un-normalized
    // observations. Advisory on purpose — the artifact may predate a runner
    // fix, and `--audit` must stay runnable without a report at hand; the
    // in-run scan is the one that fails runs.
    let last_report = Path::new(env!("CARGO_MANIFEST_DIR")).join(args::DEFAULT_REPORT);
    if !last_report.is_file() {
        println!(
            "audit: no report artifact at {} — normalization scan skipped (advisory family; run the suite first)",
            last_report.display()
        );
    } else {
        let parsed = std::fs::read_to_string(&last_report)
            .map_err(|e| e.to_string())
            .and_then(|raw| {
                serde_json::from_str::<report::Report>(&raw).map_err(|e| e.to_string())
            });
        match parsed {
            Ok(last) => {
                let violations = normalization::scan(&last.results);
                if violations.is_empty() {
                    println!(
                        "audit: normalization scan of the last report — clean ({} result(s))",
                        last.results.len()
                    );
                } else {
                    for v in &violations {
                        eprintln!("audit advisory (normalization): {v}");
                    }
                    eprintln!(
                        "audit advisory (normalization): {} violation(s) in {} — advisory here; rerun the suite to re-gate",
                        violations.len(),
                        last_report.display()
                    );
                }
            }
            Err(e) => eprintln!(
                "audit advisory (normalization): cannot parse last report {}: {e}",
                last_report.display()
            ),
        }
    }

    if problems.is_empty() {
        println!(
            "audit: OK — {} bindings, all runners present, listings match the catalog both ways",
            impls.len()
        );
        0
    } else {
        for p in &problems {
            eprintln!("audit: {p}");
        }
        eprintln!("audit: {} problem(s)", problems.len());
        1
    }
}

/// Run `node runner.js --list` in `adapter_dir` and collect the printed ids.
/// `Err` carries a one-line reason: spawn failure, non-zero exit, a hang past
/// [`LISTING_TIMEOUT`], or a listing that printed nothing (an adapter with no
/// scenarios is not a listing, it is a broken one). The child is spawned with
/// `kill_on_drop`, so the timeout's cancellation path cannot orphan it — the
/// same pattern as the signer.
async fn runner_listing(adapter_dir: &Path) -> Result<Vec<String>, String> {
    let child = tokio::process::Command::new("node")
        .arg("runner.js")
        .arg("--list")
        .current_dir(adapter_dir)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("spawn failed: {e}"))?;
    let output = tokio::time::timeout(LISTING_TIMEOUT, child.wait_with_output())
        .await
        .map_err(|_| format!("timed out after {LISTING_TIMEOUT:?}"))?
        .map_err(|e| format!("wait failed: {e}"))?;
    if !output.status.success() {
        return Err(format!("exit status {}", output.status));
    }
    let ids: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(String::from)
        .collect();
    if ids.is_empty() {
        return Err("printed no scenario ids".to_string());
    }
    Ok(ids)
}

/// One adapter's SDK version: `node runner.js --version` in `adapter_dir`,
/// trimmed stdout (e.g. `8.6.0`). `None` on any failure — spawn error,
/// non-zero exit, hang past [`VERSION_TIMEOUT`], or empty output — with the
/// reason warned on stderr: version is metadata, not a gate, so the run
/// proceeds without the pair. Same bounded, cancellation-safe spawn pattern
/// as [`runner_listing`].
async fn sdk_version(adapter_dir: &Path) -> Option<String> {
    let child = tokio::process::Command::new("node")
        .arg("runner.js")
        .arg("--version")
        .current_dir(adapter_dir)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn();
    let child = match child {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "warning: spawn `node runner.js --version` in {}: {e}",
                adapter_dir.display()
            );
            return None;
        }
    };
    let output = match tokio::time::timeout(VERSION_TIMEOUT, child.wait_with_output()).await {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => {
            eprintln!(
                "warning: wait for --version runner in {}: {e}",
                adapter_dir.display()
            );
            return None;
        }
        Err(_) => {
            eprintln!(
                "warning: --version runner in {} timed out after {VERSION_TIMEOUT:?}",
                adapter_dir.display()
            );
            return None;
        }
    };
    if !output.status.success() {
        eprintln!(
            "warning: --version runner in {} exited {}",
            adapter_dir.display(),
            output.status
        );
        return None;
    }
    let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if version.is_empty() {
        eprintln!(
            "warning: --version runner in {} printed nothing",
            adapter_dir.display()
        );
        return None;
    }
    Some(version)
}

/// Cross-check the binding table against per-sdk `--list` outputs,
/// BIDIRECTIONALLY: every `(sdk, id)` binding must appear in that sdk's
/// listing, and every id a runner lists must be a binding of that sdk — a
/// listed-but-unbound id is a scenario nobody selected, budgeted, or audited,
/// which is exactly the kind of drift the catalog exists to prevent. Sdks
/// absent from `listings` (missing runner, or a `--list` that failed) are
/// skipped — the callers that know WHY they are absent already emitted their
/// problem lines.
fn listing_problems(
    listings: &std::collections::HashMap<String, Vec<String>>,
    impls: &[(String, String)],
) -> Vec<String> {
    let mut problems: Vec<String> = impls
        .iter()
        .filter_map(|(sdk, id)| {
            listings.get(sdk).and_then(|ids| {
                (!ids.iter().any(|listed| listed == id))
                    .then(|| format!("catalog entry {id} ({sdk}) not listed by its runner"))
            })
        })
        .collect();
    // Reverse direction: listing → catalog.
    for (sdk, ids) in listings {
        for id in ids {
            if !impls.iter().any(|(s, i)| s == sdk && i == id) {
                problems.push(format!(
                    "runner {sdk} lists {id} but the catalog does not bind it"
                ));
            }
        }
    }
    problems
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
    // The guard makes the dir's lifetime match the flow: removed explicitly
    // after the loop below in the happy path, and by Drop on every early
    // error return (or panic) between here and there.
    let scratch =
        std::env::temp_dir().join(format!("pylon-conformance-env-{}", std::process::id()));
    std::fs::create_dir_all(&scratch).with_context(|| format!("create {}", scratch.display()))?;
    let _scratch_guard = ScratchDir(scratch.clone());
    let env_path = scratch.join("env.json");

    // Signing mode: the auth endpoint delegates ALL crypto to the official
    // pusher-http-node runner. Every auth hit spawns
    // `node runner.js --sign --env -- <env.json>` (env path after the
    // runner's `--` option terminator) with the request body on
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
    // Happy-path removal (the Drop guard above is the idempotent backstop
    // for the early-error paths).
    let _ = std::fs::remove_dir_all(&scratch);
    eprintln!(
        "webhook receiver captured {} envelope(s) this run",
        hooks.envelopes().len()
    );

    // SDK versions, keyed by ADAPTER id (the same id the catalog, `--sdk`,
    // and the matrix's SDK column use — the http SDK's npm package is
    // `pusher`, but the harness calls it `pusher-http-node` everywhere).
    // A failed probe drops that pair with a warning; the run proceeds.
    let mut sdk_versions = Vec::new();
    for sdk in ["pusher-js", "pusher-http-node"] {
        if let Some(version) = sdk_version(&adapters_dir().join(sdk)).await {
            sdk_versions.push((sdk.to_string(), version));
        }
    }

    let report = report::Report {
        run: report::RunMeta {
            timestamp: rfc3339_now(),
            pylon_version,
            sdk_versions,
            target: "local".to_string(),
        },
        results,
    };
    report::write_json(&report, report_path)?;
    print!("{}", report::render_human(&report));

    // Observation-normalization audit (spec §7's third family), mandatory
    // in-run: a raw socket id or timestamp in the artifact poisons the
    // stable-and-diffable report contract even when every verdict passed,
    // so violations fail the run outright.
    let violations = normalization::scan(&report.results);
    if violations.is_empty() {
        println!("normalization scan: clean — no run-unique values in any observation");
        Ok(report::exit_code(&report))
    } else {
        eprintln!();
        for v in &violations {
            eprintln!("normalization: {v}");
        }
        eprintln!(
            "normalization: {} violation(s) — observations must carry placeholders, not run-unique values (raw evidence belongs on runner stderr)",
            violations.len()
        );
        Ok(1)
    }
}

/// RAII removal for the run's scratch env dir: `run_scenarios` has several
/// early error returns between the dir's creation and its post-loop removal
/// (spawn/health/write failures) — the guard closes that window for all of
/// them at once, where per-call-site cleanups would drift.
struct ScratchDir(PathBuf);

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
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
                // The env path is dynamic: after the runner's `--` option
                // terminator it can never be misparsed as an option.
                .arg("--env")
                .arg("--")
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

/// The pylon server's version: `<bin> --version`, trimmed stdout, with the
/// leading `pylon ` program-name prefix stripped via [`strip_program_prefix`]
/// (pylon prints e.g. `pylon 0.3.0`; the report summary adds its own
/// `pylon ` prefix, so keeping it here would double it). Any failure degrades
/// to `"unknown"` — a version probe must not kill a run.
async fn pylon_version(bin: &Path) -> String {
    match tokio::process::Command::new(bin)
        .arg("--version")
        .output()
        .await
    {
        Ok(output) if output.status.success() => {
            strip_program_prefix(String::from_utf8_lossy(&output.stdout).trim()).to_string()
        }
        _ => "unknown".to_string(),
    }
}

/// Drop a leading `pylon ` from a `--version` line (`pylon 0.3.0` → `0.3.0`);
/// anything without the prefix (a bare version, `unknown`) passes through.
fn strip_program_prefix(line: &str) -> &str {
    line.strip_prefix("pylon ").unwrap_or(line)
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

    /// A unique fixture dir under the system temp dir: `<stem>-<pid>-<seq>`.
    /// Fixed names (the pre-audit-family shape) let two concurrent audits —
    /// or two `cargo test` invocations — on one host fight over the same
    /// `runner.js`; pid + an in-process counter makes every fixture its own.
    fn fixture_dir(stem: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "cf-{stem}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create fixture dir");
        dir
    }

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
        assert_eq!(ids, ["C-PUB-SUB", "S-TRIGGER", "S-WEBHOOK-VERIFY"]);
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
    fn listing_problems_flags_bindings_the_runner_omits() {
        use std::collections::HashMap;
        let impls = vec![
            ("pusher-js".to_string(), "C-ESTABLISH".to_string()),
            ("pusher-js".to_string(), "C-PING".to_string()),
            ("pusher-http-node".to_string(), "S-TRIGGER".to_string()),
        ];
        // A listing covering everything: no problems.
        let full: HashMap<String, Vec<String>> = HashMap::from([
            (
                "pusher-js".to_string(),
                vec!["C-ESTABLISH".to_string(), "C-PING".to_string()],
            ),
            (
                "pusher-http-node".to_string(),
                vec!["S-TRIGGER".to_string()],
            ),
        ]);
        assert!(super::listing_problems(&full, &impls).is_empty());

        // The runner omits C-PING: exactly one problem, naming the binding.
        let short: HashMap<String, Vec<String>> = HashMap::from([
            ("pusher-js".to_string(), vec!["C-ESTABLISH".to_string()]),
            (
                "pusher-http-node".to_string(),
                vec!["S-TRIGGER".to_string()],
            ),
        ]);
        assert_eq!(
            super::listing_problems(&short, &impls),
            vec!["catalog entry C-PING (pusher-js) not listed by its runner".to_string()]
        );

        // Reverse direction: a runner listing an id the catalog does not
        // bind is a problem of its own — exactly one line, naming the pair.
        let extra: HashMap<String, Vec<String>> = HashMap::from([
            (
                "pusher-js".to_string(),
                vec![
                    "C-ESTABLISH".to_string(),
                    "C-PING".to_string(),
                    "C-GHOST".to_string(),
                ],
            ),
            (
                "pusher-http-node".to_string(),
                vec!["S-TRIGGER".to_string()],
            ),
        ]);
        assert_eq!(
            super::listing_problems(&extra, &impls),
            vec!["runner pusher-js lists C-GHOST but the catalog does not bind it".to_string()]
        );

        // A sdk with NO listing entry (failed --list / missing runner) is
        // skipped here — its failure is reported by the caller.
        let partial: HashMap<String, Vec<String>> = HashMap::from([(
            "pusher-js".to_string(),
            vec!["C-ESTABLISH".to_string(), "C-PING".to_string()],
        )]);
        assert!(super::listing_problems(&partial, &impls).is_empty());
    }

    #[tokio::test]
    async fn runner_listing_collects_ids_and_rejects_empty_or_failing_runners() {
        if super::adapter::which_node().is_none() {
            eprintln!("skipping: node not found");
            return;
        }
        let dir = fixture_dir("audit-listing-test");
        std::fs::write(
            dir.join("runner.js"),
            "console.log('A-ONE'); console.log(''); console.log('  A-TWO  ');",
        )
        .unwrap();
        assert_eq!(
            super::runner_listing(&dir).await.unwrap(),
            vec!["A-ONE".to_string(), "A-TWO".to_string()]
        );

        let empty = fixture_dir("audit-listing-empty");
        std::fs::write(empty.join("runner.js"), "console.error('nothing');").unwrap();
        assert!(super::runner_listing(&empty)
            .await
            .unwrap_err()
            .contains("no scenario ids"));

        let failing = fixture_dir("audit-listing-failing");
        std::fs::write(failing.join("runner.js"), "process.exit(3);").unwrap();
        assert!(super::runner_listing(&failing)
            .await
            .unwrap_err()
            .contains("exit status"));

        // A runner that never lists is bounded by LISTING_TIMEOUT, not
        // forever — and the timeout names itself in the error.
        let hanging = fixture_dir("audit-listing-hanging");
        std::fs::write(hanging.join("runner.js"), "setInterval(() => {}, 1000);").unwrap();
        let started = std::time::Instant::now();
        let err = super::runner_listing(&hanging).await.unwrap_err();
        assert!(err.contains("timed out"), "error was: {err}");
        assert!(
            started.elapsed() >= super::LISTING_TIMEOUT,
            "the bound is actually enforced (elapsed {:?})",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn sdk_version_trims_stdout_and_degrades_to_none() {
        if super::adapter::which_node().is_none() {
            eprintln!("skipping: node not found");
            return;
        }
        let dir = fixture_dir("version-test");
        std::fs::write(dir.join("runner.js"), "console.log('  1.2.3  ');").unwrap();
        assert_eq!(
            super::sdk_version(&dir).await,
            Some("1.2.3".to_string()),
            "stdout is trimmed to the bare version"
        );

        let failing = fixture_dir("version-failing");
        std::fs::write(
            failing.join("runner.js"),
            "console.log('nope'); process.exit(4);",
        )
        .unwrap();
        assert_eq!(
            super::sdk_version(&failing).await,
            None,
            "non-zero exit → None"
        );

        let silent = fixture_dir("version-silent");
        std::fs::write(silent.join("runner.js"), "console.error('to stderr');").unwrap();
        assert_eq!(
            super::sdk_version(&silent).await,
            None,
            "empty stdout → None"
        );

        let missing = fixture_dir("version-missing"); // no runner.js at all
        assert_eq!(
            super::sdk_version(&missing).await,
            None,
            "spawn failure → None"
        );
    }

    #[test]
    fn scratch_dir_guard_removes_on_drop() {
        let dir = fixture_dir("scratch-guard");
        std::fs::write(dir.join("env.json"), "{}").unwrap();
        drop(super::ScratchDir(dir.clone()));
        assert!(!dir.exists(), "guard removed the scratch dir on drop");
    }

    #[test]
    fn version_prefix_is_stripped_exactly_once() {
        // pylon prints `pylon 0.3.0`; the summary re-adds `pylon `, so the
        // captured line must lose its own prefix.
        assert_eq!(super::strip_program_prefix("pylon 0.3.0"), "0.3.0");
        assert_eq!(super::strip_program_prefix("0.3.0"), "0.3.0");
        assert_eq!(super::strip_program_prefix("unknown"), "unknown");
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
