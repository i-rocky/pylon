//! Node-subprocess adapter runner: the process boundary every scenario
//! execution crosses (spec §4).
//!
//! An adapter is a directory containing a `runner.js`. [`run`] spawns
//! `node runner.js --scenario <id> --env <env.json>` with its working
//! directory set to the adapter directory (so the runner resolves its own
//! `node_modules`), stdout piped, stderr inherited (runner logs go to stderr;
//! stdout carries the verdict). The environment file is written by the
//! orchestrator via [`AdapterEnv::write_to`] — its exact field set is the
//! runner-side contract.
//!
//! The runner's FINAL non-empty stdout line must be one [`Verdict`] JSON
//! object; that object is authoritative even when the runner also exits
//! non-zero (a runner may verdict-fail and exit 1). Anything else — parse
//! failure, missing JSON, spawn failure, or exceeding the wall-clock budget —
//! becomes a synthesized `fail` verdict whose `error` carries the reason (and
//! at most the last 400 chars of stdout).
//!
//! Budget enforcement kills the runner's whole PROCESS GROUP: the child is
//! spawned with `process_group(0)`, and on expiry the group gets SIGTERM,
//! then SIGKILL after a short grace, so runner-spawned grandchildren die too.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt as _;
use tokio::process::{Child, Command};

/// Longest stdout excerpt embedded in a failure verdict's `error` text.
const TAIL_LIMIT: usize = 400;
/// Grace between the group SIGTERM and the group SIGKILL on budget expiry.
const KILL_GRACE: Duration = Duration::from_millis(750);
/// Bound on reaping the leader after the group SIGKILL (kill_on_drop is the
/// last resort if even this somehow stalls).
const REAP_TIMEOUT: Duration = Duration::from_millis(1500);

/// The environment contract handed to an adapter's `runner.js` (spec §4
/// env.json — field names are exact; runners read them by these keys).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterEnv {
    /// WebSocket endpoint the SDK connects to (`ws://host:port/app/<key>`).
    pub ws_url: String,
    /// Host of pylon's HTTP API plane.
    pub http_host: String,
    /// Port of pylon's HTTP API plane.
    pub http_port: u16,
    /// Conformance main app id (`cf-app-main`).
    pub app_id: String,
    /// Conformance main app key (`cf-key-main`).
    pub app_key: String,
    /// Conformance main app secret.
    pub app_secret: String,
    /// Harness auth endpoint (SDK-delegating signer).
    pub auth_endpoint: String,
    /// Harness webhook receiver base URL.
    pub webhook_receiver: String,
}

impl AdapterEnv {
    /// Write this env to `path` as pretty JSON with a trailing newline.
    ///
    /// The exact bytes matter: runners and their tests compare against
    /// `serde_json::to_string_pretty(env) + "\n"`.
    pub fn write_to(&self, path: &Path) -> anyhow::Result<()> {
        let mut json = serde_json::to_string_pretty(self)?;
        json.push('\n');
        std::fs::write(path, json).with_context(|| format!("write {}", path.display()))
    }
}

/// One scenario outcome, as printed by a runner's final stdout line.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Verdict {
    /// Scenario id the runner believes it ran.
    pub scenario: String,
    /// `pass` | `fail` (| `skip` in later phases).
    pub verdict: String,
    /// Runner observations — an arbitrary JSON object.
    pub observations: serde_json::Value,
    /// Set on `fail`: why.
    pub error: Option<String>,
    /// Wall-clock duration the runner measured, in milliseconds.
    pub duration_ms: u64,
}

/// Probe for a usable `node` on `PATH`; returns its version (e.g. `v22.21.1`).
pub fn which_node() -> Option<String> {
    let output = std::process::Command::new("node")
        .arg("--version")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Run one scenario through the adapter at `adapter_dir`, enforcing a
/// wall-clock budget. See the [module docs](self) for the full contract.
pub async fn run(adapter_dir: &Path, scenario: &str, env_path: &Path, budget_ms: u64) -> Verdict {
    let started = Instant::now();

    // The child chdirs into `adapter_dir`, so a relative `--env` path would be
    // resolved against the WRONG directory; absolutize against ours first
    // (same class of fix Task 2 applied to the pylon bin path).
    let env_path: PathBuf =
        std::path::absolute(env_path).unwrap_or_else(|_| env_path.to_path_buf());

    let mut command = Command::new("node");
    command
        .arg("runner.js")
        .arg("--scenario")
        .arg(scenario)
        .arg("--env")
        .arg(&env_path)
        .current_dir(adapter_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        // Safety net for the paths that return without an explicit reap.
        .kill_on_drop(true);
    // Own process group: a budget kill must reach runner-spawned grandchildren
    // (and the runner must not be able to signal the harness).
    #[cfg(unix)]
    command.process_group(0);

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(e) => {
            return fail_verdict(
                scenario,
                format!("spawn `node runner.js` in {}: {e}", adapter_dir.display()),
                started,
            );
        }
    };

    let pid = child.id();
    let mut stdout = child.stdout.take().expect("stdout is piped above");

    // Read stdout to END inside the budget: the verdict is the runner's final
    // line, so EOF (child exit, all writers gone) is exactly when to parse.
    // A grandchild holding the pipe open past the budget is caught by the
    // group kill below.
    let monitored = async {
        let mut out = Vec::new();
        stdout.read_to_end(&mut out).await?;
        let status = child.wait().await?;
        std::io::Result::Ok((out, status))
    };

    match tokio::time::timeout(Duration::from_millis(budget_ms), monitored).await {
        // Runner finished within budget.
        Ok(Ok((out, status))) => {
            let stdout = String::from_utf8_lossy(&out);
            // The JSON is authoritative even when the runner also exited
            // non-zero (a runner may verdict-fail AND exit 1).
            if let Some(verdict) = parse_verdict(&stdout) {
                return verdict;
            }
            let tail = tail400(stdout.trim_end());
            if status.success() {
                fail_verdict(
                    scenario,
                    format!("no verdict JSON on stdout; stdout tail: {tail}"),
                    started,
                )
            } else {
                let code = status
                    .code()
                    .map(|code| format!("exit code {code}"))
                    .unwrap_or_else(|| "terminated by signal".to_string());
                fail_verdict(scenario, format!("{code}; stdout tail: {tail}"), started)
            }
        }
        // The pipe/wait broke before the budget did.
        Ok(Err(e)) => fail_verdict(
            scenario,
            format!("reading runner output failed: {e}"),
            started,
        ),
        // Budget expiry: take down the whole group, then reap the leader.
        Err(_elapsed) => {
            kill_group_and_reap(pid, &mut child).await;
            fail_verdict(scenario, "budget exceeded".to_string(), started)
        }
    }
}

/// A synthesized failure verdict: no runner observations exist, so
/// `observations` is JSON `null` (`serde_json::Value::default()`).
fn fail_verdict(scenario: &str, error: String, started: Instant) -> Verdict {
    Verdict {
        scenario: scenario.to_string(),
        verdict: "fail".to_string(),
        observations: serde_json::Value::Null,
        error: Some(error),
        duration_ms: started.elapsed().as_millis() as u64,
    }
}

/// Kill the runner's whole process group and reap the leader.
///
/// std has no killpg and one syscall is not worth a libc/nix dependency
/// (same ruling as `PylonServer::shutdown`), so the signals go through the
/// ubiquitous `kill(1)` utility: SIGTERM to the group first (node exits
/// immediately on TERM), a short grace, then SIGKILL to the group. The
/// negative pgid is passed after `--` so it is not parsed as a flag. If even
/// the group KILL misses (a `PATH` without `kill(1)`), `start_kill` takes the
/// leader alone — degraded mode, documented: grandchildren could survive
/// only that last-resort path.
async fn kill_group_and_reap(pid: Option<u32>, child: &mut Child) {
    #[cfg(unix)]
    if let Some(pid) = pid {
        send_group_signal(pid, "TERM");
    }
    #[cfg(not(unix))]
    let _ = pid;

    let deadline = tokio::time::Instant::now() + KILL_GRACE;
    loop {
        match child.try_wait() {
            Ok(Some(_)) | Err(_) => return, // reaped / unreapable
            Ok(None) => {}
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[cfg(unix)]
    if let Some(pid) = pid {
        send_group_signal(pid, "KILL");
    }
    let _ = child.start_kill();
    let _ = tokio::time::timeout(REAP_TIMEOUT, child.wait()).await;
}

/// `kill -<SIGNAL> -- -<pid>`: signal the process GROUP (negative id = group).
#[cfg(unix)]
fn send_group_signal(pid: u32, signal: &str) {
    let _ = std::process::Command::new("kill")
        .arg(format!("-{signal}"))
        .arg("--")
        .arg(format!("-{pid}"))
        .status();
}

/// The runner's verdict: the LAST non-empty stdout line, parsed as one
/// [`Verdict`] JSON object. Earlier lines are runner chatter and ignored.
fn parse_verdict(stdout: &str) -> Option<Verdict> {
    let last = stdout.lines().rev().find(|line| !line.trim().is_empty())?;
    serde_json::from_str(last.trim()).ok()
}

/// The last ≤400 chars of `s` (char-boundary safe), for error messages.
fn tail400(s: &str) -> &str {
    if s.len() <= TAIL_LIMIT {
        return s;
    }
    let mut start = s.len() - TAIL_LIMIT;
    while !s.is_char_boundary(start) {
        start += 1;
    }
    &s[start..]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_json_round_trips_exact_schema() {
        let e = AdapterEnv {
            ws_url: "ws://127.0.0.1:19800/app/cf-key-main".into(),
            http_host: "127.0.0.1".into(),
            http_port: 19800,
            app_id: "cf-app-main".into(),
            app_key: "cf-key-main".into(),
            app_secret: "cf-secret-main-0123456789abcdef".into(),
            auth_endpoint: "http://127.0.0.1:19802/auth".into(),
            webhook_receiver: "http://127.0.0.1:19803/hooks".into(),
        };
        let json = serde_json::to_string(&e).unwrap();
        for key in [
            "ws_url",
            "http_host",
            "http_port",
            "app_id",
            "app_key",
            "app_secret",
            "auth_endpoint",
            "webhook_receiver",
        ] {
            assert!(json.contains(&format!("\"{key}\"")), "missing {key}");
        }
        let dir = std::env::temp_dir().join("cf-env-test");
        std::fs::create_dir_all(&dir).unwrap();
        e.write_to(&dir.join("env.json")).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.join("env.json")).unwrap(),
            serde_json::to_string_pretty(&e).unwrap() + "\n"
        );
    }

    #[test]
    fn verdict_parses_and_rejects_garbage() {
        let v: Verdict = serde_json::from_str(
            r#"{"scenario":"X","verdict":"pass","observations":{},"duration_ms":5}"#,
        )
        .unwrap();
        assert_eq!(v.verdict, "pass");
        assert!(serde_json::from_str::<Verdict>("not json").is_err());
    }

    #[test]
    fn verdict_parse_takes_last_non_empty_line() {
        let stdout = "earlier chatter line\n\n{\"scenario\":\"A\",\"verdict\":\"pass\",\"observations\":{},\"duration_ms\":2}\n";
        let v = parse_verdict(stdout).unwrap();
        assert_eq!(v.scenario, "A");
        assert_eq!(v.verdict, "pass");
        // Blank-only stdout, and a JSON line that is not a full Verdict.
        assert!(parse_verdict("\n\n").is_none());
        assert!(parse_verdict("{\"verdict\":\"pass\"}\n").is_none());
        assert!(parse_verdict("").is_none());
    }

    #[test]
    fn stdout_tail_is_capped_at_400_chars() {
        assert_eq!(tail400("short"), "short");
        let long = "x".repeat(500);
        assert_eq!(tail400(&long).len(), 400);
        // Multi-byte chars must not be split mid-char.
        let multibyte = "é".repeat(300);
        let tailed = tail400(&multibyte);
        assert!(tailed.chars().all(|c| c == 'é'));
    }

    #[tokio::test]
    async fn run_executes_node_runner_and_enforces_budget() {
        if which_node().is_none() {
            eprintln!("skipping: node not found");
            return;
        }
        let dir = std::env::temp_dir().join("cf-adapter-test");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("runner.js"),
            "console.error('log'); console.log(JSON.stringify({scenario:'T',verdict:'pass',observations:{},duration_ms:1}));",
        )
        .unwrap();
        let dir = dir.canonicalize().unwrap();
        // Happy path: logs on stderr, contract JSON as the final stdout line.
        let v = run(&dir, "T", Path::new("/dev/null"), 10_000).await;
        assert_eq!(v.verdict, "pass");
        assert_eq!(v.scenario, "T");
        assert_eq!(v.error, None);

        if which_node().is_none() {
            eprintln!("skipping budget leg: node not found");
            return;
        }
        // Budget kill: a runner that never prints and never exits on its own.
        let slow = dir.parent().unwrap().join("cf-adapter-slow");
        std::fs::create_dir_all(&slow).unwrap();
        std::fs::write(slow.join("runner.js"), "setTimeout(()=>{},60000);").unwrap();
        let started = std::time::Instant::now();
        let v = run(
            &slow.canonicalize().unwrap(),
            "T",
            Path::new("/dev/null"),
            1_500,
        )
        .await;
        assert_eq!(v.verdict, "fail");
        assert_eq!(v.error.as_deref(), Some("budget exceeded"));
        assert!(started.elapsed() < std::time::Duration::from_secs(10));
    }
}
