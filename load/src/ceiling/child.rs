//! Pylon child-process manager: spawn, core-pin, readiness-wait, /proc reads, teardown.

use anyhow::{bail, Context};
use std::net::TcpStream;
use std::time::{Duration, Instant};
use tokio::process::Command;

use crate::metrics::{parse_cpu_ticks, parse_rss_kb};

/// Options for spawning a pylon child process.
pub struct ChildOpts {
    pub pylon_bin: String,
    pub port: u16,
    pub workers: usize,
    /// taskset CPU list, e.g. "0-3" or "0,2"
    pub cores: String,
    pub apps_path: String,
}

/// A managed pylon child process.
pub struct PylonChild {
    child: tokio::process::Child,
    pgid: u32,
    apps_path: String,
}

impl PylonChild {
    /// Spawn a pylon child under taskset, wait for it to be listening, then return.
    pub async fn spawn(opts: &ChildOpts) -> anyhow::Result<Self> {
        let mut cmd = Command::new("taskset");
        cmd.args(["-c", &opts.cores, &opts.pylon_bin])
            .env("PYLON_APPS_PATH", &opts.apps_path)
            .env("PYLON_WORKERS", opts.workers.to_string())
            .env("PYLON_PORT", opts.port.to_string())
            .env("PYLON_BIND", "127.0.0.1")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            // Put child in its own process group (safe, stable on Linux).
            .process_group(0);

        let mut child = cmd.spawn().context("failed to spawn pylon via taskset")?;

        // The child is the process-group leader because we called process_group(0).
        let pid = child.id().context("child exited before we could read its PID")?;
        let pgid = pid;

        // Poll until the child is listening or we time out (20 s — debug builds and
        // loaded candidate servers can be slow to bind).
        let deadline = Instant::now() + Duration::from_secs(20);
        let addr = format!("127.0.0.1:{}", opts.port);
        loop {
            // Check if the child has already exited.
            match child.try_wait() {
                Ok(Some(status)) => bail!("pylon child exited early with status {status}"),
                Ok(None) => {}
                Err(e) => {
                    let _ = child.start_kill();
                    let _ = child.try_wait();
                    bail!("try_wait error: {e}");
                }
            }

            if TcpStream::connect(&addr as &str).is_ok() {
                break;
            }

            if Instant::now() >= deadline {
                // Reliably kill the started-but-not-ready child before erroring, so a
                // failed readiness check never leaks a pylon process.
                let _ = child.start_kill();
                let _ = child.try_wait();
                bail!("pylon child did not become ready on {} within 20 s", addr);
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Ok(PylonChild { child, pgid, apps_path: opts.apps_path.clone() })
    }

    /// Return the child PID.
    pub fn pid(&self) -> u32 {
        // `id()` returns None once the child has been waited; pgid equals pid at spawn.
        self.pgid
    }

    /// Current RSS in bytes, read from `/proc/<pid>/status`.
    pub fn rss_bytes(&self) -> Option<u64> {
        let status = std::fs::read_to_string(format!("/proc/{}/status", self.pgid)).ok()?;
        parse_rss_kb(&status).map(|kb| kb * 1024)
    }

    /// (utime, stime) clock ticks, read from `/proc/<pid>/stat`.
    pub fn cpu_ticks(&self) -> Option<(u64, u64)> {
        let stat = std::fs::read_to_string(format!("/proc/{}/stat", self.pgid)).ok()?;
        parse_cpu_ticks(&stat)
    }
}

impl Drop for PylonChild {
    fn drop(&mut self) {
        // Graceful first: SIGTERM the child by its POSITIVE pid. (The negative
        // process-group form via the `kill` binary is unreliable on util-linux —
        // it returns success but does not signal the group across sessions, which
        // silently leaks the child.) pylon is a single process, so the pid suffices.
        let _ = std::process::Command::new("kill")
            .args(["-TERM", &self.pgid.to_string()])
            .status();

        // Reap, escalating to a GUARANTEED SIGKILL via the tokio handle if the child
        // hasn't exited within 2 s. `start_kill` signals the kernel directly (no CLI
        // parsing), so it cannot silently no-op. Best-effort: never panic in Drop.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => break, // child exited and reaped
                Ok(None) => {}        // still running
                Err(_) => break,      // unexpected error — give up
            }
            if std::time::Instant::now() >= deadline {
                let _ = self.child.start_kill(); // SIGKILL — guaranteed delivery
                let _ = self.child.try_wait(); // reap the now-dead child
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }

        // Remove the temp apps file last. Best-effort.
        let _ = std::fs::remove_file(&self.apps_path);
    }
}

/// Write a throwaway apps JSON file to a temp path and return the path.
pub fn write_temp_apps() -> anyhow::Result<String> {
    let path = std::env::temp_dir()
        .join(format!("pylon-ceiling-apps-{}.json", std::process::id()));
    let json = r#"[{"name":"T","id":"app","key":"app-key","secret":"app-secret","capacity":2000000,"client_messages_enabled":true}]"#;
    std::fs::write(&path, json).context("write temp apps")?;
    path.to_str()
        .map(|s| s.to_owned())
        .context("temp path is not valid UTF-8")
}

/// Return the path to the pylon binary.
///
/// Searches for a `pylon` binary in:
/// 1. The same directory as the current executable (normal installed layout).
/// 2. The parent of that directory (Cargo layout: integration-test binaries live
///    in `target/<profile>/deps/`, while bin outputs live in `target/<profile>/`).
///
/// Falls back to `"pylon"` (PATH lookup) if neither exists.
pub fn default_pylon_bin() -> String {
    if let Ok(exe) = std::env::current_exe() {
        // Try same dir first.
        if let Some(dir) = exe.parent() {
            let candidate = dir.join("pylon");
            if candidate.exists() {
                if let Some(s) = candidate.to_str() {
                    return s.to_owned();
                }
            }
            // Try one level up (Cargo test-binary layout: deps/ → profile dir).
            if let Some(parent) = dir.parent() {
                let candidate = parent.join("pylon");
                if candidate.exists() {
                    if let Some(s) = candidate.to_str() {
                        return s.to_owned();
                    }
                }
            }
        }
    }
    "pylon".to_owned()
}
