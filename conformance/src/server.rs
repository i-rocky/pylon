//! Pylon server lifecycle for the conformance harness.
//!
//! [`spawn_pylon`] is the whole configuration mechanism: pylon reads `apps.json`
//! relative to its working directory and takes its bind address/port from
//! `PYLON_BIND`/`PYLON_PORT` (no pylon.toml involved). The harness therefore
//! gives each run a temp directory containing exactly the `apps.json` rendered
//! from the [`AppSpec`] list and starts the binary there with three env
//! overrides — nothing else is touched, so the parent `PATH`/`HOME` survive:
//!
//! - `PYLON_BIND=127.0.0.1`
//! - `PYLON_PORT=<port>`
//! - `PYLON_WEBHOOK_ALLOW_PRIVATE_TARGETS=1` (the harness webhook receiver is
//!   on localhost, which pylon refuses to deliver to by default)
//!
//! Teardown is two-stage: [`PylonServer::shutdown`] sends SIGTERM, waits up to
//! 5s, then force-kills; [`Drop`] removes the temp directory. The child is also
//! `kill_on_drop(true)` so a dropped-without-shutdown server cannot leak a
//! process.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::process::Command;

/// The seven pylon webhook event types (mirror of the main repo's
/// `WEBHOOK_EVENT_TYPES`): the conformance main app subscribes to all of them
/// so every webhook scenario has a receiver-side witness.
const ALL_EVENT_TYPES: [&str; 7] = [
    "channel_occupied",
    "channel_vacated",
    "member_added",
    "member_removed",
    "client_event",
    "cache_miss",
    "subscription_count",
];

/// A harness app definition, mirroring one `apps.json` entry (see
/// `src/app/mod.rs` in the main repo for the canonical shape).
#[derive(Debug, Clone)]
pub struct AppSpec {
    pub name: String,
    pub id: String,
    pub key: String,
    pub secret: String,
    pub enabled: bool,
    pub client_messages_enabled: bool,
    pub capacity: u32,
    pub subscription_count_enabled: bool,
    /// `(url, event_types)` pairs — exactly one entry per app in the harness.
    pub webhooks: Vec<(String, Vec<String>)>,
}

impl AppSpec {
    /// The primary harness app: enabled, full-featured, one webhook pointed at
    /// the harness receiver subscribing to every event type.
    pub fn conformance_main(webhook_url: &str) -> Self {
        Self {
            name: "conformance-main".to_string(),
            id: "cf-app-main".to_string(),
            key: "cf-key-main".to_string(),
            secret: "cf-secret-main-0123456789abcdef".to_string(),
            enabled: true,
            client_messages_enabled: true,
            capacity: 100,
            subscription_count_enabled: true,
            webhooks: vec![(
                webhook_url.to_string(),
                ALL_EVENT_TYPES.iter().map(|s| s.to_string()).collect(),
            )],
        }
    }

    /// The control app: same fixed credentials family but `enabled: false`,
    /// used to prove disabled apps reject connections. Remaining fields keep
    /// pylon's serde defaults (`client_messages_enabled=false`, `capacity=0`,
    /// `subscription_count_enabled=false`, no webhooks).
    pub fn conformance_disabled() -> Self {
        Self {
            name: "conformance-disabled".to_string(),
            id: "cf-app-disabled".to_string(),
            key: "cf-key-disabled".to_string(),
            secret: "cf-secret-disabled-0123456789ab".to_string(),
            enabled: false,
            client_messages_enabled: false,
            capacity: 0,
            subscription_count_enabled: false,
            webhooks: Vec::new(),
        }
    }
}

/// `apps.json` webhook entry (mirror; `headers` is omitted — pylon's
/// deserializer defaults it to empty).
#[derive(Serialize)]
struct WebhookJson<'a> {
    url: &'a str,
    event_types: &'a [String],
}

/// `apps.json` app entry (mirror of the main repo's `App`, same field order).
#[derive(Serialize)]
struct AppJson<'a> {
    name: &'a str,
    id: &'a str,
    key: &'a str,
    secret: &'a str,
    enabled: bool,
    client_messages_enabled: bool,
    capacity: u32,
    subscription_count_enabled: bool,
    webhooks: Vec<WebhookJson<'a>>,
}

/// Render the pylon static apps file for `apps`.
pub fn render_apps_json(apps: &[AppSpec]) -> String {
    let mirrored = apps
        .iter()
        .map(|app| AppJson {
            name: &app.name,
            id: &app.id,
            key: &app.key,
            secret: &app.secret,
            enabled: app.enabled,
            client_messages_enabled: app.client_messages_enabled,
            capacity: app.capacity,
            subscription_count_enabled: app.subscription_count_enabled,
            webhooks: app
                .webhooks
                .iter()
                .map(|(url, event_types)| WebhookJson { url, event_types })
                .collect(),
        })
        .collect::<Vec<_>>();
    serde_json::to_string_pretty(&mirrored).expect("apps render to JSON infallibly")
}

/// A spawned pylon server bound to `127.0.0.1:{port}`, rooted in `base_dir`.
///
/// The temp dir is `<tmp>/pylon-conformance-<pid>`, i.e. one per harness
/// process: sequential spawns wipe and recreate it clean. (Two servers alive
/// in the same process at once would share it — the harness never does that.)
pub struct PylonServer {
    /// Port pylon was told to bind (`PYLON_PORT`).
    pub port: u16,
    /// Temp dir that holds `apps.json` and is removed on drop.
    pub base_dir: PathBuf,
    /// The pylon child process; `None` once [`Self::shutdown`] has run.
    child: Option<tokio::process::Child>,
}

/// Spawn `bin` configured entirely by a fresh temp dir + three env overrides.
///
/// See the [module docs](self) for the environment contract. This is `async`
/// (not merely spawn-shaped) so callers stay uniform with `wait_ready` /
/// `shutdown` when Task 6 wires them into the runner's tokio runtime.
pub async fn spawn_pylon(bin: &str, port: u16, apps: &[AppSpec]) -> Result<PylonServer> {
    // The child's CWD is the temp dir, and a relative program path would be
    // resolved against THAT (post-chdir), so absolutize against ours first —
    // callers may keep passing e.g. "../target/release/pylon".
    let bin = std::path::absolute(bin)
        .with_context(|| format!("absolutize bin path {bin:?}"))?
        .to_string_lossy()
        .into_owned();

    let base_dir = std::env::temp_dir().join(format!("pylon-conformance-{}", std::process::id()));

    // Recreate clean each spawn: a stale apps.json from a previous run must
    // not leak into this one.
    match std::fs::remove_dir_all(&base_dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("clean {}", base_dir.display())),
    }
    std::fs::create_dir_all(&base_dir).with_context(|| format!("create {}", base_dir.display()))?;
    std::fs::write(base_dir.join("apps.json"), render_apps_json(apps))
        .with_context(|| format!("write {}", base_dir.join("apps.json").display()))?;

    let child = match Command::new(&bin)
        .current_dir(&base_dir)
        // ONLY these three are overridden; PATH/HOME etc. pass through so the
        // binary (and anything it shells out to) keeps working.
        .envs([
            ("PYLON_BIND", "127.0.0.1".to_string()),
            ("PYLON_PORT", port.to_string()),
            ("PYLON_WEBHOOK_ALLOW_PRIVATE_TARGETS", "1".to_string()),
        ])
        // Safety net: a dropped-without-shutdown server must not leak.
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            // The binary never started; don't leak the fresh temp dir (no
            // PylonServer exists yet, so Drop will not clean it).
            let _ = std::fs::remove_dir_all(&base_dir);
            return Err(e).with_context(|| format!("spawn {bin} in {}", base_dir.display()));
        }
    };

    Ok(PylonServer {
        port,
        base_dir,
        child: Some(child),
    })
}

impl PylonServer {
    /// Poll `GET /health` until it answers 200 or `timeout` elapses.
    ///
    /// Connection errors and non-200s both mean not-ready-yet; only the
    /// deadline turns them into a failure.
    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Ok(200) = crate::plumbing::http_get("127.0.0.1", self.port, "/health").await {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "pylon at 127.0.0.1:{} not healthy within {timeout:?}",
                    self.port
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Stop the server: SIGTERM, wait up to 5s for a clean exit, then kill.
    ///
    /// std's `Child::start_kill` only delivers SIGKILL, and one syscall is not
    /// worth a libc/nix dependency, so the graceful TERM goes through the
    /// ubiquitous `kill(1)` utility (unix-only, like the harness itself).
    pub async fn shutdown(&mut self) {
        let Some(mut child) = self.child.take() else {
            return; // already shut down
        };
        if let Some(pid) = child.id() {
            let _ = std::process::Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            match child.try_wait() {
                Ok(Some(_)) | Err(_) => return, // exited (reaped) / unreapable
                Ok(None) => {}
            }
            if tokio::time::Instant::now() >= deadline {
                let _ = child.start_kill();
                let _ = child.wait().await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for PylonServer {
    fn drop(&mut self) {
        // kill_on_drop(true) reaps the child if shutdown() never ran; either
        // way the temp dir goes away with the server.
        let _ = std::fs::remove_dir_all(&self.base_dir);
    }
}
