//! Pylon child-process manager: spawn, core-pin, readiness-wait, /proc reads, teardown.

use anyhow::{bail, Context};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::net::TcpStream;
use std::os::unix::fs::OpenOptionsExt;
use std::time::{Duration, Instant};
use tokio::process::Command;

use crate::metrics::{parse_cpu_ticks, parse_rss_kb};

/// A pylon app key and secret for one ceiling run.
pub struct AppCredentials {
    pub key: String,
    pub secret: String,
}

impl AppCredentials {
    /// Draw a fresh key and secret from the OS-seeded CSPRNG.
    pub fn generate() -> Self {
        let mut rng = rand::rng();
        let mut key = [0u8; 16];
        let mut secret = [0u8; 32];
        rng.fill(&mut key[..]);
        rng.fill(&mut secret[..]);
        Self {
            key: hex::encode(key),
            secret: hex::encode(secret),
        }
    }
}

impl std::fmt::Debug for AppCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AppCredentials { key: <redacted>, secret: <redacted> }")
    }
}

#[derive(Serialize, Deserialize)]
struct CeilingApp {
    name: String,
    id: String,
    key: String,
    secret: String,
    #[serde(default)]
    capacity: u32,
    #[serde(default)]
    client_messages_enabled: bool,
}

/// The apps JSON file the pylon child reads, and the credentials that unlock it.
///
/// A file this tool created is removed when the guard drops; a file the caller
/// supplied is left exactly as it was found.
pub struct AppsFile {
    path: String,
    app_id: String,
    credentials: AppCredentials,
    owned: bool,
}

impl AppsFile {
    /// Write a one-app apps file, with freshly generated credentials, to a temp
    /// path this process creates exclusively and only its owner can read.
    pub fn create_temp() -> anyhow::Result<Self> {
        let credentials = AppCredentials::generate();
        let mut suffix = [0u8; 8];
        rand::rng().fill(&mut suffix[..]);
        let path = std::env::temp_dir()
            .join(format!(
                "pylon-ceiling-apps-{}-{}.json",
                std::process::id(),
                hex::encode(suffix)
            ))
            .to_str()
            .context("temp path is not valid UTF-8")?
            .to_owned();

        let app_id = "app".to_owned();
        let body = serde_json::to_vec(&[CeilingApp {
            name: "T".into(),
            id: app_id.clone(),
            key: credentials.key.clone(),
            secret: credentials.secret.clone(),
            capacity: 2_000_000,
            client_messages_enabled: true,
        }])
        .context("serialise temp apps")?;

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .with_context(|| format!("create temp apps file {path}"))?;

        let guard = Self {
            path,
            app_id,
            credentials,
            owned: true,
        };
        file.write_all(&body)
            .with_context(|| format!("write temp apps file {}", guard.path))?;
        Ok(guard)
    }

    /// Take the caller's apps file as it stands, reading the app id and
    /// credentials of its first app. The file is never written to nor removed.
    pub fn use_existing(path: &str) -> anyhow::Result<Self> {
        let body = std::fs::read(path).with_context(|| format!("read apps file {path}"))?;
        let apps: Vec<CeilingApp> =
            serde_json::from_slice(&body).with_context(|| format!("parse apps file {path}"))?;
        let app = apps
            .into_iter()
            .next()
            .with_context(|| format!("apps file {path} lists no apps"))?;
        Ok(Self {
            path: path.to_owned(),
            app_id: app.id,
            credentials: AppCredentials {
                key: app.key,
                secret: app.secret,
            },
            owned: false,
        })
    }

    /// Path to hand the pylon child.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// App id the REST publish path addresses.
    pub fn app_id(&self) -> &str {
        &self.app_id
    }

    /// Credentials the pylon child will accept.
    pub fn credentials(&self) -> &AppCredentials {
        &self.credentials
    }

    /// The path this process created, or `None` when the caller supplied the file.
    pub fn owned_path(&self) -> Option<&str> {
        self.owned.then_some(self.path.as_str())
    }

    /// Remove a temp apps file, reporting a failure instead of hiding it.
    pub fn remove_temp(path: &str) {
        if let Err(e) = std::fs::remove_file(path) {
            eprintln!("pylon-ceiling: temp apps file {path} was not removed: {e}");
        }
    }
}

impl Drop for AppsFile {
    fn drop(&mut self) {
        if let Some(path) = self.owned_path() {
            Self::remove_temp(path);
        }
    }
}

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
        let pid = child
            .id()
            .context("child exited before we could read its PID")?;
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

        Ok(PylonChild { child, pgid })
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
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::sync::atomic::{AtomicU32, Ordering};

    static NEXT_DIR: AtomicU32 = AtomicU32::new(0);

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            let n = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
            let dir =
                std::env::temp_dir().join(format!("pylon-ceiling-test-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&dir).expect("create test dir");
            Self(dir)
        }

        fn file(&self, name: &str, body: &str) -> String {
            let path = self.0.join(name);
            std::fs::write(&path, body).expect("write test file");
            path.to_str().expect("test path is UTF-8").to_owned()
        }

        fn path(&self, name: &str) -> String {
            self.0
                .join(name)
                .to_str()
                .expect("test path is UTF-8")
                .to_owned()
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    const CALLER_APPS: &str = r#"[{"name":"Caller","id":"caller-app","key":"caller-key","secret":"caller-secret","capacity":10,"client_messages_enabled":true}]"#;

    fn run_that_fails_after_taking(path: &str) -> anyhow::Result<()> {
        let _apps = AppsFile::use_existing(path)?;
        anyhow::bail!("pylon child exited early")
    }

    #[test]
    fn two_runs_generate_different_credentials() {
        let a = AppCredentials::generate();
        let b = AppCredentials::generate();
        assert_ne!(a.key, b.key, "each run must get its own app key");
        assert_ne!(a.secret, b.secret, "each run must get its own app secret");
    }

    #[test]
    fn generated_credentials_have_the_expected_shape() {
        let c = AppCredentials::generate();
        assert_eq!(c.key.len(), 32, "key is 16 random bytes, hex-encoded");
        assert_eq!(c.secret.len(), 64, "secret is 32 random bytes, hex-encoded");
        assert!(
            c.key
                .chars()
                .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase()),
            "key must be lowercase hex"
        );
        assert!(
            c.secret
                .chars()
                .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase()),
            "secret must be lowercase hex"
        );
    }

    #[test]
    fn credentials_never_render_themselves_in_debug_output() {
        let c = AppCredentials::generate();
        let rendered = format!("{c:?}");
        assert!(
            !rendered.contains(&c.key),
            "debug output must not carry the app key"
        );
        assert!(
            !rendered.contains(&c.secret),
            "debug output must not carry the app secret"
        );
    }

    #[test]
    fn a_temp_apps_file_is_readable_only_by_its_owner() {
        let apps = AppsFile::create_temp().expect("create temp apps");
        let mode = std::fs::metadata(apps.path())
            .expect("stat temp apps")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            mode, 0o600,
            "the apps file holds an app secret and must not be readable by other users"
        );
    }

    #[test]
    fn a_temp_apps_file_carries_the_generated_credentials() {
        let apps = AppsFile::create_temp().expect("create temp apps");
        let body = std::fs::read_to_string(apps.path()).expect("read temp apps");
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("temp apps is JSON");
        let app = &parsed.as_array().expect("apps file is an array")[0];
        assert_eq!(app["key"], apps.credentials().key.as_str());
        assert_eq!(app["secret"], apps.credentials().secret.as_str());
        assert_eq!(app["id"], apps.app_id());
        assert_eq!(app["id"], "app");
        assert_eq!(app["capacity"], 2_000_000);
        assert_eq!(app["client_messages_enabled"], true);
    }

    #[test]
    fn a_temp_apps_file_is_removed_when_the_run_ends() {
        let path = {
            let apps = AppsFile::create_temp().expect("create temp apps");
            apps.path().to_owned()
        };
        assert!(
            !Path::new(&path).exists(),
            "a file the tool created must not outlive the run"
        );
    }

    #[test]
    fn a_temp_apps_file_overwritten_after_creation_is_still_removed() {
        let path = {
            let apps = AppsFile::create_temp().expect("create temp apps");
            std::fs::write(apps.path(), "[]").expect("overwrite temp apps");
            apps.path().to_owned()
        };
        assert!(
            !Path::new(&path).exists(),
            "the tool owns the path it exclusively created, whatever now sits at it"
        );
    }

    #[test]
    fn a_caller_supplied_apps_file_supplies_its_own_credentials() {
        let dir = TempDir::new();
        let path = dir.file("apps.json", CALLER_APPS);
        let apps = AppsFile::use_existing(&path).expect("use caller apps");
        assert_eq!(apps.credentials().key, "caller-key");
        assert_eq!(apps.credentials().secret, "caller-secret");
        assert_eq!(apps.app_id(), "caller-app");
        assert_eq!(apps.path(), path);
    }

    #[test]
    fn a_caller_supplied_apps_file_survives_the_run() {
        let dir = TempDir::new();
        let path = dir.file("apps.json", CALLER_APPS);
        drop(AppsFile::use_existing(&path).expect("use caller apps"));
        assert!(
            Path::new(&path).exists(),
            "the tool must never remove a file the caller gave it"
        );
        assert_eq!(
            std::fs::read_to_string(&path).expect("read caller apps"),
            CALLER_APPS
        );
    }

    #[test]
    fn a_caller_supplied_apps_file_survives_a_run_that_fails_partway() {
        let dir = TempDir::new();
        let path = dir.file("apps.json", CALLER_APPS);
        assert!(run_that_fails_after_taking(&path).is_err());
        assert!(
            Path::new(&path).exists(),
            "a failed run must still leave the caller's file alone"
        );
    }

    #[test]
    fn a_caller_supplied_apps_file_survives_a_panic_partway() {
        let dir = TempDir::new();
        let path = dir.file("apps.json", CALLER_APPS);
        let taken = path.clone();
        let outcome = std::panic::catch_unwind(move || {
            let _apps = AppsFile::use_existing(&taken).expect("use caller apps");
            panic!("pylon child died mid-sweep");
        });
        assert!(outcome.is_err(), "the closure must have panicked");
        assert!(
            Path::new(&path).exists(),
            "unwinding past the guard must still leave the caller's file alone"
        );
    }

    #[test]
    fn a_missing_caller_apps_file_is_an_error() {
        let dir = TempDir::new();
        let path = dir.path("absent.json");
        assert!(AppsFile::use_existing(&path).is_err());
    }

    #[test]
    fn an_unparseable_caller_apps_file_is_an_error_and_stays_on_disk() {
        let dir = TempDir::new();
        let path = dir.file("apps.json", "{ not json");
        assert!(AppsFile::use_existing(&path).is_err());
        assert!(
            Path::new(&path).exists(),
            "a rejected caller file must not be deleted"
        );
    }

    #[test]
    fn a_caller_apps_file_with_no_apps_is_an_error() {
        let dir = TempDir::new();
        let path = dir.file("apps.json", "[]");
        assert!(AppsFile::use_existing(&path).is_err());
    }

    #[test]
    fn remove_temp_deletes_the_path_and_tolerates_an_absent_one() {
        let dir = TempDir::new();
        let path = dir.file("apps.json", CALLER_APPS);
        AppsFile::remove_temp(&path);
        assert!(!Path::new(&path).exists());
        AppsFile::remove_temp(&path);
    }
}
