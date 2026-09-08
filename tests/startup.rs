//! Process-level tests for the `pylon` binary.
//!
//! Everything here runs the REAL executable (`CARGO_BIN_EXE_pylon`) with a real
//! `PYLON_*` environment, because that is the only honest way to exercise the
//! startup wiring in `main.rs`: the CLI surface (which ends in
//! `std::process::exit`), the adapter/app-manager selection, the REST + percore
//! fleet assembly, `pylon::init_tracing`, and the two-phase graceful shutdown
//! driven by `server::shutdown::shutdown_signal`.
//!
//! The Redis-backed test uses the dedicated test instance
//! (`PYLON_TEST_REDIS_URL`, default port 6390 — never the 6379 production
//! default) under a per-test key prefix.

#![cfg(unix)]

use std::io::Write as _;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;

const APP_ID: &str = "startup-app";
const KEY: &str = "startup-key";
const SECRET: &str = "startup-secret";

const APPS_JSON: &str = r#"[
    {"name":"Startup","id":"startup-app","key":"startup-key","secret":"startup-secret",
     "capacity":10,"client_messages_enabled":false,"subscription_count_enabled":false}
]"#;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_pylon")
}

/// Reserve then release an ephemeral port, mirroring the percore harnesses.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve an ephemeral port");
    l.local_addr().expect("reserved port").port()
}

/// Write the standard apps file into a fresh temp dir and return both (the dir
/// must outlive the server, which reads the file at startup).
fn apps_file() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir for apps.json");
    let path = dir.path().join("apps.json");
    let mut f = std::fs::File::create(&path).expect("create apps.json");
    f.write_all(APPS_JSON.as_bytes()).expect("write apps.json");
    (dir, path)
}

/// A spawned `pylon` process, killed on drop so a failing assertion never
/// leaves a server holding a port.
struct Server {
    child: Child,
    port: u16,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Server {
    fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Send SIGTERM — the signal `shutdown_signal` selects on. `Child::kill`
    /// would send SIGKILL and skip the graceful path entirely.
    fn sigterm(&self) {
        self.signal("TERM");
    }

    /// Send SIGINT: the other arm of `shutdown_signal`'s select (Ctrl-C).
    fn sigint(&self) {
        self.signal("INT");
    }

    fn signal(&self, name: &str) {
        let status = Command::new("kill")
            .arg(format!("-{name}"))
            .arg(self.pid().to_string())
            .status()
            .expect("run kill(1)");
        assert!(status.success(), "kill -{name} must be delivered");
    }

    /// Wait for the process to exit, or return `None` past the deadline.
    fn wait_exit(&mut self, budget: Duration) -> Option<std::process::ExitStatus> {
        let deadline = std::time::Instant::now() + budget;
        loop {
            match self.child.try_wait().expect("try_wait") {
                Some(status) => return Some(status),
                None if std::time::Instant::now() >= deadline => return None,
                None => std::thread::sleep(Duration::from_millis(20)),
            }
        }
    }
}

/// Base environment for a booted server: a real apps file, one worker, and the
/// caller's port. Callers layer their own `PYLON_*` on top.
fn server_command(port: u16, apps_path: &std::path::Path) -> Command {
    let mut cmd = Command::new(binary());
    cmd.env("PYLON_BIND", "127.0.0.1")
        .env("PYLON_PORT", port.to_string())
        .env("PYLON_APPS_PATH", apps_path)
        .env("PYLON_WORKERS", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

/// One blocking-free HTTP/1.1 request over a fresh connection. Returns
/// `(status, body)`; `None` when the port refuses the connection.
async fn http(port: u16, method: &str, target: &str, body: Option<&[u8]>) -> Option<(u16, String)> {
    let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .ok()?;
    let mut req = format!("{method} {target} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n");
    if let Some(b) = body {
        req.push_str("Content-Type: application/json\r\n");
        req.push_str(&format!("Content-Length: {}\r\n", b.len()));
    }
    req.push_str("\r\n");
    sock.write_all(req.as_bytes()).await.ok()?;
    if let Some(b) = body {
        sock.write_all(b).await.ok()?;
    }
    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), sock.read_to_end(&mut raw))
        .await
        .ok()?
        .ok()?;
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status: u16 = text.split_whitespace().nth(1)?.parse().ok()?;
    let payload = text.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
    Some((status, payload.to_string()))
}

/// Poll `GET /health` until the server answers 200, or fail the test. This is
/// the observable "the binary finished booting AND wired the REST handoff"
/// event — a wall-clock sleep would race a cold-start compile-cache miss.
async fn await_healthy(server: &mut Server, budget: Duration) {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if let Some((200, body)) = http(server.port, "GET", "/health", None).await {
            assert_eq!(body, "ok", "/health body");
            return;
        }
        if let Some(status) = server.child.try_wait().expect("try_wait") {
            panic!(
                "pylon exited during startup with {status}: {}",
                drain(server)
            );
        }
        if tokio::time::Instant::now() >= deadline {
            let port = server.port;
            panic!(
                "pylon did not serve /health on port {port} within the budget: {}",
                drain(server)
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Best-effort capture of whatever the child wrote to stderr, for assertion
/// messages. Only called on a failure path or after the child has exited.
fn drain(server: &mut Server) -> String {
    let mut out = String::new();
    if let Some(mut err) = server.child.stderr.take() {
        use std::io::Read as _;
        let _ = err.read_to_string(&mut out);
    }
    out
}

/// The child's stdout — where `init_tracing`'s `fmt` subscriber writes its
/// log lines. Only called after the child has exited.
fn drain_stdout(server: &mut Server) -> String {
    let mut out = String::new();
    if let Some(mut o) = server.child.stdout.take() {
        use std::io::Read as _;
        let _ = o.read_to_string(&mut out);
    }
    out
}

/// Run the binary with `args` and no server environment; returns
/// `(exit code, stdout, stderr)`.
fn run_cli(args: &[&str]) -> (i32, String, String) {
    let out = Command::new(binary())
        .args(args)
        .output()
        .expect("run the pylon binary");
    (
        out.status.code().expect("exited via code, not a signal"),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Run the binary with `env` and expect it to FAIL startup; returns stderr.
fn expect_startup_failure(env: &[(&str, String)]) -> String {
    let mut cmd = Command::new(binary());
    cmd.env("PYLON_BIND", "127.0.0.1")
        .env("PYLON_PORT", free_port().to_string())
        .env("PYLON_WORKERS", "1");
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("run the pylon binary");
    assert!(
        !out.status.success(),
        "expected a non-zero exit; got {:?} with stdout {:?}",
        out.status,
        String::from_utf8_lossy(&out.stdout)
    );
    String::from_utf8_lossy(&out.stderr).into_owned()
}

// ── CLI surface ───────────────────────────────────────────────────────────────

#[test]
fn version_flag_prints_the_crate_version_and_exits_zero() {
    for flag in ["--version", "-V"] {
        let (code, stdout, _) = run_cli(&[flag]);
        assert_eq!(code, 0, "{flag} must exit 0");
        assert_eq!(
            stdout,
            format!("pylon {}\n", env!("CARGO_PKG_VERSION")),
            "{flag} must print exactly the crate version"
        );
    }
}

#[test]
fn help_flag_prints_usage_and_exits_zero() {
    for flag in ["--help", "-h"] {
        let (code, stdout, _) = run_cli(&[flag]);
        assert_eq!(code, 0, "{flag} must exit 0");
        assert!(
            stdout.contains("Usage: pylon"),
            "{flag} must print the usage line: {stdout}"
        );
        assert!(
            stdout.contains("PYLON_"),
            "{flag} must point at the env-var surface: {stdout}"
        );
    }
}

/// A typo'd flag must not be silently ignored — that would boot a server the
/// operator believed they had configured differently.
#[test]
fn an_unrecognized_argument_exits_one_and_names_it() {
    let (code, stdout, stderr) = run_cli(&["--bogus"]);
    assert_eq!(code, 1, "an unknown flag must exit 1");
    assert!(stdout.is_empty(), "nothing should go to stdout: {stdout}");
    assert!(
        stderr.contains("'--bogus'") && stderr.contains("--help"),
        "stderr must name the argument and hint at --help: {stderr}"
    );
}

// ── startup failures ──────────────────────────────────────────────────────────

#[test]
fn a_db_app_manager_without_a_dsn_fails_startup_naming_the_missing_knob() {
    for kind in ["sqlite", "mysql", "postgres", "mongo"] {
        let stderr = expect_startup_failure(&[("PYLON_APP_MANAGER", kind.to_string())]);
        assert!(
            stderr.contains("PYLON_APP_DSN"),
            "PYLON_APP_MANAGER={kind} without a DSN must name PYLON_APP_DSN: {stderr}"
        );
    }
}

#[test]
fn a_missing_apps_file_fails_startup_rather_than_booting_with_no_apps() {
    let dir = tempfile::tempdir().expect("temp dir");
    let missing = dir.path().join("does-not-exist.json");
    let stderr =
        expect_startup_failure(&[("PYLON_APPS_PATH", missing.to_string_lossy().into_owned())]);
    assert!(
        !stderr.is_empty(),
        "a missing apps file must report an error on stderr"
    );
}

/// A mistyped numeric knob is a fatal misconfiguration, not a silent fall back
/// to the default: the operator did not choose the default, they typed
/// something wrong. `env_parse` reports and exits the process, so only a
/// subprocess can observe that decision at all.
#[test]
fn a_malformed_numeric_env_var_exits_one_and_names_the_variable() {
    let (_dir, apps_path) = apps_file();
    let out = Command::new(binary())
        .env("PYLON_BIND", "127.0.0.1")
        .env("PYLON_APPS_PATH", &apps_path)
        .env("PYLON_PORT", "not-a-port")
        .output()
        .expect("run the pylon binary");
    assert_eq!(
        out.status.code(),
        Some(1),
        "an unparseable PYLON_* value must exit 1, not boot on the default"
    );
    let logs = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        logs.contains("PYLON_PORT") && logs.contains("not-a-port"),
        "the report must name the variable and the offending value: {logs}"
    );
}

/// Half-configured TLS is a fatal misconfiguration, not a silent fall back to
/// plain mode — a server the operator believes is encrypted must never boot
/// unencrypted.
#[test]
fn a_tls_cert_without_a_key_fails_startup() {
    let (_dir, apps_path) = apps_file();
    let stderr = expect_startup_failure(&[
        ("PYLON_APPS_PATH", apps_path.to_string_lossy().into_owned()),
        ("PYLON_TLS_CERT", "/nonexistent/cert.pem".into()),
    ]);
    assert!(
        stderr.contains("PYLON_TLS_KEY"),
        "the error must name the missing knob: {stderr}"
    );
}

#[test]
fn an_unreadable_tls_cert_fails_startup() {
    let (_dir, apps_path) = apps_file();
    let stderr = expect_startup_failure(&[
        ("PYLON_APPS_PATH", apps_path.to_string_lossy().into_owned()),
        ("PYLON_TLS_CERT", "/nonexistent/cert.pem".into()),
        ("PYLON_TLS_KEY", "/nonexistent/key.pem".into()),
    ]);
    assert!(
        stderr.contains("PYLON_TLS_CERT"),
        "the error must name the unreadable file's knob: {stderr}"
    );
}

// ── a real boot ───────────────────────────────────────────────────────────────

/// Sign a REST request the way an SDK does (query-string auth v1.0).
fn signed_query(method: &str, path: &str, body: &[u8]) -> String {
    use pylon::auth::signature::{hmac_sha256_hex, md5_hex};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_secs();
    let canon = format!(
        "auth_key={KEY}&auth_timestamp={now}&auth_version=1.0&body_md5={}",
        md5_hex(body)
    );
    let signed = format!("{}\n{}\n{}", method.to_uppercase(), path, canon);
    format!(
        "{canon}&auth_signature={}",
        hmac_sha256_hex(SECRET, &signed)
    )
}

async fn next_json(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Value {
    loop {
        match tokio::time::timeout(Duration::from_secs(10), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                return serde_json::from_str(&t).expect("server frames are JSON")
            }
            Ok(Some(Ok(_))) => continue,
            other => panic!("expected a text frame, got {other:?}"),
        }
    }
}

/// The end-to-end proof that `main` wires a working server: the binary boots
/// from env alone, serves health, WS and the REST handoff, and then honours
/// SIGTERM through both phases of the C2a shutdown — `/ready` flips to 503
/// while the listener is still up, the connected client is closed with the
/// 4200 protocol error, and the process exits 0.
#[tokio::test]
async fn boots_from_env_serves_ws_and_rest_then_drains_on_sigterm() {
    let (dir, apps_path) = apps_file();
    let port = free_port();
    let child = server_command(port, &apps_path)
        // Long enough that the drain phase is observable without racing it.
        .env("PYLON_SHUTDOWN_PREDRAIN_MS", "3000")
        .env("PYLON_SHUTDOWN_GRACE_MS", "3000")
        .env("RUST_LOG", "warn")
        .spawn()
        .expect("spawn pylon");
    let mut server = Server {
        child,
        port,
        _dir: dir,
    };

    await_healthy(&mut server, Duration::from_secs(30)).await;
    assert_eq!(
        http(port, "GET", "/ready", None).await,
        Some((200, "ready".to_string())),
        "a booted, non-draining node must report ready"
    );

    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/app/{KEY}?protocol=7"))
            .await
            .expect("ws handshake against the real binary");
    let established = next_json(&mut ws).await;
    assert_eq!(established["event"], "pusher:connection_established");

    ws.send(Message::text(
        serde_json::json!({"event":"pusher:subscribe","data":{"channel":"startup-chan"}})
            .to_string(),
    ))
    .await
    .expect("send subscribe");
    let sub = next_json(&mut ws).await;
    assert_eq!(sub["event"], "pusher_internal:subscription_succeeded");

    let body = serde_json::json!({
        "name": "boot-event",
        "channel": "startup-chan",
        "data": "{\"ok\":true}"
    })
    .to_string();
    let path = format!("/apps/{APP_ID}/events");
    let target = format!("{path}?{}", signed_query("POST", &path, body.as_bytes()));
    let (status, rest_body) = http(port, "POST", &target, Some(body.as_bytes()))
        .await
        .expect("REST publish reaches the handed-off axum plane");
    assert_eq!(status, 200, "REST publish body: {rest_body}");

    let delivered = next_json(&mut ws).await;
    assert_eq!(
        delivered["event"], "boot-event",
        "the REST publish must reach the WS subscriber through the real binary"
    );

    server.sigterm();

    // Phase 1 of C2a: draining flips before the listener goes away, so a load
    // balancer sees 503 while the socket still answers. Asserted, not assumed —
    // a test that merely waited for exit would pass even if the drain never ran.
    let mut saw_draining = false;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(2500);
    while tokio::time::Instant::now() < deadline {
        if let Some((503, body)) = http(port, "GET", "/ready", None).await {
            assert_eq!(
                body, "draining",
                "503 must be the draining reason, not starting"
            );
            saw_draining = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        saw_draining,
        "SIGTERM must flip /ready to 503 draining before the predrain window elapses"
    );

    // Phase 2: the worker closes live connections with the 4200 protocol error.
    let mut saw_4200 = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                if t.contains("4200") {
                    saw_4200 = true;
                }
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                saw_4200 = saw_4200 || frame.is_some_and(|f| u16::from(f.code) == 4200);
                break;
            }
            Ok(None) | Ok(Some(Err(_))) => break,
            Ok(Some(Ok(_))) => continue,
            Err(_) => continue,
        }
    }
    assert!(
        saw_4200,
        "the drain must close live connections with the 4200 protocol error"
    );

    let status = server
        .wait_exit(Duration::from_secs(20))
        .expect("pylon must exit after SIGTERM");
    assert!(
        status.success(),
        "graceful shutdown must exit 0, got {status}: {}",
        drain(&mut server)
    );
}

/// SIGINT (Ctrl-C) is the other arm of `shutdown_signal`'s select and must
/// drain exactly like SIGTERM. `RUST_LOG` is unset here so the same run also
/// exercises `init_tracing`'s default-filter fallback.
#[tokio::test]
async fn ctrl_c_also_drains_and_exits_zero() {
    let (dir, apps_path) = apps_file();
    let port = free_port();
    let child = server_command(port, &apps_path)
        .env_remove("RUST_LOG")
        .env("PYLON_SHUTDOWN_PREDRAIN_MS", "0")
        .spawn()
        .expect("spawn pylon");
    let mut server = Server {
        child,
        port,
        _dir: dir,
    };

    await_healthy(&mut server, Duration::from_secs(30)).await;
    server.sigint();
    let status = server
        .wait_exit(Duration::from_secs(20))
        .expect("pylon must exit after SIGINT");
    assert!(status.success(), "expected a clean exit, got {status}");
}

/// A memory budget that resolves to zero silently disables overload shedding,
/// the REST 503 gate and the memory-derived connection ceiling. The server
/// still boots — zero means "unconfigured" downstream — but it must say so
/// loudly, or a fat-fingered fraction disables three safety controls in
/// silence.
#[tokio::test]
async fn a_memory_budget_that_resolves_to_zero_is_warned_about_loudly() {
    let (dir, apps_path) = apps_file();
    let port = free_port();
    let child = server_command(port, &apps_path)
        .env("PYLON_MEMORY_BUDGET_FRACTION", "1e-18")
        .env("PYLON_SHUTDOWN_PREDRAIN_MS", "0")
        .env("RUST_LOG", "warn")
        .spawn()
        .expect("spawn pylon");
    let mut server = Server {
        child,
        port,
        _dir: dir,
    };

    await_healthy(&mut server, Duration::from_secs(30)).await;
    server.sigterm();
    let status = server
        .wait_exit(Duration::from_secs(20))
        .expect("pylon must exit after SIGTERM");
    assert!(status.success(), "expected a clean exit, got {status}");

    let logs = drain_stdout(&mut server);
    assert!(
        logs.contains("memory budget resolved to 0"),
        "a zero budget must be warned about, not passed off as routine: {logs}"
    );
}

/// TLS turns the whole listener into a TLS endpoint. Booting with a cert+key
/// must (a) succeed and (b) leave a port that speaks TLS and NOT plaintext —
/// a plain HTTP request must not be answered as if the server were in plain
/// mode.
#[tokio::test]
async fn boots_with_tls_and_serves_only_encrypted_traffic() {
    let (dir, apps_path) = apps_file();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
        .expect("generate a self-signed cert");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).expect("write cert");
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).expect("write key");

    let port = free_port();
    let child = server_command(port, &apps_path)
        .env("PYLON_TLS_CERT", &cert_path)
        .env("PYLON_TLS_KEY", &key_path)
        .env("PYLON_SHUTDOWN_PREDRAIN_MS", "0")
        .env("RUST_LOG", "info")
        .spawn()
        .expect("spawn pylon");
    let mut server = Server {
        child,
        port,
        _dir: dir,
    };

    // The listener is up once it accepts TCP; TLS then has to complete a real
    // handshake before anything is served.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the TLS listener never came up on port {port}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // rustls 0.23 needs a process-global provider before a client config is
    // built; pylon ships exactly one (ring).
    pylon::transport::tls::install_crypto_provider();
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(cert.cert.der().clone())
        .expect("trust the generated cert");
    let client = std::sync::Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(20),
        tokio_tungstenite::connect_async_tls_with_config(
            format!("wss://127.0.0.1:{port}/app/{KEY}?protocol=7"),
            None,
            false,
            Some(tokio_tungstenite::Connector::Rustls(client)),
        ),
    )
    .await
    .expect("wss connect within 20s")
    .expect("wss handshake against the TLS-enabled binary");
    let established = next_json(&mut ws).await;
    assert_eq!(
        established["event"], "pusher:connection_established",
        "a TLS-enabled binary must still serve the v7 handshake"
    );

    assert!(
        http(port, "GET", "/health", None).await.is_none(),
        "a TLS listener must not answer a plaintext HTTP request"
    );

    server.sigterm();
    let status = server
        .wait_exit(Duration::from_secs(20))
        .expect("pylon must exit after SIGTERM");
    assert!(status.success(), "expected a clean exit, got {status}");
}

// ── SQL app store + app cache + invalidation + purge sweep ────────────────────

/// Create a SQLite apps DB holding the standard test app, and return its DSN.
async fn sqlite_app_store(dir: &std::path::Path) -> String {
    let dsn = format!("sqlite://{}?mode=rwc", dir.join("apps.db").display());
    sqlx::any::install_default_drivers();
    let pool = sqlx::AnyPool::connect(&dsn)
        .await
        .expect("create the sqlite apps db");
    sqlx::query(include_str!("../deploy/db/sqlite/001_apps.sql"))
        .execute(&pool)
        .await
        .expect("create the apps table");
    sqlx::query(
        "INSERT INTO apps (id,key,secret,name,capacity,client_messages_enabled,\
         subscription_count_enabled,enabled,webhooks) VALUES \
         ('startup-app','startup-key','startup-secret','Startup',10,0,0,1,'[]')",
    )
    .execute(&pool)
    .await
    .expect("seed the app row");
    pool.close().await;
    dsn
}

/// The full DB-backed production shape: apps come from SQL, wrapped in the L1
/// cache with a Redis L2, with cross-node invalidation and the purge sweep
/// both armed. Every one of those is a separate startup branch that the
/// static-file path never reaches, and a client must still resolve its app.
/// The local and clustered entry points assemble it independently, so both are
/// driven through this one body.
async fn sql_app_store_boot(adapter: Option<&str>) {
    let dir = tempfile::tempdir().expect("temp dir");
    let dsn = sqlite_app_store(dir.path()).await;
    let port = free_port();

    let mut cmd = Command::new(binary());
    cmd.env("PYLON_BIND", "127.0.0.1")
        .env("PYLON_PORT", port.to_string())
        .env("PYLON_WORKERS", "1")
        .env("PYLON_APP_MANAGER", "sqlite")
        .env("PYLON_APP_DSN", &dsn)
        .env("PYLON_APP_CACHE", "1")
        .env("PYLON_APP_CACHE_REDIS_URL", test_redis_url())
        .env("PYLON_APP_SWEEP_INTERVAL", "1")
        .env("PYLON_SHUTDOWN_PREDRAIN_MS", "0")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(adapter) = adapter {
        cmd.env("PYLON_ADAPTER", adapter)
            .env("PYLON_REDIS_URL", test_redis_url())
            .env(
                "PYLON_REDIS_PREFIX",
                format!("startup-sql-{}-{port}", std::process::id()),
            );
    }
    let mut server = Server {
        child: cmd.spawn().expect("spawn pylon"),
        port,
        _dir: dir,
    };

    await_healthy(&mut server, Duration::from_secs(30)).await;

    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/app/{KEY}?protocol=7"))
            .await
            .expect("ws handshake");
    let established = next_json(&mut ws).await;
    assert_eq!(
        established["event"], "pusher:connection_established",
        "the app must resolve through SQL → L1 → L2, not from any apps.json"
    );

    server.sigterm();
    let status = server
        .wait_exit(Duration::from_secs(20))
        .expect("pylon must exit after SIGTERM");
    assert!(status.success(), "expected a clean exit, got {status}");
}

#[tokio::test]
async fn local_adapter_boots_against_a_sql_app_store_with_cache_and_sweep() {
    sql_app_store_boot(None).await;
}

#[tokio::test]
async fn redis_adapter_boots_against_a_sql_app_store_with_cache_and_sweep() {
    sql_app_store_boot(Some("redis")).await;
}

// ── the clustered (redis) startup path ────────────────────────────────────────

fn test_redis_url() -> String {
    std::env::var("PYLON_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6390".to_string())
}

/// `PYLON_ADAPTER=redis` takes an entirely separate startup function
/// (`run_redis_percore`): the cluster bridge comes up first, webhooks attach to
/// it, and the worker fleet runs clustered. It must serve and shut down exactly
/// like the local path.
#[tokio::test]
async fn redis_adapter_boots_the_clustered_path_and_shuts_down_cleanly() {
    let (dir, apps_path) = apps_file();
    let port = free_port();
    let prefix = format!("startup-test-{}-{port}", std::process::id());
    let child = server_command(port, &apps_path)
        .env("PYLON_ADAPTER", "redis")
        .env("PYLON_REDIS_URL", test_redis_url())
        .env("PYLON_REDIS_PREFIX", &prefix)
        .env("PYLON_SHUTDOWN_PREDRAIN_MS", "0")
        .env("RUST_LOG", "warn")
        .spawn()
        .expect("spawn pylon");
    let mut server = Server {
        child,
        port,
        _dir: dir,
    };

    await_healthy(&mut server, Duration::from_secs(30)).await;

    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/app/{KEY}?protocol=7"))
            .await
            .expect("ws handshake against the clustered binary");
    let established = next_json(&mut ws).await;
    assert_eq!(
        established["event"], "pusher:connection_established",
        "the clustered path must serve the v7 handshake"
    );

    // `/metrics` on the redis path carries the cluster gauge the local path
    // omits — the observable proof that this really is the clustered wiring.
    let (status, metrics) = http(port, "GET", "/metrics", None)
        .await
        .expect("metrics reachable");
    assert_eq!(status, 200);
    assert!(
        metrics.contains("pylon_redis_connected"),
        "the clustered AppState must publish cluster metrics: {metrics}"
    );

    server.sigterm();
    let status = server
        .wait_exit(Duration::from_secs(20))
        .expect("pylon must exit after SIGTERM on the redis path");
    assert!(status.success(), "expected a clean exit, got {status}");
}

// ── the Mongo app store, cached without an L2 ─────────────────────────────────

fn test_mongo_url() -> String {
    std::env::var("PYLON_TEST_MONGO_URL")
        .unwrap_or_else(|_| "mongodb://127.0.0.1:27018/pylon_test".to_string())
}

/// The Mongo app manager is its own startup arm, and caching WITHOUT a Redis
/// L2 (and with the purge sweep off) is the other side of every branch the
/// SQL test above takes. A client whose app exists only in Mongo must still
/// resolve.
#[tokio::test]
async fn mongo_app_store_boots_with_the_l1_cache_and_no_l2() {
    use mongodb::bson::{doc, Document};

    let uri = test_mongo_url();
    let client = mongodb::Client::with_uri_str(&uri)
        .await
        .expect("connect Mongo (is pylon-test-mongo up on 27018?)");
    let apps = client
        .default_database()
        .expect("the mongo url must name a database")
        .collection::<Document>("apps");

    let n = uuid::Uuid::new_v4().to_string();
    let (app_id, key) = (format!("startup-mongo-{n}"), format!("mongo-key-{n}"));
    apps.insert_one(doc! {
        "id": &app_id, "key": &key, "secret": "startup-secret", "name": "StartupMongo",
        "capacity": 10_i32, "client_messages_enabled": false,
        "subscription_count_enabled": false, "enabled": true, "webhooks": [],
    })
    .await
    .expect("seed the mongo app row");

    let dir = tempfile::tempdir().expect("temp dir");
    let port = free_port();
    let mut cmd = Command::new(binary());
    cmd.env("PYLON_BIND", "127.0.0.1")
        .env("PYLON_PORT", port.to_string())
        .env("PYLON_WORKERS", "1")
        .env("PYLON_APP_MANAGER", "mongo")
        .env("PYLON_APP_DSN", &uri)
        .env("PYLON_APP_CACHE", "1")
        // No PYLON_APP_CACHE_REDIS_URL: L1 only, no invalidation subscriber.
        .env("PYLON_APP_SWEEP_INTERVAL", "0")
        .env("PYLON_SHUTDOWN_PREDRAIN_MS", "0")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut server = Server {
        child: cmd.spawn().expect("spawn pylon"),
        port,
        _dir: dir,
    };

    await_healthy(&mut server, Duration::from_secs(30)).await;

    let connected =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/app/{key}?protocol=7"))
            .await;
    let established = match connected {
        Ok((mut ws, _)) => next_json(&mut ws).await,
        Err(e) => panic!("ws handshake against the mongo-backed binary: {e}"),
    };
    assert_eq!(
        established["event"], "pusher:connection_established",
        "the app must resolve through Mongo → L1"
    );

    server.sigterm();
    let status = server
        .wait_exit(Duration::from_secs(20))
        .expect("pylon must exit after SIGTERM");
    assert!(status.success(), "expected a clean exit, got {status}");

    let _ = apps.delete_one(doc! { "id": &app_id }).await;
}
