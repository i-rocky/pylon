//! Integration tests for native TLS (Part B): end-to-end wss:// handshake,
//! encrypted WS subscribe + event delivery roundtrip, resilience after a
//! plain client sends garbage to the TLS port, and REST publish over the same
//! native-TLS port.

use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::auth::signature::{hmac_sha256_hex, md5_hex};
use pylon::channel::registry::Registry;
use pylon::protocol::event::ServerEvent;
use pylon::server::config::ServerConfig;
use pylon::server::router::{build_router, AppState};
use pylon::webhook::WebhookHandle;
use rcgen::generate_simple_self_signed;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

const APPS: &str = r#"[
    {"name":"TlsTest","id":"tls-app","key":"tls-key","secret":"tls-secret",
     "capacity":100,"client_messages_enabled":false,"subscription_count_enabled":false}
]"#;

const KEY: &str = "tls-key";
const APP_ID: &str = "tls-app";
const CHANNEL: &str = "tls-channel";

// ── cert generation ────────────────────────────────────────────────────────────

/// Generate a self-signed cert+key, write PEM files to temp dir. Returns
/// (cert_der_bytes, cert_pem_path, key_pem_path).
fn gen_cert() -> (Vec<u8>, PathBuf, PathBuf) {
    let cert = generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
        .expect("rcgen: generate self-signed cert");

    let dir = std::env::temp_dir();
    let pid = std::process::id();
    // Use a random suffix so parallel test runs don't collide.
    let n: u64 = rand::random();
    let cert_path = dir.join(format!("pylon-tls-test-cert-{pid}-{n}.pem"));
    let key_path = dir.join(format!("pylon-tls-test-key-{pid}-{n}.pem"));

    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();

    let cert_der = cert.cert.der().to_vec();
    (cert_der, cert_path, key_path)
}

/// Build a rustls ClientConfig that trusts ONLY our self-signed cert.
fn tls_client_config(cert_der: &[u8]) -> Arc<rustls::ClientConfig> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut root_store = rustls::RootCertStore::empty();
    let cert = rustls::pki_types::CertificateDer::from(cert_der.to_vec());
    root_store
        .add(cert)
        .expect("add self-signed cert to root store");
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    Arc::new(config)
}

// ── server harness ─────────────────────────────────────────────────────────────

struct TlsHarness {
    port: u16,
    adapter: Arc<dyn Adapter>,
}

/// Spawn a TLS-enabled percore server on 127.0.0.1 with REST handoff wired.
/// Returns the port and adapter.
async fn spawn_tls_server(cert_path: &std::path::Path, key_path: &std::path::Path) -> TlsHarness {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };

    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new()), Arc::new(pylon::adapter::app_registry::AppRegistry::new())));
    let adapter: Arc<dyn Adapter> = local.clone();
    let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());
    let webhooks = WebhookHandle::null();

    let config = ServerConfig {
        bind: "127.0.0.1".into(),
        port,
        workers: 1,
        tls_cert_path: Some(cert_path.to_str().unwrap().to_string()),
        tls_key_path: Some(key_path.to_str().unwrap().to_string()),
        ..ServerConfig::default()
    };

    let tls = pylon::transport::tls::resolve_tls(
        &config.tls_cert_path,
        &config.tls_key_path,
        &config.tls_ca_path,
    )
    .expect("TLS config should load from test cert/key");

    // Wire REST handoff so HTTPS REST requests go through TlsRestStream.
    let (rest_tx, rest_rx) = tokio::sync::mpsc::unbounded_channel::<pylon::transport::RestConn>();
    let rest_state = AppState {
        config: config.clone(),
        apps: apps.clone(),
        adapter: adapter.clone(),
        conn_counts: Arc::clone(&conn_counts),
        webhooks: webhooks.clone(),
        saturated: Some(local.saturation_flag()),
        draining: Arc::new(AtomicBool::new(false)),
        cluster_metrics: None,
        invalidator: None,
    };
    tokio::spawn(pylon::transport::rest::serve(
        rest_rx,
        build_router(rest_state),
    ));

    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = shutdown.clone();
    let local_for_sink = Some(local.clone());
    // Phase 7: capture the runtime handle here (async context) before spawning.
    let worker_runtime = tokio::runtime::Handle::current();
    let handle = std::thread::spawn(move || {
        let _ = pylon::transport::run_percore(
            config,
            apps,
            adapter,
            conn_counts,
            Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            webhooks,
            Some(rest_tx),
            worker_shutdown,
            local_for_sink,
            false,
            tls,
            worker_runtime,
        );
    });
    std::mem::forget((shutdown, handle));

    tokio::time::sleep(Duration::from_millis(250)).await;
    TlsHarness {
        port,
        adapter: local,
    }
}

/// Build a signed REST query string for the TLS app (`tls-key` / `tls-secret`).
fn tls_signed_query(method: &str, path: &str, body: &[u8]) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut p: BTreeMap<String, String> = BTreeMap::new();
    p.insert("auth_key".into(), KEY.to_string());
    p.insert("auth_timestamp".into(), now.to_string());
    p.insert("auth_version".into(), "1.0".into());
    if !body.is_empty() {
        p.insert("body_md5".into(), md5_hex(body));
    }
    let canon = p
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let signed = format!("{}\n{}\n{}", method.to_uppercase(), path, canon);
    let sig = hmac_sha256_hex("tls-secret", &signed);
    format!("{canon}&auth_signature={sig}")
}

// ── helper: connect a wss client ───────────────────────────────────────────────

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect_wss(port: u16, cert_der: &[u8]) -> Ws {
    let client_cfg = tls_client_config(cert_der);
    let connector = tokio_tungstenite::Connector::Rustls(client_cfg);
    let url = format!("wss://127.0.0.1:{port}/app/{KEY}?protocol=7");
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async_tls_with_config(&url, None, false, Some(connector)),
    )
    .await
    .expect("wss connect: timeout")
    .expect("wss connect: error");
    ws
}

/// Receive the next text frame from `ws`, with a 5-second timeout.
async fn next_text(ws: &mut Ws) -> String {
    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("recv: timeout")
        .expect("recv: stream ended")
        .expect("recv: frame error");
    msg.into_text().expect("expected text frame")
}

// ── tests ──────────────────────────────────────────────────────────────────────

/// A wss:// client can complete the TLS handshake + WS upgrade and receive the
/// `connection_established` frame — end-to-end encrypted.
#[tokio::test]
async fn wss_handshake_receives_connection_established() {
    let (cert_der, cert_path, key_path) = gen_cert();
    let harness = spawn_tls_server(&cert_path, &key_path).await;

    let mut ws = connect_wss(harness.port, &cert_der).await;

    let text = next_text(&mut ws).await;
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        v["event"].as_str().unwrap(),
        "pusher:connection_established",
        "expected connection_established, got: {text}"
    );

    let _ = std::fs::remove_file(&cert_path);
    let _ = std::fs::remove_file(&key_path);
}

/// A wss:// client can subscribe to a channel and receive a broadcast event
/// delivered by the adapter — encrypted end-to-end.
#[tokio::test]
async fn wss_subscribe_and_receive_broadcast() {
    let (cert_der, cert_path, key_path) = gen_cert();
    let harness = spawn_tls_server(&cert_path, &key_path).await;

    let mut ws = connect_wss(harness.port, &cert_der).await;

    // 1. Receive connection_established and extract socket_id.
    let text = next_text(&mut ws).await;
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        v["event"].as_str().unwrap(),
        "pusher:connection_established"
    );
    let data: Value = serde_json::from_str(v["data"].as_str().unwrap()).unwrap();
    let _socket_id = data["socket_id"].as_str().unwrap().to_string();

    // 2. Subscribe to a channel.
    let sub = json!({"event":"pusher:subscribe","data":{"channel":CHANNEL}});
    ws.send(Message::Text(sub.to_string())).await.unwrap();

    let text = next_text(&mut ws).await;
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        v["event"].as_str().unwrap(),
        "pusher_internal:subscription_succeeded",
        "expected subscription_succeeded, got: {text}"
    );

    // 3. Broadcast via the adapter.
    futures_executor::block_on(harness.adapter.broadcast(
        APP_ID,
        CHANNEL,
        ServerEvent::ChannelEvent {
            channel: CHANNEL.to_string(),
            event: "tls-event".to_string(),
            data: json!({"hello": "tls"}),
            user_id: None,
        },
        None,
    ));

    // 4. Receive the broadcast over the encrypted socket.
    let text = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let t = next_text(&mut ws).await;
            let v: Value = serde_json::from_str(&t).unwrap();
            if v["event"].as_str() == Some("tls-event") {
                return t;
            }
        }
    })
    .await
    .expect("timeout waiting for tls-event");

    assert!(
        text.contains("tls-event"),
        "expected tls-event delivery over wss, got: {text}"
    );

    let _ = std::fs::remove_file(&cert_path);
    let _ = std::fs::remove_file(&key_path);
}

/// A plain ws:// client connecting to a TLS port gets a handshake error (TLS
/// record parsing failure). The server should not crash and a subsequent wss://
/// client should succeed.
#[tokio::test]
async fn plain_ws_to_tls_port_fails_server_survives() {
    let (cert_der, cert_path, key_path) = gen_cert();
    let harness = spawn_tls_server(&cert_path, &key_path).await;

    // Plain ws:// to a TLS port — should fail (TLS record rejection).
    let plain_url = format!("ws://127.0.0.1:{}/app/{}?protocol=7", harness.port, KEY);
    let plain_result = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(&plain_url),
    )
    .await;
    let failed = match plain_result {
        Err(_) => true,     // timeout
        Ok(Err(_)) => true, // connection or handshake error — expected
        Ok(Ok(_)) => false, // unexpectedly succeeded
    };
    assert!(failed, "plain ws:// to a TLS port must fail");

    // Server should still be alive: a wss:// client succeeds immediately after.
    let mut ws2 = connect_wss(harness.port, &cert_der).await;

    let text = next_text(&mut ws2).await;
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        v["event"].as_str().unwrap(),
        "pusher:connection_established",
        "server alive after plain failure: expected connection_established, got: {text}"
    );

    let _ = std::fs::remove_file(&cert_path);
    let _ = std::fs::remove_file(&key_path);
}

/// A wss:// subscriber and an HTTPS REST publish to the **same** native-TLS
/// port: proves that `TlsRestStream` correctly drives the rustls session in the
/// async REST plane and that the event reaches the subscriber end-to-end.
///
/// Flow:
/// 1. Start the TLS percore server with REST handoff wired.
/// 2. Connect a `wss://` subscriber and subscribe to the test channel.
/// 3. POST a signed `POST /apps/{id}/events` over HTTPS (same port).
/// 4. Assert the response is HTTP 200.
/// 5. Assert the subscriber receives the published event over the encrypted
///    WebSocket connection.
#[tokio::test]
async fn rest_publish_over_native_tls() {
    let (cert_der, cert_path, key_path) = gen_cert();
    let harness = spawn_tls_server(&cert_path, &key_path).await;

    // ── 1. Connect a wss:// subscriber ──────────────────────────────────────
    let mut ws = connect_wss(harness.port, &cert_der).await;

    // Receive connection_established.
    let text = next_text(&mut ws).await;
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        v["event"].as_str().unwrap(),
        "pusher:connection_established",
        "expected connection_established, got: {text}"
    );

    // Subscribe to the test channel.
    let sub = json!({"event":"pusher:subscribe","data":{"channel":CHANNEL}});
    ws.send(Message::Text(sub.to_string())).await.unwrap();

    let text = next_text(&mut ws).await;
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        v["event"].as_str().unwrap(),
        "pusher_internal:subscription_succeeded",
        "expected subscription_succeeded, got: {text}"
    );

    // ── 2. POST an event via HTTPS REST to the same TLS port ─────────────────
    let body = json!({"name":"tls-rest-event","data":"{\"from\":\"https\"}","channels":[CHANNEL]})
        .to_string();
    let path = format!("/apps/{APP_ID}/events");
    let q = tls_signed_query("POST", &path, body.as_bytes());

    // Build a reqwest client that trusts our self-signed cert.
    let cert = reqwest::Certificate::from_der(&cert_der).expect("parse cert DER");
    let client = reqwest::Client::builder()
        .add_root_certificate(cert)
        .build()
        .expect("build reqwest client with custom root");

    let resp = client
        .post(format!("https://127.0.0.1:{}{path}?{q}", harness.port))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("HTTPS POST to TLS port");

    assert_eq!(
        resp.status(),
        200,
        "expected 200 from HTTPS REST publish, got: {}",
        resp.status()
    );

    // ── 3. Assert the subscriber receives the event over wss:// ─────────────
    let event_text = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let t = next_text(&mut ws).await;
            let v: Value = serde_json::from_str(&t).unwrap();
            if v["event"].as_str() == Some("tls-rest-event") {
                return t;
            }
        }
    })
    .await
    .expect("timeout waiting for tls-rest-event over wss://");

    assert!(
        event_text.contains("tls-rest-event"),
        "expected tls-rest-event delivery over wss, got: {event_text}"
    );

    let _ = std::fs::remove_file(&cert_path);
    let _ = std::fs::remove_file(&key_path);
}

// ── Large-response test (C1 regression) ───────────────────────────────────────

/// App config with high capacity for the large-response test (we open one WS
/// connection that subscribes to many channels without hitting the cap).
const LARGE_RESP_APPS: &str = r#"[
    {"name":"LargeRespTest","id":"large-app","key":"large-key","secret":"large-secret",
     "capacity":10000,"client_messages_enabled":false,"subscription_count_enabled":false}
]"#;
const LARGE_KEY: &str = "large-key";
const LARGE_APP_ID: &str = "large-app";
const LARGE_N_CHANNELS: usize = 2000;

async fn spawn_tls_server_large(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> TlsHarness {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let apps: Arc<dyn AppManager> =
        Arc::new(StaticFileAppManager::from_json(LARGE_RESP_APPS).unwrap());
    let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new()), Arc::new(pylon::adapter::app_registry::AppRegistry::new())));
    let adapter: Arc<dyn Adapter> = local.clone();
    let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());
    let webhooks = WebhookHandle::null();
    let config = ServerConfig {
        bind: "127.0.0.1".into(),
        port,
        workers: 1,
        tls_cert_path: Some(cert_path.to_str().unwrap().to_string()),
        tls_key_path: Some(key_path.to_str().unwrap().to_string()),
        // 0 = unlimited: this test subscribes one connection to LARGE_N_CHANNELS
        // channels to build a large REST response body; the subscription cap must
        // not limit it.
        max_subscriptions_per_connection: 0,
        ..ServerConfig::default()
    };
    let tls = pylon::transport::tls::resolve_tls(
        &config.tls_cert_path,
        &config.tls_key_path,
        &config.tls_ca_path,
    )
    .expect("TLS config should load");
    let (rest_tx, rest_rx) = tokio::sync::mpsc::unbounded_channel::<pylon::transport::RestConn>();
    let rest_state = AppState {
        config: config.clone(),
        apps: apps.clone(),
        adapter: adapter.clone(),
        conn_counts: Arc::clone(&conn_counts),
        webhooks: webhooks.clone(),
        saturated: Some(local.saturation_flag()),
        draining: Arc::new(AtomicBool::new(false)),
        cluster_metrics: None,
        invalidator: None,
    };
    tokio::spawn(pylon::transport::rest::serve(
        rest_rx,
        build_router(rest_state),
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = shutdown.clone();
    let local_for_sink = Some(local.clone());
    // Phase 7: capture the runtime handle here (async context) before spawning.
    let worker_runtime = tokio::runtime::Handle::current();
    let handle = std::thread::spawn(move || {
        let _ = pylon::transport::run_percore(
            config,
            apps,
            adapter,
            conn_counts,
            Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            webhooks,
            Some(rest_tx),
            worker_shutdown,
            local_for_sink,
            false,
            tls,
            worker_runtime,
        );
    });
    std::mem::forget((shutdown, handle));
    tokio::time::sleep(Duration::from_millis(250)).await;
    TlsHarness {
        port,
        adapter: local,
    }
}

fn large_signed_query(method: &str, path: &str, body: &[u8]) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut p: BTreeMap<String, String> = BTreeMap::new();
    p.insert("auth_key".into(), LARGE_KEY.to_string());
    p.insert("auth_timestamp".into(), now.to_string());
    p.insert("auth_version".into(), "1.0".into());
    if !body.is_empty() {
        p.insert("body_md5".into(), md5_hex(body));
    }
    let canon = p
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let signed = format!("{}\n{}\n{}", method.to_uppercase(), path, canon);
    let sig = hmac_sha256_hex("large-secret", &signed);
    format!("{canon}&auth_signature={sig}")
}

/// Exercises `TlsRestStream` with a large HTTPS response body (C1 regression
/// test). Subscribes one WSS client to `LARGE_N_CHANNELS` channels with long
/// names, then GETs the channel list over HTTPS. The response spans multiple
/// TLS records, exercising the `out_ct` drain loop.
///
/// A truncated body (the pre-fix C1 bug) would fail the Content-Length check
/// or produce invalid JSON.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rest_large_response_over_native_tls() {
    let (cert_der, cert_path, key_path) = gen_cert();
    let harness = spawn_tls_server_large(&cert_path, &key_path).await;

    // ── 1. Connect one WSS client and subscribe it to LARGE_N_CHANNELS ────────
    let mut ws = {
        let client_cfg = tls_client_config(&cert_der);
        let connector = tokio_tungstenite::Connector::Rustls(client_cfg);
        let url = format!(
            "wss://127.0.0.1:{}/app/{}?protocol=7",
            harness.port, LARGE_KEY
        );
        let (ws, _) = tokio::time::timeout(
            Duration::from_secs(10),
            tokio_tungstenite::connect_async_tls_with_config(&url, None, false, Some(connector)),
        )
        .await
        .expect("wss connect: timeout")
        .expect("wss connect: error");
        ws
    };

    // Receive connection_established.
    let _ = next_text(&mut ws).await;

    // Send subscribe messages in batches, draining replies between batches to
    // prevent the WS send buffer from filling up and causing a connection reset.
    let base = "abcdefghij".repeat(10); // 100-char base
    const BATCH: usize = 50;
    let mut total_confirmed = 0usize;
    let mut i = 0usize;
    while i < LARGE_N_CHANNELS {
        // Send one batch.
        let end = (i + BATCH).min(LARGE_N_CHANNELS);
        for j in i..end {
            let ch = format!("{base}-{j:04}"); // 106 chars
            let sub = json!({"event": "pusher:subscribe", "data": {"channel": ch}});
            ws.send(Message::Text(sub.to_string()))
                .await
                .expect("subscribe send");
        }
        i = end;
        // Drain replies for a short window so the server's write buffer doesn't back up.
        let confirmed = tokio::time::timeout(Duration::from_millis(100), async {
            let mut cnt = 0usize;
            while let Ok(Some(Ok(Message::Text(t)))) =
                tokio::time::timeout(Duration::from_millis(10), ws.next()).await
            {
                if t.contains("subscription_succeeded") {
                    cnt += 1;
                }
            }
            cnt
        })
        .await
        .unwrap_or(0);
        total_confirmed += confirmed;
    }
    // Final drain: wait for remaining subscription_succeeded frames.
    let _ = tokio::time::timeout(Duration::from_secs(15), async {
        while let Ok(Some(Ok(Message::Text(t)))) =
            tokio::time::timeout(Duration::from_millis(300), ws.next()).await
        {
            if t.contains("subscription_succeeded") {
                total_confirmed += 1;
                if total_confirmed >= LARGE_N_CHANNELS {
                    break;
                }
            }
        }
    })
    .await;
    let _ = total_confirmed; // consumed above; suppress unused warning

    // ── 2. GET /apps/{id}/channels over HTTPS ─────────────────────────────────
    let path = format!("/apps/{LARGE_APP_ID}/channels");
    let q = large_signed_query("GET", &path, &[]);
    let cert = reqwest::Certificate::from_der(&cert_der).expect("parse cert DER");
    let client = reqwest::Client::builder()
        .add_root_certificate(cert)
        .build()
        .expect("build reqwest client");

    let resp = client
        .get(format!("https://127.0.0.1:{}{path}?{q}", harness.port))
        .send()
        .await
        .expect("HTTPS GET /channels");

    assert_eq!(
        resp.status(),
        200,
        "expected HTTP 200, got {}",
        resp.status()
    );

    // ── 3. Verify the body is complete and valid ───────────────────────────────
    let content_length = resp.content_length();
    let body_bytes = resp.bytes().await.expect("read response body");
    let body_len = body_bytes.len();

    // Content-Length must match actual body length (truncation → mismatch).
    if let Some(cl) = content_length {
        assert_eq!(
            cl as usize, body_len,
            "Content-Length ({cl}) != actual body ({body_len}): truncated TLS response (C1 bug)"
        );
    }

    // Body must be valid JSON with a "channels" key.
    let parsed: Value =
        serde_json::from_slice(&body_bytes).expect("response body must be valid JSON");
    let channels = parsed["channels"]
        .as_object()
        .expect("must have 'channels' object");
    let n = channels.len();

    // Expect at least half the channels registered (allows for any slow
    // processing of subscribe messages vs the drain timeout above).
    assert!(
        n >= LARGE_N_CHANNELS / 2,
        "expected at least {} channels in response, got {n} (body={body_len}B)",
        LARGE_N_CHANNELS / 2,
    );

    println!(
        "[rest_large_response_over_native_tls] channels={n}, body_size={body_len}B ({:.1}KB)",
        body_len as f64 / 1024.0,
    );

    let _ = std::fs::remove_file(&cert_path);
    let _ = std::fs::remove_file(&key_path);
}
