//! Wiring-level tests for [`pylon::transport::run_percore`]: the address and
//! worker-failure paths that decide whether the process starts at all, and the
//! sink-less wiring in which broadcasts fall back to the registry mailbox
//! instead of the sharded per-core fan-out.
//!
//! These live in their own test binary because `run_percore` writes the
//! process-global percore metrics registry — sharing a binary with a suite that
//! reads that registry (`tests/metrics.rs`) would make either one order
//! dependent.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::protocol::event::ServerEvent;
use pylon::server::config::ServerConfig;
use pylon::webhook::WebhookHandle;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

const APPS: &str = r#"[
    {"name":"Wiring","id":"wiring-app","key":"wiring-key","secret":"wiring-secret",
     "capacity":10,"client_messages_enabled":false,"subscription_count_enabled":false}
]"#;
const APP_ID: &str = "wiring-app";
const CHANNEL: &str = "wiring-chan";

/// An address in TEST-NET-3 (RFC 5737). It is never assigned to a local
/// interface, so binding it fails with "address not available" — the cheapest
/// deterministic way to make a worker thread fail at startup.
const UNBINDABLE: &str = "203.0.113.1";

fn base_config(bind: &str, port: u16) -> ServerConfig {
    ServerConfig {
        bind: bind.into(),
        port,
        workers: 1,
        ..ServerConfig::default()
    }
}

/// `run_percore` with the pieces every test here shares. `webhooks` is built by
/// the caller: `WebhookHandle::null()` needs a tokio reactor, and these workers
/// run on plain threads.
fn run(
    config: ServerConfig,
    local: Option<Arc<LocalAdapter>>,
    adapter: Arc<dyn Adapter>,
    webhooks: WebhookHandle,
    shutdown: Arc<AtomicBool>,
    runtime: tokio::runtime::Handle,
) -> std::io::Result<()> {
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    pylon::transport::run_percore(
        config,
        apps,
        adapter,
        Arc::new(DashMap::new()),
        Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
        Arc::new(AtomicUsize::new(0)),
        webhooks,
        None,
        shutdown,
        local,
        false,
        None,
        None,
        runtime,
    )
}

/// A `PYLON_BIND` that is not an IP address must be rejected as a
/// configuration error before any thread is spawned — not turned into a DNS
/// lookup or a silent bind to something else.
#[tokio::test]
async fn run_percore_rejects_a_bind_address_that_is_not_an_ip() {
    let runtime = tokio::runtime::Handle::current();
    let local = Arc::new(LocalAdapter::new(
        Arc::new(Registry::new()),
        Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
    ));
    let adapter: Arc<dyn Adapter> = local.clone();
    let webhooks = WebhookHandle::null();
    let err = tokio::task::spawn_blocking(move || {
        run(
            base_config("definitely not an address", 4321),
            Some(local),
            adapter,
            webhooks,
            Arc::new(AtomicBool::new(false)),
            runtime,
        )
    })
    .await
    .expect("join")
    .expect_err("an unparseable bind address must fail");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::InvalidInput,
        "an unparseable bind address is a configuration error: {err}"
    );
}

/// When a worker thread cannot bind its `SO_REUSEPORT` listener, `run_percore`
/// must surface that error to the caller rather than joining quietly and
/// reporting success — the process would otherwise "start" with nothing
/// listening.
#[tokio::test]
async fn run_percore_propagates_a_worker_bind_failure() {
    let addr: SocketAddr = format!("{UNBINDABLE}:9").parse().expect("literal address");
    assert!(
        std::net::TcpListener::bind(addr).is_err(),
        "precondition: {addr} must be unbindable on this host, or the test proves nothing"
    );

    let runtime = tokio::runtime::Handle::current();
    let local = Arc::new(LocalAdapter::new(
        Arc::new(Registry::new()),
        Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
    ));
    let adapter: Arc<dyn Adapter> = local.clone();
    let webhooks = WebhookHandle::null();
    let err = tokio::task::spawn_blocking(move || {
        run(
            base_config(UNBINDABLE, 9),
            Some(local),
            adapter,
            webhooks,
            Arc::new(AtomicBool::new(false)),
            runtime,
        )
    })
    .await
    .expect("join")
    .expect_err("a listener that cannot bind must fail the whole fleet");
    assert_ne!(
        err.kind(),
        std::io::ErrorKind::InvalidInput,
        "the address parsed fine; the failure must come from the bind: {err}"
    );
}

/// The sink-less wiring (`local = None`): no sharded broadcast sink is
/// installed, so a channel broadcast has to reach the subscriber through the
/// registry mailbox fallback instead. Delivery is the assertion — this is the
/// only wiring in which that fallback is exercised at all.
#[tokio::test]
async fn broadcasts_reach_subscribers_without_the_sharded_sink() {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a port");
        l.local_addr().expect("reserved").port()
    };
    let local = Arc::new(LocalAdapter::new(
        Arc::new(Registry::new()),
        Arc::new(pylon::adapter::app_registry::AppRegistry::new()),
    ));
    let adapter: Arc<dyn Adapter> = local.clone();
    let shutdown = Arc::new(AtomicBool::new(false));

    let runtime = tokio::runtime::Handle::current();
    let worker_adapter = adapter.clone();
    let worker_shutdown = shutdown.clone();
    let webhooks = WebhookHandle::null();
    let worker = std::thread::spawn(move || {
        let _ = run(
            base_config("127.0.0.1", port),
            // No concrete LocalAdapter ⇒ no broadcast sink, no saturation flag.
            None,
            worker_adapter,
            webhooks,
            worker_shutdown,
            runtime,
        );
    });

    let addr: SocketAddr = format!("127.0.0.1:{port}")
        .parse()
        .expect("literal address");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::net::TcpStream::connect(addr).await.is_err() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the sink-less worker must bind {addr} within 5s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{addr}/app/wiring-key?protocol=7"))
            .await
            .expect("ws handshake");
    assert_eq!(
        next_json(&mut ws).await["event"],
        "pusher:connection_established"
    );

    ws.send(Message::text(
        json!({"event":"pusher:subscribe","data":{"channel":CHANNEL}}).to_string(),
    ))
    .await
    .expect("send subscribe");
    assert_eq!(
        next_json(&mut ws).await["event"],
        "pusher_internal:subscription_succeeded"
    );

    adapter
        .broadcast(
            APP_ID,
            CHANNEL,
            ServerEvent::ChannelEvent {
                channel: CHANNEL.to_string(),
                event: "sinkless-event".to_string(),
                data: json!({"hello": "mailbox"}),
                user_id: None,
            },
            None,
        )
        .await;

    let delivered = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let frame = next_json(&mut ws).await;
            if frame["event"] == "sinkless-event" {
                return frame;
            }
        }
    })
    .await
    .expect("the registry mailbox fallback must still deliver the broadcast");
    assert_eq!(delivered["channel"], CHANNEL);

    shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = worker.join();
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
