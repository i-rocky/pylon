//! Lean per-core WebSocket transport (SP9).
//!
//! This module owns the raw RFC 6455 frame layer for the new per-connection
//! transport. Unlike `tokio-tungstenite`, it does **not** eagerly allocate a
//! large (128 KiB) read buffer per connection: framing operates over a
//! caller-owned [`bytes::BytesMut`] that grows lazily, and parsed payloads are
//! cheap `Bytes` slices into that buffer.
//!
//! [`frame`] is the RFC 6455 codec; [`conn`] is the per-connection state +
//! non-blocking read/write that the worker event loop drives. The event loop
//! itself is built in later SP9 tasks.

pub mod conn;
pub mod frame;
pub mod handshake;
pub mod worker;

use crate::adapter::Adapter;
use crate::app::AppManager;
use crate::server::config::ServerConfig;
use crate::webhook::WebhookHandle;
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use worker::{DispatchEnv, Mode, WorkerConfig};

/// Run the per-core (`PYLON_TRANSPORT=percore`) worker as the actual server.
///
/// Takes the already-built shared pieces (the same ones `main`/`AppState`
/// assemble), builds a [`DispatchEnv`], and drives a single [`worker::run`]
/// event loop on the calling thread bound to `config.bind:config.port`. Blocks
/// until `shutdown` is observed (or a fatal bind/poll error occurs).
///
/// Single worker for now; multi-worker `SO_REUSEPORT` fan-out is a later task.
/// REST handling in percore mode is deferred — the worker closes non-WS
/// connections (`HeadResult::Rest`), so this enables WS connection + the full
/// v7 protocol, which is what the benchmark exercises next.
pub fn run_percore(
    config: ServerConfig,
    apps: Arc<dyn AppManager>,
    adapter: Arc<dyn Adapter>,
    conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
    webhooks: WebhookHandle,
    shutdown: Arc<AtomicBool>,
) -> std::io::Result<()> {
    let addr: std::net::SocketAddr = format!("{}:{}", config.bind, config.port)
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let env = DispatchEnv {
        apps,
        adapter,
        limits: config.limits(),
        activity_timeout: config.activity_timeout,
        strict_protocol: config.strict_protocol,
        conn_counts,
        webhooks,
    };

    // WS frame cap: bound a single inbound frame's payload. The configured
    // event-payload limit is small (KiB), so use a 1 MiB frame ceiling that
    // comfortably covers any legitimate Pusher frame while bounding abuse.
    let max_payload = config.max_event_payload_bytes.max(1 << 20);
    // Per-connection outbound high-water before a backpressure close (4 MiB).
    let high_water = 4 << 20;

    let cfg = WorkerConfig {
        addr,
        max_payload,
        high_water,
        mode: Mode::Dispatch(Arc::new(env)),
    };

    tracing::info!(
        %addr,
        "pylon percore worker listening (PYLON_TRANSPORT=percore)"
    );
    worker::run(cfg, shutdown)
}
