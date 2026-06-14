// `deny` (not `forbid`) so the single maintainer-approved unsafe site — the fd
// transfer in `transport::rest` that moves an accepted connection from the mio
// worker to the tokio/axum REST plane (SP9) — can opt in via a local
// `#[allow(unsafe_code)]` with a `// SAFETY:` justification. `forbid` cannot be
// overridden by a local `allow`; every other module remains unsafe-free.
#![deny(unsafe_code)]
//! pylon — a Pusher-compatible real-time WebSocket server.

pub mod adapter;
pub mod app;
pub mod channel;
pub mod connection;
pub mod http;
pub mod presence;
pub mod protocol;
pub mod server;
// SP9 lean per-core transport. Self-contained RFC 6455 frame codec + the
// readiness-driven worker event loop that drives the v7 protocol dispatch.
#[allow(dead_code)]
pub mod transport;
pub mod user;
pub mod webhook;
pub mod ws;

pub mod auth;

/// Initialize tracing from `RUST_LOG` (defaults to `info`).
pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .try_init();
}
