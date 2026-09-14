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
// SP11 per-core clustering: the `ClusterBridge` hosts a `RedisAdapter` on a dedicated
// tokio runtime so the SYNC percore workers can fire fire-and-forget cluster commands
// without ever blocking on Redis.
pub mod cluster;
pub mod connection;
pub mod http;
pub mod presence;
pub mod protocol;
pub mod rate;
pub mod server;
// SP9 lean per-core transport. Self-contained RFC 6455 frame codec + the
// readiness-driven worker event loop that drives the v7 protocol dispatch.
pub mod transport;
pub mod user;
pub mod webhook;
pub mod ws;

pub mod auth;

/// Initialize tracing from `RUST_LOG` (defaults to `info`), in the format
/// `PYLON_LOG_FORMAT` selects (`text` by default, or `json`).
pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let mut format = crate::server::config::LogFormat::default();
    crate::server::config::env_parse("PYLON_LOG_FORMAT", &mut format);
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    match format {
        crate::server::config::LogFormat::Text => {
            let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
        }
        crate::server::config::LogFormat::Json => {
            let _ = tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .try_init();
        }
    }
}
