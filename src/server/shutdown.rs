//! Graceful-shutdown future: resolves on Ctrl-C or SIGTERM.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;

pub async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    tracing::info!("shutdown signal received");
}

pub async fn supervise_fleet(
    mut fleet: JoinHandle<std::io::Result<()>>,
    signal: impl Future<Output = ()>,
    draining: &AtomicBool,
    shutdown: &AtomicBool,
    predrain: Duration,
) -> anyhow::Result<()> {
    tokio::select! {
        exited = &mut fleet => return Ok(exited??),
        () = signal => {}
    }
    draining.store(true, Ordering::SeqCst);
    tokio::time::sleep(predrain).await;
    shutdown.store(true, Ordering::SeqCst);
    fleet.await??;
    Ok(())
}
