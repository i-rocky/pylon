use pylon::adapter::app_registry::AppRegistry;
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::{AppLookup, AppLookupError, AppManager};
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::server::shutdown::supervise_fleet;
use pylon::webhook::WebhookHandle;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct PanickingAppStore;

#[async_trait::async_trait]
impl AppManager for PanickingAppStore {
    async fn by_key(&self, _key: &str) -> Result<AppLookup, AppLookupError> {
        Ok(AppLookup::NotFound)
    }

    async fn by_id(&self, _id: &str) -> Result<AppLookup, AppLookupError> {
        Ok(AppLookup::NotFound)
    }

    fn by_key_cached(&self, key: &str) -> Option<Result<AppLookup, AppLookupError>> {
        panic!("app store panicked resolving {key} on a percore worker");
    }
}

struct StopFleetOnDrop(Arc<AtomicBool>);

impl Drop for StopFleetOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve an ephemeral port");
    listener.local_addr().expect("reserved port").port()
}

async fn await_listener(port: u16) {
    for _ in 0..250 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the percore fleet never bound 127.0.0.1:{port}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_panic_ends_the_server_with_an_error_before_any_shutdown_signal() {
    let port = free_port();
    let config = ServerConfig {
        bind: "127.0.0.1".into(),
        port,
        workers: 2,
        ..Default::default()
    };
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(
        Arc::new(Registry::new()),
        Arc::new(AppRegistry::new()),
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let _stop_fleet = StopFleetOnDrop(shutdown.clone());
    let draining = AtomicBool::new(false);
    let fleet_shutdown = shutdown.clone();
    let runtime = tokio::runtime::Handle::current();
    let fleet = tokio::task::spawn_blocking(move || {
        pylon::transport::run_percore(
            config,
            Arc::new(PanickingAppStore),
            adapter,
            Arc::new(Default::default()),
            Arc::new(AppRegistry::new()),
            Arc::new(AtomicUsize::new(0)),
            WebhookHandle::null(),
            None,
            fleet_shutdown,
            None,
            false,
            None,
            None,
            runtime,
        )
    });
    await_listener(port).await;

    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/app/any-key?protocol=7")),
    )
    .await;

    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        supervise_fleet(
            fleet,
            std::future::pending(),
            &draining,
            &shutdown,
            Duration::ZERO,
        ),
    )
    .await
    .expect("a panicked worker must end the server without waiting for a shutdown signal");

    let error = outcome.expect_err("a panicked worker must end the server with an error");
    assert!(
        error.to_string().contains("percore worker thread panicked"),
        "the error must name the worker panic, got: {error:#}"
    );
}
