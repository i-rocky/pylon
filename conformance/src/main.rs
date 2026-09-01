// Task 1 scaffold: the catalog and plumbing have no runtime consumer yet, so
// they are only compiled for tests (clippy still covers them via
// --all-targets). Later tasks replace this with plain `mod catalog;` /
// `mod plumbing;` alongside `mod server;` (server::wait_ready will call
// plumbing::http_get).
#[cfg(test)]
mod catalog;

#[cfg(test)]
mod plumbing;

// Same Task-1 precedent: `server` is only consumed by tests so far; Task 6
// (runner wiring) flips this to a plain `mod server;` next to the others.
#[cfg(test)]
mod server;

// Task-4 adapter runner: `run`/`AdapterEnv` gain their runtime consumer in
// Task 5 (the orchestrator); until then the module is test-gated like the
// ones above — Task 6 flips it together with the rest.
#[cfg(test)]
mod adapter;

fn main() {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::server::{self, render_apps_json, AppSpec};

    #[test]
    fn apps_json_renders_both_apps_with_webhook() {
        let apps = vec![
            AppSpec::conformance_main("http://127.0.0.1:9902/hooks"),
            AppSpec::conformance_disabled(),
        ];
        let json = render_apps_json(&apps);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v.as_array().unwrap().len(), 2);
        let main = &v[0];
        assert_eq!(main["key"], "cf-key-main");
        assert_eq!(main["enabled"], true);
        assert_eq!(main["client_messages_enabled"], true);
        assert_eq!(main["subscription_count_enabled"], true);
        assert_eq!(main["webhooks"][0]["url"], "http://127.0.0.1:9902/hooks");
        assert!(
            main["webhooks"][0]["event_types"]
                .as_array()
                .unwrap()
                .iter()
                .count()
                >= 1
        );
        assert_eq!(v[1]["enabled"], false);
        assert_eq!(v[1]["key"], "cf-key-disabled");
    }

    #[test]
    #[ignore = "needs ../target/release/pylon; run in CI or after cargo build --release"]
    fn spawns_and_shuts_down_real_pylon() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut s = server::spawn_pylon(
                "../target/release/pylon",
                19801,
                &[AppSpec::conformance_main("http://127.0.0.1:1/hooks")],
            )
            .await
            .unwrap();
            s.wait_ready(Duration::from_secs(30)).await.unwrap();
            s.shutdown().await;
        });
    }
}
