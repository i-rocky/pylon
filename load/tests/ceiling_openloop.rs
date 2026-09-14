use pylon_load::ceiling::child::{default_pylon_bin, write_temp_apps, ChildOpts, PylonChild};
use pylon_load::ceiling::openloop::{publish_openloop, OpenLoopConfig};
use pylon_load::metrics::Counters;
use std::sync::Arc;

#[tokio::test]
async fn openloop_reaches_target_rate() {
    // 1. Spawn a real pylon child.
    let apps_path = write_temp_apps().unwrap();
    let opts = ChildOpts {
        pylon_bin: default_pylon_bin(),
        port: 7702,
        workers: 2,
        cores: "0-1".into(),
        apps_path,
    };
    let child = PylonChild::spawn(&opts).await.expect("spawn pylon child");

    // 2. Run the open-loop publisher for 3 seconds at 500 msg/s.
    let counters = Arc::new(Counters::default());
    let result = publish_openloop(
        OpenLoopConfig {
            rest: "http://127.0.0.1:7702".into(),
            app_id: "app".into(),
            key: "app-key".into(),
            secret: "app-secret".into(),
            channels: vec![
                "c0".into(),
                "c1".into(),
                "c2".into(),
                "c3".into(),
                "c4".into(),
            ],
            target_rate: 500,
            max_inflight: 128,
            secs: 3,
        },
        counters,
        std::time::Instant::now(), // epoch (this test asserts rate, not latency)
    )
    .await;

    // 3. Assertions: open-loop should achieve ~target rate (>= 80% of 500*3 = 1500)
    assert!(
        result.attempted >= 1200,
        "open-loop should attempt >= 1200 publishes in 3s at 500 msg/s, got {}",
        result.attempted
    );
    assert!(
        result.succeeded > 0,
        "at least some publishes should return 200, got 0"
    );

    // child drops here — teardown is automatic
    drop(child);
}
