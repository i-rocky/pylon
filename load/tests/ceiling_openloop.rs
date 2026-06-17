use pylon_load::ceiling::child::{ChildOpts, PylonChild, default_pylon_bin, write_temp_apps};
use pylon_load::ceiling::openloop::publish_openloop;
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
        "http://127.0.0.1:7702".into(),
        "app".into(),
        "app-key".into(),
        "app-secret".into(),
        vec!["c0".into(), "c1".into(), "c2".into(), "c3".into(), "c4".into()],
        500,   // target_rate
        128,   // max_inflight
        3,     // secs
        counters,
    )
    .await;

    // 3. Assertions: open-loop should achieve ~target rate (>= 80% of 500*3 = 1500)
    assert!(
        result.attempted >= 1200,
        "open-loop should attempt >= 1200 publishes in 3s at 500 msg/s, got {}",
        result.attempted
    );
    assert!(result.succeeded > 0, "at least some publishes should return 200, got 0");

    // child drops here — teardown is automatic
    drop(child);
}
