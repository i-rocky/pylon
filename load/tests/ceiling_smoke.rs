use pylon_load::ceiling::child::{default_pylon_bin, AppsFile, ChildOpts, PylonChild};
use pylon_load::ceiling::{conn_ramp, rate_ramp, spec};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ceiling_smoke_produces_sane_envelope() {
    let spec = spec::detect();
    let apps = AppsFile::create_temp().unwrap();
    let opts = ChildOpts {
        pylon_bin: default_pylon_bin(),
        port: 7710,
        workers: 2,
        cores: "0-1".into(),
        apps_path: apps.path().to_owned(),
    };
    let child = PylonChild::spawn(&opts).await.expect("spawn pylon child");
    let url = format!("ws://127.0.0.1:7710/app/{}", apps.credentials().key);

    // Connection-ceiling sweep: capped at 300 conns, high mem ceiling (won't trip → stops on MaxConns).
    let conn = conn_ramp::run(
        &child,
        &spec,
        &conn_ramp::ConnRampOpts {
            url: url.clone(),
            key: apps.credentials().key.clone(),
            secret: apps.credentials().secret.clone(),
            conn_batch: 100,
            max_conns: 300,
            mem_ceiling_pct: 95,
            client_ips: vec!["127.0.0.1".into(), "127.0.0.2".into()],
            fail_threshold: 100,
        },
    )
    .await;
    assert!(
        conn.max_conns >= 200,
        "expected >=200 conns, got {}",
        conn.max_conns
    );
    assert!(conn.bytes_per_conn > 0, "bytes_per_conn should be >0");

    // Throughput-ceiling sweep: small, short. Stops on MaxRate (p99 budget large).
    let tput = rate_ramp::run(
        &child,
        &rate_ramp::RateRampOpts {
            url,
            app_id: apps.app_id().to_owned(),
            rest: "http://127.0.0.1:7710".into(),
            key: apps.credentials().key.clone(),
            secret: apps.credentials().secret.clone(),
            tput_conns: 200,
            channels: 10,
            rate_start: 50,
            rate_step: 50,
            max_rate: 100,
            p99_budget_ms: 5000,
            max_inflight: 64,
            step_secs: 2,
            server_cores: 2,
            client_ips: vec!["127.0.0.1".into(), "127.0.0.2".into()],
        },
    )
    .await;
    assert!(
        tput.best.delivered_per_s > 0,
        "expected some deliveries, got {}",
        tput.best.delivered_per_s
    );
    // child drops here → process group torn down
}
