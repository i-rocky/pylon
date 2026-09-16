use pylon_load::ceiling::child::{default_pylon_bin, AppsFile, ChildOpts, PylonChild};

#[tokio::test]
async fn spawns_pins_reads_and_tears_down() {
    let apps = AppsFile::create_temp().unwrap();
    let apps_path = apps.path().to_owned();
    let opts = ChildOpts {
        pylon_bin: default_pylon_bin(),
        port: 7700,
        workers: 2,
        cores: "0-1".into(),
        apps_path: apps_path.clone(),
    };
    let child = PylonChild::spawn(&opts).await.expect("spawn");
    let pid = child.pid();
    // listening
    assert!(std::net::TcpStream::connect("127.0.0.1:7700").is_ok());
    // /proc readable
    assert!(child.rss_bytes().unwrap() > 0);
    assert!(child.cpu_ticks().is_some());
    drop(child);
    // after drop, the pid is gone (poll briefly)
    std::thread::sleep(std::time::Duration::from_millis(500));
    assert!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "child not reaped"
    );
    assert!(
        std::path::Path::new(&apps_path).exists(),
        "the child must not remove the apps file"
    );
    drop(apps);
    assert!(
        !std::path::Path::new(&apps_path).exists(),
        "temp apps not cleaned"
    );
}
