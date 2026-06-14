//! pylon-load — Pusher-protocol load-test harness.
use clap::Parser;
use pylon_load::cli::{Cli, Scenario};
use pylon_load::scenario;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").try_init().ok();
    let cli = Cli::parse();
    match cli.scenario {
        Scenario::Fanout => scenario::fanout::run(&cli).await,
        Scenario::Connect => scenario::connect::run(&cli).await,
        Scenario::Channels => scenario::channels::run(&cli).await,
        Scenario::Cluster => scenario::cluster::run(&cli).await,
    }
}
