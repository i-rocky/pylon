use clap::{Parser, ValueEnum};

#[derive(Parser, Clone)]
#[command(name = "pylon-load", about = "Pusher-protocol load-test harness")]
pub struct Cli {
    /// WS URL, e.g. ws://127.0.0.1:7000/app/<app-key>
    #[arg(long)]
    pub url: String,
    /// Second WS URL for the cluster scenario (node B)
    #[arg(long)]
    pub url_b: Option<String>,
    /// REST base, e.g. http://127.0.0.1:7000
    #[arg(long, default_value = "http://127.0.0.1:7000")]
    pub rest: String,
    #[arg(long)]
    pub app_id: String,
    #[arg(long)]
    pub key: String,
    #[arg(long)]
    pub secret: String,
    #[arg(long, value_enum, default_value_t = Scenario::Fanout)]
    pub scenario: Scenario,
    /// Number of connections
    #[arg(long, default_value_t = 1000)]
    pub conns: usize,
    /// Number of channels (channels scenario)
    #[arg(long, default_value_t = 1)]
    pub channels: usize,
    /// Publish rate (events/sec)
    #[arg(long, default_value_t = 10)]
    pub rate: u64,
    /// Number of concurrent publishers (fanout scenario); each runs the publish loop
    /// at `rate` events/sec, all fanning out to the same channel.
    #[arg(long, default_value_t = 1)]
    pub publishers: usize,
    /// Measured duration (seconds)
    #[arg(long, default_value_t = 10)]
    pub secs: u64,
    /// Connection ramp (new conns/sec; 0 = all at once)
    #[arg(long, default_value_t = 2000)]
    pub ramp_per_sec: usize,
    /// Private channels (sign the subscribe)
    #[arg(long, default_value_t = false)]
    pub private: bool,
    /// Server PID to sample CPU/RSS (optional)
    #[arg(long)]
    pub server_pid: Option<u32>,
    /// Comma-separated client source IPs to spread sockets across (ephemeral-port headroom),
    /// e.g. 127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4
    #[arg(long, value_delimiter = ',', default_value = "127.0.0.1")]
    pub client_ips: Vec<String>,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    Connect,
    Fanout,
    Channels,
    Cluster,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;
    use clap::CommandFactory;

    const FULL: [&str; 9] = [
        "pylon-load",
        "--url",
        "ws://127.0.0.1:7000/app/k3y",
        "--app-id",
        "an-app",
        "--key",
        "k3y",
        "--secret",
        "s3cret",
    ];

    #[test]
    fn starting_without_the_app_identity_is_refused() {
        let Err(err) = Cli::try_parse_from(["pylon-load"]) else {
            panic!("the harness must not start without an app identity");
        };
        assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
        let rendered = err.to_string();
        for flag in ["--url", "--app-id", "--key", "--secret"] {
            assert!(rendered.contains(flag), "{flag} must be named as missing");
        }
    }

    #[test]
    fn starting_without_the_secret_alone_is_refused() {
        let Err(err) = Cli::try_parse_from(&FULL[..7]) else {
            panic!("the harness must not start without a secret");
        };
        assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
        assert!(err.to_string().contains("--secret"));
    }

    #[test]
    fn the_full_app_identity_parses_to_what_was_passed() {
        let Ok(cli) = Cli::try_parse_from(FULL) else {
            panic!("a full app identity must parse");
        };
        assert_eq!(cli.url, "ws://127.0.0.1:7000/app/k3y");
        assert_eq!(cli.app_id, "an-app");
        assert_eq!(cli.key, "k3y");
        assert_eq!(cli.secret, "s3cret");
    }

    #[test]
    fn the_command_surface_carries_no_credential_literal() {
        let rendered = Cli::command().render_long_help().to_string();
        assert!(
            !rendered.contains("app-secret"),
            "no default may put a secret in the CLI surface"
        );
        assert!(
            !rendered.contains("/app/app-key"),
            "no default may put an app key in the CLI surface"
        );
    }
}
