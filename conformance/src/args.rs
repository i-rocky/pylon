//! Hand-rolled CLI parsing for the conformance harness (no clap: three
//! commands and a handful of flags do not justify the dependency).
//!
//! Grammar:
//!
//! ```text
//! pylon-conformance [run] [--sdk <name>] [--scenario <id>] [--smoke]
//!                             [--report <path>] [--port-base <n>] [--pylon-bin <path>]
//! pylon-conformance --list
//! pylon-conformance --audit
//! ```
//!
//! `run` is the default command (also spellable as the literal word `run`);
//! `--list` and `--audit` are selector-style flags so the common invocations
//! stay short. Everything is validated eagerly: unknown flags, missing flag
//! values, a non-numeric or out-of-range `--port-base` (pylon binds
//! `base + 2`, so the base is capped at [`MAX_PORT_BASE`]), or mixing
//! `--list`/`--audit` with run flags all fail with a message naming the
//! offending argument.

use std::path::PathBuf;

/// Default port the plumbing's auth endpoint binds (`port_base + 0`;
/// webhooks get `+1`, pylon `+2`).
pub const DEFAULT_PORT_BASE: u16 = 19800;

/// Highest usable `--port-base`: pylon itself binds `port_base + 2`, which
/// must still fit a `u16` port.
pub const MAX_PORT_BASE: u16 = 65533;

/// Default JSON artifact path (written into the current working directory).
pub const DEFAULT_REPORT: &str = "conformance-report.json";

/// One usage line per command; shown when parsing fails.
pub const USAGE: &str = "usage: pylon-conformance [run] [--sdk <name>] [--scenario <id>] [--smoke] [--report <path>] [--port-base <n>] [--pylon-bin <path>]
       pylon-conformance --list
       pylon-conformance --audit";

/// The parsed command line.
#[derive(Debug, PartialEq)]
pub struct Args {
    pub command: Command,
}

/// What to do. `Run` is the default; field defaults live in [`parse`].
#[derive(Debug, PartialEq)]
pub enum Command {
    Run {
        /// Only scenarios driven by this SDK (e.g. `pusher-js`).
        sdk: Option<String>,
        /// Only this scenario id.
        scenario: Option<String>,
        /// The three-scenario subset C-PUB-SUB + S-TRIGGER +
        /// S-WEBHOOK-VERIFY.
        smoke: bool,
        /// Where the JSON artifact goes.
        report: PathBuf,
        /// First port the harness binds.
        port_base: u16,
        /// Explicit pylon binary (default: `<repo>/target/release/pylon`).
        pylon_bin: Option<PathBuf>,
    },
    List,
    Audit,
}

/// Parse `argv` (WITHOUT the program name) into [`Args`], or a human-facing
/// error message.
pub fn parse(argv: &[&str]) -> Result<Args, String> {
    let mut list = false;
    let mut audit = false;
    let mut run_word = false;
    let mut sdk = None;
    let mut scenario = None;
    let mut smoke = false;
    let mut report: Option<PathBuf> = None;
    let mut port_base: Option<u16> = None;
    let mut pylon_bin: Option<PathBuf> = None;

    let mut i = 0;
    while i < argv.len() {
        let arg = argv[i];
        i += 1;
        match arg {
            "run" => run_word = true,
            "--list" => list = true,
            "--audit" => audit = true,
            "--smoke" => smoke = true,
            "--sdk" => sdk = Some(value(argv, &mut i, "--sdk")?.to_string()),
            "--scenario" => scenario = Some(value(argv, &mut i, "--scenario")?.to_string()),
            "--report" => report = Some(PathBuf::from(value(argv, &mut i, "--report")?)),
            "--port-base" => {
                let raw = value(argv, &mut i, "--port-base")?;
                let parsed: u16 = raw
                    .parse()
                    .map_err(|_| format!("invalid --port-base {raw:?}: expected a port number"))?;
                // pylon binds port_base + 2, so the base itself is capped.
                if parsed > MAX_PORT_BASE {
                    return Err(format!(
                        "invalid --port-base {parsed}: must be <= {MAX_PORT_BASE} (pylon binds port_base + 2)"
                    ));
                }
                port_base = Some(parsed);
            }
            "--pylon-bin" => pylon_bin = Some(PathBuf::from(value(argv, &mut i, "--pylon-bin")?)),
            other => return Err(format!("unknown argument {other:?}")),
        }
    }

    let saw_run_flag = run_word
        || smoke
        || sdk.is_some()
        || scenario.is_some()
        || report.is_some()
        || port_base.is_some()
        || pylon_bin.is_some();
    if list && audit {
        return Err("--list and --audit cannot be combined".to_string());
    }
    if (list || audit) && saw_run_flag {
        return Err(format!(
            "{} takes no run flags (run flags: --sdk/--scenario/--smoke/--report/--port-base/--pylon-bin)",
            if list { "--list" } else { "--audit" }
        ));
    }

    let command = if list {
        Command::List
    } else if audit {
        Command::Audit
    } else {
        Command::Run {
            sdk,
            scenario,
            smoke,
            report: report.unwrap_or_else(|| PathBuf::from(DEFAULT_REPORT)),
            port_base: port_base.unwrap_or(DEFAULT_PORT_BASE),
            pylon_bin,
        }
    };
    Ok(Args { command })
}

/// The value following flag `name` at position `i` (already advanced past the
/// flag itself), or an error naming the flag when it is missing / was the
/// last argument.
fn value<'a>(argv: &'a [&'a str], i: &mut usize, flag: &str) -> Result<&'a str, String> {
    match argv.get(*i) {
        Some(v) => {
            *i += 1;
            Ok(v)
        }
        None => Err(format!("{flag} expects a value")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_run_with_default_report_and_port_base() {
        let Command::Run {
            sdk,
            scenario,
            smoke,
            report,
            port_base,
            pylon_bin,
        } = parse(&[]).unwrap().command
        else {
            panic!("no arguments must mean Run");
        };
        assert_eq!((sdk, scenario, smoke, pylon_bin), (None, None, false, None));
        assert_eq!(report, PathBuf::from(DEFAULT_REPORT));
        assert_eq!(port_base, DEFAULT_PORT_BASE);
    }

    #[test]
    fn parses_every_run_flag() {
        let Command::Run {
            sdk,
            scenario,
            smoke,
            report,
            port_base,
            pylon_bin,
        } = parse(&[
            "run",
            "--sdk",
            "pusher-http-node",
            "--scenario",
            "S-TRIGGER",
            "--smoke",
            "--report",
            "/tmp/x.json",
            "--port-base",
            "20000",
            "--pylon-bin",
            "./pylon",
        ])
        .unwrap()
        .command
        else {
            panic!();
        };
        assert_eq!(sdk.as_deref(), Some("pusher-http-node"));
        assert_eq!(scenario.as_deref(), Some("S-TRIGGER"));
        assert!(smoke);
        assert_eq!(report, PathBuf::from("/tmp/x.json"));
        assert_eq!(port_base, 20000);
        assert_eq!(pylon_bin.as_deref(), Some(std::path::Path::new("./pylon")));
    }

    #[test]
    fn list_and_audit_work_without_the_run_word_but_reject_run_flags() {
        // Flags without the `run` word still select run/list/audit.
        assert!(matches!(
            parse(&["--smoke"]).unwrap().command,
            Command::Run { .. }
        ));
        assert!(matches!(parse(&["--list"]).unwrap().command, Command::List));
        assert!(matches!(
            parse(&["--audit"]).unwrap().command,
            Command::Audit
        ));
        // But the selector flags are exclusive with run flags and each other.
        assert!(parse(&["--list", "--audit"]).is_err());
        assert!(parse(&["--list", "--smoke"]).is_err());
        assert!(parse(&["--audit", "--sdk", "pusher-js"]).is_err());
        assert!(parse(&["run", "--list"]).is_err());
    }

    #[test]
    fn rejects_unknown_flags_missing_values_and_bad_ports() {
        assert_eq!(
            parse(&["--bogus"]).unwrap_err(),
            "unknown argument \"--bogus\""
        );
        assert_eq!(parse(&["--sdk"]).unwrap_err(), "--sdk expects a value");
        assert!(parse(&["--port-base", "nope"])
            .unwrap_err()
            .contains("invalid --port-base"));
        assert!(
            parse(&["--port-base", "99999"]).is_err(),
            "u16 overflow is an error"
        );
        // base + 2 must fit u16: 65534/65535 are rejected at parse time,
        // 65533 is the highest accepted base.
        assert!(parse(&["--port-base", "65534"]).is_err());
        assert!(parse(&["--port-base", "65535"]).is_err());
        assert!(parse(&["--port-base", "65533"]).is_ok());
    }
}
