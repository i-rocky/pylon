//! Report aggregation: fold a run's [`Verdict`]s into the three artifacts the
//! harness emits — the human matrix printed at the end of a run, the
//! pretty-JSON artifact written next to it, and the process exit code.
//!
//! Plane grouping keys off the catalog ([`catalog::find`]), the single source
//! of truth for which plane a scenario belongs to and which SDK drives it.

use std::path::Path;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

use crate::adapter::Verdict;
use crate::catalog::{self, Plane};

/// Everything about a run that isn't a per-scenario result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunMeta {
    /// RFC 3339 timestamp of the run (stamped by the orchestrator).
    pub timestamp: String,
    /// Version of the pylon server under test.
    pub pylon_version: String,
    /// Each adapter's `version`-mode output, as `(sdk, version)` pairs.
    pub sdk_versions: Vec<(String, String)>,
    /// What the run targeted (e.g. `local`).
    pub target: String,
}

/// A whole conformance run: metadata plus one verdict per scenario, in
/// execution order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Report {
    pub run: RunMeta,
    pub results: Vec<Verdict>,
}

/// Render the human-readable matrix: the client-plane block first, then the
/// server-plane block (catalog order), one `ID | SDK | VERDICT | <ms>ms` row
/// per scenario, then a blank line and the summary
/// `P passed, F failed, S skipped — <sdk>@<ver>, ..., pylon <version>`
/// (the sdk segment is omitted entirely when `sdk_versions` is empty — no
/// dangling `— ,`).
///
/// A scenario id missing from the catalog cannot occur through the
/// orchestrator ([`catalog::audit`] rejects such bindings up front); if one
/// appears anyway it is grouped LAST under a plain `unknown` heading (SDK
/// column `unknown`) rather than silently dropped, so it stays visible.
pub fn render_human(report: &Report) -> String {
    let mut client: Vec<(&Verdict, &str)> = Vec::new();
    let mut server: Vec<(&Verdict, &str)> = Vec::new();
    let mut unknown: Vec<(&Verdict, &str)> = Vec::new();
    for v in &report.results {
        // One catalog lookup per verdict: the plane decides the block and the
        // catalog (not the verdict) carries the SDK name.
        match catalog::find(&v.scenario) {
            Some(s) => match s.plane {
                Plane::Client => client.push((v, s.sdk)),
                Plane::Server => server.push((v, s.sdk)),
            },
            None => unknown.push((v, "unknown")),
        }
    }

    let mut out = String::new();
    push_block(&mut out, "Client plane", &client);
    push_block(&mut out, "Server plane", &server);
    push_block(&mut out, "unknown", &unknown);

    let passed = count_verdicts(report, "pass");
    let failed = count_verdicts(report, "fail");
    let skipped = count_verdicts(report, "skip");
    let sdks: Vec<String> = report
        .run
        .sdk_versions
        .iter()
        .map(|(sdk, version)| format!("{sdk}@{version}"))
        .collect();
    // The sdk segment only renders when there is one — an empty table would
    // otherwise leave a dangling `— , pylon ...`.
    let sdk_segment = if sdks.is_empty() {
        format!("pylon {}", report.run.pylon_version)
    } else {
        format!("{}, pylon {}", sdks.join(", "), report.run.pylon_version)
    };
    out.push('\n');
    out.push_str(&format!(
        "{passed} passed, {failed} failed, {skipped} skipped — {sdk_segment}\n"
    ));
    out
}

/// Append one plane's heading and rows, unless the block is empty.
fn push_block(out: &mut String, heading: &str, rows: &[(&Verdict, &str)]) {
    if rows.is_empty() {
        return;
    }
    out.push_str(heading);
    out.push('\n');
    for (v, sdk) in rows {
        out.push_str(&format!(
            "{} | {} | {} | {}ms\n",
            v.scenario, sdk, v.verdict, v.duration_ms
        ));
    }
}

/// How many results carry exactly this verdict string.
fn count_verdicts(report: &Report, verdict: &str) -> usize {
    report
        .results
        .iter()
        .filter(|v| v.verdict == verdict)
        .count()
}

/// Write the machine-readable artifact: pretty JSON with a trailing newline —
/// the same artifact convention as [`crate::adapter::AdapterEnv::write_to`].
pub fn write_json(report: &Report, path: &Path) -> anyhow::Result<()> {
    let mut json = serde_json::to_string_pretty(report)?;
    json.push('\n');
    std::fs::write(path, json).with_context(|| format!("write {}", path.display()))
}

/// The harness exit code: 0 iff every non-skip verdict is `pass`. Skips do
/// not fail a run; a `fail` — or any verdict string the contract does not
/// define — does.
pub fn exit_code(report: &Report) -> i32 {
    let all_ok = report
        .results
        .iter()
        .all(|v| v.verdict == "pass" || v.verdict == "skip");
    if all_ok {
        0
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::Verdict;

    #[test]
    fn human_report_groups_and_summarizes() {
        let r = Report {
            run: RunMeta {
                timestamp: "2026-09-01T00:00:00Z".into(),
                pylon_version: "v0.3.0".into(),
                sdk_versions: vec![
                    ("pusher-js".into(), "8.4.0".into()),
                    ("pusher-http-node".into(), "5.2.0".into()),
                ],
                target: "local".into(),
            },
            results: vec![
                Verdict {
                    scenario: "C-ESTABLISH".into(),
                    verdict: "pass".into(),
                    observations: Default::default(),
                    error: None,
                    duration_ms: 120,
                },
                Verdict {
                    scenario: "C-ENC-SUB".into(),
                    verdict: "skip".into(),
                    observations: Default::default(),
                    error: Some("no WebCrypto".into()),
                    duration_ms: 0,
                },
                Verdict {
                    scenario: "S-TRIGGER".into(),
                    verdict: "fail".into(),
                    observations: Default::default(),
                    error: Some("boom".into()),
                    duration_ms: 9,
                },
            ],
        };
        let text = render_human(&r);
        assert!(text.contains("C-ESTABLISH | pusher-js | pass"));
        assert!(text.find("Client plane").unwrap() < text.find("Server plane").unwrap());
        assert!(text.contains("1 passed, 1 failed, 1 skipped"));
        assert!(text.contains("pusher-js@8.4.0"));
        assert_eq!(exit_code(&r), 1);
        let mut ok = r.clone();
        ok.results.truncate(1);
        ok.results[0].verdict = "pass".into();
        assert_eq!(exit_code(&ok), 0);
    }

    #[test]
    fn exit_code_skips_do_not_fail_but_unrecognized_verdicts_do() {
        let mk = |verdicts: &[&str]| Report {
            run: RunMeta {
                timestamp: String::new(),
                pylon_version: String::new(),
                sdk_versions: vec![],
                target: String::new(),
            },
            results: verdicts
                .iter()
                .map(|v| verdict_row("C-ESTABLISH", v, 1))
                .collect(),
        };
        assert_eq!(exit_code(&mk(&[])), 0);
        assert_eq!(exit_code(&mk(&["skip", "skip"])), 0);
        assert_eq!(exit_code(&mk(&["pass", "skip"])), 0);
        assert_eq!(exit_code(&mk(&["pass", "garbage"])), 1);
    }

    #[test]
    fn write_json_round_trips_pretty_artifact() {
        let r = Report {
            run: RunMeta {
                timestamp: "2026-09-01T00:00:00Z".into(),
                pylon_version: "v0.3.0".into(),
                sdk_versions: vec![("pusher-js".into(), "8.4.0".into())],
                target: "local".into(),
            },
            results: vec![verdict_row("C-ESTABLISH", "pass", 120)],
        };
        let dir = std::env::temp_dir().join("cf-report-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("report.json");
        write_json(&r, &path).unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.ends_with("}\n"), "pretty JSON with trailing newline");
        assert!(
            raw.contains("\n  \"run\""),
            "pretty-printed (indented top-level keys)"
        );
        assert!(
            raw.find("\"run\"").unwrap() < raw.find("\"results\"").unwrap(),
            "run metadata is serialized before the results array"
        );
        assert_eq!(serde_json::from_str::<Report>(&raw).unwrap(), r);
    }

    #[test]
    fn empty_sdk_versions_leave_no_dangling_separator() {
        // The run is over before any adapter reported a version (e.g. a
        // single-scenario run): the summary must go straight to `pylon ...`.
        let r = Report {
            run: RunMeta {
                timestamp: String::new(),
                pylon_version: "0.3.0".into(),
                sdk_versions: vec![],
                target: "local".into(),
            },
            results: vec![verdict_row("C-ESTABLISH", "pass", 120)],
        };
        let text = render_human(&r);
        assert!(
            text.contains("1 passed, 0 failed, 0 skipped — pylon 0.3.0"),
            "summary renders without a dangling separator: {}",
            text.trim_end()
        );
        assert!(!text.contains("— ,"));
    }

    #[test]
    fn unknown_scenarios_group_last_under_plain_unknown_heading() {
        let r = Report {
            run: RunMeta {
                timestamp: String::new(),
                pylon_version: "v0.3.0".into(),
                sdk_versions: vec![],
                target: "local".into(),
            },
            results: vec![
                verdict_row("S-TRIGGER", "pass", 5),
                verdict_row("X-NOPE", "fail", 6),
            ],
        };
        let text = render_human(&r);
        assert!(text.contains("S-TRIGGER | pusher-http-node | pass | 5ms"));
        let unknown_heading = text.find("unknown\n").expect("plain `unknown` heading");
        assert!(
            unknown_heading > text.find("S-TRIGGER").unwrap(),
            "unknown block renders after the server block"
        );
        assert!(text.contains("X-NOPE | unknown | fail | 6ms"));
    }

    fn verdict_row(scenario: &str, verdict: &str, duration_ms: u64) -> Verdict {
        Verdict {
            scenario: scenario.into(),
            verdict: verdict.into(),
            observations: Default::default(),
            error: None,
            duration_ms,
        }
    }
}
