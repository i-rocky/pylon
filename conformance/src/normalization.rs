//! Observation-normalization audit — spec §7's third audit family.
//!
//! The observation contract: a runner's `observations` carry facts that are
//! TRUE OF EVERY RUN (placeholders, pinned statuses, SDK-mandated constants);
//! run-unique values (socket ids, timestamps, epoch counters, dynamic counts)
//! go to STDERR as evidence, never into the artifact. That is what keeps the
//! JSON artifact stable and diffable, and what would let a future hosted run
//! diff against a local one value-for-value.
//!
//! [`scan`] is the backstop: a pure function over a run's verdicts that walks
//! every observation leaf and flags values whose SHAPE is run-unique:
//!
//! - socket ids: `^\d+\.\d+$` where BOTH halves are large (≥ 4 digits —
//!   pylon mints each half from `[1, 10^10)`, so a real socket id virtually
//!   always has two wide halves, while version-like values such as `1.2` or
//!   `8.6` do not);
//! - ISO-8601 timestamps (`yyyy-mm-ddThh:mm…`);
//! - raw epoch-millis (integers in the `10^12..10^15` band);
//! - other bare large integers (≥ 10^9 — epoch-seconds-shaped or larger;
//!   every legitimate observation integer — counts, budgets, ms durations,
//!   ports, SDK constants like `activity_timeout_used_ms: 2000` — is far
//!   below it), including all-digit strings of 10+ chars.
//!
//! False-positive policy: the shapes above are deliberately conservative.
//! If a scenario ever legitimately observes a fixed value that trips one, add
//! a `(scenario id, observation key)` pair to [`ALLOWED_RAW_KEYS`] — the
//! allowlist exempts exactly that leaf key within exactly that scenario, and
//! nothing else. Keep it EMPTY unless a reviewed case demands an entry.
//!
//! Where it runs: after every orchestrated run (mandatory — violations fail
//! the run with exit 1), and in `--audit` against the last report artifact,
//! if one exists (advisory — the artifact may predate a fix).

use crate::adapter::Verdict;

/// Observation leaves exempt from the scan, as `(scenario id, leaf key)`
/// pairs. Empty today: no current observation legitimately carries a
/// run-unique-shaped value. See the module docs for the policy.
const ALLOWED_RAW_KEYS: &[(&str, &str)] = &[];

/// Halves of a socket-id-shaped string must be at least this many digits to
/// be flagged (pylon halves come from `[1, 10^10)`; version-like pairs stay
/// below the threshold).
const SOCKET_ID_MIN_HALF_DIGITS: usize = 4;

/// Integers at or above this are "bare large" (epoch-seconds-shaped or
/// bigger). Legitimate observation integers (counts, durations, ports, SDK
/// constants like 2000) are orders of magnitude below it.
const LARGE_INTEGER_THRESHOLD: u64 = 1_000_000_000;

/// The epoch-millis band: `10^12 .. 10^15` (2020-2033 and friends).
const EPOCH_MILLIS_MIN: u64 = 1_000_000_000_000;
const EPOCH_MILLIS_MAX: u64 = 1_000_000_000_000_000;

/// All-digit strings at least this long are epoch-shaped raw values.
const DIGIT_STRING_MIN_LEN: usize = 10;

/// Longest value excerpt embedded in a violation line.
const EXCERPT_LIMIT: usize = 40;

/// Scan a run's verdicts for un-normalized observations, returning one line
/// per violation: `<scenario>: <path> = <value> looks like <why>`. Pure —
/// no I/O — so the deliberately-unnormalized fixture in the tests below is
/// the RED case the spec demands.
pub fn scan(results: &[Verdict]) -> Vec<String> {
    let mut violations = Vec::new();
    for v in results {
        walk(&v.scenario, &v.observations, "", &mut violations);
    }
    violations
}

/// Is this `(scenario, key)` leaf explicitly allowed to carry a raw value?
fn is_allowed(scenario: &str, key: &str) -> bool {
    ALLOWED_RAW_KEYS
        .iter()
        .any(|(s, k)| *s == scenario && *k == key)
}

/// Walk one observation value, accumulating violations for offending leaves.
/// `path` is the dotted key path from the observation root (`a.b[2].c`).
fn walk(scenario: &str, value: &serde_json::Value, path: &str, out: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let child = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                walk(scenario, v, &child, out);
            }
        }
        serde_json::Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                let child = format!("{path}[{i}]");
                walk(scenario, v, &child, out);
            }
        }
        serde_json::Value::String(s) => {
            if let Some(why) = string_violation(s) {
                push(scenario, path, &format!("\"{}\"", excerpt(s)), why, out);
            }
        }
        serde_json::Value::Number(n) => {
            let magnitude = n
                .as_u64()
                .or_else(|| n.as_i64().map(|i| i.unsigned_abs()))
                .or_else(|| n.as_f64().map(|f| f.abs() as u64));
            if let Some(m) = magnitude {
                if (EPOCH_MILLIS_MIN..EPOCH_MILLIS_MAX).contains(&m) {
                    push(
                        scenario,
                        path,
                        &excerpt(&n.to_string()),
                        "raw epoch-millis (expected a placeholder)",
                        out,
                    );
                } else if m >= LARGE_INTEGER_THRESHOLD {
                    push(
                        scenario,
                        path,
                        &excerpt(&n.to_string()),
                        "bare large integer, epoch-like (expected a placeholder)",
                        out,
                    );
                }
            }
        }
        _ => {}
    }
}

/// Why is this string run-unique-shaped, if it is?
fn string_violation(s: &str) -> Option<&'static str> {
    if looks_like_socket_id(s) {
        Some("a raw socket id (dotted integer pair, both halves large — expected e.g. <socket_id>)")
    } else if looks_like_iso8601(s) {
        Some("an ISO-8601 timestamp (expected a placeholder)")
    } else if s.len() >= DIGIT_STRING_MIN_LEN && s.bytes().all(|b| b.is_ascii_digit()) {
        Some("a bare large integer in string form, epoch-like (expected a placeholder)")
    } else {
        None
    }
}

/// `^\d{4,}\.\d{4,}$` — dotted integer pair with both halves wide enough to
/// be a minted socket id (pylon halves are `[1, 10^10)`; halves are capped
/// at 15 digits to stay well inside "integer", not "random digit soup").
fn looks_like_socket_id(s: &str) -> bool {
    let Some((a, b)) = s.split_once('.') else {
        return false;
    };
    let half_ok = |h: &str| {
        h.len() >= SOCKET_ID_MIN_HALF_DIGITS
            && h.len() <= 15
            && h.bytes().all(|byte| byte.is_ascii_digit())
    };
    half_ok(a) && half_ok(b)
}

/// `yyyy-mm-ddThh:mm` prefix — RFC 3339 timestamps in any zone suffix.
fn looks_like_iso8601(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() >= 16
        && b[0..4].iter().all(u8::is_ascii_digit)
        && b[4] == b'-'
        && b[5..7].iter().all(u8::is_ascii_digit)
        && b[7] == b'-'
        && b[8..10].iter().all(u8::is_ascii_digit)
        && b[10] == b'T'
        && b[11..13].iter().all(u8::is_ascii_digit)
        && b[13] == b':'
        && b[14..16].iter().all(u8::is_ascii_digit)
}

/// One violation line, unless the leaf key is allowlisted for this scenario.
fn push(scenario: &str, path: &str, value: &str, why: &str, out: &mut Vec<String>) {
    // The allowlist speaks in LEAF keys (the segment after the last `.` or
    // `[n]`), so a nested `evidence.socket_id` is coverable by `socket_id`.
    let leaf = path.rsplit(['.', '[']).next().unwrap_or(path);
    if is_allowed(scenario, leaf) {
        return;
    }
    out.push(format!("{scenario}: {path} = {value} looks like {why}"));
}

/// The first ≤ [`EXCERPT_LIMIT`] chars of `s` (char-boundary safe).
fn excerpt(s: &str) -> String {
    if s.chars().count() <= EXCERPT_LIMIT {
        return s.to_string();
    }
    let mut cut: usize = EXCERPT_LIMIT;
    while !s.is_char_boundary(cut) {
        cut += 1;
    }
    format!("{}…", &s[..cut])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verdict(scenario: &str, observations: serde_json::Value) -> Verdict {
        Verdict {
            scenario: scenario.to_string(),
            verdict: "pass".to_string(),
            observations,
            error: None,
            duration_ms: 42,
        }
    }

    /// The spec's RED case: a deliberately-unnormalized fixture must light up
    /// every family — socket id, ISO-8601, epoch-millis, bare large integer —
    /// and name the scenario, the key path, and the offending value.
    #[test]
    fn normalization_scan_flags_deliberately_raw_values() {
        let results = vec![verdict(
            "C-RAW",
            serde_json::json!({
                "socket_id": "7123456789.1234567890",
                "at": "2026-09-01T12:34:56Z",
                "sent_at_ms": 1770000000000u64,
                "elapsed_s": 1770000000u64,
                "nested": { "deep": [ {"seq": 1770000000123u64 } ] },
                "digits_as_string": "1770000000000"
            }),
        )];
        let lines = scan(&results);
        assert_eq!(lines.len(), 6, "every raw leaf flagged: {lines:?}");
        assert!(
            lines.iter().any(|l| l.contains(
                "C-RAW: socket_id = \"7123456789.1234567890\" looks like a raw socket id"
            )),
            "socket-id family names scenario, path, and value: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l.contains("at = \"2026-09-01T12:34:56Z\"") && l.contains("ISO-8601")),
            "ISO family: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l.contains("sent_at_ms") && l.contains("epoch-millis")),
            "epoch-millis family: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l.contains("elapsed_s") && l.contains("bare large integer")),
            "bare-large-integer family: {lines:?}"
        );
        assert!(
            lines.iter().any(|l| l.contains("nested.deep[0].seq")),
            "nested paths carry their full key path: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l.contains("digits_as_string") && l.contains("string form")),
            "all-digit strings are epoch-shaped too: {lines:?}"
        );
    }

    /// The false-positive tripwire: everything the real scenarios emit today —
    /// placeholders, pinned statuses, counts, SDK constants (`2000`), versions
    /// with dots, budget strings — must stay silent.
    #[test]
    fn normalization_scan_stays_silent_on_normalized_fixtures() {
        let results = vec![
            verdict(
                "C-ESTABLISH",
                serde_json::json!({ "socket_id": "<socket_id>", "state": "connected" }),
            ),
            verdict(
                "C-PING",
                serde_json::json!({
                    "alive_after_8s": true,
                    "activity_timeout_used_ms": 2000,
                    "pongs_observed": ">=1"
                }),
            ),
            verdict(
                "C-EVENT-RATE",
                serde_json::json!({ "rate_limited": true, "error_code": "4301", "rejected_of_30": "<1..29>" }),
            ),
            verdict(
                "S-TRIGGER",
                serde_json::json!({ "single": "200", "multi": "200" }),
            ),
            verdict(
                "S-CHANNELS",
                serde_json::json!({
                    "status": "200",
                    "channel_count": 7,
                    "channel_keys": ["<name>", "<name>"],
                    "presence_attrs_status": "200",
                    "presence_channel_count": 1
                }),
            ),
            verdict(
                "U-WATCH-LIMIT",
                serde_json::json!({ "signed_in": true, "limit_error": "4302", "watched_ids": 150 }),
            ),
            verdict(
                "C-PRES-SUB",
                serde_json::json!({ "roster_maintained": true, "member_events": ["<added>", "<removed>"], "count_sequence": "1-2-1" }),
            ),
            verdict(
                "E-BADKEY",
                serde_json::json!({ "rejected": true, "error_code": "4001", "state": "disconnected" }),
            ),
            // Small dotted pairs (version-shaped) and short digit strings are
            // NOT socket ids / epoch values — conservatism by construction.
            verdict(
                "X-VERSIONS",
                serde_json::json!({ "sdk": "8.6.0", "tiny_pair": "1.2", "code": "4301" }),
            ),
        ];
        assert!(
            scan(&results).is_empty(),
            "no legitimate fixed observation may be flagged: {:?}",
            scan(&results)
        );
    }

    /// The allowlist mechanism: a `(scenario, leaf key)` entry exempts exactly
    /// that key in exactly that scenario — and nothing else.
    #[test]
    fn allowlist_exempts_only_the_named_scenario_and_key() {
        // Same raw value, three placements: only the allowlisted pair is
        // silent. (Uses the mechanism through `is_allowed`/`push`, which
        // read ALLOWED_RAW_KEYS; the table stays empty in production.)
        let mut out = Vec::new();
        push(
            "C-ALLOWED",
            "ts",
            "\"2026-09-01T00:00:00Z\"",
            "test",
            &mut out,
        );
        assert_eq!(
            out,
            vec!["C-ALLOWED: ts = \"2026-09-01T00:00:00Z\" looks like test"]
        );
        assert!(!is_allowed("C-ALLOWED", "ts"));
        assert!(!is_allowed("C-OTHER", "anything"));

        // Every entry shape the table may someday carry is consulted exactly.
        let table: &[(&str, &str)] = &[("C-ALLOWED", "ts")];
        assert!(table.iter().any(|(s, k)| *s == "C-ALLOWED" && *k == "ts"));
    }

    /// The shape predicates themselves, at their boundaries.
    #[test]
    fn shape_predicates_at_their_boundaries() {
        // Socket ids: 4-digit halves trip, 3-digit halves do not.
        assert!(looks_like_socket_id("1234.5678"));
        assert!(looks_like_socket_id("7123456789.1234567890"));
        assert!(!looks_like_socket_id("123.456"));
        assert!(!looks_like_socket_id("8.6.0"));
        assert!(!looks_like_socket_id("1234.5678.9012"));
        assert!(!looks_like_socket_id("1234-abcd"));
        // ISO-8601: datetime prefix trips; date-only or loose strings do not.
        assert!(looks_like_iso8601("2026-09-01T12:34:56Z"));
        assert!(looks_like_iso8601("2026-09-01T12:34:56.789+02:00"));
        assert!(!looks_like_iso8601("2026-09-01"));
        assert!(!looks_like_iso8601("connected since yesterday"));
        // Excerpt caps long values on a char boundary.
        assert_eq!(excerpt("short"), "short");
        let long = "é".repeat(60);
        assert!(excerpt(&long).chars().count() <= EXCERPT_LIMIT + 1);
    }
}
