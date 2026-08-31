//! The single version-aware encode entry point (U2 / Task 7.1).
//!
//! Protocol-version selection is DATA here (a `u8` argument), not a scattered
//! type choice: adapters, workers and benches encode through [`encode_into`] /
//! [`encode`], and `v7::frames` is fenced (`pub(super)`) so no direct caller
//! can appear outside the protocol module family. Task 7.3 builds its
//! per-version sink frames on this seam.

use super::v7;
use super::{MAX_PROTOCOL, MIN_PROTOCOL};
use crate::protocol::event::ServerEvent;

/// Every protocol version this server can encode, materialized from the
/// negotiation range `MIN_PROTOCOL..=MAX_PROTOCOL` (`[7]` today). The sink's
/// fan-out frames are built per entry of this list (7.3); cluster-wide shared
/// frames (the redis relay envelope) still encode at `ACTIVE_VERSIONS[0]`
/// until a v8 cluster envelope exists.
pub const ACTIVE_VERSIONS: &[u8] = &all_versions();

/// `MIN_PROTOCOL..=MAX_PROTOCOL` as a const array.
const fn all_versions() -> [u8; (MAX_PROTOCOL - MIN_PROTOCOL + 1) as usize] {
    let mut out = [0u8; (MAX_PROTOCOL - MIN_PROTOCOL + 1) as usize];
    let mut i = 0;
    while i < out.len() {
        out[i] = MIN_PROTOCOL + i as u8;
        i += 1;
    }
    out
}

/// Append the wire form of `event` for `version` to `out` WITHOUT clearing it
/// (append semantics — same contract as [`crate::protocol::codec::Codec::encode_into`]).
/// The only sanctioned encode path; `v7::frames` is not reachable from outside
/// the protocol module family.
///
/// # Panics
/// On a `version` outside `MIN_PROTOCOL..=MAX_PROTOCOL` (plus the test-only 8
/// below). Callers pass either a codec-negotiated version ([`crate::protocol::codec::Codec::version`]) or an
/// entry of [`ACTIVE_VERSIONS`] — both are validated by `negotiate`'s range
/// check before a connection ever reaches an encode.
pub fn encode_into(version: u8, event: &ServerEvent, out: &mut String) {
    match version {
        7 => v7::frames::encode_into(event, out),
        // Test-only second protocol version (Task 7.3's two-version fan-out
        // fixture; the same pattern as the 7.2 test codecs). NOT a real
        // protocol — it exists so the sink's per-version plumbing can be
        // exercised with distinct bytes while `MAX_PROTOCOL` is still 7.
        // `negotiate` never hands out 8, so no production path can reach it.
        #[cfg(any(test, feature = "test-hooks"))]
        8 => test_v8_encode_into(event, out),
        _ => unreachable!("version {version} outside MIN..=MAX; negotiate validated the range"),
    }
}

/// The fixture v8 wire form: the v7 JSON wrapped in a `[8,…]` envelope —
/// valid JSON, deterministic, and byte-distinct from v7 for EVERY event (the
/// fan-out fixture asserts on exactly that distinction, so a wrong-slot
/// delivery cannot alias). Compiled only for tests / the `test-hooks` feature.
#[cfg(any(test, feature = "test-hooks"))]
fn test_v8_encode_into(event: &ServerEvent, out: &mut String) {
    out.push_str("[8,");
    v7::frames::encode_into(event, out);
    out.push(']');
}

/// Encode `event` for `version` into a fresh `String`.
pub fn encode(version: u8, event: &ServerEvent) -> String {
    let mut out = String::new();
    encode_into(version, event, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The materialized range is exactly MIN..=MAX, element for element.
    #[test]
    fn active_versions_is_the_negotiation_range() {
        let mut want = Vec::new();
        let mut v = MIN_PROTOCOL;
        while v <= MAX_PROTOCOL {
            want.push(v);
            v += 1;
        }
        assert_eq!(ACTIVE_VERSIONS, &want[..]);
    }

    /// `wire::encode` produces the codec's bytes, verbatim — a REAL cross-check
    /// of the two sanctioned paths (free function vs `Codec` trait object) —
    /// and `encode_into` keeps append semantics.
    #[test]
    fn encode_matches_the_codec_and_appends() {
        use crate::protocol::codec::Codec;
        let ev = ServerEvent::SubscriptionSucceeded {
            channel: "c".into(),
            presence: None,
        };
        assert_eq!(encode(7, &ev), v7::V7Codec.encode(&ev));
        let mut buf = String::from("<sentinel>");
        encode_into(7, &ev, &mut buf);
        assert!(buf.starts_with("<sentinel>"));
        assert_eq!(&buf["<sentinel>".len()..], &encode(7, &ev));
    }
}
