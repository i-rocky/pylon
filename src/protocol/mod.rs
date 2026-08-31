pub mod codec;
pub mod command;
pub mod error;
pub mod event;
pub mod socket_id;
pub mod v7;
pub mod wire;

use codec::Codec;
use error::PusherError;

pub const MIN_PROTOCOL: u8 = 7;
pub const MAX_PROTOCOL: u8 = 7;

/// Pick a codec for the connection's `?protocol=` / `?version=` params.
///
/// Per the Pusher protocol spec, `?protocol=` is authoritative when present:
/// parseable + supported → that codec, unparseable → 4006 ("Invalid version
/// string format"), parseable but unsupported → 4007. When `?protocol=` is
/// absent the server *infers* the protocol from the `?version=` library
/// version's major ("7.4.1" → 7; kept for legacy JS clients); a malformed
/// `version` string is a 4006. With neither param, strict mode answers 4008
/// ("No protocol version supplied") while lenient mode defaults to the latest
/// supported protocol.
pub fn negotiate(
    protocol: Option<&str>,
    version: Option<&str>,
    strict: bool,
) -> Result<Box<dyn Codec>, PusherError> {
    match protocol {
        Some(s) => {
            let n: u32 = s.parse().map_err(|_| PusherError::invalid_version())?;
            supported_or_4007(n)
        }
        None => {
            if let Some(v) = version {
                let major = major_of(v).ok_or_else(PusherError::invalid_version)?;
                supported_or_4007(major)
            } else if strict {
                Err(PusherError::no_protocol())
            } else {
                Ok(codec_for(MAX_PROTOCOL))
            }
        }
    }
}

/// The major component of a library version string ("7.4.1" → 7), parsed
/// wide (u32) so an out-of-range-but-well-formed major lands on 4007
/// (unsupported) rather than 4006 (malformed).
fn major_of(version: &str) -> Option<u32> {
    version.split('.').next().unwrap_or_default().parse().ok()
}

fn supported_or_4007(major: u32) -> Result<Box<dyn Codec>, PusherError> {
    if (u32::from(MIN_PROTOCOL)..=u32::from(MAX_PROTOCOL)).contains(&major) {
        Ok(codec_for(major as u8))
    } else {
        Err(PusherError::unsupported_protocol())
    }
}

/// The single extension point for new protocol versions.
fn codec_for(_version: u8) -> Box<dyn Codec> {
    Box::new(v7::V7Codec)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Branch 1: `protocol` present, parseable, supported → used as-is.
    #[test]
    fn supported_explicit_protocol_ok() {
        assert_eq!(negotiate(Some("7"), None, false).unwrap().version(), 7);
    }

    // Branch 2: `protocol` present but unparseable → 4006 (invalid version
    // string format); a well-formed `version` must NOT rescue it.
    #[test]
    fn unparseable_protocol_is_4006_even_with_version() {
        assert_eq!(
            negotiate(Some("abc"), Some("7.4.1"), false)
                .unwrap_err()
                .code,
            4006
        );
    }

    // Branch 3a: `protocol` absent, `version` parseable → infer the major
    // ("7.4.1" → 7). Pusher documents this fallback for legacy JS clients.
    #[test]
    fn missing_protocol_infers_major_from_version() {
        assert_eq!(negotiate(None, Some("7.4.1"), false).unwrap().version(), 7);
        assert_eq!(negotiate(None, Some("7"), false).unwrap().version(), 7);
    }

    // Branch 3b: inferred major outside the supported range → 4007.
    #[test]
    fn missing_protocol_unsupported_inferred_major_is_4007() {
        assert_eq!(negotiate(None, Some("8.1"), false).unwrap_err().code, 4007);
    }

    // Branch 4: `version` present but unparseable → 4006.
    #[test]
    fn missing_protocol_unparseable_version_is_4006() {
        assert_eq!(
            negotiate(None, Some("banana"), false).unwrap_err().code,
            4006
        );
    }

    // Branch 5: both absent → strict 4008, lenient default (unchanged).
    #[test]
    fn both_absent_strict_is_4008() {
        assert_eq!(negotiate(None, None, true).unwrap_err().code, 4008);
    }

    #[test]
    fn both_absent_lenient_defaults_to_latest() {
        assert_eq!(negotiate(None, None, false).unwrap().version(), 7);
    }

    // `strict` only polices a *missing* protocol/version; an inferable
    // `version` satisfies it.
    #[test]
    fn strict_accepts_inferred_version() {
        assert_eq!(negotiate(None, Some("7.4.1"), true).unwrap().version(), 7);
    }

    #[test]
    fn unsupported_explicit_protocol_is_4007() {
        assert_eq!(negotiate(Some("3"), None, false).unwrap_err().code, 4007);
    }

    // A parseable-but-out-of-range protocol integer (e.g. 300) is merely
    // *unsupported* (4007), not a malformed version string (4006) — same rule
    // the wide-parse `version` inference already applies.
    #[test]
    fn out_of_range_protocol_integer_is_4007_not_4006() {
        assert_eq!(negotiate(Some("300"), None, false).unwrap_err().code, 4007);
    }

    // Beyond u32 range the value is no longer a well-formed version integer;
    // that stays a 4006.
    #[test]
    fn u32_overflow_protocol_integer_is_still_4006() {
        assert_eq!(
            negotiate(Some("99999999999"), None, false)
                .unwrap_err()
                .code,
            4006
        );
    }
}
