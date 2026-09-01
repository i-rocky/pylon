//! HTTP request-head parsing and the RFC 6455 server handshake for the
//! per-core transport.
//!
//! A per-core `mio` worker accepts raw TCP and reads the
//! HTTP request head out of the connection's initial bytes. This module is the
//! pure, socket-free logic that decides what that head *is*:
//!
//! * a WebSocket upgrade (any path — a wrong path still completes the 101 and
//!   is rejected with 4005 "Path not found" by the worker's path parsing) →
//!   [`HeadResult::WsUpgrade`], for which the worker replies with
//!   [`accept_response`] and keeps the connection; or
//! * any other complete HTTP request (the REST API `/apps/...`) →
//!   [`HeadResult::Rest`], which the worker hands off to the tokio/axum control
//!   plane (replaying the consumed head bytes).
//!
//! Everything here is total functions over byte buffers — no sockets, no `mio`,
//! 100% safe Rust (the crate root sets `#![forbid(unsafe_code)]`).

use base64::Engine as _;
use sha1::{Digest, Sha1};

/// RFC 6455 §1.3 magic GUID appended to `Sec-WebSocket-Key` before hashing.
const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// Result of reading an HTTP request head from a connection's initial bytes.
#[derive(Debug, PartialEq)]
pub enum HeadResult {
    /// A WebSocket upgrade: the `Sec-WebSocket-Key` value and the request path.
    WsUpgrade { key: String, path: String },
    /// A non-WS HTTP request (REST). `consumed` = number of head bytes parsed;
    /// the caller hands the whole stream (including these bytes) to the HTTP
    /// control plane.
    Rest { consumed: usize },
    /// The head is not yet fully received (no CRLFCRLF terminator yet). Read
    /// more.
    NeedMore,
    /// Malformed / unsupported request.
    Bad(&'static str),
}

/// Parse the HTTP request head from `buf` (the bytes received so far), bounded
/// by `max_head_bytes` (G3 slowloris hardening).
///
/// The cap bounds the HEAD — the start line + headers, i.e. everything up to
/// the CRLFCRLF terminator — NOT the total buffer: a REST request's BODY is
/// read into the same buffer and is legitimately large. The head is over-size
/// iff no CRLFCRLF appears within the first `max_head_bytes` bytes, so BOTH a
/// headerless dribble crossing the line AND a giant complete head whose
/// terminator sits beyond the cap are [`HeadResult::Bad`] — checked before
/// parsing, so it fires on the first read that crosses the line — and the
/// worker closes the connection, capping the per-connection
/// head-accumulation memory. A head whose terminator lies within the cap is
/// honoured however many body bytes follow it. `0` disables the cap (no
/// legitimate head comes close; the default 16384 is generous).
///
/// A WS upgrade is: method `GET`, an `Upgrade: websocket` header
/// (case-insensitive), a `Connection: Upgrade` header (case-insensitive, may be
/// a comma list), a `Sec-WebSocket-Version: 13` header, and a
/// `Sec-WebSocket-Key` header. The path may be anything: a WS client targeting
/// a path that is not the `/app/{key}` shape still completes the 101 here and
/// is rejected by the worker with 4005 "Path not found" (Pusher parity). A
/// `GET` targeting `/app/...` whose upgrade headers are missing or malformed is
/// a bad upgrade, not a REST request. Anything else complete (any non-GET
/// method, or a plain GET without the upgrade header set) is
/// [`HeadResult::Rest`]. An incomplete head is [`HeadResult::NeedMore`].
pub fn read_head(buf: &[u8], max_head_bytes: usize) -> HeadResult {
    if max_head_bytes != 0 && buf.len() > max_head_bytes {
        let head_within_cap = buf[..max_head_bytes].windows(4).any(|w| w == b"\r\n\r\n");
        if !head_within_cap {
            return HeadResult::Bad("request head too large");
        }
    }
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut req = httparse::Request::new(&mut headers);

    let consumed = match req.parse(buf) {
        Ok(httparse::Status::Complete(n)) => n,
        Ok(httparse::Status::Partial) => return HeadResult::NeedMore,
        Err(_) => return HeadResult::Bad("malformed http request"),
    };

    let method = req.method.unwrap_or("");
    let path = req.path.unwrap_or("");

    // Only `GET` can be a WebSocket upgrade (RFC 6455 §4.2.1). Everything else
    // complete is REST and is handed off verbatim to the control plane.
    if method != "GET" {
        return HeadResult::Rest { consumed };
    }

    // From here the request is a GET, so the client may be *trying* to open a
    // WebSocket. Whether it succeeds depends only on the upgrade headers — a
    // WS client with a wrong path (e.g. `/nope/`) still deserves the 101 + a
    // protocol-level 4005 rejection rather than an opaque REST 404.
    let mut upgrade_ok = false;
    let mut connection_upgrade = false;
    let mut version_ok = false;
    let mut ws_key: Option<String> = None;

    for h in req.headers.iter() {
        if h.name.eq_ignore_ascii_case("upgrade") {
            // Value may be a single token `websocket` (RFC 6455 §4.2.1).
            if header_value_eq_ignore_ascii_case(h.value, b"websocket") {
                upgrade_ok = true;
            }
        } else if h.name.eq_ignore_ascii_case("connection") {
            // May be a comma list, e.g. `keep-alive, Upgrade`. Look for the
            // `upgrade` token case-insensitively.
            if connection_contains_upgrade(h.value) {
                connection_upgrade = true;
            }
        } else if h.name.eq_ignore_ascii_case("sec-websocket-version") {
            if header_value_eq_ignore_ascii_case(h.value, b"13") {
                version_ok = true;
            }
        } else if h.name.eq_ignore_ascii_case("sec-websocket-key") {
            // The key is base64 of a 16-byte nonce; we forward it verbatim and
            // never reinterpret it, so any valid UTF-8 token is accepted here
            // and validated only by being echoed through the SHA-1 accept.
            if let Ok(v) = std::str::from_utf8(h.value) {
                let trimmed = v.trim();
                if !trimmed.is_empty() {
                    ws_key = Some(trimmed.to_owned());
                }
            }
        }
    }

    match (upgrade_ok, connection_upgrade, version_ok, ws_key) {
        (true, true, true, Some(key)) => HeadResult::WsUpgrade {
            key,
            path: path.to_owned(),
        },
        // A GET targeting `/app/...` with broken upgrade headers is a bad
        // upgrade (the client clearly wanted a WebSocket), not a REST request.
        _ if path.starts_with("/app/") => HeadResult::Bad("invalid websocket upgrade"),
        // A plain GET (no complete WS-upgrade header set) elsewhere is REST.
        _ => HeadResult::Rest { consumed },
    }
}

/// Compare a raw header value to an expected token, ignoring ASCII case and
/// surrounding whitespace.
fn header_value_eq_ignore_ascii_case(value: &[u8], expected: &[u8]) -> bool {
    trim_ascii(value).eq_ignore_ascii_case(expected)
}

/// Does a `Connection` header value contain the `upgrade` token (comma list,
/// case-insensitive)?
fn connection_contains_upgrade(value: &[u8]) -> bool {
    let Ok(s) = std::str::from_utf8(value) else {
        return false;
    };
    s.split(',')
        .any(|tok| tok.trim().eq_ignore_ascii_case("upgrade"))
}

/// Trim leading/trailing ASCII whitespace from a byte slice. (`[u8]::trim_ascii`
/// is stable but kept explicit here to avoid edition-feature surprises.)
fn trim_ascii(mut v: &[u8]) -> &[u8] {
    while let [first, rest @ ..] = v {
        if first.is_ascii_whitespace() {
            v = rest;
        } else {
            break;
        }
    }
    while let [rest @ .., last] = v {
        if last.is_ascii_whitespace() {
            v = rest;
        } else {
            break;
        }
    }
    v
}

/// The RFC 6455 server handshake response (HTTP 101) for a given
/// `Sec-WebSocket-Key`.
///
/// `Sec-WebSocket-Accept = base64( sha1( key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11" ) )`.
pub fn accept_response(ws_key: &str) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(ws_key.as_bytes());
    hasher.update(WS_GUID.as_bytes());
    let digest = hasher.finalize();
    let accept = base64::engine::general_purpose::STANDARD.encode(digest);

    format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\
         \r\n"
    )
    .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A complete `GET /app/app-key` WebSocket upgrade with all required
    /// headers. The `Sec-WebSocket-Key` is the canonical RFC 6455 §1.3 nonce.
    const WS_UPGRADE: &[u8] = b"GET /app/app-key HTTP/1.1\r\n\
        Host: example.com\r\n\
        Upgrade: websocket\r\n\
        Connection: Upgrade\r\n\
        Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
        Sec-WebSocket-Version: 13\r\n\
        \r\n";

    /// Head cap for the parse tests — far above any legitimate head, so the
    /// cap never interferes with the parsing assertions.
    const DEFAULT_TEST_HEAD_CAP: usize = 1 << 20;

    /// G3: a headerless buffer that has grown past the cap is `Bad` (not
    /// `NeedMore`) — the FIRST read crossing the line trips it, whether or not
    /// the bytes so far parse as a partial head.
    #[test]
    fn head_over_the_cap_is_bad_even_when_incomplete() {
        let dribble = vec![b'x'; 1025];
        assert_eq!(
            read_head(&dribble, 1024),
            HeadResult::Bad("request head too large")
        );
        // Exactly AT the cap is still fine (the cap is exclusive: `>`).
        assert_eq!(read_head(&dribble[..1024], 1024), HeadResult::NeedMore);
    }

    /// G3: `0` disables the cap — a giant headerless buffer stays `NeedMore`.
    #[test]
    fn head_cap_zero_disables_the_limit() {
        let dribble = vec![b'x'; 64 * 1024];
        assert_eq!(read_head(&dribble, 0), HeadResult::NeedMore);
    }

    /// G3: a COMPLETE head within the cap parses normally even when the cap is
    /// tight — real handshakes are unaffected.
    #[test]
    fn real_upgrade_parses_under_a_tight_cap() {
        assert_eq!(
            read_head(WS_UPGRADE, WS_UPGRADE.len()),
            HeadResult::WsUpgrade {
                key: "dGhlIHNhbXBsZSBub25jZQ==".to_owned(),
                path: "/app/app-key".to_owned(),
            }
        );
    }

    /// G3: the cap bounds the HEAD, not the buffer — a REST request whose small
    /// head is followed by a huge body must still parse (regression pin for the
    /// REST 413 body-limit path, whose 200 KiB body lands in the same buffer).
    #[test]
    fn large_rest_body_after_a_small_head_is_not_capped() {
        let head = b"POST /apps/app/events HTTP/1.1\r\nHost: x\r\nContent-Length: 204800\r\n\r\n";
        let mut buf = head.to_vec();
        buf.extend_from_slice(vec![b'x'; 200 * 1024].as_slice());
        assert_eq!(
            read_head(&buf, 1024),
            HeadResult::Rest {
                consumed: head.len()
            }
        );
    }

    /// G3: a head whose TERMINATOR sits beyond the cap is over-size even though
    /// it is otherwise well-formed — the giant-complete-head variant of the
    /// slowloris.
    #[test]
    fn giant_complete_head_beyond_the_cap_is_bad() {
        // A syntactically fine head: start line + one header with a huge value,
        // terminator just past the 1024-byte cap.
        let mut buf = b"POST /apps/app/events HTTP/1.1\r\nX-Pad: ".to_vec();
        buf.extend(vec![b'x'; 1100]);
        buf.extend_from_slice(b"\r\n\r\n");
        assert_eq!(
            read_head(&buf, 1024),
            HeadResult::Bad("request head too large")
        );
    }

    /// 1. RFC 6455 §1.3 canonical accept Known-Answer-Test.
    #[test]
    fn accept_response_rfc6455_kat() {
        let resp = accept_response("dGhlIHNhbXBsZSBub25jZQ==");
        let text = std::str::from_utf8(&resp).unwrap();
        assert!(
            text.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"),
            "response did not contain the canonical RFC 6455 accept value:\n{text}"
        );
    }

    /// 2. A full WS upgrade parses to `WsUpgrade` with the key and path.
    #[test]
    fn parses_ws_upgrade() {
        assert_eq!(
            read_head(WS_UPGRADE, DEFAULT_TEST_HEAD_CAP),
            HeadResult::WsUpgrade {
                key: "dGhlIHNhbXBsZSBub25jZQ==".to_owned(),
                path: "/app/app-key".to_owned(),
            }
        );
    }

    /// 3. A complete non-WS request is `Rest { consumed: <head len> }`.
    #[test]
    fn parses_rest_request() {
        let req = b"POST /apps/app/events HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n";
        assert_eq!(
            read_head(req, DEFAULT_TEST_HEAD_CAP),
            HeadResult::Rest {
                consumed: req.len()
            }
        );
    }

    /// 3b. A WS upgrade to a path outside `/app/{key}` is STILL a `WsUpgrade`:
    /// the 101 completes and the worker rejects the path with 4005 (Pusher
    /// parity) — the client asked for a WebSocket, so it gets a
    /// protocol-level rejection, not a REST handoff.
    #[test]
    fn ws_upgrade_off_app_path_still_upgrades() {
        let req = b"GET /nope/?protocol=7 HTTP/1.1\r\n\
            Host: example.com\r\n\
            Upgrade: websocket\r\n\
            Connection: Upgrade\r\n\
            Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
            Sec-WebSocket-Version: 13\r\n\
            \r\n";
        assert_eq!(
            read_head(req, DEFAULT_TEST_HEAD_CAP),
            HeadResult::WsUpgrade {
                key: "dGhlIHNhbXBsZSBub25jZQ==".to_owned(),
                path: "/nope/?protocol=7".to_owned(),
            }
        );
    }

    /// 3c. A plain GET (no complete WS-upgrade header set) to a non-`/app/`
    /// path stays REST — health checks / metrics probes are unaffected by the
    /// 4005 path rejection.
    #[test]
    fn plain_get_off_app_path_is_rest() {
        let req = b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n";
        assert_eq!(
            read_head(req, DEFAULT_TEST_HEAD_CAP),
            HeadResult::Rest {
                consumed: req.len()
            }
        );
    }

    /// 4. A head truncated before CRLFCRLF is `NeedMore`.
    #[test]
    fn incomplete_head_needs_more() {
        // Drop the terminating CRLFCRLF (and the last header is incomplete).
        let truncated = b"GET /app/app-key HTTP/1.1\r\n\
            Host: example.com\r\n\
            Upgrade: websocket\r\n\
            Connection: Upgrade\r\n";
        assert_eq!(
            read_head(truncated, DEFAULT_TEST_HEAD_CAP),
            HeadResult::NeedMore
        );
    }

    /// 5. `GET /app/` with a missing `Sec-WebSocket-Key` is a bad upgrade.
    #[test]
    fn missing_ws_key_is_bad() {
        let req = b"GET /app/app-key HTTP/1.1\r\n\
            Host: example.com\r\n\
            Upgrade: websocket\r\n\
            Connection: Upgrade\r\n\
            Sec-WebSocket-Version: 13\r\n\
            \r\n";
        assert_eq!(
            read_head(req, DEFAULT_TEST_HEAD_CAP),
            HeadResult::Bad("invalid websocket upgrade")
        );
    }

    /// 6. Header names *and* values compare case-insensitively, and the
    ///    `Connection` token list is searched for `upgrade`.
    #[test]
    fn case_insensitive_headers_parse() {
        let req = b"GET /app/app-key HTTP/1.1\r\n\
            Host: example.com\r\n\
            upgrade: WebSocket\r\n\
            connection: keep-alive, Upgrade\r\n\
            Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
            Sec-WebSocket-Version: 13\r\n\
            \r\n";
        assert_eq!(
            read_head(req, DEFAULT_TEST_HEAD_CAP),
            HeadResult::WsUpgrade {
                key: "dGhlIHNhbXBsZSBub25jZQ==".to_owned(),
                path: "/app/app-key".to_owned(),
            }
        );
    }

    /// 7. The 101 response has the exact RFC 6455 shape (correct start line and
    ///    a blank-line terminator).
    #[test]
    fn response_shape_is_well_formed() {
        let resp = accept_response("dGhlIHNhbXBsZSBub25jZQ==");
        let text = std::str::from_utf8(&resp).unwrap();
        assert!(
            text.starts_with("HTTP/1.1 101 Switching Protocols\r\n"),
            "bad start line:\n{text}"
        );
        assert!(text.ends_with("\r\n\r\n"), "bad terminator:\n{text}");
    }
}
