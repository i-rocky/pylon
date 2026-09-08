//! RFC 6455 WebSocket frame codec for the lean per-core transport.
//!
//! This is the *server* side of the protocol:
//!
//! * [`parse`] reads **client → server** frames. Per RFC 6455 §5.1 every such
//!   frame is masked; we reject any unmasked client frame as a protocol error
//!   and unmask the payload in place.
//! * [`encode`] / [`encode_text`] write **server → client** frames, which are
//!   always *unmasked* (RFC 6455 §5.1: "A server MUST NOT mask any frames").
//!
//! The codec never owns a per-connection buffer of its own: parsing splits
//! bytes out of a caller-provided [`BytesMut`], and the returned payload is a
//! cheap [`Bytes`] handle into that same allocation. 100% safe Rust — the crate
//! root sets `#![forbid(unsafe_code)]`.

use bytes::{Bytes, BytesMut};

/// WebSocket frame opcode (RFC 6455 §5.2). Only the opcodes pylon handles are
/// modelled; reserved opcodes are rejected during parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpCode {
    Continuation,
    Text,
    Binary,
    Close,
    Ping,
    Pong,
}

impl OpCode {
    /// Map the 4-bit opcode nibble onto a known [`OpCode`].
    fn from_u8(v: u8) -> Result<OpCode, ParseError> {
        Ok(match v {
            0x0 => OpCode::Continuation,
            0x1 => OpCode::Text,
            0x2 => OpCode::Binary,
            0x8 => OpCode::Close,
            0x9 => OpCode::Ping,
            0xA => OpCode::Pong,
            _ => return Err(ParseError::Protocol("reserved opcode")),
        })
    }

    /// The wire nibble for this opcode.
    fn as_u8(self) -> u8 {
        match self {
            OpCode::Continuation => 0x0,
            OpCode::Text => 0x1,
            OpCode::Binary => 0x2,
            OpCode::Close => 0x8,
            OpCode::Ping => 0x9,
            OpCode::Pong => 0xA,
        }
    }

    /// Control frames (Close/Ping/Pong) carry tighter constraints (RFC 6455 §5.5):
    /// payload ≤ 125 bytes and they must not be fragmented (FIN must be set).
    fn is_control(self) -> bool {
        matches!(self, OpCode::Close | OpCode::Ping | OpCode::Pong)
    }
}

/// A single parsed WebSocket frame. `payload` is an unmasked, owned slice into
/// the source buffer (zero-copy `Bytes`).
#[derive(Debug)]
pub struct Frame {
    pub fin: bool,
    pub opcode: OpCode,
    pub payload: Bytes,
}

/// Why a [`parse`] attempt did not yield a frame.
#[derive(Debug, PartialEq)]
pub enum ParseError {
    /// Not enough bytes buffered yet; the caller should read more and retry.
    /// The source buffer is left untouched.
    Incomplete,
    /// A fatal protocol violation; the connection must be closed.
    Protocol(&'static str),
    /// The declared payload length exceeds the caller's `max_payload` budget.
    TooLarge,
}

/// Parse one WebSocket frame from the front of `buf`.
///
/// On success the frame's bytes (header + payload) are consumed from `buf` and
/// the returned [`Frame::payload`] is unmasked. On [`ParseError::Incomplete`]
/// the buffer is left exactly as it was so the caller can read more and retry.
/// `max_payload` bounds the accepted payload size ([`ParseError::TooLarge`]).
///
/// This parses **client → server** frames, which RFC 6455 §5.1 requires to be
/// masked; an unmasked client frame is a [`ParseError::Protocol`].
pub fn parse(buf: &mut BytesMut, max_payload: usize) -> Result<Frame, ParseError> {
    let available = buf.len();

    // Need at least the 2-byte base header.
    if available < 2 {
        return Err(ParseError::Incomplete);
    }

    let b0 = buf[0];
    let b1 = buf[1];

    let fin = b0 & 0x80 != 0;
    // RSV1..3 must be zero (no extensions negotiated).
    if b0 & 0x70 != 0 {
        return Err(ParseError::Protocol("reserved bits set"));
    }
    let opcode = OpCode::from_u8(b0 & 0x0F)?;

    let masked = b1 & 0x80 != 0;
    let len7 = (b1 & 0x7F) as usize;

    // Resolve the extended-length form. `header_len` is the total number of
    // bytes before the payload (base header + extended length + mask key).
    let (payload_len, len_field_bytes) = match len7 {
        126 => {
            if available < 4 {
                return Err(ParseError::Incomplete);
            }
            let len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
            (len, 2)
        }
        127 => {
            if available < 10 {
                return Err(ParseError::Incomplete);
            }
            let raw = u64::from_be_bytes([
                buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9],
            ]);
            // RFC 6455 §5.2: the most-significant bit of a 64-bit length MUST be 0.
            if raw & 0x8000_0000_0000_0000 != 0 {
                return Err(ParseError::Protocol("64-bit length high bit set"));
            }
            // Guard the usize cast on 32-bit targets; oversized is TooLarge below.
            if raw > usize::MAX as u64 {
                return Err(ParseError::TooLarge);
            }
            (raw as usize, 8)
        }
        n => (n, 0),
    };

    // Control-frame constraints (RFC 6455 §5.5): no fragmentation, ≤125 bytes.
    if opcode.is_control() {
        if !fin {
            return Err(ParseError::Protocol("fragmented control frame"));
        }
        if payload_len > 125 {
            return Err(ParseError::Protocol("control frame payload > 125"));
        }
    }

    // Client frames must be masked (RFC 6455 §5.1).
    if !masked {
        return Err(ParseError::Protocol("unmasked client frame"));
    }

    if payload_len > max_payload {
        return Err(ParseError::TooLarge);
    }

    let mask_bytes = 4usize; // masked is guaranteed true here.
    let header_len = 2 + len_field_bytes + mask_bytes;

    // Do we have the whole frame buffered yet?
    let total = match header_len.checked_add(payload_len) {
        Some(t) => t,
        None => return Err(ParseError::TooLarge),
    };
    if available < total {
        return Err(ParseError::Incomplete);
    }

    // Commit: split the full frame out of the caller's buffer.
    let mut frame_bytes = buf.split_to(total);
    let key = [
        frame_bytes[2 + len_field_bytes],
        frame_bytes[2 + len_field_bytes + 1],
        frame_bytes[2 + len_field_bytes + 2],
        frame_bytes[2 + len_field_bytes + 3],
    ];

    // Drop the header, leaving only the (still-masked) payload, then unmask it
    // in place. `freeze` hands out a zero-copy `Bytes` over the same allocation.
    let _ = frame_bytes.split_to(header_len);
    unmask(&mut frame_bytes, key);

    Ok(Frame {
        fin,
        opcode,
        payload: frame_bytes.freeze(),
    })
}

/// Unmask `data` in place with the 4-byte `key` (RFC 6455 §5.3:
/// `transformed[i] = original[i] XOR key[i % 4]`).
///
/// Processes 8 bytes per iteration against a u64-broadcast key, with a scalar
/// tail for the remainder. Pure safe Rust — no `unsafe`, no `align_to`.
fn unmask(data: &mut [u8], key: [u8; 4]) {
    // Broadcast the 4-byte key to 8 bytes so we can XOR a whole word at a time.
    // The masking sequence is key[0..4] repeating, so the 8-byte pattern is
    // [k0 k1 k2 k3 k0 k1 k2 k3]; since len%4 chunking aligns on word boundaries
    // (8 is a multiple of 4), every 8-byte block sees the same broadcast word.
    let key64 = u64::from_le_bytes([
        key[0], key[1], key[2], key[3], key[0], key[1], key[2], key[3],
    ]);

    let (chunks, tail) = data.as_chunks_mut::<8>();
    for chunk in chunks {
        *chunk = (u64::from_le_bytes(*chunk) ^ key64).to_le_bytes();
    }

    // Scalar tail (0..7 bytes). The tail starts at an offset that is a multiple
    // of 8, hence a multiple of 4, so byte i of the tail uses key[i % 4].
    for (i, byte) in tail.iter_mut().enumerate() {
        *byte ^= key[i % 4];
    }
}

/// Encode a **server → client** frame (always unmasked, FIN set) and append it
/// to `out`. Chooses the minimal length form for `payload`.
pub fn encode(out: &mut BytesMut, fin: bool, opcode: OpCode, payload: &[u8]) {
    let b0 = if fin { 0x80 } else { 0x00 } | opcode.as_u8();
    out.extend_from_slice(&[b0]);

    let len = payload.len();
    // MASK bit is never set on server frames.
    if len <= 125 {
        out.extend_from_slice(&[len as u8]);
    } else if len <= u16::MAX as usize {
        out.extend_from_slice(&[126]);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.extend_from_slice(&[127]);
        out.extend_from_slice(&(len as u64).to_be_bytes());
    }

    out.extend_from_slice(payload);
}

/// Convenience: encode an unmasked server **text** frame (FIN set) carrying the
/// given v7 JSON payload.
pub fn encode_text(out: &mut BytesMut, payload: &[u8]) {
    encode(out, true, OpCode::Text, payload);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Build a masked client frame the way a browser would: base header, length
    // form, 4-byte key, then the masked payload.
    fn masked_frame(fin: bool, opcode: OpCode, key: [u8; 4], payload: &[u8]) -> BytesMut {
        let mut out = BytesMut::new();
        let b0 = if fin { 0x80 } else { 0x00 } | opcode.as_u8();
        out.extend_from_slice(&[b0]);

        let len = payload.len();
        if len <= 125 {
            out.extend_from_slice(&[0x80 | len as u8]);
        } else if len <= u16::MAX as usize {
            out.extend_from_slice(&[0x80 | 126]);
            out.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            out.extend_from_slice(&[0x80 | 127]);
            out.extend_from_slice(&(len as u64).to_be_bytes());
        }

        out.extend_from_slice(&key);
        for (i, &b) in payload.iter().enumerate() {
            out.extend_from_slice(&[b ^ key[i % 4]]);
        }
        out
    }

    // ---- Test 1: RFC 6455 KAT, masked client "Hello" -----------------------
    #[test]
    fn kat_masked_hello() {
        let mut buf = BytesMut::from(
            &[
                0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58,
            ][..],
        );
        let frame = parse(&mut buf, 1 << 20).expect("parse KAT");
        assert!(frame.fin);
        assert_eq!(frame.opcode, OpCode::Text);
        assert_eq!(&frame.payload[..], b"Hello");
        // Buffer fully consumed.
        assert!(buf.is_empty());
    }

    // ---- Test 2: server text round-trip is the unmasked frame --------------
    #[test]
    fn encode_text_unmasked_frame() {
        let mut out = BytesMut::new();
        encode_text(&mut out, b"Hello");
        assert_eq!(&out[..], &[0x81, 0x05, b'H', b'e', b'l', b'l', b'o'][..]);
    }

    // ---- Test 3: extended length 126 (200-byte payload) --------------------
    #[test]
    fn extended_len_126_round_trips() {
        let payload: Vec<u8> = (0..200u32).map(|i| (i * 7) as u8).collect();
        let key = [0x11, 0x22, 0x33, 0x44];
        let mut buf = masked_frame(true, OpCode::Binary, key, &payload);
        // Sanity: 126-form header (2 base + 2 len + 4 key = 8) + 200.
        assert_eq!(buf.len(), 8 + 200);

        let frame = parse(&mut buf, 1 << 20).expect("parse 126");
        assert_eq!(frame.opcode, OpCode::Binary);
        assert_eq!(frame.payload.len(), 200);
        assert_eq!(&frame.payload[..], &payload[..]);
        assert!(buf.is_empty());
    }

    // ---- Test 4: extended length 127 (70000 bytes) + high-bit rejection ----
    #[test]
    fn extended_len_127_round_trips() {
        let payload: Vec<u8> = (0..70_000u32).map(|i| (i * 13) as u8).collect();
        let key = [0xde, 0xad, 0xbe, 0xef];
        let mut buf = masked_frame(true, OpCode::Binary, key, &payload);
        // 127-form header (2 base + 8 len + 4 key = 14) + 70000.
        assert_eq!(buf.len(), 14 + 70_000);

        let frame = parse(&mut buf, 1 << 20).expect("parse 127");
        assert_eq!(frame.payload.len(), 70_000);
        assert_eq!(&frame.payload[..], &payload[..]);
        assert!(buf.is_empty());
    }

    #[test]
    fn extended_len_127_high_bit_is_protocol_error() {
        // FIN+Binary, MASK+127, then an 8-byte length with the high bit set.
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[0x82, 0x80 | 127]);
        buf.extend_from_slice(&[0x80, 0, 0, 0, 0, 0, 0, 1]); // high bit set
        buf.extend_from_slice(&[0xaa, 0xbb, 0xcc, 0xdd]); // mask key
        let err = parse(&mut buf, usize::MAX).unwrap_err();
        assert_eq!(err, ParseError::Protocol("64-bit length high bit set"));
    }

    // ---- Test 5: incomplete header and incomplete payload ------------------
    #[test]
    fn incomplete_leaves_buffer_untouched() {
        // Truncated header (1 byte).
        let mut buf = BytesMut::from(&[0x81][..]);
        assert_eq!(
            parse(&mut buf, 1 << 20).unwrap_err(),
            ParseError::Incomplete
        );
        assert_eq!(buf.len(), 1);

        // Full header + key but truncated payload (declares 5, supplies 2).
        let mut buf = BytesMut::from(&[0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f][..]);
        let before = buf.len();
        assert_eq!(
            parse(&mut buf, 1 << 20).unwrap_err(),
            ParseError::Incomplete
        );
        assert_eq!(buf.len(), before);

        // Truncated extended-length field (126 but only 1 length byte).
        let mut buf = BytesMut::from(&[0x81, 0x80 | 126, 0x00][..]);
        let before = buf.len();
        assert_eq!(
            parse(&mut buf, 1 << 20).unwrap_err(),
            ParseError::Incomplete
        );
        assert_eq!(buf.len(), before);
    }

    // ---- Test 6: unmasked client frame is a protocol error -----------------
    #[test]
    fn unmasked_client_frame_is_protocol_error() {
        // FIN+Text, MASK bit clear, len 5, then 5 plaintext bytes.
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[0x81, 0x05]);
        buf.extend_from_slice(b"Hello");
        let err = parse(&mut buf, 1 << 20).unwrap_err();
        assert_eq!(err, ParseError::Protocol("unmasked client frame"));
    }

    // ---- Test 7: max_payload exceeded --------------------------------------
    #[test]
    fn max_payload_exceeded_is_too_large() {
        let payload = vec![0u8; 300];
        let key = [1, 2, 3, 4];
        let mut buf = masked_frame(true, OpCode::Binary, key, &payload);
        assert_eq!(parse(&mut buf, 256).unwrap_err(), ParseError::TooLarge);
    }

    // ---- Test 8: control-frame constraints ---------------------------------
    #[test]
    fn control_frame_over_125_is_protocol_error() {
        // Ping with declared length 126 (extended) is illegal for a control frame.
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[0x89, 0x80 | 126]); // FIN+Ping, MASK+126
        buf.extend_from_slice(&(200u16).to_be_bytes());
        buf.extend_from_slice(&[0, 0, 0, 0]); // mask key
        let err = parse(&mut buf, 1 << 20).unwrap_err();
        assert_eq!(err, ParseError::Protocol("control frame payload > 125"));
    }

    #[test]
    fn non_fin_control_frame_is_protocol_error() {
        // Fragmented Ping (FIN clear) is illegal.
        let key = [9, 8, 7, 6];
        let mut buf = masked_frame(false, OpCode::Ping, key, b"hi");
        let err = parse(&mut buf, 1 << 20).unwrap_err();
        assert_eq!(err, ParseError::Protocol("fragmented control frame"));
    }

    // ---- Test 9: Ping / Pong / Close parse ---------------------------------
    #[test]
    fn control_opcodes_parse() {
        for op in [OpCode::Ping, OpCode::Pong, OpCode::Close] {
            let key = [0x0a, 0x0b, 0x0c, 0x0d];
            let mut buf = masked_frame(true, op, key, b"by");
            let frame = parse(&mut buf, 1 << 20).expect("parse control");
            assert_eq!(frame.opcode, op);
            assert!(frame.fin);
            assert_eq!(&frame.payload[..], b"by");
            assert!(buf.is_empty());
        }
    }

    // ---- Test 10: mask correctness over a 1000-byte payload ----------------
    #[test]
    fn mask_round_trip_1000_bytes() {
        let payload: Vec<u8> = (0..1000u32).map(|i| (i * 31) as u8).collect();
        let key = [0x5a, 0x3c, 0xf1, 0x07];
        let mut buf = masked_frame(true, OpCode::Binary, key, &payload);
        let frame = parse(&mut buf, 1 << 20).expect("parse 1000");
        assert_eq!(frame.payload.len(), 1000);
        assert_eq!(&frame.payload[..], &payload[..]);
    }

    // ---- Extra: unmask helper word/tail boundaries (lengths 0..=20) --------
    #[test]
    fn unmask_helper_all_lengths() {
        let key = [0x11, 0x77, 0xab, 0xfe];
        for len in 0..=20usize {
            let original: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(97)).collect();
            // Mask, then unmask, and confirm we recover the original.
            let mut masked: Vec<u8> = original
                .iter()
                .enumerate()
                .map(|(i, &b)| b ^ key[i % 4])
                .collect();
            unmask(&mut masked, key);
            assert_eq!(masked, original, "len {len}");
        }
    }

    // ---- Extra: server encode picks the right length form ------------------
    #[test]
    fn encode_length_forms() {
        // 125 -> inline.
        let mut out = BytesMut::new();
        encode(&mut out, true, OpCode::Binary, &[0u8; 125]);
        assert_eq!(out[1], 125);

        // 126 -> 16-bit extended.
        let mut out = BytesMut::new();
        encode(&mut out, true, OpCode::Binary, &[0u8; 126]);
        assert_eq!(out[1], 126);
        assert_eq!(u16::from_be_bytes([out[2], out[3]]), 126);

        // 65536 -> 64-bit extended.
        let mut out = BytesMut::new();
        encode(&mut out, true, OpCode::Binary, &vec![0u8; 65_536]);
        assert_eq!(out[1], 127);
        let len = u64::from_be_bytes([
            out[2], out[3], out[4], out[5], out[6], out[7], out[8], out[9],
        ]);
        assert_eq!(len, 65_536);
    }
}
