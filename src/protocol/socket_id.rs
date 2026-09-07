//! `socket_id` = two random integers joined by `.` (e.g. `123.456`).

use rand::Rng;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SocketId {
    buf: [u8; Self::CAPACITY],
    len: u8,
}

impl SocketId {
    /// Longest `socket_id` this holds; [`Self::from_raw`] truncates past it.
    pub const CAPACITY: usize = 24;

    /// Each half is drawn from `[1, 10^10)` — large enough to be unguessable.
    pub fn generate() -> Self {
        let mut rng = rand::thread_rng();
        let a: u64 = rng.gen_range(1..10_000_000_000);
        let b: u64 = rng.gen_range(1..10_000_000_000);
        Self::from_raw(format!("{a}.{b}"))
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.buf[..self.len as usize]).unwrap_or("")
    }

    /// Build a `SocketId` from a client-supplied string (e.g. a REST `socket_id`).
    pub fn from_raw(raw: impl AsRef<str>) -> Self {
        let s = raw.as_ref().as_bytes();
        let n = s.len().min(Self::CAPACITY);
        let mut buf = [0u8; Self::CAPACITY];
        buf[..n].copy_from_slice(&s[..n]);
        Self { buf, len: n as u8 }
    }
}

impl fmt::Display for SocketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_id_has_two_dotted_integers() {
        let id = SocketId::generate();
        let parts: Vec<&str> = id.as_str().split('.').collect();
        assert_eq!(parts.len(), 2, "socket_id was {id}");
        assert!(parts[0].parse::<u64>().is_ok());
        assert!(parts[1].parse::<u64>().is_ok());
    }

    #[test]
    fn generated_ids_are_distinct() {
        let a = SocketId::generate();
        let b = SocketId::generate();
        assert_ne!(a, b);
    }

    #[test]
    fn from_raw_round_trips() {
        let s = SocketId::from_raw("123.456");
        assert_eq!(s.as_str(), "123.456");
    }
}
