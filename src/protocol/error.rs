//! Pusher connection error / close codes (see protocol spec §error codes).

/// A `pusher:error` payload. Note: unlike most frames, its `data` is encoded as
/// a plain JSON object, not a double-encoded string (handled by the v7 codec).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PusherError {
    pub code: u16,
    pub message: String,
}

impl PusherError {
    pub fn new(code: u16, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
    pub fn app_not_found() -> Self {
        Self::new(4001, "Could not find app by key")
    }
    pub fn over_capacity() -> Self {
        Self::new(4004, "App connection limit reached")
    }
    pub fn invalid_version() -> Self {
        Self::new(4006, "Invalid version string format")
    }
    pub fn unsupported_protocol() -> Self {
        Self::new(4007, "Unsupported protocol version")
    }
    pub fn no_protocol() -> Self {
        Self::new(4008, "No protocol version supplied")
    }
    pub fn server_over_capacity() -> Self {
        Self::new(4100, "Server is over capacity")
    }
    /// Transient: app store unreachable. Client reconnects after backoff (41xx range).
    pub fn backend_unavailable() -> Self {
        Self::new(4103, "application store temporarily unavailable")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_carry_spec_codes() {
        assert_eq!(PusherError::app_not_found().code, 4001);
        assert_eq!(PusherError::over_capacity().code, 4004);
        assert_eq!(PusherError::invalid_version().code, 4006);
        assert_eq!(PusherError::unsupported_protocol().code, 4007);
        assert_eq!(PusherError::no_protocol().code, 4008);
        assert!(!PusherError::app_not_found().message.is_empty());
    }

    #[test]
    fn server_over_capacity_carries_4100() {
        assert_eq!(PusherError::server_over_capacity().code, 4100);
        assert!(!PusherError::server_over_capacity().message.is_empty());
    }

    #[test]
    fn invalid_version_message_matches_spec() {
        assert_eq!(
            PusherError::invalid_version().message,
            "Invalid version string format"
        );
    }

    #[test]
    fn backend_unavailable_carries_4103() {
        assert_eq!(PusherError::backend_unavailable().code, 4103);
        assert!(!PusherError::backend_unavailable().message.is_empty());
    }
}
