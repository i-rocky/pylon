//! Values returned by channel-state mutations and queries. Produced by the
//! registry, returned by the `Adapter` trait — so they live in a neutral module.

use crate::presence::member::PresenceMember;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeOutcome {
    pub subscription_count: usize,
    pub presence: Option<PresenceJoin>,
    /// True iff this subscribe took the channel from 0 → 1 subscribers.
    pub occupied: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PresenceJoin {
    pub first_for_user: bool,
    /// The pre-encoded `pusher_internal:subscription_succeeded` wire frame for
    /// the roster generation this join landed in: encoded ONCE per distinct-user
    /// set (membership generation) in the channel state and shared (`Arc`) by
    /// every join of that generation (F-5) — no per-join roster clone + encode.
    /// The clustered subscribe REPLACES it with the freshly-encoded
    /// cluster-truth frame; the node-local cache is untouched (it tracks only
    /// node-local membership).
    pub roster_frame: Arc<str>,
    pub member: PresenceMember,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnsubscribeOutcome {
    pub subscription_count: usize,
    pub presence: Option<PresenceLeave>,
    /// True iff this unsubscribe took the channel from 1 → 0 subscribers.
    pub vacated: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PresenceLeave {
    pub last_for_user: bool,
    pub user_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChannelSummary {
    pub name: String,
    pub occupied: bool,
    pub subscription_count: usize,
    pub user_count: Option<usize>,
}
