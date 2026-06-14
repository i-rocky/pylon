//! Pure debounce/coalescing over one flush batch (spec §5). 1:1 cancellation of
//! opposing transitions; client_event/cache_miss never coalesced; order preserved.

use crate::webhook::event::WebhookEvent;

/// Collapse opposing pairs within a single app's batch:
/// - `channel_occupied` ↔ `channel_vacated` on the same channel cancel 1:1.
/// - `member_added` ↔ `member_removed` on the same `(channel, user_id)` cancel 1:1.
///
/// `client_event` and `cache_miss` pass through untouched. Enqueue order is
/// preserved among the survivors.
pub fn coalesce(events: Vec<WebhookEvent>) -> Vec<WebhookEvent> {
    // Net count per coalescing key: +1 for the "add" side, -1 for the "remove"
    // side. A key nets to 0 ⇒ both sides dropped; >0 ⇒ keep that many adds;
    // <0 ⇒ keep that many removes. Survivors are emitted in first-seen order.
    use std::collections::HashMap;

    #[derive(PartialEq, Eq, Hash, Clone)]
    enum Key {
        Channel(String),        // occupied(+) / vacated(-)
        Member(String, String), // (channel,user) added(+) / removed(-)
    }

    fn key_of(e: &WebhookEvent) -> Option<(Key, i32)> {
        match e {
            WebhookEvent::ChannelOccupied { channel, .. } => {
                Some((Key::Channel(channel.clone()), 1))
            }
            WebhookEvent::ChannelVacated { channel, .. } => {
                Some((Key::Channel(channel.clone()), -1))
            }
            WebhookEvent::MemberAdded {
                channel, user_id, ..
            } => Some((Key::Member(channel.clone(), user_id.clone()), 1)),
            WebhookEvent::MemberRemoved {
                channel, user_id, ..
            } => Some((Key::Member(channel.clone(), user_id.clone()), -1)),
            // never coalesced:
            WebhookEvent::ClientEvent { .. } | WebhookEvent::CacheMiss { .. } => None,
        }
    }

    // First pass: compute net per coalescing key.
    let mut net: HashMap<Key, i32> = HashMap::new();
    for e in &events {
        if let Some((k, sign)) = key_of(e) {
            *net.entry(k).or_insert(0) += sign;
        }
    }

    // Second pass: walk in order; emit a coalescable event only while the net for
    // its key still has budget on that event's side (drains toward zero).
    let mut out = Vec::with_capacity(events.len());
    for e in events {
        match key_of(&e) {
            None => out.push(e), // client_event / cache_miss
            Some((k, sign)) => {
                let n = net.get_mut(&k).expect("key was counted");
                if sign > 0 && *n > 0 {
                    *n -= 1;
                    out.push(e);
                } else if sign < 0 && *n < 0 {
                    *n += 1;
                    out.push(e);
                }
                // otherwise this event is cancelled (dropped)
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn occ(ch: &str) -> WebhookEvent {
        WebhookEvent::ChannelOccupied {
            app: "a".into(),
            channel: ch.into(),
        }
    }
    fn vac(ch: &str) -> WebhookEvent {
        WebhookEvent::ChannelVacated {
            app: "a".into(),
            channel: ch.into(),
        }
    }
    fn add(ch: &str, u: &str) -> WebhookEvent {
        WebhookEvent::MemberAdded {
            app: "a".into(),
            channel: ch.into(),
            user_id: u.into(),
        }
    }
    fn rem(ch: &str, u: &str) -> WebhookEvent {
        WebhookEvent::MemberRemoved {
            app: "a".into(),
            channel: ch.into(),
            user_id: u.into(),
        }
    }
    fn ce(ch: &str) -> WebhookEvent {
        WebhookEvent::ClientEvent {
            app: "a".into(),
            channel: ch.into(),
            event: "m".into(),
            data: json!({}),
            socket_id: "1.1".into(),
            user_id: None,
        }
    }
    fn miss(ch: &str) -> WebhookEvent {
        WebhookEvent::CacheMiss {
            app: "a".into(),
            channel: ch.into(),
        }
    }

    #[test]
    fn occupied_and_vacated_same_channel_cancel() {
        assert!(coalesce(vec![occ("c"), vac("c")]).is_empty());
        assert!(coalesce(vec![vac("c"), occ("c")]).is_empty());
    }

    #[test]
    fn member_add_remove_same_channel_user_cancel() {
        assert!(coalesce(vec![add("presence-c", "u1"), rem("presence-c", "u1")]).is_empty());
    }

    #[test]
    fn different_channel_does_not_cancel() {
        let out = coalesce(vec![occ("a"), vac("b")]);
        assert_eq!(out, vec![occ("a"), vac("b")]);
    }

    #[test]
    fn different_user_does_not_cancel() {
        let out = coalesce(vec![add("presence-c", "u1"), rem("presence-c", "u2")]);
        assert_eq!(out, vec![add("presence-c", "u1"), rem("presence-c", "u2")]);
    }

    #[test]
    fn vacated_alone_survives_when_occupied_was_before_window() {
        // Only a vacated arrives this window — it must survive.
        assert_eq!(coalesce(vec![vac("c")]), vec![vac("c")]);
    }

    #[test]
    fn client_event_and_cache_miss_never_coalesced() {
        let out = coalesce(vec![ce("private-c"), ce("private-c"), miss("cache-c")]);
        assert_eq!(out, vec![ce("private-c"), ce("private-c"), miss("cache-c")]);
    }

    #[test]
    fn enqueue_order_preserved_among_survivors() {
        // occ(a), occ(b), vac(a) -> a cancels, b survives, order: occ(b).
        let out = coalesce(vec![occ("a"), occ("b"), vac("a")]);
        assert_eq!(out, vec![occ("b")]);
    }
}
