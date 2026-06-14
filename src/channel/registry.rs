//! Per-(app, channel) registry over `ChannelState`. Stores mailbox handles +
//! presence membership, never sockets. Private store behind `LocalAdapter`.

use crate::channel::outcome::{ChannelSummary, SubscribeOutcome, UnsubscribeOutcome};
use crate::channel::state::ChannelState;
use crate::connection::handle::ConnectionHandle;
use crate::presence::member::PresenceMember;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use dashmap::DashMap;

#[derive(Default)]
pub struct Registry {
    channels: DashMap<(String, String), ChannelState>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(
        &self,
        app: &str,
        channel: &str,
        handle: ConnectionHandle,
        member: Option<PresenceMember>,
    ) -> SubscribeOutcome {
        let key = (app.to_string(), channel.to_string());
        let mut state = self.channels.entry(key).or_default();
        let was_empty = state.subscription_count() == 0;
        let presence = state.add(handle, member);
        let count = state.subscription_count();
        SubscribeOutcome {
            subscription_count: count,
            presence,
            occupied: was_empty && count == 1,
        }
    }

    pub fn unsubscribe(
        &self,
        app: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> UnsubscribeOutcome {
        let key = (app.to_string(), channel.to_string());
        let (count, presence, now_empty) = match self.channels.get_mut(&key) {
            Some(mut state) => {
                let presence = state.remove(socket_id);
                (state.subscription_count(), presence, state.is_empty())
            }
            None => (0, None, false),
        };
        if now_empty {
            self.channels.remove_if(&key, |_, s| s.is_empty());
        }
        UnsubscribeOutcome {
            subscription_count: count,
            presence,
            vacated: now_empty,
        }
    }

    pub fn broadcast(
        &self,
        app: &str,
        channel: &str,
        event: &ServerEvent,
        except: Option<&SocketId>,
    ) {
        if let Some(state) = self.channels.get(&(app.to_string(), channel.to_string())) {
            state.broadcast(event, except);
        }
    }

    pub fn channel_summary(&self, app: &str, channel: &str) -> ChannelSummary {
        match self.channels.get(&(app.to_string(), channel.to_string())) {
            Some(s) => ChannelSummary {
                name: channel.to_string(),
                occupied: !s.is_empty(),
                subscription_count: s.subscription_count(),
                user_count: s.user_count(),
            },
            None => ChannelSummary {
                name: channel.to_string(),
                occupied: false,
                subscription_count: 0,
                user_count: None,
            },
        }
    }

    pub fn channels(&self, app: &str, prefix: Option<&str>) -> Vec<ChannelSummary> {
        self.channels
            .iter()
            .filter(|e| e.key().0 == app && !e.value().is_empty())
            .filter(|e| prefix.is_none_or(|p| e.key().1.starts_with(p)))
            .map(|e| ChannelSummary {
                name: e.key().1.clone(),
                occupied: true,
                subscription_count: e.value().subscription_count(),
                user_count: e.value().user_count(),
            })
            .collect()
    }

    pub fn presence_members(&self, app: &str, channel: &str) -> Vec<PresenceMember> {
        self.channels
            .get(&(app.to_string(), channel.to_string()))
            .map(|s| s.members())
            .unwrap_or_default()
    }

    /// Every local subscription as `(app, channel, socket_id)`, across all channels.
    /// One tuple per socket. Used by the Redis adapter's membership TTL heartbeat to
    /// re-stamp each local member's `expireAt` so live nodes never expire.
    pub fn local_members(&self) -> Vec<(String, String, SocketId)> {
        let mut out = Vec::new();
        for entry in self.channels.iter() {
            let (app, channel) = entry.key();
            for sid in entry.value().socket_ids() {
                out.push((app.clone(), channel.clone(), sid));
            }
        }
        out
    }

    /// Number of tracked `(app, channel)` entries. Test-only.
    #[cfg(test)]
    pub fn channel_entry_count(&self) -> usize {
        self.channels.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::event::ServerEvent;
    use tokio::sync::mpsc;

    fn handle() -> (ConnectionHandle, mpsc::UnboundedReceiver<ServerEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            ConnectionHandle {
                socket_id: SocketId::generate(),
                mailbox: tx,
            },
            rx,
        )
    }

    #[test]
    fn subscribe_counts_and_broadcasts_excluding_sender() {
        let reg = Registry::new();
        let (h1, mut rx1) = handle();
        let (h2, mut rx2) = handle();
        let sid1 = h1.socket_id.clone();
        assert_eq!(reg.subscribe("app", "c", h1, None).subscription_count, 1);
        assert_eq!(reg.subscribe("app", "c", h2, None).subscription_count, 2);
        reg.broadcast("app", "c", &ServerEvent::Pong, Some(&sid1));
        assert!(rx1.try_recv().is_err());
        // `broadcast` encodes once and fans out `Raw` frames; the excluded sender
        // (`sid1`) still gets nothing, and the other subscriber receives the
        // verbatim wire frame for `Pong`.
        match rx2.try_recv() {
            Ok(ServerEvent::Raw(f)) => {
                assert_eq!(&*f, crate::protocol::v7::frames::encode(&ServerEvent::Pong))
            }
            other => panic!("expected Raw(Pong), got {other:?}"),
        }
    }

    #[test]
    fn unsubscribe_prunes_empty_channel() {
        let reg = Registry::new();
        let (h, _rx) = handle();
        let sid = h.socket_id.clone();
        reg.subscribe("app", "c", h, None);
        let out = reg.unsubscribe("app", "c", &sid);
        assert_eq!(out.subscription_count, 0);
        assert_eq!(
            reg.channel_entry_count(),
            0,
            "empty channel entry must be pruned"
        );
    }

    #[test]
    fn first_subscriber_sets_occupied_true_then_false() {
        let reg = Registry::new();
        let (h1, _r1) = handle();
        let (h2, _r2) = handle();
        // 0 -> 1 : occupied
        assert!(reg.subscribe("app", "c", h1, None).occupied);
        // 1 -> 2 : NOT an occupancy edge
        assert!(!reg.subscribe("app", "c", h2, None).occupied);
    }

    #[test]
    fn last_unsubscribe_sets_vacated_true_only_on_zero() {
        let reg = Registry::new();
        let (h1, _r1) = handle();
        let (h2, _r2) = handle();
        let s1 = h1.socket_id.clone();
        let s2 = h2.socket_id.clone();
        reg.subscribe("app", "c", h1, None);
        reg.subscribe("app", "c", h2, None);
        // 2 -> 1 : not vacated
        assert!(!reg.unsubscribe("app", "c", &s1).vacated);
        // 1 -> 0 : vacated
        assert!(reg.unsubscribe("app", "c", &s2).vacated);
    }

    #[test]
    fn unsubscribe_unknown_channel_is_not_vacated() {
        let reg = Registry::new();
        let sid = SocketId::generate();
        assert!(!reg.unsubscribe("app", "missing", &sid).vacated);
    }

    #[test]
    fn local_members_enumerates_every_subscription_across_channels() {
        let reg = Registry::new();
        let (h1, _r1) = handle();
        let (h2, _r2) = handle();
        let (h3, _r3) = handle();
        let s1 = h1.socket_id.clone();
        let s2 = h2.socket_id.clone();
        let s3 = h3.socket_id.clone();
        // Two channels: "c1" has two sockets, "c2" has one.
        reg.subscribe("app", "c1", h1, None);
        reg.subscribe("app", "c1", h2, None);
        reg.subscribe("app", "c2", h3, None);

        let mut got = reg.local_members();
        got.sort();
        let mut want = vec![
            ("app".to_string(), "c1".to_string(), s1),
            ("app".to_string(), "c1".to_string(), s2),
            ("app".to_string(), "c2".to_string(), s3),
        ];
        want.sort();
        assert_eq!(got, want);
    }

    #[test]
    fn channels_query_filters_by_prefix() {
        let reg = Registry::new();
        let (h1, _r1) = handle();
        let (h2, _r2) = handle();
        reg.subscribe("app", "private-a", h1, None);
        reg.subscribe("app", "public-b", h2, None);
        let names: Vec<String> = reg
            .channels("app", Some("private-"))
            .into_iter()
            .map(|s| s.name)
            .collect();
        assert_eq!(names, vec!["private-a".to_string()]);
    }
}
