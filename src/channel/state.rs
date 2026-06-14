//! State for one `(app, channel)`: its subscribers and (for presence) the
//! distinct-user roster with reference counting for join/leave deduplication.

use crate::channel::outcome::{PresenceJoin, PresenceLeave};
use crate::connection::handle::ConnectionHandle;
use crate::presence::member::PresenceMember;
use crate::protocol::event::{PresencePayload, ServerEvent};
use crate::protocol::socket_id::SocketId;
use serde_json::Value;
use std::collections::HashMap;

struct Subscriber {
    handle: ConnectionHandle,
    member: Option<PresenceMember>,
}

struct PresenceUser {
    user_info: Value,
    conn_count: usize,
}

#[derive(Default)]
pub struct ChannelState {
    subscribers: HashMap<SocketId, Subscriber>,
    users: HashMap<String, PresenceUser>, // user_id -> info + live connection count
}

impl ChannelState {
    /// Add a subscriber. Returns `Some(PresenceJoin)` for presence channels.
    pub fn add(
        &mut self,
        handle: ConnectionHandle,
        member: Option<PresenceMember>,
    ) -> Option<PresenceJoin> {
        let socket_id = handle.socket_id.clone();
        let join = member.as_ref().map(|m| {
            let first_for_user = !self.users.contains_key(&m.user_id);
            let u = self
                .users
                .entry(m.user_id.clone())
                .or_insert_with(|| PresenceUser {
                    user_info: m.user_info.clone(),
                    conn_count: 0,
                });
            u.conn_count += 1;
            PresenceJoin {
                first_for_user,
                roster: PresencePayload::default(), // filled below after insert
                member: m.clone(),
            }
        });
        self.subscribers
            .insert(socket_id, Subscriber { handle, member });
        join.map(|mut j| {
            j.roster = self.roster();
            j
        })
    }

    /// Remove a subscriber by socket id. Returns `Some(PresenceLeave)` if it was a
    /// presence member (with `last_for_user` set when its last connection left).
    pub fn remove(&mut self, socket_id: &SocketId) -> Option<PresenceLeave> {
        let sub = self.subscribers.remove(socket_id)?;
        let member = sub.member?;
        let last_for_user = match self.users.get_mut(&member.user_id) {
            Some(u) => {
                u.conn_count -= 1;
                if u.conn_count == 0 {
                    self.users.remove(&member.user_id);
                    true
                } else {
                    false
                }
            }
            None => true,
        };
        Some(PresenceLeave {
            last_for_user,
            user_id: member.user_id,
        })
    }

    pub fn subscription_count(&self) -> usize {
        self.subscribers.len()
    }

    /// Socket ids of every current subscriber. Used to enumerate local members for
    /// the membership TTL heartbeat (each gets its `expireAt` re-stamped in Redis).
    pub fn socket_ids(&self) -> Vec<SocketId> {
        self.subscribers.keys().cloned().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.subscribers.is_empty()
    }

    /// Distinct-user count (presence) — `None` for non-presence channels.
    pub fn user_count(&self) -> Option<usize> {
        if self.users.is_empty() {
            None
        } else {
            Some(self.users.len())
        }
    }

    /// Build the presence roster: sorted ids, id->user_info hash, distinct count.
    pub fn roster(&self) -> PresencePayload {
        let mut ids: Vec<String> = self.users.keys().cloned().collect();
        ids.sort();
        let mut hash = serde_json::Map::new();
        for id in &ids {
            hash.insert(id.clone(), self.users[id].user_info.clone());
        }
        PresencePayload {
            count: ids.len(),
            ids,
            hash,
        }
    }

    pub fn members(&self) -> Vec<PresenceMember> {
        let mut ids: Vec<String> = self.users.keys().cloned().collect();
        ids.sort();
        ids.into_iter()
            .map(|id| PresenceMember {
                user_info: self.users[&id].user_info.clone(),
                user_id: id,
            })
            .collect()
    }

    /// Deliver `event` to every subscriber's mailbox except `except`.
    ///
    /// Encode the wire frame ONCE here and fan out cheap `Arc<str>` clones rather
    /// than re-encoding in every connection task (was N encodes + N deep clones for
    /// N local subscribers). If the event is already pre-encoded (`Raw`, e.g. a
    /// broadcast relayed verbatim from another node), reuse its `Arc`. Control
    /// events (`Close`) never reach `broadcast`.
    pub fn broadcast(&self, event: &ServerEvent, except: Option<&SocketId>) {
        let frame: std::sync::Arc<str> = match event {
            ServerEvent::Raw(f) => f.clone(),
            other => std::sync::Arc::from(crate::protocol::v7::frames::encode(other).as_str()),
        };
        for (sid, sub) in &self.subscribers {
            if Some(sid) == except {
                continue;
            }
            let _ = sub.handle.mailbox.send(ServerEvent::Raw(frame.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn handle() -> ConnectionHandle {
        let (tx, _rx) = mpsc::unbounded_channel();
        ConnectionHandle {
            socket_id: SocketId::generate(),
            mailbox: tx,
        }
    }

    fn handle_with_rx() -> (ConnectionHandle, mpsc::UnboundedReceiver<ServerEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            ConnectionHandle {
                socket_id: SocketId::generate(),
                mailbox: tx,
            },
            rx,
        )
    }

    fn member(user_id: &str) -> PresenceMember {
        PresenceMember {
            user_id: user_id.into(),
            user_info: serde_json::json!({"n": user_id}),
        }
    }

    #[test]
    fn public_add_remove_counts() {
        let mut s = ChannelState::default();
        let h = handle();
        let sid = h.socket_id.clone();
        assert!(s.add(h, None).is_none());
        assert_eq!(s.subscription_count(), 1);
        assert!(s.user_count().is_none());
        assert!(s.remove(&sid).is_none());
        assert!(s.is_empty());
    }

    #[test]
    fn presence_dedup_same_user_two_connections() {
        let mut s = ChannelState::default();
        let (h1, h2) = (handle(), handle());
        let (s1, s2) = (h1.socket_id.clone(), h2.socket_id.clone());

        let j1 = s.add(h1, Some(member("u1"))).unwrap();
        assert!(j1.first_for_user);
        assert_eq!(j1.roster.count, 1);

        let j2 = s.add(h2, Some(member("u1"))).unwrap();
        assert!(
            !j2.first_for_user,
            "second connection of same user is not first"
        );
        assert_eq!(s.user_count(), Some(1));
        assert_eq!(s.subscription_count(), 2);

        let l1 = s.remove(&s1).unwrap();
        assert!(!l1.last_for_user, "user still has a connection");
        let l2 = s.remove(&s2).unwrap();
        assert!(l2.last_for_user);
        assert_eq!(l2.user_id, "u1");
        assert_eq!(s.user_count(), None);
    }

    #[test]
    fn broadcast_encodes_once_and_fans_out_raw() {
        let mut s = ChannelState::default();
        let (h1, mut rx1) = handle_with_rx();
        let (h2, mut rx2) = handle_with_rx();
        s.add(h1, None);
        s.add(h2, None);

        let original = ServerEvent::ChannelEvent {
            channel: "my-channel".into(),
            event: "my-event".into(),
            data: serde_json::json!({"x": 1}),
            user_id: None,
        };
        let expected = crate::protocol::v7::frames::encode(&original);

        s.broadcast(&original, None);

        for rx in [&mut rx1, &mut rx2] {
            match rx.try_recv().expect("subscriber received a frame") {
                ServerEvent::Raw(f) => assert_eq!(&*f, expected.as_str()),
                other => panic!("expected Raw, got {other:?}"),
            }
        }
    }

    #[test]
    fn roster_sorted_and_distinct() {
        let mut s = ChannelState::default();
        s.add(handle(), Some(member("b")));
        s.add(handle(), Some(member("a")));
        let r = s.roster();
        assert_eq!(r.ids, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(r.count, 2);
    }
}
