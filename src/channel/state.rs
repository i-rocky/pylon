//! State for one `(app, channel)`: its subscribers and (for presence) the
//! distinct-user roster with reference counting for join/leave deduplication.

use crate::channel::outcome::{PresenceJoin, PresenceLeave};
use crate::connection::handle::ConnectionHandle;
use crate::presence::member::PresenceMember;
use crate::protocol::event::{PresencePayload, ServerEvent};
use crate::protocol::socket_id::SocketId;
use rayon::prelude::*;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

/// Above this subscriber count, `broadcast` fans the per-mailbox enqueue out
/// across the rayon pool; at or below it the serial loop is cheaper than the
/// pool dispatch overhead (presence/small channels stay serial).
const PARALLEL_THRESHOLD: usize = 256;

/// Subscribers per rayon job in the parallel fan-out. Sized so each job amortizes
/// the work-stealing dispatch cost over a batch of (cheap) mailbox sends while
/// still producing enough jobs to spread across the pool at N≫threshold.
const SEND_CHUNK: usize = 512;

struct Subscriber {
    handle: ConnectionHandle,
    member: Option<PresenceMember>,
}

struct PresenceUser {
    user_info: Value,
    conn_count: usize,
}

/// Shared membership snapshot for fan-out: every `(socket_id, subscriber)` pair
/// of a channel, behind one `Arc` so a broadcast can detach from the registry
/// shard lock with a single refcount bump (F7).
type SharedSnapshot = Arc<[(SocketId, Arc<Subscriber>)]>;

#[derive(Default)]
pub struct ChannelState {
    subscribers: HashMap<SocketId, Arc<Subscriber>>,
    /// Distinct presence users (user_id -> info + live connection count) in a
    /// `BTreeMap`: the map keeps the user ids in SORTED order incrementally on
    /// every add/remove, so the roster walk below is already ordered — no
    /// per-join `keys()` collect + re-sort, no unsorted scatter pass (F8). The
    /// per-join allocation left is exactly the one owned `PresencePayload`
    /// (`ids` Vec + `hash` Map with cloned user_info) that the join-outcome
    /// API requires.
    users: BTreeMap<String, PresenceUser>,
    /// Membership snapshot for fan-out, rebuilt lazily: `add`/`remove` reset it,
    /// the next `fanout` rebuilds it under the caller's registry shard guard.
    /// Retention bound: every membership change resets it, so a channel that
    /// keeps ≥1 member holds at most ONE stale generation here — freed by the
    /// next rebuild, or with the whole state when a vacate prunes the channel
    /// (in-flight `Fanout`s keep one clone alive only until their `send`
    /// completes).
    snapshot: OnceLock<SharedSnapshot>,
}

impl ChannelState {
    /// Add a subscriber. Returns `Some(PresenceJoin)` for presence channels.
    pub fn add(
        &mut self,
        handle: ConnectionHandle,
        member: Option<PresenceMember>,
    ) -> Option<PresenceJoin> {
        let socket_id = handle.socket_id;
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
            .insert(socket_id, Arc::new(Subscriber { handle, member }));
        self.snapshot.take(); // membership changed: next fan-out rebuilds
        join.map(|mut j| {
            j.roster = self.roster();
            j
        })
    }

    /// Remove a subscriber by socket id. Returns `Some(PresenceLeave)` if it was a
    /// presence member (with `last_for_user` set when its last connection left).
    pub fn remove(&mut self, socket_id: &SocketId) -> Option<PresenceLeave> {
        let sub = self.subscribers.remove(socket_id)?;
        self.snapshot.take(); // membership changed: next fan-out rebuilds
        let member = sub.member.clone()?;
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
    /// One ordered walk of `users` — the BTreeMap maintains the id order
    /// incrementally on add/remove, so `ids`, the `hash` keys and `count` agree
    /// by construction and no per-join re-sort happens (F8).
    pub fn roster(&self) -> PresencePayload {
        let hash: serde_json::Map<String, Value> = self
            .users
            .iter()
            .map(|(id, u)| (id.clone(), u.user_info.clone()))
            .collect();
        PresencePayload {
            count: self.users.len(),
            ids: self.users.keys().cloned().collect(),
            hash,
        }
    }

    pub fn members(&self) -> Vec<PresenceMember> {
        self.users
            .iter()
            .map(|(id, u)| PresenceMember {
                user_id: id.clone(),
                user_info: u.user_info.clone(),
            })
            .collect()
    }

    /// Capture this channel's half of a broadcast as an owned [`Fanout`]
    /// snapshot: the wire frame encoded ONCE here (reusing a pre-encoded
    /// `Raw` event's `Arc` rather than re-encoding) plus the channel's shared
    /// membership snapshot (rebuilt here under the caller's guard iff
    /// membership changed since the last fan-out; `except` is applied at send
    /// time). Control events (`Close`) never reach this path.
    ///
    /// MUST be called while the caller holds the registry shard read guard;
    /// the returned snapshot is then executed via [`Fanout::send`] AFTER that
    /// guard is dropped, so the shard is never held across mailbox enqueues
    /// (finding F7 — see `Registry::broadcast`).
    ///
    /// The snapshot is exactly the guard-time subscriber set: collect-then-send
    /// races with concurrent subscribe/unsubscribe the same way the previous
    /// send-under-guard loop did — a member removed after the snapshot may or
    /// may not still receive the frame (at-most-once per mailbox, as ever).
    pub fn fanout<'a>(&self, event: &ServerEvent, except: Option<&'a SocketId>) -> Fanout<'a> {
        // One frame is shared by every subscriber of the channel, so today it
        // encodes at the sole active version — 7.3 replaces this with
        // per-version frames built on the `wire` seam.
        let frame: Arc<str> = match event {
            ServerEvent::Raw(f) => f.clone(),
            other => Arc::from(
                crate::protocol::wire::encode(crate::protocol::wire::ACTIVE_VERSIONS[0], other)
                    .as_str(),
            ),
        };
        Fanout {
            frame,
            except,
            snapshot: self
                .snapshot
                .get_or_init(|| {
                    let mut v = Vec::with_capacity(self.subscribers.len());
                    for (sid, sub) in &self.subscribers {
                        v.push((*sid, Arc::clone(sub)));
                    }
                    Arc::from(v.into_boxed_slice())
                })
                .clone(),
            // Path choice stays keyed on the FULL subscriber count so the
            // serial/parallel cutover is identical to the previous in-guard
            // loop.
            parallel: self.subscribers.len() > PARALLEL_THRESHOLD,
        }
    }
}

/// One broadcast's fan-out, detached from the registry shard lock (F7): the
/// frame encoded once plus a shared membership snapshot (one refcount for the
/// whole subscriber set, not one handle clone per subscriber per broadcast),
/// with the send path (serial vs rayon) chosen at snapshot time.
pub struct Fanout<'a> {
    frame: Arc<str>,
    snapshot: SharedSnapshot,
    except: Option<&'a SocketId>,
    parallel: bool,
}

impl Fanout<'_> {
    /// Deliver the frame to every snapshotted mailbox — serially for small
    /// snapshots, fanned out across the rayon work-stealing pool above
    /// `PARALLEL_THRESHOLD`. Runs with NO registry shard guard held (that is
    /// the point). For large channels the serial per-subscriber `mailbox.send`
    /// loop is the publish-side bottleneck (at N=10k it caps fan-out below the
    /// worker ceiling), hence the rayon path. This is correctness-safe:
    /// subscribers are keyed by `SocketId`, so each distinct mailbox appears in
    /// `snapshot` at most once and is sent to exactly once per broadcast — no
    /// two threads ever push to the same mailbox, and per-channel send ordering
    /// is preserved (a connection only receives via its own mailbox). Small
    /// broadcasts stay on the serial path so presence/small channels pay zero
    /// pool overhead.
    pub fn send(self) {
        let except = self.except;
        if !self.parallel {
            for (sid, sub) in &*self.snapshot {
                if Some(sid) == except {
                    continue;
                }
                let _ = sub
                    .handle
                    .mailbox
                    .send(ServerEvent::Raw(self.frame.clone()));
            }
            return;
        }
        // Chunk the fan-out so each rayon job does a meaningful batch of sends
        // (a single `Mailbox::send` is ~tens of ns; per-element rayon dispatch
        // would otherwise dominate). The frame `Arc` is cloned once per send.
        // Each `send` also marks its target dirty + wakes that connection's
        // worker (when the mailbox is wired).
        self.snapshot.par_chunks(SEND_CHUNK).for_each(|chunk| {
            for (sid, sub) in chunk {
                if Some(sid) == except {
                    continue;
                }
                let _ = sub
                    .handle
                    .mailbox
                    .send(ServerEvent::Raw(self.frame.clone()));
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn handle() -> ConnectionHandle {
        let (tx, _rx) = mpsc::channel(1024);
        ConnectionHandle {
            socket_id: SocketId::generate(),
            mailbox: crate::connection::handle::Mailbox::new(tx, None, None),
        }
    }

    fn handle_with_rx() -> (ConnectionHandle, mpsc::Receiver<Box<ServerEvent>>) {
        let (tx, rx) = mpsc::channel(1024);
        (
            ConnectionHandle {
                socket_id: SocketId::generate(),
                mailbox: crate::connection::handle::Mailbox::new(tx, None, None),
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
        let sid = h.socket_id;
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
        let (s1, s2) = (h1.socket_id, h2.socket_id);

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
        let expected = crate::protocol::wire::encode(7, &original);

        s.fanout(&original, None).send();

        for rx in [&mut rx1, &mut rx2] {
            match rx
                .try_recv()
                .map(|b| *b)
                .expect("subscriber received a frame")
            {
                ServerEvent::Raw(f) => assert_eq!(&*f, expected.as_str()),
                other => panic!("expected Raw, got {other:?}"),
            }
        }
    }

    #[test]
    fn broadcast_parallel_path_delivers_to_all_and_excludes_sender() {
        // > PARALLEL_THRESHOLD subscribers forces the rayon fan-out path; verify
        // every mailbox receives exactly the encoded frame and `except` is skipped.
        let mut s = ChannelState::default();
        let n = PARALLEL_THRESHOLD + 50;
        let mut rxs = Vec::with_capacity(n);
        let mut excluded_sid = None;
        for i in 0..n {
            let (h, rx) = handle_with_rx();
            if i == 0 {
                excluded_sid = Some(h.socket_id);
            }
            s.add(h, None);
            rxs.push((i, rx));
        }
        let except = excluded_sid.unwrap();

        let original = ServerEvent::ChannelEvent {
            channel: "big".into(),
            event: "ev".into(),
            data: serde_json::json!({"k": "v"}),
            user_id: None,
        };
        let expected = crate::protocol::wire::encode(7, &original);

        s.fanout(&original, Some(&except)).send();

        let mut delivered = 0;
        for (i, rx) in &mut rxs {
            match rx.try_recv().map(|b| *b) {
                Ok(ServerEvent::Raw(f)) => {
                    assert_eq!(&*f, expected.as_str());
                    delivered += 1;
                }
                Ok(other) => panic!("expected Raw, got {other:?}"),
                Err(_) if *i == 0 => {} // the excluded sender receives nothing
                Err(e) => panic!("subscriber {i} got no frame: {e:?}"),
            }
        }
        assert_eq!(
            delivered,
            n - 1,
            "every subscriber except `except` receives the frame exactly once"
        );
    }

    #[test]
    fn fanout_snapshot_is_the_guard_time_set_and_survives_later_mutation() {
        // F7 contract: the snapshot is taken under the registry shard guard and
        // the sends run after it is dropped — so a subscriber removed between
        // snapshot and send STILL receives the frame (the race the old
        // send-under-guard loop also allowed: removal could not complete before
        // the sends finished, but delivery was never conditioned on the entry
        // still being present). The NEXT fan-out must see the new membership:
        // removal invalidates the cached snapshot, which is rebuilt under the
        // next guard.
        let mut s = ChannelState::default();
        let (h1, mut rx1) = handle_with_rx();
        let (h2, mut rx2) = handle_with_rx();
        let s1 = h1.socket_id;
        s.add(h1, None);
        s.add(h2, None);

        let plan = s.fanout(&ServerEvent::Pong, None);
        s.remove(&s1); // unsubscribe lands between snapshot and send
        plan.send();

        for rx in [&mut rx1, &mut rx2] {
            match rx.try_recv().map(|b| *b) {
                Ok(ServerEvent::Raw(f)) => {
                    assert_eq!(&*f, crate::protocol::wire::encode(7, &ServerEvent::Pong))
                }
                other => panic!("expected Raw(Pong), got {other:?}"),
            }
        }

        // Snapshot after the mutation: exactly the remaining subscriber.
        s.fanout(&ServerEvent::Pong, None).send();
        assert!(rx1.try_recv().is_err(), "removed member gets nothing");
        assert!(matches!(
            rx2.try_recv().map(|b| *b),
            Ok(ServerEvent::Raw(_))
        ));
    }

    #[test]
    fn fanout_snapshot_excludes_subscriber_added_after_snapshot() {
        // Mirror of the remove-side contract above (add half): a subscriber
        // ADDED between snapshot and send is not in the guard-time set, so it
        // must NOT receive the pre-add frame — and the add resets the cached
        // snapshot, so the NEXT fan-out is rebuilt with the new member in.
        let mut s = ChannelState::default();
        let (h1, mut rx1) = handle_with_rx();
        s.add(h1, None);

        let plan = s.fanout(&ServerEvent::Pong, None);
        let (h2, mut rx2) = handle_with_rx();
        s.add(h2, None); // join lands between snapshot and send
        plan.send();

        // Guard-time set = {h1}: the pre-add frame went to h1 only.
        match rx1.try_recv().map(|b| *b) {
            Ok(ServerEvent::Raw(f)) => {
                assert_eq!(&*f, crate::protocol::wire::encode(7, &ServerEvent::Pong))
            }
            other => panic!("expected Raw(Pong), got {other:?}"),
        }
        assert!(
            rx2.try_recv().is_err(),
            "subscriber added after the snapshot must not receive the pre-add frame"
        );

        // Next fan-out rebuilds under the NEW membership: both receive.
        s.fanout(&ServerEvent::Ping, None).send();
        for rx in [&mut rx1, &mut rx2] {
            match rx.try_recv().map(|b| *b) {
                Ok(ServerEvent::Raw(f)) => {
                    assert_eq!(&*f, crate::protocol::wire::encode(7, &ServerEvent::Ping))
                }
                other => panic!("expected Raw(Ping), got {other:?}"),
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

    /// Golden roster bytes (F8 / Task 6.6): the `subscription_succeeded`
    /// presence payload must serialize to EXACTLY these bytes — ids in
    /// byte-sorted order, `hash` keys in the SAME order (the roster walks
    /// `users` in its incrementally-maintained sorted order and serde_json's
    /// workspace `preserve_order` feature keeps that insertion order),
    /// `count` = distinct users, `data` double-encoded as a string. Members
    /// join deliberately OUT of sorted order with escaping-worthy content
    /// (quotes, backslash, emoji, nested nulls/bools, a capital-letter id that
    /// sorts before lowercase), plus a duplicate-user second connection whose
    /// DIFFERENT `user_info` must NOT leak into the roster (the first
    /// connection's info wins), a partial removal that must not change the
    /// roster while the user still has a connection, and a MIDDLE full removal
    /// that must restore the pre-join bytes. Literals captured from the
    /// pre-refactor encoder: any byte drift (field order, id order, number
    /// formatting, escaping) is a parity regression.
    #[test]
    fn golden_roster_bytes_across_joins_and_removals() {
        let ch = "presence-golden";
        let frame = |roster: PresencePayload| {
            crate::protocol::wire::encode(
                7,
                &ServerEvent::SubscriptionSucceeded {
                    channel: ch.into(),
                    presence: Some(roster),
                },
            )
        };
        let rich =
            serde_json::json!({"name":"A \"quoted\" \\ back","emoji":"🚀","arr":[1,2,null,true]});
        let nested = serde_json::json!({"twelve":12,"nested":{"x":[{"y":null}]}});
        let weird = "we\"ird\\";
        let m = |id: &str, info: Value| PresenceMember {
            user_id: id.into(),
            user_info: info,
        };
        let mut s = ChannelState::default();

        // 1 member.
        let j = s
            .add(handle(), Some(m("zebra", serde_json::json!({"n":"z"}))))
            .unwrap();
        assert_eq!(
            frame(j.roster),
            r#"{"event":"pusher_internal:subscription_succeeded","channel":"presence-golden","data":"{\"presence\":{\"ids\":[\"zebra\"],\"hash\":{\"zebra\":{\"n\":\"z\"}},\"count\":1}}"}"#
        );

        // 2 members, joined out of order: ids come back sorted.
        let j = s.add(handle(), Some(m("alpha", rich))).unwrap();
        assert_eq!(
            frame(j.roster),
            r#"{"event":"pusher_internal:subscription_succeeded","channel":"presence-golden","data":"{\"presence\":{\"ids\":[\"alpha\",\"zebra\"],\"hash\":{\"alpha\":{\"name\":\"A \\\"quoted\\\" \\\\ back\",\"emoji\":\"🚀\",\"arr\":[1,2,null,true]},\"zebra\":{\"n\":\"z\"}},\"count\":2}}"}"#
        );

        // 3 members: capital "Mid" sorts before the lowercase ids (byte order).
        let three = r#"{"event":"pusher_internal:subscription_succeeded","channel":"presence-golden","data":"{\"presence\":{\"ids\":[\"Mid\",\"alpha\",\"zebra\"],\"hash\":{\"Mid\":{\"twelve\":12,\"nested\":{\"x\":[{\"y\":null}]}},\"alpha\":{\"name\":\"A \\\"quoted\\\" \\\\ back\",\"emoji\":\"🚀\",\"arr\":[1,2,null,true]},\"zebra\":{\"n\":\"z\"}},\"count\":3}}"}"#;
        let j = s.add(handle(), Some(m("Mid", nested))).unwrap();
        assert_eq!(frame(j.roster), three);

        // 4 members: an id that itself needs JSON escaping appears in `ids`
        // AND as a `hash` key, escaped identically in both.
        let hw = handle();
        let sid_w = hw.socket_id;
        let j = s
            .add(hw, Some(m(weird, serde_json::json!({"w":1}))))
            .unwrap();
        let four = r#"{"event":"pusher_internal:subscription_succeeded","channel":"presence-golden","data":"{\"presence\":{\"ids\":[\"Mid\",\"alpha\",\"we\\\"ird\\\\\",\"zebra\"],\"hash\":{\"Mid\":{\"twelve\":12,\"nested\":{\"x\":[{\"y\":null}]}},\"alpha\":{\"name\":\"A \\\"quoted\\\" \\\\ back\",\"emoji\":\"🚀\",\"arr\":[1,2,null,true]},\"we\\\"ird\\\\\":{\"w\":1},\"zebra\":{\"n\":\"z\"}},\"count\":4}}"}"#;
        assert_eq!(frame(j.roster), four);

        // Second connection of an existing user: roster byte-identical (dedup;
        // the new connection's different user_info must not leak).
        let ha1 = handle();
        let sid_a1 = ha1.socket_id;
        let j = s
            .add(ha1, Some(m("alpha", serde_json::json!({"ignored":true}))))
            .unwrap();
        assert_eq!(frame(j.roster), four);
        assert_eq!(frame(s.roster()), four);

        // Removing ONE of alpha's two connections: roster still byte-identical.
        s.remove(&sid_a1);
        assert_eq!(frame(s.roster()), four);

        // Removing a MIDDLE id (`weird`, its user's last connection): the
        // remaining order is exactly the 3-member bytes from before it joined.
        s.remove(&sid_w);
        assert_eq!(frame(s.roster()), three);
    }

    /// The empty roster shape: empty `ids` array, empty `hash` object, count 0.
    #[test]
    fn golden_roster_bytes_empty() {
        let s = ChannelState::default();
        assert_eq!(
            crate::protocol::wire::encode(
                7,
                &ServerEvent::SubscriptionSucceeded {
                    channel: "presence-golden".into(),
                    presence: Some(s.roster()),
                }
            ),
            r#"{"event":"pusher_internal:subscription_succeeded","channel":"presence-golden","data":"{\"presence\":{\"ids\":[],\"hash\":{},\"count\":0}}"}"#
        );
    }
}
