//! In-memory user/connection index behind the Adapter seam. A user is "online"
//! while signed in on >= 1 connection. Keyed by (app_id, user_id). The Redis
//! equivalent lands in SP7 behind the same Adapter methods.

use crate::connection::handle::ConnectionHandle;
use crate::protocol::socket_id::SocketId;
use crate::user::{UserJoinOutcome, UserLeaveOutcome};
use dashmap::DashMap;
use std::collections::HashMap;

#[derive(Default)]
pub struct UserRegistry {
    // (app_id, user_id) -> { socket_id -> handle }
    users: DashMap<(String, String), HashMap<SocketId, ConnectionHandle>>,
    // (app_id, watched_user_id) -> { socket_id -> watcher handle }
    watchers: DashMap<(String, String), HashMap<SocketId, ConnectionHandle>>,
    // (app_id, socket_id) -> watched user_ids (for O(1) disconnect cleanup)
    watching: DashMap<(String, SocketId), Vec<String>>,
}

impl UserRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn signin(&self, app: &str, user_id: &str, handle: ConnectionHandle) -> UserJoinOutcome {
        let mut entry = self
            .users
            .entry((app.to_string(), user_id.to_string()))
            .or_default();
        let first_for_user = entry.is_empty();
        entry.insert(handle.socket_id.clone(), handle);
        UserJoinOutcome { first_for_user }
    }

    pub fn signout(&self, app: &str, user_id: &str, socket_id: &SocketId) -> UserLeaveOutcome {
        let key = (app.to_string(), user_id.to_string());
        let last_for_user = {
            let Some(mut entry) = self.users.get_mut(&key) else {
                return UserLeaveOutcome {
                    last_for_user: false,
                };
            };
            entry.remove(socket_id);
            entry.is_empty()
        };
        // Only delete the (app,user) entry if it is STILL empty — a concurrent
        // signin may have repopulated it after we dropped the guard. remove_if
        // re-checks under the shard lock, mirroring channel::Registry.
        self.users.remove_if(&key, |_, sockets| sockets.is_empty());
        UserLeaveOutcome { last_for_user }
    }

    pub fn handles(&self, app: &str, user_id: &str) -> Vec<ConnectionHandle> {
        self.users
            .get(&(app.to_string(), user_id.to_string()))
            .map(|e| e.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn is_online(&self, app: &str, user_id: &str) -> bool {
        self.users
            .get(&(app.to_string(), user_id.to_string()))
            .is_some_and(|e| !e.is_empty())
    }

    pub fn watch(&self, app: &str, handle: ConnectionHandle, watched: Vec<String>) -> Vec<String> {
        let sock = handle.socket_id.clone();
        // Idempotent: drop any prior watch state for this connection before
        // recording the new one, so a re-watch can't leak stale `watchers` entries.
        self.unwatch(app, &sock);
        for w in &watched {
            self.watchers
                .entry((app.to_string(), w.clone()))
                .or_default()
                .insert(sock.clone(), handle.clone());
        }
        let online = watched
            .iter()
            .filter(|w| self.is_online(app, w))
            .cloned()
            .collect();
        self.watching.insert((app.to_string(), sock), watched);
        online
    }

    pub fn unwatch(&self, app: &str, socket_id: &SocketId) {
        let Some((_, watched)) = self.watching.remove(&(app.to_string(), socket_id.clone())) else {
            return;
        };
        for w in watched {
            let key = (app.to_string(), w);
            // Drop the get_mut guard before the conditional remove (deadlock avoidance),
            // and use remove_if so a concurrent watch that repopulated the set is not
            // clobbered — same pattern as signout / channel::Registry.
            {
                let Some(mut set) = self.watchers.get_mut(&key) else {
                    continue;
                };
                set.remove(socket_id);
            }
            self.watchers.remove_if(&key, |_, set| set.is_empty());
        }
    }

    pub fn watchers_of(&self, app: &str, user_id: &str) -> Vec<ConnectionHandle> {
        self.watchers
            .get(&(app.to_string(), user_id.to_string()))
            .map(|e| e.values().cloned().collect())
            .unwrap_or_default()
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
    fn first_and_subsequent_signin_flags() {
        let r = UserRegistry::new();
        let (h1, _r1) = handle();
        let (h2, _r2) = handle();
        assert!(r.signin("app", "u", h1.clone()).first_for_user);
        assert!(!r.signin("app", "u", h2).first_for_user);
        assert!(r.is_online("app", "u"));
        assert_eq!(r.handles("app", "u").len(), 2);
    }

    #[test]
    fn signout_reports_last_and_clears() {
        let r = UserRegistry::new();
        let (h1, _r1) = handle();
        let (h2, _r2) = handle();
        let s1 = h1.socket_id.clone();
        let s2 = h2.socket_id.clone();
        r.signin("app", "u", h1);
        r.signin("app", "u", h2);
        assert!(!r.signout("app", "u", &s1).last_for_user);
        assert!(r.signout("app", "u", &s2).last_for_user);
        assert!(!r.is_online("app", "u"));
    }

    #[test]
    fn watch_returns_online_subset_and_watchers_resolve() {
        let r = UserRegistry::new();
        let (online_user, _o) = handle();
        r.signin("app", "b", online_user); // b is online; c is not
        let (watcher, _w) = handle();
        let sock = watcher.socket_id.clone();
        let online = r.watch("app", watcher, vec!["b".into(), "c".into()]);
        assert_eq!(online, vec!["b".to_string()]); // only b currently online
        assert_eq!(r.watchers_of("app", "b").len(), 1);
        r.unwatch("app", &sock);
        assert!(r.watchers_of("app", "b").is_empty());
    }

    #[test]
    fn rewatch_replaces_prior_watchlist_without_leak() {
        let r = UserRegistry::new();
        let (watcher, _w) = handle();
        let sock = watcher.socket_id.clone();
        // First watch covers a and b.
        r.watch("app", watcher.clone(), vec!["a".into(), "b".into()]);
        assert_eq!(r.watchers_of("app", "a").len(), 1);
        assert_eq!(r.watchers_of("app", "b").len(), 1);
        // Re-watch with a different set must DROP the stale a/b entries (only c remains).
        r.watch("app", watcher, vec!["c".into()]);
        assert!(
            r.watchers_of("app", "a").is_empty(),
            "stale watcher for a leaked"
        );
        assert!(
            r.watchers_of("app", "b").is_empty(),
            "stale watcher for b leaked"
        );
        assert_eq!(r.watchers_of("app", "c").len(), 1);
        // And a final unwatch clears everything.
        r.unwatch("app", &sock);
        assert!(r.watchers_of("app", "c").is_empty());
    }
}
