//! `pusher:signin` handling, split from `ws::handler`.

use super::handler::ConnectionContext;
use crate::protocol::error::PusherError;
use crate::protocol::event::ServerEvent;
use crate::user::parse_user_data;

impl ConnectionContext {
    pub(in crate::ws) async fn signin(&mut self, auth: String, user_data: String) {
        // Re-signin: same user_data is idempotent (re-ack); different is fatal.
        if let Some(existing) = &self.user {
            if existing.user_data_raw == user_data {
                self.send_self(ServerEvent::SigninSuccess { user_data });
            } else {
                self.fail_signin("Connection not authorized.");
            }
            return;
        }
        // 1. Verify the signature. All failures collapse to one 4009 (no secret leak).
        if crate::auth::user::verify(
            &self.app.key,
            &self.app.secret,
            self.socket_id.as_str(),
            &user_data,
            &auth,
        )
        .is_err()
        {
            return self.fail_signin("Connection not authorized.");
        }
        // 2. Parse + validate user_data (id must be a non-empty string).
        let user = match parse_user_data(&user_data) {
            Ok(u) => u,
            Err(_) => {
                return self.fail_signin("The returned user data must contain the \"id\" field.")
            }
        };
        // 3. Register and acknowledge.
        let outcome = self
            .adapter
            .signin_user(&self.app.id, &user.id, self.handle())
            .await;
        self.send_self(ServerEvent::SigninSuccess {
            user_data: user.user_data_raw.clone(),
        });
        // Watchlist: notify watchers if this signin brought the user online,
        // then register this connection's own watchlist and snapshot who's online.
        if outcome.first_for_user {
            self.notify_watchers(&user.id, "online").await;
        }
        let watched = self.capped_watchlist(&user.watchlist);
        if !watched.is_empty() {
            let online = self
                .adapter
                .watch(&self.app.id, self.handle(), watched)
                .await;
            if !online.is_empty() {
                self.send_self(ServerEvent::WatchlistEvents {
                    events: vec![crate::protocol::event::WatchlistChange {
                        name: "online".to_string(),
                        user_ids: online,
                    }],
                });
            }
        }
        self.user = Some(user);
    }

    /// Truncate the watchlist to the configured cap, emitting a non-fatal 4302
    /// (overflow) error when it is exceeded. Signin still proceeds.
    fn capped_watchlist(&self, watchlist: &[String]) -> Vec<String> {
        let max = self.limits.max_watchlist_size;
        if watchlist.len() > max {
            let msg =
                format!("Watchlist limit exceeded; only the first {max} entries are tracked.");
            self.send_self(ServerEvent::Error(PusherError::new(4302, msg)));
            watchlist[..max].to_vec()
        } else {
            watchlist.to_vec()
        }
    }

    /// Emit `pusher:error` 4009 then close the connection (fatal, no reconnect).
    /// Matches soketi ws-handler (error frame then `ws.end(4009)`).
    fn fail_signin(&self, message: &str) {
        self.send_self(ServerEvent::Error(PusherError::new(4009, message)));
        self.send_self(ServerEvent::Close {
            code: 4009,
            reason: message.to_string(),
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::adapter::local::LocalAdapter;
    use crate::adapter::Adapter;
    use crate::app::App;
    use crate::auth::signature::user_signature;
    use crate::channel::registry::Registry;
    use crate::protocol::command::ClientCommand;
    use crate::protocol::event::ServerEvent;
    use crate::protocol::socket_id::SocketId;
    use crate::ws::handler::ConnectionContext;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn app() -> App {
        serde_json::from_value::<App>(serde_json::json!({
            "name": "t", "id": "app", "key": "app-key", "secret": "app-secret",
            "client_messages_enabled": true, "subscription_count_enabled": false
        }))
        .unwrap()
    }

    fn ctx() -> (ConnectionContext, mpsc::UnboundedReceiver<ServerEvent>) {
        let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));
        ctx_on(adapter, "123.456")
    }

    fn signin_cmd(c: &ConnectionContext, user_data: &str) -> ClientCommand {
        let sig = user_signature("app-secret", c.socket_id.as_str(), user_data);
        ClientCommand::Signin {
            auth: format!("app-key:{sig}"),
            user_data: user_data.to_string(),
        }
    }

    #[tokio::test]
    async fn valid_signin_acks_and_registers() {
        let (mut c, mut rx) = ctx();
        c.dispatch(signin_cmd(&c, r#"{"id":"7"}"#)).await;
        match rx.try_recv() {
            Ok(ServerEvent::SigninSuccess { user_data }) => assert_eq!(user_data, r#"{"id":"7"}"#),
            other => panic!("expected SigninSuccess, got {other:?}"),
        }
        assert!(c.user.is_some());
        assert!(
            c.adapter
                .signout_user("app", "7", &c.socket_id)
                .await
                .last_for_user
        );
    }

    #[tokio::test]
    async fn bad_signature_errors_4009_and_closes() {
        let (mut c, mut rx) = ctx();
        c.dispatch(ClientCommand::Signin {
            auth: "app-key:deadbeef".into(),
            user_data: r#"{"id":"7"}"#.into(),
        })
        .await;
        assert!(matches!(rx.try_recv(), Ok(ServerEvent::Error(e)) if e.code == 4009));
        assert!(matches!(
            rx.try_recv(),
            Ok(ServerEvent::Close { code: 4009, .. })
        ));
        assert!(c.user.is_none());
    }

    #[tokio::test]
    async fn missing_id_errors_4009_and_closes() {
        let (mut c, mut rx) = ctx();
        // valid signature over a body that lacks a string id
        c.dispatch(signin_cmd(&c, r#"{"name":"x"}"#)).await;
        assert!(matches!(rx.try_recv(), Ok(ServerEvent::Error(e)) if e.code == 4009));
        assert!(matches!(
            rx.try_recv(),
            Ok(ServerEvent::Close { code: 4009, .. })
        ));
    }

    fn ctx_on(
        adapter: Arc<dyn Adapter>,
        socket: &str,
    ) -> (ConnectionContext, mpsc::UnboundedReceiver<ServerEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let c = ConnectionContext {
            app: app(),
            socket_id: SocketId::from_raw(socket),
            self_tx: tx,
            adapter,
            limits: crate::server::config::ServerConfig::default().limits(),
            subscribed: HashSet::new(),
            user: None,
            webhooks: crate::webhook::WebhookHandle::null(),
            presence_membership: std::collections::HashMap::new(),
            client_event_rate: crate::ws::rate::RateWindow::new(0),
        };
        (c, rx)
    }

    #[tokio::test]
    async fn watcher_is_notified_when_watched_user_signs_in() {
        let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));
        // watcher C signs in watching B (B offline -> no initial snapshot)
        let (mut c_watch, mut rx_watch) = ctx_on(adapter.clone(), "1.1");
        let sig_c = user_signature("app-secret", "1.1", r#"{"id":"C","watchlist":["B"]}"#);
        c_watch
            .dispatch(ClientCommand::Signin {
                auth: format!("app-key:{sig_c}"),
                user_data: r#"{"id":"C","watchlist":["B"]}"#.into(),
            })
            .await;
        let _ = rx_watch.try_recv(); // signin_success
        assert!(rx_watch.try_recv().is_err(), "no snapshot while B offline");

        // B signs in -> C receives an online watchlist event for B
        let (mut c_b, _rx_b) = ctx_on(adapter.clone(), "2.2");
        let sig_b = user_signature("app-secret", "2.2", r#"{"id":"B"}"#);
        c_b.dispatch(ClientCommand::Signin {
            auth: format!("app-key:{sig_b}"),
            user_data: r#"{"id":"B"}"#.into(),
        })
        .await;
        match rx_watch.try_recv() {
            Ok(ServerEvent::WatchlistEvents { events }) => {
                assert_eq!(events[0].name, "online");
                assert_eq!(events[0].user_ids, vec!["B".to_string()]);
            }
            other => panic!("expected online WatchlistEvents, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn watcher_is_notified_when_watched_user_disconnects() {
        let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));
        // B signs in (online).
        let (mut c_b, _rx_b) = ctx_on(adapter.clone(), "2.2");
        let sig_b = user_signature("app-secret", "2.2", r#"{"id":"B"}"#);
        c_b.dispatch(ClientCommand::Signin {
            auth: format!("app-key:{sig_b}"),
            user_data: r#"{"id":"B"}"#.into(),
        })
        .await;
        // C watches B (B online -> immediate online snapshot, drained below).
        let (mut c_watch, mut rx_watch) = ctx_on(adapter.clone(), "1.1");
        let sig_c = user_signature("app-secret", "1.1", r#"{"id":"C","watchlist":["B"]}"#);
        c_watch
            .dispatch(ClientCommand::Signin {
                auth: format!("app-key:{sig_c}"),
                user_data: r#"{"id":"C","watchlist":["B"]}"#.into(),
            })
            .await;
        while rx_watch.try_recv().is_ok() {} // drain signin_success + online snapshot

        c_b.on_close().await; // B's last connection closes -> offline
        match rx_watch.try_recv() {
            Ok(ServerEvent::WatchlistEvents { events }) => {
                assert_eq!(events[0].name, "offline");
                assert_eq!(events[0].user_ids, vec!["B".to_string()]);
            }
            other => panic!("expected offline WatchlistEvents, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn second_connection_does_not_reemit_online_to_watchers() {
        let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));
        // C watches B.
        let (mut c_watch, mut rx_watch) = ctx_on(adapter.clone(), "1.1");
        let sig_c = user_signature("app-secret", "1.1", r#"{"id":"C","watchlist":["B"]}"#);
        c_watch
            .dispatch(ClientCommand::Signin {
                auth: format!("app-key:{sig_c}"),
                user_data: r#"{"id":"C","watchlist":["B"]}"#.into(),
            })
            .await;
        while rx_watch.try_recv().is_ok() {} // drain C's signin_success (B offline, no snapshot)

        // B signs in on connection 1 -> C gets ONE online event.
        let (mut c_b1, _rx_b1) = ctx_on(adapter.clone(), "2.2");
        let sig_b1 = user_signature("app-secret", "2.2", r#"{"id":"B"}"#);
        c_b1.dispatch(ClientCommand::Signin {
            auth: format!("app-key:{sig_b1}"),
            user_data: r#"{"id":"B"}"#.into(),
        })
        .await;
        match rx_watch.try_recv() {
            Ok(ServerEvent::WatchlistEvents { events }) => {
                assert_eq!(events[0].name, "online");
                assert_eq!(events[0].user_ids, vec!["B".to_string()]);
            }
            other => panic!("expected one online event, got {other:?}"),
        }

        // B signs in on connection 2 (same user, first_for_user == false) -> C gets NOTHING more.
        let (mut c_b2, _rx_b2) = ctx_on(adapter.clone(), "3.3");
        let sig_b2 = user_signature("app-secret", "3.3", r#"{"id":"B"}"#);
        c_b2.dispatch(ClientCommand::Signin {
            auth: format!("app-key:{sig_b2}"),
            user_data: r#"{"id":"B"}"#.into(),
        })
        .await;
        assert!(
            rx_watch.try_recv().is_err(),
            "second connection must NOT re-emit online to watchers"
        );
    }

    #[tokio::test]
    async fn non_last_connection_close_does_not_emit_offline_to_watchers() {
        let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));
        // B signs in on TWO connections.
        let (mut c_b1, _rx_b1) = ctx_on(adapter.clone(), "2.2");
        let sig_b1 = user_signature("app-secret", "2.2", r#"{"id":"B"}"#);
        c_b1.dispatch(ClientCommand::Signin {
            auth: format!("app-key:{sig_b1}"),
            user_data: r#"{"id":"B"}"#.into(),
        })
        .await;
        let (mut c_b2, _rx_b2) = ctx_on(adapter.clone(), "3.3");
        let sig_b2 = user_signature("app-secret", "3.3", r#"{"id":"B"}"#);
        c_b2.dispatch(ClientCommand::Signin {
            auth: format!("app-key:{sig_b2}"),
            user_data: r#"{"id":"B"}"#.into(),
        })
        .await;
        // C watches B (B online -> snapshot, drained).
        let (mut c_watch, mut rx_watch) = ctx_on(adapter.clone(), "1.1");
        let sig_c = user_signature("app-secret", "1.1", r#"{"id":"C","watchlist":["B"]}"#);
        c_watch
            .dispatch(ClientCommand::Signin {
                auth: format!("app-key:{sig_c}"),
                user_data: r#"{"id":"C","watchlist":["B"]}"#.into(),
            })
            .await;
        while rx_watch.try_recv().is_ok() {} // drain signin_success + online snapshot

        // First of B's two connections closes -> NOT last -> no offline.
        c_b1.on_close().await;
        assert!(
            rx_watch.try_recv().is_err(),
            "non-last connection close must NOT emit offline"
        );

        // Second (last) connection closes -> offline emitted exactly once.
        c_b2.on_close().await;
        match rx_watch.try_recv() {
            Ok(ServerEvent::WatchlistEvents { events }) => {
                assert_eq!(events[0].name, "offline");
                assert_eq!(events[0].user_ids, vec!["B".to_string()]);
            }
            other => panic!("expected offline WatchlistEvents, got {other:?}"),
        }
    }
}
