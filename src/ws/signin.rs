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
        let _outcome = self
            .adapter
            .signin_user(&self.app.id, &user.id, self.handle())
            .await;
        self.send_self(ServerEvent::SigninSuccess {
            user_data: user.user_data_raw.clone(),
        });
        self.user = Some(user);
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
        let (tx, rx) = mpsc::unbounded_channel();
        let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));
        let c = ConnectionContext {
            app: app(),
            socket_id: SocketId::from_raw("123.456"),
            self_tx: tx,
            adapter,
            limits: crate::server::config::ServerConfig::default().limits(),
            subscribed: HashSet::new(),
            user: None,
        };
        (c, rx)
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
}
