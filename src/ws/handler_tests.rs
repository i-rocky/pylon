use super::*;
use crate::adapter::local::LocalAdapter;
use crate::channel::registry::Registry;
use pylon_dispatcher_helpers::*;
use tokio::sync::mpsc;

mod pylon_dispatcher_helpers {
    use crate::app::{App, AppManager};
    use crate::webhook::dispatcher::FixedClock;
    use crate::webhook::transport::RecordingTransport;
    use crate::webhook::{spawn, WebhookHandle};
    use async_trait::async_trait;
    use std::sync::Arc;

    pub struct OneApp(pub App);
    #[async_trait]
    impl AppManager for OneApp {
        async fn by_key(&self, key: &str) -> Option<App> {
            (self.0.key == key).then(|| self.0.clone())
        }
        async fn by_id(&self, id: &str) -> Option<App> {
            (self.0.id == id).then(|| self.0.clone())
        }
    }

    /// Build a webhook handle whose dispatcher records deliveries, returning the
    /// recorder so the test can assert. `batch_ms` is small for fast windows.
    pub fn recording_webhooks(app: App, batch_ms: u64) -> (WebhookHandle, Arc<RecordingTransport>) {
        let recorder = Arc::new(RecordingTransport::new());
        let apps: Arc<dyn AppManager> = Arc::new(OneApp(app));
        let handle = spawn(
            apps,
            recorder.clone(),
            Arc::new(FixedClock(1700000000000)),
            batch_ms,
            1024,
        );
        (handle, recorder)
    }
}

fn app(sub_count: bool) -> App {
    serde_json::from_value::<App>(serde_json::json!({
        "name": "t", "id": "app", "key": "k", "secret": "s",
        "client_messages_enabled": true,
        "subscription_count_enabled": sub_count
    }))
    .unwrap()
}

fn app_with_client_messages(enabled: bool) -> App {
    serde_json::from_value::<App>(serde_json::json!({
        "name": "t", "id": "app", "key": "k", "secret": "s",
        "client_messages_enabled": enabled, "subscription_count_enabled": false
    }))
    .unwrap()
}

fn ctx(app: App) -> (ConnectionContext, mpsc::UnboundedReceiver<ServerEvent>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let registry = Arc::new(Registry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry));
    let c = ConnectionContext {
        app,
        socket_id: SocketId::generate(),
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
async fn ping_enqueues_pong() {
    let (mut c, mut rx) = ctx(app(false));
    c.dispatch(ClientCommand::Ping).await;
    assert!(matches!(rx.try_recv(), Ok(ServerEvent::Pong)));
}

#[tokio::test]
async fn public_subscribe_succeeds_and_registers() {
    let (mut c, mut rx) = ctx(app(false));
    c.dispatch(ClientCommand::Subscribe {
        channel: "room".into(),
        auth: None,
        channel_data: None,
    })
    .await;
    assert!(matches!(
        rx.try_recv(),
        Ok(ServerEvent::SubscriptionSucceeded { .. })
    ));
    assert_eq!(c.adapter.channel("app", "room").await.subscription_count, 1);
}

#[tokio::test]
async fn public_cache_channel_subscribes_as_public() {
    // bare cache-foo must subscribe as public, not error 4009
    let (mut c, mut rx) = ctx(app(false));
    c.dispatch(ClientCommand::Subscribe {
        channel: "cache-foo".into(),
        auth: None,
        channel_data: None,
    })
    .await;
    assert!(
        matches!(rx.try_recv(), Ok(ServerEvent::SubscriptionSucceeded { .. })),
        "bare cache-foo must subscribe as public, not error 4009"
    );
}

#[tokio::test]
async fn subscription_count_emitted_when_enabled() {
    let (mut c, mut rx) = ctx(app(true));
    c.dispatch(ClientCommand::Subscribe {
        channel: "room".into(),
        auth: None,
        channel_data: None,
    })
    .await;
    assert!(matches!(
        rx.try_recv(),
        Ok(ServerEvent::SubscriptionSucceeded { .. })
    ));
    match rx.try_recv() {
        Ok(ServerEvent::SubscriptionCount { count, .. }) => assert_eq!(count, 1),
        other => panic!("expected SubscriptionCount, got {other:?}"),
    }
}

#[tokio::test]
async fn private_subscribe_without_auth_errors_non_fatally() {
    let (mut c, mut rx) = ctx(app(false));
    c.dispatch(ClientCommand::Subscribe {
        channel: "private-x".into(),
        auth: None,
        channel_data: None,
    })
    .await;
    match rx.try_recv() {
        Ok(ServerEvent::SubscriptionError {
            channel, status, ..
        }) => {
            assert_eq!(channel, "private-x");
            assert_eq!(status, 401);
        }
        other => panic!("expected SubscriptionError, got {other:?}"),
    }
}

#[tokio::test]
async fn private_subscribe_with_valid_auth_succeeds() {
    let (mut c, mut rx) = ctx(app(false));
    let sid = c.socket_id.as_str().to_string();
    let sig = crate::auth::signature::channel_signature("s", &sid, "private-x", None);
    let token = format!("k:{sig}"); // app key "k", secret "s" from the `app()` helper
    c.dispatch(ClientCommand::Subscribe {
        channel: "private-x".into(),
        auth: Some(token),
        channel_data: None,
    })
    .await;
    assert!(matches!(
        rx.try_recv(),
        Ok(ServerEvent::SubscriptionSucceeded { .. })
    ));
}

#[tokio::test]
async fn encrypted_subscribe_with_valid_auth_succeeds() {
    let (mut c, mut rx) = ctx(app(false));
    let sid = c.socket_id.as_str().to_string();
    // Encrypted subscribe is signed exactly like a private channel (no channel_data).
    let sig = crate::auth::signature::channel_signature("s", &sid, "private-encrypted-x", None);
    c.dispatch(ClientCommand::Subscribe {
        channel: "private-encrypted-x".into(),
        auth: Some(format!("k:{sig}")),
        channel_data: None,
    })
    .await;
    match rx.try_recv() {
        Ok(ServerEvent::SubscriptionSucceeded { channel, presence }) => {
            assert_eq!(channel, "private-encrypted-x");
            assert!(presence.is_none(), "encrypted channels carry no roster");
        }
        other => panic!("expected SubscriptionSucceeded, got {other:?}"),
    }
    assert_eq!(
        c.adapter
            .channel("app", "private-encrypted-x")
            .await
            .subscription_count,
        1
    );
}

#[tokio::test]
async fn encrypted_subscribe_without_auth_errors_non_fatally() {
    let (mut c, mut rx) = ctx(app(false));
    c.dispatch(ClientCommand::Subscribe {
        channel: "private-encrypted-x".into(),
        auth: None,
        channel_data: None,
    })
    .await;
    match rx.try_recv() {
        Ok(ServerEvent::SubscriptionError {
            channel, status, ..
        }) => {
            assert_eq!(channel, "private-encrypted-x");
            assert_eq!(status, 401);
        }
        other => panic!("expected SubscriptionError, got {other:?}"),
    }
}

#[tokio::test]
async fn presence_subscribe_returns_roster_and_broadcasts_member_added() {
    let (mut c, mut rx) = ctx(app(false));
    let sid = c.socket_id.as_str().to_string();
    let cd = r#"{"user_id":"u1","user_info":{"name":"Ann"}}"#;
    let sig = crate::auth::signature::channel_signature("s", &sid, "presence-x", Some(cd));
    c.dispatch(ClientCommand::Subscribe {
        channel: "presence-x".into(),
        auth: Some(format!("k:{sig}")),
        channel_data: Some(cd.into()),
    })
    .await;
    match rx.try_recv() {
        Ok(ServerEvent::SubscriptionSucceeded {
            presence: Some(p), ..
        }) => {
            assert_eq!(p.count, 1);
            assert_eq!(p.ids, vec!["u1".to_string()]);
        }
        other => panic!("expected presence SubscriptionSucceeded, got {other:?}"),
    }
    // Self is excluded from its own member_added, so no further self-delivered event.
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn presence_subscribe_with_bad_auth_errors() {
    let (mut c, mut rx) = ctx(app(false));
    c.dispatch(ClientCommand::Subscribe {
        channel: "presence-x".into(),
        auth: Some("k:bad".into()),
        channel_data: Some(r#"{"user_id":"u1"}"#.into()),
    })
    .await;
    assert!(matches!(
        rx.try_recv(),
        Ok(ServerEvent::SubscriptionError { .. })
    ));
}

#[tokio::test]
async fn presence_unsubscribe_broadcasts_member_removed_to_others() {
    // Shared adapter so two contexts see the same channel.
    let registry = Arc::new(Registry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry));
    let mk = |adapter: Arc<dyn Adapter>| {
        let (tx, rx) = mpsc::unbounded_channel();
        let c = ConnectionContext {
            app: app(false),
            socket_id: SocketId::generate(),
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
    };
    let (mut a, mut rxa) = mk(adapter.clone());
    let (mut b, _rxb) = mk(adapter.clone());

    for (c, who) in [(&mut a, "ua"), (&mut b, "ub")] {
        let sid = c.socket_id.as_str().to_string();
        let cd = format!(r#"{{"user_id":"{who}"}}"#);
        let sig = crate::auth::signature::channel_signature("s", &sid, "presence-x", Some(&cd));
        c.dispatch(ClientCommand::Subscribe {
            channel: "presence-x".into(),
            auth: Some(format!("k:{sig}")),
            channel_data: Some(cd),
        })
        .await;
    }
    // Drain a's queued frames (its own subscription_succeeded + member_added for ub).
    while rxa.try_recv().is_ok() {}

    b.unsubscribe("presence-x".into()).await;
    // a should now see member_removed for ub.
    let mut saw = false;
    while let Ok(ev) = rxa.try_recv() {
        if let ServerEvent::MemberRemoved { user_id, .. } = ev {
            assert_eq!(user_id, "ub");
            saw = true;
        }
    }
    assert!(saw, "remaining member should receive member_removed");
}

#[tokio::test]
async fn client_event_rejected_when_messaging_disabled() {
    let (mut c, mut rx) = ctx(app_with_client_messages(false));
    c.dispatch(ClientCommand::ClientEvent {
        event: "client-x".into(),
        channel: "private-x".into(),
        data: serde_json::json!({}),
    })
    .await;
    match rx.try_recv() {
        Ok(ServerEvent::ClientEventError { code, .. }) => assert_eq!(code, 4301),
        other => panic!("expected 4301, got {other:?}"),
    }
}

// P11 — oversize payload → 4301 (was: silent drop) + channel on client-event errors

#[tokio::test]
async fn client_event_oversize_payload_returns_4301() {
    // Subscribe to a private channel first (needs auth).
    let (mut c, mut rx) = ctx(app_with_client_messages(true));
    let sid = c.socket_id.as_str().to_string();
    let sig = crate::auth::signature::channel_signature("s", &sid, "private-x", None);
    c.dispatch(ClientCommand::Subscribe {
        channel: "private-x".into(),
        auth: Some(format!("k:{sig}")),
        channel_data: None,
    })
    .await;
    let _ = rx.try_recv(); // drain subscription_succeeded

    // Build a payload that exceeds the default max_event_payload_bytes (10 KiB = 10240 bytes).
    let big_data = serde_json::json!({ "x": "a".repeat(11_000) });
    c.dispatch(ClientCommand::ClientEvent {
        event: "client-x".into(),
        channel: "private-x".into(),
        data: big_data,
    })
    .await;
    // Must receive pusher:error 4301 (was: silence)
    match rx.try_recv() {
        Ok(ServerEvent::ClientEventError { code, channel, .. }) => {
            assert_eq!(code, 4301, "oversize payload must return 4301");
            assert_eq!(channel, "private-x", "error frame must carry the channel");
        }
        other => panic!("expected ClientEventError 4301, got {other:?}"),
    }
}

#[tokio::test]
async fn client_event_messaging_disabled_error_carries_channel() {
    // The 4301 for messaging-disabled must carry the `channel` field (soketi parity).
    let (mut c, mut rx) = ctx(app_with_client_messages(false));
    c.dispatch(ClientCommand::ClientEvent {
        event: "client-x".into(),
        channel: "private-x".into(),
        data: serde_json::json!({}),
    })
    .await;
    match rx.try_recv() {
        Ok(ServerEvent::ClientEventError { code, channel, .. }) => {
            assert_eq!(code, 4301);
            assert_eq!(
                channel, "private-x",
                "messaging-disabled 4301 must carry channel"
            );
        }
        other => panic!("expected ClientEventError with channel, got {other:?}"),
    }
}

#[tokio::test]
async fn client_event_dropped_when_not_subscribed() {
    let (mut c, mut rx) = ctx(app_with_client_messages(true));
    c.dispatch(ClientCommand::ClientEvent {
        event: "client-x".into(),
        channel: "private-x".into(),
        data: serde_json::json!({}),
    })
    .await;
    assert!(
        rx.try_recv().is_err(),
        "unsubscribed client event is silently dropped"
    );
}

#[tokio::test]
async fn duplicate_presence_subscribe_is_idempotent() {
    let (mut c, _rx) = ctx(app(false));
    let sid = c.socket_id.as_str().to_string();
    let cd = r#"{"user_id":"u1"}"#;
    let sig = crate::auth::signature::channel_signature("s", &sid, "presence-x", Some(cd));
    let make = || ClientCommand::Subscribe {
        channel: "presence-x".into(),
        auth: Some(format!("k:{sig}")),
        channel_data: Some(cd.into()),
    };
    c.dispatch(make()).await;
    c.dispatch(make()).await; // duplicate must be ignored, not double-counted
    c.unsubscribe("presence-x".into()).await;
    // If the duplicate had inflated conn_count, the user would still be present.
    assert_eq!(
        c.adapter.channel("app", "presence-x").await.user_count,
        None
    );
}

#[tokio::test]
async fn client_event_on_encrypted_channel_is_dropped() {
    let registry = Arc::new(Registry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry));
    let mk = |adapter: Arc<dyn Adapter>| {
        let (tx, rx) = mpsc::unbounded_channel();
        let c = ConnectionContext {
            app: app_with_client_messages(true),
            socket_id: SocketId::generate(),
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
    };
    let (mut a, _rxa) = mk(adapter.clone());
    let (mut b, mut rxb) = mk(adapter.clone());
    for c in [&mut a, &mut b] {
        let sid = c.socket_id.as_str().to_string();
        let sig = crate::auth::signature::channel_signature("s", &sid, "private-encrypted-x", None);
        c.dispatch(ClientCommand::Subscribe {
            channel: "private-encrypted-x".into(),
            auth: Some(format!("k:{sig}")),
            channel_data: None,
        })
        .await;
    }
    while rxb.try_recv().is_ok() {} // drain b's subscription_succeeded

    // a sends a client event on the encrypted channel; b must NOT receive it.
    a.dispatch(ClientCommand::ClientEvent {
        event: "client-x".into(),
        channel: "private-encrypted-x".into(),
        data: serde_json::json!({}),
    })
    .await;
    assert!(
        rxb.try_recv().is_err(),
        "client events on encrypted channels must not be relayed"
    );
}

#[tokio::test]
async fn cache_subscribe_replays_cached_event() {
    let (mut c, mut rx) = ctx(app(false));
    // Seed the cache directly through the adapter seam.
    c.adapter
        .cache_set(
            "app",
            "cache-feed",
            crate::channel::cache::CachedEvent {
                event: "my-event".into(),
                data: "{\"hi\":1}".into(),
            },
            std::time::Duration::from_secs(60),
        )
        .await;
    c.dispatch(ClientCommand::Subscribe {
        channel: "cache-feed".into(),
        auth: None,
        channel_data: None,
    })
    .await;
    // First the success frame, then the replayed cached event.
    assert!(matches!(
        rx.try_recv(),
        Ok(ServerEvent::SubscriptionSucceeded { .. })
    ));
    match rx.try_recv() {
        Ok(ServerEvent::ChannelEvent {
            channel,
            event,
            data,
            ..
        }) => {
            assert_eq!(channel, "cache-feed");
            assert_eq!(event, "my-event");
            assert_eq!(data, serde_json::Value::String("{\"hi\":1}".into()));
        }
        other => panic!("expected replayed ChannelEvent, got {other:?}"),
    }
}

#[tokio::test]
async fn cache_subscribe_with_empty_cache_emits_cache_miss() {
    let (mut c, mut rx) = ctx(app(false));
    c.dispatch(ClientCommand::Subscribe {
        channel: "cache-feed".into(),
        auth: None,
        channel_data: None,
    })
    .await;
    assert!(matches!(
        rx.try_recv(),
        Ok(ServerEvent::SubscriptionSucceeded { .. })
    ));
    match rx.try_recv() {
        Ok(ServerEvent::CacheMiss { channel }) => assert_eq!(channel, "cache-feed"),
        other => panic!("expected CacheMiss, got {other:?}"),
    }
}

#[tokio::test]
async fn presence_cache_subscribe_replays_after_join() {
    let (mut c, mut rx) = ctx(app(false));
    // Seed the cache for the presence-cache channel (app id "app" matches the harness).
    c.adapter
        .cache_set(
            "app",
            "presence-cache-room",
            crate::channel::cache::CachedEvent {
                event: "my-event".into(),
                data: "{\"hi\":1}".into(),
            },
            std::time::Duration::from_secs(60),
        )
        .await;
    let sid = c.socket_id.as_str().to_string();
    let channel_data = serde_json::json!({"user_id":"u1","user_info":{"name":"U"}}).to_string();
    let sig = crate::auth::signature::channel_signature(
        "s",
        &sid,
        "presence-cache-room",
        Some(&channel_data),
    );
    c.dispatch(ClientCommand::Subscribe {
        channel: "presence-cache-room".into(),
        auth: Some(format!("k:{sig}")),
        channel_data: Some(channel_data),
    })
    .await;
    // First the roster success frame, then the replayed cached event.
    assert!(matches!(
        rx.try_recv(),
        Ok(ServerEvent::SubscriptionSucceeded { .. })
    ));
    match rx.try_recv() {
        Ok(ServerEvent::ChannelEvent { channel, event, .. }) => {
            assert_eq!(channel, "presence-cache-room");
            assert_eq!(event, "my-event");
        }
        other => panic!("expected replayed ChannelEvent, got {other:?}"),
    }
}

#[tokio::test]
async fn encrypted_cache_subscribe_replays_after_auth() {
    let (mut c, mut rx) = ctx(app(false));
    // Seed the cache for the encrypted-cache channel (app id "app" matches the harness).
    c.adapter
        .cache_set(
            "app",
            "private-encrypted-cache-x",
            crate::channel::cache::CachedEvent {
                event: "secret".into(),
                data: "{\"nonce\":\"abc\",\"ciphertext\":\"xyz\"}".into(),
            },
            std::time::Duration::from_secs(60),
        )
        .await;
    // Encrypted subscribe = private-style HMAC over socket_id:channel, no channel_data.
    let sid = c.socket_id.as_str().to_string();
    let sig =
        crate::auth::signature::channel_signature("s", &sid, "private-encrypted-cache-x", None);
    c.dispatch(ClientCommand::Subscribe {
        channel: "private-encrypted-cache-x".into(),
        auth: Some(format!("k:{sig}")),
        channel_data: None,
    })
    .await;
    // First subscription_succeeded (no roster), then the verbatim ciphertext replay.
    assert!(matches!(
        rx.try_recv(),
        Ok(ServerEvent::SubscriptionSucceeded { .. })
    ));
    match rx.try_recv() {
        Ok(ServerEvent::ChannelEvent {
            channel,
            event,
            data,
            ..
        }) => {
            assert_eq!(channel, "private-encrypted-cache-x");
            assert_eq!(event, "secret");
            assert_eq!(
                data,
                serde_json::Value::String("{\"nonce\":\"abc\",\"ciphertext\":\"xyz\"}".into())
            );
        }
        other => panic!("expected replayed ChannelEvent, got {other:?}"),
    }
}

#[tokio::test]
async fn presence_over_member_cap_errors() {
    let registry = Arc::new(Registry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry));
    let mk = || {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut limits = crate::server::config::ServerConfig::default().limits();
        limits.max_presence_members = 1;
        let c = ConnectionContext {
            app: app(false),
            socket_id: SocketId::generate(),
            self_tx: tx,
            adapter: adapter.clone(),
            limits,
            subscribed: HashSet::new(),
            user: None,
            webhooks: crate::webhook::WebhookHandle::null(),
            presence_membership: std::collections::HashMap::new(),
            client_event_rate: crate::ws::rate::RateWindow::new(0),
        };
        (c, rx)
    };
    let sub = |c: &ConnectionContext, user: &str| {
        let sid = c.socket_id.as_str().to_string();
        let cd = format!(r#"{{"user_id":"{user}"}}"#);
        let sig = crate::auth::signature::channel_signature("s", &sid, "presence-x", Some(&cd));
        ClientCommand::Subscribe {
            channel: "presence-x".into(),
            auth: Some(format!("k:{sig}")),
            channel_data: Some(cd),
        }
    };
    let (mut a, _rxa) = mk();
    let cmd_a = sub(&a, "ua");
    a.dispatch(cmd_a).await; // fills the cap (max=1)
    let (mut b, mut rxb) = mk();
    let cmd_b = sub(&b, "ub");
    b.dispatch(cmd_b).await; // second distinct user exceeds the cap
    match rxb.try_recv() {
        Ok(ServerEvent::SubscriptionError {
            error_type, status, ..
        }) => {
            assert_eq!(error_type, "LimitReached");
            assert_eq!(status, 4004);
        }
        other => panic!("expected LimitReached SubscriptionError, got {other:?}"),
    }
}

#[tokio::test]
async fn on_close_signs_out_bound_user() {
    use crate::auth::signature::user_signature;
    // app(false): key "k" / secret "s" / id "app"
    let (mut c, _rx) = ctx(app(false));
    let sig = user_signature("s", c.socket_id.as_str(), r#"{"id":"9"}"#);
    c.dispatch(ClientCommand::Signin {
        auth: format!("k:{sig}"),
        user_data: r#"{"id":"9"}"#.into(),
    })
    .await;
    assert!(c.user.is_some(), "user must be bound after signin");
    c.on_close().await;
    assert!(
        !c.adapter.is_user_online("app", "9").await,
        "user must be signed out after connection close"
    );
}

#[tokio::test]
async fn server_to_user_subscribe_succeeds_when_signed_in_and_matches() {
    use crate::auth::signature::user_signature;
    let (mut c, mut rx) = ctx(app(false));
    let sig = user_signature("s", c.socket_id.as_str(), r#"{"id":"9"}"#);
    c.dispatch(ClientCommand::Signin {
        auth: format!("k:{sig}"),
        user_data: r#"{"id":"9"}"#.into(),
    })
    .await;
    let _ = rx.try_recv(); // drain signin_success
    c.dispatch(ClientCommand::Subscribe {
        channel: "#server-to-user-9".into(),
        auth: None,
        channel_data: None,
    })
    .await;
    assert!(matches!(
        rx.try_recv(),
        Ok(ServerEvent::SubscriptionSucceeded { .. })
    ));
    // Reserved channels never enter the channel registry:
    assert_eq!(
        c.adapter
            .channel("app", "#server-to-user-9")
            .await
            .subscription_count,
        0
    );
}

#[tokio::test]
async fn server_to_user_subscribe_errors_on_mismatch() {
    let (mut c, mut rx) = ctx(app(false)); // not signed in
    c.dispatch(ClientCommand::Subscribe {
        channel: "#server-to-user-9".into(),
        auth: None,
        channel_data: None,
    })
    .await;
    assert!(matches!(
        rx.try_recv(),
        Ok(ServerEvent::SubscriptionError { .. })
    ));
}

#[tokio::test]
async fn server_to_user_subscribe_errors_when_signed_in_as_different_user() {
    use crate::auth::signature::user_signature;
    let (mut c, mut rx) = ctx(app(false));
    // Sign in as user "7" ...
    let sig = user_signature("s", c.socket_id.as_str(), r#"{"id":"7"}"#);
    c.dispatch(ClientCommand::Signin {
        auth: format!("k:{sig}"),
        user_data: r#"{"id":"7"}"#.into(),
    })
    .await;
    let _ = rx.try_recv(); // drain signin_success
                           // ... then try to subscribe to a DIFFERENT user's channel -> must be rejected.
    c.dispatch(ClientCommand::Subscribe {
        channel: "#server-to-user-9".into(),
        auth: None,
        channel_data: None,
    })
    .await;
    assert!(matches!(
        rx.try_recv(),
        Ok(ServerEvent::SubscriptionError { .. })
    ));
}

#[tokio::test]
async fn arbitrary_hash_channel_subscribe_errors() {
    let (mut c, mut rx) = ctx(app(false));
    c.dispatch(ClientCommand::Subscribe {
        channel: "#some-reserved-channel".into(),
        auth: None,
        channel_data: None,
    })
    .await;
    assert!(matches!(
        rx.try_recv(),
        Ok(ServerEvent::SubscriptionError { .. })
    ));
}

#[tokio::test]
async fn subscribe_emits_channel_occupied_then_close_emits_vacated() {
    // App with occupied+vacated webhooks on one endpoint.
    let mut app = serde_json::from_value::<App>(serde_json::json!({
        "name": "t", "id": "app", "key": "app-key", "secret": "app-secret",
        "webhooks": [{ "url": "https://e.test/wh",
            "event_types": ["channel_occupied","channel_vacated"] }]
    }))
    .unwrap();
    app.recompute_has_flags();

    let (webhooks, recorder) = recording_webhooks(app.clone(), 30);
    let adapter: std::sync::Arc<dyn crate::adapter::Adapter> =
        std::sync::Arc::new(crate::adapter::local::LocalAdapter::new(
            std::sync::Arc::new(crate::channel::registry::Registry::new()),
        ));
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let mut c = crate::ws::handler::ConnectionContext {
        app,
        socket_id: crate::protocol::socket_id::SocketId::from_raw("1.1"),
        self_tx: tx,
        adapter,
        limits: crate::server::config::ServerConfig::default().limits(),
        subscribed: std::collections::HashSet::new(),
        user: None,
        webhooks,
        presence_membership: std::collections::HashMap::new(),
        client_event_rate: crate::ws::rate::RateWindow::new(0),
    };

    // Subscribe to a public channel → occupied.
    c.dispatch(crate::protocol::command::ClientCommand::Subscribe {
        channel: "my-channel".into(),
        auth: None,
        channel_data: None,
    })
    .await;
    // Let the occupied window (30ms) close and flush BEFORE the vacated trigger,
    // so the two transitions land in SEPARATE batches and don't coalesce away.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    // Close → unsubscribes all → vacated.
    c.on_close().await;

    // Wait out the vacated window + a margin.
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;

    let recorded = recorder.recorded().await;
    // occupied (subscribe window) then vacated (close window). To assert a clean
    // separation we collect all event names across deliveries:
    let names: Vec<String> = recorded
        .iter()
        .flat_map(|d| {
            let env: serde_json::Value = serde_json::from_str(&d.body).unwrap();
            env["events"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| e["name"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        names.contains(&"channel_occupied".to_string()),
        "got {names:?}"
    );
    assert!(
        names.contains(&"channel_vacated".to_string()),
        "got {names:?}"
    );
}

#[tokio::test]
async fn rapid_subscribe_unsubscribe_in_window_emits_nothing() {
    let mut app = serde_json::from_value::<App>(serde_json::json!({
        "name": "t", "id": "app", "key": "app-key", "secret": "app-secret",
        "webhooks": [{ "url": "https://e.test/wh",
            "event_types": ["channel_occupied","channel_vacated"] }]
    }))
    .unwrap();
    app.recompute_has_flags();

    // Large window so subscribe+unsubscribe land in the SAME batch → coalesce → empty.
    let (webhooks, recorder) = recording_webhooks(app.clone(), 200);
    let adapter: std::sync::Arc<dyn crate::adapter::Adapter> =
        std::sync::Arc::new(crate::adapter::local::LocalAdapter::new(
            std::sync::Arc::new(crate::channel::registry::Registry::new()),
        ));
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let mut c = crate::ws::handler::ConnectionContext {
        app,
        socket_id: crate::protocol::socket_id::SocketId::from_raw("1.1"),
        self_tx: tx,
        adapter,
        limits: crate::server::config::ServerConfig::default().limits(),
        subscribed: std::collections::HashSet::new(),
        user: None,
        webhooks,
        presence_membership: std::collections::HashMap::new(),
        client_event_rate: crate::ws::rate::RateWindow::new(0),
    };

    c.dispatch(crate::protocol::command::ClientCommand::Subscribe {
        channel: "my-channel".into(),
        auth: None,
        channel_data: None,
    })
    .await;
    c.dispatch(crate::protocol::command::ClientCommand::Unsubscribe {
        channel: "my-channel".into(),
    })
    .await;

    tokio::time::sleep(std::time::Duration::from_millis(260)).await;
    assert!(
        recorder.recorded().await.is_empty(),
        "occupied+vacated in one window must coalesce to no delivery"
    );
}

#[tokio::test]
async fn presence_first_and_last_emit_member_added_then_removed() {
    let mut app = serde_json::from_value::<crate::app::App>(serde_json::json!({
        "name": "t", "id": "app", "key": "app-key", "secret": "app-secret",
        "webhooks": [{ "url": "https://e.test/wh",
            "event_types": ["member_added","member_removed"] }]
    }))
    .unwrap();
    app.recompute_has_flags();

    let (webhooks, recorder) = recording_webhooks(app.clone(), 30);
    let adapter: std::sync::Arc<dyn crate::adapter::Adapter> =
        std::sync::Arc::new(crate::adapter::local::LocalAdapter::new(
            std::sync::Arc::new(crate::channel::registry::Registry::new()),
        ));
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let socket = crate::protocol::socket_id::SocketId::from_raw("9.9");
    let cd = r#"{"user_id":"u1"}"#;
    let auth = format!(
        "app-key:{}",
        crate::auth::signature::channel_signature("app-secret", "9.9", "presence-room", Some(cd))
    );
    let mut c = crate::ws::handler::ConnectionContext {
        app,
        socket_id: socket,
        self_tx: tx,
        adapter,
        limits: crate::server::config::ServerConfig::default().limits(),
        subscribed: std::collections::HashSet::new(),
        user: None,
        webhooks,
        presence_membership: std::collections::HashMap::new(),
        client_event_rate: crate::ws::rate::RateWindow::new(0),
    };

    c.dispatch(crate::protocol::command::ClientCommand::Subscribe {
        channel: "presence-room".into(),
        auth: Some(auth),
        channel_data: Some(cd.into()),
    })
    .await;
    // Sleep longer than the batch window (30ms) so member_added lands in its own
    // batch before member_removed fires; prevents coalescing cancellation.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    c.on_close().await;

    // Wait out the member_removed batch window + margin.
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;

    let names: Vec<String> = recorder
        .recorded()
        .await
        .iter()
        .flat_map(|d| {
            let env: serde_json::Value = serde_json::from_str(&d.body).unwrap();
            env["events"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| {
                    format!(
                        "{}:{}",
                        e["name"].as_str().unwrap(),
                        e["user_id"].as_str().unwrap_or("")
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        names.contains(&"member_added:u1".to_string()),
        "got {names:?}"
    );
    assert!(
        names.contains(&"member_removed:u1".to_string()),
        "got {names:?}"
    );
}

#[tokio::test]
async fn client_event_on_presence_includes_user_id_webhook() {
    let mut app = serde_json::from_value::<crate::app::App>(serde_json::json!({
        "name": "t", "id": "app", "key": "app-key", "secret": "app-secret",
        "client_messages_enabled": true,
        "webhooks": [{ "url": "https://e.test/wh", "event_types": ["client_event"] }]
    }))
    .unwrap();
    app.recompute_has_flags();

    let (webhooks, recorder) = recording_webhooks(app.clone(), 30);
    let adapter: std::sync::Arc<dyn crate::adapter::Adapter> =
        std::sync::Arc::new(crate::adapter::local::LocalAdapter::new(
            std::sync::Arc::new(crate::channel::registry::Registry::new()),
        ));
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let cd = r#"{"user_id":"u1"}"#;
    let auth = format!(
        "app-key:{}",
        crate::auth::signature::channel_signature("app-secret", "9.9", "presence-room", Some(cd))
    );
    let mut c = crate::ws::handler::ConnectionContext {
        app,
        socket_id: crate::protocol::socket_id::SocketId::from_raw("9.9"),
        self_tx: tx,
        adapter,
        limits: crate::server::config::ServerConfig::default().limits(),
        subscribed: std::collections::HashSet::new(),
        user: None,
        webhooks,
        presence_membership: std::collections::HashMap::new(),
        client_event_rate: crate::ws::rate::RateWindow::new(0),
    };
    c.dispatch(crate::protocol::command::ClientCommand::Subscribe {
        channel: "presence-room".into(),
        auth: Some(auth),
        channel_data: Some(cd.into()),
    })
    .await;
    c.dispatch(crate::protocol::command::ClientCommand::ClientEvent {
        event: "client-msg".into(),
        channel: "presence-room".into(),
        data: serde_json::json!({"hello":"world"}),
    })
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    let recorded = recorder.recorded().await;
    let env: serde_json::Value = serde_json::from_str(&recorded[0].body).unwrap();
    let ev = &env["events"][0];
    assert_eq!(ev["name"], "client_event");
    assert_eq!(ev["channel"], "presence-room");
    assert_eq!(ev["event"], "client-msg");
    assert_eq!(ev["data"], serde_json::json!({"hello":"world"}));
    assert_eq!(ev["socket_id"], "9.9");
    assert_eq!(ev["user_id"], "u1", "presence sender carries user_id");
}

#[tokio::test]
async fn client_event_on_private_omits_user_id_webhook() {
    let mut app = serde_json::from_value::<crate::app::App>(serde_json::json!({
        "name": "t", "id": "app", "key": "app-key", "secret": "app-secret",
        "client_messages_enabled": true,
        "webhooks": [{ "url": "https://e.test/wh", "event_types": ["client_event"] }]
    }))
    .unwrap();
    app.recompute_has_flags();
    let (webhooks, recorder) = recording_webhooks(app.clone(), 30);
    let adapter: std::sync::Arc<dyn crate::adapter::Adapter> =
        std::sync::Arc::new(crate::adapter::local::LocalAdapter::new(
            std::sync::Arc::new(crate::channel::registry::Registry::new()),
        ));
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let auth = format!(
        "app-key:{}",
        crate::auth::signature::channel_signature("app-secret", "9.9", "private-c", None)
    );
    let mut c = crate::ws::handler::ConnectionContext {
        app,
        socket_id: crate::protocol::socket_id::SocketId::from_raw("9.9"),
        self_tx: tx,
        adapter,
        limits: crate::server::config::ServerConfig::default().limits(),
        subscribed: std::collections::HashSet::new(),
        user: None,
        webhooks,
        presence_membership: std::collections::HashMap::new(),
        client_event_rate: crate::ws::rate::RateWindow::new(0),
    };
    c.dispatch(crate::protocol::command::ClientCommand::Subscribe {
        channel: "private-c".into(),
        auth: Some(auth),
        channel_data: None,
    })
    .await;
    c.dispatch(crate::protocol::command::ClientCommand::ClientEvent {
        event: "client-msg".into(),
        channel: "private-c".into(),
        data: serde_json::json!({"x":1}),
    })
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    let recorded = recorder.recorded().await;
    let env: serde_json::Value = serde_json::from_str(&recorded[0].body).unwrap();
    assert!(
        env["events"][0].get("user_id").is_none(),
        "private sender has no user_id"
    );
}

#[tokio::test]
async fn client_event_webhook_gated_off_when_app_lacks_it() {
    // App has client messaging but NO client_event webhook → no delivery.
    let mut app = serde_json::from_value::<crate::app::App>(serde_json::json!({
        "name": "t", "id": "app", "key": "app-key", "secret": "app-secret",
        "client_messages_enabled": true,
        "webhooks": [{ "url": "https://e.test/wh", "event_types": ["channel_occupied"] }]
    }))
    .unwrap();
    app.recompute_has_flags();
    let (webhooks, recorder) = recording_webhooks(app.clone(), 30);
    let adapter: std::sync::Arc<dyn crate::adapter::Adapter> =
        std::sync::Arc::new(crate::adapter::local::LocalAdapter::new(
            std::sync::Arc::new(crate::channel::registry::Registry::new()),
        ));
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let auth = format!(
        "app-key:{}",
        crate::auth::signature::channel_signature("app-secret", "9.9", "private-c", None)
    );
    let mut c = crate::ws::handler::ConnectionContext {
        app,
        socket_id: crate::protocol::socket_id::SocketId::from_raw("9.9"),
        self_tx: tx,
        adapter,
        limits: crate::server::config::ServerConfig::default().limits(),
        subscribed: std::collections::HashSet::new(),
        user: None,
        webhooks,
        presence_membership: std::collections::HashMap::new(),
        client_event_rate: crate::ws::rate::RateWindow::new(0),
    };
    c.dispatch(crate::protocol::command::ClientCommand::Subscribe {
        channel: "private-c".into(),
        auth: Some(auth),
        channel_data: None,
    })
    .await;
    c.dispatch(crate::protocol::command::ClientCommand::ClientEvent {
        event: "client-msg".into(),
        channel: "private-c".into(),
        data: serde_json::json!({"x":1}),
    })
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    // Only channel_occupied may appear; never a client_event.
    let has_ce = recorder.recorded().await.iter().any(|d| {
        let env: serde_json::Value = serde_json::from_str(&d.body).unwrap();
        env["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["name"] == "client_event")
    });
    assert!(!has_ce, "client_event webhook must be gated off");
}

#[tokio::test]
async fn cache_channel_miss_emits_cache_miss_webhook() {
    let mut app = serde_json::from_value::<crate::app::App>(serde_json::json!({
        "name": "t", "id": "app", "key": "app-key", "secret": "app-secret",
        "webhooks": [{ "url": "https://e.test/wh", "event_types": ["cache_miss"] }]
    }))
    .unwrap();
    app.recompute_has_flags();
    let (webhooks, recorder) = recording_webhooks(app.clone(), 30);
    let adapter: std::sync::Arc<dyn crate::adapter::Adapter> =
        std::sync::Arc::new(crate::adapter::local::LocalAdapter::new(
            std::sync::Arc::new(crate::channel::registry::Registry::new()),
        ));
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let mut c = crate::ws::handler::ConnectionContext {
        app,
        socket_id: crate::protocol::socket_id::SocketId::from_raw("9.9"),
        self_tx: tx,
        adapter,
        limits: crate::server::config::ServerConfig::default().limits(),
        subscribed: std::collections::HashSet::new(),
        user: None,
        webhooks,
        presence_membership: std::collections::HashMap::new(),
        client_event_rate: crate::ws::rate::RateWindow::new(0),
    };
    // public cache channel: no auth, miss on first subscribe.
    c.dispatch(crate::protocol::command::ClientCommand::Subscribe {
        channel: "cache-x".into(),
        auth: None,
        channel_data: None,
    })
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    let recorded = recorder.recorded().await;
    let env: serde_json::Value = serde_json::from_str(&recorded[0].body).unwrap();
    assert_eq!(env["events"][0]["name"], "cache_miss");
    assert_eq!(env["events"][0]["channel"], "cache-x");
}

// P8 — channel-name length + charset validation

#[tokio::test]
async fn subscribe_over_length_channel_name_errors_4009() {
    let long_name = "a".repeat(165); // > default max of 164
    let (mut c, mut rx) = ctx(app(false));
    c.dispatch(ClientCommand::Subscribe {
        channel: long_name.clone(),
        auth: None,
        channel_data: None,
    })
    .await;
    match rx.try_recv() {
        Ok(ServerEvent::SubscriptionError {
            channel, status, ..
        }) => {
            assert_eq!(channel, long_name);
            assert_eq!(status, 4009);
        }
        other => panic!("expected SubscriptionError 4009 for over-length channel, got {other:?}"),
    }
    // Must NOT be in the registry
    assert_eq!(
        c.adapter
            .channel("app", &long_name)
            .await
            .subscription_count,
        0
    );
}

#[tokio::test]
async fn subscribe_illegal_char_channel_name_errors_4009() {
    let bad_name = "bad channel!"; // space and ! are illegal
    let (mut c, mut rx) = ctx(app(false));
    c.dispatch(ClientCommand::Subscribe {
        channel: bad_name.to_string(),
        auth: None,
        channel_data: None,
    })
    .await;
    match rx.try_recv() {
        Ok(ServerEvent::SubscriptionError {
            channel, status, ..
        }) => {
            assert_eq!(channel, bad_name);
            assert_eq!(status, 4009);
        }
        other => panic!("expected SubscriptionError 4009 for bad-charset channel, got {other:?}"),
    }
    assert_eq!(
        c.adapter.channel("app", bad_name).await.subscription_count,
        0
    );
}

#[tokio::test]
async fn subscribe_valid_channel_names_still_succeed() {
    for name in ["my-channel", "presence-room", "private-x", "cache-feed"] {
        let (mut c, mut rx) = ctx(app(false));
        if name.starts_with("private-") || name.starts_with("presence-") {
            // Auth required; just confirm no channel-name error
            c.dispatch(ClientCommand::Subscribe {
                channel: name.to_string(),
                auth: None,
                channel_data: None,
            })
            .await;
            // Will get an auth error (401) — but NOT a 4009 charset error
            match rx.try_recv() {
                Ok(ServerEvent::SubscriptionError { status, .. }) => {
                    assert_ne!(status, 4009, "valid name '{name}' must not get 4009");
                }
                other => panic!("expected SubscriptionError for unauthed {name}, got {other:?}"),
            }
        } else {
            c.dispatch(ClientCommand::Subscribe {
                channel: name.to_string(),
                auth: None,
                channel_data: None,
            })
            .await;
            assert!(
                matches!(rx.try_recv(), Ok(ServerEvent::SubscriptionSucceeded { .. })),
                "valid name '{name}' must subscribe successfully"
            );
        }
    }
}

#[tokio::test]
async fn subscribe_server_to_user_channel_still_works_after_p8() {
    // #server-to-user- channels must NOT be run through charset validation
    // (the # is handled by the reserved-prefix path before validation).
    use crate::auth::signature::user_signature;
    let (mut c, mut rx) = ctx(app(false));
    let sig = user_signature("s", c.socket_id.as_str(), r#"{"id":"9"}"#);
    c.dispatch(ClientCommand::Signin {
        auth: format!("k:{sig}"),
        user_data: r#"{"id":"9"}"#.into(),
    })
    .await;
    let _ = rx.try_recv(); // drain signin_success
    c.dispatch(ClientCommand::Subscribe {
        channel: "#server-to-user-9".into(),
        auth: None,
        channel_data: None,
    })
    .await;
    assert!(
        matches!(rx.try_recv(), Ok(ServerEvent::SubscriptionSucceeded { .. })),
        "#server-to-user- channel must still succeed after P8 charset validation"
    );
}

// P4 — presence channels must NOT receive pusher_internal:subscription_count

/// Subscribe to `channel` (with valid auth if presence) on an app that has
/// `subscription_count_enabled = true`, then drain all queued events and return
/// whether any of them were a `SubscriptionCount` frame.
async fn sub_count_emitted_after_subscribe(channel: &str, channel_data: Option<&str>) -> bool {
    let (mut c, mut rx) = ctx(app(true)); // subscription_count_enabled = true
    let sid = c.socket_id.as_str().to_string();
    let auth = if channel_data.is_some()
        || channel.starts_with("presence-")
        || channel.starts_with("private-")
    {
        let sig = crate::auth::signature::channel_signature("s", &sid, channel, channel_data);
        Some(format!("k:{sig}"))
    } else {
        None
    };
    c.dispatch(ClientCommand::Subscribe {
        channel: channel.into(),
        auth,
        channel_data: channel_data.map(String::from),
    })
    .await;
    let mut saw_count = false;
    while let Ok(ev) = rx.try_recv() {
        if matches!(ev, ServerEvent::SubscriptionCount { .. }) {
            saw_count = true;
        }
    }
    saw_count
}

#[tokio::test]
async fn subscription_count_not_emitted_for_presence_channel() {
    // Presence channels must never receive pusher_internal:subscription_count
    // (Pusher parity P4 — count is communicated via member_added/member_removed).
    let emitted = sub_count_emitted_after_subscribe(
        "presence-room",
        Some(r#"{"user_id":"u1","user_info":{}}"#),
    )
    .await;
    assert!(
        !emitted,
        "subscription_count must NOT be emitted on presence channels (P4)"
    );
}

#[tokio::test]
async fn subscription_count_emitted_for_public_channel() {
    // Public channels must still receive subscription_count when enabled.
    let emitted = sub_count_emitted_after_subscribe("room", None).await;
    assert!(
        emitted,
        "subscription_count MUST be emitted on public channels when enabled"
    );
}

// P10 — presence user_id (≤128 chars) and user_info (≤1024 bytes) size limits

/// Build a properly-signed presence subscribe command for the given channel_data JSON string.
fn signed_presence_sub(c: &ConnectionContext, channel_data: &str) -> ClientCommand {
    let sid = c.socket_id.as_str().to_string();
    let sig =
        crate::auth::signature::channel_signature("s", &sid, "presence-x", Some(channel_data));
    ClientCommand::Subscribe {
        channel: "presence-x".into(),
        auth: Some(format!("k:{sig}")),
        channel_data: Some(channel_data.to_string()),
    }
}

#[tokio::test]
async fn presence_subscribe_with_oversized_user_id_errors() {
    // user_id of 129 chars exceeds the 128-char limit → subscription_error
    let long_uid = "u".repeat(129);
    let cd = serde_json::json!({"user_id": long_uid, "user_info": {}}).to_string();
    let (mut c, mut rx) = ctx(app(false));
    let cmd = signed_presence_sub(&c, &cd);
    c.dispatch(cmd).await;
    match rx.try_recv() {
        Ok(ServerEvent::SubscriptionError {
            channel,
            error_type,
            ..
        }) => {
            assert_eq!(channel, "presence-x");
            assert_eq!(error_type, "InvalidPresenceData");
        }
        other => panic!("expected SubscriptionError for oversized user_id, got {other:?}"),
    }
    // Must NOT have been registered
    assert_eq!(
        c.adapter
            .channel("app", "presence-x")
            .await
            .subscription_count,
        0
    );
}

#[tokio::test]
async fn presence_subscribe_with_oversized_user_info_errors() {
    // user_info serialized to >1024 bytes → subscription_error
    // Build a user_info value whose JSON representation exceeds 1024 bytes.
    let big_val: String = "x".repeat(1030);
    let cd = serde_json::json!({"user_id": "u1", "user_info": {"data": big_val}}).to_string();
    let (mut c, mut rx) = ctx(app(false));
    let cmd = signed_presence_sub(&c, &cd);
    c.dispatch(cmd).await;
    match rx.try_recv() {
        Ok(ServerEvent::SubscriptionError {
            channel,
            error_type,
            ..
        }) => {
            assert_eq!(channel, "presence-x");
            assert_eq!(error_type, "InvalidPresenceData");
        }
        other => panic!("expected SubscriptionError for oversized user_info, got {other:?}"),
    }
    assert_eq!(
        c.adapter
            .channel("app", "presence-x")
            .await
            .subscription_count,
        0
    );
}

#[tokio::test]
async fn presence_subscribe_with_valid_sized_data_succeeds() {
    // user_id exactly 128 chars and user_info just under 1024 bytes → succeeds
    let uid_128 = "u".repeat(128);
    // small user_info well under 1024 bytes
    let cd = serde_json::json!({"user_id": uid_128, "user_info": {"role": "admin"}}).to_string();
    let (mut c, mut rx) = ctx(app(false));
    let cmd = signed_presence_sub(&c, &cd);
    c.dispatch(cmd).await;
    match rx.try_recv() {
        Ok(ServerEvent::SubscriptionSucceeded { channel, .. }) => {
            assert_eq!(channel, "presence-x");
        }
        other => panic!("expected SubscriptionSucceeded for valid presence data, got {other:?}"),
    }
}

// P3 — presence client-events must carry the originator's top-level `user_id`
// in the broadcast frame; private channels must omit it. pusher-js
// presence_channel.ts reads `event.user_id` → `metadata.user_id`.

/// Two members on a shared adapter; A subscribes to `channel` with `channel_data`
/// (when presence), then triggers `client-foo`. Returns the v7-encoded frame B
/// receives for that broadcast.
async fn relayed_client_event_frame(
    channel: &str,
    channel_data: Option<&str>,
) -> serde_json::Value {
    let registry = Arc::new(Registry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry));
    let mk = |adapter: Arc<dyn Adapter>| {
        let (tx, rx) = mpsc::unbounded_channel();
        let c = ConnectionContext {
            app: app_with_client_messages(true),
            socket_id: SocketId::generate(),
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
    };
    let (mut a, _rxa) = mk(adapter.clone());
    let (mut b, mut rxb) = mk(adapter.clone());
    for c in [&mut a, &mut b] {
        let sid = c.socket_id.as_str().to_string();
        let sig = crate::auth::signature::channel_signature("s", &sid, channel, channel_data);
        c.dispatch(ClientCommand::Subscribe {
            channel: channel.into(),
            auth: Some(format!("k:{sig}")),
            channel_data: channel_data.map(String::from),
        })
        .await;
    }
    while rxb.try_recv().is_ok() {} // drain b's subscription_succeeded + member_added

    a.dispatch(ClientCommand::ClientEvent {
        event: "client-foo".into(),
        channel: channel.into(),
        data: serde_json::json!({"hello": "world"}),
    })
    .await;

    let frame = match rxb.try_recv() {
        Ok(ev @ ServerEvent::ChannelEvent { .. }) => ev,
        other => panic!("expected relayed ChannelEvent, got {other:?}"),
    };
    serde_json::from_str(&crate::protocol::v7::frames::encode(&frame)).unwrap()
}

#[tokio::test]
async fn presence_client_event_broadcast_carries_user_id() {
    // Member A joins presence-room as user_id "u1"; the frame B receives for A's
    // client event must carry top-level `user_id: "u1"`.
    let frame = relayed_client_event_frame("presence-room", Some(r#"{"user_id":"u1"}"#)).await;
    assert_eq!(frame["event"], "client-foo");
    assert_eq!(frame["channel"], "presence-room");
    assert_eq!(
        frame["user_id"], "u1",
        "presence client-event broadcast must carry the sender's user_id"
    );
}

#[tokio::test]
async fn private_client_event_broadcast_omits_user_id() {
    // No presence membership on a private channel → no top-level user_id.
    let frame = relayed_client_event_frame("private-c", None).await;
    assert_eq!(frame["event"], "client-foo");
    assert_eq!(frame["channel"], "private-c");
    assert!(
        frame.get("user_id").is_none(),
        "private client-event broadcast must not carry user_id"
    );
}

// P12 — per-connection client-event rate limit (10/s) → in-band 4301 + drop

/// Sender and receiver share an adapter; sender has a tight rate window of 3.
/// After 3 allowed events, the 4th must produce a 4301 to the SENDER and the
/// RECEIVER must have received exactly 3 ChannelEvent broadcasts.
#[tokio::test]
async fn client_event_rate_limit_returns_4301_and_drops() {
    let registry = Arc::new(Registry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry));

    // Build sender with a 3-event/second rate window.
    let (tx_sender, mut rx_sender) = mpsc::unbounded_channel();
    let mut sender = ConnectionContext {
        app: app_with_client_messages(true),
        socket_id: SocketId::generate(),
        self_tx: tx_sender,
        adapter: adapter.clone(),
        limits: crate::server::config::ServerConfig::default().limits(),
        subscribed: HashSet::new(),
        user: None,
        webhooks: crate::webhook::WebhookHandle::null(),
        presence_membership: std::collections::HashMap::new(),
        client_event_rate: crate::ws::rate::RateWindow::new(3),
    };

    // Build receiver (unlimited, just needs to see broadcasts).
    let (tx_recv, mut rx_recv) = mpsc::unbounded_channel();
    let mut receiver = ConnectionContext {
        app: app_with_client_messages(true),
        socket_id: SocketId::generate(),
        self_tx: tx_recv,
        adapter: adapter.clone(),
        limits: crate::server::config::ServerConfig::default().limits(),
        subscribed: HashSet::new(),
        user: None,
        webhooks: crate::webhook::WebhookHandle::null(),
        presence_membership: std::collections::HashMap::new(),
        client_event_rate: crate::ws::rate::RateWindow::new(0),
    };

    // Both subscribe to the same private channel.
    let channel = "private-rate-test";
    for c in [&mut sender, &mut receiver] {
        let sid = c.socket_id.as_str().to_string();
        let sig = crate::auth::signature::channel_signature("s", &sid, channel, None);
        c.dispatch(ClientCommand::Subscribe {
            channel: channel.into(),
            auth: Some(format!("k:{sig}")),
            channel_data: None,
        })
        .await;
    }
    // Drain subscription_succeeded frames from both.
    while rx_sender.try_recv().is_ok() {}
    while rx_recv.try_recv().is_ok() {}

    // Fire 4 client events in a burst (all within the same 1-second window).
    for _ in 0..4 {
        sender
            .dispatch(ClientCommand::ClientEvent {
                event: "client-foo".into(),
                channel: channel.into(),
                data: serde_json::json!({"n": 1}),
            })
            .await;
    }

    // Receiver must have received exactly 3 ChannelEvent broadcasts (not 4).
    let mut broadcast_count = 0;
    while let Ok(ev) = rx_recv.try_recv() {
        if matches!(ev, ServerEvent::ChannelEvent { .. }) {
            broadcast_count += 1;
        }
    }
    assert_eq!(
        broadcast_count, 3,
        "receiver should see exactly 3 broadcasts; 4th was rate-limited"
    );

    // Sender must have received exactly one 4301 ClientEventError.
    let mut rate_errors = 0;
    while let Ok(ev) = rx_sender.try_recv() {
        if let ServerEvent::ClientEventError {
            code, channel: ch, ..
        } = ev
        {
            assert_eq!(code, 4301, "rate-limit error must be code 4301");
            assert_eq!(ch, channel, "error must carry the channel name");
            rate_errors += 1;
        }
    }
    assert_eq!(
        rate_errors, 1,
        "sender must receive exactly one 4301 rate-limit error"
    );
}
