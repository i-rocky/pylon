use super::*;
use crate::adapter::local::LocalAdapter;
use crate::channel::registry::Registry;
use tokio::sync::mpsc;

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
        Ok(ServerEvent::Error(e)) => assert_eq!(e.code, 4301),
        other => panic!("expected 4301, got {other:?}"),
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
