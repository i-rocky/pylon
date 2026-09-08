//! Cross-node pub/sub receive loop.
//!
//! One [`receive_loop`] runs per [`RedisAdapter`](super::RedisAdapter). It drains
//! the SubscriberClient's collapsed message stream, decodes each [`Envelope`],
//! drops envelopes this node published itself (self-dedup via `node_id`), and
//! re-delivers the pre-encoded frame to local sockets honouring any `except`.

use super::envelope::{Envelope, EnvelopeKind};
use crate::adapter::local::LocalAdapter;
use crate::adapter::Adapter;
use crate::protocol::event::{ServerEvent, WatchlistChange};
use crate::protocol::socket_id::SocketId;
use fred::clients::SubscriberClient;
use fred::error::Error as FredError;
use fred::interfaces::PubsubInterface;
use fred::types::Message;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Subscribe the node's subscriber client to a Pylon pub/sub channel, honoring
/// the `PYLON_REDIS_SHARDED_PUBSUB` flag: Redis 7 `SSUBSCRIBE` (sharded pub/sub —
/// the channel's traffic is routed to and fan-out stays on the slot-owning shard)
/// when enabled, ordinary `SUBSCRIBE` otherwise. The two modes are SEPARATE
/// namespaces server-side, so every node of a cluster must run with the same
/// flag. Error handling stays at the call sites (log + keep going, never fatal).
pub(crate) async fn sub_channel(
    sub: &SubscriberClient,
    channel: String,
    sharded: bool,
) -> Result<(), FredError> {
    if sharded {
        sub.ssubscribe(channel).await
    } else {
        sub.subscribe(channel).await
    }
}

/// Teardown twin of [`sub_channel`]: `SUNSUBSCRIBE` vs `UNSUBSCRIBE`.
pub(crate) async fn unsub_channel(
    sub: &SubscriberClient,
    channel: String,
    sharded: bool,
) -> Result<(), FredError> {
    if sharded {
        sub.sunsubscribe(channel).await
    } else {
        sub.unsubscribe(channel).await
    }
}

/// Consume the subscriber's message stream forever, fanning each remote broadcast
/// out to this node's local sockets.
///
/// The stream is a single collapsed `tokio::sync::broadcast` channel shared across
/// every Redis pub/sub subscription on this node, so the loop must read every
/// message and route by the envelope's `(app, channel)` — it cannot assume the
/// fred channel name. Messages we published ourselves are dropped (`is_from`); a
/// lagged receiver is logged and we keep going; a closed receiver ends the loop.
pub async fn receive_loop(
    mut rx: broadcast::Receiver<Message>,
    local: Arc<LocalAdapter>,
    node_id: String,
) {
    loop {
        match rx.recv().await {
            Ok(msg) => {
                // The publisher sends the envelope JSON as a UTF-8 string, so the
                // received value comes back as a (bytes-backed) string. Pull it
                // out and decode; skip anything that isn't a well-formed envelope.
                let payload = match msg.value.into_string() {
                    Some(s) => s,
                    None => continue,
                };
                let env = match Envelope::decode(payload.as_bytes()) {
                    Ok(e) => e,
                    Err(_) => continue,
                };
                // Self-dedup: this node already delivered locally + published; its
                // own echo must not be re-delivered.
                if env.is_from(&node_id) {
                    continue;
                }
                // Route by kind. For user-directed kinds, `env.channel` carries
                // the target `user_id` rather than a channel name.
                match env.kind {
                    EnvelopeKind::Broadcast => {
                        // The envelope carries the finished v7 frame — preferring
                        // the additive `frame_b64` (F16; base64 of the raw frame
                        // bytes, no JSON string escaping on either end) and
                        // falling back to the legacy `event` JSON string, so
                        // mixed-version clusters relay in both directions.
                        let frame = match env.frame() {
                            Some(f) => f,
                            None => continue,
                        };
                        // Honour `except` even on the relaying node (usually a no-op:
                        // the excepted socket lives on the originating node).
                        let except = env.except.as_deref().map(SocketId::from_raw);
                        local
                            .broadcast(&env.app, &env.channel, ServerEvent::Raw(frame), except)
                            .await;
                    }
                    EnvelopeKind::UserSend => {
                        let frame = match env.frame() {
                            Some(f) => f,
                            None => continue,
                        };
                        local
                            .send_to_user(&env.app, &env.channel, ServerEvent::Raw(frame))
                            .await;
                    }
                    EnvelopeKind::UserTerminate => {
                        local.terminate_user(&env.app, &env.channel).await;
                    }
                    EnvelopeKind::WatchOnline | EnvelopeKind::WatchOffline => {
                        let name = if env.kind == EnvelopeKind::WatchOnline {
                            "online"
                        } else {
                            "offline"
                        };
                        let ev = ServerEvent::WatchlistEvents {
                            events: vec![WatchlistChange {
                                name: name.to_string(),
                                user_ids: vec![env.channel.clone()],
                            }],
                        };
                        for h in local.watchers_of(&env.app, &env.channel).await {
                            let _ = h.mailbox.send(ev.clone());
                        }
                    }
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(skipped = n, "redis sub stream lagged; dropped messages");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::app_registry::AppRegistry;
    use crate::channel::registry::Registry;
    use crate::connection::handle::{ConnectionHandle, Mailbox};
    use crate::protocol::error::PusherError;
    use fred::prelude::Server;
    use fred::types::{MessageKind, Value};

    /// This node's id in every test below; an envelope stamped with it is the
    /// node's own echo.
    const SELF_NODE: &str = "node-self";

    fn local_adapter() -> Arc<LocalAdapter> {
        Arc::new(LocalAdapter::new(
            Arc::new(Registry::new()),
            Arc::new(AppRegistry::new()),
        ))
    }

    /// A connection whose mailbox the test can read.
    fn conn() -> (
        ConnectionHandle,
        tokio::sync::mpsc::Receiver<Box<ServerEvent>>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        (
            ConnectionHandle {
                socket_id: SocketId::generate(),
                mailbox: Mailbox::new(tx, None, None),
            },
            rx,
        )
    }

    /// A pub/sub message carrying `value`, shaped exactly as fred hands one to
    /// the receive loop.
    fn message(value: Value) -> Message {
        Message {
            channel: "pylon:relay".into(),
            value,
            kind: MessageKind::Message,
            server: Server::new("127.0.0.1", 6390),
        }
    }

    /// A message carrying `env`'s JSON, as the publishing node PUBLISHes it.
    fn envelope_message(env: &Envelope) -> Message {
        let json = String::from_utf8(env.encode()).expect("envelope JSON is UTF-8");
        message(Value::String(json.into()))
    }

    fn envelope(kind: EnvelopeKind, node_id: &str, channel: &str, frame: &str) -> Envelope {
        Envelope {
            node_id: node_id.to_string(),
            app: "app1".to_string(),
            kind,
            channel: channel.to_string(),
            event: serde_json::Value::String(frame.to_string()),
            except: None,
            frame_b64: None,
        }
    }

    /// Feed `msgs` through a fresh receive loop and return once the loop has
    /// consumed every one of them: the sender is dropped up front, so the loop
    /// ends on `Closed` after the last message rather than on a timer.
    async fn drive(local: Arc<LocalAdapter>, capacity: usize, msgs: Vec<Message>) {
        let (tx, rx) = broadcast::channel(capacity);
        for m in msgs {
            tx.send(m).expect("the loop's receiver must still be live");
        }
        drop(tx);
        receive_loop(rx, local, SELF_NODE.to_string()).await;
    }

    fn drained(rx: &mut tokio::sync::mpsc::Receiver<Box<ServerEvent>>) -> Vec<ServerEvent> {
        let mut out = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            out.push(*ev);
        }
        out
    }

    /// A remote broadcast reaches this node's local subscribers as the exact
    /// pre-encoded frame the publisher put on the wire.
    #[tokio::test]
    async fn remote_broadcast_is_redelivered_verbatim_to_local_subscribers() {
        let local = local_adapter();
        let (handle, mut rx) = conn();
        local.subscribe("app1", "public-c", handle, None).await;

        let env = envelope(EnvelopeKind::Broadcast, "node-other", "public-c", "FRAME-1");
        drive(local, 16, vec![envelope_message(&env)]).await;

        assert_eq!(
            drained(&mut rx),
            vec![ServerEvent::Raw(Arc::from("FRAME-1"))],
            "a remote broadcast must arrive as the publisher's exact frame"
        );
    }

    /// The node's OWN echo is dropped: it already delivered locally before
    /// publishing, so re-delivering here would double every broadcast.
    #[tokio::test]
    async fn own_echo_is_dropped_before_local_delivery() {
        let local = local_adapter();
        let (handle, mut rx) = conn();
        local.subscribe("app1", "public-c", handle, None).await;

        let mine = envelope(EnvelopeKind::Broadcast, SELF_NODE, "public-c", "ECHO");
        let theirs = envelope(EnvelopeKind::Broadcast, "node-other", "public-c", "REMOTE");
        drive(
            local,
            16,
            vec![envelope_message(&mine), envelope_message(&theirs)],
        )
        .await;

        assert_eq!(
            drained(&mut rx),
            vec![ServerEvent::Raw(Arc::from("REMOTE"))],
            "only the remote frame may be delivered; the node's own echo is deduped"
        );
    }

    /// `except` is honoured on the RELAYING node too, so a socket the publisher
    /// excluded stays excluded even when it lives here.
    #[tokio::test]
    async fn except_excludes_a_local_socket_on_the_relaying_node() {
        let local = local_adapter();
        let (excluded, mut excluded_rx) = conn();
        let (other, mut other_rx) = conn();
        let excluded_id = excluded.socket_id;
        local.subscribe("app1", "public-c", excluded, None).await;
        local.subscribe("app1", "public-c", other, None).await;

        let mut env = envelope(EnvelopeKind::Broadcast, "node-other", "public-c", "FRAME");
        env.except = Some(excluded_id.to_string());
        drive(local, 16, vec![envelope_message(&env)]).await;

        assert!(
            drained(&mut excluded_rx).is_empty(),
            "the excepted socket must receive nothing"
        );
        assert_eq!(
            drained(&mut other_rx).len(),
            1,
            "every other local subscriber still receives the frame"
        );
    }

    /// A payload that is not UTF-8 text, an envelope that is not valid JSON, and
    /// a frame-kind envelope carrying no frame are all skipped — and the loop
    /// keeps going, proven by the good message behind them still landing.
    #[tokio::test]
    async fn malformed_payloads_are_skipped_without_stopping_the_loop() {
        let local = local_adapter();
        let (handle, mut rx) = conn();
        local.subscribe("app1", "public-c", handle, None).await;

        let mut frameless = envelope(EnvelopeKind::Broadcast, "node-other", "public-c", "");
        frameless.event = serde_json::Value::Null;
        assert!(
            frameless.frame().is_none(),
            "fixture precondition: this envelope must carry no frame"
        );
        let good = envelope(EnvelopeKind::Broadcast, "node-other", "public-c", "GOOD");

        drive(
            local,
            16,
            vec![
                message(Value::Bytes(bytes::Bytes::from_static(&[0xff, 0xfe]))),
                message(Value::String("not json at all".into())),
                envelope_message(&frameless),
                envelope_message(&good),
            ],
        )
        .await;

        assert_eq!(
            drained(&mut rx),
            vec![ServerEvent::Raw(Arc::from("GOOD"))],
            "only the well-formed envelope may be delivered"
        );
    }

    /// A `UserSend` envelope routes by `channel` as a USER id, reaching that
    /// user's signed-in connections rather than a channel's subscribers.
    #[tokio::test]
    async fn user_send_reaches_the_users_signed_in_connections() {
        let local = local_adapter();
        let (handle, mut rx) = conn();
        local.signin_user("app1", "u7", handle).await;

        let env = envelope(EnvelopeKind::UserSend, "node-other", "u7", "USER-FRAME");
        drive(local, 16, vec![envelope_message(&env)]).await;

        assert_eq!(
            drained(&mut rx),
            vec![ServerEvent::Raw(Arc::from("USER-FRAME"))],
            "a UserSend envelope must reach the named user's connections"
        );
    }

    /// A `UserTerminate` envelope evicts the user's local connections with the
    /// documented 4009 error + close pair.
    #[tokio::test]
    async fn user_terminate_evicts_the_users_local_connections_with_4009() {
        let local = local_adapter();
        let (handle, mut rx) = conn();
        local.signin_user("app1", "u7", handle).await;

        let env = envelope(EnvelopeKind::UserTerminate, "node-other", "u7", "");
        drive(local, 16, vec![envelope_message(&env)]).await;

        assert_eq!(
            drained(&mut rx),
            vec![
                ServerEvent::Error(PusherError::new(4009, "You got disconnected by the app.")),
                ServerEvent::Close {
                    code: 4009,
                    reason: "You got disconnected by the app.".to_string(),
                },
            ],
            "a remote terminate must close the user's local sockets with 4009"
        );
    }

    /// The two watch kinds map to the `online` / `offline` watchlist changes for
    /// the user named in `channel`, delivered to this node's local watchers.
    #[tokio::test]
    async fn watch_kinds_notify_local_watchers_with_the_matching_change_name() {
        for (kind, name) in [
            (EnvelopeKind::WatchOnline, "online"),
            (EnvelopeKind::WatchOffline, "offline"),
        ] {
            let local = local_adapter();
            let (watcher, mut rx) = conn();
            local.watch_edges("app1", watcher, vec!["u9".to_string()]);

            let env = envelope(kind, "node-other", "u9", "");
            drive(local, 16, vec![envelope_message(&env)]).await;

            assert_eq!(
                drained(&mut rx),
                vec![ServerEvent::WatchlistEvents {
                    events: vec![WatchlistChange {
                        name: name.to_string(),
                        user_ids: vec!["u9".to_string()],
                    }],
                }],
                "a remote {name} transition must reach this node's local watchers"
            );
        }
    }

    /// A lagged receiver loses the skipped messages but the loop survives: the
    /// newest message still in the ring is delivered.
    #[tokio::test]
    async fn a_lagged_stream_drops_the_skipped_messages_and_keeps_going() {
        let local = local_adapter();
        let (handle, mut rx) = conn();
        local.subscribe("app1", "public-c", handle, None).await;

        // Capacity 1 with three sends before the loop reads: the receiver is two
        // behind, so its first `recv` is `Lagged(2)` and only the last message
        // is still in the ring.
        let msgs = ["A", "B", "C"]
            .iter()
            .map(|f| {
                envelope_message(&envelope(
                    EnvelopeKind::Broadcast,
                    "node-other",
                    "public-c",
                    f,
                ))
            })
            .collect();
        drive(local, 1, msgs).await;

        assert_eq!(
            drained(&mut rx),
            vec![ServerEvent::Raw(Arc::from("C"))],
            "only the message still in the ring survives a lag"
        );
    }
}
