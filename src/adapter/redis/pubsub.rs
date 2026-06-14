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
use fred::types::Message;
use std::sync::Arc;
use tokio::sync::broadcast;

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
                        // The envelope carries the finished v7 frame as a JSON string.
                        let frame = match env.event.as_str() {
                            Some(s) => s.to_string(),
                            None => continue,
                        };
                        // Honour `except` even on the relaying node (usually a no-op:
                        // the excepted socket lives on the originating node).
                        let except = env.except.as_deref().map(SocketId::from_raw);
                        local
                            .broadcast(
                                &env.app,
                                &env.channel,
                                ServerEvent::Raw(std::sync::Arc::from(frame)),
                                except,
                            )
                            .await;
                    }
                    EnvelopeKind::UserSend => {
                        let frame = match env.event.as_str() {
                            Some(s) => s.to_string(),
                            None => continue,
                        };
                        local
                            .send_to_user(
                                &env.app,
                                &env.channel,
                                ServerEvent::Raw(std::sync::Arc::from(frame)),
                            )
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
