//! Per-connection task: owns the socket writer, drains the mailbox, enforces liveness.

use crate::adapter::Adapter;
use crate::app::App;
use crate::protocol::codec::Codec;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use crate::ws::handler::ConnectionContext;
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

pub struct ConnectionParams {
    pub app: App,
    pub adapter: Arc<dyn Adapter>,
    pub limits: crate::server::config::Limits,
    pub activity_timeout: u32,
    pub pong_timeout: u32,
    pub conn_count: Arc<AtomicUsize>,
    pub webhooks: crate::webhook::WebhookHandle,
}

pub async fn run(socket: WebSocket, codec: Box<dyn Codec>, params: ConnectionParams) {
    let socket_id = SocketId::generate();
    let (tx, mut rx) = mpsc::unbounded_channel::<ServerEvent>();
    let (mut sink, mut stream) = socket.split();

    let established = ServerEvent::ConnectionEstablished {
        socket_id: socket_id.clone(),
        activity_timeout: params.activity_timeout,
    };
    if sink
        .send(Message::Text(codec.encode(&established).into()))
        .await
        .is_err()
    {
        params.conn_count.fetch_sub(1, Ordering::SeqCst);
        return;
    }

    let mut ctx = ConnectionContext {
        app: params.app.clone(),
        socket_id,
        self_tx: tx.clone(),
        adapter: params.adapter.clone(),
        client_event_rate: crate::ws::rate::RateWindow::new(
            params.limits.max_client_events_per_second,
        ),
        limits: params.limits,
        subscribed: HashSet::new(),
        user: None,
        webhooks: params.webhooks.clone(),
        presence_membership: std::collections::HashMap::new(),
    };

    let activity = Duration::from_secs(params.activity_timeout as u64);
    let pong = Duration::from_secs(params.pong_timeout as u64);
    let mut last_activity = Instant::now();
    let mut ping_sent_at: Option<Instant> = None;
    let mut ticker = tokio::time::interval(pong.max(Duration::from_millis(250)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            maybe = stream.next() => {
                match maybe {
                    Some(Ok(Message::Text(t))) => {
                        last_activity = Instant::now();
                        ping_sent_at = None;
                        match codec.decode(t.as_str()) {
                            Ok(cmd) => ctx.dispatch(cmd).await,
                            Err(e) => {
                                // Unparseable frames are silently dropped; 4200 is a
                                // close/reconnect code and must not be sent in-band.
                                tracing::trace!("dropping malformed client frame: {e}");
                            }
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        last_activity = Instant::now();
                        let _ = sink.send(Message::Pong(p)).await;
                    }
                    Some(Ok(Message::Pong(_))) => {
                        last_activity = Instant::now();
                        ping_sent_at = None;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
            Some(ev) = rx.recv() => {
                match ev {
                    ServerEvent::Close { code, reason } => {
                        use axum::extract::ws::CloseFrame;
                        let _ = sink
                            .send(Message::Close(Some(CloseFrame { code, reason: reason.into() })))
                            .await;
                        break;
                    }
                    other => {
                        if sink.send(Message::Text(codec.encode(&other).into())).await.is_err() {
                            break;
                        }
                    }
                }
            }
            _ = ticker.tick() => {
                let now = Instant::now();
                match ping_sent_at {
                    Some(sent) if now.duration_since(sent) >= pong => {
                        use axum::extract::ws::CloseFrame;
                        let _ = sink
                            .send(Message::Close(Some(CloseFrame {
                                code: 4201,
                                reason: "Pong reply not received: ping was sent to the client, but no reply was received".into(),
                            })))
                            .await;
                        break;
                    }
                    None if now.duration_since(last_activity) >= activity => {
                        let ping = codec.encode(&ServerEvent::Ping);
                        if sink.send(Message::Text(ping.into())).await.is_err() {
                            break;
                        }
                        ping_sent_at = Some(now);
                    }
                    _ => {}
                }
            }
        }
    }

    ctx.on_close().await;
    params.conn_count.fetch_sub(1, Ordering::SeqCst);
}
