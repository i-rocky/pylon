//! Single-worker `mio` event loop for the per-core transport (SP9).
//!
//! [`run`] binds a listener, then drives a non-blocking accept → handshake →
//! frame loop entirely on the calling thread with one [`mio::Poll`]. A
//! [`slab::Slab`] is the connection table: the slab key *is* the connection's
//! [`mio::Token`] value, so a readiness event maps to its [`Connection`] in
//! O(1). The listener uses a reserved token ([`LISTENER`]) that no slab key can
//! collide with.
//!
//! Readiness is managed edge-friendly: a connection is registered
//! `READABLE`-only and only gains `WRITABLE` interest when a [`flush`] returns
//! [`WriteStatus::WouldBlock`]; the interest is dropped back to `READABLE` once
//! the queue drains. This keeps the loop from spinning on a writable socket with
//! nothing to send.
//!
//! Two behaviours are supported:
//!
//! * [`Mode::Echo`] — every inbound data frame is re-encoded and queued straight
//!   back, pings are answered with pongs, a close tears the connection down.
//!   Used by the transport's own unit tests.
//! * [`Mode::Dispatch`] — the real Pusher v7 protocol. On handshake completion
//!   the worker resolves the `/app/{key}` tenant, builds a
//!   [`ConnectionContext`] (mirroring `ws::upgrade`), emits
//!   `pusher:connection_established`, and from then on decodes each inbound Text
//!   frame to a [`ClientCommand`] and drives `ctx.dispatch(..)` via
//!   `block_on`. After every dispatch (and once per loop iteration) every Open
//!   connection's mailbox is drained: queued [`ServerEvent`]s are encoded and
//!   written, so broadcast fan-out reaches its subscribers. This REUSES all
//!   subscribe/presence/client-event/signin logic — it does not reimplement the
//!   protocol.
//!
//! `block_on` is safe here because the [`LocalAdapter`](crate::adapter::local)
//! async methods never await real I/O; they complete synchronously.
//!
//! Safe Rust — the crate root sets `#![deny(unsafe_code)]`; this module adds no
//! `unsafe`.
//!
//! Multiple of these worker loops run in `TransportMode::Percore` (one per CPU),
//! each with its own `SO_REUSEPORT` listener on the same `bind:port`, so the
//! kernel spreads accepts across workers; see [`crate::transport::run_percore`].

use crate::adapter::Adapter;
use crate::app::AppManager;
use crate::protocol::command::ClientCommand;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use crate::protocol::{codec::Codec, negotiate};
use crate::transport::conn::{ConnError, ConnState, Connection, WriteStatus};
use crate::transport::frame::{self, OpCode};
use crate::transport::handshake::{self, HeadResult};
use crate::ws::handler::ConnectionContext;
use bytes::BytesMut;
use dashmap::DashMap;
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use std::collections::{HashMap, HashSet};
use std::io::{ErrorKind, Read};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Reserved token for the listener. Slab keys grow from 0, so the maximum
/// `usize` is guaranteed never to collide with a connection token.
const LISTENER: Token = Token(usize::MAX);

/// Reserved token for this worker's broadcast-inbox [`mio::Waker`]. One below
/// [`LISTENER`]; slab keys grow from 0 so neither reserved value can collide
/// with a connection token.
const BCAST_WAKER: Token = Token(usize::MAX - 1);

/// Shared, `Arc`-cloneable bundle of the `AppState` pieces a [`Mode::Dispatch`]
/// worker needs to build a [`ConnectionContext`] per connection — the same
/// inputs `ws::upgrade::serve` threads into `ConnectionParams`.
pub struct DispatchEnv {
    pub apps: Arc<dyn AppManager>,
    pub adapter: Arc<dyn Adapter>,
    pub limits: crate::server::config::Limits,
    pub activity_timeout: u32,
    pub strict_protocol: bool,
    /// Per-app live connection counters (shared with the rest of the server),
    /// mirroring `AppState::conn_counts`.
    pub conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
    pub webhooks: crate::webhook::WebhookHandle,
}

/// Configuration for a single worker event loop.
pub struct WorkerConfig {
    /// Address to bind the listener to.
    pub addr: std::net::SocketAddr,
    /// Maximum accepted WebSocket payload size (bytes) per frame.
    pub max_payload: usize,
    /// Per-connection outbound high-water mark (bytes) before backpressure-close.
    pub high_water: usize,
    /// Behaviour applied to inbound frames.
    pub mode: Mode,
    /// Sink for plain-HTTP (REST) connections accepted here but served on the
    /// tokio/axum plane (SP9 §3.4). `None` ⇒ no REST plane (the worker's own
    /// tests); a `Rest` head is then closed as before.
    pub rest_handoff: Option<mpsc::UnboundedSender<crate::transport::rest::RestConn>>,
    /// This worker's index among the spawned per-core workers, used only for
    /// accept-distribution logging (so an operator can confirm `SO_REUSEPORT` is
    /// spreading connections across cores). `0` for a lone/test worker.
    pub worker_id: usize,
    /// Per-core SHARDED broadcast wiring (SP9). `Some` for percore dispatch
    /// workers: the inbound side of this worker's broadcast inbox (paired with
    /// the `Sender` held in the sink) plus the slot whose `waker` `OnceLock` this
    /// worker fills at startup so the sink can nudge it. `None` for echo workers
    /// and the single-worker `tests/percore.rs` parity harness, which fall back
    /// to draining nothing here (those tests use no sink, so broadcasts route via
    /// the legacy registry mailbox path instead).
    pub broadcast: Option<BroadcastWiring>,
}

/// The per-worker half of the sharded broadcast plumbing handed to [`run`].
pub struct BroadcastWiring {
    /// Inbound broadcast hand-offs from the sink (the matching `Sender` lives in
    /// `slot.tx`). Drained on the [`BCAST_WAKER`] event and once per loop.
    pub rx: std::sync::mpsc::Receiver<crate::transport::fanout::BroadcastMsg>,
    /// This worker's sink slot; its `waker` `OnceLock` is filled at startup with
    /// a `Waker` built from this worker's own `Poll` registry.
    pub slot: Arc<crate::transport::fanout::WorkerSlot>,
}

/// Worker behaviour for inbound frames.
pub enum Mode {
    /// Echo every data frame back to the sender; answer pings with pongs.
    Echo,
    /// Drive the real Pusher v7 protocol via [`ConnectionContext::dispatch`].
    Dispatch(Arc<DispatchEnv>),
}

/// Per-connection v7 protocol state, present once the WS handshake completes on
/// a [`Mode::Dispatch`] worker. Mirrors what `connection::task::run` owns.
struct Session {
    ctx: ConnectionContext,
    /// Inbound side of the connection mailbox; the matching sender lives in
    /// `ctx.self_tx` (and is handed to other connections via `ctx.handle()`).
    rx: mpsc::UnboundedReceiver<ServerEvent>,
    codec: Box<dyn Codec>,
    /// The app id + its connection counter, so disconnect can decrement.
    conn_count: Arc<AtomicUsize>,
    /// The channel set this connection was in as of the last `local_subs`
    /// reconcile. Diffed against `ctx.subscribed` after each dispatch to compute
    /// the worker-local subscription-index deltas (added/removed channels).
    subs: HashSet<String>,
}

/// Per-connection slab entry: the [`Connection`] plus its read remainder and,
/// for dispatch workers, the v7 [`Session`] built at handshake completion.
///
/// `inbuf` is empty or tiny when the connection is idle (it only holds bytes
/// that arrived mid-frame). During [`ConnState::Handshaking`] it doubles as the
/// head-accumulation buffer until [`handshake::read_head`] returns something
/// other than [`HeadResult::NeedMore`].
struct Entry {
    conn: Connection,
    inbuf: BytesMut,
    /// The [`Token`] this connection is registered under (== `Token(slab_key)`).
    token: Token,
    /// v7 protocol state; `None` for echo workers and pre-handshake connections.
    session: Option<Session>,
}

/// Build a `mio` listener bound to `addr` with `SO_REUSEADDR` + `SO_REUSEPORT`
/// set before bind. `SO_REUSEPORT` lets every per-core worker bind the SAME
/// `bind:port` independently; the kernel then load-balances incoming connections
/// across the workers' listener sockets (one accept queue per worker).
fn reuseport_listener(addr: std::net::SocketAddr) -> std::io::Result<TcpListener> {
    let sock = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    sock.set_reuse_address(true)?;
    sock.set_reuse_port(true)?; // SO_REUSEPORT — kernel load-balances accepts across workers
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    sock.listen(1024)?;
    Ok(TcpListener::from_std(std::net::TcpListener::from(sock)))
}

/// Run the worker event loop until `shutdown` is set. Blocks the calling thread.
///
/// Builds its OWN `SO_REUSEPORT` listener on `cfg.addr` — every worker calls this
/// with the same address, and the kernel spreads accepts across them. Returns
/// once `shutdown` is observed `true` (clean stop) or a fatal I/O error occurs
/// while binding/polling.
pub fn run(mut cfg: WorkerConfig, shutdown: Arc<AtomicBool>) -> std::io::Result<()> {
    let mut poll = Poll::new()?;
    let mut listener = reuseport_listener(cfg.addr)?;
    poll.registry()
        .register(&mut listener, LISTENER, Interest::READABLE)?;

    // Per-core sharded broadcast plumbing (SP9). Take the wiring out of `cfg`
    // (the `Receiver` is not `Sync`, so it can't stay borrowed); build this
    // worker's own `Waker` on the reserved token and publish it into the sink
    // slot so the publisher can nudge us to drain. `None` ⇒ no broadcast inbox
    // (echo workers / single-worker parity harness): broadcasts route via the
    // legacy mailbox path, drained by `drain_all_sessions` as before.
    let broadcast = cfg.broadcast.take();
    let broadcast_rx = match &broadcast {
        Some(w) => {
            let waker = Arc::new(mio::Waker::new(poll.registry(), BCAST_WAKER)?);
            // The slot is created with an empty `OnceLock`; this is its only set.
            let _ = w.slot.waker.set(waker);
            Some(&w.rx)
        }
        None => None,
    };

    // Worker-local subscription index: which of THIS worker's connections are in
    // each `(app, channel)`. Populated by reconciling `ctx.subscribed` after each
    // dispatch; consulted when a `BroadcastMsg` arrives to fan the frame out to
    // exactly this worker's local subscribers.
    let mut local_subs: HashMap<(String, String), HashSet<SocketId>> = HashMap::new();
    // Reverse lookup: a subscriber's `socket_id` → its slab token, so a broadcast
    // delivery can find the connection in O(1) without scanning the slab.
    let mut sid_to_token: HashMap<SocketId, usize> = HashMap::new();

    let mut events = Events::with_capacity(1024);
    let mut conns: slab::Slab<Entry> = slab::Slab::new();

    // Adaptive poll timeout: when the previous iteration did real work (or any
    // connection still has buffered writes), poll non-blocking so cross-worker
    // mailbox deliveries drain promptly under load; when idle, block up to 50ms
    // (which also bounds how long `shutdown` goes unchecked) to avoid spinning.
    // TODO(followup): Waker-based selective drain for low-latency idle cross-worker delivery.
    let mut did_work = true;
    let dispatch = matches!(cfg.mode, Mode::Dispatch(_));
    // Total connections this worker has accepted — logged at shutdown so an
    // operator can confirm SO_REUSEPORT spread accepts across cores.
    let mut accepted_total: u64 = 0;

    loop {
        if shutdown.load(Ordering::SeqCst) {
            tracing::debug!(
                worker = cfg.worker_id,
                accepted = accepted_total,
                "percore worker stopping"
            );
            return Ok(());
        }

        let pending_writes = conns.iter().any(|(_, e)| e.conn.has_pending_writes());
        let timeout = if did_work || pending_writes {
            Some(Duration::from_millis(0))
        } else {
            Some(Duration::from_millis(50))
        };

        if let Err(e) = poll.poll(&mut events, timeout) {
            // A signal can interrupt the poll syscall; just retry.
            if e.kind() == ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }

        // Track whether this iteration accomplished anything worth a tight
        // re-poll: any readiness event, or a non-empty cross-worker drain below.
        let mut work = !events.is_empty();

        for event in events.iter() {
            match event.token() {
                LISTENER => {
                    accepted_total += accept_ready(&poll, &mut listener, &mut conns, &cfg);
                }
                // The broadcast `Waker` only exists to unblock the poll so the
                // post-loop drain runs promptly; no per-event work here.
                BCAST_WAKER => {}
                token => {
                    let key = token.0;
                    // The connection may have been removed earlier in this same
                    // event batch (e.g. a read closed it before its writable
                    // event is processed); skip stale tokens.
                    if !conns.contains(key) {
                        continue;
                    }

                    // A peer hangup / error: tear down regardless of r/w intent.
                    if event.is_error() || event.is_read_closed() || event.is_write_closed() {
                        remove(&poll, &mut conns, key, &mut local_subs, &mut sid_to_token);
                        continue;
                    }

                    if event.is_readable() {
                        match handle_readable(&poll, &mut conns, key, &cfg) {
                            Action::Close => {
                                remove(&poll, &mut conns, key, &mut local_subs, &mut sid_to_token);
                                continue;
                            }
                            Action::Handoff(prefix) => {
                                handoff_rest(&poll, &mut conns, key, &cfg, prefix);
                                continue;
                            }
                            Action::Keep => {
                                // A subscribe/unsubscribe in this readable batch
                                // may have changed channel membership; reconcile
                                // this connection's worker-local subscription
                                // index so later broadcasts route correctly.
                                if let Some(entry) = conns.get_mut(key) {
                                    if let Some(session) = entry.session.as_mut() {
                                        reconcile_membership(
                                            session,
                                            key,
                                            &mut local_subs,
                                            &mut sid_to_token,
                                        );
                                    }
                                }
                            }
                        }
                    }

                    if event.is_writable()
                        && conns.contains(key)
                        && handle_writable(&poll, &mut conns, key) == Action::Close
                    {
                        remove(&poll, &mut conns, key, &mut local_subs, &mut sid_to_token);
                    }
                }
            }
        }

        // Per-core SHARDED fan-out: drain this worker's broadcast inbox and
        // deliver each already-WS-framed payload to its LOCAL subscribers by
        // direct slab-enqueue (no per-conn mpsc, no per-conn wake). Run every
        // iteration (the Waker wakes an idle worker; the unconditional drain is a
        // safety net under load when no Waker event fires). Drains are no-ops
        // when the inbox is empty.
        if let Some(rx) = broadcast_rx {
            if drain_broadcasts(&poll, &mut conns, rx, &mut local_subs, &mut sid_to_token) {
                work = true;
            }
        }

        // Draining every Open dispatch connection once per loop iteration is how
        // a DIRECT send queued onto a connection's mailbox (subscription_succeeded,
        // member rosters, send_to_user, terminate, …) — which had no readiness
        // event of its own — is flushed without waiting for that peer to speak.
        // (Channel broadcasts now go through `drain_broadcasts` above when a sink
        // is wired; the legacy registry mailbox path still uses this drain.)
        // `drain_all_sessions` reports whether it actually wrote anything so the
        // adaptive timeout stays tight under load.
        if dispatch && drain_all_sessions(&poll, &mut conns, &mut local_subs, &mut sid_to_token) {
            work = true;
        }

        did_work = work;
    }
}

/// Outcome of handling a connection event: keep it, close it, or hand it off to
/// the tokio/axum REST plane.
#[derive(Debug, PartialEq, Eq)]
enum Action {
    Keep,
    Close,
    /// A plain-HTTP request head was detected: transfer the connection (and the
    /// `Vec<u8>` of bytes already read off the socket, to be replayed) to the
    /// REST handoff channel. Carries the bytes to replay.
    Handoff(Vec<u8>),
}

/// Drain the listener's accept backlog, registering every accepted socket.
/// Returns the number of connections accepted this call (for accept-distribution
/// accounting).
fn accept_ready(
    poll: &Poll,
    listener: &mut TcpListener,
    conns: &mut slab::Slab<Entry>,
    cfg: &WorkerConfig,
) -> u64 {
    let mut accepted = 0;
    loop {
        match listener.accept() {
            Ok((mut stream, _peer)) => {
                let entry = conns.vacant_entry();
                let key = entry.key();
                if let Err(e) =
                    poll.registry()
                        .register(&mut stream, Token(key), Interest::READABLE)
                {
                    // Registration failed: drop the socket, leave the slab slot
                    // unused (vacant_entry didn't consume it).
                    tracing::debug!(error = %e, "failed to register accepted socket");
                    continue;
                }
                entry.insert(Entry {
                    conn: Connection::new(stream, cfg.high_water),
                    inbuf: BytesMut::new(),
                    token: Token(key),
                    session: None,
                });
                accepted += 1;
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
            Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => {
                tracing::debug!(error = %e, "listener accept error");
                break;
            }
        }
    }
    accepted
}

/// Handle a readable event: either advance the handshake or process frames.
fn handle_readable(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    key: usize,
    cfg: &WorkerConfig,
) -> Action {
    let entry = &mut conns[key];
    match entry.conn.state {
        ConnState::Handshaking => handle_handshake(poll, entry, cfg),
        ConnState::Open | ConnState::Closing => handle_frames(poll, entry, cfg),
    }
}

/// Accumulate request-head bytes and, once complete, complete the WS upgrade.
fn handle_handshake(poll: &Poll, entry: &mut Entry, cfg: &WorkerConfig) -> Action {
    // Pull all available bytes into the head-accumulation buffer (`inbuf`).
    if drain_into(&mut entry.conn, &mut entry.inbuf) == ReadOutcome::Closed {
        return Action::Close;
    }

    match handshake::read_head(&entry.inbuf) {
        HeadResult::NeedMore => Action::Keep,
        HeadResult::WsUpgrade { key: ws_key, path } => {
            let response = handshake::accept_response(&ws_key).into_boxed_slice();
            // Drop-head queue never rejects; the 101 response always enqueues.
            let _ = entry.conn.queue(Arc::from(response));
            // A browser never sends data frames before the 101, so any bytes
            // after the head would be a protocol error anyway; clearing is safe.
            entry.inbuf.clear();
            entry.conn.state = ConnState::Open;

            // For a dispatch worker, build the v7 session now: resolve the app,
            // check capacity, create the mailbox + ConnectionContext, and queue
            // the connection_established frame. On failure we just close the
            // socket (acceptable for this task; the legacy path sends an error
            // frame first).
            if let Mode::Dispatch(env) = &cfg.mode {
                match establish_session(env, &path) {
                    Some(session) => {
                        let established = ServerEvent::ConnectionEstablished {
                            socket_id: session.ctx.socket_id.clone(),
                            activity_timeout: env.activity_timeout,
                        };
                        let text = session.codec.encode(&established);
                        let mut out = BytesMut::new();
                        frame::encode_text(&mut out, text.as_bytes());
                        let _ = entry.conn.queue(Arc::from(out.to_vec().into_boxed_slice()));
                        entry.session = Some(session);
                    }
                    None => return Action::Close,
                }
            }

            flush_and_arm(poll, entry)
        }
        // A plain-HTTP request (a Pusher REST publish): hand the connection off
        // to the tokio/axum plane. We have read *all* currently-available bytes
        // into `inbuf` (head + any body that arrived with it); the whole buffer
        // is the prefix to replay to the HTTP parser. With no REST plane wired
        // (`rest_handoff == None`, e.g. the worker's own echo tests) we close.
        HeadResult::Rest { .. } => {
            if cfg.rest_handoff.is_some() {
                Action::Handoff(entry.inbuf.to_vec())
            } else {
                Action::Close
            }
        }
        HeadResult::Bad(_) => Action::Close,
    }
}

/// Resolve the app + protocol from a `/app/{key}?protocol=N` path and build the
/// v7 [`Session`], mirroring `ws::upgrade::serve`: negotiate codec, look up the
/// app by key, enforce per-app capacity, and assemble the [`ConnectionContext`]
/// the same way `connection::task::run` does. Returns `None` (→ close) on any
/// rejection (bad protocol, unknown app, over capacity).
fn establish_session(env: &Arc<DispatchEnv>, path: &str) -> Option<Session> {
    let (key, protocol) = parse_app_path(path);

    let codec = negotiate(protocol.as_deref(), env.strict_protocol).ok()?;

    let app = futures_executor::block_on(env.apps.by_key(&key))?;

    let counter = env
        .conn_counts
        .entry(app.id.clone())
        .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
        .clone();
    let current = counter.fetch_add(1, Ordering::SeqCst);
    if app.capacity != 0 && current >= app.capacity as usize {
        counter.fetch_sub(1, Ordering::SeqCst);
        return None;
    }

    let socket_id = SocketId::generate();
    let (tx, rx) = mpsc::unbounded_channel::<ServerEvent>();
    let ctx = ConnectionContext {
        app,
        socket_id,
        self_tx: tx,
        adapter: env.adapter.clone(),
        client_event_rate: crate::ws::rate::RateWindow::new(
            env.limits.max_client_events_per_second,
        ),
        limits: env.limits,
        subscribed: HashSet::new(),
        user: None,
        webhooks: env.webhooks.clone(),
        presence_membership: HashMap::new(),
    };

    Some(Session {
        ctx,
        rx,
        codec,
        conn_count: counter,
        subs: HashSet::new(),
    })
}

/// Split a `/app/{key}` path (with an optional `?protocol=N&...` query) into the
/// app key and the `protocol` query value, mirroring how axum's `Path`/`Query`
/// extractors feed `ws::upgrade`.
fn parse_app_path(path: &str) -> (String, Option<String>) {
    let (raw_path, query) = match path.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (path, None),
    };
    let key = raw_path.strip_prefix("/app/").unwrap_or("").to_string();
    let protocol = query.and_then(|q| {
        q.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            (k == "protocol").then(|| v.to_string())
        })
    });
    (key, protocol)
}

/// Read and process every complete frame currently buffered, per [`Mode`].
fn handle_frames(poll: &Poll, entry: &mut Entry, cfg: &WorkerConfig) -> Action {
    let frames = {
        // Split the borrow so `inbuf` (the read remainder) and `conn` can be
        // borrowed at once via a temporary swap-out of the buffer.
        let mut scratch = std::mem::take(&mut entry.inbuf);
        let result = entry.conn.read_frames(&mut scratch, cfg.max_payload);
        entry.inbuf = scratch;
        match result {
            Ok(frames) => frames,
            // EOF or a fatal protocol violation: close.
            Err(ConnError::Closed) | Err(ConnError::Protocol(_)) => return Action::Close,
            Err(ConnError::Backpressure) => return Action::Close,
        }
    };

    match &cfg.mode {
        Mode::Echo => echo_frames(poll, entry, frames),
        Mode::Dispatch(_) => dispatch_frames(poll, entry, frames),
    }
}

/// [`Mode::Echo`]: re-encode every data frame back, answer pings with pongs.
fn echo_frames(poll: &Poll, entry: &mut Entry, frames: Vec<frame::Frame>) -> Action {
    let mut wrote = false;
    for f in frames {
        match f.opcode {
            OpCode::Text | OpCode::Binary | OpCode::Continuation => {
                let mut out = BytesMut::new();
                frame::encode(&mut out, f.fin, f.opcode, &f.payload);
                let _ = entry.conn.queue(Arc::from(out.to_vec().into_boxed_slice()));
                wrote = true;
            }
            OpCode::Ping => {
                let mut out = BytesMut::new();
                frame::encode(&mut out, true, OpCode::Pong, &f.payload);
                let _ = entry.conn.queue(Arc::from(out.to_vec().into_boxed_slice()));
                wrote = true;
            }
            // A peer pong is unsolicited noise here; ignore it.
            OpCode::Pong => {}
            OpCode::Close => return Action::Close,
        }
    }

    if wrote {
        flush_and_arm(poll, entry)
    } else {
        Action::Keep
    }
}

/// [`Mode::Dispatch`]: decode each Text frame to a [`ClientCommand`] and drive
/// `ctx.dispatch`, answer pings with pongs, close on a Close frame, then drain
/// this connection's mailbox so any self-directed replies go out.
fn dispatch_frames(poll: &Poll, entry: &mut Entry, frames: Vec<frame::Frame>) -> Action {
    for f in frames {
        match f.opcode {
            OpCode::Text => {
                // The session always exists once Open on a dispatch worker.
                let Some(session) = entry.session.as_mut() else {
                    return Action::Close;
                };
                let text = match std::str::from_utf8(&f.payload) {
                    Ok(t) => t,
                    // A non-UTF-8 text frame is malformed; mirror legacy and drop.
                    Err(_) => continue,
                };
                match session.codec.decode(text) {
                    Ok(cmd) => dispatch_command(session, cmd),
                    Err(e) => {
                        // Unparseable frames are silently dropped (parity with
                        // `connection::task`); 4200 is a close/reconnect code and
                        // must not be sent in-band.
                        tracing::trace!("dropping malformed client frame: {e}");
                    }
                }
            }
            OpCode::Ping => {
                let mut out = BytesMut::new();
                frame::encode(&mut out, true, OpCode::Pong, &f.payload);
                let _ = entry.conn.queue(Arc::from(out.to_vec().into_boxed_slice()));
            }
            OpCode::Pong => {}
            // Binary/Continuation are not part of the Pusher protocol; ignore.
            OpCode::Binary | OpCode::Continuation => {}
            OpCode::Close => return Action::Close,
        }
    }

    // Drain this connection's mailbox: dispatch may have enqueued self-directed
    // replies (subscription_succeeded, pong, errors) plus the adapter may have
    // fanned a broadcast onto it.
    drain_session(poll, entry).0
}

/// Run one command through the (async) protocol handler synchronously.
fn dispatch_command(session: &mut Session, cmd: ClientCommand) {
    futures_executor::block_on(session.ctx.dispatch(cmd));
}

/// Drain every queued [`ServerEvent`] from this connection's mailbox: encode and
/// queue each as a Text frame, except [`ServerEvent::Close`] which becomes a
/// WebSocket Close frame and ends the connection. Returns the resulting
/// [`Action`] (`Close` if a close was requested or a write failed) plus whether
/// anything was actually written (so the loop's adaptive poll stays tight).
fn drain_session(poll: &Poll, entry: &mut Entry) -> (Action, bool) {
    let Some(session) = entry.session.as_mut() else {
        return (Action::Keep, false);
    };

    let mut close_after = false;
    let mut wrote = false;
    while let Ok(ev) = session.rx.try_recv() {
        match ev {
            ServerEvent::Close { code, reason } => {
                let mut out = BytesMut::new();
                let mut frame_body = Vec::with_capacity(2 + reason.len());
                frame_body.extend_from_slice(&code.to_be_bytes());
                frame_body.extend_from_slice(reason.as_bytes());
                frame::encode(&mut out, true, OpCode::Close, &frame_body);
                let _ = entry.conn.queue(Arc::from(out.to_vec().into_boxed_slice()));
                wrote = true;
                close_after = true;
                break;
            }
            other => {
                let text = session.codec.encode(&other);
                let mut out = BytesMut::new();
                frame::encode_text(&mut out, text.as_bytes());
                let _ = entry.conn.queue(Arc::from(out.to_vec().into_boxed_slice()));
                wrote = true;
            }
        }
    }

    if wrote && flush_and_arm(poll, entry) == Action::Close {
        return (Action::Close, wrote);
    }
    if close_after {
        (Action::Close, wrote)
    } else {
        (Action::Keep, wrote)
    }
}

/// Drain every Open dispatch connection's mailbox once. Connections that request
/// a close (or whose write fails) are torn down. Called once per loop iteration
/// so a broadcast queued onto a peer's mailbox is delivered even when that peer
/// produced no readiness event of its own. Returns `true` if any connection
/// actually wrote a queued event (used to keep the adaptive poll tight).
fn drain_all_sessions(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    local_subs: &mut HashMap<(String, String), HashSet<SocketId>>,
    sid_to_token: &mut HashMap<SocketId, usize>,
) -> bool {
    let keys: Vec<usize> = conns
        .iter()
        .filter(|(_, e)| e.session.is_some() && e.conn.state == ConnState::Open)
        .map(|(k, _)| k)
        .collect();
    let mut wrote_any = false;
    for key in keys {
        if !conns.contains(key) {
            continue;
        }
        let (action, wrote) = drain_session(poll, &mut conns[key]);
        wrote_any |= wrote;
        if action == Action::Close {
            remove(poll, conns, key, local_subs, sid_to_token);
        }
    }
    wrote_any
}

/// Handle a writable event: flush and, when drained, drop writable interest.
fn handle_writable(poll: &Poll, conns: &mut slab::Slab<Entry>, key: usize) -> Action {
    let entry = &mut conns[key];
    flush_and_arm(poll, entry)
}

/// Flush the outbound queue and reconcile writable interest with what remains.
///
/// * [`WriteStatus::Drained`] → re-arm `READABLE`-only (drop `WRITABLE`).
/// * [`WriteStatus::WouldBlock`] → add `WRITABLE` so we get a writable event.
/// * [`WriteStatus::Closed`] → close.
fn flush_and_arm(poll: &Poll, entry: &mut Entry) -> Action {
    // Read the token before the mutable stream borrow below.
    let token = entry.token;
    match entry.conn.flush() {
        WriteStatus::Drained => {
            if poll
                .registry()
                .reregister(&mut entry.conn.stream, token, Interest::READABLE)
                .is_err()
            {
                return Action::Close;
            }
            Action::Keep
        }
        WriteStatus::WouldBlock => {
            if poll
                .registry()
                .reregister(
                    &mut entry.conn.stream,
                    token,
                    Interest::READABLE | Interest::WRITABLE,
                )
                .is_err()
            {
                return Action::Close;
            }
            Action::Keep
        }
        WriteStatus::Closed => Action::Close,
    }
}

/// Read all currently-available bytes off the socket into `buf`, stopping on
/// `WouldBlock` (socket drained) or EOF. Used only during the handshake, where
/// we accumulate the raw head before any framing.
#[derive(PartialEq, Eq)]
enum ReadOutcome {
    Ok,
    Closed,
}

fn drain_into(conn: &mut Connection, buf: &mut BytesMut) -> ReadOutcome {
    let mut chunk = [0u8; 16 * 1024];
    loop {
        match conn.stream.read(&mut chunk) {
            Ok(0) => return ReadOutcome::Closed,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => return ReadOutcome::Ok,
            Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(_) => return ReadOutcome::Closed,
        }
    }
}

/// Transfer a plain-HTTP connection to the tokio/axum REST plane (SP9 §3.4).
///
/// Order matters: deregister the stream from this `Poll` and remove the slab
/// entry BEFORE moving the fd out of mio, so mio's registry/slab no longer
/// reference it. Then [`rest::mio_to_std`] transfers fd ownership into a
/// `std::net::TcpStream` (the single audited `unsafe` site), and the connection
/// plus its already-read `prefix` bytes are sent to the handoff channel. The
/// stream stays non-blocking (inherited from mio), which is what tokio wants.
///
/// On a missing handoff sender, or a closed channel, the connection is simply
/// dropped (closed). A pre-handshake REST connection never has a [`Session`], so
/// no on-close hook / counter decrement is needed.
fn handoff_rest(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    key: usize,
    cfg: &WorkerConfig,
    prefix: Vec<u8>,
) {
    let Some(mut entry) = conns.try_remove(key) else {
        return;
    };
    let _ = poll.registry().deregister(&mut entry.conn.stream);

    let Some(tx) = cfg.rest_handoff.as_ref() else {
        // No REST plane; dropping `entry` closes the socket.
        return;
    };

    let std_stream = crate::transport::rest::mio_to_std(entry.conn.into_stream());
    if let Err(e) = tx.send(crate::transport::rest::RestConn {
        fd_stream: std_stream,
        prefix,
    }) {
        // Receiver gone (REST task ended): dropping the RestConn closes the fd.
        tracing::debug!(error = %e, "REST handoff channel closed; dropping connection");
    }
}

/// Remove a connection: drop it from the worker's sharded-broadcast indexes,
/// run the protocol on-close hook (dispatch only), decrement the app's
/// connection counter, deregister its socket, and drop the slab entry.
///
/// The index cleanup happens BEFORE `on_close()` so that the unsubscribe-driven
/// broadcasts `on_close` fans out (member_removed / subscription_count) can no
/// longer route back to this very connection, and so a concurrent broadcast
/// drain never targets a slab slot that is about to vanish.
fn remove(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    key: usize,
    local_subs: &mut HashMap<(String, String), HashSet<SocketId>>,
    sid_to_token: &mut HashMap<SocketId, usize>,
) {
    if let Some(mut entry) = conns.try_remove(key) {
        if let Some(mut session) = entry.session.take() {
            deindex_connection(&session, local_subs, sid_to_token);
            futures_executor::block_on(session.ctx.on_close());
            session.conn_count.fetch_sub(1, Ordering::SeqCst);
        }
        let _ = poll.registry().deregister(&mut entry.conn.stream);
    }
}

/// Drop a closing connection's `socket_id` from every `(app, channel)` it was
/// indexed under, and from the reverse `socket_id → token` map. Uses the
/// session's last-reconciled `subs` set (the channels recorded in `local_subs`),
/// so it removes exactly the entries `reconcile_membership` inserted.
fn deindex_connection(
    session: &Session,
    local_subs: &mut HashMap<(String, String), HashSet<SocketId>>,
    sid_to_token: &mut HashMap<SocketId, usize>,
) {
    let app = session.ctx.app.id.clone();
    let sid = &session.ctx.socket_id;
    for channel in &session.subs {
        let k = (app.clone(), channel.clone());
        if let Some(set) = local_subs.get_mut(&k) {
            set.remove(sid);
            if set.is_empty() {
                local_subs.remove(&k);
            }
        }
    }
    sid_to_token.remove(sid);
}

/// Reconcile a connection's worker-local subscription index against the protocol
/// state after a dispatch. Diffs the session's previously-recorded channel set
/// (`session.subs`) against `ctx.subscribed`: channels newly joined are inserted
/// into `local_subs` (and the `socket_id → token` reverse map is (re)stamped),
/// channels left are removed. Cheap when nothing changed (two set diffs over the
/// usually-tiny per-connection channel set). `token` is this connection's slab
/// key. No-op for a connection in no channels with no change.
fn reconcile_membership(
    session: &mut Session,
    token: usize,
    local_subs: &mut HashMap<(String, String), HashSet<SocketId>>,
    sid_to_token: &mut HashMap<SocketId, usize>,
) {
    if session.subs == session.ctx.subscribed {
        return;
    }
    let app = session.ctx.app.id.clone();
    let sid = &session.ctx.socket_id;

    // Added channels: present in ctx.subscribed, absent from the recorded set.
    for channel in session.ctx.subscribed.difference(&session.subs) {
        local_subs
            .entry((app.clone(), channel.clone()))
            .or_default()
            .insert(sid.clone());
    }
    // Removed channels: were recorded, no longer subscribed.
    for channel in session.subs.difference(&session.ctx.subscribed) {
        let k = (app.clone(), channel.clone());
        if let Some(set) = local_subs.get_mut(&k) {
            set.remove(sid);
            if set.is_empty() {
                local_subs.remove(&k);
            }
        }
    }
    // Keep the reverse map current (stamp on first subscribe; harmless re-stamp).
    sid_to_token.insert(sid.clone(), token);
    // Record the new set as the reconcile baseline.
    session.subs = session.ctx.subscribed.clone();
}

/// Deliver every queued [`BroadcastMsg`] to this worker's local subscribers.
///
/// For each message: look up the `(app, channel)` subscriber set, and for each
/// subscriber (skipping `except`) find its slab entry via `sid_to_token` and
/// `queue` the already-WS-framed `frame` (an `Arc` bump — never re-encoded).
/// Connections that backpressure-close are torn down. Returns `true` if any
/// frame was queued (so the adaptive poll stays tight) — the actual socket write
/// is done by flushing touched connections, handled by the caller.
fn drain_broadcasts(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    rx: &std::sync::mpsc::Receiver<crate::transport::fanout::BroadcastMsg>,
    local_subs: &mut HashMap<(String, String), HashSet<SocketId>>,
    sid_to_token: &mut HashMap<SocketId, usize>,
) -> bool {
    let mut touched: HashSet<usize> = HashSet::new();
    // Connections that backpressured during delivery; closed after the drain so
    // we don't mutate the slab mid-lookup.
    let mut to_close: Vec<usize> = Vec::new();

    while let Ok(msg) = rx.try_recv() {
        let key = (msg.app.to_string(), msg.channel.to_string());
        let Some(subs) = local_subs.get(&key) else {
            continue; // no local subscribers for this channel on this worker
        };
        for sid in subs.iter() {
            if msg.except.as_ref() == Some(sid) {
                continue; // sender exclusion
            }
            let Some(&token) = sid_to_token.get(sid) else {
                continue; // stale index entry; connection gone
            };
            if to_close.contains(&token) {
                continue;
            }
            let Some(entry) = conns.get_mut(token) else {
                continue;
            };
            // Only deliver to Open dispatch connections.
            if entry.session.is_none() || entry.conn.state != ConnState::Open {
                continue;
            }
            // SP10: the per-connection queue is byte-bounded drop-head — it never
            // rejects. A slow consumer simply loses its OLDEST queued frame(s)
            // (freshest-wins, at-most-once), keeping memory bounded without
            // closing the connection or stalling the fast path.
            let _dropped = entry.conn.queue(msg.frame.clone());
            touched.insert(token);
        }
    }

    let wrote = !touched.is_empty();
    // Flush every connection we queued onto. A flush that backpressures arms
    // writable interest (handled in flush_and_arm); a failed flush closes.
    for token in touched {
        if to_close.contains(&token) {
            continue;
        }
        if let Some(entry) = conns.get_mut(token) {
            if flush_and_arm(poll, entry) == Action::Close {
                to_close.push(token);
            }
        }
    }
    for token in to_close {
        remove(poll, conns, token, local_subs, sid_to_token);
    }
    wrote
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use std::sync::atomic::AtomicBool;
    use tokio_tungstenite::tungstenite::Message;

    /// Reserve a free port via a throwaway std listener, then drop it. The OS
    /// won't immediately hand the same port to a different process, so the
    /// worker re-binding it moments later is race-free in practice.
    fn free_port() -> u16 {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    }

    /// Spawn the worker on its own OS thread bound to `addr` in [`Mode::Echo`],
    /// returning the shutdown flag and the join handle.
    fn spawn_worker(addr: std::net::SocketAddr) -> (Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        let shutdown = Arc::new(AtomicBool::new(false));
        let sd = shutdown.clone();
        let handle = std::thread::spawn(move || {
            run(
                WorkerConfig {
                    addr,
                    max_payload: 1 << 20,
                    high_water: 1 << 20,
                    mode: Mode::Echo,
                    rest_handoff: None,
                    worker_id: 0,
                    broadcast: None,
                },
                sd,
            )
            .expect("worker run failed");
        });
        (shutdown, handle)
    }

    /// THE GATE: a real `tokio-tungstenite` client completes the RFC 6455
    /// handshake against the worker and gets its text frame echoed back.
    #[tokio::test]
    async fn worker_handshakes_and_echoes() {
        let port = free_port();
        let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let (shutdown, handle) = spawn_worker(addr);

        // Give the worker a moment to bind before the client connects.
        tokio::time::sleep(Duration::from_millis(150)).await;

        let url = format!("ws://127.0.0.1:{port}/app/app-key");
        let (mut ws, _resp) = tokio_tungstenite::connect_async(url)
            .await
            .expect("ws connect/handshake");

        ws.send(Message::Text("hello".into()))
            .await
            .expect("send text");

        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("echo within 5s")
            .expect("stream not ended")
            .expect("frame ok");
        assert_eq!(msg.into_text().unwrap(), "hello");

        // A ping must be answered with a pong carrying the same payload.
        ws.send(Message::Ping(b"ping-payload".to_vec()))
            .await
            .expect("send ping");
        // tungstenite auto-responds to pongs at the protocol layer, so drive the
        // stream until we observe the pong (or our own buffered messages).
        let pong = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match ws.next().await {
                    Some(Ok(Message::Pong(p))) => return Some(p),
                    Some(Ok(_)) => continue,
                    _ => return None,
                }
            }
        })
        .await
        .expect("pong within 5s");
        assert_eq!(pong.as_deref(), Some(&b"ping-payload"[..]));

        shutdown.store(true, Ordering::SeqCst);
        handle.join().unwrap();
    }

    /// A second connection on the same worker also handshakes and echoes,
    /// exercising the slab's multi-connection path (distinct tokens).
    #[tokio::test]
    async fn worker_handles_multiple_connections() {
        let port = free_port();
        let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let (shutdown, handle) = spawn_worker(addr);
        tokio::time::sleep(Duration::from_millis(150)).await;

        let url = format!("ws://127.0.0.1:{port}/app/app-key");
        let (mut a, _) = tokio_tungstenite::connect_async(url.clone())
            .await
            .expect("connect a");
        let (mut b, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("connect b");

        a.send(Message::Text("aaa".into())).await.unwrap();
        b.send(Message::Text("bbb".into())).await.unwrap();

        let ma = tokio::time::timeout(Duration::from_secs(5), a.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let mb = tokio::time::timeout(Duration::from_secs(5), b.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(ma.into_text().unwrap(), "aaa");
        assert_eq!(mb.into_text().unwrap(), "bbb");

        shutdown.store(true, Ordering::SeqCst);
        handle.join().unwrap();
    }

    #[test]
    fn parse_app_path_extracts_key_and_protocol() {
        assert_eq!(
            parse_app_path("/app/app-key?protocol=7"),
            ("app-key".to_string(), Some("7".to_string()))
        );
        assert_eq!(
            parse_app_path("/app/app-key"),
            ("app-key".to_string(), None)
        );
        assert_eq!(
            parse_app_path("/app/k?foo=1&protocol=7&bar=2"),
            ("k".to_string(), Some("7".to_string()))
        );
    }
}
