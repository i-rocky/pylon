use hmac::{Hmac, KeyInit, Mac};
use md5::{Digest, Md5};
use rand::RngExt;
use serde_json::{json, Value};
use sha2::Sha256;

/// Lowercase hex HMAC-SHA256(secret, msg).
pub fn hmac_hex(secret: &str, msg: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac key");
    mac.update(msg.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Channel subscribe auth token: "<key>:" + HMAC(secret, "socket_id:channel[:channel_data]").
pub fn channel_auth(
    key: &str,
    secret: &str,
    socket_id: &str,
    channel: &str,
    channel_data: Option<&str>,
) -> String {
    let mut msg = format!("{socket_id}:{channel}");
    if let Some(cd) = channel_data {
        msg.push(':');
        msg.push_str(cd);
    }
    format!("{key}:{}", hmac_hex(secret, &msg))
}

#[cfg(test)]
mod sign_tests {
    use super::*;

    #[test]
    fn channel_auth_known_answer() {
        let token = channel_auth("app-key", "app-secret", "123.456", "private-foo", None);
        assert!(token.starts_with("app-key:"));
        let sig = token.strip_prefix("app-key:").unwrap();
        assert_eq!(sig.len(), 64); // hex sha256
        assert_eq!(
            sig,
            "c53bc505cb3d68dc9905dea8d5ed3c42f9e24aeed9453b7b9b200ff759958c02"
        );
    }
}

/// Build the signed query string for `POST /apps/{id}/events`.
/// Canonical string = "POST\n/apps/{id}/events\n<sorted k=v joined by &>", where the
/// sorted params include auth_key, auth_timestamp, auth_version=1.0, body_md5.
/// Returns the full query string (without leading '?') including auth_signature.
pub fn sign_post_events(
    key: &str,
    secret: &str,
    app_id: &str,
    body: &str,
    timestamp: u64,
) -> String {
    let body_md5 = hex::encode(Md5::digest(body.as_bytes()));
    let mut params = [
        ("auth_key".to_string(), key.to_string()),
        ("auth_timestamp".to_string(), timestamp.to_string()),
        ("auth_version".to_string(), "1.0".to_string()),
        ("body_md5".to_string(), body_md5),
    ];
    params.sort_by(|a, b| a.0.cmp(&b.0));
    let query: String = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let path = format!("/apps/{app_id}/events");
    let to_sign = format!("POST\n{path}\n{query}");
    let sig = hmac_hex(secret, &to_sign);
    format!("{query}&auth_signature={sig}")
}

#[cfg(test)]
mod rest_sign_tests {
    use super::*;

    #[test]
    fn sign_post_events_shape() {
        let q = sign_post_events("app-key", "app-secret", "app", "{}", 1_700_000_000);
        assert!(q.contains("auth_key=app-key"));
        assert!(q.contains("auth_timestamp=1700000000"));
        assert!(q.contains("auth_version=1.0"));
        assert!(q.contains(&format!("body_md5={}", hex::encode(md5::Md5::digest("{}")))));
        assert!(q.contains("&auth_signature="));
        assert!(q.starts_with("auth_key="));
    }
}

/// A parsed Pusher frame: `{event, channel?, data?}` where data is the (possibly
/// double-encoded) payload string.
#[derive(Debug, Clone)]
pub struct Frame {
    pub event: String,
    pub channel: Option<String>,
    pub data: Option<String>,
}

pub fn parse_frame(text: &str) -> Option<Frame> {
    let v: Value = serde_json::from_str(text).ok()?;
    Some(Frame {
        event: v.get("event")?.as_str()?.to_string(),
        channel: v.get("channel").and_then(|c| c.as_str()).map(String::from),
        data: v.get("data").map(|d| match d {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        }),
    })
}

/// Encode a `pusher:subscribe` frame.
pub fn subscribe_frame(channel: &str, auth: Option<&str>, channel_data: Option<&str>) -> String {
    let mut data = json!({ "channel": channel });
    if let Some(a) = auth {
        data["auth"] = json!(a);
    }
    if let Some(cd) = channel_data {
        data["channel_data"] = json!(cd);
    }
    json!({ "event": "pusher:subscribe", "data": data }).to_string()
}

pub fn pong_frame() -> String {
    json!({ "event": "pusher:pong", "data": {} }).to_string()
}

pub fn stamp_payload(seq: u64, publish_nanos: u128, run_id: &str) -> String {
    json!({ "seq": seq, "t": publish_nanos.to_string(), "p": run_id }).to_string()
}

pub struct Delivery {
    pub seq: u64,
    pub t: u128,
    pub p: String,
}

pub fn parse_delivery(data: &str) -> Option<Delivery> {
    let v: Value = serde_json::from_str(data).ok()?;
    Some(Delivery {
        seq: v.get("seq")?.as_u64()?,
        t: v.get("t")?.as_str()?.parse().ok()?,
        p: v.get("p")?.as_str()?.to_string(),
    })
}

pub fn run_id() -> String {
    let mut buf = [0u8; 4];
    rand::rng().fill(&mut buf[..]);
    hex::encode(buf)
}

#[cfg(test)]
mod frame_tests {
    use super::*;

    #[test]
    fn parse_connection_established() {
        let f = parse_frame(
            r#"{"event":"pusher:connection_established","data":"{\"socket_id\":\"1.2\"}"}"#,
        )
        .unwrap();
        assert_eq!(f.event, "pusher:connection_established");
        let inner: Value = serde_json::from_str(f.data.as_ref().unwrap()).unwrap();
        assert_eq!(inner["socket_id"], "1.2");
    }

    #[test]
    fn subscribe_frame_includes_auth() {
        let s = subscribe_frame("private-x", Some("app-key:deadbeef"), None);
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["event"], "pusher:subscribe");
        assert_eq!(v["data"]["channel"], "private-x");
        assert_eq!(v["data"]["auth"], "app-key:deadbeef");
    }

    #[test]
    fn stamp_and_parse_roundtrip() {
        let payload = stamp_payload(7, 123_456_789, "ab12cd34");
        let delivery = parse_delivery(&payload).unwrap();
        assert_eq!(delivery.seq, 7);
        assert_eq!(delivery.t, 123_456_789);
        assert_eq!(delivery.p, "ab12cd34");
    }
}

#[cfg(test)]
mod delivery_tests {
    use super::*;
    use crate::metrics::{Counters, Latency};
    use std::time::Instant;

    #[test]
    fn own_delivery_is_counted_and_measured() {
        let lat = Latency::default();
        let counters = Counters::default();
        let epoch = Instant::now();
        let payload = stamp_payload(1, epoch.elapsed().as_nanos(), "own-id");
        record_delivery(&payload, "own-id", epoch, &lat, &counters);
        assert_eq!(
            counters.received.load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            counters
                .received_foreign
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(lat.summary_us().0, 1);
    }

    #[test]
    fn foreign_delivery_is_counted_and_not_measured() {
        let lat = Latency::default();
        let counters = Counters::default();
        let epoch = Instant::now();
        let payload = stamp_payload(1, epoch.elapsed().as_nanos(), "other-id");
        record_delivery(&payload, "own-id", epoch, &lat, &counters);
        assert_eq!(
            counters.received.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            counters
                .received_foreign
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(lat.summary_us().0, 0);
    }
}

use crate::metrics::{Counters, Latency};
use futures_util::{SinkExt, StreamExt};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::{TcpSocket, TcpStream};
use tokio_tungstenite::{client_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

pub type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Open a TCP stream from a specific source IP to the ws host, then do the WS upgrade.
/// `src_ip` of `None` lets the OS pick the source address (default behavior).
async fn connect_bound(url: &str, src_ip: Option<IpAddr>) -> anyhow::Result<Ws> {
    let parsed = url::Url::parse(url)?;
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("no host"))?;
    let port = parsed.port().unwrap_or(80);
    // The host is an IP literal in our usage; otherwise resolve it.
    let addr: SocketAddr = match format!("{host}:{port}").parse() {
        Ok(a) => a,
        Err(_) => tokio::net::lookup_host(format!("{host}:{port}"))
            .await?
            .next()
            .ok_or_else(|| anyhow::anyhow!("no address for {host}:{port}"))?,
    };
    let socket = TcpSocket::new_v4()?;
    if let Some(ip) = src_ip {
        socket.bind(SocketAddr::new(ip, 0))?;
    }
    let stream = socket.connect(addr).await?;
    let (ws, _) = client_async(url, MaybeTlsStream::Plain(stream)).await?;
    Ok(ws)
}

/// Monotonic nanos since a shared process epoch, so publisher and subscribers share a clock.
pub fn epoch() -> Instant {
    // Callers pass a single shared Instant; this is a convenience for tests.
    Instant::now()
}

pub struct ClientConfig {
    pub url: String,
    pub key: String,
    pub secret: String,
    pub channel: String,
    pub private: bool, // sign the subscribe
    /// Source IP to bind the TCP socket to (None = OS default).
    pub src_ip: Option<IpAddr>,
    pub own_run_id: String,
}

fn record_delivery(
    data: &str,
    own_run_id: &str,
    epoch: Instant,
    lat: &Latency,
    counters: &Counters,
) {
    let Some(delivery) = parse_delivery(data) else {
        return;
    };
    if delivery.p == own_run_id {
        let now = epoch.elapsed().as_nanos();
        let d = now.saturating_sub(delivery.t);
        lat.record_nanos(d.min(u64::MAX as u128) as u64);
        Counters::inc(&counters.received);
    } else {
        Counters::inc(&counters.received_foreign);
    }
}

/// Connect, handshake, subscribe, then receive until `shutdown` notifies. Records latency
/// of every `data` event carrying a timestamp; replies to ping with pong.
pub async fn run_client(
    cfg: ClientConfig,
    epoch: Instant,
    lat: Arc<Latency>,
    counters: Arc<Counters>,
    shutdown: Arc<tokio::sync::Notify>,
) -> anyhow::Result<()> {
    let mut ws = match connect_bound(&cfg.url, cfg.src_ip).await {
        Ok(ok) => ok,
        Err(e) => {
            Counters::inc(&counters.connect_failed);
            return Err(e);
        }
    };
    Counters::inc(&counters.connected);

    // 1. connection_established → socket_id
    let socket_id = loop {
        match ws.next().await {
            Some(Ok(Message::Text(t))) => {
                if let Some(f) = parse_frame(&t) {
                    if f.event == "pusher:connection_established" {
                        let inner: Value = serde_json::from_str(f.data.as_deref().unwrap_or("{}"))?;
                        break inner["socket_id"].as_str().unwrap_or_default().to_string();
                    }
                }
            }
            Some(Ok(_)) => {}
            _ => anyhow::bail!("closed before established"),
        }
    };

    // 2. subscribe (signed if private)
    let auth = if cfg.private {
        Some(channel_auth(
            &cfg.key,
            &cfg.secret,
            &socket_id,
            &cfg.channel,
            None,
        ))
    } else {
        None
    };
    ws.send(Message::text(subscribe_frame(
        &cfg.channel,
        auth.as_deref(),
        None,
    )))
    .await?;

    // 3. receive loop
    loop {
        tokio::select! {
            _ = shutdown.notified() => break,
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(t))) => {
                    if let Some(f) = parse_frame(&t) {
                        match f.event.as_str() {
                            "pusher_internal:subscription_succeeded" => {
                                Counters::inc(&counters.subscribed);
                            }
                            "pusher:ping" => {
                                ws.send(Message::text(pong_frame())).await.ok();
                            }
                            _ => {
                                if let Some(data) = f.data.as_deref() {
                                    record_delivery(data, &cfg.own_run_id, epoch, &lat, &counters);
                                }
                            }
                        }
                    }
                }
                Some(Ok(Message::Ping(p))) => { ws.send(Message::Pong(p)).await.ok(); }
                Some(Ok(_)) => {}
                _ => break,
            }
        }
    }
    let _ = ws.close(None).await;
    Ok(())
}

/// Signed REST publisher. `base` like "http://127.0.0.1:7000".
pub struct Publisher {
    http: reqwest::Client,
    base: String,
    app_id: String,
    key: String,
    secret: String,
}

impl Publisher {
    pub fn new(base: String, app_id: String, key: String, secret: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            base,
            app_id,
            key,
            secret,
        }
    }

    /// Publish `event` to `channel` with a timestamped payload. `now_unix` = wall-clock secs.
    pub async fn publish(
        &self,
        channel: &str,
        event: &str,
        payload: &str,
        now_unix: u64,
    ) -> anyhow::Result<()> {
        let body = json!({ "name": event, "channel": channel, "data": payload }).to_string();
        let query = sign_post_events(&self.key, &self.secret, &self.app_id, &body, now_unix);
        let url = format!("{}/apps/{}/events?{}", self.base, self.app_id, query);
        let resp = self
            .http
            .post(url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await?;
        anyhow::ensure!(
            resp.status().is_success(),
            "publish status {}",
            resp.status()
        );
        Ok(())
    }
}

/// Wall-clock unix seconds (for the auth_timestamp).
pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
