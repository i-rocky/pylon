use hmac::{Hmac, Mac};
use md5::{Digest, Md5};
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

/// Embed publish-nanos into an event payload object so subscribers can compute latency.
pub fn stamp_payload(seq: u64, publish_nanos: u128) -> String {
    json!({ "seq": seq, "t": publish_nanos.to_string() }).to_string()
}

/// Extract publish-nanos from a received data payload (string-encoded JSON object).
pub fn extract_nanos(data: &str) -> Option<u128> {
    let v: Value = serde_json::from_str(data).ok()?;
    v.get("t")?.as_str()?.parse().ok()
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
    fn stamp_and_extract_roundtrip() {
        let p = stamp_payload(7, 123_456_789);
        assert_eq!(extract_nanos(&p), Some(123_456_789));
    }
}

use crate::metrics::{Counters, Latency};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

pub type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

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
    let (mut ws, _) = match connect_async(&cfg.url).await {
        Ok(ok) => ok,
        Err(e) => {
            Counters::inc(&counters.connect_failed);
            return Err(e.into());
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
        Some(channel_auth(&cfg.key, &cfg.secret, &socket_id, &cfg.channel, None))
    } else {
        None
    };
    ws.send(Message::Text(subscribe_frame(&cfg.channel, auth.as_deref(), None)))
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
                                ws.send(Message::Text(pong_frame())).await.ok();
                            }
                            _ => {
                                if let Some(data) = f.data.as_deref() {
                                    if let Some(t_ns) = extract_nanos(data) {
                                        let now = epoch.elapsed().as_nanos();
                                        let d = now.saturating_sub(t_ns);
                                        lat.record_nanos(d.min(u64::MAX as u128) as u64);
                                        Counters::inc(&counters.received);
                                    }
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
        Self { http: reqwest::Client::new(), base, app_id, key, secret }
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
        let resp = self.http.post(url).header("Content-Type", "application/json").body(body).send().await?;
        anyhow::ensure!(resp.status().is_success(), "publish status {}", resp.status());
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
