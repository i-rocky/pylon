# Pusher Protocol Reference

Pylon implements **Pusher Channels protocol v7** over WebSocket (RFC 6455).
This page documents the wire details that pylon enforces. In practice, app
developers interact with the protocol through an official Pusher SDK — this
page is reference material for contributors and integration authors.

Sources: [`src/protocol/`](https://github.com/i-rocky/pylon/blob/master/src/protocol/),
[`src/auth/`](https://github.com/i-rocky/pylon/blob/master/src/auth/)

---

## Connection Establishment

Clients connect to `ws[s]://host:port/app/{app_key}?protocol=7`.

On successful upgrade, pylon immediately sends:

```json
{
  "event": "pusher:connection_established",
  "data": "{\"socket_id\":\"<sid>\",\"activity_timeout\":120}"
}
```

Note that `data` is a **JSON-encoded string** (double-encoded), not a nested
object — this is the standard Pusher convention for all frames except
`pusher:error` (see below). `activity_timeout` is the server-configured idle
ping interval in seconds (default 120; configurable via `PYLON_ACTIVITY_TIMEOUT`).

If the app key is not found, pylon sends a `pusher:error` frame with code
`4001` followed by a WebSocket Close frame with the same code, then tears down
the connection.

---

## The `data` Double-Encoding Convention

For all frames **except `pusher:error`**, the `data` field is a JSON-encoded
string — the inner object is serialised to a string and that string is used as
the `data` value:

```json
{ "event": "pusher_internal:subscription_succeeded", "channel": "presence-x",
  "data": "{\"presence\":{\"ids\":[\"7\"],\"hash\":{\"7\":{}},\"count\":1}}" }
```

**Exception — `pusher:error`.** The `data` field is a **plain JSON object**,
not a double-encoded string:

```json
{ "event": "pusher:error", "data": { "code": 4001, "message": "Could not find app by key" } }
```

Source:
[`src/protocol/v7/frames.rs`](https://github.com/i-rocky/pylon/blob/master/src/protocol/v7/frames.rs)

---

## Ping / Pong

The Pusher protocol uses an application-level ping/pong, distinct from the
WebSocket protocol ping/pong opcodes.

**Server → client ping** (sent after `activity_timeout` seconds of inactivity):

```json
{ "event": "pusher:ping", "data": {} }
```

**Client → server pong** (the client must reply before `pong_timeout` elapses):

```json
{ "event": "pusher:pong", "data": {} }
```

Any inbound frame (not just `pusher:pong`) resets the inactivity timer. If no
pong is received within `pong_timeout` seconds (default 30; configurable via
`PYLON_PONG_TIMEOUT`), pylon closes the connection with code `4201`.

---

## Channel Subscription

**Client sends:**

```json
{
  "event": "pusher:subscribe",
  "data": {
    "channel": "my-channel",
    "auth": "<app_key>:<hmac>",
    "channel_data": "{\"user_id\":\"42\",\"user_info\":{}}"
  }
}
```

`auth` and `channel_data` are required for private and presence channels; they
are omitted for public channels. `channel_data` is only used for presence
channels.

**Server replies on success:**

```json
{
  "event": "pusher_internal:subscription_succeeded",
  "channel": "my-channel",
  "data": ""
}
```

For presence channels `data` is the double-encoded roster:

```json
{
  "event": "pusher_internal:subscription_succeeded",
  "channel": "presence-room",
  "data": "{\"presence\":{\"ids\":[\"42\"],\"hash\":{\"42\":{\"name\":\"Alice\"}},\"count\":1}}"
}
```

---

## Authentication Signatures

Pylon uses HMAC-SHA256. All signatures are lowercase hex strings. The auth
token has the form `{app_key}:{hex_signature}`.

### Private channel

The signed string is `"{socket_id}:{channel}"`:

```
HMAC-SHA256(app_secret, "123.456:private-chat")
```

### Presence channel

The signed string appends the channel_data JSON:

```
HMAC-SHA256(app_secret, "123.456:presence-room:{\"user_id\":\"42\",\"user_info\":{}}")
```

`channel_data` is the exact JSON string the client sends — do not re-serialise
or canonicalise it.

### User authentication (`pusher:signin`)

The signed string uses a `::user::` separator:

```
HMAC-SHA256(app_secret, "123.456::user::{\"id\":\"42\",\"name\":\"Alice\"}")
```

Source:
[`src/auth/signature.rs`](https://github.com/i-rocky/pylon/blob/master/src/auth/signature.rs),
[`src/auth/channel.rs`](https://github.com/i-rocky/pylon/blob/master/src/auth/channel.rs),
[`src/auth/user.rs`](https://github.com/i-rocky/pylon/blob/master/src/auth/user.rs)

---

## Close Codes

WebSocket close codes in the 4xxx range are Pusher-defined. Pylon sends a
`pusher:error` text frame immediately before the WebSocket Close frame so the
client receives the code as a structured event regardless of whether it can
inspect the Close payload.

### Bands

| Range | Pusher client behaviour |
|---|---|
| 4000–4099 | Do **not** reconnect |
| 4100–4199 | Reconnect with back-off |
| 4200–4299 | Reconnect immediately |

Codes 4300–4399 are non-fatal in-band errors delivered as `pusher:error`
events on an otherwise open connection (the socket is **not** closed).

### Pylon's specific codes

| Code | Cause |
|---|---|
| `4001` | App key not found |
| `4004` | App connection limit reached |
| `4006` | Invalid protocol version string format |
| `4007` | Unsupported protocol version |
| `4008` | No protocol version supplied (strict mode) |
| `4009` | Channel name invalid or connection not authorised |
| `4200` | Server shutting down — reconnect immediately |
| `4201` | Pong timeout (connection went silent) |
| `4301` | Client event rate-limited (non-fatal, connection stays open) |
| `4302` | Watchlist too large (non-fatal, connection stays open) |

For operator guidance on these codes see
[Troubleshooting & FAQ](../user-guide/troubleshooting.md#close-codes).

Sources:
[`src/protocol/error.rs`](https://github.com/i-rocky/pylon/blob/master/src/protocol/error.rs),
[`src/transport/worker.rs`](https://github.com/i-rocky/pylon/blob/master/src/transport/worker.rs)
