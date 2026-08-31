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
| `4001` | App key not found (well-formed `/app/{key}` path, unknown key) |
| `4004` | App connection limit reached (per-app `capacity`) |
| `4005` | Connection path malformed — not the `/app/{key}` shape, or an empty key (distinct from `4001`) |
| `4006` | Invalid protocol version string format |
| `4007` | Unsupported protocol version |
| `4008` | No protocol version supplied (strict mode) |
| `4009` | Connection not authorised — **fatal close**: `pusher:signin` verification failure, user termination (`terminate_connections`), or the app being removed/disabled mid-connection (admin purge or sweep) |
| `4100` | Server is over capacity — the node's connection ceiling (`PYLON_MAX_CONNECTIONS`) is reached, or the broadcast pipeline is saturated at connection admission |
| `4103` | Application store temporarily unavailable (transient backend error) |
| `4200` | Server shutting down — reconnect immediately |
| `4201` | Pong timeout (connection went silent) |
| `4202` | Maximum connection lifetime reached (`PYLON_MAX_CONN_LIFETIME_SECS`, default 24 h; absolute from establishment, not reset by activity) |
| `4301` | Client event rejected (non-fatal, connection stays open) — see below |
| `4302` | Watchlist too large (non-fatal, connection stays open) |

### Non-fatal in-band errors

**`pusher:subscription_error`** is a channel-scoped, non-fatal frame: the socket
stays open and only that subscription failed. Its `data` object carries a
`status` field — `4009` for an invalid channel name, `401` for an
authentication failure (bad/missing auth on private/presence/encrypted
channels, or a reserved `#` channel the connection may not join). These
statuses share numeric values with the close-code namespace but never close
the connection.

**`4301`** covers all four client-event rejection classes: client messaging
disabled for the app, event name too long, payload too large, and the
per-connection rate limit (10 events/sec). The rate-limit message is hosted
Pusher's error-table text verbatim: `Client event rejected due to rate limit`.

For operator guidance on these codes see
[Troubleshooting & FAQ](../user-guide/troubleshooting.md#close-codes).

Sources:
[`src/protocol/error.rs`](https://github.com/i-rocky/pylon/blob/master/src/protocol/error.rs),
[`src/transport/worker.rs`](https://github.com/i-rocky/pylon/blob/master/src/transport/worker.rs)

---

## Supporting a new protocol version (contributor checklist)

Pylon's version seam is designed so that a v8 (or vN) is a contained change.
Everything below describes what EXISTS today — the v7 files are the template,
and the fixture v8 that 7.3 used to prove the plumbing shows exactly which
pieces light up. Work through the checklist in order; each step names the real
files involved.

### 1. Create `src/protocol/vN/` implementing `Codec` (with an honest `Capabilities`)

Copy the shape of
[`src/protocol/v7/mod.rs`](https://github.com/i-rocky/pylon/blob/master/src/protocol/v7/mod.rs)
(the `V7Codec` — a unit struct implementing
[`Codec`](https://github.com/i-rocky/pylon/blob/master/src/protocol/codec.rs))
and
[`src/protocol/v7/frames.rs`](https://github.com/i-rocky/pylon/blob/master/src/protocol/v7/frames.rs)
(the encode/decode bodies). Two hard rules from the seam's design:

* **`frames` stays `pub(super)`.** `src/protocol/v7/mod.rs` declares
  `pub(super) mod frames;` so the frame functions are reachable ONLY from
  inside the `protocol` module family. A direct `v7::frames::…` (or
  `vN::frames::…`) call anywhere else in the crate, the benches, or the
  integration tests is a compile error — no out-of-protocol encode caller can
  silently appear. All encoding flows through
  [`src/protocol/wire.rs`](https://github.com/i-rocky/pylon/blob/master/src/protocol/wire.rs)
  (`wire::encode_into` / `wire::encode`) or a `Codec` trait object.
* **Report honest capabilities.** `Codec::capabilities()` returns the six
  flag structs (`client_events`, `presence`, `encrypted_channels`,
  `cache_channels`, `user_auth`, `watchlist` — see `Capabilities` in
  `src/protocol/codec.rs`). The dispatch layer consults them at the single
  snapshot point: `finish_establish` (in
  [`src/transport/worker.rs`](https://github.com/i-rocky/pylon/blob/master/src/transport/worker.rs))
  copies `codec.capabilities()` into
  `ConnectionContext::capabilities` (in
  [`src/ws/handler.rs`](https://github.com/i-rocky/pylon/blob/master/src/ws/handler.rs)),
  and every feature gate in the handler family reads that snapshot. Turn OFF
  whatever vN does not support — a feature the version lacks degrades
  gracefully, reusing the error frames the analogous v7 path already emits.
  `Capabilities::v7()` (all on) exists so v7's behavior stays byte-identical
  to the pre-capability code.

### 2. Extend `MIN_PROTOCOL` / `MAX_PROTOCOL`, `codec_for`, and the `wire` arm

* Bump `MAX_PROTOCOL` in
  [`src/protocol/mod.rs`](https://github.com/i-rocky/pylon/blob/master/src/protocol/mod.rs).
  `negotiate`'s range check (`supported_or_4007`) and `codec_for` (the single
  extension point — today `fn codec_for(_version: u8) -> Box<dyn Codec>`)
  pick the version up from there; add the vN arm to `codec_for`.
* `ACTIVE_VERSIONS` (in `src/protocol/wire.rs`) is a const array materialized
  from `MIN_PROTOCOL..=MAX_PROTOCOL` — bumping the range extends it
  automatically, so the sink's fan-out frames cover the new version with no
  further edits.
* Add the real `vN` arm to `wire::encode_into` in
  `src/protocol/wire.rs`. While developing you can lean on the same pattern
  7.3 used: a `#[cfg(any(test, feature = "test-hooks"))]` second arm
  (today's fixture `8 => test_v8_encode_into(...)`) whose bytes are
  deterministic and byte-distinct from v7 for every event, so wrong-slot
  deliveries cannot pass by aliasing. `negotiate` never hands the fixture
  version out, so no production path reaches it. Replace the fixture with the
  real arm when the version goes live.

### 3. Negotiation tests: the 4006 / 4007 / 4008 matrix

Cover every branch of `negotiate` (`src/protocol/mod.rs` unit tests) and the
end-to-end handshake (integration tests in
[`tests/integration.rs`](https://github.com/i-rocky/pylon/blob/master/tests/integration.rs)):
`?protocol=` unparseable → **4006**; parseable but outside `MIN..=MAX` →
**4007**; neither `?protocol=` nor `?version=` in strict mode → **4008**; the
`?version=` major-inference fallback (`"7.4.1"` → 7) and its 4006/4007 edges.
Extend the matrix with vN above and below the new bounds (vN−1 unsupported →
4007, vN supported, a future vN+1 unsupported).

### 4. Per-version sink frames are automatic — but prove the plumbing

The per-core broadcast sink already builds one finished WebSocket frame per
`ACTIVE_VERSIONS` entry (`fanout::frames_for` in
[`src/transport/fanout.rs`](https://github.com/i-rocky/pylon/blob/master/src/transport/fanout.rs),
called with `wire::ACTIVE_VERSIONS` from the broadcast path in
[`src/adapter/local.rs`](https://github.com/i-rocky/pylon/blob/master/src/adapter/local.rs)),
and each worker's drain delivers every subscriber the frame for ITS
negotiated version: `sid_to_token` (in `src/transport/worker.rs`) carries
`(slab token, negotiated version)` — stamped at reconcile from the session's
negotiated codec — and the drain picks the matching slot (single-version
fast path when `frames.len() == 1`). `ServerEvent::Raw` stays version-agnostic
and shares ONE buffer across slots (the no-copy property; pinned by
pointer identity in the fanout tests). With a real vN this all lights up
without sink changes; the existing two-version fixture test
(`drain_delivers_each_subscriber_its_negotiated_versions_frame` in
`src/transport/worker.rs` tests) is the pattern to keep green — and to
promote from the fixture version to the real one.

### 5. Capability degradation tests (the stub-codec pattern)

Alongside the new codec, port the `capability_gates` test module pattern from
[`src/ws/handler_tests.rs`](https://github.com/i-rocky/pylon/blob/master/src/ws/handler_tests.rs):
a `NoFeatureCodec` test double that is v7 on the WIRE but reports the
all-false `Capabilities` profile — proving the dispatch gates key off
capabilities, not off the wire format or version number. For each feature
vN lacks, assert the graceful refusal AND that the connection stays usable.
Note the cache-channel semantics specifically: a cache-incapable version's
subscribe is REFUSED (`AuthError`/401 subscription_error), not silently
skipped — the gate sits before the adapter call so no replay ever fires
(this matters in cluster mode, where the bridge replays inside
`adapter.subscribe`).

### 6. Parity-test against hosted — and prefer live captures over docs

For every wire shape vN changes or adds, capture what hosted Pusher actually
sends and pin it. The lesson from P12: the non-presence
`pusher_internal:subscription_succeeded` `data` is the STRING `"{}"` — the
protocol docs were ambiguous about this, and a live capture from
`ws-eu.pusher.com` (protocol=7) resolved it; the capture note lives in
`src/protocol/v7/frames.rs` next to the encoding it settled. When the docs and
the wire disagree, the wire wins — record the capture date and endpoint in a
comment beside the assertion.

### 7. Update the close-code and feature tables

If vN adds or changes close codes, update the table above and
[`src/protocol/error.rs`](https://github.com/i-rocky/pylon/blob/master/src/protocol/error.rs).
If vN's `Capabilities` differ from v7's, document which features are off for
that version (the `Capabilities` struct in `src/protocol/codec.rs` is the
source of truth).

### Known gaps — what is NOT version-aware yet (v8 follow-ups)

Be honest about these when planning a v8; each is single-version **by
design**, with the seam documented where it bites:

* **The cluster relay envelope.** The Redis `Envelope`
  ([`src/adapter/redis/envelope.rs`](https://github.com/i-rocky/pylon/blob/master/src/adapter/redis/envelope.rs))
  carries ONE pre-encoded frame in the additive `frame_b64` field, encoded at
  `ACTIVE_VERSIONS[0]`. A mixed-version cluster cannot relay per-subscriber
  versions until a versioned envelope exists.
* **The legacy mailbox path.** Direct sends and the non-percore broadcast
  path (`src/adapter/local.rs`, e.g. user fan-out) re-encode at
  `ACTIVE_VERSIONS[0]` by design; per-version fan-out lives only in the
  percore sink.
* **Drain fallback wants debug_asserts.** On an unknown version byte the
  percore drain falls FORWARD to `frames[0].1` (`.unwrap_or(&frames[0].1)` in
  `src/transport/worker.rs`) rather than dropping the broadcast, and the
  empty-`frames` edge is likewise unchecked at runtime — both deserve
  `debug_assert!`s when a second real version goes active.
