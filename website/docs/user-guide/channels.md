# Channels

Pylon supports all Pusher Channels v7 channel types. The channel name determines its type: the
prefix (or absence of one) tells Pylon what authentication and behaviour to apply.

Channel names may contain letters, digits, and the characters `_`, `-`, `=`, `@`, `,`, `.`, `;`.
The maximum length is 164 bytes.

---

## Public channels

**Prefix:** _(none)_ — e.g. `chat`, `notifications`

Any connected client can subscribe to a public channel without an auth token. Use public channels
for content that is safe to broadcast to everyone — live scores, public feeds, status pages, etc.

```js
const channel = pusher.subscribe("chat");
channel.bind("new-message", (data) => console.log(data));
```

---

## Private channels

**Prefix:** `private-` — e.g. `private-user-42`, `private-orders`

Subscribing to a private channel requires a valid auth signature from your backend. The client sends
its socket ID and the channel name to your auth endpoint; your server signs them with the app secret
and returns the token. Pylon verifies the token with a constant-time comparison before allowing the
subscription.

See [Applications & Authentication](applications.md) for the signing details.

```js
const channel = pusher.subscribe("private-user-42");
```

---

## Presence channels

**Prefix:** `presence-` — e.g. `presence-room`, `presence-lobby`

Presence channels combine private-channel auth with a member roster. When a client subscribes it
provides `channel_data` (at minimum `{"user_id": "..."}`) which is included in the auth signature.
Pylon tracks all members and sends `pusher:member_added` / `pusher:member_removed` events to every
subscriber as the roster changes.

The roster is available immediately after subscription via the `members` property on the channel
object (in pusher-js). A configurable `PYLON_MAX_PRESENCE_MEMBERS` cap (default 100) limits how
many members may be in a presence channel simultaneously.

```js
const channel = pusher.subscribe("presence-lobby");
channel.bind("pusher:subscription_succeeded", (members) => {
  members.each((member) => console.log(member.id));
});
channel.bind("pusher:member_added", (member) => {
  console.log("joined:", member.id);
});
channel.bind("pusher:member_removed", (member) => {
  console.log("left:", member.id);
});
```

---

## Encrypted channels

**Prefix:** `private-encrypted-` — e.g. `private-encrypted-dm`

Encrypted channels authenticate exactly like private channels but the event payload is end-to-end
encrypted by the client before it is sent. Pylon relays the opaque ciphertext to subscribers
unchanged — it never sees or stores the plaintext.

The shared encryption secret comes from your own auth endpoint; pylon has no knowledge of it.
Because the ciphertext is opaque, **client events are not permitted** on encrypted channels. Pylon
also enforces that an encrypted channel must be targeted **alone** in a REST trigger — you cannot
publish to a mix of encrypted and non-encrypted channels in the same `POST /apps/{id}/events` call.

---

## Cache channels

Cache variants replay the most recently published event to new subscribers as soon as they join.
If no event has been published yet, Pylon fires a `cache_miss` webhook (if configured).

The cached event expires after `PYLON_CACHE_TTL_SECS` (default 1800 seconds).

| Prefix | Auth |
|---|---|
| `cache-` | None (public) |
| `private-cache-` | Auth required |
| `presence-cache-` | Auth + roster |

The `private-encrypted-cache-` prefix is also valid; it combines encryption, auth, and last-event
replay.

```js
// Subscribing to a cache channel: if a cached event exists, it arrives immediately.
const channel = pusher.subscribe("cache-dashboard");
channel.bind("stats-updated", (data) => render(data));
```

---

## Per-connection subscription cap

A single connection may hold at most 200 simultaneous channel subscriptions, configurable with
`PYLON_MAX_SUBSCRIPTIONS_PER_CONNECTION` (set `0` to disable the cap). A subscribe that would exceed
the cap is rejected with a non-fatal `pusher:subscription_error` (`LimitReached`, status `4004`) —
the connection stays open and every subscription already held remains live. Re-subscribing to a
channel the connection already holds is exempt from the cap and never errors.

This is a deliberate pylon-specific resource guard, not Pusher parity. Hosted Pusher documents no
per-connection subscription limit: the [channels doc][pusher-channels] and the
[WebSocket protocol reference][pusher-protocol] specify no cap on subscriptions per connection, and
Pusher's limits FAQ only states there is no restriction on the number of channels per app —
subscriptions are multiplexed over one connection. Pylon needs the cap because every subscription
holds server-side state (registry membership, presence rosters, per-channel bookkeeping), so an
uncapped connection could grow memory without bound. The default of 200 comfortably exceeds what a
well-behaved client uses while bounding per-connection memory.

[pusher-channels]: https://pusher.com/docs/channels/using_channels/channels/
[pusher-protocol]: https://pusher.com/docs/channels/library_auth_reference/pusher-websockets-protocol/

---

## Subscription count events

When the app has `subscription_count_enabled` set (via the REST `info` parameter), Pylon can
return live subscription counts alongside trigger responses. If enabled server-side, clients may
also receive `pusher:subscription_count` events on a channel as the subscriber count changes.

---

## Client events

Clients may publish events directly to other subscribers on private and presence channels using the
`client-` event prefix. Client events are enabled per-app with the `client_messages_enabled` field
in `apps.json` and are subject to a per-connection rate limit (`PYLON_MAX_CLIENT_EVENTS_PER_SECOND`,
default 10). Client events are not available on public or encrypted channels.

---

!!! note "Raw protocol"
    This page covers behaviour you interact with through the SDK. The low-level WebSocket frames,
    handshake messages, and error codes that implement this behaviour are documented in the
    Developer Guide.
