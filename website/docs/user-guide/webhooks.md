# Webhooks

Pylon fires signed HTTP POST requests (webhooks) to your server when specific channel events
occur. Webhooks are configured per-app in `apps.json`. Like Pusher Channels, pylon retries any
delivery that does not get a 2xx response, with exponential backoff, for up to five minutes.

---

## Configuration

Each app in `apps.json` has a `webhooks` array. Each entry specifies a target URL, the event
types to deliver, and optional custom headers:

```json
{
  "webhooks": [
    {
      "url": "https://your-server.example.com/pusher/webhooks",
      "event_types": [
        "channel_occupied",
        "channel_vacated",
        "member_added",
        "member_removed",
        "client_event",
        "cache_miss",
        "subscription_count"
      ],
      "headers": {
        "X-Custom-Token": "secret-value"
      }
    }
  ]
}
```

| Field | Description |
|---|---|
| `url` | HTTPS (or HTTP) endpoint that will receive the POST. |
| `event_types` | List of event type names to deliver to this endpoint. Omit types you don't need. |
| `headers` | Optional map of extra HTTP headers to include. Cannot override `Content-Type`, `X-Pusher-Key`, or `X-Pusher-Signature`. |

You may define multiple webhook entries per app — each with its own URL and event-type filter.

---

## Event types

Pylon fires seven event types:

| Event type | Fired when |
|---|---|
| `channel_occupied` | The first subscriber joins a channel (channel transitions from empty to occupied). |
| `channel_vacated` | The last subscriber leaves a channel (channel becomes empty). A configurable grace period (`PYLON_WEBHOOK_VACATED_GRACE_MS`, default 3 000 ms) delays this event — on every deployment mode — to absorb brief reconnects; if the channel is re-occupied within the window the webhook is suppressed — see the note below. |
| `member_added` | A client joins a presence channel (`presence-*`). Fires immediately, even on a rejoin inside the grace window (the doc's suppression is removal-side only). |
| `member_removed` | A client leaves a presence channel (`presence-*`). Delayed by the same grace period (`PYLON_WEBHOOK_VACATED_GRACE_MS`); if the user re-joins the channel within the window the webhook is suppressed — see the note below. |
| `client_event` | A client publishes a `client-` prefixed event (only fired when `client_messages_enabled` is `true` for the app). |
| `cache_miss` | A new subscriber joins a cache channel (`cache-*`, `private-cache-*`, `presence-cache-*`) and no cached event exists for that channel. |
| `subscription_count` | A client subscribes to or unsubscribes from a non-presence channel and the app has `subscription_count_enabled: true`. The payload carries the channel's new subscriber count. Fires for every count change (no count is ever `0` — the last subscriber leaving is signalled by `channel_vacated`). |

---

## Request format

Pylon batches events that fire within the same `PYLON_WEBHOOK_BATCH_MS` window (default 50 ms) and
sends them in a single POST. The body is a JSON object:

```json
{
  "time_ms": 1700000000000,
  "events": [
    {
      "name": "channel_occupied",
      "channel": "my-channel"
    }
  ]
}
```

The `events` array contains one or more event objects. Their shapes by type:

**`channel_occupied` / `channel_vacated` / `cache_miss`:**
```json
{ "name": "channel_occupied", "channel": "my-channel" }
```

**`member_added` / `member_removed`:**
```json
{ "name": "member_added", "channel": "presence-room", "user_id": "user-42" }
```

**`subscription_count`:**
```json
{ "name": "subscription_count", "channel": "my-channel", "subscription_count": 2 }
```

Requires `subscription_count_enabled: true` on the app (the same toggle that enables the
`pusher_internal:subscription_count` WebSocket event) in addition to the `event_types`
entry. It fires on all channel types except presence channels. Hosted Channels throttles
this webhook to once every 5 seconds on channels with more than 100 connected clients;
pylon delivers every count change through its normal batch window and does not throttle.

**`client_event`:**
```json
{
  "name": "client_event",
  "channel": "private-chat",
  "event": "client-typing",
  "data": { "user": "alice" },
  "socket_id": "123.456",
  "user_id": "user-42"
}
```

`user_id` is only present on `client_event` when the sender is a member of a presence channel;
it is omitted otherwise.

---

## Verification

Every webhook POST carries two signature headers:

| Header | Value |
|---|---|
| `X-Pusher-Key` | Your app's public key |
| `X-Pusher-Signature` | `HMAC-SHA256(app_secret, raw_body)` as a lowercase hex string |

To verify a webhook:

1. Read the raw request body bytes (do not parse JSON first).
2. Compute `HMAC-SHA256(your_app_secret, raw_body)`.
3. Compare the result (constant-time) to the value of `X-Pusher-Signature`.
4. Reject requests that fail verification.

Example in Node.js:

```js
const crypto = require("crypto");

function verifyWebhook(rawBody, signature, appSecret) {
  const expected = crypto
    .createHmac("sha256", appSecret)
    .update(rawBody)
    .digest("hex");
  return crypto.timingSafeEqual(
    Buffer.from(expected, "utf8"),
    Buffer.from(signature, "utf8")
  );
}

// In your Express handler:
app.post("/pusher/webhooks", (req, res) => {
  const sig = req.headers["x-pusher-signature"];
  if (!verifyWebhook(req.rawBody, sig, process.env.PUSHER_APP_SECRET)) {
    return res.status(403).send("Forbidden");
  }
  const payload = JSON.parse(req.rawBody);
  for (const event of payload.events) {
    console.log(event.name, event.channel);
  }
  res.sendStatus(200);
});
```

!!! warning "Use the raw body"
    Parse the body only after verifying the signature. Many frameworks re-serialize JSON
    with different whitespace, which will break the HMAC check.

---

## Batching and delivery knobs

Pylon batches events that arrive within `PYLON_WEBHOOK_BATCH_MS` (default 50 ms) into a single
POST to reduce request overhead — batching groups events, it never drops them: a channel that is
created and vacated within one window still produces both the `channel_occupied` and the
`channel_vacated` event (matching hosted Channels, where `channel_vacated` / `member_removed`
are only delayed — "up to three seconds" — and suppressed only if the client reconnects within
that delay). See [Configuration](configuration.md) for the full set of tuning variables:

| Variable | Default | Purpose |
|---|---|---|
| `PYLON_WEBHOOK_BATCH_MS` | `50` | Batching window in milliseconds |
| `PYLON_WEBHOOK_MAX_CONCURRENCY` | `100` | Maximum simultaneous in-flight deliveries |
| `PYLON_WEBHOOK_BACKOFF_BASE_MS` | `1000` | First retry delay; doubles each attempt |
| `PYLON_WEBHOOK_BACKOFF_CAP_MS` | `60000` | Upper bound for each retry delay |
| `PYLON_WEBHOOK_RETRY_BUDGET_MS` | `300000` | Total time a delivery may keep retrying |
| `PYLON_WEBHOOK_TIMEOUT_MS` | `5000` | Per-attempt HTTP request timeout |
| `PYLON_WEBHOOK_VACATED_GRACE_MS` | `3000` | Reconnect grace period before firing `channel_vacated` / `member_removed` (see below) |

### `channel_vacated` / `member_removed` timing: the reconnect grace

Hosted Channels delays `channel_vacated` and `member_removed` by "up to three
seconds" and — "if the client reconnects within this delay" — sends no webhook
for them. Pylon implements this as a grace window plus a re-check at fire
time: a `channel_vacated` whose channel is occupied again, or a
`member_removed` whose user is present in the channel again, is suppressed;
only the opposite side of the flap (`channel_occupied` / `member_added`) ever
fires. `member_added` itself is never delayed or suppressed.

The grace applies on **every deployment mode** — the hosted doc draws no
single-node/cluster distinction. On the Redis path the re-check reads the
cluster-wide state (it exists to absorb the cluster's eventual-consistency
window: a reconnecting client's re-subscribe may race the webhook); on a
single-node deployment (`PYLON_ADAPTER=local`) the node's own registry is the
oracle, so the re-check is exact. Setting
`PYLON_WEBHOOK_VACATED_GRACE_MS=0` restores immediate delivery on both modes.

### Retries

Respond to the POST with any `2XX` status to acknowledge a webhook — anything else is a failure.
Following Channels' documented behavior ("If a non 2XX status code is returned, Channels will
retry sending the webhook, with exponential backoff, for 5 minutes"), pylon retries **every
non-2xx response and every transport error** (timeout, connection refused, DNS failure, …):

- The delay before the first retry is `PYLON_WEBHOOK_BACKOFF_BASE_MS` (default 1 s), also
  clamped to `PYLON_WEBHOOK_BACKOFF_CAP_MS`.
- Each subsequent delay doubles, up to `PYLON_WEBHOOK_BACKOFF_CAP_MS` (default 60 s).
- Retrying stops once `PYLON_WEBHOOK_RETRY_BUDGET_MS` (default 300 000 ms = 5 minutes) of total
  elapsed time — attempts included — has passed since the first attempt; the delivery is then
  counted as failed. `0` disables retries (single attempt). The final backoff is shortened to
  the budget's remaining time, so give-up happens at (not after) the budget.

With the defaults and an unresponsive endpoint, attempts are made at roughly 0, 1, 3, 7, 15, 31,
63, 123, 183, 243, and 300 seconds (11 attempts, giving up at the 5-minute mark).

### Deprecated variables

| Variable | Status |
|---|---|
| `PYLON_WEBHOOK_RETRY_BASE_MS` | Deprecated alias of `PYLON_WEBHOOK_BACKOFF_BASE_MS`; honored (with a startup warning) for one release. If both are set, the new variable wins. |
| `PYLON_WEBHOOK_MAX_RETRIES` | Deprecated and ignored (warns at startup). Retries are bounded by `PYLON_WEBHOOK_RETRY_BUDGET_MS` (total time, not attempt count). |

Deliveries that never receive a 2xx within the retry budget are counted as failures in the
Prometheus metrics exposed at `/metrics`.

---

## Target restrictions (SSRF guard)

Webhook URLs are configured per-app in `apps.json`, but pylon itself sends the
POSTs — so a hostile app config could aim them at internal addresses (cloud
metadata at `169.254.169.254`, loopback services, RFC1918 hosts). Before any
HTTP is sent, every delivery runs a pre-flight check:

1. **Scheme**: the URL must be `http://` or `https://`. Anything else
   (`file://`, `ftp://`, …) is refused.
2. **Address classification**: for a literal-IP URL the address itself is
   classified; for a hostname, DNS is resolved first and the delivery is
   refused if **any** resolved address is private. Refused classes:
   - IPv4 loopback (`127.0.0.0/8`), unspecified (`0.0.0.0`),
     link-local (`169.254.0.0/16` — includes the cloud metadata addresses),
     and RFC1918 (`10/8`, `172.16/12`, `192.168/16`).
   - IPv4 shared/CGNAT (`100.64.0.0/10`, RFC 6598) — Tailscale and several
     Kubernetes CNIs run real internal infrastructure there.
   - IPv4 multicast (`224.0.0.0/4`) and broadcast (`255.255.255.255`).
   - IPv6 loopback (`::1`), unspecified (`::`), unique-local (`fc00::/7`),
     link-local (`fe80::/10`), and multicast (`ff00::/8`).
   - IPv4-mapped (`::ffff:10.0.0.5`) and IPv4-compatible (`::10.0.0.5`) IPv6
     forms are classified by their embedded IPv4 address.
3. **Pinning**: for hostname URLs the delivery is pinned to the addresses the
   pre-flight resolved (`ClientBuilder::resolve_to_addrs`), so the actual
   connect cannot drift to a second, un-checked DNS lookup between the check
   and the request.

A refused delivery is a **configuration error**: it fails fast with a
`webhook target refused by SSRF guard` warning log, is counted once in
`pylon_webhook_delivered_total{status="failed"}`, sends no HTTP at all, and
does **not** consume the retry budget (retrying a refused target for five
minutes cannot succeed). A hostname that does not resolve at all is treated
the same way.

### Redirects are never followed

The webhook client does not follow 3xx responses. A `302` is treated as an
ordinary non-2xx outcome and retried per the schedule above. This closes a
bypass where an attacker-controlled endpoint answers `302 →
http://169.254.169.254/…` — following the redirect would sidestep the
pre-flight check and pinning entirely.

### Allowing internal targets

If your webhook receivers genuinely live on internal networks, set:

| Variable | Default | Purpose |
|---|---|---|
| `PYLON_WEBHOOK_ALLOW_PRIVATE_TARGETS` | `false` | Set `1` (or `true`) to permit delivery to private/loopback/link-local addresses. The scheme check and the pinning still apply; only the address classification is relaxed. |
