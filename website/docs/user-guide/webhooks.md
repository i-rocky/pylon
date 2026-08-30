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
        "cache_miss"
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

Pylon fires six event types:

| Event type | Fired when |
|---|---|
| `channel_occupied` | The first subscriber joins a channel (channel transitions from empty to occupied). |
| `channel_vacated` | The last subscriber leaves a channel (channel becomes empty). A configurable grace period (`PYLON_WEBHOOK_VACATED_GRACE_MS`, default 3 000 ms) delays this event to absorb brief reconnects. |
| `member_added` | A client joins a presence channel (`presence-*`). |
| `member_removed` | A client leaves a presence channel (`presence-*`). |
| `client_event` | A client publishes a `client-` prefixed event (only fired when `client_messages_enabled` is `true` for the app). |
| `cache_miss` | A new subscriber joins a cache channel (`cache-*`, `private-cache-*`, `presence-cache-*`) and no cached event exists for that channel. |

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
| `PYLON_WEBHOOK_VACATED_GRACE_MS` | `3000` | Grace period before firing `channel_vacated` |

### Retries

Respond to the POST with any `2XX` status to acknowledge a webhook — anything else is a failure.
Following Channels' documented behavior ("If a non 2XX status code is returned, Channels will
retry sending the webhook, with exponential backoff, for 5 minutes"), pylon retries **every
non-2xx response and every transport error** (timeout, connection refused, DNS failure, …):

- The delay before the first retry is `PYLON_WEBHOOK_BACKOFF_BASE_MS` (default 1 s).
- Each subsequent delay doubles, up to `PYLON_WEBHOOK_BACKOFF_CAP_MS` (default 60 s).
- Retrying stops once `PYLON_WEBHOOK_RETRY_BUDGET_MS` (default 300 000 ms = 5 minutes) of total
  elapsed time — attempts included — has passed since the first attempt; the delivery is then
  counted as failed. `0` disables retries (single attempt).

With the defaults and an unresponsive endpoint, attempts are made at roughly 0, 1, 3, 7, 15, 31,
63, 123, 183, 243, and 303 seconds (11 attempts).

### Deprecated variables

| Variable | Status |
|---|---|
| `PYLON_WEBHOOK_RETRY_BASE_MS` | Deprecated alias of `PYLON_WEBHOOK_BACKOFF_BASE_MS`; honored (with a startup warning) for one release. If both are set, the new variable wins. |
| `PYLON_WEBHOOK_MAX_RETRIES` | Deprecated and ignored (warns at startup). Retries are bounded by `PYLON_WEBHOOK_RETRY_BUDGET_MS` (total time, not attempt count). |

Deliveries that never receive a 2xx within the retry budget are counted as failures in the
Prometheus metrics exposed at `/metrics`.
