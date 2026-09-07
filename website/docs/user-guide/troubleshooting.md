# Troubleshooting & FAQ

---

## Close Codes {#close-codes}

When pylon closes a WebSocket connection it sends a Pusher error frame with a
numeric code before the WebSocket close. The table below lists all codes,
their meanings, and the recommended client action.

| Code | Meaning | Client action |
|---|---|---|
| `4001` | App key not found | Do not reconnect; check `key` in your Pusher client config |
| `4003` | Application disabled (the key resolves to an app with `enabled: false` — the WS-plane close code; REST answers the same state with 403) | Do not reconnect; the app must be re-enabled server-side |
| `4004` | App connection limit reached (per-app `capacity`) | Do not reconnect; contact the server operator |
| `4005` | Connection path malformed (not `/app/{key}`, or empty key) | Do not reconnect; fix the WebSocket URL |
| `4006` | Invalid protocol version string format | Do not reconnect; fix client configuration |
| `4007` | Unsupported protocol version | Do not reconnect; upgrade client library |
| `4008` | No protocol version supplied | Do not reconnect; upgrade client library |
| `4009` | Connection not authorised — sign-in verification failed, the user's connections were terminated, or the app was removed/disabled mid-connection | Do not reconnect; fix authentication or check the app's status |
| `4100` | Server is over capacity (node connection ceiling reached, or the node is [saturated](#overload)) | Reconnect with exponential back-off |
| `4103` | Application store temporarily unavailable | Reconnect with exponential back-off |
| `4200` | Server restarting | Reconnect immediately; pylon is doing a graceful restart |
| `4201` | Activity / pong timeout | Reconnect; connection went silent too long |
| `4202` | Maximum connection lifetime reached (default 24 h) | Reconnect immediately; this is a normal scheduled recycle |
| `4301` | Client event rejected — messaging disabled, name/payload too large, or rate limit (in-band error, connection stays open) | Fix the event or slow down client event sends |
| `4302` | Watchlist too large (in-band error, connection stays open) | Reduce the number of channels in the watchlist |

Codes `4001`–`4009` are terminal and should not trigger automatic reconnection.
Codes `4100` and `4103` are transient — reconnect with exponential back-off.
Code `4201` warrants an exponential back-off before reconnecting.
Codes `4200` and `4202` warrant an immediate reconnect (the new process will be
ready / the lifetime recycle is routine).
Codes `4301` and `4302` are delivered as `pusher:error` events on an otherwise
open connection — they do not close the socket. A `pusher:subscription_error`
frame is likewise non-fatal: `data.status` `4009` means the channel name was
invalid, `401` means the subscription auth failed, and `4004`
(`LimitReached`) means either the per-connection subscription cap was hit or
the node is [over capacity](#overload) — the connection stays open in all
cases.

---

## Overload: 503s and dropped client events {#overload}

Pylon has a node-wide **saturation signal** that every admission-control path
reads. Each worker raises its own budget-pressure bit when its queued outbound
bytes reach **100 %** of its share of the memory budget, and releases it only
once it has fallen back below **80 %** (the gap is deliberate, so the signal
cannot flap at the boundary). The signal is also raised by a publisher that
finds a worker's broadcast hand-off channel full, and cleared by that worker
once it drains.

!!! warning "This engages where it previously did not"
    In earlier builds the node-wide flag was cleared unconditionally on every
    worker loop, so none of the responses below could ever fire. They now do.
    An operator upgrading onto a node that was already running hot will start
    seeing 503s and dropped `client-*` events under sustained load — that is
    the shedding working, not a new fault.

While the node is saturated:

| Path | What happens | What the caller sees |
|---|---|---|
| `POST /apps/{id}/events`, `POST /apps/{id}/batch_events` | Publish rejected before any broadcast | **`503`** with `Retry-After: 1` and body `{"error":"Server overloaded","status":503}` |
| A **new** `pusher:subscribe` | Subscription refused | `pusher:subscription_error` — `LimitReached`, status `4004`, "Server is over capacity; try again shortly". Non-fatal; the connection and its existing subscriptions stay live |
| A `client-*` event | Dropped at ingress, not broadcast | **Nothing** — the drop is silent by design (it is a server-side shed, not a client-side limit, so it sends no in-band `4301`) |
| A new connection | Refused at accept | Close code **`4100`** ("Server is over capacity") |

Re-subscribing to a channel a connection already holds is **not** refused —
the gate runs after the idempotency check, so only genuinely new subscriptions
are shed.

### Diagnosing it

`pylon_saturation_flag` reads `1` while the node is shedding. Correlate it with
`pylon_inflight_bytes` (per worker) against `pylon_worker_budget_bytes`, and
with `pylon_broadcast_dropped_total`. See [Observability](observability.md).

```bash
curl -s http://localhost:7000/metrics | grep -E 'saturation_flag|inflight_bytes|worker_budget'
```

### What to do about it

- **Confirm the budget is real.** A resolved memory budget of `0` disables all
  of the above. Pylon logs a startup `warn` naming the disabled controls when
  that happens. See
  [Production Tuning — Memory Budget](production-tuning.md#memory-budget).
- **Add capacity** — more nodes behind the load balancer, or a larger memory
  envelope, is the actual fix for sustained saturation.
- **Check for slow consumers.** A backed-up outbound queue is what drives a
  worker over budget; rising `pylon_drophead_dropped_total` and
  `pylon_codel_dropped_total` point at consumers that cannot keep up.
- **Do not simply raise the budget** past what the host really has: the point
  of the ceiling is that the node degrades predictably instead of being killed
  by the OOM killer.

---

## Other rejections you may not have seen before

These are all validation failures that older builds accepted:

| Symptom | Cause |
|---|---|
| `400 "Invalid socket id"` from a REST trigger | `socket_id` must be two runs of ASCII digits joined by one dot (`\d+\.\d+`) and at most 24 bytes. Previously any string was accepted and silently excluded nothing. For `batch_events` every item is validated before any delivery, so one bad `socket_id` rejects the whole batch |
| `401 "Invalid query: two parameters differ only by case"` | Two REST query keys that differ only by case (e.g. `info` and `Info`). The signing string lowercases keys, so such a request had no stable signature. No official SDK builds one |
| `431 Request Header Fields Too Large` | The request head carried more than 128 header fields. Answered as JSON in the usual Pusher error shape, on both the REST plane and a WebSocket upgrade. The head's total *size* is bounded separately by `PYLON_MAX_HEAD_BYTES` |
| The server refuses to start, naming an app | An app `id`, `key`, or `secret` is empty/whitespace-only; an app `key` contains `:`; or two apps share an `id` or `key`. See [Applications](applications.md) |
| The server exits non-zero at startup, naming a variable | A numeric `PYLON_*` variable is set to a value that cannot be parsed. See [Configuration](configuration.md) |

---

## Common Issues

### Client Won't Connect

1. **Wrong app key or host** — verify `key`, `wsHost`, and `wsPort` match your
   pylon configuration. See [Applications & Authentication](applications.md).

2. **TLS mismatch** — if pylon is behind a TLS terminator and the client is
   configured with `forceTLS: true`, make sure the proxy is forwarding the
   correct `Upgrade: websocket` header. If you terminated TLS at pylon itself,
   see [TLS / SSL](tls.md).

3. **Wrong transport or cluster setting** — pusher-js defaults to
   `cluster: 'mt1'`. Override it:

    ```js
    const pusher = new Pusher('YOUR_APP_KEY', {
      wsHost: 'your-pylon-host',
      wsPort: 7000,
      forceTLS: false,      // or true if TLS is in use
      enabledTransports: ['ws'],
      cluster: '',          // must be empty or omitted when using wsHost
    });
    ```

4. **Firewall** — ensure port `PYLON_PORT` (default `7000`) is reachable from
   the client network.

---

### 401 from the REST API

The Pusher REST authentication scheme signs requests with an HMAC over the
method, path, query string, and body MD5. A `401` can mean:

| Root cause | Fix |
|---|---|
| **Clock skew** between client and server | Ensure both clocks are synchronised (NTP/chronyc). Adjust `PYLON_REST_AUTH_WINDOW_SECS` (default: 600 s) to widen the window if necessary. |
| **Wrong secret** | Verify the `secret` in your pylon app config matches the secret used to initialise your SDK client. See [Applications & Authentication](applications.md). |
| **Incorrect body MD5** | Some HTTP clients (or proxies) silently re-encode the body. The MD5 of the exact bytes sent must be sent as the `body_md5` **query parameter** (not a header) — confirm it matches the MD5 of the bytes actually transmitted. |

---

### Does Pylon Scale?

Yes. Pylon scales **horizontally** by connecting multiple nodes to a shared
Redis instance — all nodes share channel state through the Redis pub/sub bus.
Clients can connect to any node; events triggered on one node are broadcast to
subscribers on all nodes.

See [Clustering & Scaling](clustering.md) for setup instructions.

---

### Too Many Open Files (`EMFILE`)

Every WebSocket connection consumes one file descriptor. The default Linux
per-process limit of 1 024 is too low for any production deployment.

See [Production Tuning — Open File Descriptors](production-tuning.md#open-file-descriptors)
for instructions on raising `LimitNOFILE` in systemd, Docker, and
`/etc/security/limits.conf`.

---

### Encrypted Channels

Pusher end-to-end encrypted channels (`private-encrypted-*`) rely on a
`shared_secret` that your **app's auth endpoint** generates and delivers to
the subscribing client. Pylon relays ciphertext frames as opaque bytes and
does **not** decrypt or inspect channel payloads — it never sees the
plaintext. The encryption/decryption happens entirely in your application
server and the browser/native client library.

To use encrypted channels:

1. Generate a 32-byte master key in your app server.
2. Implement a Pusher-compatible auth endpoint that returns the per-channel
   `shared_secret` alongside the standard `auth` token.
3. Configure your client library with the `channelAuthorization` endpoint.

See the [Pusher encrypted channels documentation](https://pusher.com/docs/channels/using_channels/encrypted-channels/)
for the full protocol. Your app's auth endpoint is the one you implement in
step 2 — pylon itself has no auth endpoint to configure.

---

## FAQ

**Q: Can I use pylon as a drop-in replacement for Pusher Channels?**

Yes — pylon implements the Pusher v7 WebSocket protocol and the Pusher HTTP
REST API. Any Pusher SDK that supports specifying a custom `wsHost`/`host`
works without code changes. See [Connecting Clients](clients.md) and
[Triggering Events](triggering-events.md).

---

**Q: How many connections can a single node handle?**

Pylon is designed for millions of mostly-idle connections per host. The
practical ceiling depends on available RAM (≈3.2 KB of kernel memory per
idle socket plus a few KB of application state) and the fd limit. See
[Production Tuning](production-tuning.md) for detailed planning constants
and tuning steps.

---

**Q: What happens to existing connections during a deploy?**

If you use `SIGTERM` + a process manager, pylon drains gracefully: it stops
accepting new connections, returns `503` from `/ready` so the load balancer
removes it from rotation, then closes existing connections with Pusher code
**4200** ("server restarting — reconnect immediately"). The Pusher.js client
reconnects automatically. See [Production Tuning — Graceful Restart](production-tuning.md#graceful-restart).

---

**Q: My webhook URL is getting no requests — why?**

Check `pylon_webhook_dropped_total` in [Observability](observability.md). A
rising count means the webhook mailbox is full. Also check
`pylon_webhook_delivered_total{status="failed"}` for delivery errors — pylon
will log the HTTP status returned by your endpoint. Ensure the endpoint is
reachable from the pylon process and responds within the delivery timeout. See
[Webhooks](webhooks.md).

---

**Q: I see `pylon_redis_connected 0` in metrics — what do I do?**

Pylon has lost its Redis connection. Check Redis server health, network
connectivity, and the `PYLON_REDIS_URL` configuration. Pylon will reconnect
automatically; no restart is needed. While disconnected, cluster fan-out is
suspended and `pylon_cluster_cmd_dropped_total` will rise. See
[Clustering & Scaling](clustering.md).
