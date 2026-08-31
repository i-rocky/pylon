# Troubleshooting & FAQ

---

## Close Codes {#close-codes}

When pylon closes a WebSocket connection it sends a Pusher error frame with a
numeric code before the WebSocket close. The table below lists all codes,
their meanings, and the recommended client action.

| Code | Meaning | Client action |
|---|---|---|
| `4001` | App key not found | Do not reconnect; check `key` in your Pusher client config |
| `4004` | App connection limit reached (per-app `capacity`) | Do not reconnect; contact the server operator |
| `4005` | Connection path malformed (not `/app/{key}`, or empty key) | Do not reconnect; fix the WebSocket URL |
| `4006` | Invalid protocol version string format | Do not reconnect; fix client configuration |
| `4007` | Unsupported protocol version | Do not reconnect; upgrade client library |
| `4008` | No protocol version supplied | Do not reconnect; upgrade client library |
| `4009` | Connection not authorised — sign-in verification failed, the user's connections were terminated, or the app was removed/disabled mid-connection | Do not reconnect; fix authentication or check the app's status |
| `4100` | Server is over capacity (node connection ceiling reached, or the broadcast pipeline is saturated) | Reconnect with exponential back-off |
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
invalid and `401` means the subscription auth failed — the connection stays
open in both cases.

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
