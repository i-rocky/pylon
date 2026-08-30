# Triggering Events

Server-side code triggers events by calling the Pusher HTTP API — either via an official server
SDK (which handles auth for you) or by making raw signed HTTP requests.

Pylon imposes these limits on every trigger call:

- **Event payload:** 10 240 bytes (10 KiB) maximum.
- **Channels per publish:** up to 100 channels in a single `POST /apps/{app_id}/events` call.
- **Batch size:** up to 10 events in a single `POST /apps/{app_id}/batch_events` call.

---

## Server SDKs

Using an official Pusher server SDK is the recommended approach. The SDK builds and signs every
request automatically.

=== "Node.js (pusher-http-node)"

    ```bash
    npm install pusher
    ```

    ```js
    const Pusher = require("pusher");

    const pusher = new Pusher({
      appId: "<your-app-id>",
      key: "<your-app-key>",
      secret: "<your-app-secret>",
      host: "127.0.0.1",
      port: "7000",
      useTLS: false,
    });

    // Trigger an event on a single channel
    await pusher.trigger("my-channel", "my-event", { message: "hello" });

    // Trigger to multiple channels at once (up to 100)
    await pusher.trigger(["ch-1", "ch-2"], "update", { value: 42 });

    // Exclude the originating socket (prevents echo)
    await pusher.trigger("my-channel", "my-event", { text: "hi" }, {
      socket_id: socketId,
    });
    ```

    For TLS-terminated production deployments point `host` at your proxy and set `useTLS: true`.

=== "PHP / Laravel (pusher-php-server)"

    ```bash
    composer require pusher/pusher-php-server
    ```

    ```php
    $pusher = new Pusher\Pusher(
        '<your-app-key>',
        '<your-app-secret>',
        '<your-app-id>',
        [
            'host'    => '127.0.0.1',
            'port'    => 7000,
            'scheme'  => 'http',
            'cluster' => 'mt1',
        ]
    );

    // Trigger an event
    $pusher->trigger('my-channel', 'my-event', ['message' => 'hello']);
    ```

    In a Laravel application the broadcaster config in `config/broadcasting.php` already reads from
    the `PUSHER_*` env vars described on the [Connecting Clients](clients.md) page — no extra
    Pusher object is needed; use `broadcast(new MyEvent())` as usual.

---

## REST API reference

Every REST endpoint is authenticated via HMAC-SHA256 signed query parameters. The official SDKs
build these automatically; see [REST authentication](#rest-authentication) below if you need to
sign requests yourself.

### Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/apps/{app_id}/events` | Trigger one event on one or more channels |
| `POST` | `/apps/{app_id}/batch_events` | Trigger multiple events in a single request |
| `GET` | `/apps/{app_id}/channels` | List active channels (with optional prefix filter and stats) |
| `GET` | `/apps/{app_id}/channels/{channel_name}` | Get a single channel's state |
| `GET` | `/apps/{app_id}/channels/{channel_name}/users` | List presence members (presence channels only) |
| `POST` | `/apps/{app_id}/users/{user_id}/terminate_connections` | Disconnect all connections for a user |

---

#### `POST /apps/{app_id}/events`

Trigger a single named event on one or more channels.

**Request body (JSON):**

| Field | Required | Description |
|---|---|---|
| `name` | yes | Event name (e.g. `"order-updated"`) |
| `data` | yes | Event payload as a JSON-encoded string; max 10 240 bytes |
| `channels` | yes* | Array of channel names (up to 100) |
| `channel` | yes* | Single channel name (alternative to `channels`) |
| `socket_id` | no | Socket ID to exclude from delivery (prevents echo) |
| `info` | no | Comma-separated attributes to return: `subscription_count`, `user_count` |

*Provide either `channel` or `channels`, not both.

Encrypted channels (`private-encrypted-*`) must be targeted alone — mixing them with other channels
in one call returns an error.

##### Server-to-user channels (`#server-to-user-<user_id>`)

Triggering an event on the reserved channel `#server-to-user-<user_id>` delivers it to **every
live connection of that user** — the client must have authenticated via `pusher:signin`. The event
is routed through the user registry (cluster-wide on the Redis adapter), is never cached, and
`socket_id` exclusion does not apply to it (there is no originating socket among the recipient's
connections).

```bash
# Same signing steps as the raw curl example below, with the reserved channel in the body:
BODY='{"name":"invoice-paid","channel":"#server-to-user-42","data":"{\"amount\":99}"}'
BODY_MD5=$(echo -n "$BODY" | md5sum | cut -d' ' -f1)
QUERY="auth_key=${APP_KEY}&auth_timestamp=${TIMESTAMP}&auth_version=1.0&body_md5=${BODY_MD5}"
SIGNATURE=$(echo -en "POST\n/apps/${APP_ID}/events\n${QUERY}" \
  | openssl dgst -sha256 -hmac "$APP_SECRET" | cut -d' ' -f2)
curl -s -X POST \
  "http://${HOST}/apps/${APP_ID}/events?${QUERY}&auth_signature=${SIGNATURE}" \
  -H "Content-Type: application/json" -d "$BODY"
```

On the client side the signed-in user's connections receive it as an ordinary channel event on
`#server-to-user-<user_id>` — pusher-js surfaces it through its user-notification handling. A
client may subscribe to **its own** `#server-to-user-<user_id>` channel (after signin) and gets a
`subscription_succeeded`; subscribing to another user's channel — or any other `#`-prefixed name —
is rejected non-fatally with `pusher:subscription_error` status `401`.

---

#### `POST /apps/{app_id}/batch_events`

Trigger up to 10 events in a single request. Each item targets exactly one channel.

**Request body (JSON):**

```json
{
  "batch": [
    {
      "channel": "my-channel",
      "name": "event-a",
      "data": "{\"key\":\"value\"}"
    },
    {
      "channel": "presence-room",
      "name": "event-b",
      "data": "{\"n\":2}",
      "socket_id": "123.456"
    }
  ]
}
```

Each item has the same fields as a single event (`channel`, `name`, `data`, optional `socket_id`
and `info`).

---

#### `GET /apps/{app_id}/channels`

List all currently occupied channels for the app.

**Query parameters (in addition to auth params):**

| Parameter | Description |
|---|---|
| `filter_by_prefix` | Return only channels whose name starts with this prefix |
| `info` | Comma-separated attributes: `subscription_count`, `user_count` |

`user_count` is only valid when `filter_by_prefix` is set to a `presence-` prefix.

---

#### `GET /apps/{app_id}/channels/{channel_name}`

Fetch the state of one channel.

**Query parameters (in addition to auth params):**

| Parameter | Description |
|---|---|
| `info` | Comma-separated attributes: `occupied`, `subscription_count`, `user_count` |

---

#### `GET /apps/{app_id}/channels/{channel_name}/users`

Return the list of member IDs currently subscribed to a presence channel.

Only works on `presence-*` channels. Returns `{"users": [{"id": "..."}, ...]}`.

---

#### `POST /apps/{app_id}/users/{user_id}/terminate_connections`

Disconnect all current WebSocket connections for the specified user across the cluster.
Returns `{}` on success.

---

### REST authentication

Every request must carry four query parameters (or five for requests with a body):

| Parameter | Description |
|---|---|
| `auth_key` | Your app's public key |
| `auth_timestamp` | Current Unix time in seconds |
| `auth_version` | Always `1.0` |
| `body_md5` | MD5 hex digest of the raw request body — **required when a body is present** |
| `auth_signature` | HMAC-SHA256 hex signature (see below) |

**Signing string:**

```
{METHOD}\n{path}\n{sorted-query}
```

Where `{sorted-query}` is all query parameters **except `auth_signature`**, with keys
lowercased and sorted alphabetically, joined as `key=value&key=value`.

**Signature:**

```
HMAC-SHA256(app_secret, signing_string)  →  hex string
```

In practice the official server SDKs build this automatically — you only need to supply
`appId`, `key`, `secret`, `host`, and `port`.

---

### Raw `curl` example

The example below triggers an event without a server SDK. The signature is pre-computed here for
illustration — in production, compute it dynamically.

```bash
# Variables
APP_ID="my-app-id"
APP_KEY="my-app-key"
APP_SECRET="my-app-secret"
HOST="127.0.0.1:7000"
TIMESTAMP=$(date +%s)

# Request body
BODY='{"name":"order-updated","channel":"orders","data":"{\"id\":99}"}'

# Compute body MD5
BODY_MD5=$(echo -n "$BODY" | md5sum | cut -d' ' -f1)

# Build signing string (params sorted: auth_key, auth_timestamp, auth_version, body_md5)
QUERY="auth_key=${APP_KEY}&auth_timestamp=${TIMESTAMP}&auth_version=1.0&body_md5=${BODY_MD5}"
SIGNING_STRING="POST\n/apps/${APP_ID}/events\n${QUERY}"

# Compute HMAC-SHA256 signature
SIGNATURE=$(echo -en "$SIGNING_STRING" | openssl dgst -sha256 -hmac "$APP_SECRET" | cut -d' ' -f2)

# Send the request
curl -s -X POST \
  "http://${HOST}/apps/${APP_ID}/events?${QUERY}&auth_signature=${SIGNATURE}" \
  -H "Content-Type: application/json" \
  -d "$BODY"
```
