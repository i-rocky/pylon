# Applications & Authentication

## The application registry

Pylon supports multiple applications on a single server instance. Each application is identified
by a unique `id` and has its own key/secret pair. Applications are defined in a JSON file — by
default `apps.json` (configurable via [`PYLON_APPS_PATH`](configuration.md)).

### apps.json format

```json
[
  {
    "name": "Example App",
    "id": "<your-app-id>",
    "key": "<your-app-key>",
    "secret": "<your-app-secret>",
    "enabled": true,
    "client_messages_enabled": true,
    "subscription_count_enabled": false,
    "capacity": 10000,
    "webhooks": [
      {
        "url": "https://example.test/pusher/webhooks",
        "event_types": [
          "channel_occupied",
          "channel_vacated",
          "member_added",
          "member_removed",
          "client_event",
          "cache_miss",
          "subscription_count"
        ],
        "headers": { "X-Custom": "value" }
      }
    ]
  }
]
```

### Field reference

| Field | Type | Description |
|---|---|---|
| `name` | string | Human-readable label for this app (not used in the protocol). |
| `id` | string | Unique app identifier. Included in REST API paths (`/apps/{id}/...`). |
| `key` | string | Public app key. Clients use this to identify the app when connecting. |
| `secret` | string | Shared secret for HMAC signing. Never sent to clients. |
| `enabled` | boolean | When `false`, the app is treated as if it did not exist: new connections are rejected and (with a DB-backed store) existing connections are force-closed. Defaults to `true`. |
| `client_messages_enabled` | boolean | When `true`, clients may publish events to channels via `client_event`. Defaults to `false`. |
| `subscription_count_enabled` | boolean | When `true`, the server emits `pusher_internal:subscription_count` events as a channel's subscriber count changes, and the `subscription_count` webhook (if the endpoint also lists it in `event_types`). Defaults to `false`. |
| `capacity` | integer | Maximum concurrent WebSocket connections for this app (`0` = unlimited). Connections beyond this limit are refused with WebSocket close code **4004**. |
| `webhooks` | array | Zero or more webhook targets. Each entry has a `url`, an `event_types` list, and an optional `headers` map. See the [Webhooks](webhooks.md) page for the full event-type reference. |

!!! note
    Unknown fields in `apps.json` are ignored. Earlier examples included `host`, `path`, and
    `statistics_enabled` — these are **not** read by Pylon and have no effect; they are safe to
    leave in an existing file but are no longer documented.

---

## Database-backed app stores

`apps.json` is ideal for a fixed set of applications. For a **SaaS-scale** deployment — apps
provisioned by a control plane, or more apps than fit comfortably in a file — Pylon can read
applications from a **database** instead. Set `PYLON_APP_MANAGER` and `PYLON_APP_DSN`:

| `PYLON_APP_MANAGER` | Example `PYLON_APP_DSN` |
|---|---|
| `sqlite` | `sqlite:///var/lib/pylon/apps.db` (single-node / edge / dev) |
| `mysql` | `mysql://user:pass@db-host:3306/pylon` |
| `postgres` | `postgres://user:pass@db-host:5432/pylon` |
| `mongo` | `mongodb://db-host:27017/pylon` |

Pylon only **reads** the application store — provisioning (creating, updating, deleting apps) is
your control plane's job. The lookup path is the same as for `apps.json`: an app's record is
resolved once at connection establish and once per REST publish, never per message.

### Schema

The relational drivers expect an `apps` table; the columns mirror the
[field reference](#field-reference) above. Ready-to-run DDL ships in the repository under
[`deploy/db/`](https://github.com/i-rocky/pylon/tree/master/deploy/db) — one file per engine:

```sql
-- deploy/db/postgres/001_apps.sql (MySQL/SQLite equivalents alongside)
CREATE TABLE IF NOT EXISTS apps (
    id          VARCHAR(255) NOT NULL PRIMARY KEY,
    key         VARCHAR(255) NOT NULL UNIQUE,
    secret      VARCHAR(255) NOT NULL,
    name        VARCHAR(255) NOT NULL DEFAULT '',
    capacity    BIGINT NOT NULL DEFAULT 0,
    client_messages_enabled     BIGINT NOT NULL DEFAULT 0,   -- 0/1
    subscription_count_enabled  BIGINT NOT NULL DEFAULT 0,   -- 0/1
    enabled     BIGINT NOT NULL DEFAULT 1,                   -- 0/1
    webhooks    TEXT NOT NULL DEFAULT '[]',                  -- JSON array
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

The unique index on `key` and the primary key on `id` make both lookups index hits. Boolean
columns are stored as `0`/`1` integers and `webhooks` as a JSON array string (the same shape as
the `apps.json` `webhooks` field). MongoDB uses an `apps` collection with the same fields and
unique indexes on `id` and `key` (see `deploy/db/mongo/001_indexes.js`).

### Caching

A DB-backed store is fronted by a two-tier cache so per-connection lookups stay fast even with an
unbounded app catalogue:

- **L1 (in-process):** a bounded cache (TinyLFU, per-entry TTL, single-flight) — a warm hit is
  ~hundreds of nanoseconds, faster than scanning a large file. Concurrent misses for the same app
  collapse into one database query, so a connection storm to a cold app does not stampede the DB.
- **L2 (Redis, optional):** set `PYLON_APP_CACHE_REDIS_URL` to share warm apps across nodes and
  survive restarts — a cold node reads from Redis instead of the database.
- **Negative cache:** a separate, smaller, short-TTL cache holds "no such app" so a flood of bad
  keys can never evict real apps and never reaches the database.

A backend outage is distinguished from a genuinely-unusable app: a missing app is rejected
fatally (WS `4001`), a disabled app with its own fatal code (WS `4003`), while a transient
DB/Redis error is rejected *retryably* (WS
`4103`) and never negatively cached, so clients reconnect and succeed when the backend recovers.
See [Configuration → Application store](configuration.md#application-store) for the cache tuning
variables.

### Keeping the cache correct

Every cache entry has a TTL, so the worst-case staleness is bounded even if no signal ever
arrives. To apply a change immediately (a rotated secret, a disabled or deleted app), tell Pylon
to invalidate its cache. With `PYLON_APP_CACHE_REDIS_URL` set, an authenticated admin call
publishes the invalidation to every node:

```bash
# Refresh: re-fetch this app on the next lookup (e.g. after a config/secret change).
curl -X POST "http://pylon:7000/admin/apps/<app-id>/invalidate" \
  -H "Authorization: Bearer $PYLON_ADMIN_TOKEN" \
  -d '{"key":"<app-key>","action":"refresh"}'

# Remove: the app is gone — every node evicts it AND force-closes all of its live
# connections with WebSocket close code 4009.
curl -X POST "http://pylon:7000/admin/apps/<app-id>/invalidate" \
  -H "Authorization: Bearer $PYLON_ADMIN_TOKEN" \
  -d '{"key":"<app-key>","action":"remove"}'
```

`action` defaults to `refresh` if omitted. The admin API is **disabled (404)** unless
`PYLON_ADMIN_TOKEN` is set, requires the bearer token (constant-time compared), and requires
`PYLON_APP_CACHE_REDIS_URL` for cross-node delivery (otherwise it returns 503). Your control
plane can call this endpoint on any app write, or publish to the `pylon:app:invalidate` Redis
channel directly.

As a backstop, set `PYLON_APP_SWEEP_INTERVAL` to a number of seconds: Pylon then periodically
re-checks the currently-connected apps against the database and force-closes any that have been
disabled or deleted, even if an invalidation signal was missed.

---

## Key and secret usage

**Clients** use the `key` to connect. A Pusher-compatible client library is initialised with the
app key and optionally a cluster/host pointing at your Pylon server:

```js
const pusher = new Pusher("<your-app-key>", {
  wsHost: "pylon.example.com",
  wsPort: 7000,
  forceTLS: false,
  enabledTransports: ["ws"],
});
```

**Server-side code** uses both the `key` and `secret` to authenticate REST calls and to generate
subscription auth tokens for private and presence channels. The Pusher HTTP client libraries
(`pusher-http-node`, `pusher-http-python`, etc.) accept these as `appId`, `key`, and `secret`
constructor parameters.

---

## HMAC authentication model

Pylon uses HMAC-SHA256 for all authentication operations, matching the Pusher v7 protocol.
The `secret` is the shared HMAC key; it never leaves the server.

### Private and presence channels

Clients subscribing to a `private-*` or `presence-*` channel must first obtain an auth token
from your own backend server. The backend signs the subscription using HMAC-SHA256 and returns
a token of the form `<app_key>:<hex_signature>`.

The signing strings are:

- **Private channel**: `HMAC-SHA256(secret, "<socket_id>:<channel>")`
- **Presence channel**: `HMAC-SHA256(secret, "<socket_id>:<channel>:<channel_data>")`
  where `channel_data` is the verbatim JSON string containing at minimum `{"user_id": "..."}`.

Pylon verifies this token with a constant-time comparison before allowing the subscription.

### User sign-in (`pusher:signin`)

The `pusher:signin` flow lets a client authenticate as a named user. The signing string is:

```
HMAC-SHA256(secret, "<socket_id>::user::<user_data>")
```

where `user_data` is the verbatim JSON string the client sends (must contain `"id"`).

### REST authentication

HTTP requests to the Pylon REST API (triggering events, querying channel state) are authenticated
using HMAC-SHA256 over a canonical query string derived from the request method, path, and
parameters. REST signing details are covered on the [Triggering Events](triggering-events.md) page.

---

## Per-app connection capacity

The `capacity` field sets a hard ceiling on concurrent WebSocket connections for that app.
When a new connection would push the count over the limit, Pylon sends a WebSocket close frame
with code **4004** (capacity exceeded) and refuses the connection.

In single-node mode the limit is enforced by that node's local connection count for the app.
With the Redis adapter the limit is enforced **cluster-wide**: after the fast node-local check
passes, the admission decision is completed in Redis — an atomic check-and-increment against a
per-app cluster counter (`{prefix}:appconns`) shared by every node — so N nodes jointly hold
the app to `capacity` connections, not `N × capacity`.

Two operational notes for clustered deployments:

* **Bridge fail-open.** The Redis admission runs on the node's cluster bridge. If that bridge is
  momentarily unavailable (channel saturated, bridge stalled, or a transient Redis error), the
  connection is **admitted anyway** rather than locked out — during such a blip each node falls
  back to enforcing its local count only, so the cluster-wide ceiling may be exceeded by up to
  the blip's worth of admissions. If a Redis outage outlasts the per-node capacity-hash TTL
  (~65s at the defaults), the counters **self-heal** when Redis comes back: each node's
  heartbeat re-seeds its per-app counts in Redis from that node's live local connections, so
  the cluster totals resume from truth. (Connections that closed *during* the outage still
  leak their single unit each — the same bounded one-unit leak as any dropped release.)
* **Crashed nodes.** A node that dies without closing its connections still holds its capacity
  units in Redis until the sweeper reclaims them. Reclaim timing is heartbeat-based: the dead
  node's heartbeat key expires (`3 × PYLON_REDIS_NODE_HEARTBEAT` after its last beat), the
  sweep lease it may still hold expires after that, and the next sweep pass frees its counts —
  worst case `3 × heartbeat + lease + one sweep interval` ≈ **55 seconds at the defaults**
  (5s heartbeat, 10s sweep; the lease is `max(3 × sweep, 5s)`), typically faster.

Set `capacity` to `0` to disable the limit (unrestricted). For most production deployments,
sizing capacity to match your expected peak concurrent users plus a comfortable headroom is
recommended.

---

## Security: keep apps.json out of version control

`apps.json` contains the `secret` for each app — treat it like a password file.

The repo's `.gitignore` already excludes `apps.json`. Keep it that way: **do not commit
`apps.json` to version control**. Use a secrets manager, environment-specific config injection,
or a mounted secret volume to deliver the file at runtime.

If a secret is ever exposed, generate a new key/secret pair and update `apps.json` — all
currently connected clients will need to re-authenticate.
