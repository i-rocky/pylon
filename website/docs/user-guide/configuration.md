# Configuration

Pylon uses two layers of configuration:

- **The application store** — defines the applications (with their keys, secrets, and per-app
  settings). By default this is a local **`apps.json`** file; for SaaS-scale deployments it can
  instead be a **database** (SQLite, MySQL, Postgres, or MongoDB) fronted by an in-process +
  Redis cache. See the [Applications & Authentication](applications.md) page for details.
- **`PYLON_*` environment variables** — control server-wide behaviour: networking, worker count,
  the application store, protocol limits, adapter selection, overload policy, and more.

All variables are optional. Unset variables fall back to the defaults shown below.

!!! note "Auto-tuned defaults"
    Several defaults self-tune to the host at startup: `PYLON_WORKERS` defaults to the number
    of available CPU cores, and the memory budget is derived from the cgroup/host effective
    memory when not set explicitly.

---

## Core

| Variable | Default | Description |
|---|---|---|
| `PYLON_BIND` | `0.0.0.0` | IP address the WebSocket listener binds to. |
| `PYLON_PORT` | `7000` | TCP port for the WebSocket listener and HTTP REST API. |
| `PYLON_APPS_PATH` | `apps.json` | Path to the JSON file that defines the application registry (used when `PYLON_APP_MANAGER=static`). |
| `PYLON_WORKERS` | `0` | Number of per-core worker threads. `0` = auto (one per available CPU). |

---

## Application store

By default Pylon reads applications from the local `apps.json` file (`PYLON_APP_MANAGER=static`).
For SaaS-scale deployments — more apps than fit comfortably in a file, or apps provisioned by a
control plane — set `PYLON_APP_MANAGER` to a database driver and provide `PYLON_APP_DSN`. A
DB-backed store is fronted by a two-tier cache (in-process L1 + optional Redis L2) so the
per-connection and per-publish lookups stay fast. See
[Applications & Authentication](applications.md#database-backed-app-stores) for the full guide
(schema, caching, invalidation, and the admin API).

| Variable | Default | Description |
|---|---|---|
| `PYLON_APP_MANAGER` | `static` | Application store backend: `static` (the `apps.json` file), `sqlite`, `mysql`, `postgres`, or `mongo`. |
| `PYLON_APP_DSN` | _(none)_ | Database connection string for a non-`static` manager, e.g. `sqlite:///var/lib/pylon/apps.db`, `mysql://user:pass@host/db`, `postgres://user:pass@host/db`, `mongodb://host/db`. |
| `PYLON_APP_CACHE` | `true` | Enable the cache in front of a DB-backed store. Set `0`, `off`, or `false` to disable (every lookup hits the database). No effect for `static`. |
| `PYLON_APP_CACHE_MAX` | `100000` | L1 (in-process) cache max capacity, in number of apps. Bounded — "unlimited apps" never grows memory without bound. |
| `PYLON_APP_CACHE_TTL` | `300` | L1 positive-entry TTL (seconds). The worst-case staleness floor even if no invalidation signal ever arrives. |
| `PYLON_APP_CACHE_NEG_MAX` | `10000` | L1 negative-cache (unknown-key) max capacity. A separate, smaller cache so a flood of bad keys never evicts real apps. |
| `PYLON_APP_CACHE_NEG_TTL` | `30` | L1 negative-entry TTL (seconds). Kept short. |
| `PYLON_APP_CACHE_REDIS_URL` | _(none)_ | Optional Redis URL for the shared L2 cache + the cross-node invalidation channel. When set, a cold node reads warm apps from Redis instead of the database, and the admin invalidate API is enabled. |
| `PYLON_ADMIN_TOKEN` | _(none)_ | Bearer token for the admin API (`POST /admin/apps/{id}/invalidate`). When unset, the admin API is **disabled** (returns 404). |
| `PYLON_METRICS_TOKEN` | _(none)_ | Bearer token for `GET /metrics`. When unset, metrics are open (default). When set, a scrape must carry `Authorization: Bearer <token>`; anything else returns **404** (not 401 — no existence disclosure). `/health` and `/ready` are never gated. Empty string is treated as unset. See [Deployment: Protecting /metrics](deployment.md#protecting-metrics). |
| `PYLON_APP_SWEEP_INTERVAL` | `0` | Interval (seconds) for the app-purge sweep backstop. `0` disables it. When set, the sweep periodically reconciles connected apps against the database and force-closes any that have been removed/disabled. |

---

## Adapter / Redis

| Variable | Default | Description |
|---|---|---|
| `PYLON_ADAPTER` | `local` | Channel-state adapter. `local` for single-node; `redis` for clustered deployments. |
| `PYLON_REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection URL (used when `PYLON_ADAPTER=redis`). |
| `PYLON_REDIS_PREFIX` | `pylon` | Key prefix applied to all Redis keys to avoid collisions with other services. |
| `PYLON_REDIS_POOL_SIZE` | `6` | Size of the Redis connection pool per server instance. |
| `PYLON_REDIS_MEMBERSHIP_TTL` | `60` | Seconds after which a cluster node's membership entry expires if not renewed. |
| `PYLON_REDIS_PRESENCE_HEARTBEAT` | `25` | Interval (seconds) at which presence member entries are refreshed in Redis. |
| `PYLON_REDIS_NODE_HEARTBEAT` | `5` | Interval (seconds) at which each node publishes its heartbeat to Redis. |
| `PYLON_REDIS_SWEEP_INTERVAL` | `10` | Interval (seconds) at which stale presence and membership entries are swept. |
| `PYLON_REDIS_SHARDED_PUBSUB` | `false` | Enable Redis 7+ sharded Pub/Sub. Set `1` or `true` to enable. |

---

## TLS

TLS is optional. Both `PYLON_TLS_CERT` and `PYLON_TLS_KEY` must be set together to enable TLS;
setting only one is a fatal configuration error. An empty string is treated the same as unset.

| Variable | Default | Description |
|---|---|---|
| `PYLON_TLS_CERT` | _(none)_ | Path to the PEM certificate chain file. Must be set with `PYLON_TLS_KEY` to enable TLS. |
| `PYLON_TLS_KEY` | _(none)_ | Path to the PEM private key file (PKCS#8, RSA, or EC). Must be set with `PYLON_TLS_CERT`. |
| `PYLON_TLS_CA` | _(none)_ | Optional path to a PEM CA certificate. When set, enables mTLS client verification (requires cert+key). |

TLS configuration is covered in detail on the [TLS / SSL](tls.md) page.

---

## Protocol / Limits

| Variable | Default | Description |
|---|---|---|
| `PYLON_ACTIVITY_TIMEOUT` | `120` | Seconds of inactivity after which the server sends a `pusher:ping`. |
| `PYLON_PONG_TIMEOUT` | `30` | Seconds the server waits for a `pusher:pong` reply before closing the connection. |
| `PYLON_MAX_CONN_LIFETIME_SECS` | `86400` | Maximum connection age in seconds before the server closes the connection with code `4202` ("Closed after inactivity" — Pusher's 24-hour maximum connection lifetime; clients reconnect immediately). The deadline is absolute from connection establishment and is not reset by activity. Set `0` to disable. |
| `PYLON_MAX_HEAD_BYTES` | `16384` | Maximum accepted HTTP request-head size in bytes (WebSocket upgrade or REST request) a connection may accumulate before the handshake completes. A head larger than this is rejected and the connection closed — a slowloris client dribbling headerless bytes cannot grow server memory without bound. Generous against any legitimate head (real upgrade heads are ~200 bytes). Set `0` to disable. |
| `PYLON_HANDSHAKE_TIMEOUT_MS` | `10000` | Slowloris protection: milliseconds from TCP accept within which a connection must complete its handshake (HTTP head + TLS + WebSocket upgrade + session establish). On expiry the pre-session connection is closed and its slot reclaimed. The deadline is absolute from accept and is NOT reset by inbound activity (a dribbling client is reaped at the deadline all the same). Cleared the moment the session establishes; never applies to live sessions. Set `0` to disable. |
| `PYLON_STRICT_PROTOCOL` | `false` | When `true`, reject any Pusher protocol violation instead of silently ignoring it. Set `1` or `true` to enable. |
| `PYLON_MAX_CHANNEL_NAME_LENGTH` | `164` | Maximum allowed channel name length in bytes. |
| `PYLON_MAX_EVENT_NAME_LENGTH` | `200` | Maximum allowed event name length in bytes. |
| `PYLON_MAX_EVENT_PAYLOAD_BYTES` | `10240` | Maximum event payload size in bytes (10 KiB). |
| `PYLON_MAX_PRESENCE_MEMBERS` | `100` | Maximum number of members allowed in a presence channel. |
| `PYLON_MAX_PRESENCE_USER_ID_LENGTH` | `128` | Maximum length of a presence member's `user_id` in bytes. |
| `PYLON_MAX_PRESENCE_USER_INFO_BYTES` | `1024` | Maximum size of a presence member's `user_info` JSON in bytes. |
| `PYLON_MAX_CLIENT_EVENTS_PER_SECOND` | `10` | Maximum client events a single connection may send per second. |
| `PYLON_MAX_SUBSCRIPTIONS_PER_CONNECTION` | `200` | Maximum simultaneous channel subscriptions per connection. Excess subscribes get a non-fatal `pusher:subscription_error` (`LimitReached`, `4004`). A pylon-specific guard — hosted Pusher documents no such limit. Set `0` for unlimited. |
| `PYLON_MAX_WATCHLIST_SIZE` | `100` | Maximum number of channels a single connection may watch simultaneously. |
| `PYLON_CACHE_TTL_SECS` | `1800` | TTL (seconds) for cached channel and presence state (30 minutes). |
| `PYLON_MAX_CHANNELS_PER_PUBLISH` | `100` | Maximum number of channels a single REST publish call may target. |
| `PYLON_MAX_BATCH_EVENTS` | `10` | Maximum number of events in a single batch publish request. |
| `PYLON_REST_AUTH_WINDOW_SECS` | `600` | Acceptable clock-skew window (seconds) for REST request timestamp validation. |

---

## Webhooks

| Variable | Default | Description |
|---|---|---|
| `PYLON_WEBHOOK_BATCH_MS` | `50` | Time window (milliseconds) over which outgoing webhook events are batched. |
| `PYLON_WEBHOOK_MAX_CONCURRENCY` | `100` | Maximum number of concurrent in-flight webhook deliveries. |
| `PYLON_WEBHOOK_BACKOFF_BASE_MS` | `1000` | First retry delay (milliseconds) for a failed webhook delivery; doubles each attempt. |
| `PYLON_WEBHOOK_BACKOFF_CAP_MS` | `60000` | Upper bound (milliseconds) for each webhook retry delay. |
| `PYLON_WEBHOOK_RETRY_BUDGET_MS` | `300000` | Total time (milliseconds, attempts included) a webhook delivery may keep retrying without a 2xx — Pusher parity ("exponential backoff, for 5 minutes"). `0` disables retries. |
| `PYLON_WEBHOOK_TIMEOUT_MS` | `5000` | HTTP request timeout (milliseconds) for each webhook delivery attempt. |
| `PYLON_WEBHOOK_ALLOW_PRIVATE_TARGETS` | `false` | SSRF guard escape hatch: set `1`/`true` to allow webhook delivery to private/loopback/link-local targets. Refused targets fail fast (no HTTP sent, no retries — a configuration error). See [Webhooks: Target restrictions](webhooks.md#target-restrictions-ssrf-guard). |
| `PYLON_WEBHOOK_VACATED_GRACE_MS` | `3000` | Grace period (milliseconds) after the last member leaves a channel before a `channel_vacated` webhook fires. |

Deprecated (honored or ignored with a startup warning, for one release): `PYLON_WEBHOOK_RETRY_BASE_MS` (alias of `PYLON_WEBHOOK_BACKOFF_BASE_MS`), `PYLON_WEBHOOK_MAX_RETRIES` (ignored — retries are budget-bounded, not count-bounded).

---

## Overload / Capacity

These variables control Pylon's adaptive back-pressure system. All defaults are automatically
derived from the host's memory envelope and CPU count. Override only when you need to tune for
a specific workload.

| Variable | Default | Description |
|---|---|---|
| `PYLON_MEMORY_BUDGET_BYTES` | `0` | Total memory budget in bytes for the transport layer. `0` = auto (derived from cgroup/host memory using the `max(1.5 GiB, 7%)` reserve formula). |
| `PYLON_MEMORY_BUDGET_FRACTION` | `0.0` | Memory budget as a fraction of effective host memory (0.0–1.0). Applied when `PYLON_MEMORY_BUDGET_BYTES` is `0`. `0.0` = use the built-in reserve formula. |
| `PYLON_MAX_CONNECTIONS` | `0` | Node-wide ceiling on simultaneous connections across all apps. Connections beyond it are closed with WebSocket code `4100`. `0` = auto-derive from the memory budget (`budget / PYLON_EXPECTED_PER_CONN_BYTES`). |
| `PYLON_EXPECTED_PER_CONN_BYTES` | `8192` | Expected per-connection memory footprint (bytes), used to auto-derive `PYLON_MAX_CONNECTIONS` when it is `0`. |
| `PYLON_MAILBOX_CAPACITY` | `256` | Capacity (frames) of each connection's inbound mailbox — the bounded channel used for direct sends (presence rosters, `member_added`/`member_removed`, user-targeted events, watchlist notifications, cluster deliveries). When full, a frame is silently dropped and `pylon_mailbox_dropped_total` increments. Must be `> 0`. |
| `PYLON_EXPECTED_CONNS_PER_WORKER` | `50000` | Expected concurrent connections per worker thread, used to derive the per-connection out-queue cap. |
| `PYLON_PERCONN_QUEUE_MIN_BYTES` | `262144` | Lower clamp for the per-connection outbound queue cap (bytes). Default 256 KiB. |
| `PYLON_PERCONN_QUEUE_MAX_BYTES` | `8388608` | Upper clamp for the per-connection outbound queue cap (bytes). Default 8 MiB. |
| `PYLON_CODEL_TARGET_MS` | `5` | CoDel freshness target (milliseconds). A frame whose sojourn exceeds 2× this while the queue is overloaded is dropped. Set `0` to disable CoDel. |
| `PYLON_CODEL_INTERVAL_MS` | `100` | CoDel interval (milliseconds): the window over which the minimum sojourn is tracked. |
| `PYLON_PSI_THRESHOLD` | `15.0` | PSI `full avg10` memory-pressure threshold (percent). When exceeded, the memory budget factor is shrunk. |
| `PYLON_PSI_BACKSTOP` | _(auto)_ | PSI memory-pressure backstop. Auto-enabled when the kernel pressure file is readable. Set `1`/`true` to force on, `0`/`false` to force off. |
| `PYLON_BROADCAST_HANDOFF_CAP` | `1024` | Capacity (frames) of each worker's bounded broadcast hand-off channel. |

### Graceful shutdown

| Variable | Default | Description |
|---|---|---|
| `PYLON_SHUTDOWN_PREDRAIN_MS` | `2000` | Milliseconds to hold `/ready` at 503 before workers begin draining. Gives load balancers time to stop sending new traffic. |
| `PYLON_SHUTDOWN_GRACE_MS` | `10000` | Milliseconds each worker waits for in-flight connections to drain before force-closing. |

---

## Deliberate restrictions vs hosted Pusher

Pylon is a drop-in replacement for the common path, and a few tighter limits are
deliberate hardening rather than gaps. They are all operator-tunable where that
makes sense:

- **REST request body cap.** POST bodies are capped at
  `max_batch_events × max_event_payload_bytes + 64 KiB` of JSON-framing headroom
  — **164 KiB at the defaults** (10 × 10 KiB + 64 KiB), where hosted Pusher
  accepts up to a 10 MB envelope. Every legitimate request (a full batch of
  max-size events) fits; the smaller cap simply bounds how much memory one
  unauthenticated request can make the server allocate. Raising
  `PYLON_MAX_BATCH_EVENTS` / `PYLON_MAX_EVENT_PAYLOAD_BYTES` raises the cap with
  them.
- **Per-connection subscription cap.** `PYLON_MAX_SUBSCRIPTIONS_PER_CONNECTION`
  (default `200`, `0` = unlimited). Hosted Pusher documents no per-connection
  subscription limit; this is a pylon-specific memory guard (each subscription
  holds server-side state). Excess subscribes fail non-fatally with
  `pusher:subscription_error` (`LimitReached`, `4004`).
- **Protocol v7 only.** Pylon speaks Pusher Channels protocol **v7** and
  nothing else: connections negotiating an unsupported `protocol`/`version` are
  rejected with `4007`. There is no v5/v6 compatibility surface.
- **Encrypted channels are a pure relay.** Like current hosted Pusher, the
  encryption happens in the client libraries (and your app server for
  server-triggered payloads) — pylon transports `private-encrypted-*` frames as
  opaque ciphertext and never decrypts or inspects them. There is no
  server-side `encryption_master_key` facility to configure.

---

For the authoritative full list of variables (including any added after this page was written),
see [`src/server/config.rs`](https://github.com/i-rocky/pylon/blob/master/src/server/config.rs).

Production tuning guidance (NUMA pinning, memory-budget sizing, CoDel tuning) is covered on the
[Production Tuning](production-tuning.md) page. Clustering and Redis adapter setup is covered on
the [Clustering & Scaling](clustering.md) page. Metrics and health endpoints are described on the
[Observability](observability.md) page.
