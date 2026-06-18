# Configuration

Pylon uses two layers of configuration:

- **`apps.json`** — defines the applications (with their keys, secrets, and per-app settings).
  See the [Applications & Authentication](applications.md) page for details.
- **`PYLON_*` environment variables** — control server-wide behaviour: networking, worker count,
  protocol limits, adapter selection, overload policy, and more.

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
| `PYLON_APPS_PATH` | `apps.json` | Path to the JSON file that defines the application registry. |
| `PYLON_WORKERS` | `0` | Number of per-core worker threads. `0` = auto (one per available CPU). |

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
| `PYLON_STRICT_PROTOCOL` | `false` | When `true`, reject any Pusher protocol violation instead of silently ignoring it. Set `1` or `true` to enable. |
| `PYLON_MAX_CHANNEL_NAME_LENGTH` | `164` | Maximum allowed channel name length in bytes. |
| `PYLON_MAX_EVENT_NAME_LENGTH` | `200` | Maximum allowed event name length in bytes. |
| `PYLON_MAX_EVENT_PAYLOAD_BYTES` | `10240` | Maximum event payload size in bytes (10 KiB). |
| `PYLON_MAX_PRESENCE_MEMBERS` | `100` | Maximum number of members allowed in a presence channel. |
| `PYLON_MAX_PRESENCE_USER_ID_LENGTH` | `128` | Maximum length of a presence member's `user_id` in bytes. |
| `PYLON_MAX_PRESENCE_USER_INFO_BYTES` | `1024` | Maximum size of a presence member's `user_info` JSON in bytes. |
| `PYLON_MAX_CLIENT_EVENTS_PER_SECOND` | `10` | Maximum client events a single connection may send per second. |
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
| `PYLON_WEBHOOK_MAX_RETRIES` | `3` | Number of retry attempts for a failed webhook delivery. |
| `PYLON_WEBHOOK_RETRY_BASE_MS` | `100` | Base delay (milliseconds) for webhook retry back-off. |
| `PYLON_WEBHOOK_TIMEOUT_MS` | `5000` | HTTP request timeout (milliseconds) for each webhook delivery attempt. |
| `PYLON_WEBHOOK_VACATED_GRACE_MS` | `3000` | Grace period (milliseconds) after the last member leaves a channel before a `channel_vacated` webhook fires. |

---

## Overload / Capacity

These variables control Pylon's adaptive back-pressure system. All defaults are automatically
derived from the host's memory envelope and CPU count. Override only when you need to tune for
a specific workload.

| Variable | Default | Description |
|---|---|---|
| `PYLON_MEMORY_BUDGET_BYTES` | `0` | Total memory budget in bytes for the transport layer. `0` = auto (derived from cgroup/host memory using the `max(1.5 GiB, 7%)` reserve formula). |
| `PYLON_MEMORY_BUDGET_FRACTION` | `0.0` | Memory budget as a fraction of effective host memory (0.0–1.0). Applied when `PYLON_MEMORY_BUDGET_BYTES` is `0`. `0.0` = use the built-in reserve formula. |
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

For the authoritative full list of variables (including any added after this page was written),
see [`src/server/config.rs`](https://github.com/i-rocky/pylon/blob/master/src/server/config.rs).

Production tuning guidance (NUMA pinning, memory-budget sizing, CoDel tuning) is covered on the
[Production Tuning](production-tuning.md) page. Clustering and Redis adapter setup is covered on
the [Clustering & Scaling](clustering.md) page. Metrics and health endpoints are described on the
[Observability](observability.md) page.
