# Observability

Pylon exposes Prometheus metrics, a liveness probe, and a readiness probe on the
same HTTP port as the REST API (`PYLON_PORT`, default 7000).

---

## `GET /metrics` — Prometheus Exposition

Returns a Prometheus text-format (v0.0.4) snapshot.

By default the endpoint is unauthenticated (back-compat). Set
`PYLON_METRICS_TOKEN` to require `Authorization: Bearer <token>` on every
scrape; a request without the correct token then gets **404** (not 401, so an
unauthenticated prober cannot learn that the endpoint exists). `/health` and
`/ready` are never gated — load-balancer probes keep working without the
token. See [Deployment: Protecting /metrics](deployment.md#protecting-metrics).

```bash
curl http://localhost:7000/metrics

# With the token gate armed:
curl -H "Authorization: Bearer $PYLON_METRICS_TOKEN" http://localhost:7000/metrics
```

### Metrics Reference

All series use cardinality-safe labels only: `worker` (integer index), `app`
(app ID string), `status` (`ok` / `failed`). There are **no per-channel
labels**.

#### Process

| Series | Type | Labels | Description |
|---|---|---|---|
| `pylon_up` | gauge | — | Always `1`; confirms the process is alive and the scrape succeeded |

#### Per-App (emitted for each app with ≥1 tracked connection)

| Series | Type | Labels | Description |
|---|---|---|---|
| `pylon_connections` | gauge | `app` | Live WebSocket connections for the app |
| `pylon_channels_occupied` | gauge | `app` | Channels with at least one subscriber |
| `pylon_subscriptions` | gauge | `app` | Total channel subscriptions across all connections |

#### Per-Worker Transport

| Series | Type | Labels | Description |
|---|---|---|---|
| `pylon_accepted_connections_total` | counter | `worker` | Cumulative connections accepted by each worker since start |
| `pylon_broadcast_dropped_total` | counter | `worker` | Broadcasts dropped because the worker hand-off channel was full |
| `pylon_codel_dropped_total` | counter | `worker` | Frames discarded by the CoDel staleness check (stale frames removed from the queue before sending) |
| `pylon_drophead_dropped_total` | counter | `worker` | Frames evicted by the per-connection drop-head queue: when a slow consumer's outbound queue is at its byte cap, the oldest queued frames are dropped to make room for newer ones (freshest-wins) |
| `pylon_mailbox_dropped_total` | counter | `worker` | Frames dropped because a connection's inbound mailbox (the bounded direct-send channel for presence rosters, member events, user-targeted sends, watchlist notifications, cluster deliveries) was full when a producer tried to enqueue |
| `pylon_inflight_bytes` | gauge | `worker` | Bytes currently queued in each worker's outbound buffer |
| `pylon_inflight_bytes_sum` | gauge | — | Sum of `pylon_inflight_bytes` across all workers |
| `pylon_worker_budget_bytes` | gauge | — | Per-worker memory budget in bytes |
| `pylon_budget_factor` | gauge | — | PSI memory-pressure budget factor. A background loop polls kernel memory pressure (`full avg10`) once per second; above `PYLON_PSI_THRESHOLD` (default 15%) each worker's effective budget shrinks toward a 0.8 floor and recovers toward 1.0 when pressure clears. Steady-state range is **0.8–1.0** — a sustained value below 0.9 indicates real memory pressure, not queue backlog |
| `pylon_saturation_flag` | gauge | — | `1` if the broadcast pipeline is saturated, `0` otherwise; omitted when the saturation monitor is not running |

#### Webhook Pipeline

| Series | Type | Labels | Description |
|---|---|---|---|
| `pylon_webhook_enqueued_total` | counter | — | Webhook events successfully placed in the delivery mailbox |
| `pylon_webhook_dropped_total` | counter | — | Webhook events dropped because the mailbox was full or closed |
| `pylon_webhook_delivered_total` | counter | `status` | Completed delivery attempts; `status="ok"` or `status="failed"` |
| `pylon_webhook_queue_depth` | gauge | — | Current number of events waiting in the webhook mailbox |

#### Cluster / Redis (only present on the Redis clustering path)

| Series | Type | Labels | Description |
|---|---|---|---|
| `pylon_cluster_cmd_dropped_total` | counter | — | `ClusterCmd` messages dropped on a full bridge channel |
| `pylon_redis_connected` | gauge | — | `1` = Redis connection healthy; `0` = error/disconnected |

---

### Prometheus Scrape Config

Add pylon as a scrape target in `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: pylon
    static_configs:
      - targets:
          - "pylon-host-1:7000"
          - "pylon-host-2:7000"
    # No auth needed unless PYLON_METRICS_TOKEN is set; restrict at the
    # network layer regardless.
    # authorization:
    #   type: Bearer
    #   credentials: ${PYLON_METRICS_TOKEN}
```

For Kubernetes, use a `ServiceMonitor` (Prometheus Operator) pointing at port
`7000` with path `/metrics`.

---

### Grafana

Import the series above into Grafana dashboards. Useful panel ideas:

- **Connection density**: `pylon_connections{app="…"}` per app, plus
  `sum(pylon_inflight_bytes_sum)` for buffer pressure.
- **Drop rates**: rate of `pylon_broadcast_dropped_total`,
  `pylon_codel_dropped_total`, `pylon_drophead_dropped_total`, and
  `pylon_mailbox_dropped_total` — non-zero values indicate backpressure
  (drop-head evictions mean slow consumers are losing their oldest queued
  frames; mailbox drops mean direct sends to a connection overran its
  inbound mailbox).
- **Webhook health**: `pylon_webhook_dropped_total` rate and
  `pylon_webhook_queue_depth` — rising queue depth signals a slow upstream.
- **Redis health**: `pylon_redis_connected` as a status panel; alert on `< 1`.
- **Memory pressure**: `pylon_budget_factor` — alert when sustained below 0.9
  (the factor only ever ranges 0.8–1.0; it tracks kernel PSI memory pressure,
  not queue depth).

---

## `GET /health` — Liveness Probe

```
200 OK   body: ok
```

Always returns `200 ok` as long as the process can handle HTTP requests. Use
this as a **liveness probe**: if it fails, restart the container.

```bash
curl -f http://localhost:7000/health
```

Kubernetes liveness probe:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 7000
  initialDelaySeconds: 5
  periodSeconds: 10
```

Also available at `/healthz`.

---

## `GET /ready` — Readiness Probe

| Status | Body | Meaning |
|---|---|---|
| `200 OK` | `ready` | Workers up and not draining — safe to route traffic here |
| `503 Service Unavailable` | `draining` | Shutdown in progress; stop routing new connections |
| `503 Service Unavailable` | `starting` | Workers not yet initialised |

Use this as a **readiness probe**: the load balancer or k8s controller stops
sending new connections to a node when it returns non-200, which is exactly what
you want during a [graceful restart](production-tuning.md#graceful-restart).

```bash
curl -f http://localhost:7000/ready
```

Kubernetes readiness probe:

```yaml
readinessProbe:
  httpGet:
    path: /ready
    port: 7000
  initialDelaySeconds: 3
  periodSeconds: 5
  failureThreshold: 2
```

Also available at `/readyz`.

!!! tip "Load-balancer health checks"
    Point your load balancer's health check at `/ready` (not `/health`). This
    ensures that draining nodes are removed from the rotation before their
    connections are closed with [Pusher code 4200](troubleshooting.md#close-codes).
