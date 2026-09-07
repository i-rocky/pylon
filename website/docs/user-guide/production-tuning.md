# Production Tuning

This page covers OS-level and pylon-specific settings to maximise connection
density and reliability in production. For deployment artefacts (systemd unit,
Docker Compose, etc.) see [Deployment](deployment.md).

---

## Open File Descriptors

Every WebSocket connection is a file descriptor. The Linux default of 1 024 fds
per process is far too low for any production deployment.

### Quick check

```bash
ulimit -n          # current soft limit for this shell
cat /proc/sys/fs/file-max   # system-wide kernel ceiling
```

### Raise the limit

=== "systemd (recommended)"

    In your pylon service unit:

    ```ini
    [Service]
    LimitNOFILE=2000000
    ```

    Then `systemctl daemon-reload && systemctl restart pylon`.

=== "/etc/security/limits.conf"

    ```
    pylon  soft  nofile  2000000
    pylon  hard  nofile  2000000
    ```

    Effective for the `pylon` user on next login / service start.

=== "Docker"

    ```bash
    docker run --ulimit nofile=2000000:2000000 …
    ```

    Or in Compose:

    ```yaml
    services:
      pylon:
        ulimits:
          nofile:
            soft: 2000000
            hard: 2000000
    ```

=== "shell (testing only)"

    ```bash
    ulimit -n 2000000
    ```

!!! warning "Kernel ceiling must also be high"
    `fs.nr_open` (per-process kernel ceiling) must be ≥ your target fd count.
    `LimitNOFILE` cannot exceed it. The sysctl file below sets `fs.nr_open =
    20000500`.

---

## Kernel sysctl Settings

Apply `deploy/systemd/99-pylon.sysctl.conf` to persist the recommended kernel
parameters across reboots:

```bash
sudo cp deploy/systemd/99-pylon.sysctl.conf /etc/sysctl.d/
sudo sysctl --system
```

For a full explanation of every setting see
[`docs/ops/sysctl-tuning.md`](https://github.com/i-rocky/pylon/blob/master/docs/ops/sysctl-tuning.md)
in the repository. Key highlights:

| Setting | Recommended value | Purpose |
|---|---|---|
| `net.core.somaxconn` | `65535` | Accept-queue depth; prevents silent drops during connect bursts |
| `net.ipv4.tcp_max_syn_backlog` | `65535` | Half-open SYN queue |
| `net.ipv4.tcp_rmem` / `tcp_wmem` | `1024 4096 16384` | Tiny socket-buffer floors (~3.2 KB/connection kernel floor) |
| `net.ipv4.tcp_mem` | `10000000 10000000 10000000` | System-wide TCP memory (~40 GB headroom at 4 KB/page) |
| `fs.file-max` | `12000500` | System-wide fd ceiling |
| `fs.nr_open` | `20000500` | Per-process fd ceiling |
| `net.ipv4.tcp_migrate_req` | `1` | Migrate SYNs on `SO_REUSEPORT` socket churn (Linux ≥ 5.14) |

---

## Ephemeral Port Range

The ephemeral port range limits **outbound** connections originating from the
server (load-test clients, Redis connections, webhook delivery). It does **not**
limit incoming WebSocket connections, which are all accepted on the single
`PYLON_PORT`.

Check and widen if needed:

```bash
cat /proc/sys/net/ipv4/ip_local_port_range
# e.g. 32768 60999  →  ~28 000 outbound ports

# Widen (add to /etc/sysctl.d/ to persist):
net.ipv4.ip_local_port_range = 1024 65535
```

For extreme local fan-out scenarios (e.g. load-testing from a single host),
spread connections across multiple client IPs bound to different network
interfaces or IP aliases.

---

## Capacity and Memory Budget

### Workers

Pylon auto-detects the number of CPU cores and starts one **pinned OS thread
per core, each running its own `mio` event loop** with its own
`SO_REUSEPORT` listener (the kernel shards accepts across them). Tokio drives
only the control plane — the REST API, webhooks, and the Redis adapter — not
the WebSocket hot path. Set `PYLON_WORKERS` to override:

```bash
PYLON_WORKERS=8 pylon
```

### Memory Budget

Pylon reads the available memory from the host or cgroup and divides it evenly
among workers. A rough planning constant: **≈3.2 KB of unswappable kernel
memory per idle connection** (socket buffer floors; see sysctl section above)
plus a few KB of application-level state per connection.

By default the budget is the effective envelope minus an OS reserve of
`max(1.5 GiB, 7 %)`, **and that reserve is capped at half the envelope**:

```
budget = envelope − min( max(1.5 GiB, 7% of envelope), 50% of envelope )
```

The cap matters only below the ~3 GiB crossover, where the flat 1.5 GiB floor
would otherwise claim more than half the envelope — and at or below 1.5 GiB
would claim all of it, leaving a budget of zero. A 1 GiB container now gets a
512 MiB budget rather than none. Above ~3 GiB the cap never binds, so larger
hosts are arithmetically unchanged (4 GiB → 2.5 GiB, 256 GiB → 238 GiB).

!!! danger "A budget of zero disables every overload control"
    A resolved budget of `0` is read downstream as "unconfigured": the
    graduated shed pins to its normal band, which disables the REST `503`
    admission gate, the `client-*` ingress drop, and the subscribe-time
    pressure gate — and the node connection ceiling becomes unlimited. Pylon
    now logs a startup `warn` naming exactly which controls are off whenever
    the resolved budget is still `0`, so this can no longer happen silently.
    If you see that warning, set `PYLON_MEMORY_BUDGET_BYTES` explicitly.

Override the budget with environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `PYLON_MEMORY_BUDGET_BYTES` | `0` | Total budget across all workers in bytes. Takes precedence over everything else; `0` means "not set" |
| `PYLON_MEMORY_BUDGET_FRACTION` | `0.0` | Budget as a fraction of effective (host/cgroup) memory, range 0.0–1.0. Applied only when `PYLON_MEMORY_BUDGET_BYTES` is `0`; `0.0` means "use the reserve formula above" |

When a worker's inflight queue approaches its budget, pylon applies
backpressure (per-connection drop-head eviction and CoDel drops) and sheds new
subscriptions and client events. The `pylon_budget_factor` metric reflects
**kernel memory pressure** (PSI `full avg10`), not queue utilisation: a
background loop polls PSI once a second and scales each worker's effective
budget down toward a `0.8` floor while pressure exceeds
`PYLON_PSI_THRESHOLD` (default 15%), ramping back toward `1.0` when it
clears — so the metric's steady-state range is **0.8–1.0**, and a sustained
value below `0.9` means the host is genuinely under memory pressure.

### Admission control under overload

The budget is not only a shedding input — it drives a node-wide **saturation
signal** that rejects work outright. Each worker raises its budget-pressure bit
at **100 %** of its share of the budget and releases it below **80 %**; while
any worker holds it (or a publisher has found a worker's broadcast hand-off
full), the node:

- answers `POST /apps/{id}/events` and `/batch_events` with **`503`** and
  `Retry-After: 1`,
- refuses **new** subscriptions with a non-fatal `pusher:subscription_error`
  (`LimitReached`, `4004`),
- **silently drops** inbound `client-*` events, and
- refuses new connections with close code **`4100`**.

!!! warning "New in this release: these actually fire now"
    The node-wide flag was previously cleared unconditionally on every worker
    loop, so none of these responses could engage. After upgrading, a node that
    was already running past its budget will begin returning 503s and dropping
    client events where it silently queued them before. Budget for this before
    a rolling upgrade, and see
    [Troubleshooting — Overload](troubleshooting.md#overload) for how to
    diagnose and respond.

Graduated shedding runs below the saturation point and is unchanged: above
80 % of budget a broadcast skips subscribers whose own out-queue is more than
half full, and above 95 % it skips any subscriber that is non-trivially backed
up.

### Per-App Connection Cap

Set a per-app connection ceiling in your app configuration (`apps.json` —
the same file `PYLON_APPS_PATH` points at):

```json
[
  {
    "id": "my-app",
    "key": "…",
    "secret": "…",
    "capacity": 10000
  }
]
```

Connections beyond `capacity` are closed with Pusher error **4004** (over
capacity). See [Applications & Authentication](applications.md).

---

## Graceful Restart

Pylon supports bounded-drain restarts when used with a process manager:

1. Send `SIGTERM` to the running process.
2. Pylon stops accepting new connections and sets `/ready` to `503 draining`.
3. The load balancer / k8s controller detects the 503 and stops routing new
   traffic here.
4. Existing connections are closed with Pusher close code **4200** ("server
   restarting — reconnect immediately") within `PYLON_SHUTDOWN_GRACE_MS`
   milliseconds (default: 10 000 ms).
5. The process exits cleanly; the process manager starts the new binary.

What the drain guarantees — and what it does not: each worker keeps flushing
frames already **queued** for a connection until its outbound queue empties or
the grace deadline passes, so in-flight delivery is not truncated mid-stream.
Clients then reconnect (immediately, per the 4200 band) to the new process.
**Events triggered during the restart gap are not replayed** — a publish that
arrives while no node holds the channel's subscribers is simply delivered to
whoever is subscribed at that moment, and a client that reconnects after the
gap does not receive events from before its re-subscribe. If your workload
needs gap coverage, drain with multiple instances behind the load balancer
(restart one at a time) so surviving nodes hold the subscriptions.

```bash
# systemd rolling restart
systemctl restart pylon
```

The unit ships no `ExecReload`, so there is no `systemctl reload` — restart is
the supported operation. Pylon's two-phase drain (503 on `/ready`, then a
bounded 4200 close) is what makes the restart rolling rather than dropped;
for zero-downtime at the front, run multiple instances behind a load balancer
that health-checks `/ready` and restart them one at a time.

Set `PYLON_SHUTDOWN_GRACE_MS` to allow enough time for slow consumers to drain
their queued frames before the old process exits. Pusher.js reconnects
immediately on a 4200 close, but note the boundary above: the grace window
bounds queue flushing, not delivery of events published during the gap.

See [Observability](observability.md) for how to use `/ready` as a load-balancer
health check.
