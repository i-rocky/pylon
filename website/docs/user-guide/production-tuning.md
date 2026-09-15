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

## Flood protection and rate limits

Every limit below defaults to off, because the right number is a property of
your hardware, not of pylon. Measure it first with the capacity finder, which
spawns a core-pinned pylon child and sweeps both axes to their real ceilings on
the machine you will deploy on. Pass `--tput-conns`, `--channels` and
`--max-rate` explicitly — the defaults are sized for a quick smoke test, not
for finding a production ceiling:

```sh
cargo run -p pylon-load --release --bin pylon-ceiling -- \
  --phase both --tput-conns 20000 --channels 2000 --max-rate 20000 --json
```

Its **connection phase** reports the maximum sustainable connection count
(`conn_ceiling.max_conns`), RSS at that count, bytes per connection and
connections per GB. Its **throughput phase** ramps the publish rate until
deliveries drop, p99 exceeds `--p99-budget-ms` (default 100 ms) or the CPU
saturates, and stops at the last *clean* requested rate — `tput_ceiling.best.rate`.

`best.rate` is not what the server delivered. The sweep's open-loop publisher
sheds a tick whenever its `--max-inflight` window is full, and `drop_pct` is
computed against *attempted* publishes, i.e. after shedding — so shedding
never shows up as a drop, and the sweep keeps climbing while the achieved rate
underneath it rises far more slowly than the requested rate. Derive `R` from
what was delivered instead:

```
R = best.delivered_per_s ÷ (--tput-conns ÷ --channels)
```

Neither `--tput-conns` nor `--channels` appears in `pylon-ceiling`'s own
report — only `rate`, `delivered_per_s`, `drop_pct`, the latencies,
`cpu_busy_pct` and `stop_reason` do. Pass both explicitly, as in the command
above, so the fan-out is a number you chose. Reading a run that left
`--tput-conns` at its default (`0`) instead: it resolved to
`min(conn_ceiling.max_conns, 50000)` when the connection phase ran, or
`10000` when it did not — read that from your own invocation, never by
dividing by `conn_ceiling.max_conns` in the JSON. On a box where `max_conns`
is 200,000, the auto-resolution still caps `--tput-conns` at 50,000, so
dividing by `max_conns` instead gives a fan-out four times too large and an
`R` four times too low.

`--tput-conns ÷ --channels` is the fan-out per published event — how many
subscribers each publish reaches. Worked example, measured on a 2 vCPU arm64
box: a step requesting 5,000/s delivered 49,229/s; a later step requesting
17,000/s delivered 63,281/s. Both ran with `--tput-conns` ten times
`--channels`, so the achieved publish rate was 49,229 ÷ 10 = 4,923/s and
63,281 ÷ 10 = 6,328/s — nowhere near the 5,000 and 17,000 the sweep reports as
`best.rate`. Compute the same quotient from your own run's
`best.delivered_per_s` and your own `--tput-conns ÷ --channels`; that is `R`.

!!! warning "Quote `--max-inflight` and `--p99-budget-ms` with every throughput number"
    `--max-inflight` is itself a throughput knob, not just a safety valve: at a
    fixed requested rate of 12,000 on the box above, a window of 256 delivered
    51,367/s at p99 99 ms, 1,024 delivered 60,125/s at p99 345 ms, and 4,096
    delivered 77,304/s at p99 1,063 ms. A bare "R publishes/s" figure is
    meaningless without the `--max-inflight` and `--p99-budget-ms` it was
    measured at — including a later run of your own against a different
    window.

Take `C` (max connections, `conn_ceiling.max_conns`) and `R` (achieved
publishes per second, derived above) and set:

| Variable | Suggested value | Why |
|---|---|---|
| `PYLON_MAX_ACCEPTS_PER_SECOND` | `C / 60` | Refills the node's full connection population in about a minute, so a fleet-wide reconnect storm is spread rather than absorbed in one spike. |
| `PYLON_MAX_REST_REQUESTS_PER_SECOND` | `R × 1.5` | Above the measured publish ceiling, so the cap bites only on a genuine flood and never on healthy traffic. |
| `PYLON_MAX_BACKEND_EVENTS_PER_SECOND` | `R / (expected apps)` | One tenant cannot spend the whole node's publish budget. Raise per app with `max_backend_events_per_second`. |
| `PYLON_MAX_READ_REQUESTS_PER_SECOND` | `100` | `GET /channels` walks the channel registry; reads are far rarer than publishes in a healthy integration. |
| `PYLON_MAX_FRAMES_PER_SECOND` | `100` (default) | Ten times the Pusher client-event ceiling, so control frames and subscribes have ample headroom while a Ping flood does not. |
| `PYLON_MAX_FRAMES_BURST` | `250` (default) | Absorbs a client's opening subscribe storm. Auto-raised to `PYLON_MAX_SUBSCRIPTIONS_PER_CONNECTION + 50` unless you set it yourself — set it below that and pylon refuses to start. |

The node cap runs **before** authentication, so an unsigned flood costs no
app-store lookup; `/health`, `/ready`, `/metrics` and the admin API are never
limited, because an operator has to reach them during exactly the flood this
bounds. The per-app caps run after authentication and are overridable per app
(see [Applications & Authentication](applications.md)); a `POST /batch_events`
costs its event count, so a non-zero `PYLON_MAX_BACKEND_EVENTS_PER_SECOND` below
`PYLON_MAX_BATCH_EVENTS` is refused at startup — a full-size batch could never
be afforded. A per-app `max_backend_events_per_second` is not validated against
the batch cap, so check that one yourself.

Every one of these limits is enforced **per node**, with no cluster
coordination — unlike an app's `capacity`, which the Redis adapter enforces
cluster-wide. The numbers above are per-node numbers because the ceilings
`pylon-ceiling` measures are per-node ceilings; a fleet of N nodes behind a
load balancer therefore jointly allows `N ×` each value. If you need a
cluster-wide ceiling, divide by your node count — and remember that a balancer
spreading traffic unevenly will trip one node's cap before the fleet's share is
used up.

Watch `pylon_rest_rate_limited_total`, `pylon_frame_limited_total` and
`pylon_accept_limited_total` after turning any of them on: a non-zero value in
steady state means the limit is below your real traffic, not that you are under
attack.

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
