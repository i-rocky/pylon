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

Pylon auto-detects the number of CPU cores and starts one Tokio worker per
core. Set `PYLON_WORKERS` to override:

```bash
PYLON_WORKERS=8 pylon
```

### Memory Budget

Pylon reads the available memory from the host or cgroup and divides it evenly
among workers. A rough planning constant: **≈3.2 KB of unswappable kernel
memory per idle connection** (socket buffer floors; see sysctl section above)
plus a few KB of application-level state per connection.

Override the budget with environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `PYLON_MEMORY_BUDGET_FRACTION` | auto | Budget as a fraction of effective (host/cgroup) memory, range 0.0–1.0; if `PYLON_MEMORY_BUDGET_BYTES` is also set it takes precedence |
| `PYLON_MEMORY_BUDGET_BYTES` | — | Total budget across all workers in bytes (takes precedence over `PYLON_MEMORY_BUDGET_FRACTION`) |

When a worker's inflight queue approaches its budget, pylon applies backpressure
and the `pylon_budget_factor` metric drops toward 0.

### Per-App Connection Cap

Set a per-app connection ceiling in your app configuration:

```toml
[[apps]]
id = "my-app"
key = "…"
secret = "…"
capacity = 10000   # max concurrent WebSocket connections for this app
```

Connections beyond `capacity` are closed with Pusher error **4004** (over
capacity). See [Applications & Authentication](applications.md).

---

## Graceful Restart

Pylon supports zero-dropped-message restarts when used with a process manager:

1. Send `SIGTERM` to the running process.
2. Pylon stops accepting new connections and sets `/ready` to `503 draining`.
3. The load balancer / k8s controller detects the 503 and stops routing new
   traffic here.
4. Existing connections are closed with Pusher close code **4200** ("server
   restarting — reconnect immediately") within `PYLON_SHUTDOWN_GRACE_MS`
   milliseconds (default: 10 000 ms).
5. The process exits cleanly; the process manager starts the new binary.

```bash
# systemd rolling restart
systemctl restart pylon
# or for zero-downtime with socket activation / multiple instances:
systemctl reload pylon
```

Set `PYLON_SHUTDOWN_GRACE_MS` to allow enough time for your clients to
reconnect before the old process exits. Pusher.js will reconnect immediately on
a 4200 close, so even a short grace window avoids message loss.

See [Observability](observability.md) for how to use `/ready` as a load-balancer
health check.
