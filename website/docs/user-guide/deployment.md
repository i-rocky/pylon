# Deployment

Pylon ships deploy artifacts for three targets. Choose the tab that matches your environment.

=== "Bare metal / systemd"

    Systemd is the primary deployment target. The artifacts in `deploy/systemd/` are
    production-grade and include a hardened unit file, kernel tuning, and an annotated
    environment-variable template.

    ### Files

    | File | Purpose |
    |---|---|
    | `deploy/systemd/pylon.service` | systemd unit — runs pylon as the `pylon` system user, sets `LimitNOFILE=2000000`, handles graceful shutdown via `SIGTERM`. |
    | `deploy/systemd/99-pylon.sysctl.conf` | Kernel tuning drop-in — TCP buffer sizes, `somaxconn`, `fs.file-max`, `fs.nr_open`, and `tcp_migrate_req` for millions of idle WebSocket connections. |
    | `deploy/systemd/pylon.env.example` | Template environment file — all variables documented inline; copy to `/etc/pylon/pylon.env` and edit. |
    | `deploy/systemd/apps.example.json` | Sample apps.json — change the `secret` field before use. |

    ### Install steps

    **1. Apply kernel tuning (once per host, as root):**

    ```bash
    cp deploy/systemd/99-pylon.sysctl.conf /etc/sysctl.d/
    sysctl --system
    ```

    The drop-in sets `fs.nr_open = 20000500`, which must be ≥ `LimitNOFILE` in
    the service unit (`2000000`). It also shrinks per-socket TCP buffer sizes to
    save ~15 KiB RAM per idle WebSocket connection.

    **2. Build the binary:**

    ```bash
    cargo build --release
    # Produces: target/release/pylon
    ```

    **3. Create the service account and install files:**

    ```bash
    # Create the system user.
    useradd --system --no-create-home --shell /sbin/nologin pylon

    # Install the binary.
    install -m 0755 target/release/pylon /usr/local/bin/pylon

    # Create the config directory (owned root, readable by pylon group).
    install -d -m 0750 -o root -g pylon /etc/pylon

    # Install and edit the environment file.
    install -m 0640 -o root -g pylon \
        deploy/systemd/pylon.env.example /etc/pylon/pylon.env
    # Edit /etc/pylon/pylon.env — set adapter, Redis URL, etc.

    # Install the apps config.
    install -m 0640 -o root -g pylon \
        deploy/systemd/apps.example.json /etc/pylon/apps.json
    # IMPORTANT: change the "secret" field in apps.json.

    # Install the systemd unit.
    cp deploy/systemd/pylon.service /etc/systemd/system/
    systemctl daemon-reload
    ```

    **4. Enable and start:**

    ```bash
    systemctl enable --now pylon
    ```

    ### Day-2 operations

    ```bash
    # Status
    systemctl status pylon

    # Graceful restart (SIGTERM → drain → restart)
    systemctl restart pylon

    # Tail logs
    journalctl -u pylon -f

    # Health check
    curl -s http://localhost:7000/health
    curl -s http://localhost:7000/ready
    ```

    ### Redis adapter (multi-node)

    To run multiple nodes behind a load balancer, edit `/etc/pylon/pylon.env` on
    every host:

    ```env
    PYLON_ADAPTER=redis
    PYLON_REDIS_URL=redis://your-redis-host:6379
    ```

    If Redis runs on the same host, uncomment the `Requires=redis.service` lines in
    `pylon.service`. See [Clustering & Scaling](clustering.md) for the full
    multi-node setup guide.

    ### Graceful shutdown

    The unit sets `KillSignal=SIGTERM` and `TimeoutStopSec=20`. On `systemctl stop`
    or `systemctl restart`, pylon:

    1. Flips `/ready` to 503 immediately (LB stops sending new connections).
    2. Waits `PYLON_SHUTDOWN_PREDRAIN_MS` (default 2 s).
    3. Sends WebSocket Close (1001) to all connections.
    4. Flushes up to `PYLON_SHUTDOWN_GRACE_MS` (default 10 s).
    5. Exits.

    Worst-case drain is ~12 s. The 20 s `TimeoutStopSec` provides slack before
    systemd force-kills the process.

    !!! tip "Host file-descriptor limits"
        `LimitNOFILE=2000000` covers 1 M WebSocket connections plus headroom for
        epoll descriptors, timer fds, and sockets. Host `fs.nr_open` must be at
        least this value — the `99-pylon.sysctl.conf` drop-in ensures it.
        Connection-count and memory-budget tuning are covered in Production Tuning.

=== "Docker / Compose"

    ### Published image

    A multi-arch image (`linux/amd64` + `linux/arm64`) is published on each release:

    ```
    ghcr.io/i-rocky/pylon:latest
    ghcr.io/i-rocky/pylon:X.Y.Z   # pinned release
    ghcr.io/i-rocky/pylon:X.Y     # floating minor
    ```

    ### Single-node quick start

    ```bash
    docker run -d --name pylon -p 7000:7000 \
      -v "$PWD/apps.json:/etc/pylon/apps.json:ro" \
      -e PYLON_APPS_PATH=/etc/pylon/apps.json \
      --ulimit nofile=1048576:1048576 \
      ghcr.io/i-rocky/pylon:latest
    ```

    Volume-mount your `apps.json` at `/etc/pylon/apps.json` and pass the path
    via `PYLON_APPS_PATH`. The `--ulimit` flag raises the file-descriptor limit
    for the container.

    ### Host prerequisites

    Apply the kernel tuning drop-in on the Docker host before starting containers:

    ```bash
    cp deploy/systemd/99-pylon.sysctl.conf /etc/sysctl.d/
    sysctl --system
    ```

    Ensure the Docker daemon allows high nofile limits by adding to
    `/etc/docker/daemon.json`:

    ```json
    {
      "default-ulimits": {
        "nofile": {"Name": "nofile", "Hard": 2000000, "Soft": 2000000}
      }
    }
    ```

    Restart the Docker daemon after editing this file.

    ### Two-node Compose cluster

    `deploy/docker/docker-compose.yml` starts Redis 7, `pylon-1` (host port 7000),
    and `pylon-2` (host port 7001), all sharing the same `apps.json` and using the
    redis adapter.

    ```bash
    # Copy and edit the apps config — change the secret!
    cp deploy/systemd/apps.example.json deploy/docker/apps.json

    # Build and start.
    cd deploy/docker
    docker compose up -d --build

    # Verify both nodes.
    docker compose ps
    curl -s http://localhost:7000/health
    curl -s http://localhost:7001/health
    ```

    ### Rolling update

    ```bash
    docker compose up -d --no-deps --build pylon-1
    # Wait for pylon-1 to become healthy, then:
    docker compose up -d --no-deps pylon-2
    ```

    Each node's `stop_grace_period: 20s` in the compose file ensures the full
    drain cycle completes before Docker kills the container.

    !!! note "Health check uses wget"
        The healthcheck in `docker-compose.yml` uses `wget --spider` because
        `debian:bookworm-slim` (the runtime base image) does not include `curl`.
        The `Dockerfile` installs `wget` into the runtime stage for exactly this purpose.

=== "Kubernetes / Helm"

    The Helm chart is at `deploy/helm/pylon`. It packages a `Deployment`,
    `Service`, apps `ConfigMap`, liveness/readiness probes, a
    `HorizontalPodAutoscaler` (opt-in), and security contexts.

    ### Node-level prerequisites

    Kubernetes cannot apply most `net.*` sysctls per-pod. Before deploying pylon,
    apply `deploy/systemd/99-pylon.sysctl.conf` to **every node** in the cluster
    and ensure the container runtime allows `nofile ≥ 2 000 000`. See the comment
    block at the bottom of `deploy/helm/pylon/values.yaml` for runtime-specific
    instructions (containerd, docker-shim).

    ### Install

    ```bash
    # Single-node (local adapter, default):
    helm install pylon ./deploy/helm/pylon

    # Multi-node cluster (redis adapter):
    helm install pylon ./deploy/helm/pylon \
      --set config.adapter=redis \
      --set config.redisUrl=redis://my-redis:6379 \
      --set replicaCount=3
    ```

    ### Key values

    | Value | Default | Purpose |
    |---|---|---|
    | `replicaCount` | `2` | Number of pylon pods. |
    | `config.adapter` | `local` | `local` or `redis`. Must be `redis` for `replicaCount > 1`. |
    | `config.redisUrl` | `""` | Redis connection URL (required when `adapter=redis`). |
    | `config.redisPrefix` | `pylon` | Redis key prefix. |
    | `config.workers` | `0` | Worker threads. `0` = one per CPU. |
    | `config.memoryBudgetBytes` | `0` | Memory cap in bytes. `0` = auto. |
    | `autoscaling.enabled` | `false` | Enable the HPA. |
    | `autoscaling.minReplicas` | `2` | Minimum replicas when HPA is active. |
    | `autoscaling.maxReplicas` | `10` | Maximum replicas when HPA is active. |
    | `resources.requests.memory` | `512Mi` | Pod memory request. |
    | `resources.limits.memory` | `8Gi` | Pod memory limit. |

    ### Autoscaling

    ```bash
    helm upgrade pylon ./deploy/helm/pylon \
      --set autoscaling.enabled=true \
      --set autoscaling.minReplicas=2 \
      --set autoscaling.maxReplicas=10
    ```

    ### Graceful rollout

    The Deployment template sets `terminationGracePeriodSeconds: 30` and uses a
    rolling update strategy with `maxUnavailable: 0` to keep the full replica count
    serving traffic during a rollout. The readiness probe (`GET /ready`) removes a
    pod from Service endpoints as soon as it enters the drain phase.

    ### TLS (Ingress)

    Terminate TLS at the Ingress controller using cert-manager. The pylon pods and
    Service stay on plain HTTP. See [TLS / SSL](tls.md) for the full Ingress
    manifest with the required WebSocket and timeout annotations.

    ### Apps config security

    The `apps` list in `values.yaml` is rendered into a ConfigMap in plain text.
    **Change the `secret` field from `CHANGE_ME` before deploying.** For production,
    use a Kubernetes Secret or an external secret manager and mount `apps.json` as a
    file rather than embedding credentials in the ConfigMap.

    For the full Helm values reference see `deploy/helm/pylon/values.yaml` and
    `deploy/README.md`.

---

## Health probes

All deployment targets expose the same two HTTP probes on the pylon port:

| Probe | Path | Healthy | Draining / starting |
|---|---|---|---|
| Liveness | `GET /health` | 200 "ok" | 200 "ok" (always) |
| Readiness | `GET /ready` | 200 "ready" | 503 "draining" or "starting" |

Configure your load balancer or Kubernetes `readinessProbe` to use `/ready`.
This ensures that a node in the pre-drain window stops receiving new connections
before its existing connections are closed.

!!! tip "Connection-count and memory tuning"
    File-descriptor limits (`LimitNOFILE`, `--ulimit nofile`, container runtime
    settings) and kernel TCP buffer tuning are covered in Production Tuning. Set
    `PYLON_MEMORY_BUDGET_BYTES` to cap memory consumption — see
    [Configuration](configuration.md) for the full variable reference.
