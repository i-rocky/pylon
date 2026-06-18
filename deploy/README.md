# pylon deploy

Deploy artifacts for pylon — a high-performance Pusher-compatible WebSocket server.

Targets, in order of priority:

1. **Bare metal / systemd** (primary)
2. **Docker / Compose** (two-node cluster)
3. **Kubernetes / Helm**

---

## Health probes

All deployment targets rely on the same two HTTP endpoints:

| Probe | Path | Normal | Draining / starting |
|-------|------|--------|---------------------|
| Liveness | `GET /health` | 200 "ok" | 200 "ok" (always) |
| Readiness | `GET /ready` | 200 "ready" | 503 "draining" or "starting" |

During a graceful shutdown pylon flips `/ready` to 503 first, giving load
balancers time to stop routing new connections, then drains existing ones.

Shutdown timeline:

```
SIGTERM
  └─ /ready → 503 immediately
  └─ sleep PYLON_SHUTDOWN_PREDRAIN_MS (default 2 s)   ← LB drains here
  └─ send WS Close (1001) to all connections
  └─ flush up to PYLON_SHUTDOWN_GRACE_MS (default 10 s)
  └─ exit
```

Worst-case drain: ~12 s. All stop-timeouts in these artifacts are set to **20 s**.

---

## 1. Bare metal / systemd (quick-start)

### Prerequisites

**Kernel tuning** (run once per host, as root):

```bash
cp deploy/systemd/99-pylon.sysctl.conf /etc/sysctl.d/
sysctl --system
```

The sysctl drop-in mirrors `scripts/tune.sh` and sets TCP buffer sizes,
`somaxconn`, `fs.file-max`, `fs.nr_open`, and `tcp_migrate_req` for millions
of idle WebSocket connections.

The `pylon.service` unit sets `LimitNOFILE=2000000` for the process. The host
`fs.nr_open` (20 000 500) must be ≥ this value, which the drop-in ensures.

### Build the binary

```bash
cargo build --release
# Produces: target/release/pylon
```

### Install

```bash
# 1. Create the service user.
useradd --system --no-create-home --shell /sbin/nologin pylon

# 2. Install the binary.
install -m 0755 target/release/pylon /usr/local/bin/pylon

# 3. Create the config directory.
install -d -m 0750 -o root -g pylon /etc/pylon

# 4. Install and edit the environment file.
install -m 0640 -o root -g pylon \
    deploy/systemd/pylon.env.example /etc/pylon/pylon.env
# Edit /etc/pylon/pylon.env — set adapter, redis URL, etc.

# 5. Install the apps config.
install -m 0640 -o root -g pylon \
    deploy/systemd/apps.example.json /etc/pylon/apps.json
# IMPORTANT: change the "secret" field in apps.json.

# 6. Install the systemd unit.
cp deploy/systemd/pylon.service /etc/systemd/system/
systemctl daemon-reload

# 7. Enable and start.
systemctl enable --now pylon
```

### Operations

```bash
# Status
systemctl status pylon

# Graceful restart (SIGTERM → drain → restart)
systemctl restart pylon

# Logs
journalctl -u pylon -f

# Manual probe check
curl -s http://localhost:7000/health
curl -s http://localhost:7000/ready
```

### Redis adapter (multi-node)

In `/etc/pylon/pylon.env`:

```env
PYLON_ADAPTER=redis
PYLON_REDIS_URL=redis://your-redis-host:6379
```

Uncomment the `Requires=redis.service` lines in `pylon.service` if Redis runs
on the same host.

---

## 2. Docker / Compose

### Published image

A ready-to-use multi-arch image (`linux/amd64` + `linux/arm64`) is published to GHCR on each
release: `ghcr.io/i-rocky/pylon:latest` (also tagged `X.Y.Z` and `X.Y`). Pull it instead of
building from source:

```bash
docker run -d --name pylon -p 7000:7000 \
  -v "$PWD/apps.json:/etc/pylon/apps.json:ro" \
  -e PYLON_APPS_PATH=/etc/pylon/apps.json \
  --ulimit nofile=1048576:1048576 \
  ghcr.io/i-rocky/pylon:latest
```

The published image is built by `.github/workflows/release.yml`, which packages the prebuilt
per-arch binaries into [`Dockerfile.release`](docker/Dockerfile.release). The `Dockerfile` in this
directory builds the same runtime image from source instead.

### Prerequisites (host)

Apply the sysctl drop-in on the Docker host before starting containers:

```bash
cp deploy/systemd/99-pylon.sysctl.conf /etc/sysctl.d/
sysctl --system
```

Ensure the Docker daemon allows high nofile limits:

```json
// /etc/docker/daemon.json
{
  "default-ulimits": {
    "nofile": {"Name": "nofile", "Hard": 2000000, "Soft": 2000000}
  }
}
```

### Start a 2-node cluster

```bash
# Copy and edit the apps config — change the secret!
cp deploy/systemd/apps.example.json deploy/docker/apps.json

# Build the image and start services.
cd deploy/docker
docker compose up -d --build

# Check health.
docker compose ps
curl -s http://localhost:7000/health
curl -s http://localhost:7001/health
```

The compose file starts:
- `redis` — Redis 7 (shared by both pylon nodes)
- `pylon-1` — listens on host port 7000
- `pylon-2` — listens on host port 7001

Both nodes use `PYLON_ADAPTER=redis` and share the same `apps.json`.

### Rolling update (compose)

```bash
docker compose up -d --no-deps --build pylon-1
# wait for pylon-1 healthy, then:
docker compose up -d --no-deps pylon-2
```

Each node's `stop_grace_period: 20s` ensures the full drain completes before
Docker kills the container.

**Note on `wget` in the healthcheck:** `debian:bookworm-slim` does not include
`curl`. The healthcheck uses `wget --spider`. If your image build omits `wget`,
add `wget` to the `apt-get install` line in the Dockerfile runtime stage.

---

## 3. Kubernetes / Helm

### Prerequisites (node-level)

Kubernetes cannot set most `net.*` sysctls per-pod. Apply
`deploy/systemd/99-pylon.sysctl.conf` to **every node** in the cluster, and
ensure the container runtime sets `nofile ≥ 2 000 000`. See the comment block
at the bottom of `deploy/helm/pylon/values.yaml` for options.

### Install

```bash
# Single-node (local adapter, default):
helm install pylon ./deploy/helm/pylon

# Multi-node (redis adapter):
helm install pylon ./deploy/helm/pylon \
  --set config.adapter=redis \
  --set config.redisUrl=redis://my-redis:6379 \
  --set replicaCount=3
```

**Important:** the `apps` list in `values.yaml` is rendered into a ConfigMap
in plain text. For production, use a Kubernetes Secret or an external secret
manager and mount apps.json as a file. Change the `secret` field from
`CHANGE_ME` before deploying.

### Probes (configured in values.yaml)

- `livenessProbe`: `GET /health` — always 200; kubelet restarts the pod if it fails.
- `readinessProbe`: `GET /ready` — 503 during shutdown; kubelet removes the pod from
  Service endpoints before the drain begins.

### Graceful rollout

`terminationGracePeriodSeconds: 30` (in the Deployment template) gives pylon
30 s to complete its drain (predrain 2 s + grace 10 s + slack). The rolling
update strategy (`maxUnavailable: 0`) keeps the full replica count serving
traffic throughout a rollout.

### Autoscaling

Enable the HPA:

```bash
helm upgrade pylon ./deploy/helm/pylon \
  --set autoscaling.enabled=true \
  --set autoscaling.minReplicas=2 \
  --set autoscaling.maxReplicas=10
```

---

## TLS

pylon listens on plain `ws://` and HTTP by default. TLS is optional and can be
added in two ways — choose one.

### Option 1 — Reverse proxy (recommended)

The dominant topology for production, cloud LBs, and Kubernetes: a TLS-
terminating proxy speaks `wss://` to clients while pylon stays plain on its
internal port (default `7000`).

**Caddy** (easiest — auto-HTTPS via Let's Encrypt, no cert management):

```bash
# Install Caddy, then:
caddy run --config deploy/tls/Caddyfile.example
```

Caddy provisions and renews certificates automatically. It requires a real,
publicly resolvable domain and ports 80/443 reachable from the internet. See
`deploy/tls/Caddyfile.example` for the full annotated config.

**nginx** (more explicit, good for existing nginx deployments):

See `deploy/tls/nginx.conf.example`. Key points:
- Obtain a certificate first: `certbot certonly --nginx -d your.domain.example`
- WebSocket proxying requires these three headers (the most common omission):
  ```nginx
  proxy_http_version 1.1;
  proxy_set_header Upgrade    $http_upgrade;
  proxy_set_header Connection "upgrade";
  ```
- Timeouts are set to `3600s` — well above the Pusher heartbeat period of 120 s.
  nginx's default `proxy_read_timeout` of 60 s silently kills idle connections.
- Multiple pylon nodes can be listed in the `upstream pylon {}` block for load
  balancing; the Redis adapter (`PYLON_ADAPTER=redis`) ties them together.

### Option 2 — Native TLS (single-node / no proxy)

Set two environment variables to have pylon serve `wss://` directly:

```env
PYLON_TLS_CERT=/path/to/fullchain.pem
PYLON_TLS_KEY=/path/to/privkey.pem
```

Both must be set together (PEM format). Omit both for plain `ws://`. This is
suitable for single-node deploys without a load balancer in front.

### Kubernetes

Terminate TLS at the **Ingress** using cert-manager + Let's Encrypt. The pod
and Service stay on plain HTTP — no changes to the pylon deployment are needed.

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: pylon
  annotations:
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
    # Raise read timeout above the Pusher heartbeat period (120 s).
    nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
    # Required for WebSocket upgrade on nginx-ingress:
    nginx.ingress.kubernetes.io/proxy-http-version: "1.1"
spec:
  ingressClassName: nginx
  tls:
    - hosts:
        - your.domain.example
      secretName: pylon-tls
  rules:
    - host: your.domain.example
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: pylon
                port:
                  number: 7000
```

cert-manager creates the `pylon-tls` Secret and renews it automatically.

### Keeping /metrics off the public listener

`/metrics` exposes connection counts, Redis lag, and memory stats — it should
not be publicly reachable. Options:
- **Proxy ACL:** uncomment the `location /metrics { allow ... ; deny all; }`
  block in `nginx.conf.example`, or the equivalent matcher in `Caddyfile.example`.
- **Kubernetes:** add an `nginx.ingress.kubernetes.io/whitelist-source-range`
  annotation on a separate Ingress rule for `/metrics`, or use a dedicated
  Prometheus `ServiceMonitor` that scrapes the pod IP directly (bypassing the
  Ingress entirely).

---

## File index

```
deploy/
├── README.md                        This file
├── systemd/
│   ├── pylon.service                systemd unit (primary deploy target)
│   ├── pylon.env.example            Environment variables with comments
│   ├── 99-pylon.sysctl.conf         Kernel tuning drop-in (/etc/sysctl.d/)
│   └── apps.example.json            Sample apps.json (change the secret!)
├── docker/
│   ├── Dockerfile                   Multi-stage build (rust:bookworm → debian-slim)
│   ├── .dockerignore
│   └── docker-compose.yml           2-node cluster + Redis
├── helm/
│   └── pylon/
│       ├── Chart.yaml
│       ├── values.yaml
│       └── templates/
│           ├── _helpers.tpl
│           ├── configmap.yaml       apps.json ConfigMap
│           ├── deployment.yaml      Deployment with probes + grace period
│           ├── service.yaml
│           └── hpa.yaml             HorizontalPodAutoscaler (gated by values)
└── tls/
    ├── Caddyfile.example            Caddy v2 auto-HTTPS reverse proxy
    └── nginx.conf.example           nginx TLS termination with WS proxy
```
