# Pylon

**Pylon is a self-hostable, Pusher Channels–compatible realtime WebSocket server, written in Rust.**

It speaks the Pusher Channels v7 wire protocol and the Pusher HTTP API, so existing Pusher
client SDKs (pusher-js, Laravel Echo, …) and server SDKs (pusher-http-node, pusher-http-php, …)
work against it unchanged — you just point them at your Pylon host instead of the hosted service.

> Status: actively developed, pre-1.0. The protocol/REST surface is feature-complete against
> hosted Pusher v7; APIs and defaults may still change before 1.0.

## Why Pylon

- **Drop-in replacement** — keep your existing Pusher client/server code; self-host the realtime layer.
- **Full v7 parity** — public, private, presence, encrypted, and cache channels; client events;
  user authentication; webhooks; and the complete Pusher HTTP (REST) API.
- **Built to scale** — a shared-nothing, per-core architecture with an encode-once sharded
  fan-out path and adaptive overload control.
- **Production-minded** — native TLS, Redis-backed clustering, Prometheus metrics, health/readiness
  probes, and graceful shutdown.

## Features

- **Channels** — public, `private-`, `presence-`, `private-encrypted-`, and all cache variants
  (`cache-`, `private-cache-`, `presence-cache-`).
- **Auth** — private/presence subscription auth, user authentication (signin), and client-event
  authorization with per-connection rate limiting.
- **Presence** — member add/remove events, presence rosters, and configurable member/data limits.
- **Webhooks** — `channel_occupied`/`channel_vacated`, `member_added`/`member_removed`,
  `client_event`, `cache_miss`; HMAC-signed and batched.
- **REST API** — trigger (single + batch), channel and presence-user queries, and
  terminate-user-connections, all under the Pusher HTTP authentication scheme.
- **Clustering** — horizontal scale-out across nodes via Redis (presence, user routing/termination,
  and cross-node message fan-out), with automatic recovery across Redis blips.
- **TLS** — native `rustls` (serves `wss://` and REST on the same port), or terminate at a reverse
  proxy (Caddy/nginx examples included).
- **Overload control** — drop-head queues, CoDel, graduated shedding, and a memory budget, so the
  server degrades gracefully instead of collapsing under load.
- **Observability** — Prometheus `/metrics`, plus `/health` (liveness) and `/ready` (readiness).
- **Graceful shutdown** — on `SIGTERM`, connections are drained (with a Pusher `4200`
  reconnect-immediately close) within a bounded window.

## Quick start

```sh
# 1. Configure your app(s)
cp apps.example.json apps.json
#   edit apps.json — set id / key / secret (and optionally webhooks, capacity, …)

# 2. Build and run
cargo run --release
#   Pylon listens on 0.0.0.0:7000 for both WebSocket (ws://) and REST by default.
```

Point a Pusher **client** at it:

```js
const pusher = new Pusher("<your-app-key>", {
  wsHost: "127.0.0.1",
  wsPort: 7000,
  forceTLS: false,
  enabledTransports: ["ws"],
  cluster: "",
});
```

And a Pusher **server** SDK:

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
```

## Configuration

Apps are declared in `apps.json` (see `apps.example.json`). Server behavior is tuned with `PYLON_*`
environment variables. The most common:

| Variable | Default | Purpose |
|---|---|---|
| `PYLON_BIND` | `0.0.0.0` | Bind address |
| `PYLON_PORT` | `7000` | Listen port (WebSocket + REST) |
| `PYLON_APPS_PATH` | `apps.json` | Path to the apps config |
| `PYLON_WORKERS` | `0` (= CPU cores) | Per-core worker threads |
| `PYLON_ADAPTER` | `local` | `local` (single node) or `redis` (clustered) |
| `PYLON_REDIS_URL` | `redis://127.0.0.1:6379` | Redis endpoint (when `adapter=redis`) |
| `PYLON_TLS_CERT` / `PYLON_TLS_KEY` | _(off)_ | Enable native TLS (set both or neither) |
| `PYLON_TLS_CA` | _(off)_ | Require client certificates (mTLS) |
| `PYLON_SHUTDOWN_GRACE_MS` | _(see config)_ | Max drain window on shutdown |

The complete set — overload-control tuning, webhook delivery, protocol limits, and Redis timings —
is enumerated and documented in [`src/server/config.rs`](src/server/config.rs).

## Clustering

Run multiple nodes with `PYLON_ADAPTER=redis` and a shared `PYLON_REDIS_URL`, behind a load
balancer. Presence rosters, user routing/termination, and cross-node fan-out are coordinated through
Redis; nodes resubscribe and self-heal across Redis restarts.

## Deployment

The [`deploy/`](deploy/) directory contains production artifacts:

- **Bare-metal** — a `systemd` unit and `sysctl` tuning (see also [`docs/ops/sysctl-tuning.md`](docs/ops/sysctl-tuning.md)).
- **Containers** — a `Dockerfile` and `docker-compose` example.
- **Kubernetes** — a Helm chart.
- **TLS** — reverse-proxy examples for Caddy (automatic HTTPS) and nginx.

See [`deploy/README.md`](deploy/README.md) for details.

## Observability

- `GET /metrics` — Prometheus exposition (connections, channels, fan-out drops, inflight bytes,
  webhook and cluster counters, …).
- `GET /health` — liveness.
- `GET /ready` — readiness (returns `503` while starting up or draining).

## Performance

Pylon uses a shared-nothing, per-core event-loop architecture (one worker thread per core,
`SO_REUSEPORT` accept sharding) with an encode-once, sharded fan-out path and adaptive overload
control. At roughly a few KB per idle connection it scales to millions of concurrent connections per
node within its memory budget, and sustained several million message deliveries per second on a
single multi-core workstation in internal benchmarks. These are indicative single-node figures —
benchmark on your own hardware and workload.

## Building from source

A recent stable Rust toolchain is required.

```sh
cargo build --release        # binary at target/release/pylon
cargo test                   # unit + integration suite
#   (some clustering tests require a local Redis; see the test files)
```

## Compatibility

Pylon targets the hosted Pusher Channels protocol (v7) and HTTP API. If you encounter a client or
SDK behavior that diverges from hosted Pusher, please open an issue.

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). For security issues, please
follow [SECURITY.md](SECURITY.md) rather than opening a public issue.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).

Pylon is an independent, clean-room implementation. "Pusher" is a trademark of its respective owner;
Pylon is not affiliated with or endorsed by Pusher.
