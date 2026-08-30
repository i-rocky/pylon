# Introduction

## What is Pylon

Pylon is a self-hostable, Pusher Channels–compatible realtime WebSocket server written in Rust.
It implements the Pusher Channels v7 wire protocol and the Pusher HTTP API in full, which means
your existing Pusher client and server SDKs work against it unchanged — you simply point them at
your Pylon instance instead of the hosted service.

- **Client SDKs** — pusher-js, Laravel Echo, and any other Channels v7 client connect to Pylon
  with a one-line host/port change.
- **Server SDKs** — pusher-http-node, pusher-http-php, and compatible libraries authenticate
  and publish events through Pylon's REST API identically to how they talk to hosted Pusher.

No SDK modifications, no shim layers — just reconfigure the host and port.

## Why Pylon

**Self-host the realtime layer.** Running your own WebSocket infrastructure eliminates per-message
fees, keeps connection data entirely within your network, and removes a hard dependency on a
third-party service.

**Full protocol parity.** Pylon supports every Channels v7 feature: public, private, presence,
private-encrypted, and cache channels (including all `cache-`, `private-cache-`, and
`presence-cache-` variants); client events; user authentication (`pusher:signin`); webhooks
(`channel_occupied`, `channel_vacated`, `member_added`, `member_removed`, `client_event`,
`cache_miss`, `subscription_count`); and the complete Pusher HTTP REST API including batch publish and presence queries.

**Per-core architecture.** Pylon uses a shared-nothing, per-core event-loop model — one worker
thread per CPU core, `SO_REUSEPORT` accept sharding, and an encode-once sharded fan-out path.
Throughput scales near-linearly with CPU count.

**Production-minded from the start.** Native TLS (rustls, no OpenSSL), Redis-backed horizontal
clustering, Prometheus metrics, `/health` and `/ready` probes, adaptive overload control (drop-head
queues, CoDel, graduated shedding, memory budget), and clean graceful shutdown are all built in.

## Pylon vs. hosted Pusher / soketi / Laravel Reverb

| Feature | Pylon | soketi | Laravel Reverb |
|---|---|---|---|
| Language | Rust | Node.js | PHP |
| Protocol | Pusher Channels v7 | Pusher Channels v7 | Pusher Channels v7 |
| Public channels | Yes | Yes | Yes |
| Private channels | Yes | Yes | Yes |
| Presence channels | Yes | Yes | Yes |
| Encrypted channels | Yes | Yes | No |
| Cache channels | Yes | Partial | No |
| Clustering | Redis | Redis / Nats / MQTT | Redis / Pusher |
| Native TLS | Yes (rustls) | No (reverse proxy) | No (reverse proxy) |
| Prometheus metrics | Yes | Yes | No |
| Overload control | Yes (adaptive) | No | No |
| License | Apache 2.0 | MIT | MIT |

!!! note "Trademark notice"
    "Pusher" is a trademark of its respective owner. Pylon is an independent, clean-room
    implementation and is not affiliated with or endorsed by Pusher.
