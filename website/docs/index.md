# Pylon

A self-hostable, Pusher-compatible realtime WebSocket server, written in Rust.

Pylon is a drop-in replacement for hosted Pusher. Your existing
[pusher-js](https://github.com/pusher/pusher-js),
[Laravel Echo](https://laravel.com/docs/broadcasting),
and [pusher-http-*](https://pusher.com/docs/channels/server_api/http-api/) clients
work unchanged — point them at your own server and you're done.

## Highlights

- Full Pusher v7 protocol parity
- Public, private, presence, encrypted, and cache channels
- Webhooks (channel lifecycle, presence member events)
- Full REST API (`POST /apps/:id/events`, batch, channel/user queries)
- Redis-backed clustering — horizontal scale-out with no shared memory
- Native TLS (rustls, no OpenSSL dependency)
- Prometheus metrics endpoint + `/health` and `/ready` probes
- Adaptive overload control — sheds load gracefully under pressure
- Per-core architecture — near-linear throughput scaling with CPU count

## Get started

[Quick Start](user-guide/quick-start.md){ .md-button .md-button--primary }
[:fontawesome-brands-github: View on GitHub](https://github.com/i-rocky/pylon){ .md-button }
