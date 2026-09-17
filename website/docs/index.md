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
[Benchmarks](benchmarks.md){ .md-button }
[:fontawesome-brands-github: View on GitHub](https://github.com/i-rocky/pylon){ .md-button }

## Built by ThriveDesk

Pylon is built and maintained by the team behind [ThriveDesk](https://www.thrivedesk.com),
the agentic helpdesk that unifies email, live chat and a knowledge base into one AI-powered
inbox for ecommerce and SaaS teams. Pylon is the realtime layer built for ThriveDesk's live chat,
which is why it is designed for high connection counts, low latency and a small memory
footprint on ordinary hardware.

Looking for a helpdesk rather than a WebSocket server?
[Meet ThriveDesk](https://www.thrivedesk.com){ .md-button .md-button--primary }
