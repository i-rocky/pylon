# Changelog

All notable changes to Pylon are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project is
pre-1.0 and versions track `Cargo.toml`.

## [Unreleased]

### Phase 0 — CI & test integrity (audit remediation)

#### Fixed
- **Duplicate `channel_vacated` webhooks in cluster mode** (audit G11): the Redis
  sweeper's vacate decision could straddle the atomic last-unsubscribe, emitting a
  second `channel_vacated` for one vacancy. Vacate emission is now gated by an
  atomic compare-and-swap — exactly one of {bridge last-unsubscribe, sweeper} wins
  the emission right (SREM verdict from `UNSUBSCRIBE_LUA` / new `VACATE_LUA`).
- macOS-only test race in the CoDel socketpair tests (`drain_tags` treated
  WouldBlock as end-of-data; macOS loopback delivers writes asynchronously).
- `cluster_subscribe` settle races: settle budgets now generous-but-bounded with a
  deliberate duplicate-exposure window; unsubscribes gate on the occupied webhook's
  delivery (the batch coalescer intentionally cancels occupied+vacated sharing a
  window — see audit R12a; parity review scheduled for Phase 2).

#### Changed
- **CI: the cluster/Redis integration step is now blocking** (was
  `continue-on-error`); all seven cluster suites de-flaked to event-based waits
  first (baseline was 0/10 green locally, now 10/10).
- **CI: five previously-unrun suites now run as blocking gates** — `admin`,
  `percore_nonblocking_establish`, and the `mongo`/`mysql`/`postgres` app-manager
  suites (with service containers).
- **CI: the Redis failover/self-heal regression now runs on every push** (dedicated
  job, own Redis container, previously opt-in-only and never run).

#### Tests
- `tests/metrics.rs` asserts the exact `pylon_connections` value (label-presence
  only before — a stuck-at-0 counter would have passed).
- `tests/cluster_bridge.rs` fails loud when Redis is unreachable instead of
  silently skipping.

> Full audit remediation roadmap: `docs/superpowers/` (local). Findings spec IDs
> referenced above: G11, C1–C5, R12a.

### Phase 1 — WebSocket wire-protocol parity (audit remediation)

#### Fixed
- **Fragmented WebSocket text messages are reassembled** (RFC 6455 §5.4) — previously
  FIN=0 Text frames were dispatched immediately and Continuation frames ignored.
  Interleaved control frames are answered mid-fragment; protocol violations during
  fragmentation close 1002; fragmented binary is ignored like all binary.
- **The WebSocket closing handshake is completed** — client-initiated Close is now
  echoed (client's code when present, else 1000) before teardown (RFC 6455 §5.5.1);
  lone WS-Ping replies are flushed promptly (§5.5.2).
- **Non-UTF-8 text frames close the connection with 1007** (RFC 6455 §8.1) —
  previously silently dropped.
- **Malformed connection paths now reject with 4005 "Path not found"** — previously
  collapsed into 4001 (unknown app key), which remains correct for well-formed paths
  with an unknown key.
- **Protocol negotiation infers from the `version` query param** when `protocol` is
  absent (per the protocol doc); 4006 is now scoped to genuinely malformed
  version/protocol strings — out-of-range integers (e.g. `protocol=300`) correctly
  get 4007.
- **Non-standard top-level `channel` field removed from `pusher:error` frames**
  (strict shape parity; `pusher:subscription_error` keeps its legitimate channel).

#### Added
- **4202 max-connection-lifetime close** (`PYLON_MAX_CONN_LIFETIME_SECS`, default
  86400, 0 = disabled) — absolute deadline, not reset by activity.

#### Verified-no-change (citations in code)
- Client-event rejections keep 4301 for all four classes (rate-limit message matches
  hosted Pusher verbatim; others undocumented by hosted; pusher-js tolerates any
  in-band code).
- `pusher:subscription_error` `data.status` keeps 4009 (invalid name) / 401 (auth
  failure) — undocumented by hosted Pusher, unread by pusher-js from server frames.
- Per-connection subscription cap stays at 200 — hosted Pusher documents no such
  limit; now documented as a deliberate pylon resource guard.
