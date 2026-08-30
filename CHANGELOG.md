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
