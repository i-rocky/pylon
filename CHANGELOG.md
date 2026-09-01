# Changelog

All notable changes to Pylon are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project is
pre-1.0 and versions track `Cargo.toml`.

## [Unreleased]

### Added
- `PYLON_CLUSTER_ENVELOPE_COMPAT` (default `true`) — retire the Redis cluster
  envelope's legacy `event` double-carry. The default keeps emitting both the
  `event` escaped-JSON string and the additive `frame_b64` field, preserving
  0.2.x↔0.3.x mixed-fleet rolling upgrades. Set `0`/`false` — only once the
  whole fleet is ≥0.3.0 — and frame-carrying envelopes omit the legacy field,
  halving cluster-bus bandwidth (frame-less control envelopes unchanged;
  receivers decode both shapes regardless).
- `conformance/` — an SDK-conformance harness: boots a real pylon and drives the
  official `pusher-js` and `pusher-http-node` SDKs through every protocol
  feature they can exercise (26 scenarios), with a coverage audit (`--audit`)
  and an opt-in CI job (`conformance.yml`).

## [0.3.0] - 2026-09-01

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

### Phase 2 — REST / webhook parity (audit remediation)

#### Fixed
- **Disabled apps now return REST 403** (was 401 — the audit's major REST deviation):
  `AppLookup {Found, Disabled, NotFound}` threaded through every app store (static/
  SQL/Mongo) and both cache tiers; unknown apps keep the generic 401
  (anti-enumeration); WS key lookups keep 4001.
- **Webhook retries now run ~5 minutes with capped exponential backoff** (was ~0.7s —
  the audit's major webhook deviation) and retry **all non-2xx** responses per the
  Pusher doc; concurrency permits are held per attempt (a dead endpoint can no longer
  starve healthy ones). `PYLON_WEBHOOK_BACKOFF_BASE_MS/CAP_MS/RETRY_BUDGET_MS` knobs;
  `PYLON_WEBHOOK_RETRY_BASE_MS` and `PYLON_WEBHOOK_MAX_RETRIES` deprecated.
- **Create-and-vacate in one batch window delivers BOTH webhooks** (audit R12a): the
  occupied+vacated pair cancellation was removed — the hosted doc scopes delay/
  suppression to vacated/member_removed on reconnect only; occupied is never
  cancelled.
- **REST errors are JSON bodies `{"error","status"}`** incl. the router 404 fallback
  and a JSON 405 via axum's router-wide method-not-allowed fallback.
- **Distinct auth-failure messages** (timestamp/signature/version/params); the
  unknown-key path stays byte-identical to the unknown-app message.
- **Inapplicable `info` attributes now 400** on both channel endpoints, per the
  doc's applicability matrix (the working collection `subscription_count` stays).

#### Added
- **`subscription_count` webhook event** (doc-verified): `{name, channel,
  subscription_count}`, two-toggle gating (app setting + webhook event_types),
  bridge-owned cluster counts.
- **`cache` info attribute** on `GET /channels/{name}`: `{data, ttl}` or null, TTL-
  aware through local and Redis adapters.
- **POST trigger params accepted from the query string** (body wins; batch excluded
  per the doc).

### Phase 3 — Transport correctness (audit remediation)

#### Fixed
- **Busy-spin eliminated** (G1): the worker loop polls at 0ms only when the previous
  iteration did work — one backpressured client no longer burns a whole core
  (29,711 → ≤8 zero-timeout polls in the regression window); latent `queue_ping`
  close-discard stranding fixed alongside.
- **TLS handshakes complete when the flight blocks** (G2): `DrainStatus::NeedsWrite`
  arms `WRITABLE` mid-handshake; a zero-window client can no longer pin it forever.
- **Slowloris hardening** (G3): request heads capped (`PYLON_MAX_HEAD_BYTES`,
  default 16 KiB) and never-established connections reaped by an absolute deadline
  (`PYLON_HANDSHAKE_TIMEOUT_MS`, default 10s; activity does not postpone).
- **TLS REST handoff processes every record** (G4): multi-record reads no longer
  lose record tails (>4 KiB reads were corrupted pre-fix); latent `put_slice` panic
  on small caller buffers removed.
- **Timer wheel scrubs superseded entries on re-arm** (G6): chatty connections no
  longer accrue ~120k stale timeline slots; all three timelines (liveness, lifetime,
  handshake) scrub eagerly on re-arm/teardown.
- **Cache-channel store evicts expired entries** (G7): moka TTL store replaces the
  read-lazy DashMap (distinct-channel churn was an unbounded leak).
- **Runtime panic sites removed** (G9): registry-mutex poisoning recovered;
  webhook HTTP client build failure aborts startup with a real error; the dispatcher
  degrades gracefully (fires without grace re-check + error log) instead of
  panicking on a construction invariant.
- **Redis reap failures now logged** (G10): user/presence cleanup DEL/SREM/HDEL
  errors warn with key context instead of silently leaving ghost state.
- **`local_subs` deindex hardened** (G5): close-path deindexes the union of the
  reconciled baseline and the live subscription set (defense-in-depth; the exact
  audit leak no longer manifests on the current tree).

#### Added
- **`pylon_drophead_dropped_total`** (G8): drop-head frame evictions are now
  observable in `/metrics` alongside the CoDel counter (three CoDel fold gaps
  closed too).

### Phase 4 — Features the docs promised (audit remediation)

#### Added
- **Redis 7 sharded pub/sub is real** (D1): `PYLON_REDIS_SHARDED_PUBSUB=1` now selects
  SSUBSCRIBE/SPUBLISH across every adapter subscribe/publish path (previously a
  documented knob with zero effect). All nodes must share the flag; wired into CI.
- **Per-app capacity is enforced cluster-wide** (D2): Redis admission (atomic
  `ADMIT_APP_LUA` cap-check), node-guarded release, sweeper reclaim of dead nodes'
  counts (with retry), bridge fail-open on unavailability, and heartbeat re-seeding
  of a node's counts after a long Redis outage (self-heal). Docs describe the real
  semantics including the ~55s worst-case reclaim timing.

### Phase 5 — Documentation reconciliation (audit remediation)

#### Fixed
Every false/stale doc claim from the audit is resolved: dead `enable_client_messages`
fields in deploy examples (D3); shutdown documented as 4200 everywhere (D4, incl.
stale code comments); TOML apps block (D6), `systemctl reload` (D7), nonexistent
env vars in pylon.env.example (D8) and the budget-factor "drops toward 0" myth with
its dead Grafana alert (D9); close-code tables complete (4100/4103/4005/4202) with
the 4009 fatal-vs-status split (D10); Redis Cluster recommendation replaced with the
CROSSSLOT truth (D11); "Tokio worker per core" (D12); "Content-MD5 header" (D13);
Helm "0 = no limit" (D14); sysctl "10 GB" (D15); CONTRIBUTING gating description
(D16); root apps.example.json dead fields (D17); load/README.md created (D18);
undocumented vars + metrics rows (D19); `#server-to-user-*` documented (D20);
garbled heading/sentence (D21/D22); README performance claims grounded in the
repo's own benches/harness (D23); "zero-dropped-message restarts" replaced with the
truthful bounded-drain statement (D24); a "Deliberate restrictions vs hosted Pusher"
section records the body cap, subscription cap, v7-only scope, and encrypted-channel
relay model; the `local: None` saturation-gate trap is called out in code (X2).

#### Added
- `pylon --version` / `--help` (unit-tested; unknown flags exit 1 with a hint).

### Phase 6 — Performance & hot-path efficiency (audit remediation)

#### Fixed
- **TLS writes resuming after a partial flush could re-encrypt from a stale
  cursor** (pre-existing): a WouldBlock mid-batch left the plaintext offset
  un-advanced, so the retry re-sent already-flushed bytes. Surfaced and fixed by
  the writev batching work; pinned by regression tests.
- **Client-event rate limiting now enforces a true 10 messages/sec with a bounded
  burst** (audit F13): the old fixed window admitted a 2× edge-aligned burst (20
  events in ~1.001 s). Replaced with a token bucket — capacity 10, continuous
  10/s refill, O(1) per check; a full idle second restores the whole burst.
  Clients that relied on the window edge will now see the documented limit.

#### Changed
- Connection hot path: accepted sockets set `TCP_NODELAY`; queued frames coalesce
  into single `writev` syscalls per flush batch (≤1024 slices / 256 KiB plain,
  60 KiB TLS budget); the outbound queue carries shared `Bytes` frames — encode +
  frame once per broadcast, refcount clones per connection (allocations per frame
  dropped from one-per-subscriber to a constant); `ServerEvent::Raw` frames fan
  out with zero per-subscriber copies via the new `Codec::encode_into` append
  seam; worker drain hygiene — in-place subscription diffs instead of set clones,
  close-set dedup, relaxed shutdown-flag ordering, read-buffer shrink toward 8 KiB
  after fully-drained bursts; user-directed events encode once per fan-out.
- Legacy registry fan-out no longer holds the shard lock across mailbox sends
  (snapshot under the guard, send after): subscribe/unsubscribe throughput under a
  broadcast storm went from ~115 ops/s lock-stepped to millions of ops/s in the
  new churn bench, and the 1000-subscriber broadcast bench improved ~17%.
- Presence rosters serialize straight from an incrementally-sorted member map —
  no per-join deep-clone + re-sort (wire bytes unchanged, golden-pinned).
- Cluster/Redis: membership heartbeats batch into one pipeline per tick
  (multi-field HSETs, no per-socket string clones); cluster broadcasts encode
  once and feed the same bytes to the local sink and the Redis publish halves.

#### Added
- **`frame_b64` cluster-envelope field (additive, rolling-upgrade safe)**:
  relays now carry the finished frame as base64 alongside the existing string
  field; receivers prefer it and fall back to the old field, so mixed-version
  fleets interoperate in both directions.
- `benches/fanout_sink.rs`: criterion bench for the production
  BroadcastSink→drain path (typed and `Raw` events at 1k/10k/100k subscribers,
  plus a registry-churn-under-broadcast-storm case); `benches/fanout.rs` now
  documents that it covers the legacy registry path.

### Phase 7 — Protocol-version seam (audit remediation)

#### Fixed
- **`pusher_internal:subscription_succeeded` now carries `"data":"{}"` on
  non-presence channels** (audit P12): previously pylon emitted an empty
  string. Verified against live hosted-Pusher captures (two connections, exact
  frames recorded in-code); the official docs are ambiguous on this field for
  non-presence channels. JSON-object key order differs from hosted frames
  (`event,channel,data` vs hosted `event,data,channel`) — unobservable to any
  conforming JSON parser and deliberately unchanged.

#### Changed
- All encode sites route through a single version-aware entry
  (`protocol::wire`): `encode_into`/`encode` take the protocol version
  explicitly and `ACTIVE_VERSIONS` is derived from the negotiation range; the
  v7 frames module is no longer directly callable outside the protocol family
  (compile-time fence). The REST adapter path also now encodes once per
  broadcast (matching the cluster adapter).
- `Capabilities` are real plumbing (audit U1): the dispatch layer consults the
  negotiated codec's capabilities (client events, presence, user auth/signin,
  cache channels, watchlist, encrypted channels) at a single snapshot point;
  v7 behavior is unchanged (all capabilities true), and a future
  version lacking a feature degrades gracefully through the same error frames
  v7 uses for unauthorized paths (proven by all-false stub-codec tests).

#### Added
- Sink broadcasts carry per-version frames (`Vec<(version, Bytes)>` built once
  per publish; each subscriber is delivered its negotiated version) — the
  fan-out is v8-ready with zero cost while only v7 is active (pinned by a
  two-version socket-level fixture).
- "Supporting a new protocol version" dev-guide checklist
  (`website/docs/dev-guide/protocol.md`), including the honest list of what is
  not yet version-aware (cluster envelope, legacy mailbox path).

#### Removed
- Dead `ConnError::Backpressure` variant (audit X1).

### Phase 8 — Security hardening (audit remediation)

#### Security
- **Optional bearer-token gate on `/metrics`** (audit S1): set
  `PYLON_METRICS_TOKEN` and scraping requires `Authorization: Bearer <token>`
  (case-insensitive scheme, constant-time compare). Wrong or missing token
  returns **404 — not 401** — so the endpoint's existence is not disclosed;
  `/health` and `/ready` stay open for load balancers. Unset = today's open
  behavior.
- **Webhook target SSRF guard** (audit S2): webhook URLs must be `http`/`https`,
  and delivery is refused — fast, without burning the retry budget — when the
  host resolves to (or is a literal) loopback, unspecified, link-local,
  RFC1918-private, unique-local, CGNAT/shared (100.64.0.0/10), multicast, or
  broadcast address, in v4 or v6 (including v4-mapped forms). Delivery is pinned
  to the pre-flight-resolved addresses so a second DNS lookup cannot drift, and
  the webhook client never follows redirects (a redirecting receiver gets the
  non-2xx retry treatment). **Operators pointing webhooks at RFC1918/loopback
  receivers must set `PYLON_WEBHOOK_ALLOW_PRIVATE_TARGETS=1`** — the guard is on
  by default.
- **`auth_key` verification is now constant-time** (audit S3), matching the
  existing constant-time signature and body-MD5 comparisons.

### Phase 9 — Release & hygiene

#### Fixed
- **Disabled apps close the WebSocket with 4003 "Application disabled"**
  (re-audit P13): the Pusher protocol doc's close-code table gives disabled its
  own code; WS previously collapsed it into 4001 (unknown key). 4001 stays
  reserved for unknown keys; REST keeps 403. Supersedes the Phase 2 WS-collapse
  decision.
- **`member_removed` webhooks debounced + suppressed on reconnect** (re-audit
  R12b): the hosted doc scopes its "up to three seconds" delay AND its
  reconnect suppression to `channel_vacated` AND `member_removed`. The grace
  window (`PYLON_WEBHOOK_VACATED_GRACE_MS`) now defers `member_removed` too
  and re-checks the user's presence at fire time (a re-joined user suppresses
  the webhook; `member_added` still fires on the rejoin). The grace now applies
  to the single-node path as well (the doc draws no mode distinction) — the
  local adapter serves as its own occupancy/presence oracle.
