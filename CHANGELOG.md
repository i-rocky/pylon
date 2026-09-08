# Changelog

All notable changes to Pylon are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project is
pre-1.0 and versions track `Cargo.toml`.

## [Unreleased]

### Added
- `PYLON_CLUSTER_ENVELOPE_COMPAT` (default `true`) — retire the Redis cluster
  envelope's legacy `event` double-carry. The default keeps emitting both the
  `event` escaped-JSON string and the additive `frame_b64` field, preserving
  0.2.x↔0.3.x mixed-fleet rolling upgrades. Set `0`/`false`/`off` — only once
  every node runs a build that ships this knob (**v0.3.0 does NOT qualify**: a
  0.3.0 receiver cannot decode a compat-off envelope and drops it silently) —
  and frame-carrying envelopes omit the legacy field, roughly halving
  cluster-bus bandwidth (frame-less control envelopes unchanged; receivers on
  knob-shipping builds decode both shapes regardless).
- `conformance/` — an SDK-conformance harness: boots a real pylon and drives the
  official `pusher-js` and `pusher-http-node` SDKs through every protocol
  feature they can exercise (26 scenarios), with a coverage audit (`--audit`)
  and a CI job (`conformance.yml`, nightly at 03:00 UTC plus manual dispatch).
- Conformance harness: **observation-normalization audit family** — after
  every run, all recorded observations are scanned for run-unique shapes
  (socket-id-like dotted integer pairs, ISO-8601 timestamps, raw
  epoch-millis, bare epoch-sized integers); a violation prints a warning
  block and fails the run (exit 1) even when every verdict passed, keeping
  the JSON artifact stable and diffable. `--audit` runs the same scan
  against the last report artifact, advisorially. Shapes are conservative
  (legitimately-fixed values like `activity_timeout_used_ms: 2000` or
  version strings stay silent), with a documented per-scenario key allowlist
  for reviewed exceptions (empty today).

### Changed
- **`PYLON_MAX_CHANNEL_NAME_LENGTH` default raised from 164 to 200 bytes** — both
  actively-maintained official server SDKs (`pusher-http-node`, `pusher-http-go`)
  validate channel names up to 200 bytes client-side; the live pusher.com docs
  quote 164, but a maintained SDK is the stronger signal of what real traffic
  looks like, and pylon's old default rejected a 165-200 byte channel name the
  SDKs would already send successfully to hosted Pusher. Set `164` to match the
  published doc value instead.
- **`PYLON_MAX_EVENT_PAYLOAD_BYTES` default lowered from 10,240 to 10,000
  bytes** — hosted Pusher's docs say "smaller than 10kB" (decimal, not KiB) and
  the archived OpenAPI spec pins `maxLength: 10000`; the old 10,240 (10 KiB) let
  a 10,001-10,240 byte payload pass pylon and then 413 on hosted, breaking
  migration out of pylon. Note this tightens an existing default: a payload
  between 10,001 and 10,240 bytes that pylon accepted before now gets a 413.
  Set `PYLON_MAX_EVENT_PAYLOAD_BYTES=10240` to keep the previous behaviour.
- **Malformed numeric `PYLON_*` values now fail startup instead of silently
  keeping the default** — every numeric environment variable is rejected when it
  is set to a value that fails to parse. `PYLON_PORT=abc` and
  `PYLON_MAX_CONNECTIONS=100_000` (the latter valid Rust literal syntax, invalid
  for `parse`) previously booted on the default with no indication anything was
  wrong; both now log the variable name, the offending value, and the expected
  type at `error`, then exit non-zero. Unset variables are unaffected (the
  default applies, as before), as are boolean (`0`/`false`/`off`) and
  plain-string variables, which cannot fail to parse. Anyone running with a
  typo'd numeric `PYLON_*` value must fix it before the server will start.
- **App credentials are now validated, and app loading now aborts on a bad
  entry instead of serving it** — `App::validate()` previously checked only
  the webhook array, so an app with an empty or whitespace-only `id`, `key`,
  or `secret` loaded with no warning and authenticated normally.
  HMAC-SHA256 accepts a zero-length key, and an app's `key` is public by
  design (it ships in browser bundles), so a blank `secret` let anyone
  holding the key forge REST signatures, channel-auth tokens, and
  `pusher:signin` for that app. `validate()` now rejects a blank `id`,
  `key`, or `secret`, and the static-file loader additionally rejects a
  duplicate `id` or `key` across the loaded app set, naming the offending
  value — `StaticFileAppManager::resolve` is a linear scan, so a duplicate
  key previously let the first-loaded entry win silently, attaching clients
  of the second app to the wrong secret, capacity, and webhook set (the SQL
  and Mongo schemas already enforce uniqueness on those columns, so the
  duplicate check is static-file-only). **This is breaking for any
  deployment whose `apps.json` currently carries a blank credential or a
  duplicate `id`/`key`: the server now refuses to start rather than loading
  it** — fix the offending entries before upgrading. `validate()` also runs
  per-lookup, not just at load: the SQL and Mongo backends call it from
  every `by_id`/`by_key` fetch, so a stored row with a blank field now fails
  every lookup against it as `AppLookupError::Decode`, which the REST auth
  path renders as `503 "app store temporarily unavailable"` (logged as
  `"app lookup failed (transient)"`) rather than the disabled/not-found
  response that row would otherwise produce. That's correct — it fails
  closed — but it points an operator at their database rather than the
  offending row, so check for blank credentials there too before upgrading.
- **REST `POST /events` and `/batch_events` now reject a malformed
  `socket_id` with `400`** — hosted Pusher's HTTP API validates `socket_id`
  server-side against `\A\d+\.\d+\z` (two non-empty runs of ASCII digits
  joined by exactly one dot, matching `pusher-http-node`'s client-side
  `validateSocketId`), and every other trigger field on this endpoint
  already enforced Pusher's documented rules — `socket_id` was the one gap:
  pylon fed it straight into `SocketId::from_raw`, which truncates rather
  than validating, so any string was accepted and excluded nothing from
  delivery. Validation runs on the query-merged trigger body (so the R9
  query-string fallback path can't bypass it) and, for `/batch_events`,
  against every item before any `deliver()` call runs — one bad `socket_id`
  in a batch now rejects the whole batch rather than partially delivering
  the earlier items. **This is breaking for any integration that has been
  sending a non-conforming `socket_id` and relying on the previous `200`**:
  that call now returns `400 "Invalid socket id"` — audit callers before
  upgrading.
- **An empty `channel_data` on a presence subscribe no longer signs as a
  private-channel token** — `channel_signature` collapsed `Some("")` onto the
  private signing string, so such a subscribe verified against a token signed
  without channel data and was kept out only by `parse_channel_data("")` failing
  afterwards. The join was already refused either way; the wire effect is that
  the `pusher:subscription_error` for this (malformed) request now reads
  "Invalid signature" rather than "Invalid channel_data", both still
  `AuthError`/401 and non-fatal.
- **A REST request carrying two query keys that differ only by case is now
  rejected with `401 "Invalid query: two parameters differ only by case"`** —
  the signing string lowercases every key, so `Info` and `info` collapsed into
  one entry with the survivor decided by `HashMap` iteration order. The
  signature was therefore not a function of the request: the same signed URL
  could verify on one attempt and 401 on the next. No bypass was possible (every
  field the handlers act on is read by exact case), but a legitimately signed
  request could fail intermittently. Requests without such a collision — every
  request an official SDK builds — are byte-for-byte unaffected.
- **An app `key` containing `:` is now rejected at validation instead of
  silently breaking every websocket auth** — the channel-auth and
  `pusher:signin` tokens are `<key>:<signature>` and both verifiers split at the
  first colon, so a key like `team:web` made every private and presence
  subscribe answer "Auth key mismatch" and every `pusher:signin` close the
  connection with 4009, permanently. REST was unaffected (it reads `auth_key` as
  its own query parameter), so the failure looked like a client-library bug. It
  failed closed, so nothing was exposed. `App::validate` now rejects such a key,
  naming the reason; as with the other credential checks this runs at load for
  the static-file manager and per-lookup for the SQL and Mongo backends. **This
  is breaking for any deployment whose app key contains a colon: the server now
  refuses to load it** — rotate the key before upgrading.
- **REST `socket_id` validation now caps length at 24 bytes instead of 64**,
  closing the 25–64 byte band that passed validation and was then silently
  truncated. A `SocketId` stores 24 bytes inline and `SocketId::from_raw`
  truncates rather than failing, so a signed trigger carrying a well-formed but
  over-long `socket_id` matched no connection: the exclusion did nothing and the
  call still answered `200`. The bound is now derived from `SocketId::CAPACITY`
  so the two cannot drift apart again. Every id pylon issues is at most 21 bytes
  (two 10-digit halves and a dot), so no conforming caller is affected; a caller
  sending a longer id now gets `400 "Invalid socket id"`.
- Per-core worker broadcast index consolidated to the single-map layout: each
  `local_subs` channel entry now carries its subscribers' `(slab token,
  negotiated protocol version)` directly (`(app, channel) → {socket_id →
  (token, version)}`), replacing the parallel `socket_id → (token, version)`
  map. The broadcast drain's per-subscriber loop now resolves the token and
  version from the subscriber iteration itself — the standalone per-subscriber
  probe lookup is gone, roughly halving per-subscriber fan-out cost at 1k
  subscribers and ~2.7x-ing it at 100k on the `fanout_sink` bench (33–64%
  faster end-to-end publish+drain). Delivery, close deindexing, per-version
  fan-out, and eviction semantics are unchanged.
- `RedisAdapter::send_to_user` now encodes the user event ONCE per send and
  feeds the same frame to both halves (the node-local delivery runs as a `Raw`
  frame; the `usermsg` publish reuses the identical string) — previously the
  typed event was encoded once inside the local half and again for the Redis
  publish. Wire bytes are unchanged (the same `wire::encode` at
  `ACTIVE_VERSIONS[0]`; the encode-once shape already proven for broadcasts).
- Presence `subscription_succeeded` rosters are now encoded ONCE per membership
  generation, not once per join: `ChannelState` caches the encoded roster frame
  (the same `wire::encode` seam every frame uses), invalidated only when the
  distinct-user set changes (a new user's first connection or a user's last
  disconnection), and every join of that generation shares the cached frame
  (`Arc`) instead of deep-cloning the roster into an owned payload. Wire bytes
  are byte-identical (pinned by the roster goldens and the end-to-end literal
  pins); the Redis adapter's cluster-roster overwrite still re-encodes fresh
  cluster truth per join (it replaces the frame in the join outcome, not the
  node-local cache).

### Removed
- **`PYLON_WEBHOOK_RETRY_BASE_MS` and `PYLON_WEBHOOK_MAX_RETRIES` removed** —
  deprecated in the v0.3.0 line, their one-release grace window has now
  elapsed. Use `PYLON_WEBHOOK_BACKOFF_BASE_MS` in place of the former; the
  latter has no successor (retries are bounded by `PYLON_WEBHOOK_RETRY_BUDGET_MS`,
  total time rather than attempt count). Setting either variable now has no
  effect and produces no warning.
- **`transport::conn::ConnState::Closing` removed** — the variant was never
  assigned anywhere in the crate, so the two `Open | Closing` dispatch arms
  that matched it were reachable only through `Open`. The transport closes a
  connection by queueing its Close frame, flushing once and tearing down, with
  no intermediate draining state; the variant described a lifecycle step that
  does not exist. Library consumers matching on `ConnState` exhaustively need
  to drop the arm.

### Fixed
- **The `client_event` webhook now sends `data` as a string, not as the raw JSON
  value.** `pusher-http-node` 5.3.4's `index.d.ts` declares the webhook event as
  `{name, channel, event, data: string, socket_id}`, and its `lib/webhook.js`
  parses only the envelope body — never `event.data` — so a receiver is expected
  to get text and parse it itself; hosted Pusher's docs agree. Pylon emitted the
  decoded value instead, so a client event carrying `{"msg":"hi"}` produced
  `"data": {"msg":"hi"}` where hosted produces `"data": "{\"msg\":\"hi\"}"`,
  and a consumer following the declared type threw on `JSON.parse(event.data)`.
  Receivers now see `data` as a JSON-encoded string for every payload shape a
  client can send (object, array, number, boolean, null); a payload that was
  already a JSON string is passed through unchanged rather than double-encoded.
  **A receiver that was reading `client_event`'s `data` as an object must now
  `JSON.parse` it** — no other webhook type's payload changes.
- **A clustered node no longer goes deaf to a channel it still has subscribers
  on when a leave and a re-join are applied out of order.** The percore worker
  computes the node-local 1→0 teardown edge under the shared registry lock but
  the bridge applies it later, in queue order — so a re-join landing between the
  two made the node UNSUBSCRIBE from a channel it had just re-acquired a
  subscriber for, dropping every cross-node event on it until the membership
  reconciler's next tick. The teardown is now re-checked against live node-local
  truth (channels and per-user `usermsg` bindings alike) at the moment it is
  applied.
- **A presence member reaped by the very node that owned it is now removed for
  that node's own clients too, instead of only for the rest of the cluster.**
  The sweeper stamped its compensating `member_removed` with the departed
  member's node id; when a node's own membership stamps went stale while it kept
  holding the sweep lease, that id was its own and its receive loop dropped the
  frame as a self-echo. The sweeper delivers to no local socket itself, so its
  emission now belongs to no publisher and every live node delivers it.
- **A cache channel's stored last event is written before the event is
  broadcast**, so a subscriber whose asynchronous replay races a publish can no
  longer be handed the previous event after the fresh one.
- **The graceful-shutdown drain no longer waits on inbound buffers it will
  never consume, so a rolling restart exits as soon as queued replies are
  sent instead of always burning the full grace window.** `inflight_bytes`
  (the worker's memory-pressure signal) counts inbound reassembly and
  frame buffers alongside queued outbound bytes — correct for shedding and
  admission control, but the drain's exit check read that same conflated
  total, and a connection mid-frame or mid-message pins it above zero
  forever once the peer stops sending. On a busy node some connection is
  essentially always mid-frame, so every restart burned the entire
  `shutdown_grace_ms` (10s in the shipped Helm values) per node. The drain
  now exits on a connection's queued-outbound-bytes total instead, tracked
  incrementally the same way `inflight_bytes` is; `inflight_bytes` itself is
  unchanged and still governs shedding and admission.
- **A partially-received WebSocket frame is now billed to its connection, so
  the bytes a peer pins by trickling a large frame are visible to the byte
  budget** (security-relevant). The per-connection read buffer holds a frame's
  bytes until the frame completes, and its only bound is the per-frame
  `max_payload` — whose 1 MiB floor an operator cannot lower. Those bytes were
  counted nowhere: `inflight_bytes` and everything now built on it (the REST
  `503` admission path, the `client-*` ingress drop, the graduated shed bands
  and the PSI backstop) read green while real memory climbed, so a peer opening
  N connections, sending a large frame header on each and then trickling the
  payload could pin memory outside the budget and the shedding machinery could
  not react. The buffer is now billed to the connection alongside its queued
  out-frames and its message-reassembly buffer, and released as soon as the
  frame completes. No configuration changes; the per-frame ceiling itself is
  unchanged.
- **Under `PYLON_REDIS_SHARDED_PUBSUB`, a node no longer comes back from a Redis
  reconnect subscribed to only a fraction of its channels.** Pylon subscribes
  exclusively with `SSUBSCRIBE` in that mode, so fred's ordinary-channel and pattern
  sets are empty by construction — but its `resubscribe_all` replayed them anyway,
  writing a zero-argument `SUBSCRIBE` and `PSUBSCRIBE` that Redis answers with
  errors. Neither command is response-tracked while the `SSUBSCRIBE`s that follow
  are, so a stray error frame was handed to an in-flight shard resubscribe and the
  batch was abandoned at the first hash-slot group — every later group (one per
  channel, since pylon's keys are hash-tagged) never re-issued. That node silently
  stopped delivering cross-node broadcasts, user sends, terminates and watchlist
  transitions for those channels, while it kept publishing normally, other nodes saw
  it as healthy, and its own `redis_connected` gauge stayed `true`. The membership
  reconciler does not cover this: it diffs against fred's tracked sets, which still
  list every channel after the aborted resubscribe. Pylon now owns the reconnect
  repair and re-issues each tracked channel, pattern and shard channel one at a
  time, logging and continuing past a failure instead of abandoning the rest.
- **The sweeper's user-binding reap can no longer wipe a live binding and report an
  online user as offline.** `reap_user` was a five-round-trip read-modify-write —
  the un-fixed twin of the channel-member reap already made atomic. Its `HLEN` guard
  closed the window between the `HDEL` and the guard itself, but not the one between
  the guard and the `DEL` that followed: a signin landing there had its fresh binding
  deleted, was dropped out of `users(app)` — where nothing re-adds it, since the
  heartbeat only re-`HSET`s the binding — and had a `WatchOffline` published for it,
  stamped with the DEAD node's id so no live node self-dedups it. Watchlist clients
  saw a user go online and immediately offline while they were connected and signed
  in, `is_user_online` answered `false` for up to a heartbeat, and that user became
  invisible to every later sweep, so a genuine crash of the node holding them would
  never fire `WatchOffline` at all. The reap is now one `USER_REAP_LUA` CAS, which
  Redis serialises against the signin script: the offline edge belongs to whichever
  caller's `SREM` actually removed the `users(app)` entry, exactly as the channel
  vacate and member reap already decide theirs, so a concurrent signout that
  de-indexed the user first also leaves the reap silent.
- **A connection whose cluster capacity admission failed open no longer steals a
  sibling connection's unit when it closes.** `admit_app` returning `None` — a
  bridge channel that was full or closed, a verdict that timed out, or a Redis
  error — fails open and takes no unit, but the close path fired a release for
  every connection of an app with a `capacity`. `RELEASE_APP_LUA`'s node guard is a
  per-NODE aggregate check, not a per-connection one: it only trips when this
  node's per-app field is absent or already zero, so on a node holding units for
  other connections of the same app the phantom release sailed past it and
  decremented the cluster total anyway. Per fail-open admission that later closed,
  the cluster silently believed one connection fewer than it held and admitted one
  extra past `capacity` — and on a busy long-lived node the books never re-balanced,
  because the node's per-app field effectively never bottomed out. The release is
  now gated on the connection's OWN admission verdict, making the script's floor-0
  guard the backstop it is described as. An admission whose verdict arrived after
  the worker gave up is released by the bridge instead, so it leaks nothing either.
- **A live node reclaimed as dead no longer leaves the cluster permanently
  under-counting that node's per-app connections.** A node whose `node:{id}`
  liveness key merely lapses — three missed heartbeats of Redis unreachability
  *from that node* is enough, while it keeps serving every one of its clients — is
  swept up by another node's dead-node reclaim, which subtracts its per-app units
  from the cluster total `appconns` and deletes its `nodeconns` hash. The
  heartbeat's self-heal then re-seeded only `nodeconns`, so `appconns` stayed short
  by one node's worth of connections for the life of the deployment, admitting that
  many extra past the app's configured `capacity` — and double-subtracting as the
  node's pre-existing connections closed. The self-heal now re-seeds this node's
  hash from the worker fleet's live per-app counts *and*, in the same script,
  recomputes each of those apps' cluster total as the sum over every node's hash.
  Summing rather than adding back is what makes the repair correct for both ways
  the hash can vanish: a plain TTL lapse (the units were never subtracted, so
  adding them again would double-count) and a reclaim (they were). The reclaim
  itself now re-checks the liveness key *inside* its script and declines, so a node
  that re-advertised between the sweeper's `EXISTS` probe and the `EVALSHA` is left
  alone.
- **A presence roster no longer advertises the `user_info` of a connection that
  has already left.** `ChannelState` keeps one `user_info` per distinct presence
  user, seeded by that user's first connection; `remove` only decremented the
  refcount, so once the seeding connection left, every later subscriber's
  `subscription_succeeded` roster and every `GET /apps/{id}/channels/{c}/users`
  kept serving a value **no live connection had ever presented** — for as long as
  any other connection of that user remained. The everyday shape: update a
  profile, open a new tab, close the old one, and everyone who joins afterwards
  sees the old profile. The roster entry now follows the user's OLDEST LIVE
  connection: unchanged while that connection lasts (a second connection of the
  same user still does not displace it, and still emits no `member_added`), and
  re-seated on the next-oldest survivor when it departs. Because a `user_info`
  can now change without the user set changing, the memoised
  `subscription_succeeded` frame is invalidated on that change too — a roster
  generation is everything its encoded bytes depend on, not just the set of ids.
  The clustered roster is re-seated by the entry below.
- **The CLUSTERED presence roster is re-seated too, so a single-node and a
  clustered deployment no longer disagree about `user_info`.** Redis `presinfo` is
  written once, on the cluster-wide 0→1 user edge, and nothing re-derived it: a
  clustered deployment kept advertising the seeding connection's metadata
  cluster-wide and forever, reproducing the defect fixed node-locally above for
  every clustered operator. The re-seat needs the surviving connections' own
  `user_info` values to choose from and nothing in the keyspace held them, so this
  is a **keyspace change**: a new per-channel hash `presseats` (`user_id` → that
  user's connections in cluster join order, each a `member_token` line followed by
  the `user_info` line it presented) shares the `{channel}` hash tag, so every
  existing multi-key presence script stays same-slot. `PRESENCE_LEAVE` and the
  sweeper's `REAP_MEMBER` now re-seat `presinfo` onto the user's oldest connection
  still listed in `presmembers` *inside their own scripts*, so choosing the
  survivor and installing it are one indivisible step — the same reason the
  presence cap and the vacate verdict live in theirs. `VACATE` drains the new hash
  with the other three; a clean last leave removes it as it already removed them.
  **Sizing:** `presseats` holds one `user_info` copy per presence CONNECTION
  rather than per user, each bounded by `PYLON_MAX_PRESENCE_USER_INFO_BYTES`
  (default 1024 B), so a channel's presence residency now scales with connections
  — budget roughly `connections × (user_info + ~60 B)` per presence channel.
  **Rolling upgrade:** the new hash is purely additive and needs no flag, no
  backfill and no coordinated restart. Nodes on the older build neither read nor
  write it, and `presmembers` — which both builds maintain — stays the liveness
  truth, so a seat orphaned by an older node's leave is never seated from and is
  collected the next time a new node re-seats that user. A user whose oldest live
  connection sits on a not-yet-upgraded node keeps the old first-writer value
  until that node is upgraded; the roster is never worse than it was before, and
  becomes exact once the whole fleet runs the new build. Rolling BACK leaves
  `presseats` hashes that the older `VACATE` will not drain: they are inert, and
  can be removed manually (`presseats:*`) once every node is downgraded.
- **A presence join rejected by the cluster member cap no longer swallows the
  node's 0→1 Redis `SUBSCRIBE`, which left the node deaf to the channel it still
  held members of.** `node_first` is a one-shot token — exactly one in-flight
  bridge command carries it for a given node-local 0→1 edge — and the capacity
  rejection returned before `cluster_subscribe`, the only place the channel's
  `msg` key is subscribed. A rejected joiner racing an admitted one (a second
  connection for a user already on the cluster roster, so not a new distinct user)
  therefore left the node holding a live presence member of a channel it was not a
  Redis subscriber of: no `member_added`, no `member_removed`, and no cross-node
  channel events for anyone on that node, with nothing logged. The membership
  reconciler introduced alongside this bounded the damage to one tick
  (`PYLON_REDIS_PRESENCE_HEARTBEAT`, default 25s) rather than the life of the
  process, but pub/sub has no replay, so every frame inside that window was still
  lost. The bridge now spends the pub/sub edge before the admission verdict and
  hands it back — a matching `UNSUBSCRIBE` — only when the rejection leaves the
  node with no members for the channel at all.
- **The cluster-wide `PYLON_MAX_PRESENCE_MEMBERS` cap is now decided atomically
  inside the presence-join script, so concurrent joins landing on different nodes
  can no longer push a presence roster past it.** The bridge previously probed the
  Redis count of record (`HLEN presusers` + `HEXISTS presusers <user>`) and
  committed the join several round trips later, with nothing reserving the slot in
  between: N nodes admitting at the same instant each read room and each committed,
  overshooting the cap by up to N−1 members, and the roster stayed over-cap until
  members left. `PRESENCE_JOIN_LUA` now takes the cap as an argument and weighs a
  new distinct user against `HLEN presusers` in the same indivisible script that
  records the member, returning `-1` for a rejection that wrote nothing; the
  separate capacity probe is gone. The rejection shape is unchanged — the same 4004
  `LimitReached` `subscription_error`, and a second connection of a user already on
  the roster is still admitted with the channel full.
- **Cluster state that a node computes from live membership is now reconciled
  every heartbeat instead of applied once on an edge, so a single missed edge no
  longer disables a channel for the life of the process.** Three symptoms shared
  one shape. (1) The `chans` / `users` indexes — the sweeper's only enumeration
  of occupied channels and signed-in users, and the CAS the single cluster-wide
  `channel_vacated` is won on — were written only on the cluster 0→1 edge while
  the membership heartbeat unconditionally re-created `occ` / `usr`, so a Redis
  restart, a dropped bridge command or a sweeper false-reap left every affected
  channel functionally occupied and structurally orphaned **permanently**: zero
  further `channel_vacated`, an under-reporting `GET /channels`, and no
  crash-driven `member_removed` or `WatchOffline` for those channels and users.
  (2) A dropped bridge `Subscribe` / `Signin` skipped the Redis `SUBSCRIBE` of
  the channel's `msg` key or the user's `usermsg` key, and nothing ever retried
  it — the node stayed deaf to **all** of that channel's cross-node traffic (and
  silently no-op'd cross-node `terminate_user`) indefinitely, while reporting
  `redis_connected = true`. The membership heartbeat is now a full
  reconciliation tick: it re-seeds `apps` / `chans` / `users` from the node's own
  registry in the same pipeline that re-stamps `occ` / `usr`, and re-subscribes
  any `msg` / `usermsg` key the node has local members for but is not attached to
  — a diff against the subscriber client's in-memory tracked set, so a healthy
  node pays no extra Redis round-trip. The index write inside the membership join
  script is now unconditional rather than gated on the 0→1 edge, so any
  subscribe or signin also repairs a lost entry immediately. (3) A dropped
  `pusher:subscription_succeeded` was unrecoverable, because the ack rides the
  connection's bounded mailbox and is dropped when it is full while the join it
  acknowledges is already committed — and re-issuing `pusher:subscribe`, the
  client's only recovery, was a silent `return`. A duplicate subscribe is now
  re-acknowledged (still registering nothing, so presence connection counts and
  the per-connection subscription cap are unaffected), with the presence roster
  read from the same source the original ack used — cluster-wide on a clustered
  node, node-local otherwise.
- **A connection handed off to the REST plane no longer leaks its queued bytes
  into the worker's byte total.** Every other teardown subtracts what the
  connection still holds; `handoff_rest` removed the slab entry without doing
  so, while its call sites folded those same bytes *in* first. The drain queues
  its 4200 frames onto still-handshaking connections too, so a request head
  arriving mid-drain left `inflight_bytes` permanently above zero — a phantom
  floor that no connection holds, which makes `pylon_inflight_bytes`
  over-report for the life of the worker and stops the drain's fast exit from
  ever firing again (debug builds panicked on the accounting cross-check).
- **Shutdown no longer waits out the full `shutdown_grace_ms` when a peer has
  already gone away.** The drain queued its `pusher:error` 4200 + Close(4200)
  and discarded the flush's verdict, so a connection whose peer had RST'd —
  routine on a rolling restart, where the load balancer drains clients while
  the node is stopping — was left in the slab with those frames queued and no
  WRITABLE interest armed. `inflight_bytes` could then never reach zero, so the
  drain's "everything flushed, exit now" path stopped applying and every
  restart paid the whole grace window (10 s by default); debug builds panicked
  on the queued-bytes-imply-armed invariant instead. Such a connection is now
  torn down as soon as the flush reports it unwritable.
- **A TLS connection no longer under-reports up to 60 KiB of unsent data to the
  byte budget.** The out-queue released a frame the moment rustls accepted the
  plaintext, not when the bytes reached the socket, so a backpressured TLS
  connection could sit at zero queued bytes while rustls held a whole 60 KiB
  batch behind a full send buffer. `inflight_bytes` — and with it the REST 503
  path, the `client-*` ingress drop and the graduated shed bands — therefore
  engaged later than configured on TLS deployments, and the shutdown drain's
  "everything flushed" exit could fire over Close(4200) frames that had not
  actually gone out, leaving those clients with a bare TCP close (which
  pusher-js backs off from instead of reconnecting immediately). Plaintext
  rustls has taken but not yet put on the wire is now billed to the connection
  until it is written.
- **A peer can no longer pin up to 1 MiB of reassembly buffer per connection,
  invisible to the byte budget** (security-relevant). The first fragment of a
  fragmented TEXT message (RFC 6455 §5.4) was accepted with no
  `max_message_bytes` check — the documented per-message cap fired only on the
  next append — so its only bound was the per-frame `max_payload`, whose 1 MiB
  floor an operator cannot lower. The resulting accumulator hung off the
  connection entry and was counted nowhere: `inflight_bytes`, the graduated shed
  bands, the PSI backstop and the node connection ceiling all read green while
  RSS climbed, and a peer holding N connections that each open an oversize
  message and never complete it could pin the whole configured memory budget in
  memory the budget could not see. The cap now applies from the opening fragment
  on, so an oversize message is never buffered at all; and the buffer a
  legitimate fragmented message does hold is billed to the connection, so the
  worker's `inflight_bytes` — and every shedding and admission decision built on
  it — accounts for reassembly memory.
- **An oversize fragmented TEXT message is dropped silently instead of closing
  the connection with 1002** — `max_message_bytes` is documented as dropping an
  oversize assembled message *without* closing the connection, and the drop did
  reset the accumulator, but it left no record that the message was still in
  flight. The next Continuation of that same message therefore hit the
  stray-Continuation guard and failed the connection with WebSocket Close 1002
  (protocol error), so the documented silent drop only happened when the
  overflow landed on the final fragment. A client that legitimately fragments a
  large-ish payload saw an unexplained 1002 — and, on retry after reconnecting,
  a reconnect loop. The remaining fragments of an over-cap message are now
  swallowed until its FIN=1 frame closes it out, matching what the unfragmented
  path has always done.
- **A node dying while it held the last members of a presence channel no longer
  leaks its roster into Redis forever, and no longer skips their
  `member_removed`.** The three presence side-tables (`presusers`, `presinfo`,
  `presmembers`) had no TTL and no vacate-time cleanup: their only reclaim path
  ran off the member tokens in the channel's `occ` hash, whose whole-key TTL
  expired at the same instant those tokens went stale. A sweep that arrived
  after that found an empty `occ`, reaped nothing, and fired `channel_vacated`
  with **zero** preceding `member_removed` — leaving the departed users in the
  cluster roster permanently. The leak then compounded: every later join of such
  a user on that channel incremented the ghost refcount instead of crossing the
  0→1 edge, silently suppressing both its `member_added` and, later, its
  `member_removed`, and the state was unrecoverable without a manual `DEL`.
  Three changes close it, together making the side-tables strictly unable to
  outlive the membership they describe. (1) The `occ` hash's whole-key TTL is now
  a genuine backstop rather than a second deadline — it outlives the per-member
  `expireAt` stamps it carries by `4 × sweep_interval + 5` seconds, so a crashed
  node's tokens survive to be resolved and reaped one by one through the existing
  exactly-once `member_removed` CAS. (2) The sweeper's vacate now also DRAINS the
  presence side-tables, in the same atomic script that de-indexes the channel from
  `chans` (the one structure with no TTL, and therefore the last point at which
  the roster is still reachable): the single SREM winner emits one
  `member_removed` per surviving user *before* its `channel_vacated`, then deletes
  all three hashes. (3) A presence leave now writes Redis before the membership
  half de-indexes the channel, so that index brackets the state it describes and a
  crash between the two calls can never strand a roster nothing enumerates. No
  configuration changes; the non-crash path still fires exactly one
  `member_removed` followed by one `channel_vacated`, as before.
- **HTTP request heads with more than 32 header fields are now accepted (up
  to 128), and a head that still overruns the limit is answered instead of
  the connection closing silently** — `read_head` parsed into a fixed
  32-slot array, so httparse's 33rd header field folded into a generic
  malformed-request error that the worker mapped straight to a silent
  close (no status line, no log). 32 was reachable by ordinary traffic — a
  browser's WS upgrade already carries 10-14 fields before a CDN or
  reverse-proxy chain appends `X-Forwarded-*`, `CF-*`, `Via`, `Forwarded`,
  tracing headers, and per-request cookies — so the failure looked, from
  the client, indistinguishable from a network fault. The slot count is now
  a named `MAX_HEADERS = 128`; the real memory guard, `PYLON_MAX_HEAD_BYTES`
  (bounding the head's total size, checked before parsing), is unchanged, so
  raising the slot count costs stack, not slowloris resistance. A head that
  still overruns 128 fields now gets `431 Request Header Fields Too Large`
  in the same Pusher JSON error shape every other REST error uses, on both
  the REST plane and a WS upgrade's opening handshake (RFC 6455 §4.1 permits
  an HTTP error status in place of the 101). hyper's own h1 layer
  separately capped REST requests at its own default of 100 header fields,
  independent of and lower than this transport's limit, so a request in the
  101-128 field band cleared `read_head` but then hit a bare, non-JSON 431
  built inside hyper before axum's router ever ran — the REST listener's h1
  builder now sets `max_headers` to the same `MAX_HEADERS` constant so the
  two ceilings agree.
- **Per-app Prometheus gauges (`pylon_connections`, `pylon_channels_occupied`,
  `pylon_subscriptions`) no longer vanish from `/metrics` when a configured
  app goes idle** — the per-app section was built purely from the live
  `conn_counts` map, which the worker prunes back to nothing the moment an
  app's connection count returns to zero; no series meant no evaluation, so
  a `pylon_connections{app="x"} == 0` alert rule silently stopped firing
  exactly when the app went dark. `AppManager` gained a `known_app_ids()`
  method (`Some` for the static-file store, whose full app set is fixed at
  startup and bounded in memory; `None`, unchanged, for the SQL/Mongo
  backends, whose id space is unbounded) that `/metrics` now uses to seed
  the per-app map at zero before overlaying live counts, so a
  configured-but-idle static app keeps reporting `0` — HELP/TYPE lines
  included — instead of disappearing. SQL/Mongo-backed stores are
  unaffected: a series there still only appears once an app has had a
  tracked connection and drops again at zero, so alert rules against those
  backends still need an `absent()`-aware condition, not a bare `== 0`
  comparison.
- **Small hosts now keep a real memory budget instead of it saturating to
  zero** — `memory_budget` subtracted a reserve of `max(1.5 GiB, 7%)` from
  the effective envelope; any envelope at or below 1.5 GiB saturated the
  reserve to the whole envelope, zeroing the budget. A budget of `0` is
  read downstream as "unconfigured": `shed_band` pins to `Normal`
  (disabling the REST 503 admission gate, the WS client-event ingress drop,
  and the subscribe-time memory-pressure gate), and
  `resolved_max_connections` treats it as an unlimited connection ceiling —
  so the smaller the host, the less overload protection it got, exactly
  backwards from what a memory-constrained box needs. A 1 GiB container
  limit is an entirely ordinary deployment. The reserve is now capped at
  half the effective envelope — `min(max(1.5 GiB, 7%), 50%)` — so a host at
  or below the ~3 GiB crossover keeps a real, proportionate, non-zero
  budget; hosts above the crossover are arithmetically unaffected (verified
  at 4 GiB and 256 GiB). A startup `warn` now fires, naming the disabled
  controls, whenever the resolved budget is still `0` — a genuinely
  unconfigured envelope stays possible, but is no longer silent.
- Conformance harness hardening batch: the pusher-js runner's `fire()`
  helper now bounds its `--fire-stdin` child (8s timeout, SIGTERM kill
  signal) — the last unbounded child wait in the runner; the run's scratch
  env dir is removed on early-error paths too (RAII guard; previously it
  leaked when the auth/webhook/pylon spawns or the health check failed);
  C-PING's connect wait is capped at 6s so the worst case (6s connect + 8s
  hold) fits the 15s catalog budget; C-EVENT-LIMITS' payload leg counts its
  expected 4301 rejection from a per-leg baseline instead of the name leg's
  cumulative count (the legs are independent observations);
  `--audit`'s listing↔catalog cross-check is bidirectional (a runner
  listing an id the catalog does not bind is now flagged, not just the
  missing-binding direction); audit-fixture temp dirs are unique
  (pid + counter) so concurrent audits on one host cannot collide.
- **Duplicate `member_removed` webhooks/events in cluster mode** (audit G11
  class, follow-up F-6): the Redis sweeper's stale-member reap ran
  HGET→HDEL→HINCRBY as SEPARATE commands and gated its emission on `<= 0`,
  so a stale-heartbeat member racing the socket's orderly live leave (whose
  atomic `PRESENCE_LEAVE_LUA` emits on `== 0`) could double-decrement the
  user's refcount and fire a second `member_removed` for one user removal.
  The reap is now one `REAP_MEMBER_LUA` compare-and-swap — the member analog
  of the `channel_vacated` vacate CAS: the emission right belongs to whichever
  caller's atomic op takes the refcount to exactly 0. The script resolves the
  stale token to its user, decrements (or, on the 1→0 edge, removes the user
  from `presusers`/`presinfo`) and returns `won`; the sweeper enqueues the
  compensating cross-node `member_removed` + webhook only on `won`. Redis
  serializes the two scripts, so exactly one of {live leave, sweeper reap} can
  ever observe the 1→0 edge — the live path needed no change (after a reap
  win HDELs the refcount field, a racing live leave returns −1, not 0, and
  its `== 0` gate stays silent).

### Security
- Webhook SSRF classifier: NAT64 (`64:ff9b::/96`), 6to4 (`2002::/16`), and
  class-E reserved (`240.0.0.0/4`) targets are now classified as private. A
  NAT64 gateway and a 6to4 relay both translate the embedded IPv4 address
  into interior address space, so both public and private embedded v4s are
  refused for either prefix; class E has no legitimate webhook receivers.
  `PYLON_WEBHOOK_ALLOW_PRIVATE_TARGETS=1` still relaxes the whole address
  classification.
- Dependency advisories (lockfile-only bumps; no `Cargo.toml` changes):
  `anyhow` 1.0.102→1.0.103 (RUSTSEC-2026-0190), `crossbeam-epoch`
  0.9.18→0.9.20 (RUSTSEC-2026-0204), `event-listener` 5.4.1→5.4.2
  (RUSTSEC-2026-0221), `h2` 0.4.14→0.4.16 (RUSTSEC-2026-0258), and
  `quinn-proto` 0.11.14→0.11.15 (GHSA-4w2j-m93h-cj5j / RUSTSEC-2026-0185);
  the re-resolution also pruned an orphaned `concurrent-queue` 2.5.0 lock
  entry (zero reverse deps).
- Triaged RUSTSEC-2023-0071 (`rsa` 0.9.10, Marvin-attack timing sidechannel —
  no fixed release exists): `rsa` reaches the build only via `sqlx-mysql`,
  whose sole use is client-side RSA *public-key* encryption (OAEP) of the
  nonce-XORed password during `caching_sha2_password`/`sha256_password` full
  auth; the advisory's attack surface is private-key operations, which pylon
  never performs — the vulnerable code path is unreachable.
- Noted RUSTSEC-2025-0134 (`rustls-pemfile` 2.2.0, unmaintained —
  informational, not a vulnerability): no fixed version exists; migrating to
  a maintained alternative is a future option, not part of this change.

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
