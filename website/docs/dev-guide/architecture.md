# Architecture

Pylon is a single Rust binary built around a lock-free, per-core transport.
This page describes the major subsystems and how data flows through them.

---

## Per-Core Transport

The entry point is [`src/transport/mod.rs`](https://github.com/i-rocky/pylon/blob/master/src/transport/mod.rs).
`run_percore` spawns exactly one OS thread per logical CPU, each running an
independent [`mio`](https://docs.rs/mio) event loop
([`src/transport/worker.rs`](https://github.com/i-rocky/pylon/blob/master/src/transport/worker.rs)).

**Accept sharding via `SO_REUSEPORT`.** Every worker calls
`reuseport_listener` to bind the *same* `bind:port` with `SO_REUSEPORT`. The
kernel load-balances incoming TCP connections across the workers' accept queues,
so no single thread is a bottleneck for accepts.

**`slab`-keyed connection table.** Each worker maintains a
[`slab::Slab<Entry>`](https://docs.rs/slab), where the slab key doubles as the
`mio::Token`. A readiness event maps directly to its `Connection` in O(1)
without a hash lookup. There is no task per connection and no thread-pool
dispatch — the entire protocol runs on the worker thread that owns the
connection.

**Edge-triggered I/O.** Connections are registered `READABLE`-only; writable
interest is added only when a `flush` returns `WouldBlock`, then cleared again
once the queue drains. This prevents spinning on idle sockets.

**Worker pinning.** Each worker thread is pinned to a specific CPU core via
`core_affinity` when the OS supports it, improving L1/L2 cache locality for
connection state.

Source: [`src/transport/worker.rs`](https://github.com/i-rocky/pylon/blob/master/src/transport/worker.rs)

---

## Encode-Once Sharded Fan-Out

Source: [`src/transport/fanout.rs`](https://github.com/i-rocky/pylon/blob/master/src/transport/fanout.rs)

The conventional approach (one `mpsc` send per subscriber) serialises fan-out
on the publishing thread. Pylon replaces it with a per-worker hand-off:

1. When a channel event is published, the payload is **encoded once** into a
   complete WebSocket text frame and wrapped in `Arc<[u8]>`.
2. A `BroadcastSink` hands this single `Arc` to every worker via a
   **bounded** `sync_channel` — one message per worker, not one per subscriber.
3. Each worker's event loop drains its inbox and fans the frame out to its
   **own** local subscribers by direct slab-enqueue. Each enqueue is a cheap
   `Arc` reference-count bump; no per-connection re-encode occurs.

The bounded hand-off channel (capacity configurable via
`PYLON_BROADCAST_HANDOFF_CAP`) prevents a publish flood from growing unbounded
memory. A `try_send` that finds the channel full drops the message and sets a
`saturated` flag — the HTTP publish endpoint returns 503 while this flag is set.

Source:
[`src/transport/fanout.rs`](https://github.com/i-rocky/pylon/blob/master/src/transport/fanout.rs),
[`src/transport/worker.rs`](https://github.com/i-rocky/pylon/blob/master/src/transport/worker.rs)
(`drain_broadcasts`)

---

## Selective Mailbox Drain

Direct sends — `subscription_succeeded`, presence rosters, `send_to_user`,
cluster follow-ups — arrive via a per-connection mailbox rather than the
broadcast sink. When a mailbox message is enqueued, `Mailbox::send` pushes the
target connection's slab token onto a `dirty_tx` channel and wakes the worker's
single `mio::Waker`. The worker's post-poll loop drains only the connections in
the `dirty_set` (O(dirty), not O(N)), so idle connections are never visited.

---

## Adapter Seam

Source: [`src/adapter/mod.rs`](https://github.com/i-rocky/pylon/blob/master/src/adapter/mod.rs)

The `Adapter` trait abstracts all channel/presence/user state so the protocol
handler in `src/ws/handler.rs` is identical in single-node and clustered
deployments. Two implementations sit behind the trait:

- **`LocalAdapter`** — in-process `DashMap`-backed state for single-node
  deployments.
- **Clustered path** — `ClusterBridge` owns a dedicated tokio runtime and a
  `RedisAdapter` that shards state through Redis. Workers hold a cheap-clone
  `ClusterHandle` and fire **fire-and-forget** `ClusterCmd`s over a bounded
  mpsc channel. The bridge's runtime thread drains the channel and executes each
  command against the `RedisAdapter`. Cross-node broadcasts are re-delivered to
  the local workers via the `LocalAdapter`'s broadcast sink (the same
  `BroadcastSink` that serves local publishes), so no extra delivery code is
  needed.

Zero handler changes are required to switch between modes.

Source:
[`src/cluster/bridge.rs`](https://github.com/i-rocky/pylon/blob/master/src/cluster/bridge.rs),
[`src/adapter/local.rs`](https://github.com/i-rocky/pylon/blob/master/src/adapter/local.rs),
[`src/adapter/redis/`](https://github.com/i-rocky/pylon/blob/master/src/adapter/redis/)

---

## Adaptive Overload Control

Source:
[`src/transport/conn.rs`](https://github.com/i-rocky/pylon/blob/master/src/transport/conn.rs),
[`src/transport/mod.rs`](https://github.com/i-rocky/pylon/blob/master/src/transport/mod.rs)

Pylon degrades gracefully rather than collapsing under overload. Several
mechanisms work in concert:

**Drop-head per-connection queues.** Each connection's outbound queue is
byte-bounded (`high_water`, sized from the per-worker memory budget and
`PYLON_EXPECTED_CONNS_PER_WORKER`). When a new frame would exceed the cap, the
*oldest* droppable frame(s) are evicted first (freshest-wins). A frame currently
mid-write is never evicted — dropping it would corrupt the byte stream.

**CoDel staleness drop.** Folly's Controlled Delay algorithm is applied on
dequeue: the time-in-queue (sojourn) of each frame is measured across loop
iterations. When a connection's minimum sojourn over a 100 ms window exceeds the
5 ms target, the queue enters an "overloaded" regime and stale frames are dropped
before sending, so cores always emit the freshest data available. Configurable
via `PYLON_CODEL_TARGET_MS` / `PYLON_CODEL_INTERVAL_MS`.

**Graduated shedding.** The worker tracks total queued bytes across all its
connections (`inflight_bytes`, maintained incrementally — O(work), not
O(connections)). At 80 % of the per-worker budget, backed-up connections are
skipped during fan-out; at 95 %, only fully-drained connections receive
broadcasts; at 100 %, all broadcasts are dropped and the saturated flag is set.

**PSI backstop.** A control-plane task (not a worker) polls the Linux
`/proc/pressure/memory` pressure file approximately once per second. When the
`full avg10` value exceeds the configured threshold, the shared `budget_factor`
(fixed-point ×1000) is multiplied down toward a 0.8× floor; when pressure
clears, it ramps back toward 1.0×. Workers read this factor once per loop
iteration (never inline in the hot path) and scale their effective budget by it.

**Graceful shutdown.** On `SIGTERM`, each worker deregisters its listener, sends
a `pusher:error` 4200 frame and a WebSocket Close(4200) to every open
connection, then flushes until all bytes are delivered or a configurable grace
period (`PYLON_SHUTDOWN_GRACE_MS`) expires.

---

## Safety

The crate root declares `#![deny(unsafe_code)]`. The entire codebase is safe
Rust with a single audited `unsafe` site: the fd-transfer path in
[`src/transport/rest.rs`](https://github.com/i-rocky/pylon/blob/master/src/transport/rest.rs),
which hands off an accepted TCP file descriptor from a worker thread to the
tokio/axum REST plane.

---

## Data Flow (Summary)

```
 Client TCP → SO_REUSEPORT kernel sharding
      │
      ▼  (one thread per core)
 Worker mio event loop
   ├─ Handshake (HTTP Upgrade)
   ├─ WS frame decode → ClientCommand
   ├─ Dispatch to ConnectionContext (handler)
   ├─ Broadcast inbox drain → slab-enqueue (Arc bump per subscriber)
   └─ Selective mailbox drain (dirty-token set)
      │
      ▼ (Adapter trait)
 LocalAdapter (single-node)  │  ClusterBridge → RedisAdapter (multi-node)
```

For operational details see [Clustering & Scaling](../user-guide/clustering.md)
and [Production Tuning](../user-guide/production-tuning.md).
