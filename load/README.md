# Pylon Load-Test Harness (`pylon-load`)

Workspace crate with two binaries: `pylon-load` (fixed scenario harness against
an already-running pylon) and `pylon-ceiling` (empirical capacity-envelope
finder that spawns and manages its own pylon child).

```sh
cargo run -p pylon-load --release --bin pylon-load -- --help
cargo run -p pylon-load --release --bin pylon-ceiling -- --help
```

---

## `pylon-load` — scenarios

All scenarios point at a running server via `--url` (WebSocket) and `--rest`,
authenticating REST triggers with `--app-id` / `--key` / `--secret`. Key flags:
`--conns` (default 1000), `--rate` (events/sec, default 10), `--secs`
(measured duration, default 10), `--ramp-per-sec` (connection ramp; default
2000), `--publishers` (fanout only), `--channels` (channels only), `--private`
(sign subscribes to private channels), `--server-pid` (sample the server's
CPU/RSS during the run), `--client-ips` (spread client sockets across loopback
aliases for ephemeral-port headroom).

| Scenario (`--scenario`) | What it measures |
|---|---|
| `connect` | Ramp N mostly-idle subscribers onto one channel, hold, then fire **one** broadcast — reports time-to-subscribed, the single-shot fan-out latency, and peak server RSS / bytes-per-connection. The connection-density check. |
| `fanout` | N subscribers on one channel; `--publishers` concurrent publishers each push at `--rate`/sec for `--secs` — sustained throughput, delivery latency percentiles, and drop counts under continuous fan-out. |
| `channels` | N connections spread across `--channels` channels; a publisher round-robins events at `--rate`/sec — the many-channels shape (registry lookups instead of one hot channel). |
| `cluster` | Two nodes (`--url` and required `--url-b`) on the Redis adapter; half the clients subscribe to each node, publishing happens on node A only — measures cross-node delivery latency (the Redis pub/sub hop). |

Example — 50k connections, one hot channel, 4 publishers × 100 msg/s for 30 s:

```sh
cargo run -p pylon-load --release --bin pylon-load -- \
  --url ws://127.0.0.1:7000/app/app-key \
  --rest http://127.0.0.1:7000 \
  --app-id app --key app-key --secret app-secret \
  --scenario fanout --conns 50000 --publishers 4 --rate 100 --secs 30
```

---

## `pylon-ceiling` — capacity-envelope finder

Sweeps to the server's limits on **your** hardware and prints a sizing report.
It spawns a core-pinned pylon child (`--pylon-bin`, auto-detected by default),
runs one or both phases, and reports the envelope plus a recommendation
(RAM = target-conns × measured bytes/conn × 1.3 safety factor).

- **Connection phase** — ramps connections in `--conn-batch` batches (default
  20 000) and records the max sustainable count, RSS at max, bytes/conn, and
  conns/GB. **Stop criteria**, in priority order: server RSS exceeds
  `--mem-ceiling-pct` of total RAM (default 80%), connect failures reach
  `--fail-threshold`, the hard `--max-conns` cap is hit, or worker
  backpressure engages.
- **Throughput phase** — open-loop publish-rate ramp (`--rate-start` +
  `--rate-step` per `--step-secs` window) over `--channels` channels with
  `--tput-conns` subscribers, recording deliveries/s, drop %, p50/p99 latency,
  and CPU. **Stop criteria**: deliveries drop, p99 exceeds `--p99-budget-ms`
  (default 100 ms), CPU saturates, or `--max-rate` is reached.

```sh
cargo run -p pylon-load --release --bin pylon-ceiling -- --phase both
cargo run -p pylon-load --release --bin pylon-ceiling -- --phase conn --json
```

Pass `--target-conns` / `--target-rate` to get a deploy recommendation for a
specific workload, and `--json` for machine-readable output. The tool pins the
server child with `taskset` (`--server-cores`, default half the logical cores)
so client and server compete realistically for the machine; run it on hardware
that resembles production for sizing decisions.
