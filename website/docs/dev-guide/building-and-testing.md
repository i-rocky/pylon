# Building & Testing

---

## Toolchain

The Rust toolchain is pinned in `rust-toolchain.toml` at the repository root.
`rustup` reads this file automatically, so the first build on a fresh checkout
installs the exact compiler, rustfmt, and clippy versions used by CI.

```toml
[toolchain]
channel = "1.96.0"
components = ["rustfmt", "clippy"]
profile = "minimal"
```

No manual `rustup override` is needed.

---

## Building

```bash
cargo build           # debug build
cargo build --release # optimised build → target/release/pylon
```

---

## Testing

!!! warning "A bare `cargo test` needs all four services"
    Several test binaries connect to a real service before their first test
    can even run, and Cargo stops the whole run at the first binary that
    fails. On a machine that's missing one of them — say, no MongoDB —
    `cargo test` dies at `mongo_app_manager`'s 30-second connection timeout,
    and every binary Cargo would have run after it (`percore*`,
    `postgres_app_manager`, `redis_*`, `rest`, `signin`, `tls`, `watchlist`,
    `webhooks`) never runs at all — including the ones that need no
    infrastructure. Use the commands below instead of a bare `cargo test`.

### Tests that need no infrastructure

This is the primary, always-on gate CI runs, and needs nothing but the pinned
toolchain. Run it before opening a pull request if you don't have the
services below available locally:

```bash
cargo test --locked --lib \
  --test admin --test health --test integration --test metrics \
  --test percore --test percore_drain --test percore_liveness \
  --test percore_multiworker --test percore_nonblocking_establish \
  --test percore_overload --test percore_selective_drain \
  --test rest --test signin --test tls --test watchlist --test webhooks \
  -- --test-threads=1
```

### Full suite (all services)

Running everything — the clustered/Redis suites and the per-backend
AppManager suites included — requires all four services below:

| Service | Port | Env var |
|---|---|---|
| Redis | 6390 | `PYLON_TEST_REDIS_URL` |
| MySQL 8 | 3307 | `PYLON_TEST_MYSQL_URL` |
| Postgres 16 | 5433 | `PYLON_TEST_POSTGRES_URL` |
| Mongo 7 | 27018 | `PYLON_TEST_MONGO_URL` |

[`deploy/docker/docker-compose.test.yml`](https://github.com/i-rocky/pylon/blob/master/deploy/docker/docker-compose.test.yml)
brings up all four on those ports: `docker compose -f deploy/docker/docker-compose.test.yml up -d`.

With those running and reachable, export the env vars above explicitly —
explicitly targeting each test instance is safer and clearer for potentially
destructive operations — and pass `--no-fail-fast` so one missing or
misbehaving backend doesn't hide the results of the others:

```bash
export PYLON_TEST_REDIS_URL=redis://127.0.0.1:6390
export PYLON_TEST_MYSQL_URL=mysql://root:pylon@127.0.0.1:3307/pylon_test
export PYLON_TEST_POSTGRES_URL=postgres://postgres:pylon@127.0.0.1:5433/pylon_test
export PYLON_TEST_MONGO_URL=mongodb://127.0.0.1:27018/pylon_test

cargo test --locked --no-fail-fast -- --test-threads=1
```

### Cluster / Redis tests

Tests that exercise the clustered path or the Redis adapter require a local
Redis instance. Point at it with the `PYLON_TEST_REDIS_URL` environment
variable:

```bash
PYLON_TEST_REDIS_URL=redis://127.0.0.1:6390 \
  cargo test --test cluster_bridge --test redis_cluster -- --test-threads=1
```

!!! warning "Never FLUSH a shared Redis"
    Tests isolate themselves with random key prefixes. Do **not** run
    `FLUSHALL` or `FLUSHDB` on a Redis instance that holds data you care
    about — and never point `PYLON_TEST_REDIS_URL` at a production Redis.

Cluster tests must run serially (`--test-threads=1`) because several of them
assert on short Redis round-trip timing windows that race under parallel
execution.

---

## Formatting and Linting

Both are gated in CI on every push and pull request:

```bash
cargo fmt --all --check   # check formatting (CI gate)
cargo fmt --all           # apply formatting (before committing)

cargo clippy --all-targets --locked -- -D warnings   # lint (CI gate; zero warnings allowed)
```

---

## Load-Testing Crate

The `load/` workspace crate contains scenario-based load tests and the
`pylon-ceiling` capacity-finder binary. `pylon-ceiling` performs a binary
search over connection counts to find the maximum sustainable concurrency on a
given host, taking latency, CPU, and memory constraints as stop criteria.

See [`load/`](https://github.com/i-rocky/pylon/tree/master/load) for details
on running load scenarios and the ceiling tool.
