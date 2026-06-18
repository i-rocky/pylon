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

### Unit and integration tests

```bash
cargo test
```

This runs all unit tests and the integration test suite (connection lifecycle,
protocol frames, TLS, webhooks, fan-out, graceful drain, etc.).

For deterministic, resource-isolated runs, pass `--test-threads=1`:

```bash
cargo test -- --test-threads=1
```

CI always uses `--test-threads=1` for the main suite.

### Cluster / Redis tests

Tests that exercise the clustered path or the Redis adapter require a local
Redis instance. Point at it with the `PYLON_TEST_REDIS_URL` environment
variable:

```bash
PYLON_TEST_REDIS_URL=redis://127.0.0.1:6379 \
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
