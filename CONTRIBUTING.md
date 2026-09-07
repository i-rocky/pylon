# Contributing to Pylon

Thanks for your interest in improving Pylon. This document covers how to build, test, and submit
changes.

## Development setup

A recent stable Rust toolchain is required.

```sh
cargo build              # debug build
cargo build --release    # optimized build → target/release/pylon
```

Run the server locally:

```sh
cp apps.example.json apps.json   # set id/key/secret
cargo run --release
```

## Tests

```sh
cargo test               # full suite — requires Redis, MySQL, Postgres, and Mongo (see below)
```

`cargo test` builds and runs every test binary in the workspace, and several of them fail loudly
if their service isn't reachable. Cargo stops the whole run at the first binary that fails, so on
a machine missing a service, everything ordered after it never runs either — including binaries
that need no infrastructure at all. If you don't have all four services below running locally, run
the infrastructure-free subset instead; see the "Testing" section of the
[dev guide](website/docs/dev-guide/building-and-testing.md) for the exact command.

Cluster and Redis-backed tests (e.g. `cluster_bridge`, `redis_cluster`, `percore_cluster`) require a
local Redis and **fail loudly without one** — they default to `redis://127.0.0.1:6390` (port 6390,
not the 6379 production default, so a stray run never clobbers a real instance) and refuse to
silently pass. Export `PYLON_TEST_REDIS_URL` to point them elsewhere. The one exception is
`redis_failover`, which is opt-in via `PYLON_TEST_REDIS_FAILOVER=1` because it bounces the Redis
container and would disrupt parallel suites (CI runs it against a dedicated container). Tests use
random key prefixes for isolation — never run them against a Redis that holds data you care about,
and never `FLUSHALL`/`FLUSHDB` a shared instance.

The `mysql_app_manager`, `postgres_app_manager`, and `mongo_app_manager` suites are the same story
against their own backend — each fails loudly (no silent pass) if it can't connect, applies its own
schema, and isolates its rows with a UUID prefix, so it's safe to point at a shared database:

- **MySQL 8** — `PYLON_TEST_MYSQL_URL`, defaults to `mysql://root:pylon@127.0.0.1:3307/pylon_test`
- **Postgres 16** — `PYLON_TEST_POSTGRES_URL`, defaults to `postgres://postgres:pylon@127.0.0.1:5433/pylon_test`
- **Mongo 7** — `PYLON_TEST_MONGO_URL`, defaults to `mongodb://127.0.0.1:27018/pylon_test`

## Before you open a pull request

- **Format:** `cargo fmt --all`
- **Lint:** `cargo clippy --all-targets -- -D warnings` (the tree is kept warning-clean)
- **Test:** at minimum, the infrastructure-free suite (see "Tests" above); the full suite if you
  have the four services running
- Add or update tests for behavior you change. New behavior should come with a failing test first.
- Keep changes focused; prefer small, well-scoped commits with clear messages.

## Pusher compatibility

Pylon aims for faithful parity with **hosted Pusher Channels** (protocol v7 and the HTTP API). When
a behavior is ambiguous, hosted Pusher's documented behavior is the source of truth. If a change
affects wire format, error codes, signatures, or REST semantics, call that out explicitly in the PR
and reference the relevant Pusher behavior.

## Reporting bugs and security issues

- **Bugs / features:** open a GitHub issue with a clear description and, ideally, a reproduction.
- **Security vulnerabilities:** do **not** open a public issue — follow [SECURITY.md](SECURITY.md).

## License

By contributing, you agree that your contributions will be licensed under the Apache License,
Version 2.0, consistent with the rest of the project. See [LICENSE](LICENSE).
