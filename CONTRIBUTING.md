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
cargo test               # unit + integration suite
```

Some clustering and Redis-backed tests require a local Redis and are gated on an environment
variable (e.g. `PYLON_TEST_REDIS_URL`); see the individual test files under `tests/` for what each
needs. Tests use random key prefixes for isolation — never run them against a Redis that holds data
you care about, and never `FLUSHALL`/`FLUSHDB` a shared instance.

## Before you open a pull request

- **Format:** `cargo fmt --all`
- **Lint:** `cargo clippy --all-targets -- -D warnings` (the tree is kept warning-clean)
- **Test:** `cargo test` (plus the relevant clustering tests if your change touches that path)
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
