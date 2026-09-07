# Contributing

Full contribution guidelines are in
[`CONTRIBUTING.md`](https://github.com/i-rocky/pylon/blob/master/CONTRIBUTING.md)
in the repository root. This page summarises the key expectations.

---

## Build and Test Expectations

Before opening a pull request:

1. **Format:** run `cargo fmt --all` and confirm the tree is clean.
2. **Lint:** run BOTH `cargo clippy --all-targets --locked -- -D warnings` AND
   `cargo clippy --locked --lib --bins -- -D warnings`. A dev-dependency
   self-reference enables the `test-hooks` feature whenever test targets are
   in the build graph, so `--all-targets` alone cannot see warnings that only
   appear in the default-features build (`cargo build --release`, `cargo
   install`) our users actually run — CI gates on both, and zero warnings are
   permitted on either.
3. **Test:** run the infrastructure-free suite at minimum (see
   [Building & Testing](building-and-testing.md#tests-that-need-no-infrastructure));
   run the full suite, or the relevant cluster tests with `PYLON_TEST_REDIS_URL`
   set, if your change touches the cluster or Redis adapter. A bare `cargo
   test` is not the gate: it builds and runs every test binary in the
   workspace and stops at the first one whose backing service isn't reachable,
   silently skipping every binary Cargo would have run after it. Use
   `--test-threads=1` for deterministic results.
4. **Add or update tests** for any behaviour you change. New behaviour should
   have a failing test first.

See [Building & Testing](building-and-testing.md) for exact commands.

---

## Pusher Parity Rule

Pylon aims for faithful parity with **hosted Pusher Channels** (protocol v7
and the HTTP API). When behaviour is ambiguous, hosted Pusher's documented
behaviour is the source of truth.

If a change affects wire format, error codes, authentication signatures, or
REST semantics, call that out explicitly in the PR description and reference the
relevant Pusher documentation or observed behaviour.

---

## Pull Request Process

- Keep changes focused; prefer small, well-scoped commits with clear messages.
- Reference any related issue in the PR description.
- The CI pipeline (fmt → clippy → test) must be green.

---

## Security Issues

Do **not** open a public GitHub issue for security vulnerabilities. Follow the
responsible disclosure process described in
[`SECURITY.md`](https://github.com/i-rocky/pylon/blob/master/SECURITY.md).

---

## License

By contributing, you agree that your contributions will be licensed under the
Apache License, Version 2.0, consistent with the rest of the project.
