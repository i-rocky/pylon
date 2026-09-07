# Release Process

Releases are tag-driven. Pushing a `vX.Y.Z` tag triggers the GitHub Actions
release workflow, which builds native binaries and a multi-arch container image.

---

## Steps

1. **Bump the version** in `Cargo.toml` (the root workspace manifest):

    ```toml
    [package]
    version = "1.2.3"
    ```

2. **Commit** the version bump:

    ```bash
    git commit -am "chore: release v1.2.3"
    ```

3. **Tag and push:**

    ```bash
    git tag v1.2.3
    git push origin v1.2.3
    ```

    Pushing the tag to `origin` is the trigger — the release workflow does not
    run on branch pushes.

---

## What the Workflow Builds

The release workflow (`.github/workflows/release.yml`) runs three jobs:

**`binaries`** — Builds the `pylon` binary natively for two architectures:

| Target | Runner |
|---|---|
| `x86_64-unknown-linux-gnu` | `ubuntu-22.04` |
| `aarch64-unknown-linux-gnu` | `ubuntu-22.04-arm` |

Each binary is stripped, packaged into a `.tar.gz` archive alongside the
`LICENSE`, `README.md`, and `apps.example.json`, and a `.sha256` checksum is
produced.

**`image`** — Assembles a multi-arch container image from the prebuilt binaries
using Docker Buildx and pushes it to:

```
ghcr.io/i-rocky/pylon:<version>
ghcr.io/i-rocky/pylon:<major>.<minor>
ghcr.io/i-rocky/pylon:latest
```

The image is built for `linux/amd64` and `linux/arm64`.

**`release`** — Creates a GitHub Release for the tag and uploads the `.tar.gz`
archives and checksums as release assets. Release notes are auto-generated from
the commit history since the previous tag.

---

## CI (Non-Release Builds)

The CI workflow (`.github/workflows/ci.yml`) runs on every push to `master` and
on all pull requests. It gates on:

1. `cargo fmt --all --check`
2. `cargo clippy --all-targets --locked -- -D warnings`
3. `cargo clippy --locked --lib --bins -- -D warnings` — default features only. A dev-dependency
   self-reference enables the `test-hooks` feature whenever test targets are in the build graph, so
   step 2 alone cannot see warnings that only appear in the default-features build (`cargo build
   --release`, `cargo install`); this step is the one that catches those.
4. Unit + integration tests (`--test-threads=1`)
5. Cluster / Redis tests, against a real Redis service container
6. DB-backed app-manager tests (MySQL, Postgres, Mongo service containers)

**Every one of these is blocking.** Steps 5 and 6 run with `--no-fail-fast` so
each suite reports its own result rather than halting at the first failure, but
a failure in either still fails the job — nothing in the workflow is marked
`continue-on-error`.

A second top-level job, `failover`, runs the Redis failover/self-heal
regression: it spawns a dedicated throwaway Redis container, bounces it, and
asserts that cross-node delivery resumes. It is blocking too.

The Rust toolchain is pinned by `rust-toolchain.toml` in the repository root;
`rustup show` installs it automatically in both CI and release jobs, so local
builds, CI, and release artifacts all use the same compiler.
