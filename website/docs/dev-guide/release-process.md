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
3. Unit + integration tests (`--test-threads=1`)

Cluster/Redis tests are run in the same CI job (with a Redis service container)
but are marked `continue-on-error: true` because they assert on short Redis
timing windows that can flake on the shared CI instance.

The Rust toolchain is pinned by `rust-toolchain.toml` in the repository root;
`rustup show` installs it automatically in both CI and release jobs, so local
builds, CI, and release artifacts all use the same compiler.
