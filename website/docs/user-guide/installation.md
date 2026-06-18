# Installation

## Requirements

- **OS:** Linux (amd64 or arm64). The Docker image and prebuilt binaries target `glibc 2.35+`.
- **Clustering:** A Redis instance is required only when running multiple Pylon nodes. Single-node
  deployments have no external dependencies.

## Install

=== "Docker"

    A multi-arch image (`linux/amd64` and `linux/arm64`) is published to the GitHub Container
    Registry on every release.

    ```sh
    docker pull ghcr.io/i-rocky/pylon:latest
    ```

    Available tags: `latest`, `X.Y.Z` (exact release), and `X.Y` (latest patch in a minor series).

    To run Pylon with Docker, mount your `apps.json` and expose port 7000:

    ```sh
    docker run -d --name pylon \
      -p 7000:7000 \
      -v "$PWD/apps.json:/etc/pylon/apps.json:ro" \
      -e PYLON_APPS_PATH=/etc/pylon/apps.json \
      --ulimit nofile=1048576:1048576 \
      ghcr.io/i-rocky/pylon:latest
    ```

    !!! tip
        The `--ulimit nofile=1048576:1048576` flag raises the open-file limit so Pylon can
        handle large numbers of concurrent WebSocket connections.

=== "Binary"

    Each tagged release on the [GitHub Releases page](https://github.com/i-rocky/pylon/releases)
    includes prebuilt Linux binaries for `x86_64` and `aarch64` (glibc 2.35+), each packaged as a
    `.tar.gz` with a matching `.sha256` checksum file.

    Download, verify, and extract:

    ```sh
    # Replace X.Y.Z and ARCH (x86_64 or aarch64) as appropriate
    curl -LO https://github.com/i-rocky/pylon/releases/download/vX.Y.Z/pylon-X.Y.Z-ARCH-unknown-linux-gnu.tar.gz
    curl -LO https://github.com/i-rocky/pylon/releases/download/vX.Y.Z/pylon-X.Y.Z-ARCH-unknown-linux-gnu.tar.gz.sha256

    sha256sum -c pylon-X.Y.Z-ARCH-unknown-linux-gnu.tar.gz.sha256
    tar xzf pylon-X.Y.Z-ARCH-unknown-linux-gnu.tar.gz
    ./pylon --version
    ```

=== "From source"

    Building from source requires a Rust toolchain. Pylon pins its toolchain version in
    `rust-toolchain.toml` — [rustup](https://rustup.rs/) will download the correct version
    automatically.

    ```sh
    git clone https://github.com/i-rocky/pylon.git
    cd pylon
    cargo build --release
    ```

    The binary is written to `target/release/pylon`.

    ```sh
    ./target/release/pylon --version
    ```

    You can also run directly without a separate build step:

    ```sh
    cargo run --release
    ```
