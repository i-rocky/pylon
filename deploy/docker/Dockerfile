# syntax=docker/dockerfile:1
#
# Multi-stage build for pylon.
#
# HOST REQUIREMENTS (containers inherit host kernel settings):
#   • Apply deploy/systemd/99-pylon.sysctl.conf on the host (sysctl --system)
#     before starting the container.
#   • Pass --ulimit nofile=2000000:2000000 to `docker run`, or set
#     `ulimits` in docker-compose.yml (see docker-compose.yml).
#
# Health probes (for orchestrators / docker run --health-cmd):
#   Liveness:   GET /health  → 200 "ok"      (process is alive)
#   Readiness:  GET /ready   → 200 "ready"   (accepting traffic)
#                            → 503 "draining" (shutdown in progress)

# ── Stage 1: builder ─────────────────────────────────────────────────────────
FROM rust:1-bookworm AS builder

WORKDIR /build

# Cache dependency compilation separately from application source.
# `rust-toolchain.toml` must be copied too, or the pin is silently ignored and
# the image builds with whatever 1.x the base image happens to ship.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
# The root manifest declares a workspace member (`load`) and four `[[bench]]`
# targets; cargo fails to parse the manifest if either is absent, so both must
# be present before any `cargo build` runs — including the dependency-cache one.
COPY load ./load
COPY benches ./benches
# Create a stub lib/main so `cargo build` can compile deps.
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs && \
    cargo build --release --locked && \
    rm -rf src

# Now compile the real source.
COPY src ./src
# Touch main.rs so cargo sees it as newer than the cached stub artifact.
RUN touch src/main.rs && \
    cargo build --release --locked

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# ca-certificates: the trust store behind TLS to Redis (rediss://) and outbound
# webhook HTTPS — pylon verifies against it, not a bundled root set. wget: the
# container HEALTHCHECK / compose healthcheck probes GET /health (debian-slim
# ships neither curl nor wget).
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates wget && \
    rm -rf /var/lib/apt/lists/*

# Create a non-root user.
RUN useradd --system --no-create-home --shell /usr/sbin/nologin pylon

COPY --from=builder /build/target/release/pylon /usr/local/bin/pylon

EXPOSE 7000

USER pylon

ENTRYPOINT ["/usr/local/bin/pylon"]
