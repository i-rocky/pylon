# syntax=docker/dockerfile:1
#
# Multi-stage build for pylon: static musl binary, scratch runtime.
#
# HOST REQUIREMENTS (containers inherit host kernel settings):
#   • Apply deploy/systemd/99-pylon.sysctl.conf on the host (sysctl --system)
#     before starting the container.
#   • Pass --ulimit nofile=2000000:2000000 to `docker run`, or set
#     `ulimits` in docker-compose.yml (see docker-compose.yml).
#
# Health probes (for orchestrators): probe over HTTP from OUTSIDE the
# container — a scratch image carries no shell, curl or wget.
#   Liveness:   GET /health  → 200 "ok"      (process is alive)
#   Readiness:  GET /ready   → 200 "ready"   (accepting traffic)
#                            → 503 "draining" (shutdown in progress)

# ── Stage 1: builder (musl host toolchain; crt-static by default) ────────────
FROM rust:1.98.1-alpine3.24 AS builder

# build-base supplies the C toolchain the bundled sqlite (libsqlite3-sys) and
# mimalloc compile with; musl-dev is its libc headers.
RUN apk add --no-cache build-base musl-dev

WORKDIR /build

# Cache dependency compilation separately from application source.
# `rust-toolchain.toml` must be copied too, or the pin is silently ignored and
# the image builds with whatever the base image happens to ship.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
# The root manifest declares a workspace member (`load`) and four `[[bench]]`
# targets; cargo fails to parse the manifest if either is absent, so both must
# be present before any `cargo build` runs — including the dependency-cache one.
COPY load ./load
COPY benches ./benches
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs && \
    cargo build --release --locked && \
    rm -rf src

COPY src ./src
RUN touch src/main.rs && \
    cargo build --release --locked

# ── Stage 2: CA bundle ───────────────────────────────────────────────────────
FROM debian:trixie-20260824-slim AS certs
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# ── Stage 3: runtime ─────────────────────────────────────────────────────────
FROM scratch AS runtime

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY deploy/docker/passwd.nobody /etc/passwd
COPY --from=builder /build/target/release/pylon /usr/local/bin/pylon

EXPOSE 7000

USER 65534:65534

ENTRYPOINT ["/usr/local/bin/pylon"]
