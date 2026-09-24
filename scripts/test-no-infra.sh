#!/usr/bin/env bash
set -euo pipefail
cargo test --locked --lib \
  --test admin --test health --test integration --test metrics \
  --test percore --test percore_drain --test percore_liveness \
  --test percore_multiworker --test percore_nonblocking_establish \
  --test percore_overload --test percore_selective_drain \
  --test percore_wiring --test percore_worker_panic \
  --test readiness_states \
  --test rest --test signin --test tls --test watchlist --test webhooks \
  -- --test-threads=1
