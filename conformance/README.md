# pylon SDK conformance harness

This crate answers one question: **does a real pylon speak the protocol the
official SDKs expect?** It boots a real pylon release binary, points the two
official Pusher SDKs at it, and drives every protocol feature those SDKs can
exercise — 26 scenarios covering the client plane (connection, channels,
client events, encrypted channels, presence, watchlists, error paths) and the
server plane (trigger, batch, channel queries, auth signing, webhook
verification).

It is a differential harness by construction: pylon is only ever observed
THROUGH an SDK, so any protocol drift between pylon and the documented
Pusher Channels behavior shows up here as a failing scenario.

## The anti-blindness rule

The harness implements **no auth or webhook crypto of its own**. Every
signature — channel authorization, user signin, webhook verification — is
 produced or verified by the official `pusher-http-node` SDK:

- the harness's auth endpoint receives an SDK auth request and shells out to
  the pusher-http-node runner's `--sign` mode (request on stdin, signed
  response on stdout);
- webhook envelopes are verified with the SDK's webhook verifier, not a
  hand-rolled HMAC check.

This is deliberate: if the harness re-implemented the crypto, it could
re-implement pylon's bugs and call them conformant. Delegating to the SDKs
means the harness is blind to implementation details and honest about
interoperability.

## Prerequisites

- **Node ≥ 18** on PATH (the adapters are Node runners; the exact engine
  requirement is declared in each adapter's `package.json`).
- Adapter dependencies installed once per checkout:

  ```sh
  cd conformance/adapters/pusher-js && npm ci
  cd ../pusher-http-node && npm ci
  ```

- A release pylon binary: run `cargo build --release` in the **repo root**
  first. The harness looks for `<repo>/target/release/pylon` (or pass
  `--pylon-bin`). A debug build works too if you point `--pylon-bin` at it,
  but timings and the reconnect/rate budgets assume release.

The SDKs are pinned exactly (anti-rot): `pusher-js` **8.6.0** and
`pusher-http-node` (npm `pusher`) **5.3.4**, each with a checked-in
`package-lock.json`.

## Running

All commands run from `conformance/`:

```sh
cargo run                       # full suite: 26 scenarios, catalog order
cargo run -- --smoke            # 3-scenario subset: C-ESTABLISH, S-TRIGGER, S-WEBHOOK-VERIFY
cargo run -- --list             # print the catalog (id, sdk, plane, budget, summary)
cargo run -- --audit            # binding-table honesty gate (see below)
cargo run -- --sdk pusher-js    # only one SDK's scenarios
cargo run -- --scenario C-PING  # a single scenario
cargo run -- --report out.json  # JSON artifact path (default conformance-report.json)
cargo run -- --port-base 21000  # first of three ports the harness binds (≤ 65533)
cargo run -- --pylon-bin /path/to/pylon
```

What a run does: write a temp `apps.json` (harness-fixed credentials for the
main app `cf-key-main`/`cf-app-main`, plus a disabled app for `E-DISABLED`),
spawn pylon on `port_base + 2` with it, bind the auth endpoint on
`port_base + 0` and the webhook receiver on `port_base + 1`, write one
`env.json` for the whole run, then execute each selected scenario under its
catalog budget (enforced with a process-group kill — a hung runner cannot
stall the run). At the end the harness prints a human matrix and writes the
JSON artifact. Exit code: `0` all pass/skip, `1` any failure, `2` harness
error (bad flags, missing binary, ...).

The JSON artifact is stable and diffable: run metadata (timestamp, pylon
version, target) plus one entry per scenario with its verdict, observations,
error, and duration.

## `--audit`: keep the catalog honest

The catalog (in `src/catalog.rs`) claims each scenario is implemented by a
specific adapter's `runner.js`. `--audit` verifies that claim:

- every bound `adapters/<sdk>/runner.js` exists;
- each runner's `--list` output actually contains every id bound to it;
- the binding table matches the catalog (no missing, orphaned, wrong-SDK, or
  duplicate bindings).

Run it after any change to the catalog or a runner. CI runs the full suite,
which is itself an end-to-end audit; `--audit` is the cheap early-warning
version.

## Adding a scenario

1. Add the entry to `CATALOG` in `src/catalog.rs` (id, sdk, plane, budget,
   summary). The binding table is derived from the catalog, so this one entry
   is the whole Rust-side change.
2. Implement it in the owning adapter's `runner.js` under its `SCENARIOS`
   map (same id), following the existing scenarios: return an observations
   object on success, throw on failure; observe protocol effects, never
   internal pylon state.
3. Run `cargo run -- --audit` — it proves the runner lists the new id — then
   `cargo run -- --scenario <ID>` for a focused pass, then a full rerun.

## SDK upgrade policy

The pins are exact on purpose: the harness's verdicts are only meaningful
against a known SDK version. To upgrade:

1. Edit the version in the adapter's `package.json`
   (`pusher-js/` → `pusher-js`, `pusher-http-node/` → `pusher`).
2. Regenerate the lockfile (`rm package-lock.json && npm install`).
3. Full rerun of the suite — an SDK upgrade is never partial.

If the new SDK changes behavior, scenarios fail with the observations needed
to decide whether pylon or the expectation needs updating.

## Out of catalog (on purpose)

This harness only exercises what the official SDKs can exercise. Edges that
require a raw socket — malformed frames, protocol violations, handshake
abuse — live in **pylon's own integration suite** (`tests/` in the repo
root), not here. A behavior no SDK can produce has no place in an SDK
conformance catalog.

## Phase 2: hosted differential mode

The observation model (normalized, run-unique values masked) was designed so
the same scenarios can later run against Pusher's hosted service and diff
against the local pylon run. That hosted-differential mode is **designed-in
but not built**; today the harness runs local-only (`target: "local"` in the
report).

## CI

`.github/workflows/conformance.yml` runs the whole suite on a freshly built
release pylon (Node 18, adapter `npm ci`s, `--report` artifact uploaded on
every outcome). It is opt-in via `workflow_dispatch`; to run it nightly,
uncomment the `schedule`/`cron` block at the top of the workflow file.
