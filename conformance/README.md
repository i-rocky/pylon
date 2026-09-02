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
cargo run -- --smoke            # 3-scenario subset: C-PUB-SUB, S-TRIGGER, S-WEBHOOK-VERIFY
                                # (all three scenarios exercise in the smoke subset:
                                # C-PUB-SUB's occupied/vacated envelopes feed S-WEBHOOK-VERIFY)
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
JSON artifact. The matrix ends with a summary line like

```
26 passed, 0 failed, 0 skipped — pusher-js@8.6.0, pusher-http-node@5.3.4, pylon 0.3.0
```

SDKs are keyed by adapter id (`pusher-http-node`, not its npm package name
`pusher`) — the same id the catalog, `--sdk`, and the matrix's SDK column
use. A failed version probe drops that pair with a stderr warning and the
run proceeds (version is metadata, not a gate). Exit code: `0` all
pass/skip, `1` any failure, `2` harness error (bad flags, missing binary,
...).

The JSON artifact is stable and diffable: run metadata (timestamp, pylon
version, both SDK versions, target) plus one entry per scenario with its
verdict, observations, error, and duration.

## `--audit`: keep the catalog honest

The catalog (in `src/catalog.rs`) claims each scenario is implemented by a
specific adapter's `runner.js`. `--audit` verifies that claim:

- every bound `adapters/<sdk>/runner.js` exists;
- each runner's `--list` output matches the catalog **both ways**: every
  bound id must be listed, and every listed id must be bound (a runner
  listing an id the catalog does not bind is a scenario nobody selected,
  budgeted, or audited);
- the binding table matches the catalog (no missing, orphaned, wrong-SDK, or
  duplicate bindings).

It also runs one **advisory** family: the observation-normalization scan
over the last report artifact (`conformance-report.json`), if one exists —
see below. Advisory there, mandatory in a run.

Run it after any change to the catalog or a runner. CI runs the full suite,
which is itself an end-to-end audit; `--audit` is the cheap early-warning
version.

## Observation normalization

Runner observations carry facts that are true of **every** run —
placeholders (`<socket_id>`, `<1..29>`), pinned statuses (`200`, `4301`),
SDK-mandated constants (`activity_timeout_used_ms: 2000`). Run-unique
values (socket ids, timestamps, epoch counters) go to runner stderr as
evidence, never into the report artifact — that is what keeps the JSON
stable and diffable across runs.

After every run the harness scans all recorded observations for run-unique
shapes (socket-id-like dotted integer pairs, ISO-8601 timestamps, raw
epoch-millis, bare epoch-sized integers). A clean run prints
`normalization scan: clean`; any violation prints a warning block and fails
the run (exit 1) even if all 26 verdicts passed. `--audit` re-runs the same
scan against the last report artifact, advisorially.

The shapes are conservative; if a scenario ever legitimately observes a
fixed value that trips one, add a `(scenario id, observation key)` pair to
`ALLOWED_RAW_KEYS` in `src/normalization.rs` — that exempts exactly that
leaf key within exactly that scenario. Keep it empty unless a reviewed case
demands an entry.

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
every outcome). It runs **nightly** (03:00 UTC cron) and on manual
`workflow_dispatch`.
