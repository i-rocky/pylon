//! pylon-load — Pusher-protocol load-test harness library.
// Several protocol helpers and scenario entry points are exercised only by the
// load-test binaries (`src/main.rs`, `src/bin/ceiling.rs`) and the tests under
// `tests/`, so allow the not-yet-used public API.
#![allow(dead_code)]
#![deny(unsafe_code)]

pub mod ceiling;
pub mod cli;
pub mod metrics;
pub mod pusher;
pub mod scenario;
