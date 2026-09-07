//! Startup resource detection + budget formulas (SP10).
//!
//! pylon sizes itself to *whatever host it is dropped on* — a 2-core/4 GB
//! container or 48-core/256 GB bare metal — by reading the **effective** CPU and
//! memory envelope (host limits AND cgroup limits, taking the smaller) at
//! startup. The classic container bug is sizing off `nproc` / host-RAM while a
//! cgroup caps the process far lower; these helpers read the cgroup files and
//! take the min.
//!
//! The cgroup parsing + budget arithmetic are split into **pure** functions
//! (`effective_cores_from_cpu_max`, `mem_limit_v2`, `mem_limit_v1`,
//! `memory_budget`, `per_conn_cap`) that are exhaustively unit-tested on
//! synthetic file contents, and **impure** detectors (`detect_workers`,
//! `detect_effective_mem`) that read real sysfs/procfs and fall back to the host
//! on any read failure. The detectors are only smoke-tested (they read live
//! files); the formulas carry the real test coverage.
//!
//! Safe Rust — the crate root sets `#![deny(unsafe_code)]`; this module adds no
//! `unsafe`.

/// Effective CPU count from a cgroup v2 `cpu.max` line (`"<quota> <period>"`).
///
/// Returns `ceil(quota / period)` (round **up** — a 1.5-core quota is one full
/// core plus part of another, so size for 2; matches Go 1.25's
/// `GOMAXPROCS`-from-cgroup behaviour), floored at 1. A `quota` of `max` means
/// unlimited → `None` (the caller falls back to the host CPU count). A missing
/// period defaults to the cgroup default of 100000 µs.
pub fn effective_cores_from_cpu_max(s: &str) -> Option<u64> {
    let mut it = s.split_whitespace();
    let quota = it.next()?;
    if quota == "max" {
        return None;
    }
    let q: u64 = quota.parse().ok()?;
    let p: u64 = it.next().unwrap_or("100000").parse().ok()?;
    if p == 0 {
        return None;
    }
    Some(q.div_ceil(p).max(1)) // ceil, floor 1
}

/// Parse a cgroup **v2** memory limit file (`memory.max` / `memory.high`).
/// `"max"` (the literal sentinel) means unlimited → `None`.
pub fn mem_limit_v2(s: &str) -> Option<u64> {
    if s.trim() == "max" {
        None
    } else {
        s.trim().parse().ok()
    }
}

/// Parse a cgroup **v1** memory limit file (`memory.limit_in_bytes`). v1 reports
/// "unlimited" as a huge near-`u64::MAX` sentinel rather than a word; treat any
/// value `>= 2^62` as unlimited → `None`.
pub fn mem_limit_v1(s: &str) -> Option<u64> {
    let v: u64 = s.trim().parse().ok()?;
    if v >= (1u64 << 62) {
        None // huge sentinel = unlimited
    } else {
        Some(v)
    }
}

/// The usable memory **budget** from the effective memory envelope: the envelope
/// minus an OS reserve of `max(1.5 GiB, 7%)` (Seastar's exact OS-reserve
/// formula — a flat floor on small boxes, 7% on big), capped at half the
/// envelope. Seastar's formula assumes a machine large enough that the flat
/// 1.5 GiB floor is a minority of RAM; below ~3 GiB that floor alone can
/// consume the entire envelope, saturating the budget to 0 and silently
/// disabling every control the budget sizes (shedding, admission gates, the
/// derived connection ceiling). Halving the envelope as a ceiling on the
/// reserve keeps a small box's budget small but never zero. Saturating so a
/// zero envelope yields 0 rather than underflowing.
pub fn memory_budget(effective_mem: u64) -> u64 {
    let reserve = (1536u64 << 20)
        .max(effective_mem * 7 / 100)
        .min(effective_mem / 2);
    effective_mem.saturating_sub(reserve)
}

/// Per-connection out-queue byte cap: `per_worker_budget / expected_conns`,
/// clamped to `[256 KiB, 8 MiB]`. The floor is one large frame + headroom; the
/// ceiling is the Redis-pubsub hard-limit class so one slow client can't hog
/// memory. `expected_conns` is a config estimate, not a reservation — the cap is
/// a per-connection ceiling, so over-estimating only shrinks it.
pub fn per_conn_cap(per_worker_budget: u64, expected_conns: u64) -> u64 {
    (per_worker_budget / expected_conns.max(1)).clamp(256 << 10, 8 << 20)
}

/// Parse the `full avg10` value out of a Linux PSI memory-pressure block (cgroup
/// v2 `memory.pressure` or `/proc/pressure/memory`) — SP10 §8.
///
/// A pressure block has two lines, e.g.
/// ```text
/// some avg10=0.00 avg60=0.00 avg300=0.00 total=0
/// full avg10=12.34 avg60=5.00 avg300=1.00 total=123456
/// ```
/// This returns the `full` line's `avg10` (`12.34`) — the fraction of wall time
/// in which **every** runnable task was stalled on memory, the signal the
/// backstop reacts to. `None` if the block has no `full` line or it is malformed.
pub fn psi_full_avg10(s: &str) -> Option<f64> {
    for line in s.lines() {
        let line = line.trim_start();
        if let Some(rest) = line.strip_prefix("full ") {
            for field in rest.split_whitespace() {
                if let Some(v) = field.strip_prefix("avg10=") {
                    return v.parse().ok();
                }
            }
        }
    }
    None
}

/// The kernel PSI memory-pressure file to read (SP10 §8): cgroup v2
/// `/sys/fs/cgroup/memory.pressure` when present, else the host
/// `/proc/pressure/memory`. Returns the first path that exists, or `None` if PSI
/// is unavailable on this host (kernel < 4.20 or `CONFIG_PSI` off) — the backstop
/// then no-ops, leaving the budget factor pinned at full.
pub fn psi_pressure_path() -> Option<&'static str> {
    const CGROUP_V2: &str = "/sys/fs/cgroup/memory.pressure";
    const HOST: &str = "/proc/pressure/memory";
    if std::path::Path::new(CGROUP_V2).exists() {
        Some(CGROUP_V2)
    } else if std::path::Path::new(HOST).exists() {
        Some(HOST)
    } else {
        None
    }
}

// ---- impure detectors (read real sysfs/procfs) -------------------------------

/// Number of worker reactors to spawn: `available_parallelism()`, which is
/// already cgroup-aware (it reads `sched_getaffinity` + cgroup `cpu.max` /
/// v1 quota/period and returns the min, never 0). Falls back to 1 if the OS
/// won't report it. `PYLON_WORKERS` overrides this upstream.
pub fn detect_workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// The effective memory envelope: `min(host MemTotal, cgroup memory limit)`.
/// Reads `/proc/meminfo` for the host figure and the cgroup memory file for the
/// container cap (v2 vs v1 detected by the presence of
/// `/sys/fs/cgroup/cgroup.controllers`; v2 takes `min(memory.max, memory.high)`).
/// Any read/parse failure falls back to the host figure alone; if even the host
/// figure is unavailable, returns a conservative 1 GiB so sizing never yields 0.
pub fn detect_effective_mem() -> u64 {
    let host = meminfo_memtotal().unwrap_or(1u64 << 30);
    match cgroup_mem_limit() {
        Some(cg) => host.min(cg),
        None => host,
    }
}

/// Parse `MemTotal` (in kB) from `/proc/meminfo` → bytes.
fn meminfo_memtotal() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            // "MemTotal:       16318864 kB"
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// The cgroup memory limit, or `None` if unlimited / unreadable. cgroup **v2**
/// is detected by the presence of `/sys/fs/cgroup/cgroup.controllers` and takes
/// `min(memory.max, memory.high)`; otherwise the cgroup **v1**
/// `memory/memory.limit_in_bytes` (huge sentinel = unlimited) is consulted.
fn cgroup_mem_limit() -> Option<u64> {
    if std::path::Path::new("/sys/fs/cgroup/cgroup.controllers").exists() {
        // cgroup v2: min of memory.max and memory.high (each `max` = no limit).
        let max = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
            .ok()
            .and_then(|s| mem_limit_v2(&s));
        let high = std::fs::read_to_string("/sys/fs/cgroup/memory.high")
            .ok()
            .and_then(|s| mem_limit_v2(&s));
        match (max, high) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    } else {
        // cgroup v1.
        std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
            .ok()
            .and_then(|s| mem_limit_v1(&s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cpu_max_v2() {
        assert_eq!(effective_cores_from_cpu_max("200000 100000"), Some(2)); // quota/period, round up
        assert_eq!(effective_cores_from_cpu_max("150000 100000"), Some(2)); // 1.5 -> 2 (round up)
        assert_eq!(effective_cores_from_cpu_max("max 100000"), None); // unlimited
        assert_eq!(effective_cores_from_cpu_max("50000 100000"), Some(1)); // floor 1
    }

    #[test]
    fn parse_mem_limit_handles_sentinels() {
        assert_eq!(mem_limit_v2("max"), None); // unlimited
        assert_eq!(mem_limit_v2("4294967296"), Some(4 << 30)); // 4 GiB
        assert_eq!(mem_limit_v1("9223372036854771712"), None); // huge v1 sentinel = unlimited
        assert_eq!(mem_limit_v1("4294967296"), Some(4 << 30));
    }

    #[test]
    fn budget_reserve_formula() {
        // reserve = max(1.5 GiB, 7%)
        assert_eq!(memory_budget(4 << 30), (4u64 << 30) - (1536 << 20)); // small box: flat floor
        assert_eq!(
            memory_budget(256u64 << 30),
            (256u64 << 30) - (256u64 << 30) * 7 / 100
        ); // big box: 7%
    }

    #[test]
    fn budget_reserve_cap_binds_below_crossover() {
        // reserve = min(max(1.5 GiB, 7%), 50% of envelope). Below the crossover
        // the flat 1.5 GiB floor would otherwise consume the whole envelope (or
        // more), so the half-envelope cap takes over and a small box keeps a
        // real, non-zero budget instead of saturating to 0.
        assert_eq!(memory_budget(1u64 << 30), 512 << 20); // 1 GiB -> 512 MiB
        assert_eq!(memory_budget(512u64 << 20), 256 << 20); // 512 MiB -> 256 MiB

        // Crossover: at exactly 3 GiB, half the envelope (1.5 GiB) equals the
        // flat floor (1.5 GiB), so the cap and the uncapped formula agree. Above
        // 3 GiB the flat floor is already less than half the envelope, so the
        // cap never binds again (until 7% overtakes the floor much higher up,
        // where it is smaller still) — this is why 4 GiB and 256 GiB above are
        // unaffected.
        assert_eq!(memory_budget(3u64 << 30), 1536 << 20); // 3 GiB -> 1.5 GiB

        // A zero envelope must not panic or underflow.
        assert_eq!(memory_budget(0), 0);
    }

    #[test]
    fn per_conn_cap_clamps() {
        // clamp(per_worker_budget / expected, 256KiB, 8MiB)
        // 1 GiB / 50_000 ≈ 21 KiB < 256 KiB ⇒ floor to 256 KiB.
        assert_eq!(per_conn_cap(1u64 << 30, 50_000), 256 << 10);
        // A generous per-worker budget with few expected conns hits the 8 MiB ceiling.
        assert_eq!(per_conn_cap(1u64 << 30, 16), 8 << 20);
        // expected_conns = 0 must not divide by zero (clamped to 1 internally).
        assert_eq!(per_conn_cap(512 << 10, 0), 512 << 10);
    }

    #[test]
    fn detectors_smoke() {
        // These read real files; only assert the sane-floor invariants.
        assert!(detect_workers() >= 1);
        assert!(detect_effective_mem() >= 1u64 << 30);
    }

    #[test]
    fn psi_parses_full_avg10() {
        // A real two-line PSI block: pick the `full` line's avg10, NOT `some`.
        let block = "some avg10=1.23 avg60=4.56 avg300=7.89 total=111\n\
                     full avg10=12.34 avg60=5.00 avg300=1.00 total=222\n";
        assert_eq!(psi_full_avg10(block), Some(12.34));

        // `some` first but its avg10 must not be returned.
        let some_first = "some avg10=99.99 avg60=0 avg300=0 total=0\n\
                          full avg10=0.00 avg60=0 avg300=0 total=0\n";
        assert_eq!(psi_full_avg10(some_first), Some(0.0));

        // Only a `some` line (e.g. `/proc/pressure/cpu` has no `full`): None.
        assert_eq!(
            psi_full_avg10("some avg10=3.21 avg60=0 avg300=0 total=0\n"),
            None
        );

        // Leading whitespace tolerated; integer avg10 parses as float.
        assert_eq!(
            psi_full_avg10("  full avg10=42 avg60=1 avg300=1 total=9\n"),
            Some(42.0)
        );

        // Empty / malformed input yields None.
        assert_eq!(psi_full_avg10(""), None);
        assert_eq!(psi_full_avg10("full avg60=1.0 total=5\n"), None);
    }
}
