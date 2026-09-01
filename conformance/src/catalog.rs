//! Scenario catalog for the pylon conformance harness.
//!
//! `CATALOG` is the binding registry of every conformance scenario (spec §5,
//! tables copied verbatim). Every entry must bind to exactly one scenario
//! implementation in exactly one adapter — [`audit`] enforces that.

/// Execution plane a scenario runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plane {
    Client,
    Server,
}

/// One conformance scenario: what it exercises, which official SDK drives it,
/// and the wall-clock budget the orchestrator enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scenario {
    pub id: &'static str,
    pub plane: Plane,
    pub sdk: &'static str,
    pub summary: &'static str,
    pub budget_ms: u64,
}

/// All 26 scenarios (18 client plane on `pusher-js`, 8 server plane on
/// `pusher-http-node`), in spec §5 table order.
pub const CATALOG: &[Scenario] = &[
    // Client plane — pusher-js.
    Scenario {
        id: "C-ESTABLISH",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "connect → `connection_established` → state `connected`",
        budget_ms: 20000,
    },
    Scenario {
        id: "C-RECONNECT",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "force disconnect → client reconnects → re-subscribes",
        budget_ms: 30000,
    },
    Scenario {
        id: "C-PING",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "client `pusher:ping` → pong; connection stays usable",
        budget_ms: 15000,
    },
    Scenario {
        id: "C-PUB-SUB",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "subscribe/trigger-receive/unsubscribe on a public channel",
        budget_ms: 20000,
    },
    Scenario {
        id: "C-PRIV-SUB",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "private channel via auth endpoint",
        budget_ms: 20000,
    },
    Scenario {
        id: "C-PRES-SUB",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "two connections, presence join/leave",
        budget_ms: 30000,
    },
    Scenario {
        id: "C-CACHE-SUB",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "cache channel: empty → populated → new subscriber",
        budget_ms: 20000,
    },
    Scenario {
        id: "C-ENC-SUB",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "server publishes ciphertext; client decrypts",
        budget_ms: 25000,
    },
    Scenario {
        id: "C-EVENT-ECHO",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "client event on private channel",
        budget_ms: 20000,
    },
    Scenario {
        id: "C-EVENT-LIMITS",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "oversized name (>200) and payload (>10 KiB)",
        budget_ms: 20000,
    },
    Scenario {
        id: "C-EVENT-RATE",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "burst beyond 10/s",
        budget_ms: 25000,
    },
    Scenario {
        id: "C-EVENT-PUB",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "client event attempted on public channel",
        budget_ms: 15000,
    },
    Scenario {
        id: "U-SIGNIN",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "signin via auth endpoint",
        budget_ms: 20000,
    },
    Scenario {
        id: "U-WATCH",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "watchlist across two connections",
        budget_ms: 30000,
    },
    Scenario {
        id: "U-WATCH-LIMIT",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "watchlist > 100 IDs",
        budget_ms: 20000,
    },
    Scenario {
        id: "U-TERMINATE",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "terminate-user via SDK `terminateUser` if exposed, else the documented REST endpoint (plumbing per §2.2)",
        budget_ms: 25000,
    },
    Scenario {
        id: "E-BADKEY",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "connect with unknown key",
        budget_ms: 15000,
    },
    Scenario {
        id: "E-DISABLED",
        plane: Plane::Client,
        sdk: "pusher-js",
        summary: "connect with disabled app's key",
        budget_ms: 15000,
    },
    // Server plane — pusher-http-node.
    Scenario {
        id: "S-TRIGGER",
        plane: Plane::Server,
        sdk: "pusher-http-node",
        summary: "single + multi-channel trigger",
        budget_ms: 15000,
    },
    Scenario {
        id: "S-BATCH",
        plane: Plane::Server,
        sdk: "pusher-http-node",
        summary: "batch trigger (10×10)",
        budget_ms: 15000,
    },
    Scenario {
        id: "S-CHANNELS",
        plane: Plane::Server,
        sdk: "pusher-http-node",
        summary: "query with prefix + attrs (`user_count`, `subscription_count`)",
        budget_ms: 15000,
    },
    Scenario {
        id: "S-CHANNEL",
        plane: Plane::Server,
        sdk: "pusher-http-node",
        summary: "single-channel info incl. cache attr",
        budget_ms: 15000,
    },
    Scenario {
        id: "S-USERS",
        plane: Plane::Server,
        sdk: "pusher-http-node",
        summary: "presence users list",
        budget_ms: 15000,
    },
    Scenario {
        id: "S-AUTH",
        plane: Plane::Server,
        sdk: "pusher-http-node",
        summary: "signing mode: reads an auth-request body from **stdin**, writes the SDK's auth response to **stdout** (spawned once per auth-endpoint hit; ~hundreds of ms is acceptable in a harness)",
        budget_ms: 15000,
    },
    Scenario {
        id: "S-WEBHOOK-VERIFY",
        plane: Plane::Server,
        sdk: "pusher-http-node",
        summary: "captured pylon envelopes → SDK webhook verifier",
        budget_ms: 25000,
    },
    Scenario {
        id: "S-ERRORS",
        plane: Plane::Server,
        sdk: "pusher-http-node",
        summary: "expired timestamp, bad signature, unknown app",
        budget_ms: 15000,
    },
];

/// Cross-check a list of implemented `(sdk, scenario id)` bindings against the
/// catalog, returning one problem line per issue: a catalog entry with no
/// implementation, an implementation bound to nothing in the catalog, an
/// implementation bound to the wrong SDK's scenario, and duplicate bindings.
pub fn audit(implementations: &[(String, String)]) -> Vec<String> {
    let mut problems = Vec::new();
    for s in CATALOG {
        if !implementations
            .iter()
            .any(|(sdk, id)| sdk == s.sdk && id == s.id)
        {
            problems.push(format!(
                "catalog entry {} ({}) has no implementation",
                s.id, s.sdk
            ));
        }
    }
    let mut seen = std::collections::HashSet::new();
    for (sdk, id) in implementations {
        match find(id) {
            None => problems.push(format!(
                "implementation {} ({}) is not in the catalog",
                id, sdk
            )),
            Some(s) if s.sdk != sdk => {
                problems.push(format!(
                    "{} is a {} scenario but is implemented by {}",
                    id, s.sdk, sdk
                ));
            }
            Some(_) => {}
        }
        if !seen.insert((sdk.clone(), id.clone())) {
            problems.push(format!("duplicate implementation {} ({})", id, sdk));
        }
    }
    problems
}

/// Look a scenario up by id.
pub fn find(id: &str) -> Option<&Scenario> {
    CATALOG.iter().find(|s| s.id == id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_has_all_26_entries_with_unique_ids() {
        assert_eq!(CATALOG.len(), 26);
        let mut ids: Vec<_> = CATALOG.iter().map(|s| s.id).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), CATALOG.len());
    }

    #[test]
    fn audit_flags_missing_and_orphan_implementations() {
        // Client plane = 18 ids, server plane = 8 ids (spec §5).
        let full: Vec<_> = CATALOG
            .iter()
            .map(|s| (s.sdk.to_string(), s.id.to_string()))
            .collect();
        assert!(audit(&full).is_empty());

        let mut missing_one = full.clone();
        missing_one.remove(0);
        assert_eq!(
            audit(&missing_one).len(),
            1,
            "unimplemented catalog entry must be flagged"
        );

        let mut orphan = full.clone();
        orphan.push(("pusher-js".into(), "C-NOPE".into()));
        assert!(
            audit(&orphan).iter().any(|l| l.contains("C-NOPE")),
            "orphan implementation must be flagged"
        );
    }

    #[test]
    fn audit_flags_wrong_sdk_and_duplicate_bindings() {
        let full: Vec<_> = CATALOG
            .iter()
            .map(|s| (s.sdk.to_string(), s.id.to_string()))
            .collect();

        // A scenario bound to the OTHER sdk: exactly one wrong-sdk problem.
        let mut wrong = full.clone();
        wrong.push(("pusher-http-node".into(), "C-PRES-SUB".into()));
        let problems = audit(&wrong);
        assert_eq!(
            problems.len(),
            1,
            "only the wrong-sdk binding is a problem: {problems:?}"
        );
        assert_eq!(
            problems[0],
            "C-PRES-SUB is a pusher-js scenario but is implemented by pusher-http-node"
        );

        // The same (sdk, id) twice: exactly one duplicate problem.
        let mut dup = full.clone();
        dup.push(dup[0].clone());
        let problems = audit(&dup);
        assert_eq!(
            problems.len(),
            1,
            "only the duplicate binding is a problem: {problems:?}"
        );
        assert_eq!(
            problems[0],
            "duplicate implementation C-ESTABLISH (pusher-js)"
        );
    }

    #[test]
    fn find_returns_entry_by_id() {
        assert_eq!(find("C-PRES-SUB").unwrap().sdk, "pusher-js");
        assert!(find("C-NOPE").is_none());
    }
}
