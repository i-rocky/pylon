//! Lease-locked occupancy sweeper (Task D2).
//!
//! A live node re-stamps its members' `expireAt` via the membership heartbeat
//! (`heartbeat_loop`). A node that crashes simply stops ticking — its members'
//! `expireAt` stamps fall into the past and nothing else removes them. The sweeper
//! is the cluster's garbage collector for those orphaned members: exactly one node
//! at a time holds a short-lived Redis lease (`{prefix}:sweeplock`), scans every
//! occupied channel, HDELs members whose `expireAt < now`, and — when that empties a
//! channel — DELs the occ hash, de-indexes the channel, and fires `channel_vacated`
//! (through the dispatcher's grace + cluster re-check, so a re-subscribe within the
//! grace window suppresses the webhook).
//!
//! Every Redis error is logged and skipped; one failure must never abort the whole
//! sweep. Nothing here panics or unwraps.

use super::keys::Keys;
use crate::webhook::event::WebhookEvent;
use crate::webhook::WebhookHandle;
use fred::clients::Pool;
use fred::interfaces::{HashesInterface, KeysInterface, SetsInterface};
use fred::types::{Expiration, SetOptions};
use std::time::Duration;

/// The outcome of one `sweep_once` pass. Returned by the test seam so callers can
/// assert what the sweep did this tick.
pub(crate) struct SweepReport {
    /// Whether this node held (or renewed) the sweep lease and actually swept.
    pub acquired: bool,
    /// How many stale members were HDEL'd across all channels this pass.
    pub reaped: usize,
    /// The `(app, channel)` pairs vacated by this pass (each fired a `channel_vacated`).
    pub vacated: Vec<(String, String)>,
}

/// Run one deterministic sweep pass. `now` is the current wall-clock millis used to
/// decide which members are stale (passed in so tests can drive time precisely).
///
/// Lease protocol: try `SET sweeplock node_id NX PX lease_ms`. If acquired, sweep.
/// If not, `GET sweeplock`: if we already own it, renew (`SET … PX lease_ms`, no NX)
/// and sweep; otherwise yield (another node sweeps) and return `acquired = false`.
pub(crate) async fn sweep_once(
    pool: &Pool,
    keys: &Keys,
    node_id: &str,
    lease_ms: u64,
    webhooks: &WebhookHandle,
    now: u64,
) -> SweepReport {
    if !acquire_lease(pool, keys, node_id, lease_ms).await {
        return SweepReport {
            acquired: false,
            reaped: 0,
            vacated: Vec::new(),
        };
    }

    let mut reaped = 0usize;
    let mut vacated: Vec<(String, String)> = Vec::new();

    // Enumerate apps, then each app's occupied channels.
    let apps: Vec<String> = match pool.next().smembers(keys.apps()).await {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!(error = %e, "sweeper: SMEMBERS apps failed; skipping member reap this pass");
            Vec::new()
        }
    };

    for app in apps {
        let channels: Vec<String> = match pool.next().smembers(keys.chans(&app)).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, app, "sweeper: SMEMBERS chans failed; skipping app this pass");
                continue;
            }
        };

        for channel in channels {
            let occ = keys.occ(&app, &channel);
            let members: Vec<(String, String)> = match pool.next().hgetall(&occ).await {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(error = %e, app, channel, "sweeper: HGETALL occ failed; skipping channel");
                    continue;
                }
            };

            // Collect members whose stamped `expireAt` is in the past.
            let stale: Vec<String> = members
                .iter()
                .filter_map(|(token, expire_at)| match expire_at.parse::<u64>() {
                    Ok(exp) if exp < now => Some(token.clone()),
                    Ok(_) => None,
                    Err(_) => {
                        // An unparseable stamp is treated as stale (it can never be
                        // re-stamped to a valid future value by a live node).
                        tracing::warn!(app, channel, token, value = %expire_at, "sweeper: unparseable expireAt; treating as stale");
                        Some(token.clone())
                    }
                })
                .collect();

            // Presence side-table reap: for a presence channel, each stale token's user
            // loses a connection; the →0 user edge emits member_removed (cross-node +
            // webhook). Per-token via the user refcount, so multi-connection users and
            // users still live on another node are handled correctly.
            if super::presence::is_presence(&channel) {
                for token in &stale {
                    super::presence::reap_member(pool, keys, &app, &channel, token, webhooks).await;
                }
            }

            // Reap any stale members first.
            if !stale.is_empty() {
                if let Err(e) = pool.next().hdel::<i64, _, _>(&occ, stale.clone()).await {
                    tracing::warn!(error = %e, app, channel, "sweeper: HDEL stale members failed; skipping channel");
                    continue;
                }
                reaped += stale.len();
            }

            // Decide whether the channel is now vacant. It is vacant when no live
            // members remain — either because we just reaped the last one, OR because
            // the occ hash had already been removed by its whole-key TTL backstop while
            // the `chans` index still listed it (an orphaned phantom the sweeper must
            // clean). A channel with only fresh (future-`expireAt`) members is NOT
            // vacant and is left alone.
            let had_fresh_members = members.len() > stale.len();
            if had_fresh_members {
                continue;
            }
            // `HLEN occ` is the authoritative post-reap count (also 0 when the key is
            // gone). Skip the vacate only if a member somehow remains (a concurrent
            // re-subscribe between our HGETALL and now).
            let remaining: i64 = match pool.next().hlen(&occ).await {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!(error = %e, app, channel, "sweeper: HLEN occ failed; cannot confirm vacate");
                    continue;
                }
            };
            if remaining != 0 {
                continue;
            }

            // Vacate: DEL the occ hash (no-op if the TTL already removed it), de-index
            // the channel, and fire channel_vacated (debounced + cluster re-checked, so
            // a re-subscribe within the grace window suppresses the webhook).
            if let Err(e) = pool.next().del::<i64, _>(&occ).await {
                tracing::warn!(error = %e, app, channel, "sweeper: DEL empty occ failed");
            }
            if let Err(e) = pool
                .next()
                .srem::<i64, _, _>(keys.chans(&app), channel.clone())
                .await
            {
                tracing::warn!(error = %e, app, channel, "sweeper: SREM chans failed");
            }
            webhooks.enqueue(WebhookEvent::ChannelVacated {
                app: app.clone(),
                channel: channel.clone(),
            });
            vacated.push((app.clone(), channel));
        }
    }

    // Secondary: prune dead nodes from the nodes set (their `node` key TTL-expired).
    // Member reaping above is the real cleanup; this just keeps the set tidy.
    let nodes: Vec<String> = match pool.next().smembers(keys.nodes()).await {
        Ok(n) => n,
        Err(e) => {
            tracing::warn!(error = %e, "sweeper: SMEMBERS nodes failed; skipping dead-node prune");
            Vec::new()
        }
    };
    for node in nodes {
        match pool.next().exists::<i64, _>(keys.node(&node)).await {
            Ok(0) => {
                if let Err(e) = pool
                    .next()
                    .srem::<i64, _, _>(keys.nodes(), node.clone())
                    .await
                {
                    tracing::warn!(error = %e, node, "sweeper: SREM dead node failed");
                }
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(error = %e, node, "sweeper: EXISTS node failed; skipping prune for this node");
            }
        }
    }

    SweepReport {
        acquired: true,
        reaped,
        vacated,
    }
}

/// Try to hold the sweep lease for `lease_ms`. Returns `true` if we acquired it fresh
/// or already owned it (renewed); `false` if another node holds it.
async fn acquire_lease(pool: &Pool, keys: &Keys, node_id: &str, lease_ms: u64) -> bool {
    let lock = keys.sweeplock();
    // SET lock node_id NX PX lease_ms — `get: false`, returns OK only when set.
    let set: Result<Option<String>, _> = pool
        .next()
        .set(
            &lock,
            node_id,
            Some(Expiration::PX(lease_ms as i64)),
            Some(SetOptions::NX),
            false,
        )
        .await;
    match set {
        Ok(Some(_)) => return true, // "OK" → acquired
        Ok(None) => {}              // NX rejected → already held by someone
        Err(e) => {
            tracing::warn!(error = %e, "sweeper: SET sweeplock NX failed; yielding this pass");
            return false;
        }
    }

    // Not acquired: is it ours? If so, renew (no NX) and proceed; else yield.
    let owner: Result<Option<String>, _> = pool.next().get(&lock).await;
    match owner {
        Ok(Some(o)) if o == node_id => {
            if let Err(e) = pool
                .next()
                .set::<(), _, _>(
                    &lock,
                    node_id,
                    Some(Expiration::PX(lease_ms as i64)),
                    None,
                    false,
                )
                .await
            {
                tracing::warn!(error = %e, "sweeper: lease renew failed; proceeding on prior lease");
            }
            true
        }
        Ok(_) => false, // owned by another node (or just vanished) → yield
        Err(e) => {
            tracing::warn!(error = %e, "sweeper: GET sweeplock failed; yielding this pass");
            false
        }
    }
}

/// Background sweep loop. Ticks every `interval_secs` and runs one `sweep_once` with
/// the current wall-clock millis. The lease (`lease_ms`) is sized to outlive a tick so
/// the holder keeps sweeping, but auto-frees (PX expiry) if the holder dies — letting
/// another node take over within a couple of ticks.
pub(crate) async fn sweeper_loop(
    pool: Pool,
    keys: Keys,
    node_id: String,
    lease_ms: u64,
    interval_secs: u64,
    webhooks: WebhookHandle,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
    loop {
        ticker.tick().await;
        let report = sweep_once(&pool, &keys, &node_id, lease_ms, &webhooks, super::now_ms()).await;
        if report.acquired && (report.reaped > 0 || !report.vacated.is_empty()) {
            tracing::debug!(
                reaped = report.reaped,
                vacated = ?report.vacated,
                "redis sweeper pass complete"
            );
        } else {
            tracing::trace!(
                acquired = report.acquired,
                reaped = report.reaped,
                "redis sweeper pass complete"
            );
        }
    }
}
