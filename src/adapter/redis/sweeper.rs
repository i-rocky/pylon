//! Lease-locked occupancy sweeper.
//!
//! A live node re-stamps its members' `expireAt` via the membership heartbeat
//! (`heartbeat_loop`). A node that crashes simply stops ticking — its members'
//! `expireAt` stamps fall into the past and nothing else removes them. The sweeper
//! is the cluster's garbage collector for those orphaned members: exactly one node
//! at a time holds a short-lived Redis lease (`{prefix}:sweeplock`), scans every
//! occupied channel, HDELs members whose `expireAt < now`, and — when that empties a
//! channel — vacates it via the atomic VACATE_LUA CAS (DEL the occ hash + de-index
//! the channel + drain the presence side-tables + decide the emission right in ONE
//! script) and fires `channel_vacated` only when this pass won the CAS (through the
//! dispatcher's grace + cluster re-check, so a re-subscribe within the grace window
//! suppresses the webhook). The CAS is what keeps the sweeper and a concurrent
//! last-unsubscribe (UNSUBSCRIBE_LUA) from BOTH firing `channel_vacated` for one
//! vacancy: the emission right belongs to whichever caller's SREM actually removed
//! the `chans` entry, and Redis serializes scripts — exactly one winner.
//!
//! Presence rosters have two lines of defence, because a presence member outlives
//! its `occ` token in two different ways. While ANY member of the channel is live,
//! the occ hash's whole-key TTL backstop (sized to OUTLIVE the per-member `expireAt`
//! stamps it carries — [`RedisConfig::occ_ttl_secs`]) keeps a crashed node's stale
//! tokens visible, so the per-token reap above resolves each to its user and emits
//! the exact `member_removed`. When nobody is left, the occ hash can lapse entirely
//! with its tokens; the roster the vacate drains is then the only remaining record
//! of who was in the channel, and the members it names are emitted here.
//!
//! [`RedisConfig::occ_ttl_secs`]: crate::adapter::redis::RedisConfig::occ_ttl_secs
//!
//! Every Redis error is logged and skipped; one failure must never abort the whole
//! sweep. Nothing here panics or unwraps.
//!
//! The dead-node prune carries a second reclaim duty: a dead node's per-app
//! capacity counts (`nodeconns:{node}`) are subtracted from the cluster totals
//! (`appconns`, floored at 0) and the hash deleted in the same pass — so capacity
//! held by a crashed node frees within a heartbeat window instead of leaking until
//! a manual flush.

use super::client::Scripts;
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
/// `sharded` is the cluster-wide `PYLON_REDIS_SHARDED_PUBSUB` setting, threading
/// into the reap paths' WatchOffline/member_removed publishes so they ride the
/// same pub/sub namespace (SPUBLISH vs PUBLISH) the live nodes subscribe on.
/// `envelope_compat` is the cluster-wide `PYLON_CLUSTER_ENVELOPE_COMPAT` setting
/// threading into the same reap publishes' envelope shape.
///
/// Lease protocol: try `SET sweeplock node_id NX PX lease_ms`. If acquired, sweep.
/// If not, `GET sweeplock`: if we already own it, renew (`SET … PX lease_ms`, no NX)
/// and sweep; otherwise yield (another node sweeps) and return `acquired = false`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn sweep_once(
    pool: &Pool,
    scripts: &Scripts,
    keys: &Keys,
    node_id: &str,
    lease_ms: u64,
    sharded: bool,
    envelope_compat: bool,
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

            // Presence side-table reap: each stale token's user loses a connection via
            // the atomic REAP_MEMBER_LUA CAS; only the →0 user edge (won == 1) emits
            // member_removed, so a racing live PRESENCE_LEAVE and this reap can never
            // both fire it. Per-token via the user refcount, so multi-connection users
            // and users still live on another node are handled correctly.
            if super::presence::is_presence(&channel) {
                for token in &stale {
                    super::presence::reap_member(
                        scripts,
                        pool,
                        keys,
                        &app,
                        &channel,
                        token,
                        sharded,
                        envelope_compat,
                        webhooks,
                    )
                    .await;
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

            // Vacate via the atomic VACATE_LUA CAS. The whole vacate DECISION+action is
            // one script, so unlike the old HLEN→DEL→SREM round-trips it cannot straddle
            // a concurrent last-unsubscribe's UNSUBSCRIBE_LUA and see a chans-indexed
            // channel whose occ is already gone (which double-fired channel_vacated).
            // The winner also gets the presence roster the script drained — every user
            // whose node died holding it, owed one member_removed BEFORE the vacate.
            let (won, drained): (i64, Vec<String>) = match scripts
                .vacate
                .evalsha_with_reload::<(i64, Vec<String>), _, _>(
                    pool.next(),
                    vec![
                        occ,
                        keys.chans(&app),
                        keys.presusers(&app, &channel),
                        keys.presinfo(&app, &channel),
                        keys.presmembers(&app, &channel),
                    ],
                    vec![channel.clone()],
                )
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, app, channel, "sweeper: VACATE cas failed; skipping channel");
                    continue;
                }
            };
            if won != 1 {
                continue;
            }

            super::presence::emit_drained_members(
                pool,
                keys,
                &app,
                &channel,
                &drained,
                sharded,
                envelope_compat,
                webhooks,
            )
            .await;
            webhooks.enqueue(WebhookEvent::ChannelVacated {
                app: app.clone(),
                channel: channel.clone(),
            });
            vacated.push((app.clone(), channel));
        }
    }

    // User-binding reap: a crashed node's signed-in user goes offline here, through the
    // atomic USER_REAP CAS. Still under the lease; re-`SMEMBERS apps` (the channel loop
    // consumed the earlier Vec).
    let apps: Vec<String> = match pool.next().smembers(keys.apps()).await {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!(error = %e, "sweeper: SMEMBERS apps failed; skipping user reap this pass");
            Vec::new()
        }
    };
    for app in &apps {
        let users: Vec<String> = match pool.next().smembers(keys.users(app)).await {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(error = %e, app, "sweeper: SMEMBERS users failed");
                continue;
            }
        };
        for user_id in users {
            super::user::reap_user(
                scripts,
                pool,
                keys,
                app,
                &user_id,
                sharded,
                envelope_compat,
                now,
            )
            .await;
        }
    }

    // Prune dead nodes from the nodes set, reclaiming their per-app capacity counts
    // first. Order matters: once the id leaves `nodes`, nothing enumerates its hash
    // again (the hash's TTL backstop is then the only GC, and it removes the HASH,
    // not the cluster-total residue).
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
                // On a reclaim error we SKIP the SREM: the `nodes` entry is the
                // enumeration source for the retry — the next sweep pass sees the
                // node still dead and retries the reclaim (which is idempotent:
                // floor-0 subtract + DEL). SREM-ing anyway would forget the node
                // while its counts still sit in `appconns`, and NOTHING reclaims
                // them after that — the hash's TTL backstop removes the HASH, not
                // the cluster-total residue.
                let reclaimed = scripts
                    .reclaim_node
                    .evalsha_with_reload::<i64, _, _>(
                        pool.next(),
                        vec![keys.appconns(), keys.nodeconns(&node), keys.node(&node)],
                        Vec::<String>::new(),
                    )
                    .await;
                match reclaimed {
                    Ok(-1) => {
                        tracing::debug!(
                            node,
                            "sweeper: node re-advertised itself before the reclaim ran; leaving its capacity counts alone"
                        );
                        continue;
                    }
                    Ok(apps) => {
                        if apps > 0 {
                            tracing::debug!(
                                node,
                                apps,
                                "sweeper: reclaimed dead node's per-app capacity counts"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, node, "sweeper: dead-node capacity reclaim failed; keeping the nodes entry so the next sweep retries");
                        continue;
                    }
                }
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
/// another node take over within a couple of ticks. `sharded` threads the cluster's
/// pub/sub mode into the reap publishes (see [`sweep_once`]); `envelope_compat`
/// threads the cluster's envelope shape setting into the same publishes.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn sweeper_loop(
    pool: Pool,
    keys: Keys,
    node_id: String,
    lease_ms: u64,
    interval_secs: u64,
    sharded: bool,
    envelope_compat: bool,
    webhooks: WebhookHandle,
) {
    // Compiled once (pure local SHA-1 hashing, no Redis round-trip); reused by
    // every pass for the atomic vacate CAS.
    let scripts = Scripts::new();
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
    loop {
        ticker.tick().await;
        let report = sweep_once(
            &pool,
            &scripts,
            &keys,
            &node_id,
            lease_ms,
            sharded,
            envelope_compat,
            &webhooks,
            super::now_ms(),
        )
        .await;
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
