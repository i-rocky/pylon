# Clustering & Scaling

Pylon scales horizontally by connecting multiple nodes through a shared Redis instance.
Each node is stateless from a routing perspective — any node can serve any client.
There is no sticky-session requirement.

---

## How it works

By default, pylon uses the `local` adapter, which holds all connection and channel
state in process memory. This is sufficient for a single node but cannot be shared
across multiple servers.

Switching to the `redis` adapter causes every node to:

- **Publish and subscribe** to a shared Pub/Sub channel so that an event triggered
  on one node is fanned out to connections on all other nodes.
- **Coordinate presence channels** — member joins and leaves are written to Redis so
  that any node can answer a presence query with the full, consistent member list.
- **Route user-targeted operations** — `POST /users/{id}/terminate_connections` finds
  the node(s) holding a specific user's connections via Redis and forwards the
  termination command accordingly.
- **Self-heal across Redis restarts** — nodes monitor the Redis connection and
  reconnect automatically; a brief Redis outage does not crash pylon, it only
  degrades cluster-state consistency until reconnection.

---

## Enabling the redis adapter

Set these two variables identically on every pylon node:

```env
PYLON_ADAPTER=redis
PYLON_REDIS_URL=redis://your-redis-host:6379
```

All other configuration (bind address, port, `PYLON_APPS_PATH`, …) stays per-node.
The apps list must be **identical** on every node; pylon does not replicate it through Redis.

### Optional Redis knobs

| Variable | Default | Purpose |
|---|---|---|
| `PYLON_REDIS_PREFIX` | `pylon` | Key prefix for all pylon Redis keys — change if you share a Redis instance with other services. |
| `PYLON_REDIS_POOL_SIZE` | `6` | Connection-pool size per node. |
| `PYLON_REDIS_MEMBERSHIP_TTL` | `60` | Seconds before a node's membership entry expires if it stops heartbeating. |
| `PYLON_REDIS_NODE_HEARTBEAT` | `5` | Heartbeat interval (seconds) at which each node refreshes its own `node:{id}` liveness key and its per-app capacity counts. |
| `PYLON_REDIS_PRESENCE_HEARTBEAT` | `25` | Interval (seconds) of the **membership reconciliation tick** — see [below](#membership-reconciliation). Despite the name this does considerably more than refresh presence members. |
| `PYLON_REDIS_SWEEP_INTERVAL` | `10` | Interval (seconds) at which the lease-locked sweeper reclaims stale presence members, channels, and dead nodes' capacity units. |
| `PYLON_REDIS_SHARDED_PUBSUB` | `false` | Use Redis 7+ sharded Pub/Sub (`SSUBSCRIBE`/`SPUBLISH`) for higher-throughput clusters. Requires Redis 7.0+, and **every** node must set the same value — sharded and ordinary Pub/Sub are separate namespaces. |
| `PYLON_CLUSTER_ENVELOPE_COMPAT` | `true` | Drop the legacy `event` field from relayed envelopes when set `0`/`false`/`off` — roughly halves cluster-bus bandwidth. **Only safe once every node runs a build that ships this knob; v0.3.0 is NOT enough** (see [below](#cluster-envelope-compat-post-030-fleets-only)). |

See [Configuration](configuration.md) for the full variable reference.

### Membership reconciliation {#membership-reconciliation}

Every `PYLON_REDIS_PRESENCE_HEARTBEAT` seconds (default 25) each node runs a
**level-driven reconciliation tick**: rather than replaying past events, it
re-asserts from its own in-process registry the whole of what Redis must know
about it —

- every local presence member's and user binding's `expireAt` stamp, plus the
  whole-key TTL backstops on those hashes;
- the `apps` / `chans` / `users` index sets those entries are enumerated
  through; and
- the pub/sub subscriptions that carry cross-node traffic to this node — it
  `SUBSCRIBE`s any channel or user key it has local members for but is not
  attached to.

This is what bounds the blast radius of a lost edge. An index entry or a Redis
`SUBSCRIBE` missed because a bridge command was dropped, because Redis restarted
and lost its keyspace, or because a sweeper false-reap removed live members, is
restored on the next tick instead of staying lost for the life of the process.
Previously such a loss was permanent: the node stayed deaf to that channel's
cross-node traffic while still reporting `pylon_redis_connected 1`.

Two properties worth knowing when tuning the interval:

- **A healthy node pays no extra Redis round-trip for the pub/sub half** — the
  desired subscription set is diffed against the client's own in-memory tracked
  set. All the writes ride a single pipeline per tick.
- **The interval is your worst-case repair latency.** Pub/sub has no replay, so
  frames lost inside the window are lost for good. Lowering it shortens that
  window at the cost of more frequent (but still batched) Redis writes; the
  stamp horizon `PYLON_REDIS_MEMBERSHIP_TTL` (default 60 s) must stay
  comfortably above it, since it is what a tick refreshes.

A dead node simply stops ticking — its stamps go stale and the sweeper reaps
them.

!!! note "Presence rosters follow the oldest live connection"
    A presence user's advertised `user_info` is seated on that user's oldest
    still-live connection, cluster-wide, via a per-channel `presseats` hash.
    When the seating connection leaves, the roster is re-seated onto the
    next-oldest survivor inside the same atomic script that records the leave.
    Previously the first writer's value was advertised forever, so a user who
    updated their profile, opened a new tab and closed the old one kept showing
    the stale value to everyone who joined afterwards.

    **Sizing:** `presseats` holds one `user_info` copy per presence
    **connection** rather than per user, each bounded by
    `PYLON_MAX_PRESENCE_USER_INFO_BYTES` (default 1024 B). Budget roughly
    `connections × (user_info + ~60 B)` per presence channel.

    **Rolling upgrade:** the hash is purely additive — no flag, no backfill, no
    coordinated restart. Nodes on older builds neither read nor write it, and
    `presmembers` (which both builds maintain) stays the liveness truth. A user
    whose oldest live connection sits on a not-yet-upgraded node keeps the old
    value until that node is upgraded. Rolling **back** leaves `presseats:*`
    hashes that the older vacate path will not drain; they are inert and can be
    deleted manually once every node is downgraded.

### Cluster envelope compat (post-0.3.0 fleets only)

Since v0.3.0, every relayed frame travels on the Redis bus with **both** an
`event` field (the pre-0.3 wire shape) and a `frame_b64` field — receivers
prefer `frame_b64` and fall back to `event`, which is what makes a
0.2.x ↔ 0.3.x rolling upgrade safe in both directions. The cost is that each
frame is carried twice, roughly doubling cluster-bus bandwidth.

Once **every node in the cluster runs a build that ships this knob**, set this
on all nodes to retire the duplicate:

```env
PYLON_CLUSTER_ENVELOPE_COMPAT=0
```

Frame-carrying envelopes then omit the legacy `event` field (`frame_b64` is the
sole carrier); control envelopes are unchanged, and receivers decode both
shapes either way.

**v0.3.0 itself does not qualify.** The builds that ship this knob also taught
the receiver to decode an envelope with no `event` member; a v0.3.0 receiver
never learned that, so its decode fails and it **silently drops every
compat-off frame** — cross-node delivery stops with no error anywhere. Do not
enable this on a fleet that includes any v0.3.0 (or older) node, and do not
enable it during a mixed-version rollout. The setting only changes what a node
*sends* — as with `PYLON_REDIS_SHARDED_PUBSUB`, keep it uniform across the
fleet.

---

## Load balancer requirements

Any standard TCP/HTTP load balancer works — pylon does **not** require session affinity.
The only requirements are:

1. **Pass the WebSocket Upgrade through.** The load balancer must forward the
   `Upgrade: websocket` and `Connection: Upgrade` headers unchanged, and must
   not buffer or rewrite the HTTP response. Most modern LBs (HAProxy, nginx,
   AWS ALB, GCP LB) support this out of the box; verify the WebSocket mode is
   enabled in the LB configuration.

2. **Use `/ready` for health checks.** During a rolling update, pylon flips
   `/ready` to `503` before draining connections. Configure the LB to use
   `GET /ready` as its health probe (not `/health`) so that draining nodes stop
   receiving new connections before existing ones are closed.

3. **Long connection timeouts.** The Pusher heartbeat cycle is 120 s; configure
   idle-connection timeouts well above that (3 600 s is a safe default).

---

## Two-node Docker Compose example

The repository ships a ready-to-run 2-node cluster under `deploy/docker/docker-compose.yml`.
It starts Redis 7, `pylon-1` (host port 7000), and `pylon-2` (host port 7001),
all sharing the same `apps.json` and connected via the redis adapter.

```bash
# 1. Apply host kernel tuning (needed once per Docker host):
cp deploy/systemd/99-pylon.sysctl.conf /etc/sysctl.d/
sysctl --system

# 2. Create and edit the apps config — change the secret!
cp deploy/systemd/apps.example.json deploy/docker/apps.json

# 3. Build the image and start the cluster.
cd deploy/docker
docker compose up -d --build

# 4. Verify both nodes are healthy.
curl -s http://localhost:7000/health
curl -s http://localhost:7001/health
```

In production, put a load balancer (nginx, HAProxy, or a cloud LB) in front of
the two nodes and route traffic to whichever node responds healthy on `/ready`.

---

## Redis high availability

For production, run one of:

- **Single instance** with a persistent volume — the simplest option, and a
  fit for most deployments (Redis here is a coordination plane, not a data
  plane).
- **Primary + replica**, manually or Sentinel-promoted.

Pylon reconnects automatically on connection loss, so a failover window
(typically 10–30 s) results in a brief degradation rather than a hard outage.
The event a failover subjects pylon to — Redis going away and coming back — is
exercised on every push by the `redis_failover` regression suite in CI, which
bounces a dedicated Redis container and asserts that cross-node delivery
resumes automatically.

!!! warning "Do NOT use Redis Cluster"
    Pylon's membership and per-app-capacity Lua scripts operate on multiple
    keys in one script (the per-channel occupancy hash plus the app-level
    channel index; the `appconns` and `nodeconns:{node}` capacity hashes).
    Those keys do not share a hash slot, so the scripts are **not
    CROSSSLOT-safe** under Redis Cluster and would fail at runtime. Use a
    single instance, a primary-replica pair, or Sentinel instead.

!!! warning "Redis is a coordination plane, not a data plane"
    All WebSocket frames are delivered directly between pylon nodes and their
    connected clients. Redis only coordinates channel state and fan-out routing;
    its throughput requirements are far lower than the WebSocket message rate.
