#!/usr/bin/env node
'use strict';

// pylon conformance adapter — client plane on the official pusher-js SDK
// (npm `pusher-js`, Node runtime via `pusher-js/node`).
//
// Modes (the harness spawns these; see conformance/src/adapter.rs):
//
//   node runner.js --scenario <id> --env <env.json>
//       Run one scenario; the FINAL stdout line is the verdict JSON
//       {scenario, verdict: pass|fail|skip, observations, error, duration_ms}.
//       All logs go to stderr; stdout carries only the verdict.
//
//   node runner.js --version    Print the SDK's package version.
//   node runner.js --list       Print implemented scenario ids, one per line.
//
// Server-side publishes (C-PUB-SUB/PRIV/CACHE/ENC) shell out to the sibling
// pusher-http-node runner's `--fire-stdin` mode (spec JSON on the child's
// stdin — never argv), so ALL server-plane protocol work rides on the
// official server SDK. User termination (U-TERMINATE) shells out to the same
// runner's `--terminate` mode (id shape-guarded on both sides).
//
// Verified SDK facts this runner relies on (pusher-js 8.6.0, dist inspected):
//   - `require('pusher-js/node')` exports the Pusher class directly.
//   - The options object REQUIRES a non-null `cluster` (validateOptions
//     throws otherwise); `wsHost` overrides the cluster-derived host, so we
//     pass a dummy cluster + explicit wsHost/wsPort.
//   - Encrypted channels decrypt with tweetnacl (bundled in the node runtime,
//     exposed via config.nacl) — NOT WebCrypto. The shared secret arrives as
//     `shared_secret` (base64) in the auth response, supplied by the
//     pusher-http-node `--sign` router when the app has an encryption master
//     key. The server SDK encrypts the payload on trigger for
//     private-encrypted-* channels; pylon relays the envelope verbatim.
//   - `pusher:cache_miss` is not a pusher_internal: event, so pusher-js
//     delivers it as a regular channel event (channel.bind) AND to global
//     bind_global handlers.
//   - Non-internal events reach bind_global handlers even when no channel
//     object exists — the strong detector for "no delivery after unsubscribe".
//   - In-band `pusher:error` frames (4301/4302/4009) carry no `channel`, so
//     they surface on `connection.bind('error', e => ...)` as
//     `{type:'PusherError', data:{code,message}}` — a CHANNEL bind cannot see
//     them (dist src/core/connection/connection.ts: the message handler emits
//     'error' before pusher.ts routes channel events).
//   - Handshake-phase rejections (4001 unknown key / 4003 disabled) arrive as
//     a `pusher:error` MESSAGE during the handshake; the Handshake maps it
//     through Protocol.getCloseAction/getCloseError and the manager re-emits
//     `{type:'WebSocketError', error:{type:'PusherError', data:{code}}}` then
//     disconnects → state 'disconnected'. Both error shapes are normalized by
//     `connErrorCode` below.
//   - `channel.trigger` validates ONLY the `client-` prefix client-side (dist
//     src/core/channels/channel.ts) — no name-length, payload-size, or
//     channel-kind gate — so oversize names/payloads and public-channel
//     triggers are SENT and enforcement is observed server-side.
//   - `pusher:signin_success` is consumed internally (dist src/core/user.ts
//     _onSigninSuccess emits nothing); the public detection surface is the
//     connection's `message` event. The signed-in identity is
//     `p.user.user_data.id` (no `p.user.id`).
//   - User auth: pusher-js posts `socket_id` + `userAuthentication.params` as
//     a urlencoded body and sends the RESPONSE's user_data verbatim in
//     `pusher:signin` — so the user id/watchlist ride the params and the
//     --sign router constructs the full user_data (see connectUser).
//   - The WatchlistFacade (p.user.watchlist) re-emits exactly the server's
//     watchlist event names ('online'/'offline', payload {name, user_ids}) —
//     there is NO 'initial' event in 8.6.0; the initial online snapshot
//     arrives as an ordinary 'online' event.

const fs = require('fs');
const path = require('path');
const { execFile } = require('child_process');

// The SDK: `require('pusher-js/node')` IS the Pusher class.
const Pusher = require('pusher-js/node');

const argv = process.argv.slice(2);
const arg = (n) => {
  const i = argv.indexOf(n);
  return i >= 0 && i + 1 < argv.length ? argv[i + 1] : null;
};
const has = (n) => argv.includes(n);
const log = (...a) => console.error('[runner]', ...a);

// The env contract (conformance AdapterEnv): ws_url, http_host, http_port,
// app_id, app_key, app_secret, auth_endpoint, webhook_receiver.
let env = null;
const loadEnv = () => {
  if (env === null) {
    const p = arg('--env');
    if (!p) throw new Error('--env <path> is required');
    env = JSON.parse(fs.readFileSync(p, 'utf8'));
  }
  return env;
};

const assertOk = (cond, msg) => {
  if (!cond) throw new Error(msg);
};

const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

// ---------------------------------------------------------------------------
// Client helpers.
// ---------------------------------------------------------------------------

// One pusher-js client bound to pylon's single-port WS+HTTP plane. pylon
// serves WS and REST on the same port (env.http_port). `cluster` is mandatory
// SDK validation but never consulted for the host once wsHost is set. An
// optional `key` opt overrides env.app_key (E-BADKEY / E-DISABLED).
const connect = (opts = {}) => {
  const e = loadEnv();
  const { key = e.app_key, ...rest } = opts;
  const auth = {
    channelAuthorization: {
      endpoint: e.auth_endpoint,
      transport: 'ajax',
    },
    userAuthentication: {
      endpoint: e.auth_endpoint,
      transport: 'ajax',
    },
  };
  return new Pusher(key, {
    wsHost: e.http_host,
    wsPort: e.http_port,
    httpHost: e.http_host,
    httpPort: e.http_port,
    forceTLS: false,
    cluster: 'local',
    ...auth,
    ...rest,
  });
};

// Per-client presence identity: pusher-js posts channelAuthorization.params
// alongside socket_id/channel_name; the --sign router builds the presence
// channelData from the body's user_id param.
const connectAs = (userId, opts = {}) => {
  const e = loadEnv();
  return connect({
    channelAuthorization: {
      endpoint: e.auth_endpoint,
      transport: 'ajax',
      params: { user_id: userId },
    },
    ...opts,
  });
};

// Per-client USER identity (pusher:signin). pusher-js CANNOT carry the
// user_data itself: its signin posts only `socket_id` +
// userAuthentication.params (dist src/core/auth/user_authenticator.ts,
// composeChannelQuery), and the user_data that reaches the server is the auth
// RESPONSE's, verbatim (src/core/user.ts _onAuthorize sends
// authData.user_data). So the identity rides the params: `user_id` names the
// user and `watchlist` (a JSON string) is merged into the signed user_data by
// the --sign router — all through the PUBLIC userAuthentication.params option
// (no customHandler needed).
const connectUser = (userId, watchlist, opts = {}) => {
  const e = loadEnv();
  const params = { user_id: userId };
  if (watchlist !== undefined) {
    // Official Pusher docs shape: a BARE ARRAY of user ids
    // (https://pusher.com/docs/channels/server_api/authenticating-users/).
    params.watchlist = JSON.stringify(watchlist);
  }
  return connect({
    userAuthentication: {
      endpoint: e.auth_endpoint,
      transport: 'ajax',
      params,
    },
    ...opts,
  });
};

// Resolve once the connection reports state `connected`; reject on a
// connection-level error or after `ms` of waiting.
const waitConnected = (p, ms = 10000) =>
  new Promise((res, rej) => {
    if (p.connection.state === 'connected') return res();
    const t = setTimeout(() => rej(new Error('not connected: ' + p.connection.state)), ms);
    p.connection.bind('connected', () => {
      clearTimeout(t);
      res();
    });
    p.connection.bind('error', (e) => {
      clearTimeout(t);
      rej(new Error('conn error ' + JSON.stringify((e && e.error && e.error.data) || e)));
    });
  });

// Poll until the connection reaches `state` (pusher-js emits state events,
// but polling is robust against a state that changed while we were wiring).
const waitForState = (conn, want, ms = 5000) =>
  new Promise((res, rej) => {
    const t0 = Date.now();
    const iv = setInterval(() => {
      if (conn.state === want) {
        clearInterval(iv);
        res();
      } else if (Date.now() - t0 > ms) {
        clearInterval(iv);
        rej(new Error(`state ${want} not reached within ${ms}ms (at ${conn.state})`));
      }
    }, 50);
  });

// Poll until the connection reaches ANY of `states`; reject after `ms`.
const waitForStateAny = (conn, states, ms = 5000) =>
  new Promise((res, rej) => {
    const t0 = Date.now();
    const iv = setInterval(() => {
      if (states.includes(conn.state)) {
        clearInterval(iv);
        res(conn.state);
      } else if (Date.now() - t0 > ms) {
        clearInterval(iv);
        rej(new Error(`none of [${states}] reached within ${ms}ms (at ${conn.state})`));
      }
    }, 50);
  });

// Resolve with the next `name` event data on `channel` for which `pred`
// holds; reject after `ms`.
const waitEvent = (channel, name, ms = 10000, pred = () => true) =>
  new Promise((res, rej) => {
    const t = setTimeout(() => rej(new Error(`no ${name} within ${ms}ms`)), ms);
    channel.bind(name, (d) => {
      if (pred(d)) {
        clearTimeout(t);
        res(d);
      }
    });
  });

// The error code off EITHER connection-error shape pusher-js emits:
//  - in-band `pusher:error` after connect: {type:'PusherError', data:{code}}
//  - handshake-phase rejection: {type:'WebSocketError', error:{data:{code}}}
//    (Handshake -> Protocol.getCloseError, manager re-emits as WebSocketError)
const connErrorCode = (e) => {
  const code = e && e.data && e.data.code;
  return code !== undefined ? code : e && e.error && e.error.data && e.error.data.code;
};

// Resolve with the next connection `error` event for which `pred(e)` holds.
// Bind BEFORE the action that provokes the error — errors are events, not
// state, and a late bind misses them.
const waitConnError = (p, pred = () => true, ms = 10000) =>
  new Promise((res, rej) => {
    const t = setTimeout(
      () => rej(new Error(`no matching connection error within ${ms}ms`)),
      ms
    );
    p.connection.bind('error', (e) => {
      if (pred(e)) {
        clearTimeout(t);
        res(e);
      }
    });
  });

// Collect every connection error event (evidence + counting).
const errorRecorder = (p) => {
  const errors = [];
  p.connection.bind('error', (e) => {
    errors.push(e);
    log('conn error:', JSON.stringify({ type: e.type, code: connErrorCode(e) }));
  });
  return {
    errors,
    count: (pred = () => true) => errors.filter(pred).length,
  };
};

// Resolve once `rec.count(pred)` reaches `n`; reject after `ms`. Used when
// several provoked errors are indistinguishable except by arrival count.
const waitCount = (rec, pred, n, ms = 10000) =>
  new Promise((res, rej) => {
    const t0 = Date.now();
    const iv = setInterval(() => {
      if (rec.count(pred) >= n) {
        clearInterval(iv);
        res();
      } else if (Date.now() - t0 > ms) {
        clearInterval(iv);
        rej(new Error(`only ${rec.count(pred)} of ${n} matching errors within ${ms}ms`));
      }
    }, 50);
  });

// Resolve with a count once it has stopped growing for `quietMs`, bounded by
// `ms` total (the final value is returned either way). For provoked-error
// backlogs whose drain time is unknown.
const settleCount = (get, quietMs, ms) =>
  new Promise((res) => {
    const t0 = Date.now();
    let last = get();
    let lastChange = Date.now();
    const iv = setInterval(() => {
      const now = get();
      if (now !== last) {
        last = now;
        lastChange = Date.now();
      }
      if (Date.now() - lastChange >= quietMs || Date.now() - t0 > ms) {
        clearInterval(iv);
        res(last);
      }
    }, 50);
  });

// Resolve on the `pusher:signin_success` frame. pusher-js 8.6.0 does NOT emit
// this on any public emitter (src/core/user.ts consumes it internally — no
// user.bind surface for it), but the connection re-emits every frame via
// `message`, which IS public. Bind BEFORE p.signin() to avoid the race.
const waitSigninSuccess = (p, ms = 10000) =>
  new Promise((res, rej) => {
    const t = setTimeout(
      () => rej(new Error(`no pusher:signin_success within ${ms}ms`)),
      ms
    );
    p.connection.bind('message', (m) => {
      if (m && m.event === 'pusher:signin_success') {
        clearTimeout(t);
        res(m.data);
      }
    });
  });

// Every non-pusher_internal event this client sees, via bind_global. Works
// even for channels the client has unsubscribed (no channel object needed).
const eventRecorder = (p) => {
  const seen = [];
  p.bind_global((eventName, data) => seen.push({ event: eventName, data }));
  return {
    seen,
    count: (name, pred = () => true) =>
      seen.filter((e) => e.event === name && pred(e)).length,
  };
};

// ---------------------------------------------------------------------------
// Server-side actions: shell out to the sibling http adapter's modes, so ALL
// server-plane protocol work rides on the official server SDK.
// ---------------------------------------------------------------------------

const HTTP_ADAPTER_DIR = path.join(__dirname, '..', 'pusher-http-node');

// Publish one event server-side. The spec rides child STDIN (`--fire-stdin`
// mode), NEVER argv: execFile uses no shell, but a value token in a flag
// position is still the flag-injection shape — JSON belongs on a pipe.
// The child is bounded: 8s timeout, SIGTERM kill signal — the last unbounded
// child wait in this runner (everything else already ran under an explicit
// deadline or the harness budget's process-group kill).
const FIRE_TIMEOUT_MS = 8000;
const fire = (spec) =>
  new Promise((resolve, reject) => {
    const child = execFile(
      process.execPath,
      ['runner.js', '--fire-stdin', '--env', arg('--env')],
      { cwd: HTTP_ADAPTER_DIR, timeout: FIRE_TIMEOUT_MS, killSignal: 'SIGTERM' },
      (err, stdout, stderr) => {
        if (err) {
          const timedOut = err.killed && err.signal === 'SIGTERM';
          reject(
            new Error(
              'fire failed: ' +
                (timedOut
                  ? `child killed after ${FIRE_TIMEOUT_MS}ms (SIGTERM)`
                  : String(stderr).trim() || err.message)
            )
          );
        } else {
          resolve(String(stdout).trim());
        }
      }
    );
    // EPIPE if the child dies before reading: swallowed here so the callback
    // above is the single failure path.
    child.stdin.on('error', () => {});
    child.stdin.end(JSON.stringify(spec));
  });

// Shape guard for any user id that reaches an ARGV position: alnum first
// char (never a leading `-`), then alnum/_/.//- up to 128 chars total. The
// harness only ever passes fixed ids ('u-term', ...) — pure flag-injection
// guarding, not identity validation. Mirrored in the http runner (receiver).
const USER_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;

// Terminate every connection of a signed-in user, server-side, through the
// sibling http adapter's --terminate mode (the official server SDK's
// terminateUserConnections → POST /users/<id>/terminate_connections). The id
// occupies an argv position in the child, so it is shape-guarded HERE and
// again at the receiver.
const terminateUser = (userId) =>
  new Promise((resolve, reject) => {
    if (typeof userId !== 'string' || !USER_ID_RE.test(userId)) {
      return reject(new Error('terminateUser: malformed user id (want ' + USER_ID_RE + ')'));
    }
    execFile(
      process.execPath,
      ['runner.js', '--terminate', userId, '--env', arg('--env')],
      { cwd: HTTP_ADAPTER_DIR },
      (err, stdout, stderr) => {
        if (err) {
          reject(new Error('terminate failed: ' + (String(stderr).trim() || err.message)));
        } else {
          resolve(String(stdout).trim());
        }
      }
    );
  });

// ---------------------------------------------------------------------------
// Scenarios (verdict: pass | fail | skip).
// ---------------------------------------------------------------------------

const SOCKET_ID_RE = /^\d+\.\d+$/;

const SCENARIOS = {
  'C-ESTABLISH': async () => {
    const p = connect();
    await waitConnected(p);
    const socketId = p.connection.socket_id;
    assertOk(typeof socketId === 'string' && SOCKET_ID_RE.test(socketId),
      `socket_id shape: ${socketId}`);
    // Normalization contract: observations carry no run-unique values — the
    // raw id goes to stderr as evidence, the placeholder into observations.
    log('socket_id:', socketId);
    const state = p.connection.state;
    p.disconnect();
    return { socket_id: '<socket_id>', state };
  },

  'C-RECONNECT': async () => {
    const first = connect();
    await waitConnected(first);
    const s1 = first.connection.socket_id;
    assertOk(SOCKET_ID_RE.test(s1), `first socket_id shape: ${s1}`);
    first.disconnect();
    await waitForState(first.connection, 'disconnected');
    const second = connect();
    await waitConnected(second);
    const s2 = second.connection.socket_id;
    assertOk(SOCKET_ID_RE.test(s2), `second socket_id shape: ${s2}`);
    assertOk(s2 !== s1, `socket_id did not rotate: ${s1} vs ${s2}`);
    second.disconnect();
    return { reconnected: true, socket_id_rotated: true };
  },

  // There is no public sendPing() on pusher-js 8.x. Instead drive the
  // activity/pong machinery: with activityTimeout 2000 the client sends
  // pusher:ping after every 2s of silence and expects pusher:pong within
  // pongTimeout (4000). Broken pongs make the connection cycle (the client
  // treats the silence as a dead link), so "still connected after 8s" plus
  // at least one observed pong is the honest assertion.
  'C-PING': async () => {
    const p = connect({ activityTimeout: 2000, pongTimeout: 4000 });
    // Connect wait is capped at 6s so the WORST case fits the 15s catalog
    // budget: 6s connect + 8s hold = 14s, leaving 1s of headroom (the old
    // 10s default connect wait could burn 18s+ alone).
    await waitConnected(p, 6000);
    let pongs = 0;
    p.connection.bind('message', (m) => {
      if (m && m.event === 'pusher:pong') pongs++;
    });
    await sleep(8000);
    assertOk(p.connection.state === 'connected',
      `connection not alive after 8s: ${p.connection.state}`);
    assertOk(pongs >= 1, `no pusher:pong observed in 8s (${pongs})`);
    // Normalization contract: the pong count varies run to run with timing —
    // raw count to stderr as evidence, the assert floor into observations.
    log('pusher:pong count in 8s:', pongs);
    p.disconnect();
    return { alive_after_8s: true, activity_timeout_used_ms: 2000, pongs_observed: '>=1' };
  },

  'C-PUB-SUB': async () => {
    const p = connect();
    const rec = eventRecorder(p);
    await waitConnected(p);
    const ch = p.subscribe('cf-pub-sub');
    const subData = await waitEvent(ch, 'pusher:subscription_succeeded');
    // P12 (client-side): public subscription_succeeded data stringifies to {}.
    const subJson = JSON.stringify(subData);
    assertOk(subJson === '{}', `subscription_succeeded data: ${subJson}`);
    const waiter = waitEvent(ch, 'srv-event', 10000, (d) => d && d.k === 1);
    await fire({ channel: 'cf-pub-sub', name: 'srv-event', data: { k: 1 } });
    await waiter;
    p.unsubscribe('cf-pub-sub');
    await sleep(300); // let the unsubscribe land before firing again
    await fire({ channel: 'cf-pub-sub', name: 'srv-event', data: { k: 2 } });
    await sleep(2000);
    const late = rec.count('srv-event', (e) => e.data && e.data.k === 2);
    assertOk(late === 0, `event delivered after unsubscribe (${late} time(s))`);
    p.disconnect();
    return { subscribed_delivered: true, unsubscribed_silent: true, subscription_succeeded_data: subJson };
  },

  'C-PRIV-SUB': async () => {
    const p = connect();
    await waitConnected(p);
    const ch = p.subscribe('private-cf-priv');
    await waitEvent(ch, 'pusher:subscription_succeeded');
    const waiter = waitEvent(ch, 'srv-event', 10000, (d) => d && d.echo === true);
    await fire({ channel: 'private-cf-priv', name: 'srv-event', data: { echo: true } });
    await waiter;
    p.disconnect();
    return { auth_flow: 'ok', delivered: true };
  },

  'C-PRES-SUB': async () => {
    const A = connectAs('u-a');
    const B = connectAs('u-b');
    try {
      await waitConnected(A);
      await waitConnected(B);
      const a = A.subscribe('presence-cf-pres');
      await waitEvent(a, 'pusher:subscription_succeeded');
      assertOk(a.members.count === 1, `initial members.count ${a.members.count}`);
      assertOk(a.members.me && a.members.me.id === 'u-a',
        `A's own member id: ${a.members.me && a.members.me.id}`);
      const counts = [a.members.count];
      const added = waitEvent(a, 'pusher:member_added', 10000, (m) => m && m.id === 'u-b');
      const b = B.subscribe('presence-cf-pres');
      await waitEvent(b, 'pusher:subscription_succeeded');
      await added;
      counts.push(a.members.count);
      const removed = waitEvent(a, 'pusher:member_removed', 10000, (m) => m && m.id === 'u-b');
      B.unsubscribe('presence-cf-pres');
      await removed;
      counts.push(a.members.count);
      assertOk(counts.join('-') === '1-2-1', `count sequence ${counts.join('-')}`);
      return { roster_maintained: true, member_events: ['<added>', '<removed>'], count_sequence: counts.join('-') };
    } finally {
      A.disconnect();
      B.disconnect();
    }
  },

  'C-CACHE-SUB': async () => {
    // Populated case: A subscribes, the server publishes (A sees it live and
    // pylon caches it), A leaves; fresh client B subscribes and must receive
    // the cached replay as a normal event bind.
    const A = connect();
    try {
      await waitConnected(A);
      const chA = A.subscribe('cache-cf-cache');
      await waitEvent(chA, 'pusher:subscription_succeeded');
      const liveWaiter = waitEvent(chA, 'srv-event', 10000, (d) => d === 'v1');
      await fire({ channel: 'cache-cf-cache', name: 'srv-event', data: 'v1' });
      await liveWaiter;
      A.unsubscribe('cache-cf-cache');
    } finally {
      A.disconnect();
    }
    const B = connect();
    try {
      await waitConnected(B);
      const chB = B.subscribe('cache-cf-cache');
      const replay = await waitEvent(chB, 'srv-event', 10000, (d) => d === 'v1');
      assertOk(replay === 'v1', `replay payload ${JSON.stringify(replay)}`);
      // Empty case: a genuinely never-populated cache channel. pusher-js
      // surfaces pylon's pusher:cache_miss as a regular channel event; the
      // global recorder sees it too — record which surfaces fired.
      const C = connect();
      try {
        await waitConnected(C);
        const recC = eventRecorder(C);
        const chC = C.subscribe('cache-cf-empty');
        await waitEvent(chC, 'pusher:cache_miss', 5000);
        const surfaces = [
          'channel',
          ...(recC.count('pusher:cache_miss') > 0 ? ['global'] : []),
        ].join('+');
        return { replay_delivered: 'v1', empty_cache_miss: true, cache_miss_surface: surfaces };
      } finally {
        C.disconnect();
      }
    } finally {
      B.disconnect();
    }
  },

  'C-ENC-SUB': async () => {
    // Probe the actual decryption capability: pusher-js 8.6.0 decrypts
    // encrypted channels with tweetnacl from config.nacl (bundled in the node
    // runtime), NOT WebCrypto — the brief's WebCrypto probe does not apply to
    // this SDK, so the skip gate probes what actually decrypts.
    const probe = connect();
    const probeCh = probe.subscribe('private-encrypted-cf-enc');
    const naclOk = !!(probeCh.nacl && typeof probeCh.nacl.secretbox.open === 'function');
    probe.disconnect();
    log('nacl probe:', naclOk, '| webcrypto subtle:', !!globalThis.crypto?.subtle);
    if (!naclOk) {
      return { skip: 'tweetnacl unavailable — encrypted-channel decryption impossible (pusher-js uses tweetnacl, not WebCrypto)' };
    }
    const p = connect();
    try {
      await waitConnected(p);
      const ch = p.subscribe('private-encrypted-cf-enc');
      // The auth flow carries the shared_secret the channel decrypts with.
      await waitEvent(ch, 'pusher:subscription_succeeded');
      const waiter = waitEvent(ch, 'srv-event', 10000, (d) => d && d.secret === 's3cr3t');
      await fire({
        channel: 'private-encrypted-cf-enc',
        name: 'srv-event',
        data: { secret: 's3cr3t' },
        encrypted: true,
      });
      const d = await waiter;
      assertOk(d.secret === 's3cr3t', `decrypted payload ${JSON.stringify(d)}`);
      return { decrypted: 's3cr3t' };
    } finally {
      p.disconnect();
    }
  },

  // Two clients subscribe to the SAME private channel simultaneously, then a
  // client event on A must reach B but not A (sender exclusion). The two
  // subscription waits are PRE-BOUND before either is awaited: each private
  // subscribe rides its own auth-endpoint round trip (one `node runner.js
  // --sign` spawn per client), and the two replies can complete in EITHER
  // order — under CPU contention the SECOND client's reply regularly lands
  // first. pusher-js channels do not buffer events, so with the sequential
  // shape (`await waitEvent(a); await waitEvent(b);`) B's listener was bound
  // only after A's event resolved; whenever B's `subscription_succeeded` was
  // emitted first it hit a channel with no listener and was lost forever —
  // the wait then burned its full 10s and failed with "no
  // pusher:subscription_succeeded within 10000ms" even though both
  // subscriptions had succeeded (observed on the 2-core CI runner and
  // reproduced under local CPU starvation; connect-level message trace showed
  // BOTH `pusher_internal:subscription_succeeded` frames arriving, B first).
  // Pre-binding also starts both 10s windows at the same instant, so the
  // worst case is ONE 10s window (concurrent), not two stacked ones.
  'C-EVENT-ECHO': async () => {
    const A = connect();
    const B = connect();
    try {
      await waitConnected(A);
      await waitConnected(B);
      const a = A.subscribe('private-cf-echo');
      const b = B.subscribe('private-cf-echo');
      const aSub = waitEvent(a, 'pusher:subscription_succeeded');
      const bSub = waitEvent(b, 'pusher:subscription_succeeded');
      await aSub;
      await bSub;
      // Watch for self-delivery BEFORE triggering.
      let selfDeliveries = 0;
      a.bind('client-x', () => selfDeliveries++);
      const peer = waitEvent(b, 'client-x', 10000, (d) => d && d.v === 1);
      const sent = a.trigger('client-x', { v: 1 });
      assertOk(sent === true, `trigger returned ${sent}`);
      await peer;
      // Sender exclusion is server-side (broadcast skips the sender's socket).
      await sleep(2000);
      assertOk(selfDeliveries === 0, `sender received its own client event (${selfDeliveries})`);
      return { delivered_to_peer: true, sender_excluded: true };
    } finally {
      A.disconnect();
      B.disconnect();
    }
  },

  // Observation-pinning: pusher-js 8.6.0 does NOT client-side-validate the
  // name length or payload size (src/core/channels/channel.ts trigger checks
  // only the `client-` prefix), so the enforcement lands server-side as an
  // in-band `pusher:error` 4301 — but WHICH side enforced it is exactly the
  // SDK-compat question, so both outcomes pass and the observation records
  // the side. Note the error frame carries no `channel`, so it surfaces on
  // the CONNECTION error bind, not a channel bind (dist connection.ts).
  'C-EVENT-LIMITS': async () => {
    const p = connect();
    try {
      await waitConnected(p);
      const rec = errorRecorder(p);
      const ch = p.subscribe('private-cf-limits');
      await waitEvent(ch, 'pusher:subscription_succeeded');

      const outcomes = {};

      // Oversized name: 'client-' + 210 chars = 217 > 200.
      const longName = 'client-' + 'x'.repeat(210);
      let nameThrown = null;
      try {
        ch.trigger(longName, {});
      } catch (e) {
        nameThrown = (e && e.message) || String(e);
      }
      if (nameThrown !== null) {
        log('oversized name rejected client-side:', nameThrown);
        outcomes.name_limit = 'client-rejected';
      } else {
        await waitConnError(p, (e) => connErrorCode(e) === 4301, 10000);
        outcomes.name_limit = 'server-4301';
      }

      // Oversized payload: 11,000-char string > 10 KiB budget.
      const bigPayload = 'x'.repeat(11000);
      let payloadThrown = null;
      try {
        ch.trigger('client-payload', bigPayload);
      } catch (e) {
        payloadThrown = (e && e.message) || String(e);
      }
      if (payloadThrown !== null) {
        log('oversized payload rejected client-side:', payloadThrown);
        outcomes.payload_limit = 'client-rejected';
      } else {
        // Per-leg expectation: count from THIS leg's baseline, not from the
        // name leg's outcome — whether the name leg also sent (and was
        // counted) must not change what this leg waits for. One NEW 4301
        // since the baseline is the payload leg's own rejection.
        const baseline = rec.count((e) => connErrorCode(e) === 4301);
        await waitCount(rec, (e) => connErrorCode(e) === 4301, baseline + 1, 10000);
        outcomes.payload_limit = 'server-4301';
      }

      // Raw error evidence to stderr (normalization contract).
      log('limit errors observed:', rec.count((e) => connErrorCode(e) === 4301));
      return outcomes;
    } finally {
      p.disconnect();
    }
  },

  // Burst 30 client events as fast as the loop allows: pylon's per-connection
  // budget (10/s token bucket) must reject SOME (>=1) but not all (<30 — the
  // burst allowance passes). The exact split varies with timing, so the count
  // goes to stderr and the observations carry the pinned facts.
  'C-EVENT-RATE': async () => {
    const p = connect();
    try {
      await waitConnected(p);
      const rec = errorRecorder(p);
      const ch = p.subscribe('private-cf-rate');
      await waitEvent(ch, 'pusher:subscription_succeeded');
      for (let i = 0; i < 30; i++) {
        ch.trigger('client-burst', { i });
      }
      // Wait for the rejection backlog to drain: settle when no new error has
      // arrived for 500ms, bounded at 5s.
      const settled = await settleCount(
        () => rec.count((e) => connErrorCode(e) === 4301),
        500,
        5000
      );
      log(`rate-limit 4301s: ${settled} of 30 triggers rejected`);
      assertOk(settled >= 1, `no rate-limit rejection among 30 triggers (${settled})`);
      assertOk(settled < 30, `every trigger rate-limited (${settled}) — burst allowance missing`);
      return { rate_limited: true, error_code: '4301', rejected_of_30: '<1..29>' };
    } finally {
      p.disconnect();
    }
  },

  // Ground truth vs the brief: pusher-js 8.6.0 does NOT refuse client events
  // on public channels client-side (src/core/channels/channel.ts trigger
  // checks only the `client-` prefix — no channel-kind gate). The trigger IS
  // sent and the server must silently drop it (client events are private/
  // presence-only). The observation records which side refused.
  // Same pre-bound subscription waits as C-EVENT-ECHO: today both
  // `pusher:subscribe` frames are sent synchronously in A-then-B order, so
  // the sequential awaits happen to be safe — but the server's two replies
  // ride two independent connections (possibly different workers) with no
  // FIFO guarantee, so B-first ordering would lose B's event exactly the way
  // C-EVENT-ECHO did. Pre-binding removes the dependence on reply order.
  'C-EVENT-PUB': async () => {
    const A = connect();
    const B = connect();
    try {
      await waitConnected(A);
      await waitConnected(B);
      const a = A.subscribe('cf-pub-client');
      const b = B.subscribe('cf-pub-client');
      const aSub = waitEvent(a, 'pusher:subscription_succeeded');
      const bSub = waitEvent(b, 'pusher:subscription_succeeded');
      await aSub;
      await bSub;
      let peerGot = 0;
      b.bind('client-x', () => peerGot++);
      let sent;
      let thrown = null;
      try {
        sent = a.trigger('client-x', { v: 1 });
      } catch (e) {
        thrown = (e && e.message) || String(e);
      }
      const side = thrown !== null ? 'client-refused' : 'sent-server-silent';
      log('public-channel trigger:', thrown !== null ? `threw: ${thrown}` : `returned ${sent}`);
      await sleep(2000);
      assertOk(peerGot === 0, `client event delivered on a public channel (${peerGot})`);
      return { public_client_events: side, delivered_to_peer: false };
    } finally {
      A.disconnect();
      B.disconnect();
    }
  },

  'U-SIGNIN': async () => {
    const p = connectUser('u-sign');
    try {
      await waitConnected(p);
      const ok = waitSigninSuccess(p);
      p.signin();
      await ok;
      // pusher-js 8.6.0 surface: p.user.user_data.id (p.user itself exists
      // from construction; there is no p.user.id).
      const userData = p.user && p.user.user_data;
      assertOk(userData && userData.id === 'u-sign', `signed-in user data: ${JSON.stringify(userData)}`);
      log('user_data:', JSON.stringify(userData));
      return { signed_in: 'u-sign', user_data: '<user_data>' };
    } finally {
      p.disconnect();
    }
  },

  // Watchlist across two connections. Ordering matters: O signs in FIRST, so
  // W's signin (watching u-online) must immediately produce the INITIAL
  // online snapshot — pusher-js 8.6.0 has no `initial` watchlist event (dist
  // src/core/watchlist.ts re-emits only the server's event names), so the
  // snapshot arrives as an ordinary `online` event. Then O disconnects and W
  // must see the `offline` edge.
  'U-WATCH': async () => {
    const O = connectUser('u-online');
    const W = connectUser('u-w', ['u-online']);
    try {
      await waitConnected(O);
      await waitConnected(W);
      const oIn = waitSigninSuccess(O);
      O.signin();
      await oIn;
      // Bind before signin: the snapshot rides the signin exchange itself.
      const initial = waitEvent(
        W.user.watchlist,
        'online',
        10000,
        (e) => e && Array.isArray(e.user_ids) && e.user_ids.includes('u-online')
      );
      const wIn = waitSigninSuccess(W);
      W.signin();
      await wIn;
      await initial;
      const offline = waitEvent(
        W.user.watchlist,
        'offline',
        10000,
        (e) => e && Array.isArray(e.user_ids) && e.user_ids.includes('u-online')
      );
      O.disconnect();
      await offline;
      return { initial_snapshot: 'online', offline_edge: true };
    } finally {
      O.disconnect();
      W.disconnect();
    }
  },

  // 150 watched ids > pylon's 100 cap: signin must STILL succeed (signin is
  // not the thing that fails) and the overflow surfaces as a non-fatal
  // in-band `pusher:error` 4302.
  'U-WATCH-LIMIT': async () => {
    const ids = Array.from({ length: 150 }, (_, i) => `u-lim-${i}`);
    const p = connectUser('u-watch-limit', ids);
    try {
      await waitConnected(p);
      const errP = waitConnError(p, (e) => connErrorCode(e) === 4302, 10000);
      const ok = waitSigninSuccess(p);
      p.signin();
      await ok;
      await errP;
      const userData = p.user && p.user.user_data;
      assertOk(userData && userData.id === 'u-watch-limit', `user data after capped signin: ${JSON.stringify(userData)}`);
      return { signed_in: true, limit_error: '4302', watched_ids: 150 };
    } finally {
      p.disconnect();
    }
  },

  // Server-side terminateUserConnections must tear down every connection of
  // the signed-in user (in-band `pusher:error` 4009 + WS close 4009), while a
  // plain connection of the SAME app — never signed in — stays connected.
  'U-TERMINATE': async () => {
    const A = connectUser('u-term');
    const B = connect();
    try {
      await waitConnected(A);
      await waitConnected(B);
      const ok = waitSigninSuccess(A);
      A.signin();
      await ok;
      const errP = waitConnError(A, (e) => connErrorCode(e) === 4009, 10000);
      await terminateUser('u-term');
      await errP;
      // The in-band error is followed by the WS close (4009 → 'refused' →
      // disconnect): the connection must leave `connected` — ASSERTED, not
      // just recorded (a socket that survives its own terminate is a fail).
      const state = await waitForStateAny(A.connection, ['disconnected', 'failed'], 5000).catch(
        () => A.connection.state
      );
      assertOk(state !== 'connected', `socket still ${state} after in-band 4009 terminate`);
      await sleep(1000);
      assertOk(B.connection.state === 'connected', `plain peer affected by terminate (${B.connection.state})`);
      return { terminated: true, close_class: '4009', post_terminate_state: state, plain_peer_unaffected: true };
    } finally {
      A.disconnect();
      B.disconnect();
    }
  },

  // Unknown app key: pylon answers the handshake with `pusher:error` 4001,
  // which pusher-js maps to close-action 'refused' → the connection errors
  // and lands in disconnected/failed. The rejection code is the pinned
  // observation; the state is recorded alongside it.
  'E-BADKEY': async () => rejectedConnectScenario('nope-key', '4001'),

  // The disabled app from apps.json: same shape, pylon's 4003.
  'E-DISABLED': async () => rejectedConnectScenario('cf-key-disabled', '4003'),
};

// Shared body of E-BADKEY / E-DISABLED: connect with a bad key and assert the
// connection is rejected (a 4xxx error code + a non-connected terminal
// state). The error is bound immediately after construction — the handshake
// rejection can land within milliseconds.
async function rejectedConnectScenario(key, expectedCode) {
  const p = connect({ key });
  try {
    const err = await waitConnError(
      p,
      (e) => {
        const c = connErrorCode(e);
        return typeof c === 'number' && c >= 4000 && c < 5000;
      },
      10000
    );
    const code = String(connErrorCode(err));
    log('rejection:', JSON.stringify(err && (err.data || (err.error && err.error.data))));
    assertOk(code === expectedCode, `rejection code ${code}, expected ${expectedCode}`);
    const state = await waitForStateAny(p.connection, ['disconnected', 'failed'], 5000).catch(
      () => p.connection.state
    );
    assertOk(
      state === 'disconnected' || state === 'failed',
      `state after rejection: ${state}`
    );
    return { rejected: true, error_code: code, state };
  } finally {
    p.disconnect();
  }
}

// A scenario may return {skip: reason} to request a skip verdict.
const isSkip = (o) => o !== null && typeof o === 'object' && typeof o.skip === 'string';

// --version: the SDK's own package version.
function sdkVersion() {
  return require('pusher-js/package.json').version;
}

// --scenario: run one scenario, print the verdict JSON as the final stdout
// line, exit 0 on pass and 1 on fail/skip (the JSON is authoritative).
async function scenarioMode() {
  const CURRENT = arg('--scenario');
  const T0 = Date.now();
  const out = (verdict, observations = {}, error = null) => {
    console.log(
      JSON.stringify({
        scenario: CURRENT,
        verdict,
        observations,
        error,
        duration_ms: Date.now() - T0,
      })
    );
    process.exit(verdict === 'pass' ? 0 : 1);
  };

  const fn = SCENARIOS[CURRENT];
  if (!fn) {
    return out('fail', {}, `unknown scenario ${CURRENT} — see --list`);
  }
  try {
    const observations = await fn();
    if (isSkip(observations)) {
      return out('skip', {}, observations.skip);
    }
    return out('pass', observations);
  } catch (e) {
    return out('fail', {}, (e && e.message) || String(e));
  }
}

// ---------------------------------------------------------------------------
// Entry point.
// ---------------------------------------------------------------------------

(async () => {
  if (has('--list')) {
    for (const id of Object.keys(SCENARIOS)) console.log(id);
    return;
  }
  if (has('--version')) {
    console.log(sdkVersion());
    return;
  }
  if (has('--scenario')) {
    await scenarioMode();
    return;
  }
  console.error('usage: runner.js --scenario <id> --env <env.json> | --version | --list');
  process.exit(2);
})().catch((e) => {
  console.error((e && e.stack) || String(e));
  process.exit(1);
});
