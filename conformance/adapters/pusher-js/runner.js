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
// pusher-http-node runner's `--fire` mode, so ALL server-plane protocol work
// rides on the official server SDK.
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
// SDK validation but never consulted for the host once wsHost is set.
const connect = (opts = {}) => {
  const e = loadEnv();
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
  return new Pusher(e.app_key, {
    wsHost: e.http_host,
    wsPort: e.http_port,
    httpHost: e.http_host,
    httpPort: e.http_port,
    forceTLS: false,
    cluster: 'local',
    ...auth,
    ...opts,
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
// Server-side publishing: shell out to the sibling http adapter's --fire.
// ---------------------------------------------------------------------------

const HTTP_ADAPTER_DIR = path.join(__dirname, '..', 'pusher-http-node');

const fire = (spec) =>
  new Promise((resolve, reject) => {
    execFile(
      process.execPath,
      ['runner.js', '--fire', JSON.stringify(spec), '--env', arg('--env')],
      { cwd: HTTP_ADAPTER_DIR },
      (err, stdout, stderr) => {
        if (err) {
          reject(new Error('fire failed: ' + (String(stderr).trim() || err.message)));
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
    const state = p.connection.state;
    p.disconnect();
    return { socket_id: socketId, state };
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
    await waitConnected(p);
    let pongs = 0;
    p.connection.bind('message', (m) => {
      if (m && m.event === 'pusher:pong') pongs++;
    });
    await sleep(8000);
    assertOk(p.connection.state === 'connected',
      `connection not alive after 8s: ${p.connection.state}`);
    assertOk(pongs >= 1, `no pusher:pong observed in 8s (${pongs})`);
    p.disconnect();
    return { alive_after_8s: true, activity_timeout_used_ms: 2000, pongs_observed: pongs };
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
};

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
  console.error('usage: runner.js --scenario <id> --env <path> | --version | --list');
  process.exit(2);
})().catch((e) => {
  console.error((e && e.stack) || String(e));
  process.exit(1);
});
