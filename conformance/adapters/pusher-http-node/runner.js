#!/usr/bin/env node
'use strict';

// pylon conformance adapter — server plane on the official Pusher Channels
// Node.js SDK (github.com/pusher/pusher-http-node, published on npm as
// `pusher`).
//
// Modes (the harness spawns these; see conformance/src/adapter.rs). Dynamic
// values ride after a bare `--` option terminator, where this runner's argv
// parser never treats them as flags (see the parser block below):
//
//   node runner.js --scenario <id> --env -- <env.json>
//       Run one scenario; the FINAL stdout line is the verdict JSON
//       {scenario, verdict: pass|fail|skip, observations, error, duration_ms}.
//       All logs go to stderr; stdout carries only the verdict.
//
//   node runner.js --sign --env -- <env.json>          (auth body on STDIN)
//       Sign ONE auth request with the SDK's own crypto and print the SDK's
//       response object as JSON on stdout. Routing: a body with `channel_name`
//       (+ optional `channel_data`) goes to authorizeChannel; a body with
//       `user_data` (or bare `user_id`) goes to authenticateUser. pusher-js
//       presence auth sends `user_id` alongside `channel_name` — for a
//       `presence-*` channel that becomes channelData {user_id, user_info:{}}.
//
//       User-auth watchlist: pusher-js CANNOT carry the watchlist itself — its
//       signin posts only `socket_id` + `userAuthentication.params` (dist
//       src/core/auth/user_authenticator.ts composes exactly that query), and
//       the user_data on the wire comes verbatim from THIS response. So the
//       watchlist rides the params: a body param `watchlist` (JSON string, the
//       official Pusher docs shape — a BARE ARRAY of user ids, see
//       https://pusher.com/docs/channels/server_api/authenticating-users/) is
//       parsed and merged into the userData signed here.
//
//   node runner.js --fire-stdin --env -- <env.json>  (spec JSON on STDIN)
//       Publish one event server-side via the SDK: `client().trigger`. The
//       JSON spec arrives on STDIN — never argv, where a value token can be
//       misparsed as an option by this runner's own argv scanner — as
//       {channel, name, data, encrypted?} — `data` may be any JSON value
//       (strings pass verbatim, objects are JSON-serialized by the SDK).
//       `encrypted: true` is accepted as an assertion that the channel is
//       private-encrypted-*: the SDK encrypts automatically on those channels
//       (this client carries encryptionMasterKeyBase64), so the flag only
//       cross-checks the channel prefix. Exit 0 on a 2xx response, non-zero
//       with a stderr message otherwise. Used by the client-plane (pusher-js)
//       scenarios for their server-side publishes, keeping ALL server-side
//       protocol work on the official server SDK.
//
//   node runner.js --terminate --env -- <user-id> <env.json>
//       Terminate every connection of a signed-in user server-side via the
//       SDK's `terminateUserConnections` (POST
//       /apps/{app}/users/{user}/terminate_connections). The id rides AFTER
//       the `--` option terminator (never flag-parseable) and is additionally
//       shape-guarded (USER_ID_RE) BEFORE routing. Exit 0 on a 2xx response.
//       Used by the pusher-js U-TERMINATE scenario.
//
//   node runner.js --version    Print the SDK's package version.
//   node runner.js --list       Print implemented scenario ids, one per line.

const fs = require('fs');
const path = require('path');

// The SDK: `module.exports` IS the Pusher class (no named export).
const Pusher = require('pusher');

// Harness-fixed 32-byte encryption master key (base64). Must match the client
// adapters' key so private-encrypted channels round-trip (C-ENC-SUB).
const ENCRYPTION_KEY_BASE64 = 'MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=';

// argv contract — option terminator. A bare `--` token ends flag parsing:
// tokens BEFORE it are the flag region (flags and their inline values);
// tokens AFTER it are OPERANDS and are never treated as a flag or a flag's
// value. A value-taking flag whose value is DYNAMIC (a path, an external id)
// rides BARE in the flag region and draws its value from the operand queue —
// bare value-flags draw operands left-to-right in argv order — so an
// external value beginning with `-` can never be misparsed as an option by
// this (or any child) argv scan. Static, harness-fixed values may still ride
// inline before the terminator.
const argv = process.argv.slice(2);
const TERM_INDEX = argv.indexOf('--');
const flagArgs = TERM_INDEX >= 0 ? argv.slice(0, TERM_INDEX) : argv;
const operands = TERM_INDEX >= 0 ? argv.slice(TERM_INDEX + 1) : [];
// The value-taking flags; every other flag this runner knows is boolean.
const VALUE_FLAGS = ['--terminate', '--env', '--scenario'];

// A `-`-leading token is flag-shaped and is never consumed as an inline
// value (the flag then falls through to the operand queue).
const isFlagToken = (t) => typeof t === 'string' && t.startsWith('-');

const arg = (n) => {
  const i = flagArgs.indexOf(n);
  if (i < 0) return null;
  const next = flagArgs[i + 1];
  if (next !== undefined && !isFlagToken(next)) return next; // inline value
  if (!VALUE_FLAGS.includes(n)) return null; // boolean flag: no value
  // Bare value-flag: draw the operand owned by the k-th bare value-flag.
  let slot = 0;
  for (let k = 0; k < flagArgs.length; k++) {
    if (!VALUE_FLAGS.includes(flagArgs[k])) continue;
    const v = flagArgs[k + 1];
    if (v !== undefined && !isFlagToken(v)) continue; // inline-valued
    if (k === i) return slot < operands.length ? operands[slot] : null;
    slot++;
  }
  return null;
};
const has = (n) => flagArgs.includes(n);
const log = (...a) => console.error('[runner]', ...a);

// Read all of STDIN (the --sign auth-request body, the --fire-stdin spec).
const readStdin = () =>
  new Promise((resolve, reject) => {
    let data = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', (chunk) => (data += chunk));
    process.stdin.on('end', () => resolve(data));
    process.stdin.on('error', reject);
  });

// Shape guard for the --terminate user id. The id rides AFTER the `--`
// option terminator, so it can never occupy a flag position; this guard is
// defense-in-depth (alnum first char, then alnum/_/.//- up to 128 chars
// total) on top of that. The harness only ever passes fixed ids ('u-term',
// ...) — flag-injection guarding, not identity validation.
const USER_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;

// Parse an auth-request body: JSON per the harness contract, with a
// urlencoded fallback (what pusher-js's classic AJAX authorizer posts).
const parseAuthBody = (raw) => {
  try {
    return JSON.parse(raw);
  } catch (e) {
    const params = new URLSearchParams(raw);
    const body = {};
    for (const [k, v] of params) body[k] = v;
    return body;
  }
};

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

// One SDK client bound to pylon's HTTP plane (fetch Response objects out).
const client = () => {
  const e = loadEnv();
  return new Pusher({
    appId: e.app_id,
    key: e.app_key,
    secret: e.app_secret,
    host: e.http_host,
    port: e.http_port,
    useTLS: false,
    encryptionMasterKeyBase64: ENCRYPTION_KEY_BASE64,
  });
};

// The SDK rejects (pusher.RequestError, carries .status) on any status >= 400.
const statusOf = (e) => (e && typeof e.status === 'number' ? e.status : 0);

const assertOk = (cond, msg) => {
  if (!cond) throw new Error(msg);
};

// ---------------------------------------------------------------------------
// Scenarios (verdict: pass | fail | skip).
// ---------------------------------------------------------------------------

const SCENARIOS = {
  'S-TRIGGER': async () => {
    const p = client();
    const r = await p.trigger('cf-test-channel', 'test-event', { hello: 'world' });
    assertOk(r.status === 200, `trigger status ${r.status}`);
    const r2 = await p.trigger(['cf-test-channel', 'cf-test-channel-2'], 'multi', { n: 1 });
    assertOk(r2.status === 200, `multi status ${r2.status}`);
    return { single: '200', multi: '200' };
  },

  'S-BATCH': async () => {
    const events = Array.from({ length: 10 }, (_, i) => ({
      channel: 'cf-batch',
      name: 'e' + i,
      data: { i },
    }));
    const r = await client().triggerBatch(events);
    assertOk(r.status === 200, `batch status ${r.status}`);
    return { batches: '<10x10-ok>' };
  },

  // Both shapes are valid observations: `{"channels":{}}` in a server-only
  // scoped run (triggering does not occupy channels) vs occupied channels in
  // a full run where the client plane subscribed first (catalog order puts
  // every C-*/U-*/E-* before S-*).
  'S-CHANNELS': async () => {
    const p = client();
    const r = await p.get({ path: '/channels', params: { filter_by_prefix: 'cf-' } });
    assertOk(r.status === 200, `status ${r.status}`);
    const body = await r.json();
    const ids = Object.keys((body && body.channels) || {});
    // info-attribute leg: user_count is only legal filtered to presence-.
    const r2 = await p.get({
      path: '/channels',
      params: { filter_by_prefix: 'presence-cf-', info: 'user_count' },
    });
    assertOk(r2.status === 200, `attrs status ${r2.status}`);
    const body2 = await r2.json();
    const presenceIds = Object.keys((body2 && body2.channels) || {});
    return {
      status: '200',
      channel_count: ids.length,
      channel_keys: ids.map(() => '<name>'),
      presence_attrs_status: '200',
      presence_channel_count: presenceIds.length,
    };
  },

  'S-CHANNEL': async () => {
    const r = await client().get({ path: '/channels/cf-test-channel' });
    assertOk(r.status === 200, `status ${r.status}`);
    const body = await r.json();
    assertOk(body && body.occupied !== undefined, 'occupied field present');
    return { occupied: Boolean(body.occupied) };
  },

  // Query the presence channel C-PRES-SUB occupies (`presence-cf-pres`), so a
  // full run observes the 200/users shape. Both shapes stay valid: 200 with a
  // users array (occupied, or empty-but-present before Task 8 lands), 400 when
  // the server refuses the query (unoccupied).
  'S-USERS': async () => {
    let status;
    let users = null;
    try {
      const r = await client().get({ path: '/channels/presence-cf-pres/users' });
      status = r.status;
      const body = await r.json();
      users = Array.isArray(body && body.users) ? body.users.map(() => '<id>') : null;
    } catch (e) {
      status = statusOf(e); // RequestError carries the HTTP status
    }
    if (status !== 200 && status !== 400) {
      throw new Error(`users status ${status}`);
    }
    return { status: String(status), users: users === null ? '<opaque>' : users };
  },

  // Self-test of the signing mode: private + presence channelData + user auth.
  'S-AUTH': async () => {
    const p = client();
    const priv = p.authorizeChannel('123.456', 'private-cf-test');
    const pres = p.authorizeChannel('123.456', 'presence-cf-test', {
      user_id: 'u1',
      user_info: { name: 'n' },
    });
    const usr = p.authenticateUser('123.456', {
      id: 'u1',
      // Official docs shape: a bare array of user ids (NOT {user_ids: [...]}).
      watchlist: ['u2'],
    });
    for (const [name, resp] of [
      ['private', priv],
      ['presence', pres],
      ['user', usr],
    ]) {
      assertOk(
        typeof resp.auth === 'string' && resp.auth.includes(':'),
        `${name} auth token shape`
      );
    }
    assertOk(typeof usr.user_data === 'string', 'user auth user_data shape');
    return { private: '<key:sig>', presence: '<key:sig>', user: '<token>' };
  },

  // Verify the most recent webhook envelope captured by the harness receiver.
  // In a server-only scoped run nothing has fired a webhook yet: /last is 404
  // and the verdict is skip, not fail.
  'S-WEBHOOK-VERIFY': async () => {
    const e = loadEnv();
    const resp = await fetch(e.webhook_receiver + '/last');
    if (resp.status === 404) {
      return { skip: 'no webhook envelope recorded yet' };
    }
    assertOk(resp.status === 200, `webhook receiver status ${resp.status}`);
    const envelope = await resp.json();
    assertOk(envelope && typeof envelope.body === 'string', 'envelope shape');
    const result = verifyWebhookEnvelope(envelope);
    assertOk(result.valid, result.error || 'SDK webhook verification failed');
    return { verified: true, events: result.events };
  },

  // A bad secret and an unknown app must BOTH be rejected by the server (401)
  // and surface as SDK rejections — anything that resolves is a failure.
  'S-ERRORS': async () => {
    const e = loadEnv();
    const mk = (over) =>
      new Pusher(
        Object.assign(
          { host: e.http_host, port: e.http_port, useTLS: false },
          over
        )
      );
    const outcomes = {};

    // A resolve is a FAILURE (the auth-enforcement regression this scenario
    // guards), so the rejection assertion lives OUTSIDE each try/catch — a
    // deliberate throw inside the try would be swallowed by its own catch.
    const bad = mk({ appId: e.app_id, key: e.app_key, secret: 'wrong-secret-0123456789abcdef0' });
    let badAccepted = null;
    try {
      const r = await bad.trigger('cf-test-channel', 'x', {});
      badAccepted = r.status;
    } catch (err) {
      outcomes.bad_signature = '<rejected>';
      outcomes.bad_signature_status = String(statusOf(err));
    }
    if (badAccepted !== null) {
      throw new Error(`bad secret was accepted (status ${badAccepted})`);
    }

    const unknown = mk({ appId: 'nope', key: 'nope', secret: 'x'.repeat(32) });
    let unknownAccepted = null;
    try {
      const r = await unknown.trigger('cf-test-channel', 'x', {});
      unknownAccepted = r.status;
    } catch (err) {
      outcomes.unknown_app = '<rejected>';
      outcomes.unknown_app_status = String(statusOf(err));
    }
    if (unknownAccepted !== null) {
      throw new Error(`unknown app was accepted (status ${unknownAccepted})`);
    }

    return outcomes;
  },
};

// A scenario may return {skip: reason} to request a skip verdict.
const isSkip = (o) => o !== null && typeof o === 'object' && typeof o.skip === 'string';

// ---------------------------------------------------------------------------
// Webhook verification (used by the S-WEBHOOK-VERIFY scenario, which fetches
// the envelope from the harness receiver's /last endpoint directly).
// ---------------------------------------------------------------------------

// Build the SDK webhook object from a receiver envelope {headers, body}:
// header names lowercased (the SDK reads x-pusher-key / x-pusher-signature /
// content-type), content-type application/json added when absent (the SDK
// refuses to even parse the body without it), rawBody = the body string.
function verifyWebhookEnvelope(envelope) {
  const headers = {};
  for (const [k, v] of Object.entries(envelope.headers || {})) {
    headers[String(k).toLowerCase()] = v;
  }
  if (!headers['content-type']) headers['content-type'] = 'application/json';

  try {
    const wh = client().webhook({ headers, rawBody: envelope.body });
    const valid = wh.isValid();
    if (!valid) {
      return { valid: false, events: [], error: 'SDK webhook verification failed (key/signature/body mismatch)' };
    }
    return { valid: true, events: wh.getEvents().map((ev) => ev.name), error: null };
  } catch (e) {
    return { valid: false, events: [], error: (e && e.message) || String(e) };
  }
}

// ---------------------------------------------------------------------------
// Modes.
// ---------------------------------------------------------------------------

// --sign: one auth-request body on STDIN, SDK response object on STDOUT.
async function signMode() {
  const raw = await readStdin();
  const body = parseAuthBody(raw);
  const socketId = body.socket_id === undefined ? '' : String(body.socket_id);
  const p = client();
  let resp;

  if (typeof body.channel_name === 'string' && body.channel_name !== '') {
    let channelData = body.channel_data;
    // pusher-js presence auth sends user_id as a channelAuthorization param
    // instead of channel_data; the SDK wants it AS channelData.
    if (
      channelData === undefined &&
      body.channel_name.startsWith('presence-') &&
      body.user_id !== undefined
    ) {
      channelData = { user_id: String(body.user_id), user_info: {} };
    }
    if (typeof channelData === 'string') {
      try {
        channelData = JSON.parse(channelData);
      } catch (e) {
        throw new Error('channel_data is not valid JSON');
      }
    }
    resp =
      channelData === undefined
        ? p.authorizeChannel(socketId, body.channel_name)
        : p.authorizeChannel(socketId, body.channel_name, channelData);
    log('sign: authorizeChannel', body.channel_name);
  } else if (body.user_data !== undefined || body.user_id !== undefined) {
    const userData =
      body.user_data !== undefined ? body.user_data : { id: String(body.user_id) };
    // pusher-js carries the watchlist as a `watchlist` param (a JSON string);
    // merge it into the userData the SDK signs — the user_data that reaches
    // the server is THIS response's, verbatim.
    if (body.watchlist !== undefined) {
      let wl = body.watchlist;
      if (typeof wl === 'string') {
        try {
          wl = JSON.parse(wl);
        } catch (e) {
          throw new Error('watchlist param is not valid JSON');
        }
      }
      if (!Array.isArray(wl)) {
        throw new Error('watchlist param must be a JSON array of user ids');
      }
      userData.watchlist = wl;
    }
    resp = p.authenticateUser(socketId, userData);
    log('sign: authenticateUser', userData.id, 'watchlist:', (userData.watchlist || []).length, 'id(s)');
  } else {
    throw new Error('unroutable auth body: need channel_name, or user_data/user_id');
  }

  process.stdout.write(JSON.stringify(resp) + '\n');
}

// --fire-stdin: one server-side publish through the SDK's trigger, with the
// spec JSON read from STDIN (never argv — a value token in a flag position is
// the flag-injection shape). The client is configured with
// encryptionMasterKeyBase64, so triggering on a private-encrypted-* channel
// encrypts end-to-end automatically.
async function fireStdinMode() {
  const specRaw = await readStdin();
  let spec;
  try {
    spec = JSON.parse(specRaw);
  } catch (e) {
    throw new Error('--fire-stdin body is not valid JSON');
  }
  const { channel, name, data, encrypted } = spec || {};
  if (typeof channel !== 'string' || typeof name !== 'string') {
    throw new Error('--fire-stdin needs {channel: string, name: string, data}');
  }
  if (encrypted === true && !channel.startsWith('private-encrypted-')) {
    throw new Error(`--fire-stdin encrypted:true but channel ${channel} is not private-encrypted-*`);
  }
  const r = await client().trigger(channel, name, data);
  if (r.status < 200 || r.status >= 300) {
    throw new Error(`trigger ${channel}/${name} -> status ${r.status}`);
  }
  log(`fire: ${channel}/${name} -> ${r.status}`);
}

// --terminate <user-id>: terminate the user's connections through the SDK's
// own terminateUserConnections (drives pylon's
// POST /apps/{app}/users/{user}/terminate_connections). The id arrives as an
// operand after the `--` terminator (never flag-parseable) and is
// additionally shape-guarded (USER_ID_RE) here — no leading dash, bounded
// length.
async function terminateMode(userId) {
  if (!userId || !USER_ID_RE.test(userId)) {
    throw new Error('--terminate needs a user id matching ' + USER_ID_RE);
  }
  const r = await client().terminateUserConnections(userId);
  if (r.status < 200 || r.status >= 300) {
    throw new Error(`terminateUserConnections(${userId}) -> status ${r.status}`);
  }
  log(`terminate: ${userId} -> ${r.status}`);
}

// --version: the SDK's own package version.
function sdkVersion() {
  try {
    return require('pusher/package.json').version;
  } catch (e) {
    const pkg = JSON.parse(
      fs.readFileSync(path.join(__dirname, 'node_modules', 'pusher', 'package.json'), 'utf8')
    );
    return pkg.version;
  }
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
  if (has('--sign')) {
    await signMode();
    return;
  }
  if (has('--fire-stdin')) {
    await fireStdinMode();
    return;
  }
  const terminateId = arg('--terminate');
  if (terminateId !== null) {
    // Shape-guard BEFORE routing: the id already rode past the `--`
    // terminator, and this second gate keeps a malformed id from reaching
    // the SDK call even if a caller ever regresses to inline passing.
    if (!USER_ID_RE.test(terminateId)) {
      console.error(
        `--terminate: malformed user id (want ${USER_ID_RE}): ${terminateId.slice(0, 64)}`
      );
      process.exit(2);
    }
    await terminateMode(terminateId);
    return;
  }
  if (has('--scenario')) {
    await scenarioMode();
    return;
  }
  console.error('usage: runner.js --scenario <id> --env -- <path> | --sign --env -- <path> | --fire-stdin --env -- <path> (spec on stdin) | --terminate --env -- <user-id> <path> | --version | --list');
  process.exit(2);
})().catch((e) => {
  // Modes other than --scenario have no verdict contract: errors on stderr,
  // non-zero exit; the harness surfaces them as a 500 / mode failure.
  console.error((e && e.stack) || String(e));
  process.exit(1);
});
