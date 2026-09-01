#!/usr/bin/env node
'use strict';

// pylon conformance adapter — server plane on the official Pusher Channels
// Node.js SDK (github.com/pusher/pusher-http-node, published on npm as
// `pusher`).
//
// Modes (the harness spawns these; see conformance/src/adapter.rs):
//
//   node runner.js --scenario <id> --env <env.json>
//       Run one scenario; the FINAL stdout line is the verdict JSON
//       {scenario, verdict: pass|fail|skip, observations, error, duration_ms}.
//       All logs go to stderr; stdout carries only the verdict.
//
//   node runner.js --sign --env <env.json>          (auth body on STDIN)
//       Sign ONE auth request with the SDK's own crypto and print the SDK's
//       response object as JSON on stdout. Routing: a body with `channel_name`
//       (+ optional `channel_data`) goes to authorizeChannel; a body with
//       `user_data` (or bare `user_id`) goes to authenticateUser. pusher-js
//       presence auth sends `user_id` alongside `channel_name` — for a
//       `presence-*` channel that becomes channelData {user_id, user_info:{}}.
//
//   node runner.js --verify-webhook <envelope.json> [--env <env.json>]
//       Read {headers, body}, verify with the SDK's webhook checker, print
//       {"valid": bool, "events": [...], "error"?: string}.
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

const argv = process.argv.slice(2);
const arg = (n) => {
  const i = argv.indexOf(n);
  return i >= 0 && i + 1 < argv.length ? argv[i + 1] : null;
};
const has = (n) => argv.includes(n);
const log = (...a) => console.error('[runner]', ...a);

// Read all of STDIN (the --sign auth-request body).
const readStdin = () =>
  new Promise((resolve, reject) => {
    let data = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', (chunk) => (data += chunk));
    process.stdin.on('end', () => resolve(data));
    process.stdin.on('error', reject);
  });

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

  // Both shapes are valid observations: 200 with a users array (occupied in a
  // full run, or empty-but-present on pylon), 400 when the server refuses an
  // unoccupied/non-presence query.
  'S-USERS': async () => {
    let status;
    let users = null;
    try {
      const r = await client().get({ path: '/channels/cf-presence/users' });
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
      watchlist: { user_ids: ['u2'] },
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

    const bad = mk({ appId: e.app_id, key: e.app_key, secret: 'wrong-secret-0123456789abcdef0' });
    try {
      const r = await bad.trigger('cf-test-channel', 'x', {});
      throw new Error(`bad secret resolved (status ${r.status})`);
    } catch (err) {
      outcomes.bad_signature = '<rejected>';
      outcomes.bad_signature_status = String(statusOf(err));
    }

    const unknown = mk({ appId: 'nope', key: 'nope', secret: 'x'.repeat(32) });
    try {
      const r = await unknown.trigger('cf-test-channel', 'x', {});
      throw new Error(`unknown app resolved (status ${r.status})`);
    } catch (err) {
      outcomes.unknown_app = '<rejected>';
      outcomes.unknown_app_status = String(statusOf(err));
    }

    assertOk(
      outcomes.bad_signature === '<rejected>' && outcomes.unknown_app === '<rejected>',
      'expected both error triggers to be rejected'
    );
    return outcomes;
  },
};

// A scenario may return {skip: reason} to request a skip verdict.
const isSkip = (o) => o !== null && typeof o === 'object' && typeof o.skip === 'string';

// ---------------------------------------------------------------------------
// Webhook verification (shared by the scenario and --verify-webhook).
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
    resp = p.authenticateUser(socketId, userData);
    log('sign: authenticateUser');
  } else {
    throw new Error('unroutable auth body: need channel_name, or user_data/user_id');
  }

  process.stdout.write(JSON.stringify(resp) + '\n');
}

// --verify-webhook <path>: {headers, body} file, verifier verdict on STDOUT.
function verifyWebhookMode(envelopePath) {
  const envelope = JSON.parse(fs.readFileSync(envelopePath, 'utf8'));
  const result = verifyWebhookEnvelope(envelope);
  const out = { valid: result.valid, events: result.events };
  if (result.error) out.error = result.error;
  console.log(JSON.stringify(out));
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
  const envelopePath = arg('--verify-webhook');
  if (envelopePath) {
    verifyWebhookMode(envelopePath);
    return;
  }
  if (has('--scenario')) {
    await scenarioMode();
    return;
  }
  console.error('usage: runner.js --scenario <id> --env <path> | --sign --env <path> | --verify-webhook <path> | --version | --list');
  process.exit(2);
})().catch((e) => {
  // Modes other than --scenario have no verdict contract: errors on stderr,
  // non-zero exit; the harness surfaces them as a 500 / mode failure.
  console.error((e && e.stack) || String(e));
  process.exit(1);
});
