const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const { PGlite } = require('@electric-sql/pglite');
const { createHandler } = require('../server/service.cjs');

const key = `tt_${'a'.repeat(64)}`;
process.env.ACCESS_KEY = key;
const bucket = (source, total, hour = '2026-10-18T23:30:00Z') => ({ source, model: 'gpt-5', hour_start: hour,
  input_tokens: total - 20, output_tokens: 20, total_tokens: total, conversation_count: 1 });
function call(handler, url, method = 'GET', body) {
  const res = { setHeader() {}, end(value) { this.value = JSON.parse(value); } };
  return handler({ url, method, body, headers: { authorization: `Bearer ${key}` } }, res).then(() => res);
}

test('Cursor account dedup, TRAE session correction, authoritative cost, stale upload and UTC range', async () => {
  const db = new PGlite();
  try {
    await db.exec(await fs.readFile('server/schema.sql', 'utf8'));
    const handler = createHandler({ query: (...args) => db.query(...args), connect: async () => ({ query: (...args) => db.query(...args), release() {} }) });
    const send = (device, hourly, states = [], revision = 1, at = '2026-10-18T12:00:00Z') => call(handler, '/api/ingest', 'POST',
      { device_id: device, hourly, account_session_states: states, snapshot_revision: revision, snapshot_at: at });
    const newer = bucket('cursor', 160);
    assert.equal((await send('machine_00001', [newer])).statusCode, 200);
    assert.equal((await send('machine_00002', [newer])).statusCode, 200);
    assert.equal((await send('machine_00001', [bucket('cursor', 120)], [], 0)).statusCode, 200);
    const grok = { ...bucket('grok', 100), total_cost_usd: 4.25 };
    await send('machine_00001', [grok]);
    const state = (tokens, bucketStart, verifiedAt) => ({ source: 'trae-cn', session_id: 'session-A', model: 'gpt-5', bucket_start: bucketStart,
      snapshot_verified_at: verifiedAt, input_tokens: tokens, output_tokens: 0, total_tokens: tokens });
    await send('machine_00001', [], [state(90, '2026-10-18T23:30:00Z', '2026-10-19T00:00:00Z')]);
    await send('machine_00002', [], [state(60, '2026-10-19T00:30:00Z', '2026-10-19T01:00:00Z')]);
    await send('machine_00001', [], [state(90, '2026-10-18T23:30:00Z', '2026-10-19T00:00:00Z')]);
    await db.exec("SET TIME ZONE 'Pacific/Honolulu'");
    const first = await call(handler, '/api/usage?from=2026-10-18&to=2026-10-18');
    assert.equal(first.statusCode, 200);
    assert.equal(first.value.summary.total_tokens, 260); // cursor 160 + grok 100, no old TRAE
    assert.ok(first.value.summary.cost_usd >= 4.25);
    const second = await call(handler, '/api/usage?from=2026-10-19&to=2026-10-19');
    assert.equal(second.value.summary.total_tokens, 60);
    assert.equal(second.value.daily[0].day, '2026-10-19');
    assert.equal((await call(handler, '/api/usage?from=2026-02-30&to=2026-03-01')).statusCode, 400);
  } finally { await db.close(); }
});

test('a client that omits snapshot stamps keeps last-writer-wins updates', async () => {
  const db = new PGlite();
  try {
    await db.exec(await fs.readFile('server/schema.sql', 'utf8'));
    const handler = createHandler({ query: (...a) => db.query(...a), connect: async () => ({ query: (...a) => db.query(...a), release() {} }) });
    const post = (total) => call(handler, '/api/ingest', 'POST', { device_id: 'machine_00001', hourly: [bucket('claude', total)] });
    await post(100);
    await post(250);
    const res = await call(handler, '/api/usage?from=2026-10-18&to=2026-10-18');
    assert.equal(res.value.summary.total_tokens, 250);
  } finally { await db.close(); }
});
