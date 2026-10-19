const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');
const { PGlite } = require('@electric-sql/pglite');
const { ingest } = require('../server/ingest.cjs');
const { usage } = require('../server/usage.cjs');
const { instant, parseBucket, parseSession } = require('../server/validation.cjs');
const { createHandler } = require('../server/service.cjs');

const device_id = 'review-device';
const hour_start = '2026-02-28T12:00:00Z';
const row = (source, tokens, time = hour_start) => ({ source, model: 'model', hour_start: time, total_tokens: tokens, input_tokens: tokens });
const request = (body) => ({ headers: {}, body });

async function database() {
  const db = new PGlite();
  await db.exec(await fs.readFile(path.join(__dirname, '../server/schema.sql'), 'utf8'));
  return { db, pool: { query: (...args) => db.query(...args), connect: async () => ({ query: (...args) => db.query(...args), release() {} }) } };
}

test('legacy uploads replace legacy rows but cannot replace stamped snapshots', async () => {
  const { db, pool } = await database();
  const send = (tokens, stamp) => ingest(pool, request({ device_id, hourly: [row('claude', tokens)], ...stamp }));
  try {
    await send(10);
    await send(20);
    let stored = await db.query('SELECT total_tokens, snapshot_at, snapshot_revision FROM usage_buckets');
    assert.equal(Number(stored.rows[0].total_tokens), 20);
    await send(30, { snapshot_at: '2026-02-28T12:30:00Z', snapshot_revision: 1 });
    await send(99);
    stored = await db.query('SELECT total_tokens, snapshot_at, snapshot_revision FROM usage_buckets');
    assert.equal(Number(stored.rows[0].total_tokens), 30);
    assert.equal(stored.rows[0].snapshot_at.toISOString(), '2026-02-28T12:30:00.000Z');
    assert.equal(Number(stored.rows[0].snapshot_revision), 1);
    await send(1, { snapshot_at: '2026-02-28T12:20:00Z', snapshot_revision: 9 });
    assert.equal(Number((await db.query('SELECT total_tokens FROM usage_buckets')).rows[0].total_tokens), 30);
    await send(40, { snapshot_at: '2026-02-28T12:30:00Z', snapshot_revision: 2 });
    assert.equal(Number((await db.query('SELECT total_tokens FROM usage_buckets')).rows[0].total_tokens), 40);
    // A pre-existing row with the schema's -infinity default is also legacy.
    await db.query("INSERT INTO usage_buckets (device_id, source, model, hour_start, total_tokens) VALUES ($1, 'codex', 'model', $2, 5)", [device_id, hour_start]);
    await ingest(pool, request({ device_id, hourly: [row('codex', 6)] }));
    assert.equal(Number((await db.query("SELECT total_tokens FROM usage_buckets WHERE source = 'codex'")).rows[0].total_tokens), 6);
  } finally { await db.close(); }
});

test('ingest limits actual UTF-8 bytes of pre-parsed and streamed bodies before database access', async () => {
  const noDb = { connect: () => { throw new Error('database must not be accessed'); } };
  const oversized = { device_id, hourly: [], padding: '汉'.repeat(350000) };
  await assert.rejects(ingest(noDb, request(JSON.stringify(oversized))), /Payload too large/);
  await assert.rejects(ingest(noDb, request(oversized)), /Payload too large/);
  await assert.rejects(ingest(noDb, request(Buffer.from(JSON.stringify(oversized)))), /Payload too large/);
  let chunksRead = 0;
  const streamed = {
    headers: {},
    async *[Symbol.asyncIterator]() {
      yield Buffer.from('{"device_id":"review-device","hourly":[],"padding":"');
      yield Buffer.from('汉'.repeat(350000));
      chunksRead++;
      yield Buffer.from('should never be read');
    },
  };
  await assert.rejects(ingest(noDb, streamed), /Payload too large/);
  assert.equal(chunksRead, 0);
  // Byte count is exact at the boundary, including UTF-8 split across chunks.
  const { db, pool } = await database();
  try {
    const prefix = '{"device_id":"review-device","hourly":[],"padding":"';
    const suffix = '"}';
    const utf8Count = Math.floor((1048576 - Buffer.byteLength(prefix) - Buffer.byteLength(suffix)) / 3);
    const payload = Buffer.concat([Buffer.from(prefix), Buffer.from('汉'.repeat(utf8Count)),
      Buffer.from('a'.repeat(1048576 - Buffer.byteLength(prefix) - Buffer.byteLength(suffix) - utf8Count * 3)), Buffer.from(suffix)]);
    assert.equal(payload.length, 1048576);
    assert.deepEqual(await ingest(pool, { headers: {}, async *[Symbol.asyncIterator]() {
      yield payload.subarray(0, 52);
      yield payload.subarray(52, 53);
      yield payload.subarray(53);
    } }), { inserted: 0, skipped: 0 });
  } finally { await db.close(); }
});

test('instant rejects impossible calendar dates across all timestamp fields', () => {
  assert.throws(() => instant('2026-02-30T10:00:00Z', 'snapshot_at'), /Invalid snapshot_at/);
  assert.throws(() => parseBucket(row('claude', 1, '2026-02-30T10:00:00Z')), /Invalid hour_start/);
  assert.throws(() => parseSession({ source: 'trae-cn', session_id: 's', model: 'model', bucket_start: hour_start, snapshot_verified_at: '2025-02-29T00:00:00Z' }), /Invalid snapshot_verified_at/);
  assert.equal(instant('2024-02-29T23:59:59Z', 'hour_start'), '2024-02-29T23:59:59.000Z');
});

test('usage caps combined raw rows and sends a clear 400 without partial totals', async () => {
  process.env.ACCESS_KEY = 'review-test-key';
  let sql;
  const fake = { query: async (query, params) => {
    sql = query;
    assert.equal(params[2], 50001);
    return { rows: Array(params[2]).fill(row('cursor', 1)) };
  } };
  const res = { setHeader() {}, end(text) { this.body = JSON.parse(text); } };
  await createHandler(fake)({ url: '/api/usage?from=2026-02-28&to=2026-02-28', method: 'GET', headers: { authorization: 'Bearer review-test-key' } }, res);
  assert.equal(res.statusCode, 400);
  assert.match(res.body.error, /more than 50000 rows; choose a shorter date range/);
  assert.equal('summary' in res.body, false);
  assert.match(sql, /UNION ALL[\s\S]*LIMIT \$3/);
});

test('usage under the row budget still prices rows and de-duplicates cursor by max tokens', async () => {
  const { db, pool } = await database();
  try {
    await ingest(pool, request({ device_id, hourly: [row('cursor', 10), row('claude', 20)] }));
    await ingest(pool, request({ device_id: 'other-device', hourly: [row('cursor', 30)] }));
    await ingest(pool, request({ device_id, hourly: [], account_session_states: [{ source: 'trae-cn', session_id: 'session', model: 'model', bucket_start: hour_start, snapshot_verified_at: hour_start, total_tokens: 7 }] }));
    const result = await usage(pool, new URL('/api/usage?from=2026-02-28&to=2026-02-28', 'http://localhost'));
    assert.equal(result.summary.total_tokens, 57);
    assert.equal(result.daily[0].total_tokens, 57);
    assert.ok(Number.isFinite(result.summary.cost_usd));
    assert.equal(result.models.length, 3);
  } finally { await db.close(); }
});
