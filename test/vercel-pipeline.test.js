const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { PGlite } = require('@electric-sql/pglite');
const { createHandler } = require('../server/service.cjs');
const { uploadQueue } = require('../src/commands/upload');
const { readQueueBatch } = require('../src/lib/queue-batch');

const tokenA = `tt_${'a'.repeat(64)}`;
const tokenB = `tt_${'b'.repeat(64)}`;
process.env.ACCESS_KEY = tokenA;
function response() {
  return { headers: {}, setHeader(k, v) { this.headers[k] = v; }, end(text) { this.body = JSON.parse(text); } };
}
function request(handler, url, method = 'GET', key = tokenA, body) {
  const res = response();
  return handler({ url, method, headers: { authorization: `Bearer ${key}` }, body }, res).then(() => res);
}

test('local ledger → retried upload → PostgreSQL → authenticated dashboard usage', async () => {
  const db = new PGlite();
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'tt-pipeline-'));
  const queuePath = path.join(directory, 'queue.jsonl');
  try {
    await db.exec(await fs.readFile(path.join(__dirname, '../server/schema.sql'), 'utf8'));
    const handler = createHandler({ query: (...args) => db.query(...args), connect: async () => ({ query: (...args) => db.query(...args), release() {} }) });
    const snapshot = (tokens) => ({ hour_start: '2026-10-18T12:00:00Z', source: 'claude', model: 'claude-sonnet-4', input_tokens: tokens, output_tokens: 20, total_tokens: tokens + 20, conversation_count: 1 });
    await fs.writeFile(queuePath, `${JSON.stringify(snapshot(100))}\n${JSON.stringify({ ...snapshot(140), prompt: 'NEVER_UPLOAD_PRIVATE_TEXT' })}\n`);
    const parsed = await readQueueBatch(queuePath, 0, 200);
    assert.equal(parsed.buckets.length, 1);
    assert.equal(parsed.buckets[0].total_tokens, 160);
    let fail = true;
    const send = async (url, options) => {
      if (fail) { fail = false; return { ok: false, status: 503 }; }
      assert.equal(options.body.includes('NEVER_UPLOAD_PRIVATE_TEXT'), false);
      const res = await request(handler, new URL(url).pathname, 'POST', tokenA, JSON.parse(options.body));
      return { ok: res.statusCode === 200, status: res.statusCode };
    };
    const config = { queuePath, baseUrl: 'http://localhost:3000', key: tokenA, request: send };
    await assert.rejects(uploadQueue(config), /503/);
    assert.equal((await fs.readFile(queuePath, 'utf8')).split('\n').length, 3);
    await uploadQueue(config);
    await uploadQueue(config);
    const a = await request(handler, '/api/usage?from=2026-10-18&to=2026-10-18');
    assert.equal(a.statusCode, 200);
    assert.equal(a.body.summary.total_tokens, 160);
    assert.equal(a.body.daily[0].total_tokens, 160);
    assert.equal(a.body.models[0].model, 'claude-sonnet-4');
    assert.equal((await request(handler, '/api/usage?from=2026-10-18&to=2026-10-18', 'GET', tokenB)).statusCode, 401);
    assert.equal((await request(handler, '/api/usage', 'GET', 'wrong')).statusCode, 401);
    assert.equal((await request(handler, '/api/ingest', 'POST', tokenB, { device_id: 'machine_00001', hourly: [{ ...snapshot(99), source: '<script>' }] })).statusCode, 401);
    const rows = await db.query('SELECT count(*)::int AS count FROM usage_buckets');
    assert.equal(rows.rows[0].count, 1);
  } finally { await db.close(); await fs.rm(directory, { recursive: true, force: true }); }
});

test('a missing or empty server key fails closed with 503', async () => {
  const db = new PGlite();
  try {
    await db.exec(await fs.readFile(path.join(__dirname, '../server/schema.sql'), 'utf8'));
    const handler = createHandler({ query: (...args) => db.query(...args), connect: async () => ({ query: (...args) => db.query(...args), release() {} }) });
    const previous = process.env.ACCESS_KEY;
    try {
      delete process.env.ACCESS_KEY;
      assert.equal((await request(handler, '/api/usage')).statusCode, 503);
      process.env.ACCESS_KEY = '  ';
      assert.equal((await request(handler, '/api/usage')).statusCode, 503);
    } finally { process.env.ACCESS_KEY = previous; }
  } finally { await db.close(); }
});

test('ingest accepts streamed JSON and preserves validation and public exports', async () => {
  const { parseBucket, summarize } = require('../server/service.cjs');
  assert.equal(parseBucket({ source: 'CLAUDE', model: 'test', hour_start: '2026-10-18T12:00:00Z' })[0], 'claude');
  assert.equal(summarize([]).summary.total_tokens, 0);

  const queries = [];
  const handler = createHandler({ connect: async () => ({
    async query(sql) { queries.push(sql); }, release() { queries.push('RELEASE'); },
  }) });
  const payload = { device_id: 'machine_00001', hourly: [{
    source: 'claude', model: 'test', hour_start: '2026-10-18T12:00:00Z', total_tokens: 12,
  }] };
  const req = { url: '/api/ingest', method: 'POST', headers: { authorization: `Bearer ${tokenA}` },
    async *[Symbol.asyncIterator]() { yield JSON.stringify(payload); } };
  const res = response();
  await handler(req, res);
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.body, { inserted: 1, skipped: 0 });
  assert.equal(queries[0], 'BEGIN');
  assert.equal(queries[2], 'COMMIT');
  assert.equal(queries[3], 'RELEASE');

  const invalid = await request(handler, '/api/ingest', 'POST', tokenA, { ...payload, hourly: [{ ...payload.hourly[0], total_tokens: -1 }] });
  assert.equal(invalid.statusCode, 400);
  assert.deepEqual(invalid.body, { error: 'Invalid total_tokens' });
  assert.equal(queries.length, 4);
  assert.equal((await request(handler, '/api/other')).statusCode, 404);
});
