const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');
const { PGlite } = require('@electric-sql/pglite');
const { ingest } = require('../server/ingest.cjs');
const { usage, summarize } = require('../server/usage.cjs');
const { createHandler } = require('../server/service.cjs');
const { SCHEMA_VERSION, migrate, schemaStatements } = require('../server/migrate.cjs');
const { computeRowCost } = require('../server/pricing');

const SCHEMA = path.join(__dirname, '../server/schema.sql');
const bucket = (overrides = {}) => ({ source: 'claude', model: 'claude-sonnet-4', hour_start: '2026-10-18T12:00:00Z',
  input_tokens: 100, output_tokens: 10, total_tokens: 110, conversation_count: 1, ...overrides });
const body = (value) => ({ headers: {}, body: value });

async function database({ schema = true } = {}) {
  const db = new PGlite();
  if (schema) await db.exec(await fs.readFile(SCHEMA, 'utf8'));
  const statements = [];
  const query = (sql, params) => { statements.push(sql); return db.query(sql, params); };
  return { db, statements, pool: { query, connect: async () => ({ query, release() {} }) } };
}

test('ingest writes a 200-row request with one INSERT and reports written versus skipped rows', async () => {
  const { db, pool, statements } = await database();
  try {
    const hourly = Array.from({ length: 200 }, (_, i) => bucket({ model: `m-${i}` }));
    const stamp = { snapshot_at: '2026-10-18T13:00:00Z', snapshot_revision: 5 };
    assert.deepEqual(await ingest(pool, body({ device_id: 'machine_00001', hourly, ...stamp })), { inserted: 200, skipped: 0, max_records: 200 });
    assert.equal(statements.filter((sql) => /^\s*INSERT/.test(sql)).length, 1);
    // Same stamp again: every row is rejected by the snapshot guard.
    assert.deepEqual(await ingest(pool, body({ device_id: 'machine_00001', hourly, ...stamp })), { inserted: 0, skipped: 200, max_records: 200 });
    assert.equal((await db.query('SELECT count(*)::int AS n FROM usage_buckets')).rows[0].n, 200);
  } finally { await db.close(); }
});

test('in-request duplicates resolve like sequential upserts', async () => {
  const { db, pool } = await database();
  const total = async (source) => Number((await db.query('SELECT total_tokens FROM usage_buckets WHERE source = $1', [source])).rows[0].total_tokens);
  try {
    // Stamped: an equal stamp never replaces, so the first row stays.
    await ingest(pool, body({ device_id: 'machine_00001', snapshot_at: '2026-10-18T13:00:00Z', snapshot_revision: 1,
      hourly: [bucket({ total_tokens: 1 }), bucket({ total_tokens: 2 })] }));
    assert.equal(await total('claude'), 1);
    // Legacy: last writer wins.
    const result = await ingest(pool, body({ device_id: 'machine_00001',
      hourly: [bucket({ source: 'codex', total_tokens: 1 }), bucket({ source: 'codex', total_tokens: 2 })] }));
    assert.equal(await total('codex'), 2);
    assert.deepEqual(result, { inserted: 1, skipped: 1, max_records: 200 });
    // Session states keep the newest verification stamp regardless of order.
    const state = (tokens, verified) => ({ source: 'trae-cn', session_id: 's', model: 'm', bucket_start: '2026-10-18T12:00:00Z',
      snapshot_verified_at: verified, input_tokens: tokens, total_tokens: tokens });
    await ingest(pool, body({ device_id: 'machine_00001', hourly: [],
      account_session_states: [state(9, '2026-10-18T14:00:00Z'), state(5, '2026-10-18T13:00:00Z')] }));
    assert.equal(Number((await db.query('SELECT total_tokens FROM account_session_states')).rows[0].total_tokens), 9);
  } finally { await db.close(); }
});

test('usage groups days in the requested time zone and rejects unknown zones', async () => {
  const { db, pool } = await database();
  try {
    // 20:00Z on the 18th is 04:00 on the 19th in Shanghai.
    await ingest(pool, body({ device_id: 'machine_00001', hourly: [bucket({ hour_start: '2026-10-18T20:00:00Z' })] }));
    const utc = await usage(pool, new URL('http://x/api/usage?from=2026-10-18&to=2026-10-18'));
    assert.equal(utc.tz, 'UTC');
    assert.equal(utc.daily[0].day, '2026-10-18');
    const shanghaiOld = await usage(pool, new URL('http://x/api/usage?from=2026-10-18&to=2026-10-18&tz=Asia/Shanghai'));
    assert.equal(shanghaiOld.summary.total_tokens, 0);
    const shanghai = await usage(pool, new URL('http://x/api/usage?from=2026-10-19&to=2026-10-19&tz=Asia/Shanghai'));
    assert.equal(shanghai.daily[0].day, '2026-10-19');
    assert.equal(shanghai.summary.total_tokens, 110);
    await assert.rejects(usage(pool, new URL('http://x/api/usage?tz=Mars/Base')), /Invalid time zone/);
    await assert.rejects(usage(pool, new URL("http://x/api/usage?tz=UTC';--")), /Invalid time zone/);
  } finally { await db.close(); }
});

test('SQL aggregation prices exactly like summing every raw device bucket', async () => {
  const { db, pool } = await database();
  try {
    const raw = [];
    const send = async (device, rows) => {
      await ingest(pool, body({ device_id: device, hourly: rows }));
      raw.push(...rows.map((row) => ({ ...row, device })));
    };
    // DeepSeek V4 is time-of-use priced: 02:00Z is peak, 12:00Z off-peak (a Wednesday).
    const deepseek = (hour) => bucket({ source: 'opencode', model: 'deepseek-v4-flash', hour_start: `2026-10-14T${hour}:00:00Z`,
      input_tokens: 1_000_000, output_tokens: 1_000_000, total_tokens: 2_000_000 });
    await send('machine_00001', [deepseek('02'), deepseek('12'),
      bucket({ hour_start: '2026-10-14T03:00:00Z' }), bucket({ hour_start: '2026-10-14T04:00:00Z' }),
      bucket({ source: 'grok', model: 'grok-4', total_cost_usd: 4.25, hour_start: '2026-10-14T05:00:00Z' }),
      bucket({ source: 'codex', model: 'gpt-6-astra', hour_start: '2026-10-14T06:00:00Z', input_tokens: 1_000_000,
        output_tokens: 1_000_000, total_tokens: 2_000_000, priority_input_tokens: 1_000_000, priority_output_tokens: 1_000_000 })]);
    await send('machine_00002', [deepseek('02'), bucket({ hour_start: '2026-10-14T03:00:00Z' }),
      bucket({ source: 'grok', model: 'grok-4', hour_start: '2026-10-14T05:00:00Z' })]);
    const result = await usage(pool, new URL('http://x/api/usage?from=2026-10-14&to=2026-10-14'));
    const expected = summarize(raw);
    assert.equal(result.summary.total_tokens, expected.summary.total_tokens);
    assert.ok(Math.abs(result.summary.cost_usd - expected.summary.cost_usd) < 1e-9);
    // Priority subsets survive the upload and add the Fast-tier premium.
    const astra = result.models.find((row) => row.model === 'gpt-6-astra');
    const standard = computeRowCost({ source: 'codex', model: 'gpt-6-astra', input_tokens: 1_000_000, output_tokens: 1_000_000 });
    assert.ok(Math.abs(astra.cost_usd - 2 * standard) < 1e-9);
    // Aggregation collapses devices and hours: claude ×3 raw buckets → 1 row,
    // deepseek keeps its two distinct priced hours.
    const rows = (await db.query('SELECT count(*)::int AS n FROM usage_buckets')).rows[0].n;
    assert.equal(rows, 9);
  } finally { await db.close(); }
});

test('the API migrates an empty or older database once and schema.sql records the same version', async () => {
  assert.match(await fs.readFile(SCHEMA, 'utf8'), new RegExp(`VALUES \\(1, ${SCHEMA_VERSION}\\)`));
  const { db, pool } = await database({ schema: false });
  try {
    // A version-1 database: original tables, no priority columns, no version table.
    for (const statement of schemaStatements().filter((sql) => !/priority_|usagecairn_schema/.test(sql))) await db.query(statement);
    await db.query("INSERT INTO usage_buckets (device_id, source, model, hour_start, total_tokens) VALUES ('machine_00001', 'claude', 'm', '2026-10-18T00:00:00Z', 7)");
    process.env.ACCESS_KEY = 'migrate-test-key';
    const handler = createHandler(pool, { migrate: true });
    const call = (url, method = 'GET', payload) => {
      const res = { setHeader() {}, end(text) { this.body = JSON.parse(text); } };
      return handler({ url, method, body: payload, headers: { authorization: 'Bearer migrate-test-key' } }, res).then(() => res);
    };
    const ingested = await call('/api/ingest', 'POST', { device_id: 'machine_00001', hourly: [bucket({ priority_input_tokens: 5 })] });
    assert.equal(ingested.statusCode, 200);
    assert.equal((await db.query('SELECT version FROM usagecairn_schema')).rows[0].version, SCHEMA_VERSION);
    assert.equal(Number((await db.query("SELECT total_tokens FROM usage_buckets WHERE model = 'm'")).rows[0].total_tokens), 7);
    assert.deepEqual(await migrate(pool), { migrated: false });
    assert.equal((await call('/api/usage?from=2026-10-18&to=2026-10-18')).body.summary.total_tokens, 117);
  } finally { await db.close(); }
});
