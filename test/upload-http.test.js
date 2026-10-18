const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { PGlite } = require('@electric-sql/pglite');
const { createHandler } = require('../server/service.cjs');
const { uploadQueue } = require('../src/commands/upload');
const { writeUploadConfig } = require('../src/lib/upload-config');

const key = `tt_${'d'.repeat(64)}`;
process.env.ACCESS_KEY = key;

async function startServer() {
  const db = new PGlite();
  await db.exec(await fs.readFile(path.join(__dirname, '../server/schema.sql'), 'utf8'));
  const handler = createHandler({ query: (...a) => db.query(...a), connect: async () => ({ query: (...a) => db.query(...a), release() {} }) });
  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => handler({ url: req.url, method: req.method, headers: req.headers, body: Buffer.concat(chunks).toString('utf8') || undefined }, res));
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return { db, server, port: server.address().port };
}

test('upload over real HTTP reaches the database once per snapshot and never leaks prompt text', async () => {
  const { db, server, port } = await startServer();
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'tt-http-'));
  const queuePath = path.join(dir, 'queue.jsonl');
  try {
    writeUploadConfig(queuePath, { apiUrl: `http://127.0.0.1:${port}`, accessKey: key });
    const row = (hour, total) => ({ hour_start: hour, source: 'claude', model: 'claude-sonnet-4', input_tokens: total - 20, output_tokens: 20, total_tokens: total, conversation_count: 1 });
    await fs.writeFile(queuePath, `${JSON.stringify({ ...row('2026-10-18T12:00:00Z', 1250), prompt: 'SECRET_PROMPT_TEXT' })}\n${JSON.stringify(row('2026-10-18T13:00:00Z', 600))}\n`);
    assert.deepEqual(await uploadQueue({ queuePath, env: {} }), { uploaded: 2, pending: 0 });
    assert.deepEqual(await uploadQueue({ queuePath, env: {} }), { uploaded: 2, pending: 0 });
    const usage = await new Promise((resolve, reject) => {
      http.get({ host: '127.0.0.1', port, path: '/api/usage?from=2026-10-18&to=2026-10-18', headers: { Authorization: `Bearer ${key}` } },
        (res) => { let body = ''; res.on('data', (c) => { body += c; }); res.on('end', () => resolve({ status: res.statusCode, body: JSON.parse(body) })); }).on('error', reject);
    });
    assert.equal(usage.status, 200);
    assert.equal(usage.body.summary.total_tokens, 1850);
    assert.equal(usage.body.daily.length, 1);
    const stored = await db.query('SELECT total_tokens, conversation_count FROM usage_buckets ORDER BY hour_start');
    assert.deepEqual(stored.rows.map((r) => Number(r.total_tokens)), [1250, 600]);
    const columns = await db.query("SELECT string_agg(column_name, ',') AS cols FROM information_schema.columns WHERE table_name = 'usage_buckets'");
    assert.equal(columns.rows[0].cols.includes('prompt'), false);
  } finally {
    await new Promise((resolve) => server.close(resolve));
    await db.close();
    await fs.rm(dir, { recursive: true, force: true });
  }
});
