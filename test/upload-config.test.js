const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { writeUploadConfig, readUploadConfig, resolveUploadConfig, configPathFor, isValidApiUrl } = require('../src/lib/upload-config');
const { uploadQueue } = require('../src/commands/upload');
const { cmdConnect } = require('../src/commands/connect');

const key = `tt_${'c'.repeat(64)}`;

async function sandbox() {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'tt-connect-'));
  return { dir, queuePath: path.join(dir, 'queue.jsonl') };
}

test('persisted credentials let background hooks upload without env vars', async () => {
  const { dir, queuePath } = await sandbox();
  try {
    const result = await cmdConnect(['--api-url', 'https://tracker.example.com', '--key', key], { queuePath, log: () => {} });
    assert.equal(result.saved, true);
    const mode = (await fs.stat(configPathFor(queuePath))).mode & 0o777;
    assert.equal(mode, 0o600);
    assert.deepEqual(await readUploadConfig(queuePath), { apiUrl: 'https://tracker.example.com', accessKey: key });

    const resolved = resolveUploadConfig({ queuePath, env: {} });
    assert.equal(resolved.source, 'file');
    assert.equal(resolved.apiUrl, 'https://tracker.example.com');

    const envWins = resolveUploadConfig({ queuePath, env: { TOKEN_PULSE_API_URL: 'http://localhost:3000' } });
    assert.equal(envWins.source, 'env');
    assert.equal(envWins.apiUrl, 'http://localhost:3000');

    await fs.writeFile(queuePath, `${JSON.stringify({ hour_start: '2026-10-18T12:00:00Z', source: 'claude', model: 'claude-sonnet-4', input_tokens: 10, output_tokens: 5, total_tokens: 15, conversation_count: 1 })}\n`);
    const calls = [];
    const summary = await uploadQueue({
      queuePath,
      request: async (url, options) => { calls.push({ url, options }); return { ok: true, status: 200 }; },
      env: {},
    });
    assert.equal(summary.uploaded, 1);
    assert.equal(calls[0].url, 'https://tracker.example.com/api/ingest');
    assert.equal(calls[0].options.headers.Authorization, `Bearer ${key}`);
    const body = JSON.parse(calls[0].options.body);
    assert.equal(body.hourly.length, 1);
    assert.match(body.device_id, /^[A-Za-z0-9_-]{8,}$/);
    assert.equal(typeof body.snapshot_at, 'string');
    assert.ok(body.snapshot_revision > 0);
  } finally { await fs.rm(dir, { recursive: true, force: true }); }
});

test('upload splits a backlog into requests that fit the server time limit', async () => {
  const { dir, queuePath } = await sandbox();
  try {
    writeUploadConfig(queuePath, { apiUrl: 'https://tracker.example.com', accessKey: key });
    const rows = Array.from({ length: 45 }, (_, i) => ({
      hour_start: new Date(Date.UTC(2026, 9, 18, i)).toISOString(),
      source: 'claude', model: 'claude-sonnet-4',
      input_tokens: 10, output_tokens: 5, total_tokens: 15, conversation_count: 1,
    }));
    await fs.writeFile(queuePath, rows.map((row) => JSON.stringify(row)).join('\n') + '\n');
    const batches = [];
    const request = async (_url, options) => {
      const body = JSON.parse(options.body);
      batches.push({ count: body.hourly.length, revision: body.snapshot_revision });
      return { ok: true, status: 200 };
    };
    assert.deepEqual(await uploadQueue({ queuePath, env: {}, request }), { uploaded: 45, pending: 0 });
    assert.deepEqual(batches.map((batch) => batch.count), [20, 20, 5]);
    assert.ok(batches[0].revision < batches[1].revision && batches[1].revision < batches[2].revision);
    assert.deepEqual(await uploadQueue({ queuePath, env: {}, request }), { uploaded: 0, pending: 0 });
    assert.equal(batches.length, 3);
  } finally { await fs.rm(dir, { recursive: true, force: true }); }
});

test('connect rejects unsafe URLs and empty keys, accepts any key format, and can clear credentials', async () => {
  const { dir, queuePath } = await sandbox();
  try {
    await assert.rejects(cmdConnect(['--api-url', 'http://tracker.example.com', '--key', key], { queuePath, log: () => {} }), /HTTPS origin/);
    await assert.rejects(cmdConnect(['--api-url', 'https://tracker.example.com', '--key', '  '], { queuePath, log: () => {} }), /non-empty/);
    await assert.rejects(cmdConnect(['--api-url', 'https://tracker.example.com', '--key', ''], { queuePath, log: () => {} }), /required|non-empty/);
    assert.equal(isValidApiUrl('https://user:pw@example.com'), false);
    assert.equal(isValidApiUrl('https://example.com/path'), false);
    await cmdConnect(['--api-url', 'https://tracker.example.com', '--key', 'my-custom-key-2026'], { queuePath, log: () => {} });
    assert.equal((await cmdConnect(['--clear'], { queuePath, log: () => {} })).cleared, true);
    assert.deepEqual(await readUploadConfig(queuePath), { apiUrl: null, accessKey: null });
    await assert.rejects(uploadQueue({ queuePath, env: {}, request: async () => ({ ok: true, status: 200 }) }), /usagecairn connect/);
  } finally { await fs.rm(dir, { recursive: true, force: true }); }
});
