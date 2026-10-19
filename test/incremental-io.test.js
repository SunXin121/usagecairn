"use strict";
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { uploadQueue, uploadedOffset } = require('../src/commands/upload');
const { readKiroJsonlTokens } = require('../src/lib/kiro-jsonl');
const { writeUploadConfig } = require('../src/lib/upload-config');

async function sandbox(t) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'tt-incremental-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const tracker = path.join(dir, '.token-pulse', 'tracker');
  await fs.mkdir(tracker, { recursive: true });
  return { dir, queuePath: path.join(tracker, 'queue.jsonl') };
}
const keyA = 'tt_' + 'a'.repeat(64);
const keyB = 'tt_' + 'b'.repeat(64);
const entry = (i) => JSON.stringify({ hour_start: '2026-10-18T12:00:00Z', source: 'claude',
  model: `model-${i}`, input_tokens: i, total_tokens: i }) + '\n';

test('upload persists each acknowledged batch and only reads the appended tail', async (t) => {
  const { queuePath } = await sandbox(t);
  await fs.writeFile(queuePath, Array.from({ length: 201 }, (_, i) => entry(i + 1)).join(''));
  const calls = [];
  let failSecond = true;
  const request = async (_url, opts) => {
    calls.push(JSON.parse(opts.body));
    if (failSecond && calls.length === 2) return { ok: false, status: 503 };
    return { ok: true, status: 200 };
  };
  const options = { queuePath, baseUrl: 'http://localhost:3000', key: keyA, env: {}, request };
  await assert.rejects(uploadQueue(options), /503/);
  const statePath = path.join(path.dirname(queuePath), 'upload.state.json');
  assert.equal((await fs.stat(statePath)).mode & 0o777, 0o600);
  assert.equal((await fs.readFile(statePath, 'utf8')).includes(keyA), false);
  assert.equal((await uploadQueue({ ...options, request: async (_url, opts) => {
    calls.push(JSON.parse(opts.body)); return { ok: true, status: 200 };
  } })).uploaded, 1);
  assert.equal(calls.length, 3);
  assert.equal(calls[2].hourly[0].model, 'model-201');
  assert.deepEqual(await uploadQueue(options), { uploaded: 0, pending: 0 });
  await fs.appendFile(queuePath, entry(202));
  assert.deepEqual(await uploadQueue(options), { uploaded: 1, pending: 0 });
  assert.equal(calls.at(-1).hourly[0].model, 'model-202');
  assert.equal((await uploadQueue({ ...options, replay: true })).uploaded, 202);
  assert.equal((await uploadQueue({ ...options, key: keyB })).uploaded, 202, 'new account must get full history');
});

test('a replaced queue invalidates upload progress even at the same size', async (t) => {
  const { queuePath } = await sandbox(t);
  await fs.writeFile(queuePath, entry(1));
  const seen = [];
  const options = { queuePath, baseUrl: 'http://localhost:3000', key: keyA, env: {},
    request: async (_url, opts) => { seen.push(JSON.parse(opts.body).hourly[0].input_tokens); return { ok: true, status: 200 }; } };
  await uploadQueue(options);
  const replacement = `${queuePath}.new`;
  await fs.writeFile(replacement, entry(2));
  await fs.rename(replacement, queuePath);
  assert.equal((await uploadQueue(options)).uploaded, 1);
  assert.deepEqual(seen, [1, 2]);
});

test('Kiro migrates line-only state once and subsequently reads only appended bytes', async (t) => {
  const { dir } = await sandbox(t);
  const file = path.join(dir, 'kiro.jsonl');
  const line = (n) => JSON.stringify({ model: 'agent', promptTokens: n, generatedTokens: 1 }) + '\n';
  const prefix = line(1) + line(2);
  await fs.writeFile(file, prefix + line(3));
  const migrated = await readKiroJsonlTokens(file, { lastLine: 2 });
  assert.deepEqual(migrated.rows.map((row) => row.tokens_prompt), [3]);
  assert.equal(migrated.cursor.byteOffset, Buffer.byteLength(prefix + line(3)));
  await fs.appendFile(file, line(4));
  const next = await readKiroJsonlTokens(file, migrated.cursor);
  assert.deepEqual(next.rows.map((row) => row.tokens_prompt), [4]);
  assert.equal(next.lineCount, 4);
  assert.deepEqual((await readKiroJsonlTokens(file, next.cursor)).rows, []);
  await fs.truncate(file, Buffer.byteLength(line(1)));
  const reset = await readKiroJsonlTokens(file, next.cursor);
  assert.equal(reset.reset, true);
  assert.deepEqual(reset.rows, []);
  await fs.appendFile(file, line(5));
  assert.deepEqual((await readKiroJsonlTokens(file, reset.cursor)).rows.map((row) => row.tokens_prompt), [5]);
});

test('backlog offset matches the configured destination and is reset for a new key', async (t) => {
  const { queuePath } = await sandbox(t);
  await fs.writeFile(queuePath, entry(1));
  writeUploadConfig(queuePath, { apiUrl: 'https://tracker.example.com', accessKey: keyA });
  const request = async () => ({ ok: true, status: 200 });
  assert.equal(await uploadedOffset(queuePath, {}), 0);
  await uploadQueue({ queuePath, env: {}, request });
  assert.equal(await uploadedOffset(queuePath, {}), (await fs.stat(queuePath)).size);
  writeUploadConfig(queuePath, { apiUrl: 'https://tracker.example.com', accessKey: keyB });
  assert.equal(await uploadedOffset(queuePath, {}), 0);
});
