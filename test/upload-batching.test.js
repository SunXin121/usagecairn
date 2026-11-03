"use strict";
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { uploadQueue, uploadedOffset, acquireUploadLock, compactQueueBacklog } = require('../src/commands/upload');
const { writeUploadConfig } = require('../src/lib/upload-config');

const key = 'tt_' + 'c'.repeat(64);
const origin = 'http://localhost:3000';
const entry = (i, total = i, extra = {}) => JSON.stringify({ hour_start: '2026-10-18T12:00:00Z', source: 'claude',
  model: `model-${i}`, input_tokens: total, total_tokens: total, ...extra }) + '\n';

async function sandbox(t) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'tt-batching-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const tracker = path.join(dir, 'tracker');
  await fs.mkdir(tracker, { recursive: true });
  return path.join(tracker, 'queue.jsonl');
}

const recorder = (reply = {}) => {
  const calls = [];
  const request = async (_url, opts) => {
    calls.push(JSON.parse(opts.body));
    return { ok: true, status: 200, json: async () => reply };
  };
  return { calls, request };
};

test('batches start at the legacy size and grow to the limit a batched server advertises', async (t) => {
  const queuePath = await sandbox(t);
  await fs.writeFile(queuePath, Array.from({ length: 450 }, (_, i) => entry(i + 1)).join(''));
  const legacy = recorder({ inserted: 20, skipped: 0 });
  await uploadQueue({ queuePath, baseUrl: origin, key, env: {}, request: legacy.request, replay: true });
  assert.ok(legacy.calls.every((call) => call.hourly.length <= 20), 'legacy servers keep 20-row batches');
  const batched = recorder({ inserted: 1, skipped: 0, max_records: 200 });
  assert.equal((await uploadQueue({ queuePath, baseUrl: origin, key, env: {}, request: batched.request, replay: true })).uploaded, 450);
  assert.deepEqual(batched.calls.map((call) => call.hourly.length), [20, 200, 200, 30]);
  const capped = recorder({ max_records: 5000 });
  await uploadQueue({ queuePath, baseUrl: origin, key, env: {}, request: capped.request, replay: true });
  assert.equal(Math.max(...capped.calls.map((call) => call.hourly.length)), 200);
});

test('priority-tier subsets are uploaded only when present', async (t) => {
  const queuePath = await sandbox(t);
  await fs.writeFile(queuePath, entry(1, 10, { priority_input_tokens: 4, priority_output_tokens: 0 }) + entry(2));
  const { calls, request } = recorder();
  await uploadQueue({ queuePath, baseUrl: origin, key, env: {}, request });
  const [withPriority, without] = calls[0].hourly;
  assert.equal(withPriority.priority_input_tokens, 4);
  assert.equal('priority_output_tokens' in withPriority, false);
  assert.equal(Object.keys(without).some((field) => field.startsWith('priority_')), false);
});

test('a concurrent upload is skipped instead of racing on progress', async (t) => {
  const queuePath = await sandbox(t);
  await fs.writeFile(queuePath, entry(1));
  const held = await acquireUploadLock(queuePath);
  try {
    const { calls, request } = recorder();
    const result = await uploadQueue({ queuePath, baseUrl: origin, key, env: {}, request, lockWaitMs: 0 });
    assert.equal(result.busy, true);
    assert.equal(calls.length, 0);
    assert.equal((await compactQueueBacklog(queuePath, { env: {} })).busy, true);
  } finally { await held.release(); }
  const { calls, request } = recorder();
  assert.equal((await uploadQueue({ queuePath, baseUrl: origin, key, env: {}, request, lockWaitMs: 0 })).uploaded, 1);
  assert.equal(calls.length, 1);
});

test('backlog compaction shrinks the pending tail and keeps upload progress', async (t) => {
  const queuePath = await sandbox(t);
  writeUploadConfig(queuePath, { apiUrl: origin, accessKey: key });
  await fs.writeFile(queuePath, entry(1) + entry(2));
  const first = recorder();
  await uploadQueue({ queuePath, env: {}, request: first.request });
  const uploaded = (await fs.stat(queuePath)).size;
  assert.equal(await uploadedOffset(queuePath, {}), uploaded);
  // 300 superseded snapshots of one bucket, then the latest value.
  await fs.appendFile(queuePath, Array.from({ length: 300 }, (_, i) => entry(3, i)).join('') + entry(3, 999));
  // Below the removable-bytes threshold nothing is rewritten.
  assert.equal((await compactQueueBacklog(queuePath, { env: {}, minPendingBytes: 0 })).compacted, false);
  const result = await compactQueueBacklog(queuePath, { env: {}, minPendingBytes: 0, minRemovedBytes: 1 });
  assert.equal(result.compacted, true);
  assert.equal(result.removedRecords, 300);
  assert.equal((await fs.stat(queuePath)).size, uploaded + Buffer.byteLength(entry(3, 999)));
  assert.equal(await uploadedOffset(queuePath, {}), uploaded, 'progress survives the inode change');
  const next = recorder();
  assert.equal((await uploadQueue({ queuePath, env: {}, request: next.request })).uploaded, 1);
  assert.deepEqual(next.calls[0].hourly.map((row) => row.total_tokens), [999]);
});
