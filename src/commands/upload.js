"use strict";
const crypto = require('node:crypto');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { readJson, writeFileAtomic, openLock } = require('../lib/fs');
const { readQueueBatch } = require('../lib/queue-batch');
const { getOrCreateMachineId } = require('../lib/machine-id');
const { resolveUploadConfig } = require('../lib/upload-config');
const { compactPendingQueue } = require('../lib/upload-queue');

const SAMPLE_BYTES = 4096;
// Servers before batched ingest upserted rows one statement at a time inside a
// 30-second function limit, so every upload starts with small batches. A server
// that answers with `max_records` writes a request in one statement; batches
// then grow to its advertised limit (capped here).
const LEGACY_BATCH_ROWS = 20;
const MAX_BATCH_ROWS = 200;
const PRIORITY_FIELDS = ['priority_input_tokens', 'priority_cached_input_tokens', 'priority_cache_creation_input_tokens',
  'priority_output_tokens', 'priority_reasoning_output_tokens'];
const UPLOAD_LOCK_POLL_MS = 100;

function uploadLockPath(queuePath) {
  return path.join(path.dirname(queuePath), 'upload.lock');
}

// Serializes uploads with each other and with queue compaction, which rewrites
// the pending tail of queue.jsonl. Returns null when the lock stays busy.
async function acquireUploadLock(queuePath, waitMs = 0) {
  const lockPath = uploadLockPath(queuePath);
  const deadline = Date.now() + Math.max(0, waitMs);
  for (;;) {
    const lock = await openLock(lockPath, { quietIfLocked: true });
    if (lock || Date.now() >= deadline) return lock;
    await new Promise((resolve) => setTimeout(resolve, Math.min(UPLOAD_LOCK_POLL_MS, Math.max(1, deadline - Date.now()))));
  }
}

async function advertisedBatchRows(response, current) {
  if (typeof response.json !== 'function') return current;
  try {
    const limit = (await response.json())?.max_records;
    return Number.isSafeInteger(limit) && limit > 0 ? Math.min(limit, MAX_BATCH_ROWS) : current;
  } catch { return current; }
}

async function sample(handle, position, length) {
  const bytes = Buffer.alloc(length);
  let read = 0;
  while (read < length) {
    const result = await handle.read(bytes, read, length - read, position + read);
    if (!result.bytesRead) throw new Error('Queue changed while reading upload progress');
    read += result.bytesRead;
  }
  return crypto.createHash('sha256').update(bytes).digest('hex');
}

async function queueFingerprint(queuePath, offset) {
  const handle = await fs.open(queuePath, 'r');
  try {
    const headLength = Math.min(offset, SAMPLE_BYTES);
    const tailLength = Math.min(offset, SAMPLE_BYTES);
    return {
      head: await sample(handle, 0, headLength),
      tail: await sample(handle, offset - tailLength, tailLength),
    };
  } finally { await handle.close(); }
}

// A bounded fingerprint catches truncation/replacement without rereading the
// historical ledger. If the progress file is lost, replay is safe (idempotent).
async function uploadStart(queuePath, statePath, identity, stat, replay) {
  if (replay) return 0;
  const saved = await readJson(statePath);
  if (!saved || saved.identity !== identity || !Number.isSafeInteger(saved.offset) ||
      saved.offset < 0 || saved.offset > stat.size || saved.dev !== stat.dev || saved.ino !== stat.ino) return 0;
  const fingerprint = await queueFingerprint(queuePath, saved.offset);
  return fingerprint.head === saved.head && fingerprint.tail === saved.tail ? saved.offset : 0;
}

async function uploadedOffset(queuePath, env = process.env) {
  const config = resolveUploadConfig({ queuePath, env });
  if (!config.apiUrl || !config.accessKey) return 0;
  const stat = await fs.stat(queuePath).catch((error) => {
    if (error.code === 'ENOENT') return null;
    throw error;
  });
  if (!stat) return 0;
  const deviceId = getOrCreateMachineId(queuePath);
  if (!deviceId) return 0;
  const identity = crypto.createHash('sha256').update(JSON.stringify([config.apiUrl, config.accessKey, deviceId])).digest('hex');
  return uploadStart(queuePath, path.join(path.dirname(queuePath), 'upload.state.json'), identity, stat, false);
}

// lockWaitMs: how long to wait for a concurrent upload (or compaction) to
// finish. When it never frees up the call returns { busy: true } untouched.
async function uploadQueue({ queuePath = path.join(os.homedir(), '.token-pulse', 'tracker', 'queue.jsonl'),
  baseUrl, key, request = fetch, env = process.env, replay = false, lockWaitMs = 60_000 } = {}) {
  const config = resolveUploadConfig({ queuePath, env });
  if (config.invalid) throw new Error('Stored upload credentials are invalid; run: usagecairn connect');
  const origin = baseUrl ? new URL(baseUrl).origin : config.apiUrl;
  const accessKey = key || config.accessKey;
  if (!origin) throw new Error('Set TOKEN_PULSE_API_URL or run: usagecairn connect --api-url <url> --key <key>');
  if (!accessKey) throw new Error('Set TOKEN_PULSE_ACCESS_KEY or run: usagecairn connect --api-url <url> --key <key>');
  const deviceId = getOrCreateMachineId(queuePath);
  if (!deviceId) throw new Error('Cannot persist local device identity');
  const stat = await fs.stat(queuePath).catch((error) => {
    if (error.code === 'ENOENT') return null;
    throw error;
  });
  if (!stat) return { uploaded: 0, pending: 0 };
  const lock = await acquireUploadLock(queuePath, lockWaitMs);
  if (!lock) return { uploaded: 0, pending: stat.size - await uploadedOffset(queuePath, env), busy: true };
  try {
    return await uploadLocked({ queuePath, origin, accessKey, deviceId, request, replay });
  } finally { await lock.release(); }
}

async function uploadLocked({ queuePath, origin, accessKey, deviceId, request, replay }) {
  // Re-stat under the lock: a compaction may have replaced the queue meanwhile.
  const stat = await fs.stat(queuePath).catch((error) => {
    if (error.code === 'ENOENT') return null;
    throw error;
  });
  if (!stat) return { uploaded: 0, pending: 0 };
  const size = stat.size;
  const statePath = path.join(path.dirname(queuePath), 'upload.state.json');
  const identity = crypto.createHash('sha256').update(JSON.stringify([origin, accessKey, deviceId])).digest('hex');
  let offset = await uploadStart(queuePath, statePath, identity, stat, replay);
  let uploaded = 0;
  let batchRows = LEGACY_BATCH_ROWS;
  while (offset < size) {
    const batch = await readQueueBatch(queuePath, offset, batchRows);
    if (batch.nextOffset <= offset) break; // incomplete trailing line: retry next run
    if (batch.nextOffset > size) break; // producer appended beyond this run's snapshot
    const hourly = batch.buckets.map((row) => ({
      hour_start: row.hour_start, source: row.source, model: row.model,
      input_tokens: row.input_tokens || 0, output_tokens: row.output_tokens || 0,
      cached_input_tokens: row.cached_input_tokens || 0,
      cache_creation_input_tokens: row.cache_creation_input_tokens || 0,
      reasoning_output_tokens: row.reasoning_output_tokens || 0,
      total_tokens: row.total_tokens || 0, conversation_count: row.conversation_count || 0,
      total_cost_usd: Number(row.total_cost_usd) || 0,
      ...Object.fromEntries(PRIORITY_FIELDS.filter((field) => Number(row[field]) > 0).map((field) => [field, Number(row[field])])),
    }));
    const account_session_states = batch.sessionStates.map((row) => ({
      source: row.source, session_id: row.session_id, model: row.model,
      bucket_start: row.bucket_start, snapshot_verified_at: row.snapshot_verified_at,
      input_tokens: row.input_tokens, output_tokens: row.output_tokens,
      cached_input_tokens: row.cached_input_tokens,
      cache_creation_input_tokens: row.cache_creation_input_tokens,
      reasoning_output_tokens: row.reasoning_output_tokens, total_tokens: row.total_tokens,
    }));
    if (hourly.length || account_session_states.length) {
      const response = await request(`${origin}/api/ingest`, {
        method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${accessKey}` },
        body: JSON.stringify({ device_id: deviceId, hourly, account_session_states,
          snapshot_at: stat.mtime.toISOString(), snapshot_revision: batch.nextOffset }),
        signal: AbortSignal.timeout(30000),
      });
      if (!response.ok) throw new Error(`Upload failed: HTTP ${response.status}`);
      uploaded += hourly.length + account_session_states.length;
      batchRows = await advertisedBatchRows(response, batchRows);
    }
    // A replacement during the request must not mark bytes from a different
    // ledger as uploaded. Appends are fine; the next run will pick up the tail.
    const current = await fs.stat(queuePath);
    if (current.dev !== stat.dev || current.ino !== stat.ino || current.size < batch.nextOffset) {
      throw new Error('Queue changed during upload; retry');
    }
    offset = batch.nextOffset;
    const fingerprint = await queueFingerprint(queuePath, offset);
    await writeFileAtomic(statePath, JSON.stringify({ identity, offset, dev: stat.dev, ino: stat.ino,
      ...fingerprint }) + '\n', { mode: 0o600 });
  }
  return { uploaded, pending: size - offset };
}
// Called by compaction (with the upload lock held) after it atomically replaced
// queue.jsonl: bytes [0, offset) are unchanged, so carry the saved progress over
// to the new inode instead of forcing a full replay.
async function rebindUploadState(queuePath, offset) {
  const statePath = path.join(path.dirname(queuePath), 'upload.state.json');
  const saved = await readJson(statePath);
  if (!saved || saved.offset !== offset) return false;
  const stat = await fs.stat(queuePath);
  const fingerprint = await queueFingerprint(queuePath, offset);
  if (fingerprint.head !== saved.head || fingerprint.tail !== saved.tail) return false;
  await writeFileAtomic(statePath, JSON.stringify({ ...saved, dev: stat.dev, ino: stat.ino }) + '\n', { mode: 0o600 });
  return true;
}

// Collapse superseded snapshots in the not-yet-uploaded tail so a long offline
// (or never-connected) period does not grow the ledger without bound. The
// caller holds sync.lock; the upload lock keeps uploads out while the file is
// replaced. Skips quietly when an upload is running or too little would shrink.
const COMPACT_MIN_PENDING_BYTES = 1_000_000;
const COMPACT_MIN_REMOVED_BYTES = 256 * 1024;
async function compactQueueBacklog(queuePath, { env = process.env,
  minPendingBytes = COMPACT_MIN_PENDING_BYTES, minRemovedBytes = COMPACT_MIN_REMOVED_BYTES } = {}) {
  const lock = await acquireUploadLock(queuePath, 0);
  if (!lock) return { compacted: false, busy: true };
  try {
    const offset = await uploadedOffset(queuePath, env);
    const result = await compactPendingQueue({ queuePath, offset, minPendingBytes, minRemovedBytes });
    if (result.compacted && offset > 0) await rebindUploadState(queuePath, offset);
    return result;
  } finally { await lock.release(); }
}

module.exports = { uploadQueue, compactQueueBacklog, uploadedOffset, acquireUploadLock, rebindUploadState, LEGACY_BATCH_ROWS, MAX_BATCH_ROWS };
