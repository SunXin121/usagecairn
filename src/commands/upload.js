"use strict";
const crypto = require('node:crypto');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { readJson, writeFileAtomic } = require('../lib/fs');
const { readQueueBatch } = require('../lib/queue-batch');
const { getOrCreateMachineId } = require('../lib/machine-id');
const { resolveUploadConfig } = require('../lib/upload-config');

const SAMPLE_BYTES = 4096;

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

async function uploadQueue({ queuePath = path.join(os.homedir(), '.token-pulse', 'tracker', 'queue.jsonl'),
  baseUrl, key, request = fetch, env = process.env, replay = false } = {}) {
  const config = resolveUploadConfig({ queuePath, env });
  if (config.invalid) throw new Error('Stored upload credentials are invalid; run: token-pulse connect');
  const origin = baseUrl ? new URL(baseUrl).origin : config.apiUrl;
  const accessKey = key || config.accessKey;
  if (!origin) throw new Error('Set TOKEN_PULSE_API_URL or run: token-pulse connect --api-url <url> --key <key>');
  if (!accessKey) throw new Error('Set TOKEN_PULSE_ACCESS_KEY or run: token-pulse connect --api-url <url> --key <key>');
  const deviceId = getOrCreateMachineId(queuePath);
  if (!deviceId) throw new Error('Cannot persist local device identity');
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
  while (offset < size) {
    const batch = await readQueueBatch(queuePath, offset, 200);
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
module.exports = { uploadQueue, uploadedOffset };
