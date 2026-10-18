"use strict";
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { readQueueBatch } = require('../lib/queue-batch');
const { getOrCreateMachineId } = require('../lib/machine-id');
const { resolveUploadConfig } = require('../lib/upload-config');

// Replay the ledger so queue repairs and interrupted uploads never lose counts.
// The server compares a file snapshot time + byte position before replacing rows.
async function uploadQueue({ queuePath = path.join(os.homedir(), '.tokentracker', 'tracker', 'queue.jsonl'),
  baseUrl, key, request = fetch, env = process.env } = {}) {
  const config = resolveUploadConfig({ queuePath, env });
  if (config.invalid) throw new Error('Stored upload credentials are invalid; run: tracker connect');
  const origin = baseUrl ? new URL(baseUrl).origin : config.apiUrl;
  const accessKey = key || config.accessKey;
  if (!origin) throw new Error('Set TOKENTRACKER_API_URL or run: tracker connect --api-url <url> --key <key>');
  if (!accessKey) throw new Error('Set TOKENTRACKER_ACCESS_KEY or run: tracker connect --api-url <url> --key <key>');
  const deviceId = getOrCreateMachineId(queuePath);
  if (!deviceId) throw new Error('Cannot persist local device identity');
  const stat = await fs.stat(queuePath).catch(() => ({ size: 0, mtime: new Date(0) }));
  const size = stat.size;
  let offset = 0;
  let uploaded = 0;
  while (offset < size) {
    const batch = await readQueueBatch(queuePath, offset, 200);
    if (batch.nextOffset <= offset) break; // incomplete trailing line: retry next run
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
    offset = batch.nextOffset; // advance only after a confirmed transaction
  }
  return { uploaded, pending: size - offset };
}
module.exports = { uploadQueue };
