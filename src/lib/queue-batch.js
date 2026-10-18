"use strict";

const fs = require("node:fs/promises");
const { iterateQueueLines, sessionSnapshotMs } = require("./upload-queue");

// Legacy Codex rows stored input inclusive of cache reads while total_tokens
// stayed input + output; canonical rows keep input as non-cached input only.
function isLegacyInclusiveCodexRow(row) {
  if (!row || (row.source !== "codex" && row.source !== "every-code")) return false;
  const inputTokens = Number(row.input_tokens || 0);
  const cachedInputTokens = Number(row.cached_input_tokens || 0);
  const outputTokens = Number(row.output_tokens || 0);
  const totalTokens = Number(row.total_tokens || 0);
  if (!Number.isFinite(inputTokens) || !Number.isFinite(cachedInputTokens)) return false;
  if (cachedInputTokens <= 0 || inputTokens < cachedInputTokens) return false;
  return totalTokens === inputTokens + outputTokens;
}

/**
 * Read-time healing for historical rows so an unchanged queue file still
 * uploads correct numbers.
 */
function normalizeQueueRow(row) {
  let normalized = row;
  if (isLegacyInclusiveCodexRow(normalized)) {
    normalized = {
      ...normalized,
      input_tokens: Number(normalized.input_tokens || 0) - Number(normalized.cached_input_tokens || 0),
    };
  }
  // Cursor ≤0.26.5 and a short Antigravity window wrote billable_total_tokens=0
  // for included/no-charge records. Billing and usage are orthogonal, so raise
  // billable to the observed total at read time instead of rewriting the queue.
  const sourceName = String(normalized.source || "").toLowerCase();
  if (sourceName === "cursor" || sourceName === "antigravity") {
    const totalTokens = Number(normalized.total_tokens || 0);
    const billable = Number(normalized.billable_total_tokens || 0);
    if (totalTokens > 0 && billable < totalTokens) {
      normalized = { ...normalized, billable_total_tokens: totalTokens };
    }
  }
  return normalized;
}

/**
 * Read one upload batch from the append-only queue.
 *
 * Bucket rows collapse to the last snapshot per (source, model, hour). Account
 * session states (`kind: "account_session_state"`) are control records: they
 * ride the same request, keyed by (source, session_id) with a strict
 * snapshot_verified_at LWW guard, and count toward the same record cap.
 * Incomplete trailing bytes are never consumed, so a mid-write producer is
 * retried on the next run instead of losing rows.
 */
async function readQueueBatch(queuePath, startOffset, maxBuckets) {
  const st = await fs.stat(queuePath).catch(() => null);
  if (!st || !st.isFile() || startOffset >= st.size) {
    return { buckets: [], sessionStates: [], nextOffset: startOffset };
  }
  const handle = await fs.open(queuePath, "r");
  const bucketMap = new Map();
  const sessionStateMap = new Map();
  const sessionStateStamps = new Map();
  let offset = startOffset;
  let linesRead = 0;
  try {
    for await (const record of iterateQueueLines(handle, startOffset, st.size)) {
      if (!record.complete) break;
      const line = record.bytes.toString("utf8");
      offset += record.bytes.length;
      if (!line.trim()) continue;
      let bucket;
      try {
        bucket = JSON.parse(line);
      } catch (_e) {
        continue;
      }
      if (bucket?.kind === "account_session_state") {
        if (typeof bucket.source === "string" && typeof bucket.session_id === "string" && bucket.session_id.trim()) {
          const source = bucket.source.trim().toLowerCase();
          const stateKey = `${source}|${bucket.session_id.trim()}`;
          const state = {
            source,
            session_id: bucket.session_id.trim(),
            model: typeof bucket.model === "string" ? bucket.model : "",
            bucket_start: typeof bucket.bucket_start === "string" ? bucket.bucket_start : "",
            input_tokens: Number(bucket.input_tokens) || 0,
            output_tokens: Number(bucket.output_tokens) || 0,
            cached_input_tokens: Number(bucket.cached_input_tokens) || 0,
            cache_creation_input_tokens: Number(bucket.cache_creation_input_tokens) || 0,
            reasoning_output_tokens: Number(bucket.reasoning_output_tokens) || 0,
            total_tokens: Number(bucket.total_tokens) || 0,
            ...(typeof bucket.snapshot_verified_at === "string" && bucket.snapshot_verified_at
              ? { snapshot_verified_at: bucket.snapshot_verified_at }
              : {}),
          };
          const verifiedMs = sessionSnapshotMs(bucket);
          const priorVerifiedMs = sessionStateStamps.get(stateKey) ?? null;
          if (!sessionStateMap.has(stateKey) || (verifiedMs !== null && (priorVerifiedMs === null || verifiedMs > priorVerifiedMs))) {
            sessionStateMap.set(stateKey, state);
            sessionStateStamps.set(stateKey, verifiedMs);
          }
          linesRead += 1;
          if (linesRead >= maxBuckets) break;
        }
        continue;
      }
      if (bucket?.kind === "account_sync_watermark") continue;
      const hourStart = typeof bucket?.hour_start === "string" ? bucket.hour_start : null;
      if (!hourStart) continue;
      const source = (typeof bucket?.source === "string" ? bucket.source.trim().toLowerCase() : "") || "codex";
      const model = (typeof bucket?.model === "string" ? bucket.model.trim() : "") || "unknown";
      bucket.source = source;
      bucket.model = model;
      bucket = normalizeQueueRow(bucket);
      bucketMap.set(`${source}|${model}|${hourStart}`, bucket);
      linesRead += 1;
      if (linesRead >= maxBuckets) break;
    }
  } finally {
    await handle.close();
  }
  return {
    buckets: Array.from(bucketMap.values()),
    sessionStates: Array.from(sessionStateMap.values()),
    nextOffset: offset,
  };
}

module.exports = { readQueueBatch, normalizeQueueRow, isLegacyInclusiveCodexRow };
