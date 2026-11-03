// cursor-api usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const path = require("node:path");
const { ensureDir } = require("../fs");
const {
  DEFAULT_SOURCE,
  DEFAULT_MODEL,
  enqueueTouchedBuckets,
  normalizeHourlyState,
  getHourlyBucket,
  initTotals,
  addTotals,
  toUtcHalfHourStart,
  bucketKey,
  parseBucketKey,
  normalizeSourceInput,
  normalizeModelInput,
  isAllZeroUsage,
} = require("./core");

// ── Cursor (API-based) ──

/**
 * Incremental parser for Cursor usage data fetched via API.
 *
 * Unlike other parsers that read local files, this one receives pre-parsed
 * CSV records from cursor-config.js and aggregates them into 30-min buckets.
 *
 * Incremental state is tracked in `cursors.cursorApi.lastRecordTimestamp`.
 */
async function parseCursorApiIncremental({
  records,
  cursors,
  queuePath,
  onProgress,
  source,
}) {
  await ensureDir(path.dirname(queuePath));
  const defaultSource = normalizeSourceInput(source) || "cursor";
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();

  // Cursor's CSV is an account-level API export, not an append-only local log.
  // Treat the fetched CSV as authoritative so historical backfills and row
  // corrections replace prior local bucket totals instead of being skipped.
  const lastTs = cursors?.cursorApi?.lastRecordTimestamp || null;
  let latestTs = lastTs;
  let eventsAggregated = 0;
  const cb = typeof onProgress === "function" ? onProgress : null;
  const total = records.length;

  if (records.length > 0) {
    // Guard (2026-06 audit): only wipe buckets the fetched export can
    // actually rebuild. The wipe-then-refill design assumes the CSV is a
    // FULL-history export; if Cursor ever windows or truncates the export,
    // unconditionally zeroing every bucket would erase (and upload zeros
    // over) all history older than the response. Wiping from the earliest
    // record onward is identical for full exports and fail-safe for
    // partial ones.
    let earliestBucketStart = null;
    for (const record of records) {
      if (!record?.date) continue;
      const b = toUtcHalfHourStart(record.date);
      if (b && (!earliestBucketStart || b < earliestBucketStart)) earliestBucketStart = b;
    }
    // No parseable record date at all means the export is malformed —
    // refilling would add nothing, so wiping would zero out (and upload
    // zeros over) the entire history. Skip the wipe entirely.
    if (earliestBucketStart) {
      for (const [key, bucket] of Object.entries(hourlyState.buckets || {})) {
        const parsed = parseBucketKey(key);
        const sourceKey = normalizeSourceInput(parsed.source) || DEFAULT_SOURCE;
        if (sourceKey !== defaultSource) continue;
        if (!bucket?.totals) continue;
        if (parsed.hourStart && parsed.hourStart < earliestBucketStart) continue;
        bucket.totals = initTotals();
        touchedBuckets.add(key);
      }
    }
  }

  for (let i = 0; i < records.length; i++) {
    const record = records[i];
    const recordDate = record.date;
    if (!recordDate) continue;

    const { normalizeCursorUsage } = require("../cursor-config");
    const delta = normalizeCursorUsage(record);
    if (isAllZeroUsage(delta)) continue;

    delta.conversation_count = 1;

    const bucketStart = toUtcHalfHourStart(recordDate);
    if (!bucketStart) continue;

    const model = normalizeModelInput(record.model) || DEFAULT_MODEL;
    const bucket = getHourlyBucket(hourlyState, defaultSource, model, bucketStart);
    addTotals(bucket.totals, delta);
    touchedBuckets.add(bucketKey(defaultSource, model, bucketStart));

    eventsAggregated += 1;

    // Track latest timestamp
    if (!latestTs || recordDate > latestTs) {
      latestTs = recordDate;
    }

    if (cb && (i % 200 === 0 || i === records.length - 1)) {
      cb({
        index: i + 1,
        total,
        eventsAggregated,
        bucketsQueued: touchedBuckets.size,
      });
    }
  }

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  hourlyState.updatedAt = new Date().toISOString();
  cursors.hourly = hourlyState;

  // Update cursor state
  if (!cursors.cursorApi) cursors.cursorApi = {};
  if (latestTs && latestTs !== lastTs) {
    cursors.cursorApi.lastRecordTimestamp = latestTs;
  }
  cursors.cursorApi.updatedAt = new Date().toISOString();

  return { recordsProcessed: total, eventsAggregated, bucketsQueued };
}

module.exports = { parseCursorApiIncremental };
