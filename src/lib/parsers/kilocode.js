// kilocode usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const path = require("node:path");
const { ensureDir } = require("../fs");
const {
  enqueueTouchedBuckets,
  normalizeHourlyState,
  getHourlyBucket,
  addTotals,
  toUtcHalfHourStart,
  bucketKey,
  toNonNegativeInt,
  resolveKilocodeRoots,
} = require("./core");

function resolveKilocodeTaskFiles(env = process.env) {
  const roots = resolveKilocodeRoots(env);
  const out = [];
  for (const root of roots) {
    const tasksDir = path.join(root, "User", "globalStorage", "kilocode.kilo-code", "tasks");
    if (!fssync.existsSync(tasksDir)) continue;
    let entries;
    try { entries = fssync.readdirSync(tasksDir); } catch { continue; }
    for (const taskUuid of entries) {
      const filePath = path.join(tasksDir, taskUuid, "ui_messages.json");
      if (!fssync.existsSync(filePath)) continue;
      out.push({ filePath, taskUuid, ide: path.basename(root) });
    }
  }
  out.sort((a, b) => a.filePath.localeCompare(b.filePath));
  return out;
}

// Kilo Code only persists the inference provider (e.g. "minimax",
// "Moonshot AI", "Stealth") in ui_messages.json — the actual model id is
// stored in workspace state but isn't attributed to individual turns and may
// change across sessions, so we cannot map a row back to a model id reliably.
// We surface the provider explicitly so the dashboard's Model column doesn't
// imply this is a model.
function normalizeKilocodeProviderToModel(providerName) {
  if (typeof providerName !== "string" || !providerName.trim()) return "provider:unknown";
  const slug = providerName
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "-")
    .replace(/[^a-z0-9._-]/g, "");
  // A slug consisting only of separators (dashes/dots/underscores) carries no
  // information — treat it as unknown.
  if (!slug || !/[a-z0-9]/.test(slug)) return "provider:unknown";
  return `provider:${slug}`;
}

async function parseKilocodeIncremental({
  taskFiles,
  cursors,
  queuePath,
  onProgress,
  env,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const kilocodeState =
    cursors.kilocode && typeof cursors.kilocode === "object" ? cursors.kilocode : {};
  const seenIds = new Set(
    Array.isArray(kilocodeState.seenIds) ? kilocodeState.seenIds : [],
  );
  const fileOffsets =
    kilocodeState.fileOffsets && typeof kilocodeState.fileOffsets === "object"
      ? { ...kilocodeState.fileOffsets }
      : {};

  const files = Array.isArray(taskFiles)
    ? taskFiles
    : resolveKilocodeTaskFiles(env || process.env);

  if (files.length === 0) {
    cursors.kilocode = {
      ...kilocodeState,
      seenIds: Array.from(seenIds),
      fileOffsets,
      updatedAt: new Date().toISOString(),
    };
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;
  let recordsProcessed = 0;
  let eventsAggregated = 0;

  for (let fileIdx = 0; fileIdx < files.length; fileIdx++) {
    const entry = files[fileIdx];
    const { filePath, taskUuid } = entry;
    let stat;
    try { stat = fssync.statSync(filePath); } catch { continue; }

    const prevEntry = fileOffsets[filePath];
    if (
      prevEntry &&
      Number(prevEntry.size) === stat.size &&
      Number(prevEntry.mtimeMs) === stat.mtimeMs
    ) {
      continue;
    }

    let raw;
    try { raw = fssync.readFileSync(filePath, "utf8"); } catch { continue; }
    let data;
    try { data = JSON.parse(raw); } catch { continue; }
    if (!Array.isArray(data)) continue;

    for (const msg of data) {
      if (!msg || typeof msg !== "object") continue;
      // `api_req_started` is the live billing record; `api_req_deleted` keeps
      // the same payload when a user removes a turn from the task (Cline-style
      // edit-and-retry) — tokens were already consumed by the provider, so we
      // still count them.
      if (msg.say !== "api_req_started" && msg.say !== "api_req_deleted") continue;
      if (typeof msg.text !== "string" || !msg.text.startsWith("{")) continue;

      let payload;
      try { payload = JSON.parse(msg.text); } catch { continue; }
      if (!payload || typeof payload !== "object") continue;

      const ts = Number(msg.ts);
      if (!Number.isFinite(ts) || ts <= 0) continue;

      const dedupKey = `${taskUuid}:${ts}`;
      recordsProcessed++;
      if (seenIds.has(dedupKey)) continue;

      const tokensIn = toNonNegativeInt(payload.tokensIn);
      const tokensOut = toNonNegativeInt(payload.tokensOut);
      const cacheReads = toNonNegativeInt(payload.cacheReads);
      const cacheWrites = toNonNegativeInt(payload.cacheWrites);
      if (tokensIn === 0 && tokensOut === 0 && cacheReads === 0 && cacheWrites === 0) {
        // See the roocode parser: `api_req_started` is written at request
        // START with zero tokens and back-filled in place (same ts) on
        // completion. Marking the placeholder seen would drop the
        // back-filled tokens forever when a sync races an in-flight request.
        continue;
      }

      const tsIso = new Date(ts).toISOString();
      const bucketStart = toUtcHalfHourStart(tsIso);
      if (!bucketStart) continue;

      const delta = {
        input_tokens: tokensIn,
        cached_input_tokens: cacheReads,
        cache_creation_input_tokens: cacheWrites,
        output_tokens: tokensOut,
        reasoning_output_tokens: 0,
        total_tokens: tokensIn + tokensOut + cacheReads + cacheWrites,
        conversation_count: 1,
      };

      const model = normalizeKilocodeProviderToModel(payload.inferenceProvider);
      const bucket = getHourlyBucket(hourlyState, "kilo-code", model, bucketStart);
      addTotals(bucket.totals, delta);
      touchedBuckets.add(bucketKey("kilo-code", model, bucketStart));
      seenIds.add(dedupKey);
      eventsAggregated++;
    }

    fileOffsets[filePath] = { size: stat.size, mtimeMs: stat.mtimeMs, ino: stat.ino };

    if (cb) {
      cb({
        index: fileIdx + 1,
        total: files.length,
        recordsProcessed,
        eventsAggregated,
        bucketsQueued: touchedBuckets.size,
      });
    }
  }

  // Cap seenIds to last 50k to bound cursor state size
  const seenArr = Array.from(seenIds);
  const cappedSeen = seenArr.length > 50_000 ? seenArr.slice(seenArr.length - 50_000) : seenArr;

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.kilocode = { ...kilocodeState, seenIds: cappedSeen, fileOffsets, updatedAt };

  return { recordsProcessed, eventsAggregated, bucketsQueued };
}

module.exports = {
  resolveKilocodeTaskFiles,
  normalizeKilocodeProviderToModel,
  parseKilocodeIncremental,
};
