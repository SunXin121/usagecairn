// roocode usage parser and install discovery.
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

// ─────────────────────────────────────────────────────────────────────────────
// Roo Code (rooveterinaryinc.roo-cline)
//
// Same Cline-derived ui_messages.json format as Kilo Code, but two real
// differences worth noting:
//
//   1. The model name is NOT in the per-turn payload (Roo Code only writes
//      provider via `apiProtocol`). It lives in a sibling
//      `api_conversation_history.json` inside `<environment_details>` blocks:
//
//          <environment_details>
//          <model>claude-3-7-sonnet-20250219</model>
//          </environment_details>
//
//      We read the most recent occurrence — Roo can switch models mid-task,
//      so the last-seen value is the most accurate attribution; if the file
//      or tag is missing we fall back to `protocol:<apiProtocol>` (e.g.
//      `protocol:anthropic`) and finally to "unknown".
//
//   2. Same multi-IDE root scan as Kilo Code (Cursor, Code, CodeBuddy, …) —
//      we reuse resolveKilocodeRoots so both parsers stay in sync when a new
//      VS Code fork ships.
// ─────────────────────────────────────────────────────────────────────────────

function resolveRoocodeTaskFiles(env = process.env) {
  const roots = resolveKilocodeRoots(env);
  const out = [];
  for (const root of roots) {
    const tasksDir = path.join(root, "User", "globalStorage", "rooveterinaryinc.roo-cline", "tasks");
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

// Pull the most recent <model>…</model> from a Roo Code task's
// api_conversation_history.json (each Cline turn appends a fresh
// <environment_details> block). Returns null when the sibling file is
// missing, unreadable, or contains no tag. Bounded to first 1MB to avoid
// pathological history files starving sync.
function readRoocodeTaskModel(uiMessagesPath) {
  const historyPath = path.join(path.dirname(uiMessagesPath), "api_conversation_history.json");
  let raw;
  try { raw = fssync.readFileSync(historyPath, "utf8"); } catch { return null; }
  if (raw.length > 1_048_576) {
    // Naive `slice(raw.length - 1MB)` can split a `<environment_details>`
    // block mid-tag — e.g. the keep window starts at "...<mod" so the
    // regex finds nothing and we fall back to "unknown". Align the cut
    // to the first `<environment_details>` start in the keep window so
    // every retained tag is intact.
    const naive = raw.slice(raw.length - 1_048_576);
    const blockStart = naive.indexOf("<environment_details>");
    raw = blockStart >= 0 ? naive.slice(blockStart) : naive;
  }
  let lastModel = null;
  const re = /<model>\s*([^<\s][^<]*?)\s*<\/model>/g;
  let m;
  while ((m = re.exec(raw)) !== null) {
    const value = m[1].trim();
    if (value) lastModel = value;
  }
  return lastModel;
}

function normalizeRoocodeModel({ explicitModel, apiProtocol }) {
  const trimmed = typeof explicitModel === "string" ? explicitModel.trim() : "";
  if (trimmed) return trimmed;
  if (typeof apiProtocol === "string" && apiProtocol.trim()) {
    const slug = apiProtocol.trim().toLowerCase().replace(/[^a-z0-9._-]/g, "");
    if (slug) return `protocol:${slug}`;
  }
  return "unknown";
}

async function parseRoocodeIncremental({
  taskFiles,
  cursors,
  queuePath,
  onProgress,
  env,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const roocodeState =
    cursors.roocode && typeof cursors.roocode === "object" ? cursors.roocode : {};
  const seenIds = new Set(
    Array.isArray(roocodeState.seenIds) ? roocodeState.seenIds : [],
  );
  const fileOffsets =
    roocodeState.fileOffsets && typeof roocodeState.fileOffsets === "object"
      ? { ...roocodeState.fileOffsets }
      : {};

  const files = Array.isArray(taskFiles)
    ? taskFiles
    : resolveRoocodeTaskFiles(env || process.env);

  if (files.length === 0) {
    cursors.roocode = {
      ...roocodeState,
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

    // Read sibling history once per task — model can change mid-task but is
    // stable enough at this granularity that re-reading on every entry would
    // just burn IO. Task attribution at the bucket layer is hourly anyway.
    const taskModel = readRoocodeTaskModel(filePath);

    for (const msg of data) {
      if (!msg || typeof msg !== "object") continue;
      // Like Kilo Code, accept both api_req_started (live) and api_req_deleted
      // (user-removed turn whose tokens were already consumed).
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
        // Cline-family extensions write `api_req_started` at request START
        // (zero tokens) and back-fill the SAME message in place (same ts)
        // once the request completes. Marking the zero placeholder as seen
        // would skip the back-filled tokens forever — a sync racing an
        // in-flight request silently under-counted that turn. Leave it
        // unseen; the file-level mtime gate re-evaluates it when the task
        // file is rewritten.
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

      const model = normalizeRoocodeModel({
        explicitModel: taskModel,
        apiProtocol: payload.apiProtocol,
      });
      const bucket = getHourlyBucket(hourlyState, "roocode", model, bucketStart);
      addTotals(bucket.totals, delta);
      touchedBuckets.add(bucketKey("roocode", model, bucketStart));
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

  const seenArr = Array.from(seenIds);
  const cappedSeen = seenArr.length > 50_000 ? seenArr.slice(seenArr.length - 50_000) : seenArr;

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.roocode = { ...roocodeState, seenIds: cappedSeen, fileOffsets, updatedAt };

  return { recordsProcessed, eventsAggregated, bucketsQueued };
}

module.exports = {
  resolveRoocodeTaskFiles,
  readRoocodeTaskModel,
  normalizeRoocodeModel,
  parseRoocodeIncremental,
};
