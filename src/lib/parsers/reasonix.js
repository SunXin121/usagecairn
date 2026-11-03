// reasonix usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { ensureDir } = require("../fs");
const {
  enqueueTouchedBuckets,
  normalizeHourlyState,
  getHourlyBucket,
  addTotals,
  toUtcHalfHourStart,
  bucketKey,
  normalizeModelInput,
  toNonNegativeInt,
  expandHomePath,
} = require("./core");

// Reasonix persists content-free cumulative usage beside each session JSONL.
// Reading only these telemetry sidecars keeps prompts and tool output private.
function reasonixHomeCandidates(env = process.env) {
  if (env.TOKEN_PULSE_REASONIX_HOME) {
    return [expandHomePath(env.TOKEN_PULSE_REASONIX_HOME, env)];
  }
  if (env.REASONIX_STATE_HOME) {
    return [expandHomePath(env.REASONIX_STATE_HOME, env)];
  }
  // Windows installs of Git Bash / MSYS / conda export a HOME of their own
  // (often a POSIX-shaped path), so preferring it silently sends the scan to a
  // directory that does not exist and Reasonix drops out of `status` entirely
  // with no "skipped" line to explain it. Match resolveCopilotDbPaths and take
  // USERPROFILE first on win32.
  const home =
    process.platform === "win32"
      ? env.USERPROFILE || env.HOME || os.homedir()
      : env.HOME || os.homedir();
  const candidates = [path.join(home, ".reasonix")];
  // On Windows the dot-directory is not where the data lives: Reasonix keeps
  // sessions and memory under %APPDATA%\reasonix (no leading dot) and only the
  // cache under %LOCALAPPDATA%. Taking USERPROFILE over a shell HOME was not
  // enough for #641 because ~/.reasonix does not exist on Windows at all, so
  // the provider read as "not installed" and vanished from status without a
  // skipped line. The cache root is deliberately left out.
  if (process.platform === "win32" && env.APPDATA) {
    candidates.push(path.join(env.APPDATA, "reasonix"));
  }
  return candidates;
}

// The home used for "is Reasonix installed" (src/commands/status.js,
// src/commands/init.js): the first candidate that exists, else the first, so
// the caller still has a path to report.
function resolveReasonixHome(env = process.env) {
  const candidates = reasonixHomeCandidates(env);
  return candidates.find((dir) => fssync.existsSync(dir)) || candidates[0];
}

// Depth bound because the scan starts at the Reasonix root rather than at two
// known subdirectories; it is a guard against a pathological tree, not a layout
// assumption. The known layout (projects/<p>/sessions) sits at depth 3.
const REASONIX_SCAN_MAX_DEPTH = 8;

function collectReasonixTelemetryFiles(dir, files, depth = 0) {
  if (depth > REASONIX_SCAN_MAX_DEPTH) return;
  if (!fssync.existsSync(dir)) return;
  let entries;
  try { entries = fssync.readdirSync(dir, { withFileTypes: true }); } catch { return; }
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) collectReasonixTelemetryFiles(full, files, depth + 1);
    else if (entry.isFile() && entry.name.endsWith(".jsonl.telemetry.json")) files.push(full);
  }
}

function resolveReasonixTelemetryFiles(env = process.env) {
  // Recurse from each root instead of from a hardcoded projects/ and sessions/.
  // The #641 reporter supplied a screenshot of the data location, not a
  // directory listing, so the layout under %APPDATA%\reasonix is unconfirmed —
  // and a renamed subdirectory would otherwise cost another release to notice.
  const files = [];
  const seenHomes = new Set();
  for (const home of reasonixHomeCandidates(env)) {
    if (seenHomes.has(home)) continue;
    seenHomes.add(home);
    collectReasonixTelemetryFiles(home, files);
  }
  return Array.from(new Set(files)).sort((a, b) => a.localeCompare(b));
}

function readReasonixSnapshot(filePath) {
  const telemetry = JSON.parse(fssync.readFileSync(filePath, "utf8"));
  const usage = telemetry?.usage;
  if (!usage || typeof usage !== "object") return null;
  const metaPath = filePath.slice(0, -".telemetry.json".length) + ".meta";
  let meta = {};
  try { meta = JSON.parse(fssync.readFileSync(metaPath, "utf8")); } catch {}
  const stat = fssync.statSync(filePath);
  return { usage, meta, stat };
}

function normalizeReasonixTotals(usage) {
  const prompt = toNonNegativeInt(usage.promptTokens);
  const reasoning = toNonNegativeInt(usage.reasoningTokens);
  const completion = toNonNegativeInt(usage.completionTokens);
  const cacheMiss = Math.min(prompt, toNonNegativeInt(usage.cacheMissTokens));
  const cacheHit = Math.min(prompt, toNonNegativeInt(usage.cacheHitTokens));
  const hasCacheMiss = usage.cacheMissTokens != null;
  const uncachedPrompt = hasCacheMiss ? cacheMiss : Math.max(0, prompt - cacheHit);
  const cacheWrite = Math.min(uncachedPrompt, toNonNegativeInt(usage.cacheWriteTokens));
  return {
    input: uncachedPrompt - cacheWrite,
    cacheRead: hasCacheMiss ? prompt - cacheMiss : cacheHit,
    cacheWrite,
    output: Math.max(0, completion - reasoning),
    reasoning,
    requests: toNonNegativeInt(usage.requestCount),
  };
}

function diffReasonixTotals(current, previous = {}) {
  const delta = {};
  for (const key of ["input", "cacheRead", "cacheWrite", "output", "reasoning", "requests"]) {
    delta[key] = Math.max(0, current[key] - toNonNegativeInt(previous[key]));
  }
  return delta;
}

function reasonixTimestamp(snapshot) {
  for (const value of [snapshot.meta.updated_at, snapshot.meta.created_at]) {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  return snapshot.stat.mtimeMs;
}

function normalizeReasonixModel(value) {
  const normalized = normalizeModelInput(value);
  if (!normalized) return "reasonix-unknown";
  const segments = normalized.split("/").filter(Boolean);
  return segments.at(-1) || "reasonix-unknown";
}

function addReasonixDelta(hourlyState, touchedBuckets, snapshot, delta) {
  const bucketStart = toUtcHalfHourStart(new Date(reasonixTimestamp(snapshot)).toISOString());
  if (!bucketStart) return false;
  const model = normalizeReasonixModel(snapshot.meta.model);
  const total = delta.input + delta.cacheRead + delta.cacheWrite + delta.output + delta.reasoning;
  const bucket = getHourlyBucket(hourlyState, "reasonix", model, bucketStart);
  addTotals(bucket.totals, {
    input_tokens: delta.input,
    cached_input_tokens: delta.cacheRead,
    cache_creation_input_tokens: delta.cacheWrite,
    output_tokens: delta.output,
    reasoning_output_tokens: delta.reasoning,
    total_tokens: total,
    conversation_count: delta.requests,
  });
  touchedBuckets.add(bucketKey("reasonix", model, bucketStart));
  return true;
}

async function parseReasonixIncremental({ telemetryFiles, cursors, queuePath, onProgress, env } = {}) {
  await ensureDir(path.dirname(queuePath));
  const state = cursors.reasonix && typeof cursors.reasonix === "object" ? cursors.reasonix : {};
  const sessionTotals = { ...(state.sessionTotals || {}) };
  const files = Array.isArray(telemetryFiles) ? telemetryFiles : resolveReasonixTelemetryFiles(env);
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  let recordsProcessed = 0;
  let eventsAggregated = 0;
  for (const [index, filePath] of files.entries()) {
    let snapshot;
    try { snapshot = readReasonixSnapshot(filePath); } catch { continue; }
    if (!snapshot) continue;
    recordsProcessed++;
    const current = normalizeReasonixTotals(snapshot.usage);
    const delta = diffReasonixTotals(current, sessionTotals[filePath]);
    const tokenDelta = delta.input + delta.cacheRead + delta.cacheWrite + delta.output + delta.reasoning;
    if ((tokenDelta > 0 || delta.requests > 0) &&
        addReasonixDelta(hourlyState, touchedBuckets, snapshot, delta)) eventsAggregated++;
    sessionTotals[filePath] = current;
    onProgress?.({ index: index + 1, total: files.length, recordsProcessed, eventsAggregated, bucketsQueued: touchedBuckets.size });
  }
  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.reasonix = { ...state, sessionTotals, updatedAt };
  return { recordsProcessed, eventsAggregated, bucketsQueued };
}

module.exports = {
  resolveReasonixHome,
  resolveReasonixTelemetryFiles,
  normalizeReasonixModel,
  parseReasonixIncremental,
};
