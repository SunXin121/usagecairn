// kimi usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const readline = require("node:readline");
const { ensureDir } = require("../fs");
const {
  enqueueTouchedBuckets,
  normalizeHourlyState,
  getHourlyBucket,
  addTotals,
  toUtcHalfHourStart,
  bucketKey,
  toNonNegativeInt,
  pickWin32ProviderPath,
} = require("./core");

// ─────────────────────────────────────────────────────────────────────────────
// Kimi — passive JSONL reader (~/.kimi/sessions/**/wire.jsonl)
// No hook installation needed; Kimi writes wire.jsonl automatically.

function resolveKimiDefaultModel(env = process.env) {
  const fallback = "kimi-for-coding";
  try {
    const home = env.HOME || os.homedir();
    const cfgPath = path.join(env.KIMI_HOME || path.join(home, ".kimi"), "config.toml");
    const raw = fssync.readFileSync(cfgPath, "utf8");
    const defaultMatch = raw.match(/^\s*default_model\s*=\s*"([^"]+)"/m);
    if (!defaultMatch) return fallback;
    const sectionKey = defaultMatch[1];
    const escaped = sectionKey.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const sectionRe = new RegExp(
      `\\[models\\."${escaped}"\\]([\\s\\S]*?)(?:\\n\\[|$)`,
    );
    const section = raw.match(sectionRe);
    if (section) {
      const modelMatch = section[1].match(/^\s*model\s*=\s*"([^"]+)"/m);
      if (modelMatch && modelMatch[1]) return modelMatch[1];
    }
    if (sectionKey.includes("/")) return sectionKey.split("/").pop();
    return sectionKey || fallback;
  } catch {
    return fallback;
  }
}

// ─────────────────────────────────────────────────────────────────────────────

function resolveKimiHome(env = process.env) {
  const home = os.homedir();
  const explicit = typeof env?.KIMI_HOME === "string" ? env.KIMI_HOME.trim() : "";
  if (explicit) return path.resolve(explicit);
  if (process.platform === "win32") {
    return pickWin32ProviderPath({
      env,
      nativeValue: path.join(home, ".kimi"),
      wslProviderDir: ".kimi",
    });
  }
  return path.join(home, ".kimi");
}

function resolveKimiWireFiles(env = process.env) {
  const kimiHome = resolveKimiHome(env);
  if (!kimiHome) return [];
  const sessionsDir = path.join(kimiHome, "sessions");
  if (!fssync.existsSync(sessionsDir)) return [];
  const files = [];
  try {
    for (const ws of fssync.readdirSync(sessionsDir)) {
      const wsDir = path.join(sessionsDir, ws);
      let wsStat;
      try { wsStat = fssync.statSync(wsDir); } catch { continue; }
      if (!wsStat.isDirectory()) continue;
      for (const sess of fssync.readdirSync(wsDir)) {
        const wireFile = path.join(wsDir, sess, "wire.jsonl");
        if (fssync.existsSync(wireFile)) files.push(wireFile);
      }
    }
  } catch { /* ignore */ }
  return files;
}

async function parseKimiIncremental({ wireFiles, cursors, queuePath, onProgress, env, model } = {}) {
  await ensureDir(path.dirname(queuePath));
  const kimiState = cursors.kimi && typeof cursors.kimi === "object" ? cursors.kimi : {};
  const seenIds = new Set(Array.isArray(kimiState.seenIds) ? kimiState.seenIds : []);
  const fileOffsets =
    kimiState.fileOffsets && typeof kimiState.fileOffsets === "object"
      ? { ...kimiState.fileOffsets }
      : {};

  const files = Array.isArray(wireFiles)
    ? wireFiles
    : resolveKimiWireFiles(env || process.env);
  const kimiModel = model || resolveKimiDefaultModel(env || process.env);
  if (files.length === 0) {
    cursors.kimi = { ...kimiState, seenIds: Array.from(seenIds), fileOffsets, updatedAt: new Date().toISOString() };
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;
  let recordsProcessed = 0;
  let eventsAggregated = 0;

  for (let fileIdx = 0; fileIdx < files.length; fileIdx++) {
    const filePath = files[fileIdx];
    let stat;
    try { stat = fssync.statSync(filePath); } catch { continue; }

    const prevEntry = fileOffsets[filePath] || {};
    const prevSize = Number(prevEntry.size) || 0;
    const prevIno = prevEntry.ino;
    const inodeChanged = typeof prevIno === "number" && prevIno !== stat.ino;
    const startOffset = stat.size < prevSize || inodeChanged ? 0 : prevSize;
    if (stat.size <= startOffset) continue;

    let stream;
    try {
      stream = fssync.createReadStream(filePath, { encoding: "utf8", start: startOffset });
    } catch { continue; }
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

    for await (const line of rl) {
      if (!line || !line.trim()) continue;
      let entry;
      try { entry = JSON.parse(line); } catch { continue; }

      const msg = entry.message;
      if (!msg || msg.type !== "StatusUpdate") continue;

      const payload = msg.payload;
      if (!payload) continue;
      const { token_usage, message_id } = payload;
      if (!token_usage || !message_id) continue;
      if (seenIds.has(message_id)) continue;

      recordsProcessed++;

      const input = toNonNegativeInt(token_usage.input_other);
      const output = toNonNegativeInt(token_usage.output);
      const cacheRead = toNonNegativeInt(token_usage.input_cache_read);
      const cacheCreation = toNonNegativeInt(token_usage.input_cache_creation);
      if (input === 0 && output === 0 && cacheRead === 0 && cacheCreation === 0) {
        seenIds.add(message_id);
        continue;
      }

      const epochSec = entry.timestamp ?? payload.timestamp;
      if (epochSec == null || !Number.isFinite(Number(epochSec))) continue;
      const tsIso = new Date(Number(epochSec) * 1000).toISOString();
      const bucketStart = toUtcHalfHourStart(tsIso);
      if (!bucketStart) continue;

      const delta = {
        input_tokens: input,
        cached_input_tokens: cacheRead,
        cache_creation_input_tokens: cacheCreation,
        output_tokens: output,
        reasoning_output_tokens: 0,
        total_tokens: input + output + cacheRead + cacheCreation,
        conversation_count: 1,
      };

      const bucket = getHourlyBucket(hourlyState, "kimi", kimiModel, bucketStart);
      addTotals(bucket.totals, delta);
      touchedBuckets.add(bucketKey("kimi", kimiModel, bucketStart));
      seenIds.add(message_id);
      eventsAggregated++;

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

    let postStat = stat;
    try { postStat = fssync.statSync(filePath); } catch {}
    fileOffsets[filePath] = { size: postStat.size, mtimeMs: postStat.mtimeMs, ino: postStat.ino };
  }

  // Cap seenIds to last 10k to bound cursor state size
  const seenArr = Array.from(seenIds);
  const cappedSeen = seenArr.length > 10_000 ? seenArr.slice(seenArr.length - 10_000) : seenArr;

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.kimi = { ...kimiState, seenIds: cappedSeen, fileOffsets, updatedAt };

  return { recordsProcessed, eventsAggregated, bucketsQueued };
}

// ─────────────────────────────────────────────────────────────────────────────
// Kimi Code (official @moonshot-ai/kimi-code) — passive JSONL reader.
//
// Distinct from the legacy community `kimi-cli` above (Python, ~/.kimi). The
// official single-binary product stores under ~/.kimi-code/ with a different
// session layout and wire protocol:
//
//   ~/.kimi-code/sessions/<wd_dir_hash>/<session_id>/agents/<name>/wire.jsonl
//
// proto 1.x events are namespaced and carry `type` at the top level. Per-step
// token usage rides on a `step.end` loop event (wrapped in
// `context.append_loop_event`) with an Anthropic-style usage object:
//
//   {"type":"context.append_loop_event",
//    "event":{"type":"step.end","uuid":"<stepUuid>","turnId":"..","step":N,
//      "usage":{"input_tokens":N,"output_tokens":N,
//               "cache_read_input_tokens":N,"cache_creation_input_tokens":N}},
//    "time":<epoch_ms>}
//
// Model comes from the per-session `config.update` event's `modelAlias`
// (e.g. "kimi-code/kimi-k2.6" -> "kimi-k2.6"). Emitted under source "kimi" so
// new + legacy sessions aggregate together. Independent cursor (cursors.kimiCode)
// keeps state from colliding with the legacy reader's cursors.kimi.
function resolveKimiCodeHome(env = process.env) {
  const home = os.homedir();
  const explicit = typeof env?.KIMI_CODE_HOME === "string" ? env.KIMI_CODE_HOME.trim() : "";
  if (explicit) return path.resolve(explicit);
  if (process.platform === "win32") {
    return pickWin32ProviderPath({
      env,
      nativeValue: path.join(home, ".kimi-code"),
      wslProviderDir: ".kimi-code",
    });
  }
  return path.join(home, ".kimi-code");
}

function resolveKimiCodeWireFiles(env = process.env) {
  const kimiHome = resolveKimiCodeHome(env);
  if (!kimiHome) return [];
  const sessionsDir = path.join(kimiHome, "sessions");
  if (!fssync.existsSync(sessionsDir)) return [];
  const files = [];
  const walk = (dir, depth) => {
    if (depth > 5) return;
    let entries;
    try { entries = fssync.readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const ent of entries) {
      const full = path.join(dir, ent.name);
      if (ent.isDirectory()) walk(full, depth + 1);
      else if (ent.name === "wire.jsonl") files.push(full);
    }
  };
  walk(sessionsDir, 0);
  return files;
}

function resolveKimiCodeDefaultModel(env = process.env) {
  const fallback = "kimi-for-coding";
  try {
    const kimiHome = resolveKimiCodeHome(env);
    if (!kimiHome) return fallback;
    const cfgPath = path.join(kimiHome, "config.toml");
    const raw = fssync.readFileSync(cfgPath, "utf8");
    const m = raw.match(/^\s*default_model\s*=\s*"([^"]+)"/m);
    if (!m) return fallback;
    return m[1].includes("/") ? m[1].split("/").pop() : m[1] || fallback;
  } catch {
    return fallback;
  }
}

function kimiCodeModelAlias(value) {
  if (typeof value !== "string" || !value) return null;
  return value.includes("/") ? value.split("/").pop() : value;
}

async function parseKimiCodeIncremental({ wireFiles, cursors, queuePath, onProgress, env, model } = {}) {
  await ensureDir(path.dirname(queuePath));
  const state = cursors.kimiCode && typeof cursors.kimiCode === "object" ? cursors.kimiCode : {};
  const seenIds = new Set(Array.isArray(state.seenIds) ? state.seenIds : []);
  const fileOffsets =
    state.fileOffsets && typeof state.fileOffsets === "object" ? { ...state.fileOffsets } : {};

  const files = Array.isArray(wireFiles) ? wireFiles : resolveKimiCodeWireFiles(env || process.env);
  const fallbackModel = model || resolveKimiCodeDefaultModel(env || process.env);
  if (files.length === 0) {
    cursors.kimiCode = { ...state, seenIds: Array.from(seenIds), fileOffsets, updatedAt: new Date().toISOString() };
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;
  let recordsProcessed = 0;
  let eventsAggregated = 0;

  for (let fileIdx = 0; fileIdx < files.length; fileIdx++) {
    const filePath = files[fileIdx];
    let stat;
    try { stat = fssync.statSync(filePath); } catch { continue; }

    const prevEntry = fileOffsets[filePath] || {};
    const prevSize = Number(prevEntry.size) || 0;
    const prevIno = prevEntry.ino;
    const inodeChanged = typeof prevIno === "number" && prevIno !== stat.ino;
    const startOffset = stat.size < prevSize || inodeChanged ? 0 : prevSize;
    // Persist the latest config/profile/request model so an incremental read
    // starting after that declaration keeps the same attribution.
    let fileModel = (typeof prevEntry.model === "string" && prevEntry.model) || fallbackModel;
    if (stat.size <= startOffset) continue;

    let stream;
    try {
      stream = fssync.createReadStream(filePath, { encoding: "utf8", start: startOffset });
    } catch { continue; }
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

    for await (const line of rl) {
      if (!line || !line.trim()) continue;
      let entry;
      try { entry = JSON.parse(line); } catch { continue; }

      // Kimi Code 2.x can bind or reroute the model without config.update.
      // These records precede step.end and carry the model actually requested.
      if (["config.update", "profile.bind", "llm.request"].includes(entry.type)) {
        const alias = kimiCodeModelAlias(entry.modelAlias || entry.model);
        if (alias) fileModel = alias;
        continue;
      }

      const evt =
        entry.type === "context.append_loop_event" && entry.event && typeof entry.event === "object"
          ? entry.event
          : entry;
      if (!evt || evt.type !== "step.end") continue;
      const usage = evt.usage;
      if (!usage || typeof usage !== "object") continue;
      const id = evt.uuid;
      if (!id || seenIds.has(id)) continue;

      recordsProcessed++;

      // kimi-code's wire usage comes in two shapes across versions:
      //  - camelCase (proto 0.6.0+, current): { inputOther, inputCacheRead,
      //    inputCacheCreation, output } where inputOther is already fresh
      //    (non-cached) input — this is `response.usage` straight from the LLM
      //    adapter (verified in @moonshot-ai/kimi-code 0.6.0/0.7.0/0.9.0).
      //  - Anthropic-style (older): { input_tokens, output_tokens,
      //    cache_read_input_tokens, cache_creation_input_tokens }. OpenAI-compat
      //    models fold cached reads into input_tokens and expose them via
      //    input_tokens_details.cached_tokens — subtract so we never double-count.
      // step.end and usage.record carry the SAME per-step usage object, so we
      // read only step.end here (reading both would double-count ~2x).
      let cacheCreation;
      let cacheRead;
      let input;
      let output;
      if (usage.inputOther != null) {
        input = toNonNegativeInt(usage.inputOther);
        cacheRead = toNonNegativeInt(usage.inputCacheRead);
        cacheCreation = toNonNegativeInt(usage.inputCacheCreation);
        output = toNonNegativeInt(usage.output);
      } else {
        cacheCreation = toNonNegativeInt(usage.cache_creation_input_tokens);
        if (usage.cache_read_input_tokens != null) {
          cacheRead = toNonNegativeInt(usage.cache_read_input_tokens);
          input = toNonNegativeInt(usage.input_tokens);
        } else {
          const details =
            usage.input_tokens_details && typeof usage.input_tokens_details === "object"
              ? usage.input_tokens_details
              : null;
          const cached = toNonNegativeInt(details ? details.cached_tokens : 0);
          cacheRead = cached;
          input = Math.max(0, toNonNegativeInt(usage.input_tokens) - cached);
        }
        output = toNonNegativeInt(usage.output_tokens);
      }
      if (input === 0 && output === 0 && cacheRead === 0 && cacheCreation === 0) {
        seenIds.add(id);
        continue;
      }

      const ms = entry.time ?? evt.time;
      if (ms == null || !Number.isFinite(Number(ms))) continue;
      const tsIso = new Date(Number(ms)).toISOString();
      const bucketStart = toUtcHalfHourStart(tsIso);
      if (!bucketStart) continue;

      const delta = {
        input_tokens: input,
        cached_input_tokens: cacheRead,
        cache_creation_input_tokens: cacheCreation,
        output_tokens: output,
        reasoning_output_tokens: 0,
        total_tokens: input + output + cacheRead + cacheCreation,
        conversation_count: 1,
      };

      const bucket = getHourlyBucket(hourlyState, "kimi", fileModel, bucketStart);
      addTotals(bucket.totals, delta);
      touchedBuckets.add(bucketKey("kimi", fileModel, bucketStart));
      seenIds.add(id);
      eventsAggregated++;

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

    let postStat = stat;
    try { postStat = fssync.statSync(filePath); } catch {}
    fileOffsets[filePath] = { size: postStat.size, mtimeMs: postStat.mtimeMs, ino: postStat.ino, model: fileModel };
  }

  const seenArr = Array.from(seenIds);
  const cappedSeen = seenArr.length > 10_000 ? seenArr.slice(seenArr.length - 10_000) : seenArr;

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.kimiCode = { ...state, seenIds: cappedSeen, fileOffsets, updatedAt };

  return { recordsProcessed, eventsAggregated, bucketsQueued };
}

module.exports = {
  resolveKimiDefaultModel,
  resolveKimiHome,
  resolveKimiWireFiles,
  parseKimiIncremental,
  resolveKimiCodeHome,
  resolveKimiCodeWireFiles,
  resolveKimiCodeDefaultModel,
  parseKimiCodeIncremental,
};
