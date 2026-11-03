// cline usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { ensureDir } = require("../fs");
const wsl = require("../wsl-probe");
const {
  DEFAULT_MODEL,
  enqueueTouchedBuckets,
  normalizeHourlyState,
  getHourlyBucket,
  addTotals,
  toUtcHalfHourStart,
  bucketKey,
  parseBucketKey,
  toNonNegativeInt,
  expandHomePath,
} = require("./core");

// Cost values are fractional dollars, so unlike toNonNegativeInt this keeps
// the decimals instead of flooring to whole units.
function toNonNegativeNumber(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n < 0) return 0;
  return n;
}

// ─────────────────────────────────────────────────────────────────────────────
// Cline (Cline CLI v3 / desktop app — ~/.cline)
//
// Cline outgrew its VS Code extension home. The standalone CLI and desktop app
// keep sessions in Cline's own data dir instead of the extension's
// `globalStorage/saoudrizwan.claude-dev/tasks/<id>/ui_messages.json` layout
// that Roo Code and Kilo Code still fork and that we only read from IDE
// globalStorage:
//
//   <clineDir>/data/sessions/<session_id>/<session_id>.json           metadata
//   <clineDir>/data/sessions/<session_id>/<session_id>.messages.json  turns
//
// The messages file is `{ version, updated_at, agent, sessionId, origin,
// messages[], system_prompt }`. Each assistant turn carries:
//
//   ts:        epoch ms
//   modelInfo: { id, provider, family? }
//   metrics:   { inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens,
//                reasoningTokenCount?, cost? }
//
// TOKEN SEMANTICS — why no field can be copied 1:1. Cline fills `metrics` with
// `usageDelta()`, which diffs AI SDK LanguageModelUsage totals, and those totals
// are INCLUSIVE:
//
//   inputTokens  = noCache + cacheRead + cacheWrite
//   outputTokens = text    + reasoning
//
// Cline's own legacy adapter spells out the input side —
// `tokensIn: inputTokens - cacheRead - cacheWrite` — so storing inputTokens as
// input_tokens while also storing cacheReadTokens as cached_input_tokens would
// bill the cached prefix twice (the Codex/every-code inflation CLAUDE.md warns
// about). Both cache buckets are subtracted here, and because reasoning sits
// inside outputTokens it is reported as a SUBSET: `pricing/index.js` lists
// `cline` in reasoningIncludedInOutput so it is never billed a second time.
//
// COUNTING MODEL. `metrics` is attached once, when the model call finishes
// (`usageDelta(usageBeforeModel, this.state.usage)` runs after the call), so a
// turn is either metrics-less — not counted, picked up by a later sync — or
// final. We still keep last-emitted totals per message and emit the positive
// difference: re-reading an unchanged file emits nothing, and if Cline ever
// back-fills a larger total onto a message we already counted, only the
// increase is added. The per-file mtime gate keeps the common re-read free.
// Ledgers are kept per transcript so a deleted teammate file can be pruned
// without affecting the rest of the session history.
// ────────────────────────────────────────────────────────────────────────────

const CLINE_MESSAGES_SUFFIX = ".messages.json";

// Cline's own resolution chain, each step overridable ahead of it so a snapshot
// can be pinned without touching the tool's environment:
//   CLINE_DIR              -> <clineDir>     (default ~/.cline)
//   CLINE_DATA_DIR         -> <dataDir>      (default <clineDir>/data)
//   CLINE_SESSION_DATA_DIR -> <sessionsDir>  (default <dataDir>/sessions)
function resolveClineSessionsDir(env = process.env, deps = {}) {
  const expand = deps.expandHomePath || expandHomePath;
  const nonEmpty = (value) => (typeof value === "string" && value.trim() ? value.trim() : null);

  const sessionsOverride =
    nonEmpty(env.TOKEN_PULSE_CLINE_SESSIONS_DIR) || nonEmpty(env.CLINE_SESSION_DATA_DIR);
  if (sessionsOverride) return expand(sessionsOverride, env);

  const dataDir = nonEmpty(env.TOKEN_PULSE_CLINE_DATA_DIR) || nonEmpty(env.CLINE_DATA_DIR);
  if (dataDir) return path.join(expand(dataDir, env), "sessions");

  const home = env.HOME || os.homedir();
  const clineDir = nonEmpty(env.TOKEN_PULSE_CLINE_HOME) || nonEmpty(env.CLINE_DIR);
  return path.join(clineDir ? expand(clineDir, env) : path.join(home, ".cline"), "data", "sessions");
}

// Any explicit path override means the user pointed us at one install: skip the
// WSL probe entirely rather than unioning in a distro copy they did not ask for.
function clineSessionsDirIsOverridden(env = process.env) {
  return [
    env.TOKEN_PULSE_CLINE_SESSIONS_DIR,
    env.TOKEN_PULSE_CLINE_DATA_DIR,
    env.TOKEN_PULSE_CLINE_HOME,
    env.CLINE_SESSION_DATA_DIR,
    env.CLINE_DATA_DIR,
    env.CLINE_DIR,
  ].some((value) => typeof value === "string" && value.trim());
}

// Scan native Windows and WSL installs according to the selected WSL mode.
function resolveClineSessionsDirs(env = process.env, deps = {}) {
  const platform = deps.platform || process.platform;
  const nativeDir = deps.nativeDir || resolveClineSessionsDir(env, deps);
  const single = (value) => (value ? [value] : []);
  if (clineSessionsDirIsOverridden(env) || platform !== "win32") return single(nativeDir);

  const existsSync = deps.existsSync || fssync.existsSync;
  let nativeValue = null;
  try {
    if (nativeDir && existsSync(nativeDir)) nativeValue = nativeDir;
  } catch (_error) {
    // A probe failure just means we cannot vouch for the native install.
  }

  const discoverWslHome = deps.discoverWslHome || wsl.discoverWslHome;
  const wslValue = wsl.shouldProbeWsl(env)
    ? discoverWslHome(".cline/data/sessions", { ...deps, env })
    : null;
  const resolved = wsl.resolveAllWin32Paths({ nativeValue, wslValue, env, platform });
  return [...new Set([resolved.native, resolved.wsl].filter(Boolean))];
}

function listClineSessionFiles(sessionsDir) {
  const result = scanClineSessionFiles(sessionsDir);
  if (result.error) throw result.error;
  return result.files;
}

// A root is complete only when every directory read needed to enumerate it
// succeeds. A missing root or session directory is an incomplete scan: it may
// be a transient filesystem or WSL gap, so callers must retain its ledger.
function scanClineSessionFiles(sessionsDir) {
  const out = [];
  if (typeof sessionsDir !== "string" || !sessionsDir) {
    return { files: out, complete: true, error: null };
  }
  let entries;
  try {
    entries = fssync.readdirSync(sessionsDir, { withFileTypes: true });
  } catch (error) {
    if (error.code === "ENOENT" || error.code === "ENOTDIR") {
      return { files: out, complete: false, error: null };
    }
    return { files: out, complete: false, error };
  }
  let complete = true;
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const sessionDir = path.join(sessionsDir, entry.name);
    let artifacts;
    try {
      artifacts = fssync.readdirSync(sessionDir);
    } catch (error) {
      if (error.code === "ENOENT" || error.code === "ENOTDIR") {
        complete = false;
        continue;
      }
      return { files: out, complete: false, error };
    }
    const transcripts = artifacts.filter((name) => name.endsWith(CLINE_MESSAGES_SUFFIX)).sort();
    if (transcripts.length === 0) continue;
    const metaName = `${entry.name}.json`;
    for (const messagesName of transcripts) {
      out.push({
        filePath: path.join(sessionDir, messagesName),
        sessionMetaPath: artifacts.includes(metaName) ? path.join(sessionDir, metaName) : null,
        sessionId: entry.name,
      });
    }
  }
  return { files: out, complete, error: null };
}

// Every `<home>/data/sessions/*/<session>.messages.json` transcript across the
// installs that own a Cline data dir.
function resolveClineSessionFiles(env = process.env, deps = {}) {
  const result = resolveClineSessionFilesWithStatus(env, deps);
  if (result.errors.length > 0) throw result.errors[0].error;
  return result.files;
}

function resolveClineSessionFilesWithStatus(env = process.env, deps = {}) {
  const out = [];
  const seen = new Set();
  const completedRoots = [];
  const errors = [];
  for (const sessionsDir of resolveClineSessionsDirs(env, deps)) {
    const result = scanClineSessionFiles(sessionsDir);
    for (const entry of result.files) {
      if (seen.has(entry.filePath)) continue;
      seen.add(entry.filePath);
      out.push(entry);
    }
    if (result.error) errors.push({ root: sessionsDir, error: result.error });
    else if (result.complete) completedRoots.push(sessionsDir);
  }
  out.sort((left, right) => left.filePath.localeCompare(right.filePath));
  return { files: out, completedRoots, errors };
}

// The session sidecar names the model the session started on. It is only a
// fallback: a turn's own `modelInfo.id` wins because Cline can switch models
// mid-session. Imported sessions also retain the source transcript's usage;
// the import timestamp lets us leave those already-counted turns to their
// original provider parser.
function readClineSessionMetadata(metaPath) {
  if (typeof metaPath !== "string" || !metaPath) return null;
  let parsed;
  try {
    parsed = JSON.parse(fssync.readFileSync(metaPath, "utf8"));
  } catch (_error) {
    return null;
  }
  const model = parsed && typeof parsed.model === "string" ? parsed.model.trim() : "";
  const importedAt = parsed?.metadata?.importedFrom?.importedAt;
  const importedAtMs = typeof importedAt === "string" ? Date.parse(importedAt) : NaN;
  return { model: model || null, importedAtMs: Number.isFinite(importedAtMs) ? importedAtMs : null };
}

function readClineSessionModel(metaPath) {
  return readClineSessionMetadata(metaPath)?.model || null;
}

function normalizeClineModel({ modelInfo, fallbackModel }) {
  const id = modelInfo && typeof modelInfo.id === "string" ? modelInfo.id.trim() : "";
  if (id) return id;
  const fallback = typeof fallbackModel === "string" ? fallbackModel.trim() : "";
  if (fallback) return fallback;
  // Mirrors Roo Code's `protocol:<x>`: surface the provider rather than a bare
  // "unknown" so the Model column does not imply a model id we never saw.
  const provider =
    modelInfo && typeof modelInfo.provider === "string"
      ? modelInfo.provider.trim().toLowerCase().replace(/[^a-z0-9._-]/g, "")
      : "";
  return provider ? `provider:${provider}` : DEFAULT_MODEL;
}

function clineMessageKey(message, index) {
  const id = message && typeof message.id === "string" ? message.id.trim() : "";
  // `id` is stable across in-place rewrites; ts is the fallback for a turn that
  // has not been assigned one.
  const timestamp = Number(message?.ts);
  return id || `ts:${Number.isFinite(timestamp) ? timestamp : 0}:${index}`;
}

async function parseClineIncremental({
  sessionFiles,
  scanCompleteRoots,
  cursors,
  queuePath,
  onProgress,
  env,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const clineState = cursors.cline && typeof cursors.cline === "object" ? { ...cursors.cline } : {};
  const legacyMessageTotals =
    clineState.messageTotals && typeof clineState.messageTotals === "object"
      ? { ...clineState.messageTotals }
      : {};
  const messageTotalsByFile =
    clineState.messageTotalsByFile && typeof clineState.messageTotalsByFile === "object"
      ? { ...clineState.messageTotalsByFile }
      : {};
  const fileOffsets =
    clineState.fileOffsets && typeof clineState.fileOffsets === "object"
      ? { ...clineState.fileOffsets }
      : {};

  let files;
  let discoveredRoots = null;
  if (Array.isArray(sessionFiles)) {
    files = sessionFiles;
  } else {
    const scan = resolveClineSessionFilesWithStatus(env || process.env);
    files = scan.files;
    discoveredRoots = scan.completedRoots;
  }
  // Only files with old offsets were counted before teammate support. Migrate
  // them before the unchanged-file gate, including the old renamed-root fallback.
  const legacyFilesBySession = new Map();
  for (const filePath of Object.keys(fileOffsets)) {
    const sessionId = path.basename(path.dirname(filePath));
    legacyFilesBySession.set(sessionId, filePath);
  }
  for (const [legacyKey, totals] of Object.entries(legacyMessageTotals)) {
    const separator = legacyKey.indexOf(":");
    const filePath = legacyFilesBySession.get(legacyKey.slice(0, separator));
    if (!filePath || clineState.messageTotalsByFile?.[filePath]) continue;
    const ledger = messageTotalsByFile[filePath] ||= Object.create(null);
    ledger[legacyKey.slice(separator + 1)] = totals;
  }
  delete clineState.messageTotals;

  const activeFilePaths = new Set(files.map((entry) => entry.filePath));
  const completedRoots = Array.isArray(scanCompleteRoots)
    ? new Set(scanCompleteRoots)
    : discoveredRoots
      ? new Set(discoveredRoots)
      : null;
  for (const filePath of new Set([...Object.keys(fileOffsets), ...Object.keys(messageTotalsByFile)])) {
    if (activeFilePaths.has(filePath)) continue;
    if (
      completedRoots &&
      !completedRoots.has(path.dirname(path.dirname(filePath)))
    ) {
      continue;
    }
    try {
      // A failed directory scan must not discard dedup state for existing files.
      fssync.statSync(filePath);
    } catch (error) {
      if (error.code !== "ENOENT" && error.code !== "ENOTDIR") continue;
      delete fileOffsets[filePath];
      delete messageTotalsByFile[filePath];
    }
  }

  if (files.length === 0) {
    cursors.cline = {
      ...clineState,
      messageTotalsByFile,
      fileOffsets,
      updatedAt: new Date().toISOString(),
    };
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  const hourlyState = normalizeHourlyState(cursors?.hourly);
  // Enqueue also updates sibling buckets' alignment and queued keys. Isolate
  // Cline buckets until the append succeeds, keeping other sources untouched.
  for (const [key, bucket] of Object.entries(hourlyState.buckets)) {
    if (parseBucketKey(key).source === "cline" && bucket) {
      hourlyState.buckets[key] = { ...bucket, totals: { ...bucket.totals } };
    }
  }
  hourlyState.groupQueued = { ...hourlyState.groupQueued };
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;
  let recordsProcessed = 0;
  let eventsAggregated = 0;

  for (let fileIdx = 0; fileIdx < files.length; fileIdx++) {
    const entry = files[fileIdx];
    const { filePath } = entry;
    try {
      let stat;
      let raw;
      let fd;
      try {
        // Check and read the same open file even if Cline replaces its path.
        fd = fssync.openSync(filePath, "r");
        stat = fssync.fstatSync(fd, { bigint: true });
        if (!stat.isFile()) continue;
        const prevEntry = fileOffsets[filePath];
        if (
          prevEntry &&
          prevEntry.size === stat.size.toString() &&
          prevEntry.mtimeNs === stat.mtimeNs.toString() &&
          prevEntry.dev === stat.dev.toString() &&
          prevEntry.ino === stat.ino.toString()
        ) {
          continue;
        }
        raw = fssync.readFileSync(fd, "utf8");
      } catch (_error) {
        continue;
      } finally {
        if (fd !== undefined) fssync.closeSync(fd);
      }
      let data;
      try {
        data = JSON.parse(raw);
      } catch (_error) {
        continue;
      }
      const messages = Array.isArray(data)
        ? data
        : Array.isArray(data?.messages)
          ? data.messages
          : null;
      if (!messages) continue;

      const messageTotals = Object.assign(Object.create(null), messageTotalsByFile[filePath]);
      messageTotalsByFile[filePath] = messageTotals;
      const sessionMetadata = readClineSessionMetadata(entry.sessionMetaPath);
      const fallbackModel = sessionMetadata?.model || null;
      const importedAtMs = sessionMetadata?.importedAtMs ?? null;

      for (let msgIdx = 0; msgIdx < messages.length; msgIdx++) {
        const msg = messages[msgIdx];
        if (!msg || typeof msg !== "object") continue;
        if (msg.role !== "assistant") continue;
        const metrics = msg.metrics;
        if (!metrics || typeof metrics !== "object") continue;

        const ts = Number(msg.ts);
        if (!Number.isFinite(ts) || ts <= 0) continue;
        if (importedAtMs !== null && ts <= importedAtMs) continue;

        // Cline's `inputTokens` already contains both cache buckets, so only the
        // non-cached remainder is billable input. See the header comment.
        const cacheRead = toNonNegativeInt(metrics.cacheReadTokens);
        const cacheWrite = toNonNegativeInt(metrics.cacheWriteTokens);
        const inclusiveInput = toNonNegativeInt(metrics.inputTokens);
        const inputTokens = Math.max(0, inclusiveInput - cacheRead - cacheWrite);
        // Reasoning is a subset of outputTokens; it is reported, never added to
        // total_tokens and never billed on top of output.
        const outputTokens = toNonNegativeInt(metrics.outputTokens);
        const reasoningTokens = toNonNegativeInt(metrics.reasoningTokenCount);
        const cost = toNonNegativeNumber(metrics.cost);
        const totalTokens = inputTokens + cacheRead + cacheWrite + outputTokens;

        recordsProcessed++;

        const key = clineMessageKey(msg, msgIdx);
        const timestampKey = `ts:${Number.isFinite(ts) ? ts : 0}:${msgIdx}`;
        const legacyTimestampKey = `ts:${Number.isFinite(ts) ? ts : 0}`;
        const previous =
          messageTotals[key] ?? messageTotals[timestampKey] ?? messageTotals[legacyTimestampKey];
        if (previous !== undefined) {
          if (messageTotals[key] === undefined) messageTotals[key] = previous;
          if (key !== timestampKey) delete messageTotals[timestampKey];
          if (key !== legacyTimestampKey) delete messageTotals[legacyTimestampKey];
        }
        // A turn with no usage yet is left unrecorded so a later sync counts it
        // in full rather than latching the placeholder.
        if (totalTokens === 0 && reasoningTokens === 0 && cost === 0) continue;

        const deltaInput = Math.max(0, inputTokens - (Number(previous?.input) || 0));
        const deltaCached = Math.max(0, cacheRead - (Number(previous?.cached_input) || 0));
        const deltaCreation = Math.max(0, cacheWrite - (Number(previous?.cache_creation) || 0));
        const deltaOutput = Math.max(0, outputTokens - (Number(previous?.output) || 0));
        const deltaReasoning = Math.max(0, reasoningTokens - (Number(previous?.reasoning) || 0));
        const deltaCost = Math.max(0, cost - (Number(previous?.cost) || 0));
        const deltaTotal = deltaInput + deltaCached + deltaCreation + deltaOutput;
        if (deltaTotal === 0 && deltaReasoning === 0 && deltaCost === 0) continue;

        const bucketStart = toUtcHalfHourStart(new Date(ts).toISOString());
        if (!bucketStart) continue;

        const model = normalizeClineModel({ modelInfo: msg.modelInfo, fallbackModel });
        const bucket = getHourlyBucket(hourlyState, "cline", model, bucketStart);
        addTotals(bucket.totals, {
          input_tokens: deltaInput,
          cached_input_tokens: deltaCached,
          cache_creation_input_tokens: deltaCreation,
          output_tokens: deltaOutput,
          reasoning_output_tokens: deltaReasoning,
          total_tokens: deltaTotal,
          total_cost_usd: deltaCost,
          conversation_count: previous ? 0 : 1,
        });
        touchedBuckets.add(bucketKey("cline", model, bucketStart));

        messageTotals[key] = {
          input: inputTokens,
          cached_input: cacheRead,
          cache_creation: cacheWrite,
          output: outputTokens,
          reasoning: reasoningTokens,
          cost,
        };
        eventsAggregated++;
      }

      // Strings retain large file IDs and nanosecond times through cursor JSON.
      fileOffsets[filePath] = {
        size: stat.size.toString(),
        mtimeNs: stat.mtimeNs.toString(),
        dev: stat.dev.toString(),
        ino: stat.ino.toString(),
      };
    } finally {
      // One tick per discovered transcript — including ones skipped as
      // unchanged or dropped as unreadable — so the sync progress bar always
      // reaches its total instead of stalling on the first skip.
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
  }

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.cline = { ...clineState, messageTotalsByFile, fileOffsets, updatedAt };

  return { recordsProcessed, eventsAggregated, bucketsQueued };
}

module.exports = {
  resolveClineSessionsDir,
  resolveClineSessionsDirs,
  listClineSessionFiles,
  resolveClineSessionFiles,
  resolveClineSessionFilesWithStatus,
  readClineSessionModel,
  normalizeClineModel,
  parseClineIncremental,
};
