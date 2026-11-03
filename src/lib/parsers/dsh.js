// dsh usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fs = require("node:fs/promises");
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { ensureDir } = require("../fs");
const wsl = require("../wsl-probe");
const {
  enqueueTouchedBuckets,
  cloneTotals,
  normalizeHourlyState,
  getHourlyBucket,
  initTotals,
  addTotals,
  subtractTotals,
  toUtcHalfHourStart,
  bucketKey,
  normalizeModelInput,
  toNonNegativeInt,
  safeReadDir,
  DSH_SESSION_TEXT_MAX_BYTES,
  inspectDshZstdFrames,
  decodeDshZstd,
  findDshJsonProperty,
  parseDshJsonString,
} = require("./core");

// ─────────────────────────────────────────────────────────────────────────────
// DeepSeek Harness (dsh) — passive reader of the harness's session logs.
//
// The DeepSeek Harness SDK persists each agent session as an append-only JSONL
// log under `<harness-home>/sessions/<project-key>/<session-id>/session.jsonl[.zstd]`
// (default `~/.dsh/sessions`; `$DSH_HOME` / TOKEN_PULSE_DSH_HOME override). The first line is a `type: "session"` header;
// every following line is a `SessionEvent` carrying `type`, `seq`, `time`
// (epoch ms), and `data`. Token accounting rides on `assistant/message` events
// (`data.usage`), and the model on the same event's `data.message.source.model`
// (falling back to the most recent `request/header` config). Usage counts are
// DISJOINT — `inputTokens` is uncached input only, cache reads/writes and
// reasoning travel separately — so the mapping to our queue columns is direct
// (no cache subtraction, unlike Codex).
//
// Zstd artifacts are a concatenated-frame container: a single decompress call
// decodes only the first frame (silently dropping every event line), so we
// identify the frame boundaries and decode each frame independently —
// built-in zlib zstd first, `@mongodb-js/zstd` as the Node 20 fallback —
// enforcing a cumulative plaintext bound as we go.
// Dedup is a per-file `lastSeq` watermark — seq
// is monotonic within an append-only session log, so a grow re-reads the file
// and skips everything at or below the watermark. Torn tails never advance the
// watermark until their JSON record is complete; format replacements are
// reconciled through per-session contribution ledgers.
const DSH_SESSION_LOG_MAX_BYTES = 64 * 1024 * 1024;
const DSH_SOURCE = "dsh";

// Precedence mirrors the harness's own resolveDshHome: an explicit
// UsageCairn override, then the harness's $DSH_HOME, then `~/.dsh`. Tests
// isolate $DSH_HOME via the `withHome` helper, so honoring it here cannot leak
// the real home past test isolation.
function resolveDshHome(env = process.env) {
  const explicit =
    (typeof env?.TOKEN_PULSE_DSH_HOME === "string" && env.TOKEN_PULSE_DSH_HOME.trim()) ||
    (typeof env?.DSH_HOME === "string" && env.DSH_HOME.trim()) ||
    "";
  if (explicit) return path.resolve(explicit);
  return path.join(os.homedir(), ".dsh");
}

// Windows users commonly run Harness inside WSL while UsageCairn itself runs
// natively. Respect the repository-wide WSL mode contract and keep explicit
// DSH home overrides authoritative: an override is a complete user choice, not
// one half of an automatic native/WSL discovery pair.
function resolveDshHomes(env = process.env, deps = {}) {
  const overridden = Boolean(
    (typeof env?.TOKEN_PULSE_DSH_HOME === "string" && env.TOKEN_PULSE_DSH_HOME.trim()) ||
    (typeof env?.DSH_HOME === "string" && env.DSH_HOME.trim()),
  );
  const nativeHome = deps.nativeHome || resolveDshHome(env);
  const platform = deps.platform || process.platform;
  if (overridden || platform !== "win32") return [nativeHome];

  const existsSync = deps.existsSync || fssync.existsSync;
  let nativeValue = null;
  try {
    if (existsSync(nativeHome)) nativeValue = nativeHome;
  } catch (_error) {}

  const discoverWslHome = deps.discoverWslHome || wsl.discoverWslHome;
  const wslValue = wsl.shouldProbeWsl(env)
    ? discoverWslHome(".dsh", { ...deps, env })
    : null;
  const resolved = wsl.resolveAllWin32Paths({
    nativeValue,
    wslValue,
    env,
    platform,
  });
  return [...new Set([resolved.native, resolved.wsl].filter(Boolean))];
}

const DSH_SESSION_LOG_PATTERN = /^session(?:\.v\d+)?\.jsonl(?:\.zstd)?$/;

function isDshSessionLogName(name) {
  return typeof name === "string" && DSH_SESSION_LOG_PATTERN.test(name);
}

function parseDshVersion(name) {
  const match = typeof name === "string" ? name.match(/\.v(\d+)\.jsonl/) : null;
  return match ? parseInt(match[1], 10) : 0;
}

// Walk the harness sessions root for per-session log artifacts. The tree is
// `<sessions-root>/<project-key>/<session-id>/session[.v3].jsonl[.zstd]`; only
// the matching leaf names are collected so unrelated harness files are ignored.
async function resolveDshSessionFiles(env = process.env, deps = {}) {
  const out = [];
  const seen = new Set();
  for (const dshHome of resolveDshHomes(env, deps)) {
    const sessionsRoot = path.join(dshHome, "sessions");
    const projects = await safeReadDir(sessionsRoot);
    for (const project of projects) {
      if (!project.isDirectory()) continue;
      const projectDir = path.join(sessionsRoot, project.name);
      const sessions = await safeReadDir(projectDir);
      for (const session of sessions) {
        if (!session.isDirectory()) continue;
        const sessionDir = path.join(projectDir, session.name);
        const artifacts = await safeReadDir(sessionDir);
        const transcripts = artifacts.filter(
          (artifact) => artifact.isFile() && isDshSessionLogName(artifact.name),
        );
        let selected = null;
        if (transcripts.length === 1) {
          selected = path.join(sessionDir, transcripts[0].name);
        } else if (transcripts.length > 1) {
          // Harness itself rejects mixed encodings in one root. For a passive
          // reader, choose the actively-written artifact instead of counting the
          // same session twice; a tie prefers the higher format version, then zstd.
          const ranked = await Promise.all(transcripts.map(async (artifact) => {
            const full = path.join(sessionDir, artifact.name);
            const handle = await fs.open(full, "r").catch(() => null);
            if (!handle) return { full, name: artifact.name, mtimeMs: 0 };
            try {
              const stat = await handle.stat().catch(() => null);
              return {
                full,
                name: artifact.name,
                mtimeMs: stat?.isFile() ? Number(stat.mtimeMs || 0) : 0,
              };
            } finally {
              await handle.close().catch(() => {});
            }
          }));
          ranked.sort((left, right) =>
            right.mtimeMs - left.mtimeMs ||
            parseDshVersion(right.name) - parseDshVersion(left.name) ||
            Number(right.name.endsWith(".zstd")) - Number(left.name.endsWith(".zstd")),
          );
          selected = ranked[0]?.full || null;
        }
        if (selected && !seen.has(selected)) {
          seen.add(selected);
          out.push(selected);
        }
      }
    }
  }
  out.sort((a, b) => a.localeCompare(b));
  return out;
}

function dshSessionMetadata(stat) {
  return {
    inode: stat?.ino || 0,
    size: Number.isFinite(stat?.size) ? stat.size : 0,
    mtimeMs: Number.isFinite(stat?.mtimeMs) ? stat.mtimeMs : 0,
  };
}

function sameDshSessionMetadata(previous, current) {
  return Boolean(
    previous &&
    previous.inode === current.inode &&
    previous.size === current.size &&
    previous.mtimeMs === current.mtimeMs
  );
}

function dshSessionDirectoryKey(filePath) {
  return typeof filePath === "string" ? path.dirname(path.resolve(filePath)) : null;
}

function normalizeDshContributions(value) {
  const normalized = {};
  if (!value || typeof value !== "object") return normalized;
  for (const [key, entry] of Object.entries(value)) {
    if (!entry || typeof entry !== "object" || !entry.totals) continue;
    const model = normalizeModelInput(entry.model);
    const bucketStart = typeof entry.bucketStart === "string" ? entry.bucketStart : null;
    if (!model || !bucketStart) continue;
    normalized[key] = {
      model,
      bucketStart,
      totals: cloneTotals(entry.totals),
    };
  }
  return normalized;
}

function storedDshContributions(state) {
  if (
    !state ||
    typeof state !== "object" ||
    !Object.prototype.hasOwnProperty.call(state, "contributions") ||
    !state.contributions ||
    typeof state.contributions !== "object"
  ) {
    return null;
  }
  const normalized = normalizeDshContributions(state.contributions);
  // A partially written or hand-edited ledger is not a safe subtraction
  // baseline. Treat it as absent so migration defers instead of silently
  // retracting only some of a session's contribution.
  if (Object.keys(normalized).length !== Object.keys(state.contributions).length) {
    return null;
  }
  return normalized;
}

function addDshContribution(contributions, model, bucketStart, totals) {
  const key = bucketKey(DSH_SOURCE, model, bucketStart);
  let contribution = contributions[key];
  if (!contribution) {
    contribution = {
      model,
      bucketStart,
      totals: initTotals(),
    };
    contributions[key] = contribution;
  }
  addTotals(contribution.totals, totals);
}

function dshContributionsFromDeltas(deltas) {
  const contributions = {};
  for (const delta of deltas || []) {
    const bucketStart = toUtcHalfHourStart(delta.timeMs);
    if (!bucketStart || !delta.model || !delta.totals) continue;
    addDshContribution(contributions, delta.model, bucketStart, delta.totals);
  }
  return contributions;
}

function legacyDshContributionsFromSnapshot(snapshot, previousState) {
  if (
    !snapshot?.text ||
    !previousState ||
    typeof previousState !== "object" ||
    !Number.isSafeInteger(previousState.lastSeq) ||
    previousState.lastSeq < -1 ||
    previousState.inode !== snapshot.inode ||
    !Number.isFinite(previousState.size) ||
    snapshot.size < previousState.size
  ) {
    return null;
  }
  const parsed = extractDshSessionUsage(snapshot.text, -1);
  if (!parsed.complete || parsed.sessionId !== previousState.sessionId) return null;
  // The pre-ledger parser replayed unknown-sequence usage on every pass, so
  // there is no safe prefix boundary for such a record. Defer instead.
  if (parsed.deltas.some((delta) => !Number.isSafeInteger(delta.seq))) return null;
  return dshContributionsFromDeltas(
    parsed.deltas.filter((delta) => delta.seq <= previousState.lastSeq),
  );
}

function dshContributionsCoverPrior(prior, candidate) {
  for (const [key, previous] of Object.entries(prior || {})) {
    const current = candidate?.[key];
    if (!current?.totals || !previous?.totals) return false;
    for (const field of [
      "input_tokens",
      "cached_input_tokens",
      "cache_creation_input_tokens",
      "output_tokens",
      "reasoning_output_tokens",
      "total_tokens",
      "billable_total_tokens",
      "total_cost_usd",
      "conversation_count",
    ]) {
      const available = Number(current.totals[field] || 0);
      const required = Number(previous.totals[field] || 0);
      if (!Number.isFinite(available) || !Number.isFinite(required) || available < required) {
        return false;
      }
    }
  }
  return true;
}

function dshContributionsFitHourlyState(hourlyState, contributions) {
  for (const contribution of Object.values(contributions || {})) {
    if (!contribution?.model || !contribution.bucketStart || !contribution.totals) continue;
    const key = bucketKey(DSH_SOURCE, contribution.model, contribution.bucketStart);
    const bucket = hourlyState?.buckets?.[key];
    if (!bucket?.totals) return false;
    for (const field of [
      "input_tokens",
      "cached_input_tokens",
      "cache_creation_input_tokens",
      "output_tokens",
      "reasoning_output_tokens",
      "total_tokens",
      "billable_total_tokens",
      "total_cost_usd",
      "conversation_count",
    ]) {
      const available = Number(bucket.totals[field] || 0);
      const required = Number(contribution.totals[field] || 0);
      if (!Number.isFinite(available) || !Number.isFinite(required) || available < required) {
        return false;
      }
    }
  }
  return true;
}

function applyDshContributions({ hourlyState, touchedBuckets, contributions, subtract = false }) {
  for (const contribution of Object.values(contributions || {})) {
    if (!contribution?.model || !contribution.bucketStart || !contribution.totals) continue;
    const bucket = getHourlyBucket(
      hourlyState,
      DSH_SOURCE,
      contribution.model,
      contribution.bucketStart,
    );
    if (subtract) subtractTotals(bucket.totals, contribution.totals);
    else addTotals(bucket.totals, contribution.totals);
    touchedBuckets.add(bucketKey(DSH_SOURCE, contribution.model, contribution.bucketStart));
  }
}

// Open, inspect and read through one handle so a path replacement cannot make
// the metadata describe a different file from the bytes we parse.
async function readDshSessionSnapshot(
  filePath,
  { maxOutputBytes = DSH_SESSION_TEXT_MAX_BYTES, previous = null } = {},
) {
  const handle = await fs.open(filePath, "r").catch(() => null);
  if (!handle) return null;
  try {
    const initialStat = await handle.stat().catch(() => null);
    if (!initialStat || !initialStat.isFile()) return null;
    const initialMetadata = dshSessionMetadata(initialStat);
    if (initialMetadata.size > DSH_SESSION_LOG_MAX_BYTES) {
      throw new Error(`DeepSeek Harness session log exceeds ${DSH_SESSION_LOG_MAX_BYTES} bytes`);
    }
    if (sameDshSessionMetadata(previous, initialMetadata)) {
      return { ...initialMetadata, unchanged: true, text: null };
    }
    const outputLimit = Number.isSafeInteger(maxOutputBytes) && maxOutputBytes >= 0
      ? maxOutputBytes
      : DSH_SESSION_TEXT_MAX_BYTES;
    const data = await handle.readFile();
    if (data.length > DSH_SESSION_LOG_MAX_BYTES) {
      throw new Error(`DeepSeek Harness session log exceeds ${DSH_SESSION_LOG_MAX_BYTES} bytes`);
    }
    let text;
    if (filePath.endsWith(".zstd")) {
      if (data.length === 0) text = "";
      else {
        const inspected = inspectDshZstdFrames(data, outputLimit);
        const decoded = await decodeDshZstd(data, { maxOutputBytes: outputLimit, inspected });
        if (
          decoded.length > outputLimit ||
          (inspected.totalContentBytes != null && decoded.length !== inspected.totalContentBytes)
        ) {
          throw new Error(`DeepSeek Harness decompressed session log exceeds ${outputLimit} bytes`);
        }
        text = decoded.toString("utf8");
      }
    } else {
      if (data.length > outputLimit) {
        throw new Error(`DeepSeek Harness decompressed session log exceeds ${outputLimit} bytes`);
      }
      text = data.toString("utf8");
    }

    const finalStat = await handle.stat().catch(() => initialStat);
    const metadata = dshSessionMetadata(finalStat);
    // If Harness appended while this handle was being read, force a retry on
    // the next sync unless the bytes consumed reached the final compressed/raw
    // file size. This avoids acknowledging a tail that was never parsed.
    if (metadata.size !== data.length) metadata.mtimeMs = -1;
    return { ...metadata, unchanged: false, text };
  } finally {
    await handle.close().catch(() => {});
  }
}

// Read one session log to plaintext, decompressing zstd artifacts. Returns
// null for a missing/non-file path so callers treat it as a no-op.
async function readDshSessionText(filePath, options = {}) {
  const snapshot = await readDshSessionSnapshot(filePath, options);
  return snapshot?.text ?? null;
}

// Drop a provider-qualified model id prefix ("deepseek/deepseek-v4-pro" →
// "deepseek-v4-pro"). The harness emits a bare id today, but the source
// vocabulary is provider-qualified elsewhere; keep the mapping cheap.
function normalizeDshModelName(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const slash = trimmed.lastIndexOf("/");
  const name = slash >= 0 ? trimmed.slice(slash + 1) : trimmed;
  return name ? name : null;
}

// Convert a harness `TokenUsage` object into our queue `totals` delta. Counts
// are disjoint, so every column maps 1:1; returns null for an all-zero usage.
function dshUsageToTotals(usage) {
  if (!usage || typeof usage !== "object") return null;
  const input = toNonNegativeInt(usage.inputTokens);
  const output = toNonNegativeInt(usage.outputTokens);
  const cacheRead = toNonNegativeInt(usage.cacheReadTokens);
  const cacheWrite = toNonNegativeInt(usage.cacheWriteTokens);
  const reasoning = toNonNegativeInt(usage.reasoningTokens);
  if (input === 0 && output === 0 && cacheRead === 0 && cacheWrite === 0 && reasoning === 0) {
    return null;
  }
  return {
    input_tokens: input,
    cached_input_tokens: cacheRead,
    cache_creation_input_tokens: cacheWrite,
    output_tokens: output,
    reasoning_output_tokens: reasoning,
    total_tokens: input + output + cacheRead + cacheWrite + reasoning,
    conversation_count: 1,
  };
}

function parseDshJsonNumber(raw) {
  if (typeof raw !== "string") return NaN;
  const value = Number(raw);
  return Number.isFinite(value) ? value : NaN;
}

function parseDshUsageSlice(raw) {
  if (typeof raw !== "string") return null;
  return {
    inputTokens: parseDshJsonNumber(findDshJsonProperty(raw, "inputTokens")),
    outputTokens: parseDshJsonNumber(findDshJsonProperty(raw, "outputTokens")),
    cacheReadTokens: parseDshJsonNumber(findDshJsonProperty(raw, "cacheReadTokens")),
    cacheWriteTokens: parseDshJsonNumber(findDshJsonProperty(raw, "cacheWriteTokens")),
    reasoningTokens: parseDshJsonNumber(findDshJsonProperty(raw, "reasoningTokens")),
  };
}

function isCompleteDshJsonLine(raw) {
  const text = String(raw || "").trim();
  if (!text || text[0] !== "{") return false;
  let index = 0;
  const maxDepth = 256;
  const hex = (char) => /[0-9a-f]/i.test(char || "");
  const skipWhitespace = () => {
    while (index < text.length && /\s/.test(text[index])) index += 1;
  };
  const parseString = () => {
    if (text[index] !== '"') return false;
    index += 1;
    while (index < text.length) {
      const char = text[index++];
      if (char === '"') return true;
      if (char.charCodeAt(0) < 0x20) return false;
      if (char !== "\\") continue;
      if (index >= text.length) return false;
      const escaped = text[index++];
      if (escaped === "u") {
        if (index + 4 > text.length || ![...text.slice(index, index + 4)].every(hex)) return false;
        index += 4;
      } else if (!'"\\/bfnrt'.includes(escaped)) {
        return false;
      }
    }
    return false;
  };
  const parseNumber = () => {
    const start = index;
    if (text[index] === "-") index += 1;
    if (text[index] === "0") {
      index += 1;
    } else if (/[1-9]/.test(text[index] || "")) {
      while (/[0-9]/.test(text[index] || "")) index += 1;
    } else {
      return false;
    }
    if (text[index] === ".") {
      index += 1;
      const fractionStart = index;
      while (/[0-9]/.test(text[index] || "")) index += 1;
      if (index === fractionStart) return false;
    }
    if (text[index] === "e" || text[index] === "E") {
      index += 1;
      if (text[index] === "+" || text[index] === "-") index += 1;
      const exponentStart = index;
      while (/[0-9]/.test(text[index] || "")) index += 1;
      if (index === exponentStart) return false;
    }
    return index > start;
  };
  const parseValue = (depth) => {
    if (depth > maxDepth) return false;
    skipWhitespace();
    const char = text[index];
    if (char === '"') return parseString();
    if (char === "{") {
      index += 1;
      skipWhitespace();
      if (text[index] === "}") {
        index += 1;
        return true;
      }
      while (index < text.length) {
        skipWhitespace();
        if (!parseString()) return false;
        skipWhitespace();
        if (text[index++] !== ":") return false;
        if (!parseValue(depth + 1)) return false;
        skipWhitespace();
        if (text[index] === "}") {
          index += 1;
          return true;
        }
        if (text[index++] !== ",") return false;
      }
      return false;
    }
    if (char === "[") {
      index += 1;
      skipWhitespace();
      if (text[index] === "]") {
        index += 1;
        return true;
      }
      while (index < text.length) {
        if (!parseValue(depth + 1)) return false;
        skipWhitespace();
        if (text[index] === "]") {
          index += 1;
          return true;
        }
        if (text[index++] !== ",") return false;
        skipWhitespace();
      }
      return false;
    }
    for (const literal of ["true", "false", "null"]) {
      if (text.startsWith(literal, index)) {
        index += literal.length;
        return true;
      }
    }
    return parseNumber();
  };

  if (!parseValue(0)) return false;
  skipWhitespace();
  return index === text.length;
}

// Parse one session log's plaintext into usage deltas, skipping events whose
// seq is at or below the watermark. Returns the deltas (each carrying the
// sequence, model and epoch-ms timestamp), the highest complete seq seen, the
// session id, and whether every non-empty JSONL line was complete.
function extractDshSessionUsage(text, lastSeq = -1) {
  const deltas = [];
  const watermark = Number.isFinite(lastSeq) ? lastSeq : -1;
  let maxSeq = watermark;
  let sessionId = null;
  let headerModel = null;
  let complete = true;

  const lines = String(text || "").split("\n");
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!line || !line.trim()) continue;
    if (!isCompleteDshJsonLine(line)) {
      complete = false;
      // A later event cannot make this gap safe to acknowledge. Stop at the
      // complete prefix so a repaired record is retried before seq advances.
      break;
    }
    const eventType = parseDshJsonString(findDshJsonProperty(line, "type"));
    if (!eventType) continue;

    if (eventType === "session" || eventType === "session/start") {
      const id =
        parseDshJsonString(findDshJsonProperty(line, "id")) ||
        parseDshJsonString(findDshJsonProperty(findDshJsonProperty(line, "data"), "id")) ||
        parseDshJsonString(findDshJsonProperty(findDshJsonProperty(line, "data"), "sessionId"));
      if (id) sessionId = id;
      continue;
    }

    const seq = parseDshJsonNumber(findDshJsonProperty(line, "seq"));
    const seqKnown = Number.isFinite(seq) && seq >= 0;
    if (seqKnown && seq > maxSeq) maxSeq = seq;

    const data = findDshJsonProperty(line, "data");
    if (!data) continue;

    if (eventType === "request/header") {
      const header = findDshJsonProperty(data, "header");
      const config = findDshJsonProperty(header, "config");
      const model = parseDshJsonString(findDshJsonProperty(config, "model"));
      const normalized = normalizeDshModelName(model);
      if (normalized) headerModel = normalized;
      continue;
    }

    if (eventType !== "assistant/message" && eventType !== "message/assistant") continue;
    if (seqKnown && seq <= watermark) continue;

    const message = findDshJsonProperty(data, "message");
    const source = message ? findDshJsonProperty(message, "source") : null;
    const model = normalizeDshModelName(
      (source && parseDshJsonString(findDshJsonProperty(source, "model"))) ||
      parseDshJsonString(findDshJsonProperty(data, "model")),
    ) || headerModel;
    const totals = dshUsageToTotals(
      parseDshUsageSlice(
        findDshJsonProperty(data, "usage") || findDshJsonProperty(line, "usage"),
      ),
    );
    if (!model || !totals) continue;

    const timeMs =
      parseDshJsonNumber(findDshJsonProperty(line, "time")) ||
      parseDshJsonNumber(findDshJsonProperty(line, "timestamp")) ||
      parseDshJsonNumber(findDshJsonProperty(data, "time")) ||
      parseDshJsonNumber(findDshJsonProperty(data, "timestamp"));
    if (!Number.isFinite(timeMs) || timeMs <= 0) continue;

    deltas.push({ seq: seqKnown ? seq : null, model, timeMs, totals });
  }

  return { deltas, maxSeq, sessionId, complete };
}

// Incremental parser entrypoint. Mirrors the other passive JSONL readers:
// identity-check each file (inode/size/mtime), re-read + re-parse only when it
// changed, dedup via the per-file seq watermark, accumulate into hourly
// buckets, then flush touched buckets to the queue. Session contribution ledgers
// make artifact-path migrations replace one session instead of resetting DSH.
async function parseDshIncremental({ sessionFiles, cursors, queuePath, onProgress }) {
  await ensureDir(path.dirname(queuePath));
  if (!cursors || typeof cursors !== "object") cursors = {};
  if (!cursors.dsh || typeof cursors.dsh !== "object") cursors.dsh = {};
  const dshState = cursors.dsh;
  const storedFileState =
    dshState.files && typeof dshState.files === "object" ? dshState.files : {};
  let fileState = { ...storedFileState };
  const storedSessionState =
    dshState.sessions && typeof dshState.sessions === "object" ? dshState.sessions : {};
  const sessionState = { ...storedSessionState };

  const hourlyState = normalizeHourlyState(cursors?.hourly);
  // normalizeHourlyState preserves bucket objects for the other incremental
  // parsers. DSH needs transactional ownership because replacement validation
  // can defer after inspecting several files and queue append may fail.
  hourlyState.groupQueued = { ...(hourlyState.groupQueued || {}) };
  for (const [key, bucket] of Object.entries(hourlyState.buckets || {})) {
    hourlyState.buckets[key] = {
      ...(bucket && typeof bucket === "object" ? bucket : {}),
      totals: {
        ...initTotals(),
        ...(bucket?.totals && typeof bucket.totals === "object" ? bucket.totals : {}),
      },
    };
  }
  const touchedBuckets = new Set();
  const deferredMigrationPaths = new Map();
  const cb = typeof onProgress === "function" ? onProgress : null;

  const files = Array.isArray(sessionFiles)
    ? sessionFiles.filter((filePath) => typeof filePath === "string")
    : [];
  const presentFiles = new Set(files);
  const total = files.length;
  const deferredFilePaths = new Set();
  let recordsProcessed = 0;
  let eventsAggregated = 0;

  const staleFileForSession = (sessionId, currentPath) => {
    if (!sessionId) return null;
    for (const [filePath, state] of Object.entries(fileState)) {
      if (
        filePath !== currentPath &&
        !presentFiles.has(filePath) &&
        (state?.sessionId === sessionId ||
          (!state?.sessionId &&
            dshSessionDirectoryKey(filePath) === dshSessionDirectoryKey(currentPath)))
      ) {
        return filePath;
      }
    }
    return null;
  };

  const deferStaleMigrationPaths = (currentPath, reason) => {
    let deferred = 0;
    for (const oldPath of Object.keys(fileState)) {
      if (
        !presentFiles.has(oldPath) &&
        dshSessionDirectoryKey(oldPath) === dshSessionDirectoryKey(currentPath)
      ) {
        if (!deferredMigrationPaths.has(oldPath)) deferred += 1;
        deferredFilePaths.add(oldPath);
        deferredMigrationPaths.set(oldPath, reason);
      }
    }
    return deferred;
  };

  const reportProgress = (idx, recordsProcessed, eventsAggregated, total) => {
    if (cb) {
      cb({
        index: idx + 1,
        total,
        recordsProcessed,
        eventsAggregated,
        bucketsQueued: touchedBuckets.size,
        deferredMigrations: deferredMigrationPaths.size,
      });
    }
  };

  for (let idx = 0; idx < files.length; idx++) {
    const filePath = files[idx];
    const prev = fileState[filePath] || null;
    const previousSessionId = typeof prev?.sessionId === "string" ? prev.sessionId : null;
    const previousSession = previousSessionId ? sessionState[previousSessionId] : null;
    const previousFileContributions = storedDshContributions(prev);
    const previousSessionContributions = storedDshContributions(previousSession);
    const needsLedgerBackfill = Boolean(
      previousSessionId && !previousSessionContributions && !previousFileContributions,
    );
    let snapshot;
    let parsed;
    let fullParsed = null;
    let fileReset = false;
    let sessionChanged = false;

    try {
      snapshot = await readDshSessionSnapshot(filePath, {
        previous: needsLedgerBackfill ? null : prev,
      });
      if (!snapshot) {
        // A resolver-selected replacement can disappear between discovery and
        // open. Preserve the old path so a later retry cannot re-add its full
        // contribution. This is deliberately separate from a thrown read
        // error because a missing path returns null from the snapshot helper.
        deferStaleMigrationPaths(filePath, "replacement-missing");
        reportProgress(idx, recordsProcessed, eventsAggregated, total);
        continue;
      }
      if (snapshot.unchanged && !needsLedgerBackfill) {
        reportProgress(idx, recordsProcessed, eventsAggregated, total);
        continue;
      }

      const previousHasInode = Number.isFinite(prev?.inode);
      const previousHasSize = Number.isFinite(prev?.size);
      const previousHasMtime = Number.isFinite(prev?.mtimeMs);
      fileReset = Boolean(
        prev &&
        (
          (previousHasInode && snapshot.inode !== prev.inode) ||
          (previousHasSize && snapshot.size < prev.size) ||
          (
            previousHasSize &&
            previousHasMtime &&
            snapshot.size <= prev.size &&
            snapshot.mtimeMs !== prev.mtimeMs
          )
        ),
      );
      const lastSeq = fileReset || !Number.isFinite(prev?.lastSeq) ? -1 : prev.lastSeq;
      parsed = extractDshSessionUsage(snapshot.text, lastSeq);
      sessionChanged = Boolean(
        prev &&
        parsed.sessionId &&
        parsed.sessionId !== previousSessionId,
      );
      if (sessionChanged) parsed = extractDshSessionUsage(snapshot.text, -1);
      if (!prev || needsLedgerBackfill || sessionChanged || fileReset) {
        fullParsed = extractDshSessionUsage(snapshot.text, -1);
        if (!prev || sessionChanged || fileReset) parsed = fullParsed;
      }
      if (!parsed.complete) snapshot.mtimeMs = -1;
    } catch (error) {
      if (process.env.TOKEN_PULSE_DEBUG) {
        process.stderr.write(`[dsh] skipped ${filePath}: ${error?.message || error}\n`);
      }
      // A replacement artifact may have an old path that is no longer in the
      // selected file list. Keep that cursor until the replacement is readable
      // so a failed migration cannot turn into an untracked double-count.
      deferStaleMigrationPaths(filePath, "replacement-read-failed");
      reportProgress(idx, recordsProcessed, eventsAggregated, total);
      continue;
    }

    const sessionId = typeof parsed.sessionId === "string" ? parsed.sessionId : null;
    const stalePathWithoutId = sessionId
      ? null
      : Object.keys(fileState).find(
          (oldPath) =>
            !presentFiles.has(oldPath) &&
            dshSessionDirectoryKey(oldPath) === dshSessionDirectoryKey(filePath),
        );
    if (stalePathWithoutId) {
      deferredFilePaths.add(stalePathWithoutId);
      deferredMigrationPaths.set(stalePathWithoutId, "replacement-session-id-missing");
      reportProgress(idx, recordsProcessed, eventsAggregated, total);
      continue;
    }
    const session = sessionId ? sessionState[sessionId] : null;
    const stalePath = staleFileForSession(sessionId, filePath);
    const previousPath =
      session?.lastPath && session.lastPath !== filePath ? session.lastPath : stalePath;
    const resetCurrentFile = Boolean(fileReset && !sessionChanged);
    const replacementPath = previousPath || (resetCurrentFile ? filePath : null);
    const sessionContributions = storedDshContributions(session);
    const staleContributions = storedDshContributions(stalePath && fileState[stalePath]);
    const fileContributions = storedDshContributions(prev);
    let oldContributions = sessionContributions || staleContributions || fileContributions;

    // A pure rename preserves the same physical bytes and metadata, so an old
    // cursor can safely adopt the new path without relying on rewritten seqs.
    const previousState = previousPath ? fileState[previousPath] : null;
    if (
      sessionId &&
      previousPath &&
      !oldContributions &&
      !prev &&
      parsed.complete &&
      previousState &&
      previousState.inode === snapshot.inode &&
      previousState.size === snapshot.size &&
      previousState.mtimeMs === snapshot.mtimeMs
    ) {
      // A pure rename preserves the same bytes. Reusing the parsed full
      // contribution is safe here because replacement below subtracts and
      // re-adds it; no sequence watermark is transferred.
      oldContributions = dshContributionsFromDeltas(parsed.deltas);
    }

    // Cursors written before the contribution ledger existed can still be
    // reconciled when the old artifact remains available and its same-inode
    // growth follows the Harness append-only contract. Reconstruct only the
    // prefix through the old watermark; using the whole current artifact would
    // subtract events that were appended after the old cursor was written.
    if (sessionId && previousPath && !oldContributions) {
      const oldState = fileState[previousPath];
      const oldSnapshot = await readDshSessionSnapshot(previousPath).catch(() => null);
      oldContributions = legacyDshContributionsFromSnapshot(oldSnapshot, oldState);
    }
    const replaceSession = Boolean(
      sessionId &&
      oldContributions &&
      (!prev || resetCurrentFile || (previousPath && previousPath !== filePath)),
    );
    const oldContributionCount = Object.keys(oldContributions || {}).length;
    if (sessionId && replacementPath && !oldContributions) {
      deferredFilePaths.add(replacementPath);
      deferredMigrationPaths.set(replacementPath, "legacy-baseline-unavailable");
      if (resetCurrentFile) fileState[filePath] = { ...prev, mtimeMs: -1 };
      reportProgress(idx, recordsProcessed, eventsAggregated, total);
      continue;
    }
    if (
      replaceSession &&
      (!parsed.complete || (oldContributionCount > 0 && parsed.deltas.length === 0))
    ) {
      // A readable header-only or torn replacement is not authoritative. Keep
      // the old contribution until a complete replacement with usage arrives.
      deferredFilePaths.add(replacementPath);
      deferredMigrationPaths.set(
        replacementPath,
        parsed.complete ? "replacement-has-no-usage" : "replacement-incomplete",
      );
      if (resetCurrentFile) fileState[filePath] = { ...prev, mtimeMs: -1 };
      reportProgress(idx, recordsProcessed, eventsAggregated, total);
      continue;
    }
    const candidateContributions = dshContributionsFromDeltas(parsed.deltas);
    if (
      replaceSession &&
      oldContributionCount > 0 &&
      !dshContributionsCoverPrior(oldContributions, candidateContributions)
    ) {
      // A complete-looking replacement that drops a previously counted bucket
      // is still unsafe. Format migration should preserve prior usage; defer
      // until a candidate with a verifiable superset arrives.
      deferredFilePaths.add(replacementPath);
      deferredMigrationPaths.set(replacementPath, "replacement-drops-prior-usage");
      if (resetCurrentFile) fileState[filePath] = { ...prev, mtimeMs: -1 };
      reportProgress(idx, recordsProcessed, eventsAggregated, total);
      continue;
    }
    if (replaceSession && !dshContributionsFitHourlyState(hourlyState, oldContributions)) {
      // Never let subtractTotals clamp away another session's history when the
      // persisted ledger and hourly bucket disagree. Defer for a repairable,
      // visible retry instead.
      deferredFilePaths.add(replacementPath);
      deferredMigrationPaths.set(replacementPath, "stored-contribution-not-in-bucket");
      if (resetCurrentFile) fileState[filePath] = { ...prev, mtimeMs: -1 };
      reportProgress(idx, recordsProcessed, eventsAggregated, total);
      continue;
    }

    if (replaceSession) {
      applyDshContributions({
        hourlyState,
        touchedBuckets,
        contributions: oldContributions,
        subtract: true,
      });
      if (previousPath && !presentFiles.has(previousPath)) delete fileState[previousPath];
    }

    let nextContributions = replaceSession
      ? {}
      : sessionContributions || fileContributions || {};
    if (fullParsed) {
      nextContributions = dshContributionsFromDeltas(fullParsed.deltas);
    }

    for (const delta of parsed.deltas) {
      const bucketStart = toUtcHalfHourStart(delta.timeMs);
      if (!bucketStart) continue;
      const bucket = getHourlyBucket(hourlyState, DSH_SOURCE, delta.model, bucketStart);
      addTotals(bucket.totals, delta.totals);
      touchedBuckets.add(bucketKey(DSH_SOURCE, delta.model, bucketStart));
      if (!fullParsed) addDshContribution(nextContributions, delta.model, bucketStart, delta.totals);
      eventsAggregated += 1;
    }

    const updatedAt = new Date().toISOString();
    fileState[filePath] = {
      inode: snapshot.inode,
      size: snapshot.size,
      mtimeMs: snapshot.mtimeMs,
      sessionId,
      lastSeq: parsed.maxSeq,
      contributions: nextContributions,
      updatedAt,
    };
    if (sessionId) {
      sessionState[sessionId] = {
        lastPath: filePath,
        contributions: nextContributions,
        updatedAt,
      };
    }
    recordsProcessed += 1;

    reportProgress(idx, recordsProcessed, eventsAggregated, total);
  }

  const nowMs = Date.now();
  for (const filePath of Object.keys(fileState)) {
    if (presentFiles.has(filePath) || deferredFilePaths.has(filePath)) continue;
    const state = fileState[filePath];
    const legacySessionId = typeof state?.sessionId === "string" ? state.sessionId : null;
    // Retain pre-ledger identity until a replacement is verified. Its counted
    // usage survives file deletion, so elapsed time cannot authorize replay.
    // Current ledgers retain the same identity in sessionState instead.
    if (legacySessionId && !storedDshContributions(state) && !sessionState[legacySessionId]) {
      if (!Number.isFinite(Number(state?.missingSince))) {
        fileState[filePath] = { ...state, missingSince: nowMs };
      }
      continue;
    }
    delete fileState[filePath];
  }

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  dshState.files = fileState;
  dshState.sessions = sessionState;
  if (deferredMigrationPaths.size > 0) {
    const reasons = [...new Set(deferredMigrationPaths.values())].sort();
    dshState.deferredMigrations = {
      count: deferredMigrationPaths.size,
      reasons,
      updatedAt,
    };
  } else {
    delete dshState.deferredMigrations;
  }
  dshState.updatedAt = updatedAt;

  return {
    recordsProcessed,
    eventsAggregated,
    bucketsQueued,
    deferredMigrations: deferredMigrationPaths.size,
  };
}

module.exports = {
  resolveDshHome,
  resolveDshHomes,
  isDshSessionLogName,
  parseDshVersion,
  resolveDshSessionFiles,
  readDshSessionText,
  normalizeDshModelName,
  dshUsageToTotals,
  extractDshSessionUsage,
  parseDshIncremental,
};
