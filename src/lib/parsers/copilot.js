// copilot usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const readline = require("node:readline");
const crypto = require("node:crypto");
const { ensureDir } = require("../fs");
const { readSqliteJsonRows } = require("../sqlite-reader");
const wsl = require("../wsl-probe");
const {
  enqueueTouchedBuckets,
  normalizeHourlyState,
  getHourlyBucket,
  initTotals,
  addTotals,
  subtractTotals,
  totalsKey,
  toUtcHalfHourStart,
  bucketKey,
  normalizeModelInput,
  toNonNegativeInt,
  snapshotSqliteDb,
  sqliteSidecarFingerprint,
  sameSqliteFingerprint,
} = require("./core");

// ─────────────────────────────────────────────────────────────────────────────
// GitHub Copilot CLI — OpenTelemetry JSONL exporter
// User must opt in by setting:
//   COPILOT_OTEL_ENABLED=true
//   COPILOT_OTEL_EXPORTER_TYPE=file
//   COPILOT_OTEL_FILE_EXPORTER_PATH=$HOME/.copilot/otel/copilot-otel-...jsonl
// We scan both known default directories plus the env-overridden path.
// ─────────────────────────────────────────────────────────────────────────────

function resolveCopilotOtelPaths(env = process.env) {
  const home = env.HOME || os.homedir();
  const paths = new Set();
  const scanDir = (dir) => {
    if (!fssync.existsSync(dir)) return;
    try {
      for (const entry of fssync.readdirSync(dir)) {
        if (entry.endsWith(".jsonl")) paths.add(path.join(dir, entry));
      }
    } catch (_e) {}
  };
  if (process.platform !== "win32" || wsl.shouldProbeNative(env)) {
    for (const dir of [
      path.join(home, ".copilot", "otel"),
      path.join(home, ".copilot-otel"),
    ]) {
      scanDir(dir);
    }
  }
  if (process.platform === "win32") {
    if (wsl.shouldProbeWsl(env)) {
      for (const providerDir of [".copilot/otel", ".copilot-otel"]) {
        const wslDir = wsl.discoverWslHome(providerDir, { env });
        if (wslDir) scanDir(wslDir);
      }
    }
  }
  const explicit = env.COPILOT_OTEL_FILE_EXPORTER_PATH;
  if (typeof explicit === "string" && explicit.trim() && fssync.existsSync(explicit)) {
    paths.add(explicit);
  }
  return Array.from(paths).sort();
}

function isCopilotChatSpan(record) {
  if (!record || typeof record !== "object") return false;
  // Skip metric records (resource + scopeMetrics) which have no chat usage data
  if (record.scopeMetrics) return false;
  const opName = record?.attributes?.["gen_ai.operation.name"];
  // Both Copilot CLI (Span shape with type:"span") and Copilot Chat extension
  // (OTEL JS SDK LogRecord shape with event.name:"gen_ai.client.inference.operation.details")
  // mark chat completions with gen_ai.operation.name === "chat".
  if (opName === "chat") return true;
  if (record.type === "span" && typeof record.name === "string" && record.name.startsWith("chat ")) {
    return true;
  }
  return false;
}

function copilotOtelTimeToMs(value) {
  if (!Array.isArray(value) || value.length < 2) return null;
  const seconds = Number(value[0]);
  const nanos = Number(value[1]);
  if (!Number.isFinite(seconds)) return null;
  const ns = Number.isFinite(nanos) ? nanos : 0;
  return Math.round(seconds * 1000 + ns / 1_000_000);
}

function pickCopilotModel(attrs) {
  const candidates = [attrs?.["gen_ai.response.model"], attrs?.["gen_ai.request.model"]];
  for (const c of candidates) {
    if (typeof c === "string" && c.trim()) return c.trim();
  }
  return null;
}

const COPILOT_PARSER_VERSION = 3;
const COPILOT_USAGE_CLAIM_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const COPILOT_USAGE_CLAIM_MAX_ENTRIES = 10_000;

function pruneCopilotUsageClaims(
  events,
  nowMs = Date.now(),
  maxEntries = COPILOT_USAGE_CLAIM_MAX_ENTRIES,
) {
  const cutoffMs = nowMs - COPILOT_USAGE_CLAIM_RETENTION_MS;
  const retained = [];
  for (const event of Array.isArray(events) ? events : []) {
    if (!event || typeof event !== "object" || event.consumed === true) {
      continue;
    }
    const storedFirstSeenAtMs = Number(event.firstSeenAtMs);
    const firstSeenAtMs =
      Number.isFinite(storedFirstSeenAtMs) && storedFirstSeenAtMs > 0
        ? storedFirstSeenAtMs
        : nowMs;
    if (firstSeenAtMs < cutoffMs) continue;
    retained.push(
      firstSeenAtMs === storedFirstSeenAtMs
        ? event
        : { ...event, firstSeenAtMs },
    );
  }
  return retained.length > maxEntries
    ? retained.slice(retained.length - maxEntries)
    : retained;
}

function replaceCopilotUsageClaims(
  target,
  nowMs = Date.now(),
  maxEntries = COPILOT_USAGE_CLAIM_MAX_ENTRIES,
) {
  if (!Array.isArray(target)) return;
  const retained = pruneCopilotUsageClaims(target, nowMs, maxEntries);
  target.splice(0, target.length, ...retained);
}

function isCopilotV1ChatSpan(record) {
  if (!record || record.type !== "span") return false;
  const opName = record?.attributes?.["gen_ai.operation.name"];
  if (opName === "chat") return true;
  return typeof record.name === "string" && record.name.startsWith("chat ");
}

function copilotLineHash(line) {
  return crypto.createHash("sha256").update(line).digest("hex");
}

function incrementMapCount(map, key, amount = 1) {
  map.set(key, (map.get(key) || 0) + amount);
}

function getCopilotResponseId(attrs = {}) {
  const responseId = attrs["gen_ai.response.id"];
  return typeof responseId === "string" && responseId.trim() ? responseId.trim() : "";
}

// v2 preferred spanContext for every OTEL envelope. Chat-extension LogRecords
// can share that nested context across several model requests, so keep the old
// key only for the one envelope that owns top-level traceId/spanId: CLI spans.
function getCopilotLegacyDedupKey(record, attrs = record?.attributes || {}) {
  const traceId = record?.traceId || record?.spanContext?.traceId || "";
  const spanId = record?.spanId || record?.spanContext?.spanId || "";
  const responseId = getCopilotResponseId(attrs);
  return traceId && spanId ? `${traceId}:${spanId}` : responseId ? `resp:${responseId}` : null;
}

function getCopilotDedupKey(record, attrs = record?.attributes || {}) {
  const responseId = getCopilotResponseId(attrs);
  if (!isCopilotV1ChatSpan(record)) {
    return responseId ? `resp:${responseId}` : null;
  }

  const traceId = record?.traceId || "";
  const spanId = record?.spanId || "";
  return traceId && spanId
    ? `${traceId}:${spanId}`
    : responseId
      ? `resp:${responseId}`
      : null;
}

function copilotOtelAggregateKey(model, bucketStart) {
  return JSON.stringify([model, bucketStart]);
}

function addCopilotOtelAggregate(aggregates, model, bucketStart, delta) {
  const key = copilotOtelAggregateKey(model, bucketStart);
  let totals = aggregates.get(key);
  if (!totals) {
    totals = initTotals();
    aggregates.set(key, totals);
  }
  addTotals(totals, delta);
}

function copilotTotalsCover(existing, required) {
  if (!existing || typeof existing !== "object") return false;
  for (const field of [
    "input_tokens",
    "cached_input_tokens",
    "cache_creation_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
    "billable_total_tokens",
    "conversation_count",
  ]) {
    const actual = Number(existing[field] || 0);
    const needed = Number(required?.[field] || 0);
    if (!Number.isFinite(actual) || !Number.isFinite(needed) || actual < needed) {
      return false;
    }
  }
  return true;
}

function extractCopilotOtelUsage(record) {
  if (!isCopilotChatSpan(record)) return null;

  const attrs = record.attributes || {};
  const inputRaw = toNonNegativeInt(attrs["gen_ai.usage.input_tokens"]);
  const output = toNonNegativeInt(attrs["gen_ai.usage.output_tokens"]);
  const cacheRead = toNonNegativeInt(
    attrs["gen_ai.usage.cache_read.input_tokens"] ??
      attrs["gen_ai.usage.cache_read_input_tokens"] ??
      attrs["gen_ai.usage.cached_input_tokens"],
  );
  // Copilot CLI: cache_write.input_tokens; Copilot Chat extension: cache_creation.input_tokens
  const cacheWrite = toNonNegativeInt(
    attrs["gen_ai.usage.cache_write.input_tokens"] ??
      attrs["gen_ai.usage.cache_creation.input_tokens"] ??
      attrs["gen_ai.usage.cache_write_input_tokens"] ??
      attrs["gen_ai.usage.cache_creation_input_tokens"],
  );
  // Copilot CLI: reasoning.output_tokens; Copilot Chat extension: reasoning_tokens
  const reasoning = toNonNegativeInt(
    attrs["gen_ai.usage.reasoning.output_tokens"] ??
      attrs["gen_ai.usage.reasoning_tokens"] ??
      attrs["gen_ai.usage.reasoning_output_tokens"],
  );
  const reasoningClamped = Math.min(reasoning, output);
  const outputWithoutReasoning = Math.max(0, output - reasoningClamped);
  const cliSpan = isCopilotV1ChatSpan(record);
  // CLI input includes both cache reads and writes. Chat-extension LogRecords
  // expose cache creation separately, so preserve their existing input-minus-read semantics.
  const cacheReadClamped = Math.min(cacheRead, inputRaw);
  const cacheWriteClamped = Math.min(
    cacheWrite,
    Math.max(0, inputRaw - cacheReadClamped),
  );
  const cacheWriteForAccounting = cliSpan ? cacheWriteClamped : cacheWrite;
  const input = Math.max(
    0,
    inputRaw - cacheReadClamped - (cliSpan ? cacheWriteClamped : 0),
  );
  const totalInteresting =
    input +
    outputWithoutReasoning +
    cacheReadClamped +
    cacheWriteForAccounting +
    reasoningClamped;
  if (totalInteresting === 0) return null;

  // CLI Span uses endTime/startTime; Chat extension LogRecord uses hrTime/hrTimeObserved.
  const tsMs =
    copilotOtelTimeToMs(record.endTime) ||
    copilotOtelTimeToMs(record.startTime) ||
    copilotOtelTimeToMs(record.hrTime) ||
    copilotOtelTimeToMs(record.hrTimeObserved);
  if (!tsMs) return null;
  const bucketStart = toUtcHalfHourStart(new Date(tsMs).toISOString());
  if (!bucketStart) return null;

  const model =
    normalizeCopilotAppModel(pickCopilotModel(attrs)) ||
    COPILOT_APP_DEFAULT_MODEL;
  const cliSessionId =
    typeof attrs["gen_ai.conversation.id"] === "string"
      ? attrs["gen_ai.conversation.id"].trim()
      : "";
  const matchBase = {
    sessionId: cliSessionId,
    model,
    output: outputWithoutReasoning,
    cacheRead: cacheReadClamped,
    cacheWrite: cacheWriteClamped,
    reasoning: reasoningClamped,
    tsMs,
  };
  return {
    bucketStart,
    cliSpan,
    cliSessionId,
    delta: {
      input_tokens: input,
      cached_input_tokens: cacheReadClamped,
      cache_creation_input_tokens: cacheWriteForAccounting,
      output_tokens: outputWithoutReasoning,
      reasoning_output_tokens: reasoningClamped,
      total_tokens:
        input +
        outputWithoutReasoning +
        cacheReadClamped +
        cacheWriteForAccounting +
        reasoningClamped,
      conversation_count: 1,
    },
    matchBase,
    model,
    tsMs,
  };
}

function copilotUsageMatchKey({
  sessionId,
  model,
  input,
  output,
  cacheRead,
  cacheWrite,
  reasoning,
}) {
  return [
    sessionId || "",
    normalizeCopilotAppModel(model) || COPILOT_APP_DEFAULT_MODEL,
    toNonNegativeInt(input),
    toNonNegativeInt(output),
    toNonNegativeInt(cacheRead),
    toNonNegativeInt(cacheWrite),
    toNonNegativeInt(reasoning),
  ].join("|");
}

function createCopilotStoreUsageMatcher(events) {
  const candidates = new Map();
  for (const event of Array.isArray(events) ? events : []) {
    if (!event || typeof event !== "object") continue;
    if (event.consumed === true) continue;
    const tsMs = Number(event.tsMs);
    if (!Number.isFinite(tsMs) || tsMs <= 0) continue;
    const key = copilotUsageMatchKey(event);
    const list = candidates.get(key) || [];
    list.push({ tsMs, event });
    candidates.set(key, list);
  }
  for (const list of candidates.values()) {
    list.sort((a, b) => a.tsMs - b.tsMs);
  }
  return {
    consume(event, toleranceMs = 2_000) {
      const tsMs = Number(event?.tsMs);
      if (!Number.isFinite(tsMs) || tsMs <= 0) return false;
      const key = copilotUsageMatchKey(event || {});
      const list = candidates.get(key);
      if (!list || list.length === 0) return false;
      let bestIndex = -1;
      let bestDistance = Infinity;
      for (let i = 0; i < list.length; i++) {
        const distance = Math.abs(list[i].tsMs - tsMs);
        if (distance <= toleranceMs && distance < bestDistance) {
          bestIndex = i;
          bestDistance = distance;
        }
      }
      if (bestIndex < 0) return false;
      const [matched] = list.splice(bestIndex, 1);
      matched.event.consumed = true;
      return true;
    },
  };
}

// v2 may already have advanced the file cursor while collapsing several Chat
// extension LogRecords that shared one nested spanContext. Recompute only that
// envelope's historical contribution and apply the delta to the existing
// Copilot buckets. CLI spans are deliberately left alone: their v2 key was the
// correct top-level traceId:spanId key, and session-store adoption can coexist
// with the OTEL parser.
async function migrateCopilotChatLogRecordDedup({
  files,
  fileOffsets,
  hourlyState,
  touchedBuckets,
  seenIds,
} = {}) {
  // A v2 cursor can retain an offset for a rotated or deleted OTEL file that
  // is no longer returned by discovery. Its historical contribution cannot be
  // reconciled, but the stale offset must not block migration of new files.
  const availableFiles = new Set(Array.isArray(files) ? files : []);
  for (const filePath of Object.keys(fileOffsets || {})) {
    if (!availableFiles.has(filePath)) delete fileOffsets[filePath];
  }
  const trackedFiles = Object.keys(fileOffsets || {}).filter(
    (filePath) => Number(fileOffsets[filePath]?.size) > 0,
  );
  if (trackedFiles.length === 0) {
    return { applied: true, changed: false };
  }

  const oldSeen = new Set();
  const newSeen = new Set();
  const oldTotals = new Map();
  const newTotals = new Map();

  for (const filePath of files) {
    if (!Object.prototype.hasOwnProperty.call(fileOffsets, filePath)) continue;
    const prevEntry = fileOffsets[filePath] || {};
    const prevSize = Number(prevEntry.size) || 0;
    if (prevSize <= 0) continue;

    let stat;
    try {
      stat = fssync.statSync(filePath);
    } catch (_e) {
      return { applied: false, reason: "a previously parsed OTEL file is unreadable" };
    }
    if (
      stat.size < prevSize ||
      (typeof prevEntry.ino === "number" && stat.ino !== prevEntry.ino)
    ) {
      return { applied: false, reason: "a previously parsed OTEL file changed during migration" };
    }

    let stream;
    try {
      stream = fssync.createReadStream(filePath, {
        encoding: "utf8",
        start: 0,
        end: prevSize - 1,
      });
      const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
      try {
        for await (const line of rl) {
          if (!line || !line.trim()) continue;
          let record;
          try {
            record = JSON.parse(line);
          } catch (_e) {
            continue;
          }
          const usage = extractCopilotOtelUsage(record);
          if (!usage || usage.cliSpan) continue;

          const oldKey = getCopilotLegacyDedupKey(record, record.attributes || {});
          const oldDuplicate = oldKey && oldSeen.has(oldKey);
          if (oldKey) oldSeen.add(oldKey);
          if (!oldDuplicate) {
            addCopilotOtelAggregate(
              oldTotals,
              usage.model,
              usage.bucketStart,
              usage.delta,
            );
          }

          const newKey = getCopilotDedupKey(record, record.attributes || {});
          const newDuplicate = newKey && newSeen.has(newKey);
          if (newKey) newSeen.add(newKey);
          if (!newDuplicate) {
            addCopilotOtelAggregate(
              newTotals,
              usage.model,
              usage.bucketStart,
              usage.delta,
            );
          }
        }
      } finally {
        rl.close();
      }
    } catch (_e) {
      return { applied: false, reason: "an OTEL file could not be scanned" };
    } finally {
      stream?.destroy();
    }
  }

  const keys = new Set([...oldTotals.keys(), ...newTotals.keys()]);
  // Only keys that existed in the v2 contribution need coverage validation.
  // A key that exists only in newTotals is a newly recovered Chat request; its
  // old contribution is zero, so an absent old bucket is expected.
  for (const key of oldTotals.keys()) {
    const [model, bucketStart] = JSON.parse(key);
    const oldUsage = oldTotals.get(key) || initTotals();
    if (!copilotTotalsCover(
      hourlyState.buckets[bucketKey("copilot", model, bucketStart)]?.totals,
      oldUsage,
    )) {
      return {
        applied: false,
        reason: "existing Copilot buckets do not cover the old OTEL contribution",
      };
    }
  }

  let changed = false;
  for (const key of keys) {
    const [model, bucketStart] = JSON.parse(key);
    const oldUsage = oldTotals.get(key) || initTotals();
    const newUsage = newTotals.get(key) || initTotals();
    if (totalsKey(oldUsage) === totalsKey(newUsage)) continue;
    const bucket = getHourlyBucket(hourlyState, "copilot", model, bucketStart);
    subtractTotals(bucket.totals, oldUsage);
    addTotals(bucket.totals, newUsage);
    touchedBuckets.add(bucketKey("copilot", model, bucketStart));
    changed = true;
  }

  for (const id of newSeen) seenIds.add(id);
  return { applied: true, changed };
}

// Migration helper: stream the bytes v1 already saw (0 -> prevSize), classify
// whether the file contains old CLI spans v1 processed, and whether it also
// contains v2-only chat records v1 skipped. Mixed files must be replayed, but
// their old CLI lines are skipped by hash so history does not double-count.
async function scanCopilotV1MigrationFile(filePath, maxBytes) {
  const result = {
    v1Processed: false,
    v2OnlyChat: false,
    v1LineHashes: new Map(),
  };
  if (!maxBytes || maxBytes <= 0) return result;
  try {
    const stream = fssync.createReadStream(filePath, {
      encoding: "utf8",
      start: 0,
      end: maxBytes - 1,
    });
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
    try {
      for await (const line of rl) {
        if (!line || !line.trim()) continue;
        let record;
        try {
          record = JSON.parse(line);
        } catch (_e) {
          continue;
        }
        // Must mirror v1's isCopilotChatSpan exactly: BOTH the
        // gen_ai.operation.name path AND the legacy name-prefix fallback.
        // Missing the second path lets metric-free files of name-only CLI spans
        // look like "v1 skipped" -> offset reset -> re-read -> double-count.
        if (isCopilotV1ChatSpan(record)) {
          result.v1Processed = true;
          incrementMapCount(result.v1LineHashes, copilotLineHash(line));
        } else if (isCopilotChatSpan(record)) {
          result.v2OnlyChat = true;
        }
      }
    } finally {
      rl.close();
      stream.destroy();
    }
  } catch (_e) {}
  return result;
}

async function copilotOtelCursorHasLegacyCliUsage(cursors) {
  const state =
    cursors?.copilot && typeof cursors.copilot === "object"
      ? cursors.copilot
      : {};
  if (state.legacyCliHistory === true) return true;
  if (state.usageClaimsComplete === true) return false;
  const offsets =
    state.fileOffsets && typeof state.fileOffsets === "object"
      ? state.fileOffsets
      : {};
  const entries = Object.entries(offsets).filter(
    ([, entry]) => Number(entry?.size) > 0,
  );
  const hasSeenIds =
    Array.isArray(state.seenIds) && state.seenIds.length > 0;
  if (entries.length === 0) return hasSeenIds;
  for (const [filePath, entry] of entries) {
    let stat;
    try {
      stat = fssync.statSync(filePath);
    } catch (_e) {
      return true;
    }
    if (
      stat.size < Number(entry?.size) ||
      (typeof entry?.ino === "number" && stat.ino !== entry.ino)
    ) {
      return true;
    }
    const scan = await scanCopilotV1MigrationFile(
      filePath,
      Number(entry?.size) || 0,
    );
    if (scan.v1Processed) return true;
  }
  return false;
}

async function parseCopilotIncremental({
  otelPaths,
  cursors,
  queuePath,
  onProgress,
  env,
  skipCliSpans = false,
  storeUsageEvents,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const copilotState = cursors.copilot && typeof cursors.copilot === "object" ? cursors.copilot : {};
  const seenIds = new Set(Array.isArray(copilotState.seenIds) ? copilotState.seenIds : []);
  const priorVersion = Number(copilotState.version) || 1;
  const fileOffsetsRaw =
    copilotState.fileOffsets && typeof copilotState.fileOffsets === "object"
      ? copilotState.fileOffsets
      : {};
  const fileOffsets = { ...fileOffsetsRaw };
  const claimNowMs = Date.now();
  const recentOtelUsageEvents = pruneCopilotUsageClaims(
    copilotState.recentUsageEvents,
    claimNowMs,
  );
  replaceCopilotUsageClaims(storeUsageEvents, claimNowMs, Infinity);
  const hadPriorUsageHistory =
    seenIds.size > 0 || Object.keys(fileOffsetsRaw).length > 0;
  let usageClaimsComplete =
    copilotState.usageClaimsComplete === true || !hadPriorUsageHistory;
  const files = Array.isArray(otelPaths) && otelPaths.length > 0
    ? otelPaths
    : resolveCopilotOtelPaths(env || process.env);
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  const migrationSkipLineHashes = new Map();
  let cursorVersion = COPILOT_PARSER_VERSION;
  // One-shot v1->v2 migration:
  // - pure v2-only files: clear offset and re-read all skipped Chat records
  // - pure v1 CLI files: preserve offset to avoid replaying history beyond seenIds
  // - mixed files: clear offset, but skip old v1 CLI lines by hash during replay
  if (priorVersion < 2) {
    for (const filePath of Object.keys(fileOffsets)) {
      const prevSize = Number(fileOffsets[filePath]?.size) || 0;
      const scan = await scanCopilotV1MigrationFile(filePath, prevSize);
      if (!scan.v1Processed) {
        delete fileOffsets[filePath];
      } else if (scan.v2OnlyChat) {
        delete fileOffsets[filePath];
        migrationSkipLineHashes.set(filePath, scan.v1LineHashes);
      }
    }
  }

  // v2 used spanContext as the fallback key for Chat-extension LogRecords.
  // Those records can share one context across multiple model requests, so
  // repair the already-counted file prefix before switching to response.id.
  // If the prefix cannot be verified, leave the cursor at v2 and retry on the
  // next sync rather than replaying it with a different deduplication scheme.
  if (priorVersion === 2) {
    const migration = await migrateCopilotChatLogRecordDedup({
      files,
      fileOffsets,
      hourlyState,
      touchedBuckets,
      seenIds,
    });
    if (!migration.applied) {
      return {
        recordsProcessed: 0,
        eventsAggregated: 0,
        bucketsQueued: 0,
        usageClaims: recentOtelUsageEvents,
      };
    }
  }

  if (files.length === 0) {
    cursors.copilot = {
      ...copilotState,
      version: cursorVersion,
      seenIds: Array.from(seenIds),
      fileOffsets,
      recentUsageEvents: recentOtelUsageEvents,
      usageClaimsComplete,
      updatedAt: new Date().toISOString(),
    };
    return {
      recordsProcessed: 0,
      eventsAggregated: 0,
      bucketsQueued: 0,
      usageClaims: recentOtelUsageEvents,
    };
  }
  const cb = typeof onProgress === "function" ? onProgress : null;
  let recordsProcessed = 0;
  let eventsAggregated = 0;
  const storeUsageMatcher = createCopilotStoreUsageMatcher(storeUsageEvents);

  for (let fileIdx = 0; fileIdx < files.length; fileIdx++) {
    const filePath = files[fileIdx];
    let stat;
    try {
      stat = fssync.statSync(filePath);
    } catch (_e) {
      continue;
    }
    const prevEntry = fileOffsets[filePath] || {};
    const prevSize = Number(prevEntry.size) || 0;
    const prevIno = prevEntry.ino;
    // Re-read from start if (a) file shrunk (truncate/rewrite in place) or
    // (b) inode changed (rotator deleted + recreated at same path). Without
    // the inode check, a rotator producing a same-or-larger file would leave
    // the old offset stuck and skip the new file's prefix forever.
    const inodeChanged = typeof prevIno === "number" && prevIno !== stat.ino;
    const startOffset = stat.size < prevSize || inodeChanged ? 0 : prevSize;
    if (stat.size <= startOffset) continue;

    let stream;
    try {
      stream = fssync.createReadStream(filePath, { encoding: "utf8", start: startOffset });
    } catch (_e) {
      continue;
    }
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

    for await (const line of rl) {
      if (!line || !line.trim()) continue;
      const skipLineHashes = migrationSkipLineHashes.get(filePath);
      if (skipLineHashes && skipLineHashes.size > 0) {
        const lineHash = copilotLineHash(line);
        const skipCount = skipLineHashes.get(lineHash) || 0;
        if (skipCount > 0) {
          if (skipCount === 1) skipLineHashes.delete(lineHash);
          else skipLineHashes.set(lineHash, skipCount - 1);
          continue;
        }
      }
      let record;
      try {
        record = JSON.parse(line);
      } catch (_e) {
        continue;
      }
      recordsProcessed++;
      if (!isCopilotChatSpan(record)) continue;

      const attrs = record.attributes || {};
      // Dedup: CLI puts traceId/spanId at the top level; the Chat extension
      // file exporter writes LogRecord-shaped entries without either, but
      // gen_ai.response.id is per-LLM-call unique.
      const dedupKey = getCopilotDedupKey(record, attrs);
      if (dedupKey && seenIds.has(dedupKey)) continue;
      const cliSpan = isCopilotV1ChatSpan(record);
      if (skipCliSpans && cliSpan) {
        if (dedupKey) seenIds.add(dedupKey);
        continue;
      }

      const usage = extractCopilotOtelUsage(record);
      if (!usage) continue;
      if (usage.cliSpan && !usage.cliSessionId) usageClaimsComplete = false;
      const matchedStoreUsage =
        usage.cliSpan &&
        storeUsageMatcher.consume({
          ...usage.matchBase,
          input: usage.delta.input_tokens,
        });
      if (matchedStoreUsage) {
        if (dedupKey) seenIds.add(dedupKey);
        continue;
      }

      const bucket = getHourlyBucket(
        hourlyState,
        "copilot",
        usage.model,
        usage.bucketStart,
      );
      addTotals(bucket.totals, usage.delta);
      touchedBuckets.add(bucketKey("copilot", usage.model, usage.bucketStart));
      eventsAggregated++;
      if (usage.cliSpan && usage.cliSessionId) {
        recentOtelUsageEvents.push({
          ...usage.matchBase,
          input: usage.delta.input_tokens,
          firstSeenAtMs: claimNowMs,
        });
      }
      if (dedupKey) seenIds.add(dedupKey);

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

    // Re-stat after readline drains: file may have been appended during the
    // parse loop. Without this, those new lines would be replayed next run
    // (dedup catches records with traceId+spanId, but spans missing either
    // would be double-counted).
    let postStat = stat;
    try {
      postStat = fssync.statSync(filePath);
    } catch (_e) {}
    fileOffsets[filePath] = { size: postStat.size, mtimeMs: postStat.mtimeMs, ino: postStat.ino };
  }

  // Cap dedup set to last 10k IDs to bound state size
  const seenArr = Array.from(seenIds);
  const cappedSeen = seenArr.length > 10_000 ? seenArr.slice(seenArr.length - 10_000) : seenArr;
  const retainedRecentOtelUsageEvents = pruneCopilotUsageClaims(
    recentOtelUsageEvents,
  );
  replaceCopilotUsageClaims(storeUsageEvents, Date.now(), Infinity);

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.copilot = {
    ...copilotState,
    version: cursorVersion,
    seenIds: cappedSeen,
    fileOffsets,
    recentUsageEvents: retainedRecentOtelUsageEvents,
    usageClaimsComplete,
    updatedAt,
  };

  return {
    recordsProcessed,
    eventsAggregated,
    bucketsQueued,
    usageClaims: recentOtelUsageEvents,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// GitHub Copilot local runtime — session-store.db assistant_usage_events
//
// Copilot CLI 1.0.70+ persists one row per LLM request here. The Copilot App
// uses the same runtime, so this is the only local source that remains complete
// when one session moves App -> CLI -> App (or the reverse). data.db only keeps
// the App-owned portion of those sessions.
// ─────────────────────────────────────────────────────────────────────────────

const COPILOT_STORE_CURSOR_VERSION = 2;
const COPILOT_STORE_LEGACY_MODEL = "github-copilot-legacy";

function resolveCopilotDbPaths({ fileName, overrideEnvKey }, env = process.env) {
  const home =
    process.platform === "win32"
      ? env.USERPROFILE || env.HOME || os.homedir()
      : env.HOME || os.homedir();
  const paths = new Set();
  const addDbPath = (dbPath) => {
    const normalized = normalizeCopilotDbPath(dbPath, env);
    if (normalized) paths.add(normalized);
  };
  const addHome = (homePath) => {
    const normalizedHome = normalizeCopilotDbPath(homePath, env);
    if (normalizedHome) addDbPath(path.join(normalizedHome, fileName));
  };

  if (typeof env[overrideEnvKey] === "string" && env[overrideEnvKey].trim()) {
    addDbPath(env[overrideEnvKey]);
  }
  if (typeof env.COPILOT_HOME === "string" && env.COPILOT_HOME.trim()) {
    addHome(env.COPILOT_HOME);
  }

  addDbPath(path.join(home, ".copilot", fileName));

  return Array.from(paths).sort();
}

function resolveCopilotSessionStorePaths(env = process.env) {
  return resolveCopilotDbPaths(
    {
      fileName: "session-store.db",
      overrideEnvKey: "TOKEN_PULSE_COPILOT_SESSION_STORE_DB",
    },
    env,
  );
}

function copilotStoreRowKey(row) {
  if (!row) return null;
  return crypto
    .createHash("sha256")
    .update(
      JSON.stringify([
        row.max_id ?? row.id,
        row.session_id,
        row.model,
        row.input_tokens,
        row.output_tokens,
        row.cache_read_tokens,
        row.cache_write_tokens,
        row.reasoning_tokens,
        row.created_at,
      ]),
    )
    .digest("hex");
}

function copilotStoreEventFingerprint(row) {
  const sessionId =
    typeof row?.session_id === "string" ? row.session_id.trim() : "";
  if (!sessionId) return null;
  const normalized = normalizeCopilotSessionStoreUsage(row);
  const timestamp =
    parseCopilotAppTimestamp(row?.created_at) ||
    `invalid:${String(row?.created_at ?? "")}`;
  return crypto
    .createHash("sha256")
    .update(
      JSON.stringify([
        sessionId,
        normalizeCopilotAppModel(row?.model) || COPILOT_APP_DEFAULT_MODEL,
        normalized.input_tokens,
        normalized.output_tokens,
        normalized.cached_input_tokens,
        normalized.cache_creation_input_tokens,
        normalized.reasoning_output_tokens,
        timestamp,
      ]),
    )
    .digest("hex");
}

function normalizeCopilotFingerprintCounts(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  const result = {};
  for (const [fingerprint, count] of Object.entries(value)) {
    const normalized = toNonNegativeInt(count);
    if (/^[a-f0-9]{64}$/.test(fingerprint) && normalized > 0) {
      result[fingerprint] = normalized;
    }
  }
  return result;
}

function normalizeCopilotPendingMalformedEvents(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  const result = {};
  for (const [id, fingerprint] of Object.entries(value)) {
    const normalizedId = toNonNegativeInt(id);
    if (normalizedId > 0) {
      result[normalizedId] =
        typeof fingerprint === "string" ? fingerprint : "";
    }
  }
  return result;
}

function incrementCopilotFingerprintCount(counts, fingerprint) {
  if (!fingerprint) return;
  counts[fingerprint] = toNonNegativeInt(counts[fingerprint]) + 1;
}

function latestCopilotTimestamp(first, second) {
  const values = [first, second]
    .map((value) => parseCopilotAppTimestamp(value))
    .filter(Boolean);
  if (values.length === 0) return null;
  return values.reduce((latest, value) => (value > latest ? value : latest));
}

function omitCopilotLegacyCatchupState(state) {
  const clean = { ...(state || {}) };
  delete clean.pendingCatchupMaxId;
  delete clean.pendingCatchupFingerprint;
  delete clean.pendingLegacyCatchup;
  return clean;
}

function readCopilotSessionStoreMetadata(
  dbPath,
  sqliteOptions = {},
  cursorId = 0,
) {
  const tables = readSqliteJsonRows(
    dbPath,
    "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('assistant_usage_events', 'schema_version')",
    {
      label: "GitHub Copilot session store",
      maxBuffer: 4 * 1024 * 1024,
      timeout: 10_000,
      readOnly: true,
      throwOnReadFailure: true,
      ...sqliteOptions,
    },
  );
  const names = new Set(tables.map((row) => row?.name).filter(Boolean));
  if (!names.has("assistant_usage_events")) {
    return { active: false, schemaVersion: null, maxId: 0 };
  }
  const maxRows = readSqliteJsonRows(
    dbPath,
    `
      SELECT
        id AS max_id,
        session_id,
        model,
        input_tokens,
        output_tokens,
        cache_read_tokens,
        cache_write_tokens,
        reasoning_tokens,
        created_at
      FROM assistant_usage_events
      ORDER BY id DESC
      LIMIT 1
    `.trim(),
    {
      label: "GitHub Copilot session store",
      maxBuffer: 4 * 1024 * 1024,
      timeout: 10_000,
      readOnly: true,
      throwOnReadFailure: true,
      ...sqliteOptions,
    },
  );
  let schemaVersion = null;
  if (names.has("schema_version")) {
    const versionRows = readSqliteJsonRows(
      dbPath,
      "SELECT version FROM schema_version LIMIT 1",
      {
        label: "GitHub Copilot session store",
        maxBuffer: 4 * 1024 * 1024,
        timeout: 10_000,
        readOnly: true,
        throwOnReadFailure: true,
        ...sqliteOptions,
      },
    );
    schemaVersion = toNonNegativeInt(versionRows[0]?.version);
  }
  let cursorRowKey = null;
  const normalizedCursorId = toNonNegativeInt(cursorId);
  if (normalizedCursorId > 0) {
    if (normalizedCursorId === toNonNegativeInt(maxRows[0]?.max_id)) {
      cursorRowKey = copilotStoreRowKey(maxRows[0]);
    } else {
      const cursorRows = readSqliteJsonRows(
        dbPath,
        `
          SELECT
            id,
            session_id,
            model,
            input_tokens,
            output_tokens,
            cache_read_tokens,
            cache_write_tokens,
            reasoning_tokens,
            created_at
          FROM assistant_usage_events
          WHERE id = ${normalizedCursorId}
          LIMIT 1
        `.trim(),
        {
          label: "GitHub Copilot session store",
          maxBuffer: 4 * 1024 * 1024,
          timeout: 10_000,
          readOnly: true,
          throwOnReadFailure: true,
          ...sqliteOptions,
        },
      );
      cursorRowKey = copilotStoreRowKey(cursorRows[0]);
    }
  }
  return {
    active: true,
    schemaVersion,
    maxId: toNonNegativeInt(maxRows[0]?.max_id),
    lastRowKey: copilotStoreRowKey(maxRows[0]),
    lastEventAt: parseCopilotAppTimestamp(maxRows[0]?.created_at),
    cursorRowKey,
  };
}

function describeCopilotSessionStoreDb(dbPath, sqliteOptions = {}) {
  let snap = null;
  try {
    snap = snapshotSqliteDb(dbPath);
    const metadata = readCopilotSessionStoreMetadata(snap.path, sqliteOptions);
    if (!metadata.active) {
      return {
        path: dbPath,
        active: false,
        schemaVersion: metadata.schemaVersion,
        eventCount: 0,
        lastEventId: 0,
        lastEventAt: null,
      };
    }
    const countRows = readSqliteJsonRows(
      snap.path,
      "SELECT COUNT(*) AS event_count FROM assistant_usage_events",
      {
        label: "GitHub Copilot session store",
        maxBuffer: 4 * 1024 * 1024,
        timeout: 10_000,
        readOnly: true,
        throwOnReadFailure: true,
        ...sqliteOptions,
      },
    );
    return {
      path: dbPath,
      active: true,
      schemaVersion: metadata.schemaVersion,
      eventCount: toNonNegativeInt(countRows[0]?.event_count),
      lastEventId: metadata.maxId,
      lastEventAt: metadata.lastEventAt,
    };
  } finally {
    if (snap) snap.cleanup();
  }
}

function readCopilotSessionStoreUsageRowsWhere(
  dbPath,
  whereClause,
  sqliteOptions = {},
) {
  return readSqliteJsonRows(
    dbPath,
    `
      SELECT
        id,
        session_id,
        model,
        input_tokens,
        output_tokens,
        cache_read_tokens,
        cache_write_tokens,
        reasoning_tokens,
        token_details_json,
        created_at
      FROM assistant_usage_events
      WHERE ${whereClause}
      ORDER BY id ASC
    `.trim(),
    {
      label: "GitHub Copilot session store",
      maxBuffer: 64 * 1024 * 1024,
      timeout: 60_000,
      readOnly: true,
      throwOnReadFailure: true,
      ...sqliteOptions,
    },
  );
}

function readCopilotSessionStoreUsageRows(dbPath, lastId, sqliteOptions = {}) {
  return readCopilotSessionStoreUsageRowsWhere(
    dbPath,
    `id > ${toNonNegativeInt(lastId)}`,
    sqliteOptions,
  );
}

function readCopilotSessionStoreUsageRowsByIds(
  dbPath,
  ids,
  sqliteOptions = {},
) {
  const normalizedIds = [
    ...new Set(
      (Array.isArray(ids) ? ids : [])
        .map(toNonNegativeInt)
        .filter((id) => id > 0),
    ),
  ];
  const rows = [];
  for (let start = 0; start < normalizedIds.length; start += 500) {
    const chunk = normalizedIds.slice(start, start + 500);
    rows.push(
      ...readCopilotSessionStoreUsageRowsWhere(
        dbPath,
        `id IN (${chunk.join(",")})`,
        sqliteOptions,
      ),
    );
  }
  return rows.sort((a, b) => toNonNegativeInt(a?.id) - toNonNegativeInt(b?.id));
}

function parseCopilotStoreTokenDetails(raw) {
  if (typeof raw !== "string" || !raw.trim()) return null;
  let entries;
  try {
    entries = JSON.parse(raw);
  } catch (_e) {
    return null;
  }
  if (!Array.isArray(entries)) return null;
  const totals = { input: 0, cacheRead: 0, cacheWrite: 0, output: 0 };
  let recognized = 0;
  for (const entry of entries) {
    if (!entry || typeof entry !== "object") continue;
    const count = toNonNegativeInt(entry.tokenCount);
    switch (entry.tokenType) {
      case "input":
        totals.input += count;
        recognized++;
        break;
      case "cache_read":
        totals.cacheRead += count;
        recognized++;
        break;
      case "cache_write":
        totals.cacheWrite += count;
        recognized++;
        break;
      case "output":
        totals.output += count;
        recognized++;
        break;
      default:
        break;
    }
  }
  return recognized > 0 ? totals : null;
}

function copilotStoreRecentEvent(row, firstSeenAtMs = Date.now()) {
  const tsIso = parseCopilotAppTimestamp(row?.created_at);
  const tsMs = tsIso ? Date.parse(tsIso) : NaN;
  const sessionId =
    typeof row?.session_id === "string" ? row.session_id.trim() : "";
  if (!sessionId || !Number.isFinite(tsMs) || tsMs <= 0) return null;
  const normalized = normalizeCopilotSessionStoreUsage(row);
  return {
    id: toNonNegativeInt(row?.id),
    sessionId,
    model: normalizeCopilotAppModel(row?.model) || COPILOT_APP_DEFAULT_MODEL,
    input: normalized.input_tokens,
    output: normalized.output_tokens,
    cacheRead: normalized.cached_input_tokens,
    cacheWrite: normalized.cache_creation_input_tokens,
    reasoning: normalized.reasoning_output_tokens,
    tsMs,
    firstSeenAtMs,
  };
}

function normalizeCopilotSessionStoreUsage(row) {
  const inputRaw = toNonNegativeInt(row?.input_tokens);
  const outputRaw = toNonNegativeInt(row?.output_tokens);
  const reasoning = toNonNegativeInt(row?.reasoning_tokens);
  const details = parseCopilotStoreTokenDetails(row?.token_details_json);
  let input;
  let cacheRead;
  let cacheWrite;
  let output;
  let precision = "fallback";

  if (
    details &&
    details.input + details.cacheRead + details.cacheWrite === inputRaw &&
    details.output === outputRaw
  ) {
    input = details.input;
    cacheRead = details.cacheRead;
    cacheWrite = details.cacheWrite;
    output = details.output;
    precision = "exact";
  } else {
    cacheRead = Math.min(toNonNegativeInt(row?.cache_read_tokens), inputRaw);
    cacheWrite = Math.min(
      toNonNegativeInt(row?.cache_write_tokens),
      Math.max(0, inputRaw - cacheRead),
    );
    input = Math.max(0, inputRaw - cacheRead - cacheWrite);
    output = outputRaw;
  }

  const reasoningClamped = Math.min(reasoning, output);
  const outputWithoutReasoning = Math.max(0, output - reasoningClamped);
  return {
    input_tokens: input,
    cached_input_tokens: cacheRead,
    cache_creation_input_tokens: cacheWrite,
    output_tokens: outputWithoutReasoning,
    reasoning_output_tokens: reasoningClamped,
    total_tokens:
      input + cacheRead + cacheWrite + outputWithoutReasoning + reasoningClamped,
    precision,
  };
}

async function parseCopilotSessionStoreIncremental({
  dbPath,
  dbPaths,
  cursors,
  queuePath,
  onProgress,
  env,
  sqliteOptions,
  backfillOnFirstRun = false,
  excludeSessionIdsOnFirstRun,
  excludeSessionTotalsOnFirstRun,
  expectedFingerprints,
  otelUsageEvents,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const paths = Array.isArray(dbPaths)
    ? dbPaths
    : dbPath
      ? [dbPath]
      : resolveCopilotSessionStorePaths(env || process.env);
  const storeState =
    cursors.copilotStore && typeof cursors.copilotStore === "object"
      ? cursors.copilotStore
      : {};
  const dbStates =
    storeState.dbs && typeof storeState.dbs === "object" ? { ...storeState.dbs } : {};
  let uniquePaths = uniqueCopilotDbPaths(
    paths,
    env || process.env,
    Object.keys(dbStates),
  );
  const migratedAliasPaths = coalesceCopilotDbStatesByIdentity(
    dbStates,
    uniquePaths,
    mergeCopilotStoreAliasStates,
  );
  uniquePaths = uniquePaths.filter(
    (dbPath) => !migratedAliasPaths.has(dbPath),
  );
  const seenSessions = new Set(
    Array.isArray(storeState.seenSessions) ? storeState.seenSessions : [],
  );
  const excludedFirstRunSessions = new Set(
    Array.isArray(excludeSessionIdsOnFirstRun)
      ? excludeSessionIdsOnFirstRun.filter((value) => typeof value === "string" && value)
      : [],
  );
  const excludedFirstRunTotals =
    excludeSessionTotalsOnFirstRun &&
    typeof excludeSessionTotalsOnFirstRun === "object" &&
    !Array.isArray(excludeSessionTotalsOnFirstRun)
      ? excludeSessionTotalsOnFirstRun
      : {};
  const legacyResiduals = new Map();
  const claimNowMs = Date.now();
  const recentEvents = pruneCopilotUsageClaims(
    storeState.recentEvents,
    claimNowMs,
  );
  replaceCopilotUsageClaims(otelUsageEvents, claimNowMs, Infinity);
  const otelUsageMatcher = createCopilotStoreUsageMatcher(otelUsageEvents);
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;
  const updatedAt = new Date(claimNowMs).toISOString();
  let activeDbCount = 0;
  let adoptedThisRun = false;
  let recordsProcessed = 0;
  let eventsAggregated = 0;
  let dbErrors = 0;
  const canonicalDbPaths = [];
  const globalAdoptionPending = storeState.active !== true;

  for (const resolvedDb of uniquePaths) {
    const dbState =
      dbStates[resolvedDb] && typeof dbStates[resolvedDb] === "object"
        ? dbStates[resolvedDb]
        : {};
    const seenEventCounts = normalizeCopilotFingerprintCounts(
      dbState.seenEventCounts,
    );
    let pendingMalformedEvents = normalizeCopilotPendingMalformedEvents(
      dbState.pendingMalformedEvents,
    );
    let fingerprint = null;
    let snap = null;
    try {
      fingerprint = sqliteSidecarFingerprint(resolvedDb);
      const expectedFingerprint = expectedFingerprints?.[resolvedDb];
      if (
        expectedFingerprint &&
        !sameSqliteFingerprint(fingerprint, expectedFingerprint)
      ) {
        dbStates[resolvedDb] = {
          ...omitCopilotLegacyCatchupState(dbState),
          adoptionDeferredAt: updatedAt,
          updatedAt,
        };
        continue;
      }
      const currentIno = fingerprint?.db?.ino;
      const priorLastId = toNonNegativeInt(dbState.lastId);
      snap = snapshotSqliteDb(resolvedDb);
      const metadata = readCopilotSessionStoreMetadata(
        snap.path,
        sqliteOptions,
        priorLastId,
      );
      if (!metadata.active) {
        dbStates[resolvedDb] = {
          ...omitCopilotLegacyCatchupState(dbState),
          schemaVersion: metadata.schemaVersion,
          lastError: null,
          lastErrorAt: null,
          updatedAt,
        };
        continue;
      }

      const inodeChanged =
        typeof dbState.dbIno === "number" &&
        typeof currentIno === "number" &&
        dbState.dbIno !== currentIno;
      const cursorRowChanged =
        priorLastId > 0 &&
        typeof dbState.lastRowKey === "string" &&
        dbState.lastRowKey !== metadata.cursorRowKey;
      const idRegressed = metadata.maxId < priorLastId;
      const resetDetected =
        Boolean(dbState.adoptedAt) && (idRegressed || cursorRowChanged);
      const needsFingerprintSeed =
        Boolean(dbState.adoptedAt) &&
        priorLastId > 0 &&
        Object.keys(seenEventCounts).length === 0;
      const resetWithoutFingerprintHistory =
        resetDetected && needsFingerprintSeed;
      const reconcileAllRows =
        (resetDetected || inodeChanged) && !resetWithoutFingerprintHistory;
      const needsAdoption =
        globalAdoptionPending ||
        !dbState.adoptedAt ||
        resetDetected;
      const baselineOnly =
        resetWithoutFingerprintHistory ||
        (needsAdoption && !resetDetected && !backfillOnFirstRun);
      const incrementalRows = readCopilotSessionStoreUsageRows(
        snap.path,
        reconcileAllRows || baselineOnly || needsFingerprintSeed ? 0 : priorLastId,
        sqliteOptions,
      );
      const pendingRetryRows = resetDetected || reconcileAllRows
        ? []
        : readCopilotSessionStoreUsageRowsByIds(
            snap.path,
            Object.keys(pendingMalformedEvents),
            sqliteOptions,
          );
      const pendingRetryIds = new Set(
        pendingRetryRows.map((row) => toNonNegativeInt(row?.id)),
      );
      if (resetDetected || reconcileAllRows) {
        pendingMalformedEvents = {};
      } else {
        for (const pendingId of Object.keys(pendingMalformedEvents)) {
          if (!pendingRetryIds.has(toNonNegativeInt(pendingId))) {
            delete pendingMalformedEvents[pendingId];
          }
        }
      }
      const rowsById = new Map();
      for (const row of [...pendingRetryRows, ...incrementalRows]) {
        rowsById.set(toNonNegativeInt(row?.id), row);
      }
      const rows = Array.from(rowsById.values()).sort(
        (a, b) => toNonNegativeInt(a?.id) - toNonNegativeInt(b?.id),
      );
      // Reindex/reset can rewrite every SQLite id. Subtract the exact metadata
      // multiset already observed so only genuinely new requests are emitted.
      const historicalFingerprintCounts = reconcileAllRows
        ? new Map(
            Object.entries(seenEventCounts).map(([eventFingerprint, count]) => [
              eventFingerprint,
              toNonNegativeInt(count),
            ]),
          )
        : null;
      if (needsAdoption) adoptedThisRun = true;
      if (expectedFingerprint) {
        const finalFingerprint = sqliteSidecarFingerprint(resolvedDb);
        if (!sameSqliteFingerprint(finalFingerprint, expectedFingerprint)) {
          dbStates[resolvedDb] = {
            ...omitCopilotLegacyCatchupState(dbState),
            adoptionDeferredAt: updatedAt,
            updatedAt,
          };
          continue;
        }
        fingerprint = finalFingerprint;
      }
      let lastId =
        reconcileAllRows || baselineOnly ? metadata.maxId : priorLastId;
      for (const row of rows) {
        recordsProcessed++;
        const rowId = toNonNegativeInt(row?.id);
        lastId = Math.max(lastId, rowId);
        const sessionId =
          typeof row?.session_id === "string" ? row.session_id.trim() : "";
        if (!sessionId) {
          if (rowId > 0) pendingMalformedEvents[rowId] = "";
          continue;
        }
        const eventFingerprint = copilotStoreEventFingerprint(row);
        if (historicalFingerprintCounts && eventFingerprint) {
          const historicalCount = toNonNegativeInt(
            historicalFingerprintCounts.get(eventFingerprint),
          );
          if (historicalCount > 0) {
            historicalFingerprintCounts.set(
              eventFingerprint,
              historicalCount - 1,
            );
            if (!parseCopilotAppTimestamp(row?.created_at) && rowId > 0) {
              pendingMalformedEvents[rowId] = eventFingerprint;
            }
            seenSessions.add(sessionId);
            continue;
          }
        }
        const seedOnly =
          needsFingerprintSeed && rowId <= priorLastId;
        const recentEvent = copilotStoreRecentEvent(row, claimNowMs);
        if (pendingRetryIds.has(rowId) && !recentEvent) {
          if (seedOnly) {
            incrementCopilotFingerprintCount(
              seenEventCounts,
              eventFingerprint,
            );
          }
          pendingMalformedEvents[rowId] = eventFingerprint || "";
          seenSessions.add(sessionId);
          continue;
        }
        if (rowId > 0) delete pendingMalformedEvents[rowId];
        const matchedOtelUsage =
          recentEvent && otelUsageMatcher.consume(recentEvent);
        if (matchedOtelUsage) recentEvent.consumed = true;
        if (baselineOnly || seedOnly) {
          incrementCopilotFingerprintCount(seenEventCounts, eventFingerprint);
          if (!recentEvent && rowId > 0) {
            pendingMalformedEvents[rowId] = eventFingerprint || "";
          }
          seenSessions.add(sessionId);
          continue;
        }
        incrementCopilotFingerprintCount(seenEventCounts, eventFingerprint);
        if (needsAdoption && backfillOnFirstRun && excludedFirstRunSessions.has(sessionId)) {
          if (
            !matchedOtelUsage &&
            Object.prototype.hasOwnProperty.call(
              excludedFirstRunTotals,
              sessionId,
            )
          ) {
            const normalized = normalizeCopilotSessionStoreUsage(row);
            const priorResidual = legacyResiduals.get(sessionId) || {
              storeTokens: 0,
              lastEventAt: null,
            };
            priorResidual.storeTokens += normalized.total_tokens;
            priorResidual.lastEventAt = latestCopilotTimestamp(
              priorResidual.lastEventAt,
              row?.created_at,
            );
            legacyResiduals.set(sessionId, priorResidual);
          }
          if (recentEvent && !matchedOtelUsage) recentEvents.push(recentEvent);
          seenSessions.add(sessionId);
          continue;
        }
        if (matchedOtelUsage) {
          seenSessions.add(sessionId);
          continue;
        }
        if (recentEvent) recentEvents.push(recentEvent);
        const normalized = normalizeCopilotSessionStoreUsage(row);
        if (normalized.total_tokens <= 0) {
          seenSessions.add(sessionId);
          continue;
        }
        const tsIso = parseCopilotAppTimestamp(row.created_at);
        const bucketStart = tsIso ? toUtcHalfHourStart(tsIso) : null;
        if (!bucketStart) {
          if (rowId > 0) {
            pendingMalformedEvents[rowId] = eventFingerprint || "";
          }
          seenSessions.add(sessionId);
          continue;
        }
        const model =
          normalizeCopilotAppModel(row.model) || COPILOT_APP_DEFAULT_MODEL;
        const delta = {
          input_tokens: normalized.input_tokens,
          cached_input_tokens: normalized.cached_input_tokens,
          cache_creation_input_tokens: normalized.cache_creation_input_tokens,
          output_tokens: normalized.output_tokens,
          reasoning_output_tokens: normalized.reasoning_output_tokens,
          total_tokens: normalized.total_tokens,
          conversation_count: seenSessions.has(sessionId) ? 0 : 1,
        };
        const bucket = getHourlyBucket(hourlyState, "copilot", model, bucketStart);
        addTotals(bucket.totals, delta);
        touchedBuckets.add(bucketKey("copilot", model, bucketStart));
        seenSessions.add(sessionId);
        eventsAggregated++;
        if (cb) {
          cb({
            index: recordsProcessed,
            total: rows.length,
            recordsProcessed,
            eventsAggregated,
            bucketsQueued: touchedBuckets.size,
          });
        }
      }

      dbStates[resolvedDb] = {
        ...omitCopilotLegacyCatchupState(dbState),
        adoptedAt: dbState.adoptedAt || updatedAt,
        resetAt: resetDetected ? updatedAt : dbState.resetAt || null,
        schemaVersion: metadata.schemaVersion,
        lastRowKey: metadata.lastRowKey,
        lastId: Math.max(lastId, metadata.maxId),
        lastEventAt: latestCopilotTimestamp(
          dbState.lastEventAt,
          metadata.lastEventAt,
        ),
        seenEventCounts,
        pendingMalformedEvents,
        malformedEventCount: Object.keys(pendingMalformedEvents).length,
        resetGapEventCount:
          toNonNegativeInt(dbState.resetGapEventCount) +
          (resetWithoutFingerprintHistory ? rows.length : 0),
        dbIno: typeof currentIno === "number" ? currentIno : null,
        lastDbFingerprint: fingerprint,
        lastError: null,
        lastErrorAt: null,
        updatedAt,
      };
      canonicalDbPaths.push(resolvedDb);
      activeDbCount++;
    } catch (err) {
      if (err?.code !== "ENOENT") dbErrors++;
      dbStates[resolvedDb] = {
        ...omitCopilotLegacyCatchupState(dbState),
        lastError: err && err.message ? err.message : String(err),
        lastErrorAt: updatedAt,
        updatedAt,
      };
    } finally {
      if (snap) snap.cleanup();
    }
  }

  for (const [sessionId, residual] of legacyResiduals) {
    const baseline = excludedFirstRunTotals[sessionId] || {};
    const appTotals = normalizeCopilotAppTokenDelta(baseline, {
      input: 0,
      output: 0,
      cached: 0,
      reasoning: 0,
    });
    const residualTokens = Math.max(
      0,
      toNonNegativeInt(residual.storeTokens) - appTotals.total_tokens,
    );
    if (residualTokens <= 0) continue;
    const timestamp = latestCopilotTimestamp(
      residual.lastEventAt,
      baseline.updatedAt,
    );
    const bucketStart = timestamp ? toUtcHalfHourStart(timestamp) : null;
    if (!bucketStart) continue;
    const bucket = getHourlyBucket(
      hourlyState,
      "copilot",
      COPILOT_STORE_LEGACY_MODEL,
      bucketStart,
    );
    addTotals(bucket.totals, {
      input_tokens: residualTokens,
      cached_input_tokens: 0,
      cache_creation_input_tokens: 0,
      output_tokens: 0,
      reasoning_output_tokens: 0,
      total_tokens: residualTokens,
      conversation_count: 0,
    });
    touchedBuckets.add(
      bucketKey("copilot", COPILOT_STORE_LEGACY_MODEL, bucketStart),
    );
    eventsAggregated++;
  }

  const bucketsQueued = await enqueueTouchedBuckets({
    queuePath,
    hourlyState,
    touchedBuckets,
  });
  const allCurrentPathsHealthy =
    uniquePaths.length > 0 &&
    activeDbCount === uniquePaths.length &&
    dbErrors === 0;
  const canonicalActive = storeState.active === true || allCurrentPathsHealthy;
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.copilotStore = {
    ...omitCopilotLegacyCatchupState(storeState),
    version: COPILOT_STORE_CURSOR_VERSION,
    active: canonicalActive,
    dbs: dbStates,
    seenSessions: Array.from(seenSessions),
    recentEvents: pruneCopilotUsageClaims(recentEvents),
    updatedAt,
  };
  return {
    active: canonicalActive,
    healthy: allCurrentPathsHealthy,
    adoptedThisRun,
    canonicalDbPaths,
    recordsProcessed,
    eventsAggregated,
    bucketsQueued,
    dbErrors,
    usageClaims: recentEvents,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// GitHub Copilot App — passive reader for ~/.copilot/data.db
//
// The Copilot App persists one row per local App session in `sessions`, with
// cumulative `total_*` token summary columns. We intentionally do NOT read
// session-state/<id>/events.jsonl: forked App sessions inherit parent events
// there before the child has produced any tokens, so parsing event history would
// double-count parent usage. The only accounting source here is positive deltas
// from the `sessions.total_*` counters. Those deltas feed the same aggregate
// `source="copilot"` pipeline during legacy adoption. Once session-store is
// canonical, this parser runs observe-only so a temporary store outage cannot
// create duplicate fallback writes when the store returns.
// Project attribution and per-conversation detail are intentionally absent:
// data.db's summary columns do not expose privacy-safe project/message metadata.
// ─────────────────────────────────────────────────────────────────────────────

const COPILOT_APP_CURSOR_VERSION = 1;
const COPILOT_APP_DEFAULT_MODEL = "github-copilot";

function normalizeCopilotDbPath(dbPath, env = process.env) {
  if (typeof dbPath !== "string" || !dbPath.trim()) return null;
  const raw = dbPath.trim();
  const home =
    process.platform === "win32"
      ? env.USERPROFILE || env.HOME || os.homedir()
      : env.HOME || os.homedir();
  const expanded =
    raw === "~"
      ? home
      : raw.startsWith("~/") || raw.startsWith("~\\")
        ? path.join(home, raw.slice(2))
        : raw;
  const normalized =
    path.isAbsolute(expanded) ? path.normalize(expanded) : path.resolve(expanded);
  return normalized;
}

function mergeCopilotStoreAliasStates(primary = {}, alias = {}) {
  const primaryLastId = toNonNegativeInt(primary?.lastId);
  const aliasLastId = toNonNegativeInt(alias?.lastId);
  const primaryUpdatedAt = Date.parse(primary?.updatedAt || "") || 0;
  const aliasUpdatedAt = Date.parse(alias?.updatedAt || "") || 0;
  const newer =
    aliasLastId > primaryLastId ||
    (aliasLastId === primaryLastId && aliasUpdatedAt > primaryUpdatedAt)
      ? alias
      : primary;
  const seenEventCounts = normalizeCopilotFingerprintCounts(
    primary?.seenEventCounts,
  );
  for (const [fingerprint, count] of Object.entries(
    normalizeCopilotFingerprintCounts(alias?.seenEventCounts),
  )) {
    seenEventCounts[fingerprint] = Math.max(
      toNonNegativeInt(seenEventCounts[fingerprint]),
      count,
    );
  }
  const pendingMalformedEvents = {
    ...normalizeCopilotPendingMalformedEvents(
      primary?.pendingMalformedEvents,
    ),
    ...normalizeCopilotPendingMalformedEvents(
      alias?.pendingMalformedEvents,
    ),
  };
  return {
    ...primary,
    ...newer,
    seenEventCounts,
    pendingMalformedEvents,
    malformedEventCount: Object.keys(pendingMalformedEvents).length,
    resetGapEventCount: Math.max(
      toNonNegativeInt(primary?.resetGapEventCount),
      toNonNegativeInt(alias?.resetGapEventCount),
    ),
  };
}

function coalesceCopilotDbStatesByIdentity(
  dbStates,
  selectedPaths,
  mergeStates,
) {
  if (!dbStates || typeof dbStates !== "object") return new Set();
  const selectedByIdentity = new Map();
  const selectedByInode = new Map();
  for (const selectedPath of selectedPaths) {
    try {
      const stat = fssync.statSync(selectedPath);
      selectedByIdentity.set(`${stat.dev}:${stat.ino}`, selectedPath);
      if (selectedByInode.has(stat.ino)) selectedByInode.set(stat.ino, null);
      else selectedByInode.set(stat.ino, selectedPath);
    } catch (_e) {}
  }
  const migratedPaths = new Set();
  for (const [statePath, state] of Object.entries({ ...dbStates })) {
    let dev = null;
    let ino = null;
    try {
      const stat = fssync.statSync(statePath);
      dev = stat.dev;
      ino = stat.ino;
    } catch (_e) {
      dev = state?.lastDbFingerprint?.db?.dev;
      ino = state?.lastDbFingerprint?.db?.ino;
    }
    const selectedPath =
      Number.isFinite(dev) && Number.isFinite(ino)
        ? selectedByIdentity.get(`${dev}:${ino}`)
        : Number.isFinite(ino)
          ? selectedByInode.get(ino)
          : null;
    if (!selectedPath || selectedPath === statePath) continue;
    dbStates[selectedPath] = mergeStates(
      dbStates[selectedPath],
      state,
    );
    delete dbStates[statePath];
    migratedPaths.add(statePath);
  }
  return migratedPaths;
}

function uniqueCopilotDbPaths(
  paths,
  env = process.env,
  preferredPaths = [],
) {
  const pathKey = (dbPath) =>
    process.platform === "win32" ? dbPath.toLowerCase() : dbPath;
  const preferredKeys = new Set(
    (Array.isArray(preferredPaths) ? preferredPaths : [])
      .map((candidate) => normalizeCopilotDbPath(candidate, env))
      .filter(Boolean)
      .map(pathKey),
  );
  const candidates = (Array.isArray(paths) ? paths : [])
    .map((candidate, index) => ({
      index,
      path: normalizeCopilotDbPath(candidate, env),
    }))
    .filter((candidate) => candidate.path)
    .sort((a, b) => {
      const preferredDiff =
        Number(preferredKeys.has(pathKey(b.path))) -
        Number(preferredKeys.has(pathKey(a.path)));
      return preferredDiff || a.index - b.index;
    });
  const result = [];
  const identities = new Set();
  for (const candidate of candidates) {
    const normalized = candidate.path;
    let identity =
      process.platform === "win32"
        ? `path:${normalized.toLowerCase()}`
        : `path:${normalized}`;
    try {
      const stat = fssync.statSync(normalized);
      if (
        Number.isFinite(stat.dev) &&
        Number.isFinite(stat.ino) &&
        stat.ino > 0
      ) {
        identity = `inode:${stat.dev}:${stat.ino}`;
      }
    } catch (_e) {}
    if (identities.has(identity)) continue;
    identities.add(identity);
    result.push(normalized);
  }
  return result;
}

function resolveCopilotAppDbPaths(env = process.env) {
  return resolveCopilotDbPaths(
    {
      fileName: "data.db",
      overrideEnvKey: "TOKEN_PULSE_COPILOT_APP_DB",
    },
    env,
  );
}

function resolveCopilotAppDbPath(env = process.env) {
  const paths = resolveCopilotAppDbPaths(env);
  for (const candidate of paths) {
    try {
      if (fssync.existsSync(candidate)) return candidate;
    } catch (_e) {}
  }
  return paths[0] || null;
}

function shouldCountCopilotAppSession(row) {
  const sessionType = typeof row?.session_type === "string" ? row.session_type.trim().toLowerCase() : "";
  const providerId = typeof row?.provider_id === "string" ? row.provider_id.trim().toLowerCase() : "";
  // OTEL remains the owner for recognizable Copilot CLI / VS Code extension
  // sessions. The App DB path is separately cursored, but skipping known
  // OTEL-backed surfaces avoids duplicate source="copilot" buckets if GitHub
  // mirrors those sessions into data.db in a future release.
  const markers = [sessionType, providerId];
  return !markers.some((value) =>
    value === "cli" ||
    value === "copilot-cli" ||
    value.includes("vscode") ||
    value.includes("extension") ||
    value.includes("otel")
  );
}

function parseCopilotAppTimestamp(value) {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number") {
    const ms = value > 10_000_000_000 ? value : value * 1000;
    const d = new Date(ms);
    return Number.isNaN(d.getTime()) ? null : d.toISOString();
  }
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (/^\d+$/.test(trimmed)) {
    return parseCopilotAppTimestamp(Number(trimmed));
  }
  const parsed = new Date(trimmed);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function normalizeCopilotAppModel(model) {
  if (typeof model !== "string") return "";
  const normalized = normalizeModelInput(model);
  if (!normalized) return "";
  if (/^claude-(sonnet|opus|haiku)-\d+\.\d+/.test(normalized)) {
    return normalized.replace(/^(claude-(?:sonnet|opus|haiku)-\d+)\.(\d+)/, "$1-$2");
  }
  return normalized;
}

function readCopilotAppSessionsFromSqlite(dbPath, sqliteOptions = {}) {
  const pragmaRows = readSqliteJsonRows(dbPath, "PRAGMA table_info(sessions)", {
    label: "GitHub Copilot App",
    maxBuffer: 4 * 1024 * 1024,
    timeout: 10_000,
    readOnly: true,
    throwOnReadFailure: true,
    ...sqliteOptions,
  });
  const columns = new Set(pragmaRows.map((row) => row?.name).filter(Boolean));
  if (!columns.has("id")) return [];
  const optional = (col) => (columns.has(col) ? col : `NULL AS ${col}`);
  const orderTerms = ["updated_at", "created_at"].filter((col) => columns.has(col));
  const orderBy = orderTerms.length > 0
    ? `ORDER BY COALESCE(${orderTerms.join(", ")}, ''), id`
    : "ORDER BY id";
  // Select only the columns the parser consumes. Content-ish columns like
  // `title` must stay out of the query: token counts only, never user content.
  const sql = `
    SELECT
      id,
      ${optional("session_type")},
      ${optional("model")},
      ${optional("provider_id")},
      ${optional("created_at")},
      ${optional("updated_at")},
      ${optional("total_input_tokens")},
      ${optional("total_output_tokens")},
      ${optional("total_cached_tokens")},
      ${optional("total_reasoning_tokens")}
    FROM sessions
    ${orderBy}
  `.trim();
  return readSqliteJsonRows(dbPath, sql, {
    label: "GitHub Copilot App",
    maxBuffer: 64 * 1024 * 1024,
    timeout: 60_000,
    readOnly: true,
    throwOnReadFailure: true,
    ...sqliteOptions,
  });
}

function getCopilotSqliteFingerprint(dbPath) {
  return sqliteSidecarFingerprint(dbPath);
}

function copilotAppBaselineTotal(value) {
  if (!value || typeof value !== "object") return 0;
  return (
    Number(value.input || 0) +
    Number(value.output || 0) +
    Number(value.cached || 0) +
    Number(value.reasoning || 0)
  );
}

function capCopilotAppSessionTotals(sessionTotals, maxEntries = 10_000) {
  const entries = Object.entries(sessionTotals);
  if (entries.length <= maxEntries) return sessionTotals;

  const nonzero = [];
  const zero = [];
  for (const entry of entries) {
    if (copilotAppBaselineTotal(entry[1]) > 0) nonzero.push(entry);
    else zero.push(entry);
  }

  zero.sort((a, b) => {
    const ta = Date.parse(a[1]?.updatedAt || "") || 0;
    const tb = Date.parse(b[1]?.updatedAt || "") || 0;
    return tb - ta;
  });
  const zeroBudget = Math.max(0, maxEntries - nonzero.length);
  const capped = Object.fromEntries([...nonzero, ...zero.slice(0, zeroBudget)]);
  for (const key of Object.keys(sessionTotals)) delete sessionTotals[key];
  Object.assign(sessionTotals, capped);
  return sessionTotals;
}

function normalizeCopilotAppTokenDelta(curr, prev) {
  const inputDelta = Math.max(0, Number(curr?.input || 0) - Number(prev?.input || 0));
  const cachedDelta = Math.max(0, Number(curr?.cached || 0) - Number(prev?.cached || 0));
  const cachedInputTokens = Math.min(cachedDelta, inputDelta);
  const inputTokens = Math.max(0, inputDelta - cachedInputTokens);
  const outputDelta = Math.max(0, Number(curr?.output || 0) - Number(prev?.output || 0));
  const reasoningDelta = Math.max(
    0,
    Number(curr?.reasoning || 0) - Number(prev?.reasoning || 0),
  );
  const reasoningTokens = Math.min(reasoningDelta, outputDelta);
  const outputTokens = Math.max(0, outputDelta - reasoningTokens);
  const totalTokens = inputTokens + cachedInputTokens + outputTokens + reasoningTokens;
  return {
    input_tokens: inputTokens,
    cached_input_tokens: cachedInputTokens,
    cache_creation_input_tokens: 0,
    output_tokens: outputTokens,
    reasoning_output_tokens: reasoningTokens,
    total_tokens: totalTokens,
    conversation_count: copilotAppBaselineTotal(prev) === 0 && totalTokens > 0 ? 1 : 0,
  };
}

async function parseCopilotAppDbIncremental({
  dbPath,
  dbPaths,
  cursors,
  queuePath,
  onProgress,
  env,
  sqliteOptions,
  observeOnly = false,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const paths = Array.isArray(dbPaths) && dbPaths.length > 0
    ? dbPaths
    : dbPath
      ? [dbPath]
      : resolveCopilotAppDbPaths(env || process.env);
  const appState =
    cursors.copilotApp && typeof cursors.copilotApp === "object" ? cursors.copilotApp : {};
  const dbStates =
    appState.dbs && typeof appState.dbs === "object" ? { ...appState.dbs } : {};
  const uniquePaths = uniqueCopilotDbPaths(
    paths,
    env || process.env,
    Object.keys(dbStates),
  );
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;
  let recordsProcessed = 0;
  let eventsAggregated = 0;
  let totalRows = 0;
  let dbErrors = 0;
  const updatedAt = new Date().toISOString();

  for (const resolvedDb of uniquePaths) {
    const dbState = dbStates[resolvedDb] && typeof dbStates[resolvedDb] === "object"
      ? dbStates[resolvedDb]
      : {};
    const sessionTotals =
      dbState.sessionTotals && typeof dbState.sessionTotals === "object"
        ? { ...dbState.sessionTotals }
        : {};
    let currentFingerprint = null;
    try {
      fssync.statSync(resolvedDb);
      currentFingerprint = sqliteSidecarFingerprint(resolvedDb);
    } catch (e) {
      if (e && e.code === "ENOENT") continue;
      dbErrors++;
      dbStates[resolvedDb] = {
        ...dbState,
        sessionTotals,
        lastError: e && e.message ? e.message : String(e),
        lastErrorAt: updatedAt,
      };
      continue;
    }

    if (dbState.lastDbFingerprint && sameSqliteFingerprint(currentFingerprint, dbState.lastDbFingerprint)) {
      dbStates[resolvedDb] = { ...dbState, sessionTotals, updatedAt };
      continue;
    }

    let snap = null;
    let rows = [];
    try {
      snap = snapshotSqliteDb(resolvedDb);
      rows = readCopilotAppSessionsFromSqlite(snap.path, sqliteOptions);
    } catch (err) {
      dbErrors++;
      dbStates[resolvedDb] = {
        ...dbState,
        sessionTotals,
        lastError: err && err.message ? err.message : String(err),
        lastErrorAt: updatedAt,
      };
      continue;
    } finally {
      if (snap) snap.cleanup();
    }
    totalRows += rows.length;

    for (const row of rows) {
      recordsProcessed++;
      if (!row || typeof row.id !== "string" || !row.id.trim()) continue;
      if (!shouldCountCopilotAppSession(row)) continue;

      const sessionId = row.id.trim();
      const curr = {
        input: toNonNegativeInt(row.total_input_tokens),
        output: toNonNegativeInt(row.total_output_tokens),
        cached: toNonNegativeInt(row.total_cached_tokens),
        reasoning: toNonNegativeInt(row.total_reasoning_tokens),
      };
      const prev = sessionTotals[sessionId] || { input: 0, output: 0, cached: 0, reasoning: 0 };
      const bucketDelta = normalizeCopilotAppTokenDelta(curr, prev);
      const totalDelta = bucketDelta.total_tokens;
      const rowUpdatedAt =
        parseCopilotAppTimestamp(row.updated_at) ||
        parseCopilotAppTimestamp(row.created_at) ||
        updatedAt;
      const model = normalizeCopilotAppModel(row.model) || COPILOT_APP_DEFAULT_MODEL;

      if (totalDelta <= 0) {
        sessionTotals[sessionId] = { ...curr, model, updatedAt: rowUpdatedAt };
        continue;
      }
      if (observeOnly) {
        sessionTotals[sessionId] = { ...curr, model, updatedAt: rowUpdatedAt };
        continue;
      }

      const bucketStart = toUtcHalfHourStart(rowUpdatedAt);
      if (!bucketStart) {
        sessionTotals[sessionId] = { ...curr, model, updatedAt: rowUpdatedAt };
        continue;
      }

      const bucket = getHourlyBucket(hourlyState, "copilot", model, bucketStart);
      addTotals(bucket.totals, bucketDelta);
      touchedBuckets.add(bucketKey("copilot", model, bucketStart));
      sessionTotals[sessionId] = { ...curr, model, updatedAt: rowUpdatedAt };
      eventsAggregated++;

      if (cb) {
        cb({
          index: recordsProcessed,
          total: Math.max(totalRows, recordsProcessed),
          recordsProcessed,
          eventsAggregated,
          bucketsQueued: touchedBuckets.size,
        });
      }
    }

    capCopilotAppSessionTotals(sessionTotals);

    dbStates[resolvedDb] = {
      ...dbState,
      sessionTotals,
      lastDbFingerprint: currentFingerprint,
      lastError: null,
      lastErrorAt: null,
      updatedAt,
    };
  }

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.copilotApp = {
    ...appState,
    version: COPILOT_APP_CURSOR_VERSION,
    dbs: dbStates,
    updatedAt,
  };

  return { recordsProcessed, eventsAggregated, bucketsQueued, dbErrors };
}

module.exports = {
  resolveCopilotOtelPaths,
  pruneCopilotUsageClaims,
  copilotOtelCursorHasLegacyCliUsage,
  parseCopilotIncremental,
  resolveCopilotSessionStorePaths,
  describeCopilotSessionStoreDb,
  readCopilotSessionStoreUsageRows,
  normalizeCopilotSessionStoreUsage,
  parseCopilotSessionStoreIncremental,
  normalizeCopilotDbPath,
  coalesceCopilotDbStatesByIdentity,
  uniqueCopilotDbPaths,
  resolveCopilotAppDbPaths,
  resolveCopilotAppDbPath,
  readCopilotAppSessionsFromSqlite,
  getCopilotSqliteFingerprint,
  parseCopilotAppDbIncremental,
};
