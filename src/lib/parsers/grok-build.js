// grok-build usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const readline = require("node:readline");
const { ensureDir } = require("../fs");
const wsl = require("../wsl-probe");
const { normalizeGrokUsage } = require("../grok-usage");
const {
  enqueueTouchedBuckets,
  enqueueTouchedProjectBuckets,
  normalizeHourlyState,
  normalizeProjectState,
  getHourlyBucket,
  getProjectBucket,
  addTotals,
  toUtcHalfHourStart,
  bucketKey,
  projectBucketKey,
  normalizeModelInput,
  resolveProjectContextForPath,
  pickWin32ProviderPath,
  mergeGrokUsagePrecision,
  clearSourceHourlyBuckets,
  retractStaleSourceQueueRows,
} = require("./core");

function normalizeNonNegativeNumber(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return 0;
  return n;
}

// ─────────────────────────────────────────────────────────────────────────────
// Grok Build (xAI) — passive reader for ~/.grok/sessions/**/updates.jsonl + signals.json
// Triggered either by full scan in sync or by the SessionEnd hook writing a signal.
// turn_completed.usage exposes the reported input/output/cache/reasoning split
// and, on current Grok builds, exact server cost ticks. Older/partial sessions
// without that event retain the explicitly isolated context-watermark fallback.
// ─────────────────────────────────────────────────────────────────────────────

const GROK_ESTIMATED_INPUT_RATIO = 0.8;
// v5: split cache creation + reasoning correctly and retain reported cost.
const GROK_CURSOR_VERSION = 5;

function resolveGrokBuildHome(env = process.env) {
  if (env.TOKEN_PULSE_GROK_HOME) return env.TOKEN_PULSE_GROK_HOME;
  if (env.GROK_HOME) return env.GROK_HOME;
  if (process.platform === "win32") {
    return pickWin32ProviderPath({
      env,
      nativeValue: path.join(os.homedir(), ".grok"),
      wslProviderDir: ".grok",
    });
  }
  return path.join(os.homedir(), ".grok");
}

function resolveGrokBuildSessions(env = process.env) {
  const home = resolveGrokBuildHome(env);
  if (!home) return [];
  const sessionsRoot = path.join(home, "sessions");
  if (!fssync.existsSync(sessionsRoot)) return [];

  const results = [];
  let cwdDirs = [];
  try {
    cwdDirs = fssync.readdirSync(sessionsRoot);
  } catch {
    return [];
  }

  for (const cwdDir of cwdDirs) {
    const cwdPath = path.join(sessionsRoot, cwdDir);
    let stat;
    try { stat = fssync.statSync(cwdPath); } catch { continue; }
    if (!stat.isDirectory()) continue;

    let sessionIds = [];
    try { sessionIds = fssync.readdirSync(cwdPath); } catch { continue; }

    for (const sid of sessionIds) {
      const sessionDir = path.join(cwdPath, sid);
      const signalsPath = path.join(sessionDir, "signals.json");
      const updatesPath = path.join(sessionDir, "updates.jsonl");
      if (fssync.existsSync(signalsPath) || fssync.existsSync(updatesPath)) {
        results.push({
          sessionDir,
          updatesPath,
          signalsPath,
          summaryPath: path.join(sessionDir, "summary.json"),
          sessionId: sid,
          encodedCwd: cwdDir
        });
      }
    }
  }
  return results;
}

function normalizeGrokSessionSnapshots(grokState) {
  const snapshots = {};
  if (grokState?.sessionSnapshots && typeof grokState.sessionSnapshots === "object") {
    for (const [sessionId, snapshot] of Object.entries(grokState.sessionSnapshots)) {
      const safeSessionId = normalizeModelInput(sessionId);
      if (!safeSessionId || !snapshot || typeof snapshot !== "object") continue;
      const totalTokens = normalizeNonNegativeNumber(snapshot.totalTokens);
      snapshots[safeSessionId] = {
        totalTokens,
        messageCount: normalizeNonNegativeNumber(snapshot.messageCount),
        model: normalizeModelInput(snapshot.model) || null,
        source: normalizeModelInput(snapshot.source) || null,
        lastEventId: normalizeModelInput(snapshot.lastEventId) || null,
        lastEventTimestamp: normalizeModelInput(snapshot.lastEventTimestamp) || null,
        updatedAt: normalizeModelInput(snapshot.updatedAt) || null,
        legacySeen: snapshot.legacySeen === true,
      };
    }
  }

  if (Array.isArray(grokState?.seenSessions)) {
    for (const sessionId of grokState.seenSessions) {
      const safeSessionId = normalizeModelInput(sessionId);
      if (!safeSessionId || snapshots[safeSessionId]) continue;
      snapshots[safeSessionId] = {
        totalTokens: 0,
        messageCount: 0,
        model: null,
        updatedAt: normalizeModelInput(grokState.updatedAt) || null,
        legacySeen: true,
      };
    }
  }

  return snapshots;
}

function capGrokSessionSnapshots(sessionSnapshots) {
  const entries = Object.entries(sessionSnapshots);
  if (entries.length <= 10_000) return sessionSnapshots;
  return Object.fromEntries(entries.slice(entries.length - 10_000));
}

function readGrokJsonFile(filePath) {
  if (!filePath) return null;
  try {
    return JSON.parse(fssync.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

function grokUpdatesPathForSession(sess) {
  if (typeof sess?.updatesPath === "string" && sess.updatesPath.trim()) return sess.updatesPath;
  if (typeof sess?.sessionDir === "string" && sess.sessionDir.trim()) {
    return path.join(sess.sessionDir, "updates.jsonl");
  }
  return null;
}

function grokSessionIdFor(sess) {
  return (
    normalizeModelInput(sess?.sessionId) ||
    (normalizeModelInput(sess?.sessionDir) ? path.basename(sess.sessionDir) : null)
  );
}

function grokModelFromSignals(signals) {
  return (
    normalizeModelInput(signals?.primaryModelId) ||
    normalizeModelInput(Array.isArray(signals?.modelsUsed) ? signals.modelsUsed[0] : null) ||
    normalizeModelInput(signals?.model) ||
    "grok-build"
  );
}

function grokLastActiveFromSignals(signals, summary) {
  return (
    normalizeModelInput(signals?.lastActiveAt) ||
    normalizeModelInput(signals?.updatedAt) ||
    normalizeModelInput(signals?.lastActive) ||
    normalizeModelInput(summary?.updated_at) ||
    normalizeModelInput(summary?.updatedAt) ||
    new Date().toISOString()
  );
}

function grokMessageCountFromSignals(signals) {
  return normalizeNonNegativeNumber(
    signals?.assistantMessageCount ??
      signals?.turnCount ??
      signals?.num_chat_messages ??
      signals?.messageCount,
  );
}

// Context-window telemetry only. Prefer turn_completed.usage when available —
// signals.totalTokens / contextTokensUsed track the live window, not billable
// cumulative spend across a session (especially after compaction).
function grokEffectiveTotalFromSignals(signals) {
  if (!signals || typeof signals !== "object") return 0;
  const beforeCompaction = normalizeNonNegativeNumber(signals.totalTokensBeforeCompaction);
  const totalTokens = normalizeNonNegativeNumber(signals.totalTokens);
  if (signals.contextTokensUsed == null) {
    return beforeCompaction + totalTokens;
  }
  return Math.max(
    totalTokens,
    beforeCompaction + normalizeNonNegativeNumber(signals.contextTokensUsed),
  );
}

function grokTimestampToIso(value) {
  if (value == null) return null;
  if (typeof value === "number") {
    if (!Number.isFinite(value) || value <= 0) return null;
    const millis = value < 10_000_000_000 ? value * 1000 : value;
    const dt = new Date(millis);
    return Number.isFinite(dt.getTime()) ? dt.toISOString() : null;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return null;
    if (/^[0-9]+(?:\.[0-9]+)?$/.test(trimmed)) {
      return grokTimestampToIso(Number(trimmed));
    }
    const dt = new Date(trimmed);
    return Number.isFinite(dt.getTime()) ? dt.toISOString() : null;
  }
  return null;
}

function grokTimestampFromUpdate(meta, record, fallback) {
  return (
    grokTimestampToIso(meta?.agentTimestampMs) ||
    grokTimestampToIso(meta?.timestampMs) ||
    grokTimestampToIso(record?.timestamp_ms) ||
    grokTimestampToIso(record?.timestamp) ||
    grokTimestampToIso(record?.time) ||
    fallback ||
    null
  );
}

function grokEventId(value, fallback) {
  if (typeof value === "string" && value.trim()) return value.trim();
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return fallback;
}

function grokFileEndsWithNewline(filePath, size) {
  if (!(size > 0)) return false;
  let fd;
  try {
    fd = fssync.openSync(filePath, "r");
    const buf = Buffer.alloc(1);
    const read = fssync.readSync(fd, buf, 0, 1, size - 1);
    return read === 1 && buf[0] === 0x0a; // trailing "\n"
  } catch {
    return false;
  } finally {
    if (fd !== undefined) {
      try {
        fssync.closeSync(fd);
      } catch {
        /* ignore close failure */
      }
    }
  }
}

function canonicalizeGrokUsageModel(model) {
  const raw = normalizeModelInput(model) || "grok-build";
  const lower = raw.toLowerCase();
  // Free Build SKU must not fuzzy-match paid grok-4.5 rates in native clients.
  if (lower.includes("build-free") || lower.endsWith("-free") || lower.includes("free-tier")) {
    return "grok-build-free";
  }
  if (lower === "grok-4.5-build" || lower === "grok-4-5-build") {
    return "grok-4.5-build";
  }
  return raw;
}

function normalizeGrokTurnUsage(usage, model, timestamp, eventId) {
  const normalized = normalizeGrokUsage(usage);
  if (!normalized) return null;
  return {
    ...normalized,
    // A missing/partial cost must fall back to model pricing, not turn into a
    // falsely precise $0 row in the hourly queue.
    total_cost_usd: normalized.total_cost_usd ?? 0,
    conversation_count: 1,
    model: canonicalizeGrokUsageModel(model),
    timestamp,
    eventId,
  };
}

function extractGrokTurnUsageEvents(record, fallbackTimestamp, fallbackModel, lineIndex) {
  const update = record?.params?.update;
  if (!update || typeof update !== "object") return [];
  if (update.sessionUpdate !== "turn_completed") return [];
  const usage = update.usage;
  if (!usage || typeof usage !== "object") return [];

  const meta = record?.params?._meta || record?._meta || {};
  const timestamp = grokTimestampFromUpdate(meta, record, fallbackTimestamp);
  const baseEventId = grokEventId(
    meta.eventId ?? record?.eventId ?? record?.id ?? update.prompt_id,
    String(lineIndex),
  );

  const modelUsage =
    usage.modelUsage && typeof usage.modelUsage === "object" ? usage.modelUsage : null;
  const events = [];
  if (modelUsage && Object.keys(modelUsage).length > 0) {
    for (const [modelName, modelUsageEntry] of Object.entries(modelUsage)) {
      if (!modelUsageEntry || typeof modelUsageEntry !== "object") continue;
      const event = normalizeGrokTurnUsage(
        modelUsageEntry,
        modelName,
        timestamp,
        `${baseEventId}|${modelName}`,
      );
      if (event) events.push(event);
    }
  }
  if (events.length === 0) {
    const event = normalizeGrokTurnUsage(usage, fallbackModel, timestamp, baseEventId);
    if (event) events.push(event);
  }
  return events;
}

// Context-window watermark events (legacy / fallback only).
function extractGrokContextTokenEvent(record, fallbackTimestamp, lineIndex) {
  const meta = record?.params?._meta || record?._meta;
  if (!meta || typeof meta !== "object") return null;
  const totalTokens = normalizeNonNegativeNumber(meta.totalTokens);
  if (totalTokens <= 0) return null;
  return {
    totalTokens,
    timestamp: grokTimestampFromUpdate(meta, record, fallbackTimestamp),
    eventId: grokEventId(meta.eventId ?? record?.eventId ?? record?.id, String(lineIndex)),
  };
}

async function readGrokUpdateTokenEvents(updatesPath, fallbackTimestamp, prevOffsetEntry, options = {}) {
  const fallbackModel = options.fallbackModel || "grok-build";
  if (!updatesPath) {
    return { turnEvents: [], contextEvents: [], offsetEntry: null };
  }
  let stat;
  try {
    stat = fssync.statSync(updatesPath);
    if (!stat.isFile()) return { turnEvents: [], contextEvents: [], offsetEntry: null };
  } catch {
    return { turnEvents: [], contextEvents: [], offsetEntry: null };
  }

  // updates.jsonl is append-only. Turn usage is additive per turn_completed, so
  // resuming from the last consumed byte is safe. Re-read from 0 on truncation
  // or inode change.
  const prevSize = Number(prevOffsetEntry?.size) || 0;
  const prevIno = prevOffsetEntry?.ino;
  const inodeChanged = typeof prevIno === "number" && prevIno !== stat.ino;
  const startOffset = stat.size < prevSize || inodeChanged ? 0 : prevSize;
  const baseOffset = { mtimeMs: stat.mtimeMs, ino: stat.ino };
  if (stat.size <= startOffset) {
    return {
      turnEvents: [],
      contextEvents: [],
      offsetEntry: { size: startOffset, ...baseOffset },
    };
  }

  // Only advance the stored offset to the end of the last newline-terminated
  // line. If Grok is mid-write, the final JSONL line has no trailing "\n" yet;
  // its bytes are left unconsumed so the next scan re-reads the line once it is
  // complete instead of skipping it forever (which would undercount tokens).
  const endsWithNewline = grokFileEndsWithNewline(updatesPath, stat.size);

  const turnEvents = [];
  const contextEvents = [];
  let lineIndex = 0;
  let lastLine = "";
  const input = fssync.createReadStream(updatesPath, {
    encoding: "utf8",
    start: startOffset,
    end: stat.size - 1, // inclusive; bound the read to the stat'd size
  });
  const rl = readline.createInterface({ input, crlfDelay: Infinity });
  try {
    for await (const line of rl) {
      lineIndex++;
      lastLine = line;
      if (!line || !line.trim()) continue;
      let record;
      try {
        record = JSON.parse(line);
      } catch {
        continue;
      }
      const turns = extractGrokTurnUsageEvents(
        record,
        fallbackTimestamp,
        fallbackModel,
        lineIndex,
      );
      if (turns.length > 0) {
        turnEvents.push(...turns);
        continue;
      }
      const contextEvent = extractGrokContextTokenEvent(record, fallbackTimestamp, lineIndex);
      if (contextEvent) contextEvents.push(contextEvent);
    }
  } catch {
    // Stream error mid-read: discard partial events and do not advance the
    // offset, so the next sync re-extracts from the same range exactly once
    // instead of double-counting already-parsed turn events.
    return { turnEvents: [], contextEvents: [], offsetEntry: prevOffsetEntry || null };
  }

  // When the file does not end on a newline, the final emitted line is a
  // partial tail still being written. Exclude its bytes so the committed offset
  // stays on a complete-line boundary and the line is re-read once finished.
  const trailingPartialBytes = endsWithNewline ? 0 : Buffer.byteLength(lastLine, "utf8");
  const committedSize = Math.max(startOffset, stat.size - trailingPartialBytes);
  return {
    turnEvents,
    contextEvents,
    offsetEntry: { size: committedSize, ...baseOffset },
  };
}

function estimateGrokTokenDelta(totalTokens, conversationCount, options = {}) {
  const total = Math.trunc(normalizeNonNegativeNumber(totalTokens));
  const inputTokens = Math.round(total * GROK_ESTIMATED_INPUT_RATIO);
  const outputTokens = Math.max(0, total - inputTokens);
  const rawConversations = Math.trunc(normalizeNonNegativeNumber(conversationCount));
  const conversations = options.allowZeroConversationCount ? rawConversations : Math.max(1, rawConversations);

  return {
    input_tokens: inputTokens,
    cached_input_tokens: 0,
    cache_creation_input_tokens: 0,
    output_tokens: outputTokens,
    reasoning_output_tokens: 0,
    total_tokens: total,
    billable_total_tokens: total,
    conversation_count: conversations,
  };
}

function pruneMissingGrokProjectUpdateOffsets(projectUpdateOffsets) {
  if (!projectUpdateOffsets || typeof projectUpdateOffsets !== "object") return;
  for (const updatesPath of Object.keys(projectUpdateOffsets)) {
    if (typeof updatesPath !== "string" || !updatesPath) {
      delete projectUpdateOffsets[updatesPath];
      continue;
    }
    try {
      if (!fssync.existsSync(updatesPath)) delete projectUpdateOffsets[updatesPath];
    } catch {
      delete projectUpdateOffsets[updatesPath];
    }
  }
}

function grokTryDecodeUriComponent(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  try {
    const decoded = decodeURIComponent(trimmed).trim();
    return decoded || null;
  } catch {
    return null;
  }
}

function grokEncodedCwdLooksEncoded(value) {
  return typeof value === "string" && /%[0-9A-Fa-f]{2}/.test(value);
}

// First non-empty wins: summary.info.cwd, hook sess.cwd, decode(encodedCwd),
// then decode(basename(dirname(sessionDir))). Do not guess from updates.jsonl.
function resolveGrokSessionCwd(sess, summary) {
  const summaryCwd = summary?.info?.cwd;
  if (typeof summaryCwd === "string" && summaryCwd.trim()) return summaryCwd.trim();
  if (typeof sess?.cwd === "string" && sess.cwd.trim()) return sess.cwd.trim();
  if (grokEncodedCwdLooksEncoded(sess?.encodedCwd)) {
    const decoded = grokTryDecodeUriComponent(sess.encodedCwd);
    if (decoded) return decoded;
  }
  const sessionDir = typeof sess?.sessionDir === "string" ? sess.sessionDir.trim() : "";
  if (sessionDir) {
    const decoded = grokTryDecodeUriComponent(path.basename(path.dirname(sessionDir)));
    if (decoded) return decoded;
  }
  return null;
}

async function parseGrokBuildIncremental({
  sessions,
  cursors = {},
  queuePath,
  projectQueuePath,
  publicRepoResolver,
  onProgress,
  env = process.env
} = {}) {
  if (queuePath) await ensureDir(path.dirname(queuePath));
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const grokState = cursors.grok && typeof cursors.grok === "object" ? { ...cursors.grok } : {};
  const prevVersion = Number(grokState.version) || 0;
  const needsTurnUsageMigration = prevVersion < GROK_CURSOR_VERSION;

  // v3 and earlier treated context-window totalTokens as cumulative spend;
  // v4 still overlapped output/reasoning, discarded cache creation, and ignored
  // provider-reported cost. Rebuild from turn_completed.usage for v5.
  //
  // Drop prior watermark totals / updateOffsets so files are re-read from byte 0,
  // but keep legacySeen markers from seenSessions so the one-shot baseline
  // (sessions already counted under the old scanner) still applies.
  let sessionSnapshots;
  if (needsTurnUsageMigration) {
    const normalized = normalizeGrokSessionSnapshots(grokState);
    sessionSnapshots = {};
    for (const [sessionId, snapshot] of Object.entries(normalized)) {
      if (!snapshot?.legacySeen) continue;
      sessionSnapshots[sessionId] = {
        totalTokens: 0,
        messageCount: 0,
        model: null,
        source: null,
        lastEventId: null,
        lastEventTimestamp: null,
        updatedAt: snapshot.updatedAt || null,
        legacySeen: true,
      };
    }
    clearSourceHourlyBuckets(hourlyState, "grok");
  } else {
    sessionSnapshots = normalizeGrokSessionSnapshots(grokState);
  }
  const prevUpdateOffsets =
    !needsTurnUsageMigration &&
    grokState.updateOffsets &&
    typeof grokState.updateOffsets === "object"
      ? grokState.updateOffsets
      : {};

  const projectEnabled = typeof projectQueuePath === "string" && projectQueuePath.length > 0;
  const projectState = projectEnabled ? normalizeProjectState(cursors?.projectHourly) : null;
  const projectTouchedBuckets = projectEnabled ? new Set() : null;
  const projectMetaCache = projectEnabled ? new Map() : null;
  const publicRepoCache = projectEnabled ? new Map() : null;
  // Independent of global updateOffsets so an upgrade can backfill historical
  // turn_completed events into project.queue.jsonl without re-appending totals.
  const projectUpdateOffsets =
    projectEnabled &&
    grokState.projectUpdateOffsets &&
    typeof grokState.projectUpdateOffsets === "object"
      ? { ...grokState.projectUpdateOffsets }
      : {};
  const projectSessionSnapshots = projectEnabled
    ? normalizeGrokSessionSnapshots({
        sessionSnapshots: grokState.projectSessionSnapshots,
      })
    : {};

  // Rebuilt from the sessions seen this scan, so entries for deleted session
  // dirs are pruned and the cursor stays bounded by the on-disk session count.
  const updateOffsets = {};
  const touchedBuckets = new Set();

  const sessionList = Array.isArray(sessions) && sessions.length > 0
    ? sessions
    : resolveGrokBuildSessions(env);

  let eventsAggregated = 0;

  for (let index = 0; index < sessionList.length; index++) {
    const sess = sessionList[index];
    const sessionId = grokSessionIdFor(sess);
    if (!sessionId) {
      if (onProgress) onProgress({ index: index + 1, total: sessionList.length, bucketsQueued: touchedBuckets.size });
      continue;
    }

    const signals = sess?.signals && typeof sess.signals === "object"
      ? sess.signals
      : readGrokJsonFile(sess?.signalsPath);
    const safeSignals = signals && typeof signals === "object" ? signals : {};

    const summary = sess?.summary && typeof sess.summary === "object"
      ? sess.summary
      : readGrokJsonFile(sess?.summaryPath) || {};
    const previous = sessionSnapshots[sessionId] || {};
    const previousTotal = normalizeNonNegativeNumber(previous.totalTokens);
    const previousMessageCount = normalizeNonNegativeNumber(previous.messageCount);
    const messageCount = grokMessageCountFromSignals(safeSignals);
    const model = grokModelFromSignals(safeSignals);
    const lastActive = grokLastActiveFromSignals(safeSignals, summary);

    let cumulativeTotal = previousTotal;
    let tokenDeltaForSession = 0;
    let finalTouchedHourStart = null;
    let source = previous.source || null;
    let lastEventId = previous.lastEventId || null;
    let lastEventTimestamp = previous.lastEventTimestamp || null;
    let lastModel = previous.model || model;
    let sawTurnUsage = source === "turn_usage" || previous.source === "turn_usage";
    // Defer bucket writes until after we know whether this is a legacy baseline
    // pass (first sighting of a session already counted under an older scanner).
    const pendingBucketDeltas = [];

    const updatesPath = grokUpdatesPathForSession(sess);
    const updates = await readGrokUpdateTokenEvents(
      updatesPath,
      lastActive,
      updatesPath ? prevUpdateOffsets[updatesPath] : null,
      { fallbackModel: model },
    );
    if (updatesPath && updates.offsetEntry) {
      updateOffsets[updatesPath] = updates.offsetEntry;
    }

    // Preferred path: each turn_completed carries true cumulative API usage for
    // that turn (input/output/cache/reasoning). Sum them.
    for (const event of updates.turnEvents) {
      sawTurnUsage = true;
      const hourStartStr = toUtcHalfHourStart(event.timestamp) || toUtcHalfHourStart(lastActive) || toUtcHalfHourStart(Date.now());
      if (!hourStartStr) continue;
      const eventModel = event.model || model;
      const delta = {
        input_tokens: event.input_tokens,
        cached_input_tokens: event.cached_input_tokens,
        cache_creation_input_tokens: event.cache_creation_input_tokens,
        output_tokens: event.output_tokens,
        reasoning_output_tokens: event.reasoning_output_tokens,
        total_tokens: event.total_tokens,
        billable_total_tokens: event.billable_total_tokens,
        total_cost_usd: event.total_cost_usd,
        conversation_count: event.conversation_count || 1,
      };
      pendingBucketDeltas.push({ model: eventModel, hourStartStr, delta, usagePrecision: "reported" });
      cumulativeTotal += event.total_tokens;
      tokenDeltaForSession += event.total_tokens;
      finalTouchedHourStart = hourStartStr;
      source = "turn_usage";
      lastEventId = event.eventId || lastEventId;
      lastEventTimestamp = event.timestamp || lastEventTimestamp;
      lastModel = eventModel;
    }

    // Fallback only when this session never emitted turn_completed usage
    // (older logs / partial sessions). Context watermark is a lower-bound
    // estimate and must not run on top of turn usage.
    if (!sawTurnUsage) {
      let highWatermark = previousTotal;
      for (const event of updates.contextEvents) {
        lastEventId = event.eventId || lastEventId;
        lastEventTimestamp = event.timestamp || lastEventTimestamp;
        if (event.totalTokens <= highWatermark) continue;
        const deltaTokens = event.totalTokens - highWatermark;
        highWatermark = event.totalTokens;
        const hourStartStr =
          toUtcHalfHourStart(event.timestamp) ||
          toUtcHalfHourStart(lastActive) ||
          toUtcHalfHourStart(Date.now());
        if (!hourStartStr) continue;
        const delta = estimateGrokTokenDelta(deltaTokens, 0, { allowZeroConversationCount: true });
        pendingBucketDeltas.push({ model, hourStartStr, delta, usagePrecision: "estimated" });
        tokenDeltaForSession += deltaTokens;
        finalTouchedHourStart = hourStartStr;
        source = "updates";
      }

      const effectiveSignalTotal = grokEffectiveTotalFromSignals(safeSignals);
      if (effectiveSignalTotal > highWatermark) {
        const deltaTokens = effectiveSignalTotal - highWatermark;
        highWatermark = effectiveSignalTotal;
        const hourStartStr = toUtcHalfHourStart(lastActive) || toUtcHalfHourStart(Date.now());
        if (hourStartStr) {
          const delta = estimateGrokTokenDelta(deltaTokens, 0, { allowZeroConversationCount: true });
          pendingBucketDeltas.push({ model, hourStartStr, delta, usagePrecision: "estimated" });
          tokenDeltaForSession += deltaTokens;
          finalTouchedHourStart = hourStartStr;
          source = "signals";
        }
      }
      cumulativeTotal = Math.max(previousTotal, highWatermark);
    }

    const finalTotal = Math.max(previousTotal, cumulativeTotal);
    // Sessions already observed under an older scanner must establish a watermark
    // without backfilling historical tokens as brand-new usage.
    const legacyBaselineOnly = previous.legacySeen && previousTotal === 0 && finalTotal > 0;

    if (!legacyBaselineOnly) {
      for (const pending of pendingBucketDeltas) {
        const bucket = getHourlyBucket(hourlyState, "grok", pending.model, pending.hourStartStr);
        addTotals(bucket.totals, pending.delta);
        bucket.usage_precision = mergeGrokUsagePrecision(
          bucket.usage_precision,
          pending.usagePrecision,
        );
        touchedBuckets.add(bucketKey("grok", pending.model, pending.hourStartStr));
        eventsAggregated++;
      }

      // Message/conversation count for fallback-only sessions (turn path already
      // counts each turn_completed as one conversation).
      if (!sawTurnUsage && tokenDeltaForSession > 0 && finalTouchedHourStart) {
        const deltaMessageCount =
          messageCount > previousMessageCount ? messageCount - previousMessageCount : 1;
        const bucket = getHourlyBucket(hourlyState, "grok", lastModel || model, finalTouchedHourStart);
        addTotals(bucket.totals, { conversation_count: deltaMessageCount });
        touchedBuckets.add(bucketKey("grok", lastModel || model, finalTouchedHourStart));
      }
    }

    if (finalTotal > 0 && (tokenDeltaForSession > 0 || previousTotal > 0 || legacyBaselineOnly)) {
      sessionSnapshots[sessionId] = {
        totalTokens: finalTotal,
        messageCount: Math.max(previousMessageCount, messageCount),
        model: lastModel || model,
        source: source || previous.source || null,
        lastEventId,
        lastEventTimestamp,
        updatedAt: new Date().toISOString(),
      };
    } else if (previous.legacySeen && finalTotal === 0) {
      // Keep the baseline marker across empty syncs so later growth is still
      // baselined once instead of backfilled as brand-new usage.
      sessionSnapshots[sessionId] = {
        totalTokens: 0,
        messageCount: Math.max(previousMessageCount, messageCount),
        model: lastModel || model,
        source: previous.source || null,
        lastEventId: lastEventId || previous.lastEventId || null,
        lastEventTimestamp: lastEventTimestamp || previous.lastEventTimestamp || null,
        updatedAt: new Date().toISOString(),
        legacySeen: true,
      };
    }

    if (projectEnabled) {
      const cwd = resolveGrokSessionCwd(sess, summary);
      if (cwd) {
        const projectContext = await resolveProjectContextForPath({
          startDir: wsl.mapWslCwdToUnc(cwd, updatesPath || sess?.sessionDir || cwd),
          projectMetaCache,
          publicRepoCache,
          publicRepoResolver,
          projectState,
        });
        const projectRef = projectContext?.projectRef || null;
        const projectKey = projectContext?.projectKey || null;
        // Only advance the project cursor on successful public_verified attribution.
        // Blocked / missing git / unverified stay unadvanced so a later sync can retry.
        if (projectKey && projectRef) {
          const projectPrevious = projectSessionSnapshots[sessionId] || {};
          const projectPreviousTotal = normalizeNonNegativeNumber(projectPrevious.totalTokens);
          const projectPreviousMessageCount = normalizeNonNegativeNumber(
            projectPrevious.messageCount,
          );
          let projectSawTurnUsage = projectPrevious.source === "turn_usage";
          let projectCumulativeTotal = projectPreviousTotal;
          let projectTokenDelta = 0;
          let projectFinalTouchedHourStart = null;
          let projectSource = projectPrevious.source || null;
          let projectLastModel = projectPrevious.model || model;
          const projectPendingDeltas = [];

          const projectUpdates = await readGrokUpdateTokenEvents(
            updatesPath,
            lastActive,
            updatesPath ? projectUpdateOffsets[updatesPath] : null,
            { fallbackModel: model },
          );

          for (const event of projectUpdates.turnEvents) {
            projectSawTurnUsage = true;
            const hourStartStr =
              toUtcHalfHourStart(event.timestamp) ||
              toUtcHalfHourStart(lastActive) ||
              toUtcHalfHourStart(Date.now());
            if (!hourStartStr) continue;
            const eventModel = event.model || model;
            const delta = {
              input_tokens: event.input_tokens,
              cached_input_tokens: event.cached_input_tokens,
              cache_creation_input_tokens: event.cache_creation_input_tokens,
              output_tokens: event.output_tokens,
              reasoning_output_tokens: event.reasoning_output_tokens,
              total_tokens: event.total_tokens,
              billable_total_tokens: event.billable_total_tokens,
              conversation_count: event.conversation_count || 1,
            };
            projectPendingDeltas.push({ model: eventModel, hourStartStr, delta });
            projectCumulativeTotal += event.total_tokens;
            projectTokenDelta += event.total_tokens;
            projectFinalTouchedHourStart = hourStartStr;
            projectSource = "turn_usage";
            projectLastModel = eventModel;
          }

          // Watermark fallback uses the independent project snapshot, never
          // global sessionSnapshots.totalTokens, and never stacks on turn usage.
          if (!projectSawTurnUsage) {
            let highWatermark = projectPreviousTotal;
            for (const event of projectUpdates.contextEvents) {
              if (event.totalTokens <= highWatermark) continue;
              const deltaTokens = event.totalTokens - highWatermark;
              highWatermark = event.totalTokens;
              const hourStartStr =
                toUtcHalfHourStart(event.timestamp) ||
                toUtcHalfHourStart(lastActive) ||
                toUtcHalfHourStart(Date.now());
              if (!hourStartStr) continue;
              const delta = estimateGrokTokenDelta(deltaTokens, 0, {
                allowZeroConversationCount: true,
              });
              projectPendingDeltas.push({ model, hourStartStr, delta });
              projectTokenDelta += deltaTokens;
              projectFinalTouchedHourStart = hourStartStr;
              projectSource = "updates";
            }

            const effectiveSignalTotal = grokEffectiveTotalFromSignals(safeSignals);
            if (effectiveSignalTotal > highWatermark) {
              const deltaTokens = effectiveSignalTotal - highWatermark;
              highWatermark = effectiveSignalTotal;
              const hourStartStr =
                toUtcHalfHourStart(lastActive) || toUtcHalfHourStart(Date.now());
              if (hourStartStr) {
                const delta = estimateGrokTokenDelta(deltaTokens, 0, {
                  allowZeroConversationCount: true,
                });
                projectPendingDeltas.push({ model, hourStartStr, delta });
                projectTokenDelta += deltaTokens;
                projectFinalTouchedHourStart = hourStartStr;
                projectSource = "signals";
              }
            }
            projectCumulativeTotal = Math.max(projectPreviousTotal, highWatermark);
          }

          const projectFinalTotal = Math.max(projectPreviousTotal, projectCumulativeTotal);

          // legacyBaselineOnly applies only to global buckets.
          for (const pending of projectPendingDeltas) {
            const projectBucket = getProjectBucket(
              projectState,
              projectKey,
              "grok",
              pending.hourStartStr,
              projectRef,
            );
            addTotals(projectBucket.totals, pending.delta);
            projectTouchedBuckets.add(
              projectBucketKey(projectKey, "grok", pending.hourStartStr),
            );
          }

          if (!projectSawTurnUsage && projectTokenDelta > 0 && projectFinalTouchedHourStart) {
            const deltaMessageCount =
              messageCount > projectPreviousMessageCount
                ? messageCount - projectPreviousMessageCount
                : 1;
            const projectBucket = getProjectBucket(
              projectState,
              projectKey,
              "grok",
              projectFinalTouchedHourStart,
              projectRef,
            );
            addTotals(projectBucket.totals, { conversation_count: deltaMessageCount });
            projectTouchedBuckets.add(
              projectBucketKey(projectKey, "grok", projectFinalTouchedHourStart),
            );
          }

          if (updatesPath && projectUpdates.offsetEntry) {
            projectUpdateOffsets[updatesPath] = projectUpdates.offsetEntry;
          }

          if (projectFinalTotal > 0 && (projectTokenDelta > 0 || projectPreviousTotal > 0)) {
            projectSessionSnapshots[sessionId] = {
              totalTokens: projectFinalTotal,
              messageCount: Math.max(projectPreviousMessageCount, messageCount),
              model: projectLastModel || model,
              source: projectSource || projectPrevious.source || null,
              updatedAt: new Date().toISOString(),
            };
          }
        }
      }
    }

    if (onProgress) {
      onProgress({ index: index + 1, total: sessionList.length, bucketsQueued: touchedBuckets.size });
    }
  }

  if (projectEnabled) pruneMissingGrokProjectUpdateOffsets(projectUpdateOffsets);

  let bucketsQueued = queuePath
    ? await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets })
    : 0;

  // After a semantics migration, retract stale grok queue keys that the full
  // rescan no longer produces so dashboard "latest per key" no longer keeps
  // the old undercounted rows.
  if (needsTurnUsageMigration && queuePath) {
    const keepKeys = new Set();
    for (const [key, bucket] of Object.entries(hourlyState.buckets || {})) {
      if (!key.startsWith("grok|") || !bucket?.totals) continue;
      keepKeys.add(key);
    }
    const retracted = await retractStaleSourceQueueRows(queuePath, "grok", keepKeys);
    bucketsQueued += retracted;
  }

  const projectBucketsQueued = projectEnabled
    ? await enqueueTouchedProjectBuckets({
        projectQueuePath,
        projectState,
        projectTouchedBuckets,
      })
    : 0;

  hourlyState.updatedAt = new Date().toISOString();
  cursors.hourly = hourlyState;
  if (projectState) {
    projectState.updatedAt = hourlyState.updatedAt;
    cursors.projectHourly = projectState;
  }
  sessionSnapshots = capGrokSessionSnapshots(sessionSnapshots);

  const migrations = grokState.migrations && typeof grokState.migrations === "object"
    ? { ...grokState.migrations }
    : {};
  if (needsTurnUsageMigration) {
    migrations.turnUsageV5 = {
      appliedAt: new Date().toISOString(),
      fromVersion: prevVersion,
      toVersion: GROK_CURSOR_VERSION,
    };
  }

  cursors.grok = {
    ...grokState,
    version: GROK_CURSOR_VERSION,
    sessionSnapshots,
    seenSessions: Object.keys(sessionSnapshots),
    updateOffsets,
    ...(projectEnabled
      ? {
          projectUpdateOffsets,
          projectSessionSnapshots: capGrokSessionSnapshots(projectSessionSnapshots),
        }
      : {}),
    migrations,
    updatedAt: new Date().toISOString()
  };

  return {
    recordsProcessed: eventsAggregated,
    eventsAggregated,
    bucketsQueued,
    projectBucketsQueued,
  };
}

module.exports = {
  resolveGrokBuildHome,
  resolveGrokBuildSessions,
  parseGrokBuildIncremental,
};
