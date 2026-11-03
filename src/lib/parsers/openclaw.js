// openclaw usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fs = require("node:fs/promises");
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const readline = require("node:readline");
const crypto = require("node:crypto");
const { ensureDir } = require("../fs");
const { readSqliteJsonRowsAsync } = require("../sqlite-reader");
const wsl = require("../wsl-probe");
const {
  DEFAULT_MODEL,
  enqueueTouchedBuckets,
  enqueueTouchedProjectBuckets,
  normalizeHourlyState,
  normalizeProjectState,
  getHourlyBucket,
  addTotals,
  toUtcHalfHourStart,
  bucketKey,
  parseBucketKey,
  normalizeSourceInput,
  normalizeModelInput,
  isAllZeroUsage,
  safeReadDir,
} = require("./core");

// Passive discovery of OpenClaw session transcripts (issue #264). OpenClaw was
// previously counted ONLY when its session plugin fired an `agent_end` hook and
// pointed sync at one specific <sessionId>.jsonl. That hook silently fails for a
// whole class of real usage — messages arriving through a channel (e.g. the
// WeChat ClawBot) whose sessionKey the plugin can't map back to a sessions.json
// entry, gateways where the plugin never loaded, or newer installs that keep
// runtime rows in SQLite and only leave archived transcripts on disk. In all of
// those cases usage exists but shows as 0. Scanning the on-disk transcripts on
// every full sync (like every other passive provider) closes that gap; the
// event-identity dedup in parseOpenclawSessionFile makes a plugin trigger and a
// passive scan of the same file idempotent, so the two paths never double-count.
// Mirrors OpenClaw's own precedence (OPENCLAW_HOME > state dir override), but
// deliberately keeps os.homedir() — NOT env.HOME — as the base. OpenClaw itself
// prefers env.HOME, yet every OpenClaw cursor we have ever written was keyed off
// an os.homedir()-derived path; switching the base would make Git Bash / MSYS
// (HOME=/c/Users/x vs C:\Users\x) miss its own cursor and re-count the whole
// transcript history.
function resolveOpenclawHome(env = process.env) {
  const override =
    (typeof env.TOKEN_PULSE_OPENCLAW_HOME === "string" && env.TOKEN_PULSE_OPENCLAW_HOME.trim()) ||
    (typeof env.OPENCLAW_HOME === "string" && env.OPENCLAW_HOME.trim()) ||
    (typeof env.OPENCLAW_STATE_DIR === "string" && env.OPENCLAW_STATE_DIR.trim()) ||
    "";
  return override || path.join(os.homedir(), ".openclaw");
}

// On Windows the recommended OpenClaw install provisions an app-owned WSL
// distro and runs the gateway inside it, so %USERPROFILE%\.openclaw is empty
// while the real state dir sits on the distro's ext4 home (issue #264).
function resolveOpenclawHomes(env = process.env, deps = {}) {
  const roots = [resolveOpenclawHome(env)];
  const platform = deps.platform || process.platform;
  const overridden =
    env.TOKEN_PULSE_OPENCLAW_HOME || env.OPENCLAW_HOME || env.OPENCLAW_STATE_DIR;
  if (platform === "win32" && !overridden) {
    const discoverWslHome = deps.discoverWslHome || wsl.discoverWslHome;
    const wslRoot = wsl.shouldProbeWsl(env) ? discoverWslHome(".openclaw", { ...deps, env }) : null;
    if (wslRoot) roots.push(wslRoot);
  }
  return roots;
}

// Transcript artifacts are not always a bare `<sessionId>.jsonl`: resets and
// deletes leave `<sessionId>.jsonl.reset.<iso>` / `.deleted.<ts>` siblings, and
// the SQLite migration moves still-hot transcripts into
// `session-sqlite-import-archive/`. Match the same `*.jsonl*` shape other
// OpenClaw readers use so those are not silently dropped.
const OPENCLAW_TRANSCRIPT_SUBDIRS = ["sessions", "session-sqlite-import-archive"];

function isOpenclawTranscriptName(name) {
  return typeof name === "string" && name.includes(".jsonl") && !name.endsWith(".json");
}

async function resolveOpenclawSessionFiles(env = process.env, deps = {}) {
  const out = [];
  const seen = new Set();
  for (const openclawHome of resolveOpenclawHomes(env, deps)) {
    const agentsDir = path.join(openclawHome, "agents");
    const agents = await safeReadDir(agentsDir);
    for (const agent of agents) {
      if (!agent.isDirectory()) continue;
      for (const subdir of OPENCLAW_TRANSCRIPT_SUBDIRS) {
        const dir = path.join(agentsDir, agent.name, subdir);
        const entries = await safeReadDir(dir);
        for (const entry of entries) {
          if (!entry.isFile() || !isOpenclawTranscriptName(entry.name)) continue;
          const full = path.join(dir, entry.name);
          const key = openclawCursorKey(full);
          if (seen.has(key)) continue;
          seen.add(key);
          out.push(full);
        }
      }
      for (const dbPath of [
        path.join(agentsDir, agent.name, "agent", "openclaw-agent.sqlite"),
        path.join(agentsDir, agent.name, "openclaw-agent.sqlite"),
      ]) {
        if ((await fs.stat(dbPath).catch(() => null))?.isFile()) {
          out.push(dbPath);
          break; // alternate layouts of one agent database, not two sources
        }
      }
    }
  }
  // Import archives must seed dedup before the corresponding SQLite history.
  out.sort((a, b) => Number(a.endsWith(".sqlite")) - Number(b.endsWith(".sqlite")) || a.localeCompare(b));
  return out;
}

async function parseOpenclawIncremental({
  sessionFiles,
  cursors,
  queuePath,
  projectQueuePath,
  onProgress,
  source,
}) {
  await ensureDir(path.dirname(queuePath));
  let filesProcessed = 0;
  let eventsAggregated = 0;

  const cb = typeof onProgress === "function" ? onProgress : null;
  const files = Array.isArray(sessionFiles) ? sessionFiles : [];
  const totalFiles = files.length;
  const hourlyState = normalizeHourlyState(
    cursors?.hourly ? structuredClone(cursors.hourly) : null,
  );
  const projectEnabled = typeof projectQueuePath === "string" && projectQueuePath.length > 0;
  const projectState = projectEnabled
    ? normalizeProjectState(
        cursors?.projectHourly ? structuredClone(cursors.projectHourly) : null,
      )
    : null;
  const projectTouchedBuckets = projectEnabled ? new Set() : null;
  const touchedBuckets = new Set();
  const defaultSource = normalizeSourceInput(source) || "openclaw";

  const stagedFiles =
    cursors.files && typeof cursors.files === "object"
      ? { ...cursors.files }
      : {};
  const cursorKeys = new Map();
  for (const existingKey of Object.keys(stagedFiles)) {
    cursorKeys.set(openclawCursorKey(existingKey), existingKey);
  }

  for (let idx = 0; idx < files.length; idx++) {
    const entry = files[idx];
    const filePath = typeof entry === "string" ? entry : entry?.path;
    if (!filePath) continue;
    const fileSource =
      typeof entry === "string"
        ? defaultSource
        : normalizeSourceInput(entry?.source) || defaultSource;
    const key = openclawCursorKey(filePath);
    const previousKey = cursorKeys.get(key);
    const prev = previousKey ? stagedFiles[previousKey] : null;
    const prevUsageEvents =
      prev?.usageEvents && typeof prev.usageEvents === "object" ? prev.usageEvents : {};
    const hasUsageEventCursor =
      prev &&
      Object.prototype.hasOwnProperty.call(prev, "usageEvents") &&
      prev.usageEvents &&
      typeof prev.usageEvents === "object";
    const legacyCursor = prev && !hasUsageEventCursor;
    const previousOffset = Number(prev?.offset || 0);
    let accepted = null;
    let openedRegularFile = false;
    if (filePath.endsWith(".sqlite")) {
      const sessionFilter = typeof entry?.sessionId === "string"
        ? ` AND e.session_id = '${entry.sessionId.replace(/'/g, "''")}'` : "";
      const rows = await readSqliteJsonRowsAsync(filePath, `
        SELECT e.session_id, json_object(
          'type', json_extract(e.event_json, '$.type'),
          'id', json_extract(e.event_json, '$.id'),
          'timestamp', json_extract(e.event_json, '$.timestamp'),
          'message', json_object(
            'role', json_extract(e.event_json, '$.message.role'),
            'model', json_extract(e.event_json, '$.message.model'),
            'provider', json_extract(e.event_json, '$.message.provider'),
            'api', json_extract(e.event_json, '$.message.api'),
            'timestamp', json_extract(e.event_json, '$.message.timestamp'),
            'responseId', json_extract(e.event_json, '$.message.responseId'),
            'usage', json_extract(e.event_json, '$.message.usage')
          )) AS event
        FROM transcript_events e JOIN session_windows w ON w.session_id = e.session_id
        WHERE json_valid(e.event_json) AND COALESCE(w.reason, '') != 'compaction'
          AND json_extract(e.event_json, '$.message.role') = 'assistant'
          AND json_extract(e.event_json, '$.message.usage') IS NOT NULL
          ${sessionFilter}
        ORDER BY e.session_id, e.seq
      `, { readOnly: true, throwOnReadFailure: true, label: "OpenClaw" });
      const sessions = new Map();
      for (const row of rows) {
        if (!sessions.has(row.session_id)) sessions.set(row.session_id, []);
        sessions.get(row.session_id).push(row.event);
      }
      for (const [sessionId, lines] of sessions) {
        const sessionCursorKey = `${key}#${sessionId}`;
        const prior = { ...(stagedFiles[sessionCursorKey]?.usageEvents || {}) };
        const dbDir = path.dirname(filePath);
        const agentDir = path.basename(dbDir) === "agent" && path.basename(path.dirname(path.dirname(dbDir))) === "agents"
          ? path.dirname(dbDir) : dbDir;
        const agentPrefix = openclawCursorKey(`${agentDir}${path.sep}`);
        // SQLite imports retain transcript identities. Carry forward counts
        // from this session's legacy transcript, including reset archives.
        for (const [legacyPath, cursor] of Object.entries(stagedFiles)) {
          const basename = path.basename(legacyPath);
          if (cursor.provider !== "openclaw" || !openclawCursorKey(legacyPath).startsWith(agentPrefix)) continue;
          if (!basename.startsWith(`${sessionId}.jsonl`) &&
              !(basename.startsWith(`${sessionId}-topic-`) && basename.includes(".jsonl"))) continue;
          for (const [identity, count] of Object.entries(cursor.usageEvents || {})) {
            prior[identity] = Math.max(prior[identity] || 0, count);
          }
        }
        const result = await parseOpenclawSessionFile({
          lines, previousUsageEvents: prior, appendOnly: false,
          hourlyState, touchedBuckets, source: fileSource,
        });
        stagedFiles[sessionCursorKey] = {
          provider: "openclaw", usageEvents: result.usageEvents,
          hasRealUsage: Object.keys(result.usageEvents).length > 0,
          updatedAt: new Date().toISOString(),
        };
        eventsAggregated += result.eventsAggregated;
      }
      filesProcessed += 1;
      continue;
    }
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const fileHandle = await fs.open(filePath, "r").catch(() => null);
      if (!fileHandle) break;
      try {
        const openedStat = await fileHandle.stat();
        if (!openedStat.isFile()) break;
        openedRegularFile = true;
        const fileEndOffset = openedStat.size;
        let prefixFingerprint = null;
        let prefixFingerprintState = null;
        const offsetBoundaryMatches =
          hasUsageEventCursor &&
          previousOffset <= fileEndOffset &&
          await openclawOffsetEndsAtLineBoundary(fileHandle, previousOffset);
        const parsedLegacyUpdatedAt = Date.parse(String(prev?.updatedAt || ""));
        const legacyAggregateAfterMs =
          legacyCursor
            ? Number.isFinite(parsedLegacyUpdatedAt)
              ? parsedLegacyUpdatedAt
              : Number.POSITIVE_INFINITY
            : null;
        if (
          hasUsageEventCursor &&
          offsetBoundaryMatches &&
          prev?.usageFingerprint
        ) {
          const prefix = await parseOpenclawSessionFile({
            filePath,
            fileHandle,
            startOffset: 0,
            endOffsetLimit: previousOffset,
            previousUsageEvents: {},
            appendOnly: false,
            collectOnly: true,
          });
          prefixFingerprint = prefix.usageFingerprint;
          prefixFingerprintState = prefix.usageFingerprintState;
        }
        const appendOnly =
          hasUsageEventCursor &&
          previousOffset <= fileEndOffset &&
          offsetBoundaryMatches &&
          sameOpenclawUsageFingerprint(
            prefixFingerprint,
            prev.usageFingerprint,
          );
        const startOffset = appendOnly ? previousOffset : 0;
        const attemptHourly = { version: 3, buckets: {}, groupQueued: {} };
        const attemptTouched = new Set();
        const result = await parseOpenclawSessionFile({
          filePath,
          fileHandle,
          startOffset,
          endOffsetLimit: fileEndOffset,
          previousUsageEvents: prevUsageEvents,
          appendOnly,
          aggregateAfterMs: legacyAggregateAfterMs,
          usageFingerprintState: appendOnly ? prefixFingerprintState : null,
          hourlyState: attemptHourly,
          touchedBuckets: attemptTouched,
          source: fileSource,
          projectState: null,
          projectTouchedBuckets: null,
        });
        const closedStat = await fileHandle.stat();
        const pathStat = await fs.stat(filePath).catch(() => null);
        if (
          !sameOpenclawFileGeneration(openedStat, closedStat) ||
          !sameOpenclawFileGeneration(closedStat, pathStat)
        ) {
          continue;
        }
        accepted = {
          inode: closedStat.ino || 0,
          result,
          hourly: attemptHourly,
          touched: attemptTouched,
          usageEvents: result.usageEvents,
          usageFingerprint: result.usageFingerprint,
        };
        break;
      } finally {
        await fileHandle.close().catch(() => {});
      }
    }
    if (!accepted) {
      if (!openedRegularFile) continue;
      throw new Error(`OpenClaw session changed repeatedly while parsing: ${filePath}`);
    }

    mergeOpenclawAttemptBuckets(
      hourlyState,
      touchedBuckets,
      accepted.hourly,
      accepted.touched,
    );
    if (previousKey && previousKey !== key) delete stagedFiles[previousKey];
    stagedFiles[key] = {
      provider: "openclaw",
      inode: accepted.inode,
      offset: accepted.result.endOffset,
      usageFingerprint: accepted.usageFingerprint,
      updatedAt: new Date().toISOString(),
      usageEvents: accepted.usageEvents,
      // Sticky: once a transcript has yielded real per-event usage, the
      // sessions.json totals fallback must defer to it to avoid double
      // counting the same tokens twice (see applyOpenclawTotalsFallback).
      //
      // The usageEvents check is what makes this work for EXISTING installs:
      // cursors written before this flag existed carry no `hasRealUsage`, and a
      // steady-state sync of an unchanged transcript aggregates 0 new events —
      // so keying off eventsAggregated alone would leave the flag false forever
      // and let the fallback keep double counting for exactly the users who
      // already have OpenClaw history.
      hasRealUsage:
        Boolean(prev?.hasRealUsage) ||
        accepted.result.eventsAggregated > 0 ||
        Object.keys(accepted.usageEvents || {}).length > 0,
    };
    cursorKeys.set(key, key);

    filesProcessed += 1;
    eventsAggregated += accepted.result.eventsAggregated;

    if (cb) {
      cb({
        index: idx + 1,
        total: totalFiles,
        filePath,
        filesProcessed,
        eventsAggregated,
        bucketsQueued: touchedBuckets.size,
      });
    }
  }

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const projectBucketsQueued = projectEnabled
    ? await enqueueTouchedProjectBuckets({ projectQueuePath, projectState, projectTouchedBuckets })
    : 0;
  cursors.files = stagedFiles;
  hourlyState.updatedAt = new Date().toISOString();
  cursors.hourly = hourlyState;
  if (projectState) {
    projectState.updatedAt = new Date().toISOString();
    cursors.projectHourly = projectState;
  }

  return { filesProcessed, eventsAggregated, bucketsQueued, projectBucketsQueued };
}

function openclawCursorKey(filePath) {
  if (typeof filePath !== "string") return "";
  const normalized = filePath.replace(/\\/g, "/");
  const isWindowsPath =
    /^[a-zA-Z]:\//.test(normalized) || normalized.startsWith("//");
  return isWindowsPath ? normalized.toLowerCase() : filePath;
}

function sameOpenclawFileGeneration(left, right) {
  if (!left || !right || !left.isFile?.() || !right.isFile?.()) return false;
  const leftInode = Number(left.ino || 0);
  const rightInode = Number(right.ino || 0);
  if (leftInode > 0 && rightInode > 0) {
    if (leftInode !== rightInode || Number(left.dev || 0) !== Number(right.dev || 0)) {
      return false;
    }
  }
  return (
    Number(left.size || 0) === Number(right.size || 0) &&
    Number(left.mtimeMs || 0) === Number(right.mtimeMs || 0) &&
    Number(left.ctimeMs || 0) === Number(right.ctimeMs || 0)
  );
}

async function openclawOffsetEndsAtLineBoundary(fileHandle, offset) {
  const byteOffset = Math.max(0, Number(offset) || 0);
  if (byteOffset === 0) return true;
  const buffer = Buffer.allocUnsafe(1);
  const { bytesRead } = await fileHandle.read(buffer, 0, 1, byteOffset - 1);
  return bytesRead === 1 && (buffer[0] === 0x0a || buffer[0] === 0x0d);
}

function sameOpenclawUsageFingerprint(left, right) {
  return (
    left?.version === 1 &&
    right?.version === 1 &&
    left.eventCount === right.eventCount &&
    left.digest === right.digest
  );
}

function mergeOpenclawAttemptBuckets(
  hourlyState,
  touchedBuckets,
  attemptHourly,
  attemptTouched,
) {
  for (const key of attemptTouched) {
    const parsed = parseBucketKey(key);
    const attemptBucket = attemptHourly.buckets[key];
    if (!parsed || !attemptBucket) continue;
    const bucket = getHourlyBucket(
      hourlyState,
      parsed.source,
      parsed.model,
      parsed.hourStart,
    );
    addTotals(bucket.totals, attemptBucket.totals);
    touchedBuckets.add(key);
  }
}

async function parseOpenclawSessionFile({
  lines,
  filePath,
  fileHandle,
  startOffset,
  endOffsetLimit,
  previousUsageEvents,
  appendOnly,
  aggregateAfterMs,
  usageFingerprintState,
  collectOnly = false,
  hourlyState,
  touchedBuckets,
  source,
  projectState,
  projectTouchedBuckets,
}) {
  const st = lines ? { size: lines.length } : fileHandle ? await fileHandle.stat() : await fs.stat(filePath);
  startOffset = startOffset || 0;
  const endOffset =
    Number.isFinite(endOffsetLimit) && endOffsetLimit >= 0
      ? Math.min(st.size, endOffsetLimit)
      : st.size;
  const priorCounts =
    previousUsageEvents && typeof previousUsageEvents === "object"
      ? previousUsageEvents
      : {};
  let fingerprintEventCount =
    Number.isInteger(usageFingerprintState?.eventCount) &&
    usageFingerprintState.eventCount >= 0
      ? usageFingerprintState.eventCount
      : 0;
  const fingerprintHash =
    typeof usageFingerprintState?.hash?.copy === "function"
      ? usageFingerprintState.hash.copy()
      : crypto.createHash("sha256");
  if (startOffset >= endOffset) {
    return {
      endOffset,
      eventsAggregated: 0,
      usageEvents: { ...priorCounts },
      usageFingerprint: {
        version: 1,
        eventCount: fingerprintEventCount,
        digest: fingerprintHash.copy().digest("hex"),
      },
      usageFingerprintState: {
        eventCount: fingerprintEventCount,
        hash: fingerprintHash,
      },
    };
  }

  const stream = lines ? null : fssync.createReadStream(filePath, {
    fd: fileHandle?.fd,
    autoClose: !fileHandle,
    encoding: "utf8",
    start: startOffset,
    end: endOffset - 1,
  });
  const rl = lines || readline.createInterface({ input: stream, crlfDelay: Infinity });

  let eventsAggregated = 0;
  const scanCounts = appendOnly ? { ...priorCounts } : {};
  const usageEvents = { ...priorCounts };
  for await (const line of rl) {
    if (!line) continue;
    // Fast-path filter: OpenClaw assistant messages include message.usage.totalTokens.
    if (!line.includes('"usage"') || !line.includes("totalTokens")) continue;

    let obj;
    try {
      obj = JSON.parse(line);
    } catch (_e) {
      continue;
    }

    if (obj?.type !== "message") continue;
    const msg = obj?.message;
    if (!msg || typeof msg !== "object") continue;

    const usage = normalizeOpenclawUsage(msg.usage);
    if (!usage) continue;

    const tokenTimestamp = typeof obj?.timestamp === "string" ? obj.timestamp : null;
    if (!tokenTimestamp) continue;

    const model = normalizeModelInput(msg.model) || DEFAULT_MODEL;
    const eventIdentity = openclawUsageEventIdentity(obj, msg, usage, model);
    fingerprintHash.update(
      `${JSON.stringify(
        openclawUsageFingerprintMetadata(
          obj,
          msg,
          usage,
          model,
          eventIdentity.key,
        ),
      )}\n`,
    );
    fingerprintEventCount += 1;

    // OpenClaw wraps Codex, so it follows the same OpenAI convention where
    // `input` INCLUDES cached reads. Normalize by subtracting cached from
    // input so `input_tokens` is pure non-cached (matches CLAUDE.md spec
    // and prevents downstream double-counting at the cache_read rate on top
    // of the full input rate — ~6–7x cost inflation on cache-heavy sessions).
    const openclawRawInput = Number(usage.input || 0);
    const openclawCached = Number(usage.cacheRead || 0);
    const openclawCacheWrite = Number(usage.cacheWrite || 0);
    const openclawOutput = Number(usage.output || 0);
    const openclawInput = Math.max(0, openclawRawInput - openclawCached);
    const delta = {
      input_tokens: openclawInput,
      cached_input_tokens: openclawCached,
      cache_creation_input_tokens: openclawCacheWrite,
      output_tokens: openclawOutput,
      reasoning_output_tokens: 0,
      total_tokens: openclawInput + openclawCached + openclawCacheWrite + openclawOutput,
      conversation_count: 1,
    };

    if (isAllZeroUsage(delta)) continue;

    const bucketStart = toUtcHalfHourStart(tokenTimestamp);
    if (!bucketStart) continue;

    const priorCount = Number(priorCounts[eventIdentity.key] || 0);
    const nextCount = Number(scanCounts[eventIdentity.key] || 0) + 1;
    scanCounts[eventIdentity.key] = nextCount;
    usageEvents[eventIdentity.key] = eventIdentity.unique
      ? 1
      : Math.max(Number(usageEvents[eventIdentity.key] || 0), nextCount);
    if (
      eventIdentity.unique
        ? priorCount > 0 || nextCount > 1
        : nextCount <= priorCount
    ) {
      continue;
    }

    if (
      aggregateAfterMs != null &&
      Date.parse(tokenTimestamp) <= aggregateAfterMs
    ) {
      continue;
    }

    if (!collectOnly) {
      const bucket = getHourlyBucket(hourlyState, source, model, bucketStart);
      addTotals(bucket.totals, delta);
      touchedBuckets.add(bucketKey(source, model, bucketStart));
    }

    // Project-level OpenClaw attribution is not supported yet (no stable cwd info).
    // If OpenClaw later records cwd per event, we can mirror rollout's project logic.
    if (!collectOnly) eventsAggregated += 1;
  }

  if (!lines) rl.close();
  if (!fileHandle) stream?.close?.();
  return {
    endOffset,
    eventsAggregated,
    usageEvents,
    usageFingerprint: {
      version: 1,
      eventCount: fingerprintEventCount,
      digest: fingerprintHash.copy().digest("hex"),
    },
    usageFingerprintState: {
      eventCount: fingerprintEventCount,
      hash: fingerprintHash,
    },
  };
}

function normalizeOpenclawUsage(usage) {
  if (!usage || typeof usage !== "object") return null;
  const normalized = {};
  for (const field of [
    "input",
    "cacheRead",
    "cacheWrite",
    "output",
    "totalTokens",
  ]) {
    const rawValue = usage[field];
    if (rawValue === undefined) {
      normalized[field] = 0;
      continue;
    }
    if (
      typeof rawValue !== "number" ||
      !Number.isSafeInteger(rawValue) ||
      rawValue < 0
    ) {
      return null;
    }
    normalized[field] = rawValue;
  }
  return normalized;
}

function openclawUsageFingerprintMetadata(
  obj,
  msg,
  usage,
  model,
  identityKey,
) {
  return {
    identityKey,
    timestamp: typeof obj?.timestamp === "string" ? obj.timestamp : null,
    messageTimestamp:
      typeof msg?.timestamp === "number" || typeof msg?.timestamp === "string"
        ? msg.timestamp
        : null,
    responseId: typeof msg?.responseId === "string" ? msg.responseId : null,
    model,
    provider: typeof msg?.provider === "string" ? msg.provider : null,
    api: typeof msg?.api === "string" ? msg.api : null,
    input: Number(usage?.input || 0),
    cacheRead: Number(usage?.cacheRead || 0),
    cacheWrite: Number(usage?.cacheWrite || 0),
    output: Number(usage?.output || 0),
    totalTokens: Number(usage?.totalTokens || 0),
  };
}

function openclawUsageEventIdentity(obj, msg, usage, model) {
  const stableId =
    typeof obj?.id === "string" && obj.id.trim() ? obj.id.trim() : null;
  if (stableId) {
    return {
      key: `id:${crypto.createHash("sha256").update(stableId).digest("hex")}`,
      unique: true,
    };
  }

  const metadata = {
    timestamp: typeof obj?.timestamp === "string" ? obj.timestamp : null,
    messageTimestamp: typeof msg?.timestamp === "number" || typeof msg?.timestamp === "string"
      ? msg.timestamp
      : null,
    responseId: typeof msg?.responseId === "string" ? msg.responseId : null,
    model,
    provider: typeof msg?.provider === "string" ? msg.provider : null,
    api: typeof msg?.api === "string" ? msg.api : null,
    input: Number(usage?.input || 0),
    cacheRead: Number(usage?.cacheRead || 0),
    cacheWrite: Number(usage?.cacheWrite || 0),
    output: Number(usage?.output || 0),
    totalTokens: Number(usage?.totalTokens || 0),
  };
  return {
    key: `meta:${crypto.createHash("sha256").update(JSON.stringify(metadata)).digest("hex")}`,
    unique: false,
  };
}

module.exports = {
  resolveOpenclawHome,
  resolveOpenclawHomes,
  resolveOpenclawSessionFiles,
  parseOpenclawIncremental,
  openclawCursorKey,
};
