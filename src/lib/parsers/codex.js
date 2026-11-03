// codex usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fs = require("node:fs/promises");
const fssync = require("node:fs");
const path = require("node:path");
const { ensureDir } = require("../fs");
const { physicalJsonlRecords } = require("../jsonl-lines");
const wsl = require("../wsl-probe");
const { consumeUsageDelta, createUsageDeltaState, snapshotUsageBaselines } = require("../codex-token-usage");
const { applyCodexModelEvent, createCodexModelAttributionState, currentCodexModel, snapshotCodexModelAttributionState } = require("../codex-model-attribution");
const { CODEX_SERVICE_TIER_MARKER, readCodexServiceTier, isPriorityServiceTier } = require("../codex-service-tier");
const { awaitsCompactedLine, createCompactionResponseIds, createUsageRecordState, extractTokenUsageRecord, isCodexTurnEndEvent, isRecordOnly, noteLineAfterUsageRecord, noteTokenCount, noteTurnEnd, noteUsageRecord, snapshotUsageRecordState, takeCompactionOnTokenCount } = require("../codex-usage-record");
const {
  DEFAULT_SOURCE,
  enqueueTouchedBuckets,
  enqueueTouchedProjectBuckets,
  normalizeHourlyState,
  normalizeProjectState,
  getHourlyBucket,
  getProjectBucket,
  PRIORITY_SUBSET_FIELDS,
  addTotals,
  toUtcHalfHourStart,
  bucketKey,
  projectBucketKey,
  normalizeSourceInput,
  buildProjectFileContext,
  isProjectFileContextFresh,
  resolveProjectContextForFile,
  resolveProjectContextForPath,
  isAllZeroUsage,
  safeReadDir,
  inspectDshZstdFrames,
  decodeDshZstd,
} = require("./core");
const DEFAULT_CODEX_COLD_SKIP_RECENT_DAYS = 2;
// A rollout keeps its creation date in its name, but Codex appends to a session
// for as long as it stays open, so a file dated Monday can still be growing on
// Thursday. Cold files younger than this are stat'ed every sync and re-read when
// they grew past the cursor (#592); older ones stay stat-free and rely on the
// daily cold-scan audit, which bounds the per-sync cost on machines with tens of
// thousands of rollouts.
const DEFAULT_CODEX_COLD_GROWTH_STAT_DAYS = 30;
const FILE_METADATA_CONCURRENCY = 32;

async function mapConcurrent(items, concurrency, mapper) {
  if (!Array.isArray(items) || items.length === 0) return [];
  const results = new Array(items.length);
  let nextIndex = 0;
  const workerCount = Math.min(
    items.length,
    Math.max(1, Math.floor(Number(concurrency) || 1)),
  );
  const workers = Array.from({ length: workerCount }, async () => {
    while (nextIndex < items.length) {
      const index = nextIndex;
      nextIndex += 1;
      results[index] = await mapper(items[index], index);
    }
  });
  await Promise.all(workers);
  return results;
}

async function listRolloutFiles(sessionsDir, options = {}) {
  const out = [];
  const dayInventoryCache =
    options?.dayInventoryCache && typeof options.dayInventoryCache === "object"
      ? options.dayInventoryCache
      : null;
  const stats = options?.stats && typeof options.stats === "object" ? options.stats : null;
  if (dayInventoryCache) {
    // v2: isRolloutFileName also accepts rollout-*.jsonl.zst/.zstd. Old v1
    // day lists omit archives that were already on disk when the directory
    // metadata last matched, so bump to force a one-time rescan.
    if (dayInventoryCache.version !== 2) {
      dayInventoryCache.days = {};
    }
    dayInventoryCache.version = 2;
    if (!dayInventoryCache.days || typeof dayInventoryCache.days !== "object") {
      dayInventoryCache.days = {};
    }
  }
  const years = (await safeReadDir(sessionsDir))
    .filter((entry) => /^[0-9]{4}$/.test(entry.name) && entry.isDirectory());
  const monthGroups = await mapConcurrent(
    years,
    FILE_METADATA_CONCURRENCY,
    async (year) => {
      const yearDir = path.join(sessionsDir, year.name);
      const months = await safeReadDir(yearDir);
      return months
        .filter((entry) => /^[0-9]{2}$/.test(entry.name) && entry.isDirectory())
        .map((entry) => path.join(yearDir, entry.name));
    },
  );
  const monthDirs = monthGroups.flat();
  const dayGroups = await mapConcurrent(
    monthDirs,
    FILE_METADATA_CONCURRENCY,
    async (monthDir) => {
      const days = await safeReadDir(monthDir);
      return days
        .filter((entry) => /^[0-9]{2}$/.test(entry.name) && entry.isDirectory())
        .map((entry) => path.join(monthDir, entry.name));
    },
  );
  const dayDirs = dayGroups.flat();
  const fileGroups = await mapConcurrent(
    dayDirs,
    FILE_METADATA_CONCURRENCY,
    (dayDir) => listRolloutDayFiles(dayDir, { dayInventoryCache, stats }),
  );
  for (const files of fileGroups) out.push(...files);

  out.sort((a, b) => a.localeCompare(b));
  return out;
}

async function listRolloutDayFiles(dayDir, { dayInventoryCache, stats } = {}) {
  const cacheDays = dayInventoryCache?.days;
  const dayStat = dayInventoryCache ? await fs.stat(dayDir).catch(() => null) : null;
  const statKey = dayStat && dayStat.isDirectory() ? directoryInventoryStatKey(dayStat) : null;
  const cached = cacheDays && cacheDays[dayDir];
  if (
    statKey &&
    cached &&
    typeof cached === "object" &&
    cached.statKey === statKey &&
    Array.isArray(cached.files) &&
    cached.files.every(isRolloutFileName)
  ) {
    if (stats) stats.dayInventoryCacheHits = Number(stats.dayInventoryCacheHits || 0) + 1;
    return cached.files.map((name) => path.join(dayDir, name));
  }

  if (stats && cached) stats.dayInventoryCacheMisses = Number(stats.dayInventoryCacheMisses || 0) + 1;
  const entries = await safeReadDir(dayDir);
  const files = [];
  for (const f of entries) {
    if (!f.isFile()) continue;
    if (!isRolloutFileName(f.name)) continue;
    files.push(f.name);
  }
  files.sort((a, b) => a.localeCompare(b));
  if (cacheDays && statKey) {
    cacheDays[dayDir] = {
      statKey,
      files,
      updatedAt: new Date().toISOString(),
    };
  }
  return files.map((name) => path.join(dayDir, name));
}

function isRolloutFileName(name) {
  if (typeof name !== "string") return false;
  if (name.includes("/") || name.includes("\\")) return false;
  if (name !== path.basename(name)) return false;
  if (!name.startsWith("rollout-")) return false;
  return name.endsWith(".jsonl") || isCompressedRolloutFileName(name);
}

// Compressed rollout archives cannot be tailed by byte offset: the cursor
// stores compressed bytes while the parser consumes decompressed text. Treat
// them as content-addressed snapshots — one parse per (inode, size) version —
// reusing the DSH session's zstd machinery (built-in zlib first,
// `@mongodb-js/zstd` fallback) including its multi-frame handling. Returns
// null for a missing/non-file path; an empty archive yields no lines (the
// same outcome as an empty uncompressed rollout).
const CODEX_ROLLOUT_COMPRESSED_PATTERN = /^rollout-.*\.jsonl\.zstd?$/;
const CODEX_ROLLOUT_TEXT_MAX_BYTES = 256 * 1024 * 1024;

function isCompressedRolloutFileName(name) {
  return typeof name === "string" && CODEX_ROLLOUT_COMPRESSED_PATTERN.test(name);
}

function isCompressedRolloutPath(filePath) {
  if (typeof filePath !== "string") return false;
  return isCompressedRolloutFileName(path.basename(filePath));
}

async function readCodexRolloutSnapshot(filePath, { maxOutputBytes = CODEX_ROLLOUT_TEXT_MAX_BYTES } = {}) {
  const handle = await fs.open(filePath, "r").catch(() => null);
  if (!handle) return null;
  try {
    const initialStat = await handle.stat().catch(() => null);
    if (!initialStat || !initialStat.isFile()) return null;
    const outputLimit = Number.isSafeInteger(maxOutputBytes) && maxOutputBytes >= 0
      ? maxOutputBytes
      : CODEX_ROLLOUT_TEXT_MAX_BYTES;
    let data;
    try {
      data = await handle.readFile();
    } catch (_error) {
      // Transient mount/IO failures (e.g. WSL ESTALE) must not abort the rest
      // of sync; treat like a missing archive and keep the prior cursor.
      return null;
    }
    const metadata = {
      size: data.length,
      mtimeMs: Number.isFinite(initialStat.mtimeMs) ? initialStat.mtimeMs : 0,
      inode: initialStat.ino || 0,
    };
    if (data.length === 0) return { ...metadata, unchanged: false, lines: [] };
    let decoded;
    try {
      const inspected = inspectDshZstdFrames(data, outputLimit);
      decoded = await decodeDshZstd(data, { maxOutputBytes: outputLimit, inspected });
    } catch (_error) {
      // Malformed / truncated / oversized archives must not abort the rest of
      // the sync: return null so the caller keeps the prior cursor and retries.
      return null;
    }
    // Fatal UTF-8: uncompressed JSONL parsing rejects invalid bytes under
    // invalidRecordPolicy=throw. toString("utf8") would silently replace them
    // and claim utf8Valid, so a corrupted discriminator could drop usage
    // during repairCodexRescanInflation. Fail the snapshot instead.
    let text;
    try {
      text = new TextDecoder("utf-8", { fatal: true }).decode(decoded);
    } catch (_error) {
      return null;
    }
    return { ...metadata, unchanged: false, lines: text.length > 0 ? text.split("\n") : [] };
  } finally {
    await handle.close().catch(() => {});
  }
}

function directoryInventoryStatKey(st) {
  return [
    Number(st.ino || 0),
    Number(st.size || 0),
    Number(st.mtimeMs || 0),
    Number(st.ctimeMs || 0),
  ].join(":");
}

// Collect rollout-*.jsonl at ANY depth under dir. listRolloutFiles requires the
// strict YYYY/MM/DD/ nesting Codex itself writes, but Codex-Manager archives
// sessions FLAT into ~/.codex/archived_sessions/ (issue #187), so the strict
// scanner misses them. This recursive variant handles both flat and nested
// layouts; safe because the codex event dedup keys on sessionUUID + timestamp,
// so an archived copy of an already-counted session re-reads as a no-op.
async function listRolloutFilesDeep(dir) {
  const out = [];
  async function walk(d) {
    const entries = await safeReadDir(d);
    for (const e of entries) {
      const p = path.join(d, e.name);
      if (e.isDirectory()) {
        await walk(p);
      } else if (e.isFile() && isRolloutFileName(e.name)) {
        out.push(p);
      }
    }
  }
  await walk(dir);
  out.sort((a, b) => a.localeCompare(b));
  return out;
}

async function parseRolloutIncremental({
  rolloutFiles,
  cursors,
  codexEventStore,
  queuePath,
  projectQueuePath,
  onProgress,
  source,
  publicRepoResolver,
  diagnostics,
  invalidRecordPolicy = "skip",
}) {
  if (invalidRecordPolicy !== "skip" && invalidRecordPolicy !== "throw") {
    throw new TypeError(`unsupported invalidRecordPolicy: ${invalidRecordPolicy}`);
  }
  await ensureDir(path.dirname(queuePath));
  let filesProcessed = 0;
  let eventsAggregated = 0;

  const cb = typeof onProgress === "function" ? onProgress : null;
  const totalFiles = Array.isArray(rolloutFiles) ? rolloutFiles.length : 0;
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const projectEnabled = typeof projectQueuePath === "string" && projectQueuePath.length > 0;
  const projectState = projectEnabled ? normalizeProjectState(cursors?.projectHourly) : null;
  const projectTouchedBuckets = projectEnabled ? new Set() : null;
  const projectMetaCache = projectEnabled ? new Map() : null;
  const publicRepoCache = projectEnabled ? new Map() : null;
  const projectFreshnessCache = projectEnabled ? new Map() : null;
  const projectFreshnessNowMs = Date.now();
  const touchedBuckets = new Set();
  const defaultSource = normalizeSourceInput(source) || DEFAULT_SOURCE;
  const syncDiagnostics = diagnostics && typeof diagnostics === "object" ? diagnostics : null;

  if (!cursors.files || typeof cursors.files !== "object") {
    cursors.files = {};
  }
  // Counted Codex compaction calls by response_id (issue #652). Core cursor
  // state, so a fork replay or a copied rollout under any path or day shard
  // sees every compaction counted before.
  const compactionResponseIds = createCompactionResponseIds(cursors);

  // Persisted set of seen Codex event keys (sessionUUID:eventTimestamp). Mirrors
  // the claudeHashes pattern: it makes an inode-changing re-scan idempotent so an
  // external rewrite of a session file (Codex-Manager's atomic provider-patch on
  // account switch, issue #187) cannot re-count already-counted events.
  const externalCodexEventStore =
    codexEventStore &&
    typeof codexEventStore.has === "function" &&
    typeof codexEventStore.add === "function"
      ? codexEventStore
      : null;
  const createRolloutEventDedup = ({ cursorKey, externalEventStore = null }) => {
    const previousHashes = Array.isArray(cursors?.[cursorKey]) ? cursors[cursorKey] : [];
    if (!externalEventStore && !Array.isArray(cursors[cursorKey])) {
      cursors[cursorKey] = previousHashes;
    }
    let seenEvents = null;
    let newEventKeys = null;
    let newEventKeySet = null;
    let appendOnlyEvents = null;
    let historicalEvents = null;

    const ensureNewEventKeys = () => {
      if (!newEventKeys) newEventKeys = [];
      if (!newEventKeySet) newEventKeySet = new Set();
    };
    const recordNewEvent = (key) => {
      ensureNewEventKeys();
      if (newEventKeySet.has(key)) return;
      newEventKeySet.add(key);
      newEventKeys.push(key);
      if (seenEvents) seenEvents.add(key);
      externalEventStore?.add(key);
    };
    const getAppendOnlyEvents = () => {
      if (!appendOnlyEvents) {
        appendOnlyEvents = {
          has(key) {
            return Boolean(newEventKeySet?.has(key) || seenEvents?.has(key));
          },
          add: recordNewEvent,
        };
      }
      return appendOnlyEvents;
    };
    const getHistoricalEvents = () => {
      if (externalEventStore) {
        if (!historicalEvents) {
          historicalEvents = {
            has(key) {
              return Boolean(newEventKeySet?.has(key) || externalEventStore.has(key));
            },
            add: recordNewEvent,
          };
        }
        return historicalEvents;
      }
      if (!seenEvents) {
        seenEvents = new Set(previousHashes);
        for (const key of newEventKeys || []) seenEvents.add(key);
        if (syncDiagnostics && cursorKey === "codexHashes") {
          syncDiagnostics.hash_set_constructions += 1;
        }
      }
      if (!historicalEvents) {
        historicalEvents = {
          has: (key) => seenEvents.has(key),
          add: recordNewEvent,
        };
      }
      return historicalEvents;
    };

    return {
      getAppendOnlyEvents,
      getHistoricalEvents,
      persist() {
        if (!externalEventStore) {
          for (const key of newEventKeys || []) previousHashes.push(key);
        }
      },
      size() {
        return externalEventStore
          ? Number(externalEventStore.size || 0)
          : Array.isArray(cursors[cursorKey])
            ? cursors[cursorKey].length
            : previousHashes.length;
      },
    };
  };
  const codexEventDedup = createRolloutEventDedup({
    cursorKey: "codexHashes",
    externalEventStore: externalCodexEventStore,
  });
  let acodeEventDedup = null;
  const getAcodeEventDedup = () => {
    if (!acodeEventDedup) {
      acodeEventDedup = createRolloutEventDedup({ cursorKey: "acodeHashes" });
    }
    return acodeEventDedup;
  };
  let cursorSessionPaths = null;
  if (syncDiagnostics) {
    const codexParseCandidates = (Array.isArray(rolloutFiles) ? rolloutFiles : []).reduce((count, entry) => {
      const entrySource = typeof entry === "string"
        ? defaultSource
        : normalizeSourceInput(entry?.source) || defaultSource;
      return count + (entrySource === DEFAULT_SOURCE ? 1 : 0);
    }, 0);
    const discoveredRollouts = Number(syncDiagnostics.discovered_rollouts);
    const cursorKeys = Number(syncDiagnostics.cursor_keys);
    const coldSkipped = Number(syncDiagnostics.cold_skipped);
    const parseCandidates = Number(syncDiagnostics.parse_candidates);
    Object.assign(syncDiagnostics, {
      discovered_rollouts: Number.isFinite(discoveredRollouts) ? discoveredRollouts : codexParseCandidates,
      cursor_keys: Number.isFinite(cursorKeys) ? cursorKeys : Object.keys(cursors.files).length,
      parse_candidates: Number.isFinite(parseCandidates) ? parseCandidates : codexParseCandidates,
      stat_candidates: 0,
      cold_skipped: Number.isFinite(coldSkipped) ? coldSkipped : 0,
      content_files_read: 0,
      hash_set_constructions: 0,
      hash_array_materializations: 0,
      hash_array_materialized_items: 0,
      codex_hash_count: codexEventDedup.size(),
    });
  }
  const getCursorSessionPaths = () => {
    if (cursorSessionPaths) return cursorSessionPaths;
    cursorSessionPaths = new Map();
    for (const existingPath of Object.keys(cursors.files)) {
      const sessionId = codexSessionIdFromPath(existingPath);
      if (!sessionId) continue;
      if (!cursorSessionPaths.has(sessionId)) cursorSessionPaths.set(sessionId, new Set());
      cursorSessionPaths.get(sessionId).add(existingPath);
    }
    return cursorSessionPaths;
  };
  const needsHistoricalRolloutDedup = ({
    filePath,
    prev,
    sameInode,
    truncated,
    startOffset,
    rebuildingBaseline,
  }) => {
    if (rebuildingBaseline) return true;
    if (prev && (!sameInode || truncated)) return true;

    // Same session under another cursor path — the two roots of a `union`
    // install (src/lib/install-resolver.js) reaching one file, or a
    // sessions/ -> archived_sessions/ move. This MUST be decided before the
    // steady-state short-circuit below: the append-only tracker only knows keys
    // written during the current run, so when the other path has nothing new to
    // read it contributes none, and a path whose cursor fell behind (a distro
    // that was briefly unreachable, a TOKEN_PULSE_WSL_MODE round-trip) replays
    // its gap into the persisted buckets. Only the persisted set can catch that.
    const sessionId = codexSessionIdFromPath(filePath);
    if (!sessionId) return true;
    const knownPaths = getCursorSessionPaths().get(sessionId);
    if (knownPaths) {
      for (const knownPath of knownPaths) {
        if (knownPath !== filePath) return true;
      }
    }

    // Steady state: one path, already read past, not rotated. Everything after
    // our own offset is genuinely new, so skip the persisted-set construction.
    if (sameInode && !truncated && startOffset > 0) return false;
    if (prev?.lastTotal) return true;
    return false;
  };

  // Metadata reads are independent while parsing and bucket mutation are not.
  // Prefetch stats with bounded concurrency, then retain the original stable
  // file order for parsing, deduplication, cursor updates, and queue writes.
  const rolloutStats = await mapConcurrent(
    rolloutFiles,
    FILE_METADATA_CONCURRENCY,
    async (entry) => {
      const filePath = typeof entry === "string" ? entry : entry?.path;
      if (!filePath) return null;
      return fs.stat(filePath).catch(() => null);
    },
  );
  for (let idx = 0; idx < rolloutFiles.length; idx++) {
    const entry = rolloutFiles[idx];
    const filePath = typeof entry === "string" ? entry : entry?.path;
    if (!filePath) continue;
    const fileSource =
      typeof entry === "string"
        ? defaultSource
        : normalizeSourceInput(entry?.source) || defaultSource;
    if (syncDiagnostics && fileSource === DEFAULT_SOURCE) syncDiagnostics.stat_candidates += 1;
    const st = rolloutStats[idx];
    if (!st || !st.isFile()) continue;

    const key = filePath;
    const prev = cursors.files[key] || null;
    const inode = st.ino || 0;
    const sameInode = prev && prev.inode === inode;
    const prevOffset = sameInode ? prev.offset || 0 : 0;
    const truncated = sameInode && prevOffset > st.size;
    const rebuildingCodexBaseline = Boolean(
      (fileSource === DEFAULT_SOURCE || fileSource === "acode") &&
      sameInode &&
      !truncated &&
      prevOffset > 0 &&
      prevOffset < st.size &&
      (!prev.lastTotal || typeof prev.lastTotal !== "object")
    );
    const startOffset = sameInode && !truncated && !rebuildingCodexBaseline ? prevOffset : 0;
    const lastTotal = sameInode && !truncated && !rebuildingCodexBaseline
      ? prev.lastTotal || null
      : null;
    const tokenUsageBaselines = sameInode && !truncated && !rebuildingCodexBaseline
      ? prev.tokenUsageBaselines || null
      : null;
    const lastModel = sameInode && !truncated ? prev.lastModel || null : null;
    const lastServiceTier = sameInode && !truncated ? prev.lastServiceTier || null : null;
    const modelAttributionState = sameInode && !truncated
      ? prev.modelAttributionState || null
      : null;
    // Only valid for the bytes before the resume offset; a rescan from 0
    // rebuilds it. A cursor from before #652 with a lastTotal has seen a
    // token_count already.
    const usageRecordState = startOffset > 0
      ? prev?.codexUsageRecord || (prev?.lastTotal ? { sawTokenCount: true } : null)
      : null;

    // Compressed archives are whole-file snapshots. If the compressed bytes
    // changed under the same inode, force historical dedup; parseRolloutFile
    // ignores retained baselines for the parse itself. Do NOT clear cursor
    // fields here — a soft decode failure must leave the prior cursor intact.
    const compressedSnapshotRescan = isCompressedRolloutPath(filePath)
      && sameInode
      && !truncated
      && prevOffset > 0
      && prevOffset !== st.size;

    const codexProjectFastPath =
      projectEnabled && (fileSource === DEFAULT_SOURCE || fileSource === "acode");
    const projectOffset = sameInode && !truncated ? Number(prev.projectOffset || 0) : 0;
    const projectUpToDate =
      codexProjectFastPath &&
      typeof publicRepoResolver !== "function" &&
      projectOffset >= st.size &&
      (await isProjectFileContextFresh(prev?.projectFileContext, {
        freshnessCache: projectFreshnessCache,
        nowMs: projectFreshnessNowMs,
      }));
    const projectContextOnlyScan =
      codexProjectFastPath &&
      typeof publicRepoResolver !== "function" &&
      sameInode &&
      !truncated &&
      startOffset >= st.size &&
      !projectUpToDate;
    if (
      sameInode &&
      !truncated &&
      startOffset >= st.size &&
      (!projectEnabled || projectUpToDate)
    ) {
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
      continue;
    }

    const projectContext = projectEnabled
      ? await resolveProjectContextForFile({
          filePath,
          projectMetaCache,
          publicRepoCache,
          publicRepoResolver,
          projectState,
        })
      : null;
    const projectRef = projectContext?.projectRef || null;
    const projectKey = projectContext?.projectKey || null;

    if (syncDiagnostics && !projectContextOnlyScan && fileSource === DEFAULT_SOURCE) {
      syncDiagnostics.content_files_read += 1;
    }
    const rolloutEventDedup = fileSource === DEFAULT_SOURCE
      ? codexEventDedup
      : fileSource === "acode"
        ? getAcodeEventDedup()
        : null;
    const codexEventTracker = rolloutEventDedup
      ? (compressedSnapshotRescan
          || needsHistoricalRolloutDedup({
          filePath,
          prev,
          sameInode,
          truncated,
          startOffset,
          rebuildingBaseline: rebuildingCodexBaseline,
        })
          ? rolloutEventDedup.getHistoricalEvents
          : rolloutEventDedup.getAppendOnlyEvents)
      : null;
    const result = projectContextOnlyScan
      ? await scanRolloutProjectFileContexts({
          filePath,
          fileStat: st,
          lastTotal,
          tokenUsageBaselines,
          lastModel,
          lastServiceTier,
          modelAttributionState,
          projectState,
          projectMetaCache,
          publicRepoCache,
          publicRepoResolver,
          projectContext,
          invalidRecordPolicy,
        })
      : await parseRolloutFile({
          filePath,
          fileStat: st,
          startOffset,
          lastTotal,
          tokenUsageBaselines,
          lastModel,
          lastServiceTier,
          modelAttributionState,
          usageRecordState,
          compactionResponseIds,
          hourlyState,
          touchedBuckets,
          source: fileSource,
          projectState,
          projectTouchedBuckets,
          projectRef,
          projectKey,
          projectMetaCache,
          publicRepoCache,
          publicRepoResolver,
          projectContext,
          seenCodexEvents: codexEventTracker,
          sessionId: codexSessionIdFromPath(filePath),
          invalidRecordPolicy,
        });

    if (result.snapshotUnavailable) {
      // Leave cursors.files[key] untouched so the next readable snapshot is
      // compared against the prior (inode, offset, baselines).
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
      continue;
    }

    const nextCursor = {
      inode,
      offset: result.endOffset,
      lastTotal: result.lastTotal,
      tokenUsageBaselines: result.tokenUsageBaselines,
      lastModel: result.lastModel,
      lastServiceTier: result.lastServiceTier || null,
      modelAttributionState: result.modelAttributionState,
      updatedAt: new Date().toISOString(),
    };
    const nextUsageRecordState = projectContextOnlyScan
      ? usageRecordState
      : result.usageRecordState;
    if (nextUsageRecordState) nextCursor.codexUsageRecord = nextUsageRecordState;
    // Files whose usage only exists as token_usage_record rows are not
    // counted (issue #652); keep them in core cursor state so sync and status
    // can warn without loading per-file cursor shards.
    if (fileSource === DEFAULT_SOURCE) {
      if (isRecordOnly(nextUsageRecordState)) {
        (cursors.codexUsageRecordOnlyFiles ||= {})[key] = true;
      } else if (cursors.codexUsageRecordOnlyFiles?.[key]) {
        delete cursors.codexUsageRecordOnlyFiles[key];
      }
    }
    if (codexProjectFastPath) {
      nextCursor.projectOffset = result.endOffset;
      nextCursor.projectFileContext = buildProjectFileContext(
        result.projectFileContexts,
        projectFreshnessNowMs,
      );
    } else if (Number.isFinite(prev?.projectOffset)) {
      nextCursor.projectOffset = prev.projectOffset;
      if (prev?.projectFileContext) nextCursor.projectFileContext = prev.projectFileContext;
    }
    cursors.files[key] = nextCursor;

    filesProcessed += 1;
    eventsAggregated += result.eventsAggregated;

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
  codexEventDedup.persist();
  acodeEventDedup?.persist();
  if (syncDiagnostics) {
    syncDiagnostics.codex_hash_count = codexEventDedup.size();
  }
  hourlyState.updatedAt = new Date().toISOString();
  cursors.hourly = hourlyState;
  if (projectState) {
    projectState.updatedAt = new Date().toISOString();
    cursors.projectHourly = projectState;
  }
  compactionResponseIds.persist();
  // A deleted rollout no longer needs a warning. The map is empty unless a
  // record-only writer exists, so this is normally free.
  for (const flaggedPath of Object.keys(cursors.codexUsageRecordOnlyFiles || {})) {
    if (!fssync.existsSync(flaggedPath)) delete cursors.codexUsageRecordOnlyFiles[flaggedPath];
  }

  return { filesProcessed, eventsAggregated, bucketsQueued, projectBucketsQueued };
}

async function filterColdCodexRolloutFiles({
  rolloutFiles,
  cursors,
  codexCursorStore = null,
  projectEnabled = false,
  auditDue = false,
  nowMs = Date.now(),
  recentDays = DEFAULT_CODEX_COLD_SKIP_RECENT_DAYS,
  growthStatDays = DEFAULT_CODEX_COLD_GROWTH_STAT_DAYS,
  diagnostics = null,
} = {}) {
  const files = Array.isArray(rolloutFiles) ? rolloutFiles : [];
  const syncDiagnostics = diagnostics && typeof diagnostics === "object" ? diagnostics : null;
  const isCodexEntry = (entry) => (
    typeof entry === "string" || (normalizeSourceInput(entry?.source) || DEFAULT_SOURCE) === DEFAULT_SOURCE
  );
  if (syncDiagnostics) {
    const discoveredRollouts = files.reduce(
      (count, entry) => count + (isCodexEntry(entry) ? 1 : 0),
      0,
    );
    syncDiagnostics.discovered_rollouts = discoveredRollouts;
    syncDiagnostics.cursor_keys = Number.isFinite(codexCursorStore?.fileCount)
      ? codexCursorStore.fileCount
      : Object.keys(cursors?.files || {}).length;
    syncDiagnostics.cold_skipped = 0;
    syncDiagnostics.parse_candidates = discoveredRollouts;
  }
  if (
    !codexCursorStore &&
    (auditDue || !cursors?.files || typeof cursors.files !== "object")
  ) {
    return { rolloutFiles: files, skipped: 0 };
  }

  const activeDates = activeCodexRolloutDates(files, { nowMs, recentDays });
  const freshnessCache = new Map();
  const cursorLoadDirectories = new Set();
  const coldDaySkipDecisions = new Map();
  const out = [];
  let skipped = 0;
  let cursorStoreRestarted = false;

  const loadCodexCursorDirectory = async (filePath) => {
    if (!codexCursorStore) return;
    const directory = path.dirname(filePath);
    if (cursorLoadDirectories.has(directory)) return;
    cursorLoadDirectories.add(directory);
    const result = await codexCursorStore.loadCodexFilesForPaths([filePath], cursors);
    if (result?.restarted) cursorStoreRestarted = true;
  };

  const canSkipCodexDirectory = async (filePath) => {
    if (!codexCursorStore) return false;
    const directory = path.dirname(filePath);
    if (coldDaySkipDecisions.has(directory)) {
      return coldDaySkipDecisions.get(directory);
    }
    const decision = await codexCursorStore.canSkipCodexDay({
      filePath,
      dayInventoryCache: cursors?.codexDayInventoryCache,
      nowMs,
    });
    coldDaySkipDecisions.set(directory, decision);
    return decision;
  };

  for (const entry of files) {
    const filePath = typeof entry === "string" ? entry : entry?.path;
    const source =
      typeof entry === "string"
        ? DEFAULT_SOURCE
        : normalizeSourceInput(entry?.source) || DEFAULT_SOURCE;
    if (!filePath || source !== DEFAULT_SOURCE) {
      out.push(entry);
      continue;
    }

    const rolloutDate = rolloutDateFromPath(filePath);
    if (!rolloutDate || activeDates.has(rolloutDate)) {
      await loadCodexCursorDirectory(filePath);
      if (cursorStoreRestarted) break;
      out.push(entry);
      continue;
    }

    if (auditDue) {
      await loadCodexCursorDirectory(filePath);
      if (cursorStoreRestarted) break;
      out.push(entry);
      continue;
    }

    // Neither check below notices an append: the day-level skip keys off the
    // directory stat, which only changes when a file is added or removed, and
    // the cursor offset records how far we read, not how big the file is. A
    // still-open session whose name-date fell out of the active window was
    // therefore skipped without ever being looked at, and its later days only
    // surfaced at the daily audit (#592). Stat recent cold files and keep any
    // that grew past the cursor; the parse resumes from the offset.
    if (isRecentColdRollout(rolloutDate, { nowMs, growthStatDays })) {
      await loadCodexCursorDirectory(filePath);
      if (cursorStoreRestarted) break;
      const readOffset = Number(cursors.files[filePath]?.offset);
      if (Number.isFinite(readOffset) && readOffset > 0) {
        const stat = await fs.stat(filePath).catch(() => null);
        if (syncDiagnostics) {
          syncDiagnostics.cold_growth_stats = Number(syncDiagnostics.cold_growth_stats || 0) + 1;
        }
        if (stat && stat.size > readOffset) {
          out.push(entry);
          continue;
        }
      }
    }

    if (await canSkipCodexDirectory(filePath)) {
      skipped += 1;
      continue;
    }

    await loadCodexCursorDirectory(filePath);
    if (cursorStoreRestarted) break;

    const prev = cursors.files[filePath];
    const cachedSize = Number(prev?.offset);
    if (!Number.isFinite(cachedSize) || cachedSize <= 0) {
      out.push(entry);
      continue;
    }

    if (projectEnabled) {
      const projectOffset = Number(prev?.projectOffset);
      if (!Number.isFinite(projectOffset) || projectOffset < cachedSize) {
        out.push(entry);
        continue;
      }
      if (
        !(await isProjectFileContextFresh(prev?.projectFileContext, {
          freshnessCache,
          nowMs,
        }))
      ) {
        out.push(entry);
        continue;
      }
    }

    skipped += 1;
  }

  if (cursorStoreRestarted) {
    await codexCursorStore.loadCodexFilesForPaths(files, cursors);
    if (syncDiagnostics) {
      syncDiagnostics.cold_skipped = 0;
      syncDiagnostics.parse_candidates = files.reduce(
        (count, entry) => count + (isCodexEntry(entry) ? 1 : 0),
        0,
      );
    }
    return { rolloutFiles: files, skipped: 0, restarted: true };
  }

  if (syncDiagnostics) {
    syncDiagnostics.cold_skipped = skipped;
    syncDiagnostics.parse_candidates = out.reduce(
      (count, entry) => count + (isCodexEntry(entry) ? 1 : 0),
      0,
    );
  }
  return { rolloutFiles: out, skipped, restarted: false };
}

// True when the rollout's name-date (local calendar day) is within
// `growthStatDays` of now. The date in the name is the session's creation day.
function isRecentColdRollout(rolloutDate, { nowMs = Date.now(), growthStatDays } = {}) {
  const days = Number(growthStatDays);
  if (!Number.isFinite(days) || days <= 0) return false;
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(rolloutDate || ""));
  if (!match) return false;
  const created = new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3])).getTime();
  if (!Number.isFinite(created)) return false;
  return nowMs - created <= days * 24 * 60 * 60 * 1000;
}

function activeCodexRolloutDates(
  rolloutFiles,
  { nowMs = Date.now(), recentDays = DEFAULT_CODEX_COLD_SKIP_RECENT_DAYS } = {},
) {
  const active = new Set();
  const days = Math.max(1, Math.floor(Number(recentDays) || 1));
  const now = new Date(nowMs);
  if (Number.isFinite(now.getTime())) {
    for (let i = 0; i < days; i += 1) {
      const d = new Date(now.getFullYear(), now.getMonth(), now.getDate() - i);
      active.add(formatLocalDate(d));
    }
  }

  let latest = null;
  for (const entry of Array.isArray(rolloutFiles) ? rolloutFiles : []) {
    const filePath = typeof entry === "string" ? entry : entry?.path;
    const source =
      typeof entry === "string"
        ? DEFAULT_SOURCE
        : normalizeSourceInput(entry?.source) || DEFAULT_SOURCE;
    if (source !== DEFAULT_SOURCE) continue;
    const rolloutDate = rolloutDateFromPath(filePath);
    if (rolloutDate && (!latest || rolloutDate > latest)) latest = rolloutDate;
  }
  if (latest) active.add(latest);

  return active;
}

function formatLocalDate(value) {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

/**
 * Extract the session UUID from a Codex rollout file path
 * (`rollout-<datetime>-<uuid>.jsonl`). Used as the stable per-session scope for
 * event dedup: it survives both an inode-changing rewrite (Codex-Manager
 * atomically rewrites session files to patch the provider on account switch,
 * issue #187) and a sessions/ -> archived_sessions/ move. Returns null when the
 * name has no UUID, in which case the caller falls back to the full path.
 */
function codexSessionIdFromPath(filePath) {
  if (typeof filePath !== "string") return null;
  const m = filePath.match(
    /([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\.jsonl(?:\.zstd?)?$/,
  );
  return m ? m[1] : null;
}

async function parseRolloutFile({
  filePath,
  fileStat,
  startOffset,
  lastTotal,
  tokenUsageBaselines,
  lastModel,
  lastServiceTier,
  modelAttributionState: previousModelAttributionState,
  usageRecordState: previousUsageRecordState = null,
  compactionResponseIds = null,
  hourlyState,
  touchedBuckets,
  source,
  projectState,
  projectTouchedBuckets,
  projectRef,
  projectKey,
  projectMetaCache,
  publicRepoCache,
  publicRepoResolver,
  projectContext,
  seenCodexEvents,
  sessionId,
  invalidRecordPolicy,
}) {
  const st = fileStat || (await fs.stat(filePath));
  const endOffset = st.size;
  const projectFileContexts = [];
  addProjectFileContext(projectFileContexts, projectContext);
  // Compressed archives are immutable snapshots, not append-only logs: a byte
  // range of the compressed file is not a prefix of the decompressed text, so
  // resume-in-the-middle is impossible. Parse the whole snapshot whenever its
  // (inode, size) identity is new; unchanged snapshots are skipped by the
  // caller before reaching this function.
  if (isCompressedRolloutPath(filePath)) {
    // Mirror the uncompressed exhausted-cursor guard. When a custom
    // publicRepoResolver disables the outer skip, an unchanged archive must
    // not be re-parsed with retained baselines (that inflates totals).
    if (startOffset >= endOffset) {
      return {
        endOffset,
        lastTotal,
        tokenUsageBaselines,
        lastModel,
        lastServiceTier: typeof lastServiceTier === "string" ? lastServiceTier : null,
        modelAttributionState: previousModelAttributionState,
        usageRecordState: previousUsageRecordState,
        eventsAggregated: 0,
        projectFileContexts,
      };
    }
    const snapshot = await readCodexRolloutSnapshot(filePath);
    if (!snapshot) {
      // Soft sync skips a bad archive and keeps the prior cursor so later
      // files still count. Strict rebuilds (invalidRecordPolicy=throw), used
      // by repairCodexRescanInflation, must fail closed: a silent skip here
      // would let another readable session satisfy the non-empty-bucket
      // safeguard and permanently delete this archive's prior usage.
      if (invalidRecordPolicy === "throw") {
        throw new Error(`unreadable Codex compressed archive: ${filePath}`);
      }
      return {
        endOffset: startOffset,
        lastTotal,
        tokenUsageBaselines,
        lastModel,
        lastServiceTier: typeof lastServiceTier === "string" ? lastServiceTier : null,
        modelAttributionState: previousModelAttributionState,
        usageRecordState: previousUsageRecordState,
        eventsAggregated: 0,
        projectFileContexts,
        // Tell the incremental caller not to rewrite cursors.files[key]. If the
        // archive inode changed under a transient decode miss, overwriting the
        // prior cursor would drop historical-dedup baselines and double-count.
        snapshotUnavailable: true,
      };
    }
    // Decompressed lines have no meaningful compressed-byte offsets; the
    // shared parser only needs their content plus termination flags. Byte
    // counters stay at the snapshot's compressed size so the cursor records
    // (inode, size) identity, not decompressed progress.
    const records = (async function* snapshotRecords() {
      for (let index = 0; index < snapshot.lines.length; index += 1) {
        const raw = snapshot.lines[index];
        const terminated = index + 1 < snapshot.lines.length || raw.endsWith("\n");
        yield {
          line: terminated ? raw : `${raw}`,
          physicalBytes: 0,
          terminated,
          utf8Valid: true,
        };
      }
    })();
    // Size changed under the same inode: drop retained baselines for THIS
    // parse only. Soft-fail above still returns the caller-supplied baselines
    // so the prior cursor is preserved when decode fails.
    const rescanBaselines = startOffset > 0 && startOffset < endOffset;
    const parsed = await parseRolloutRecords({
      records,
      startOffset: 0,
      endOffset: 0,
      lastTotal: rescanBaselines ? null : lastTotal,
      tokenUsageBaselines: rescanBaselines ? null : tokenUsageBaselines,
      lastModel: rescanBaselines ? null : lastModel,
      lastServiceTier: rescanBaselines ? null : lastServiceTier,
      modelAttributionState: rescanBaselines ? null : previousModelAttributionState,
      usageRecordState: rescanBaselines ? null : previousUsageRecordState,
      compactionResponseIds,
      hourlyState,
      touchedBuckets,
      source,
      projectState,
      projectTouchedBuckets,
      projectRef,
      projectKey,
      projectMetaCache,
      publicRepoCache,
      publicRepoResolver,
      projectContext,
      projectFileContexts,
      seenCodexEvents,
      sessionId,
      filePath,
      invalidRecordPolicy,
    });
    // parseRolloutRecords counts streamed bytes; snapshots have none, so pin
    // the cursor to the compressed size — the (inode, size) identity that
    // makes an unchanged archive skip on the next sync.
    parsed.endOffset = snapshot.size;
    return parsed;
  }
  if (startOffset >= endOffset) {
    return {
      endOffset,
      lastTotal,
      tokenUsageBaselines,
      lastModel,
      lastServiceTier: typeof lastServiceTier === "string" ? lastServiceTier : null,
      modelAttributionState: previousModelAttributionState,
      usageRecordState: previousUsageRecordState,
      eventsAggregated: 0,
      projectFileContexts,
    };
  }

  const stream = fssync.createReadStream(filePath, {
    start: startOffset,
    end: endOffset - 1,
  });
  // Compressed snapshots never reach this branch (parseRolloutFile routes them
  // to parseRolloutRecords directly); the byte-counting stream below is only
  // meaningful for append-only uncompressed logs.
  const records = (async function* streamRecords() {
    const invalidUtf8 = invalidRecordPolicy === "throw" ? "throw" : "record";
    for await (const record of physicalJsonlRecords(stream, { invalidUtf8 })) {
      yield record;
    }
  })();

  return parseRolloutRecords({
    records,
    startOffset,
    endOffset,
    lastTotal,
    tokenUsageBaselines,
    lastModel,
    lastServiceTier,
    modelAttributionState: previousModelAttributionState,
    usageRecordState: previousUsageRecordState,
    compactionResponseIds,
    hourlyState,
    touchedBuckets,
    source,
    projectState,
    projectTouchedBuckets,
    projectRef,
    projectKey,
    projectMetaCache,
    publicRepoCache,
    publicRepoResolver,
    projectContext,
    projectFileContexts,
    seenCodexEvents,
    sessionId,
    filePath,
    invalidRecordPolicy,
  });
}

async function parseRolloutRecords({
  records,
  startOffset,
  endOffset,
  lastTotal,
  tokenUsageBaselines,
  lastModel,
  lastServiceTier,
  modelAttributionState: previousModelAttributionState,
  usageRecordState: previousUsageRecordState = null,
  compactionResponseIds = null,
  hourlyState,
  touchedBuckets,
  source,
  projectState,
  projectTouchedBuckets,
  projectRef,
  projectKey,
  projectMetaCache,
  publicRepoCache,
  publicRepoResolver,
  projectContext,
  projectFileContexts: existingProjectFileContexts,
  seenCodexEvents,
  sessionId,
  filePath,
  invalidRecordPolicy,
}) {
  const projectFileContexts = Array.isArray(existingProjectFileContexts)
    ? existingProjectFileContexts
    : [];
  addProjectFileContext(projectFileContexts, projectContext);
  let model = typeof lastModel === "string" ? lastModel : null;
  // Codex writes thread_settings_applied before the turn_context of the turn it
  // takes effect on, and token_count rows carry no turn id, so a row's tier is
  // whatever the last such record said. A session's first turn has none, and
  // most CLI sessions have none at all — that stays null, and null is billed at
  // Standard rather than guessed from the current config.
  let serviceTier = typeof lastServiceTier === "string" ? lastServiceTier : null;
  const modelAttributionState = createCodexModelAttributionState(
    previousModelAttributionState || { model },
  );
  const usageDeltaState = createUsageDeltaState({
    lastTotal,
    baselines: tokenUsageBaselines,
  });
  let latestTotal = lastTotal && typeof lastTotal === "object" ? lastTotal : null;
  let currentCwd = null;
  let currentDate = null;
  let isForkedRollout = false;
  // The replay prefix only ever exists at the head of a freshly-forked file. A
  // resumed scan (startOffset > 0) can never see it — and never re-reads the
  // session_meta line, so isForkedRollout stays false there anyway — but gate
  // on the offset explicitly so correctness does not hinge on that distant
  // invariant. Known accepted limitation: if a sync's read races the fork's
  // single-flush write and persists a cursor INSIDE the burst, the resumed
  // scan counts the remaining burst tail (bounded over-count, ms-wide window;
  // self-heals on any inode-changing rescan and never drops genuine usage).
  let replayPrefixActive = startOffset === 0;
  let prevForkedTokenMs = null;
  const rolloutDate = rolloutDateFromPath(filePath);
  let currentProjectRef = projectRef || null;
  let currentProjectKey = projectKey || null;
  let eventsAggregated = 0;
  let scannedEndOffset = startOffset;
  let committedEndOffset = startOffset;
  // Compaction records (issue #652, rules in codex-usage-record.js). Codex only.
  const usageRecordState = source === DEFAULT_SOURCE && compactionResponseIds
    ? createUsageRecordState(previousUsageRecordState)
    : null;

  // What a record adds if it turns out to be a compaction call: model, tier
  // and project are those in effect when the record was written. A record
  // without a response_id cannot be deduplicated and is never counted.
  function buildCompactionEvent(record) {
    if (!record.responseId) return null;
    const delta = normalizeUsage(record.usage);
    if (isAllZeroUsage(delta)) return null;
    delta.conversation_count = 1;
    const bucketStart = toUtcHalfHourStart(record.timestamp);
    if (!bucketStart) return null;
    return {
      responseId: record.responseId,
      bucketStart,
      model,
      serviceTier,
      projectKey: currentProjectKey,
      projectRef: currentProjectRef,
      delta,
    };
  }

  // Counted once per response_id across every rollout: a fork replays its
  // parent's records and a copied rollout repeats them, both with the same id.
  function addCompactionEvent(event) {
    if (compactionResponseIds.has(event.responseId)) return;
    compactionResponseIds.add(event.responseId);
    const bucket = getHourlyBucket(hourlyState, source, event.model, event.bucketStart);
    addTotals(bucket.totals, event.delta);
    if (isPriorityServiceTier(event.serviceTier)) addPriorityUsage(bucket.totals, event.delta);
    touchedBuckets.add(bucketKey(source, event.model, event.bucketStart));
    if (event.projectKey && projectState && projectTouchedBuckets) {
      const projectBucket = getProjectBucket(
        projectState,
        event.projectKey,
        source,
        event.bucketStart,
        event.projectRef,
      );
      addTotals(projectBucket.totals, event.delta);
      projectTouchedBuckets.add(projectBucketKey(event.projectKey, source, event.bucketStart));
    }
    eventsAggregated += 1;
  }

  for await (const record of records) {
    scannedEndOffset += record.physicalBytes;
    if (record.terminated) committedEndOffset = scannedEndOffset;
    if (!record.utf8Valid) {
      if (!record.terminated) break;
      continue;
    }

    const { line } = record;
    if (!line) continue;
    // Only a `compacted` line directly after a record keeps it as a
    // compaction candidate. A line that is not complete yet decides nothing:
    // the read stops before it and the next sync sees it whole.
    if (awaitsCompactedLine(usageRecordState)) {
      let next;
      if (!record.terminated || line.includes('"compacted"')) {
        try {
          next = JSON.parse(line);
        } catch {
          next = undefined;
        }
      }
      if (next !== undefined || record.terminated) {
        noteLineAfterUsageRecord(usageRecordState, next?.type === "compacted");
      }
      if (next?.type === "compacted") {
        committedEndOffset = scannedEndOffset;
        continue;
      }
    }
    const maybeTokenCount = line.includes('"token_count"');
    const maybeModelReroute =
      !maybeTokenCount &&
      (line.includes('"model/rerouted"') || line.includes('"model_rerouted"'));
    const maybeTurnContext =
      !maybeTokenCount && !maybeModelReroute &&
      (line.includes('"turn_context"') || line.includes('"session_meta"')) &&
      (line.includes('"model"') ||
        line.includes('"cwd"') ||
        line.includes('"current_date"') ||
        line.includes('"forked_from_id"'));
    const maybeServiceTier =
      !maybeTokenCount && !maybeModelReroute && !maybeTurnContext &&
      line.includes(CODEX_SERVICE_TIER_MARKER);
    const maybeUsageRecord =
      !maybeTokenCount &&
      Boolean(usageRecordState) &&
      (line.includes('"token_usage_record"') ||
        (usageRecordState.unmatchedRecord &&
          (line.includes('"task_complete"') || line.includes('"turn_aborted"'))));
    if (
      !maybeTokenCount &&
      !maybeTurnContext &&
      !maybeModelReroute &&
      !maybeServiceTier &&
      !maybeUsageRecord
    ) {
      if (invalidRecordPolicy === "throw" || !record.terminated) {
        try {
          JSON.parse(line);
          committedEndOffset = scannedEndOffset;
        } catch (error) {
          if (invalidRecordPolicy === "throw") throw error;
          break;
        }
      }
      continue;
    }

    let obj;
    try {
      obj = JSON.parse(line);
    } catch (error) {
      if (invalidRecordPolicy === "throw") throw error;
      if (!record.terminated) break;
      continue;
    }
    if (!record.terminated) committedEndOffset = scannedEndOffset;

    applyCodexModelEvent(modelAttributionState, obj);
    model = currentCodexModel(modelAttributionState) || model;

    const appliedServiceTier = readCodexServiceTier(obj);
    if (appliedServiceTier) {
      serviceTier = appliedServiceTier;
      continue;
    }

    if (
      (obj?.type === "turn_context" || obj?.type === "session_meta") &&
      obj?.payload &&
      typeof obj.payload === "object"
    ) {
      if (obj.type === "session_meta" && typeof obj.payload.forked_from_id === "string") {
        isForkedRollout = obj.payload.forked_from_id.trim().length > 0;
      }
      if (obj.type === "turn_context" && typeof obj.payload.current_date === "string") {
        currentDate = normalizeIsoDate(obj.payload.current_date);
      }
      if (projectState && typeof obj.payload.cwd === "string") {
        const nextCwd = obj.payload.cwd.trim();
        if (nextCwd && nextCwd !== currentCwd) {
          const context = await resolveProjectContextForPath({
            startDir: wsl.mapWslCwdToUnc(nextCwd, filePath),
            projectMetaCache,
            publicRepoCache,
            publicRepoResolver,
            projectState,
          });
          currentCwd = nextCwd;
          currentProjectRef = context?.projectRef || null;
          currentProjectKey = context?.projectKey || null;
          addProjectFileContext(projectFileContexts, context);
        }
      }
      continue;
    }

    if (usageRecordState) {
      const usageRecord = extractTokenUsageRecord(obj);
      if (usageRecord) {
        noteUsageRecord(usageRecordState, buildCompactionEvent(usageRecord));
        continue;
      }
      if (isCodexTurnEndEvent(obj)) {
        noteTurnEnd(usageRecordState);
        continue;
      }
    }

    const token = extractTokenCount(obj);
    if (!token) continue;

    const info = token.info;
    if (!info || typeof info !== "object") continue;
    if (usageRecordState) {
      noteTokenCount(usageRecordState);
      const compaction = takeCompactionOnTokenCount(
        usageRecordState,
        usageDeltaState.lastTotal,
        info.total_token_usage,
      );
      if (compaction) addCompactionEvent(compaction);
    }

    const tokenTimestamp = typeof token.timestamp === "string" ? token.timestamp : null;
    if (!tokenTimestamp) continue;

    const lastUsage = info.last_token_usage;
    const totalUsage = info.total_token_usage;
    if (totalUsage && typeof totalUsage === "object") latestTotal = totalUsage;

    const rawDelta = consumeUsageDelta(usageDeltaState, lastUsage, totalUsage);
    const totalOnlyResetSentinel = source === DEFAULT_SOURCE
      && isCodexTotalOnlyResetSentinel(lastUsage, totalUsage);
    const delta = rawDelta && !totalOnlyResetSentinel ? normalizeUsage(rawDelta) : null;
    if (!delta || isAllZeroUsage(delta)) continue;
    delta.conversation_count = 1;

    // Forked Codex and Acode rollouts replay the parent session's entire token history
    // into the child file the moment the fork is created. The date guard below
    // (current_date < rolloutDate) catches cross-day forks, but same-day forks
    // share the parent's current_date and slip through it. The replay is written
    // in a single flush, so its rows carry near-identical timestamps (sub-ms to a
    // few ms apart across observed 0.129–0.137 samples), while the first genuine
    // live turn lands seconds later (≥11s in every sampled fork). We therefore
    // skip the *leading* run of densely-spaced token_count rows and latch off
    // permanently at the first multi-second gap: a lone fast turn (always
    // preceded by a large gap) is never dropped, and an arbitrarily large replay
    // is fully skipped regardless of length. The threshold sits far above the
    // flush spacing and well below genuine turn cadence (≥~4.6s observed). The
    // first replayed row cannot be identified without lookahead, so it is still
    // counted (a bounded, <1% residual over-count); dropping real usage is the
    // worse failure, so we bias against it. `usageDeltaState` is already advanced above,
    // keeping the cumulative lineage correct for the live turns we keep.
    // Scoped to forked Codex/Acode rollouts. (issue #169 follow-up.)
    let forkedReplaySkip = false;
    if (
      isForkedRollout &&
      (source === DEFAULT_SOURCE || source === "acode") &&
      replayPrefixActive
    ) {
      const tokenMs = Date.parse(tokenTimestamp);
      // Fail open on anything the burst heuristic was not measured against:
      // an unparseable timestamp or a backwards clock step permanently ends
      // the prefix, so no later row can be skipped off a stale baseline.
      // Replay flushes are monotonic in every sampled rollout (2,116 gaps,
      // min 0ms), so this never fires on genuine replays.
      if (!Number.isFinite(tokenMs) || (prevForkedTokenMs !== null && tokenMs < prevForkedTokenMs)) {
        replayPrefixActive = false;
      } else {
        if (prevForkedTokenMs !== null && tokenMs - prevForkedTokenMs >= FORK_REPLAY_GAP_MS) {
          replayPrefixActive = false;
        }
        forkedReplaySkip =
          replayPrefixActive &&
          prevForkedTokenMs !== null &&
          tokenMs - prevForkedTokenMs < FORK_REPLAY_GAP_MS;
        prevForkedTokenMs = tokenMs;
      }
    }

    // date matching is conservative; same-day fork replays are caught by the
    // burst detector above rather than this cross-day date guard.
    if (forkedReplaySkip || isForkedReplayToken({ isForkedRollout, rolloutDate, currentDate })) {
      continue;
    }

    const bucketStart = toUtcHalfHourStart(tokenTimestamp);
    if (!bucketStart) continue;

    // Idempotent re-scan dedup (issue #187). Codex usage is parsed incrementally
    // by (inode, offset): when the inode changes the file is re-scanned from
    // offset 0 and every event's delta is re-added to the PERSISTENT hourly
    // buckets. External tools rewrite session files without changing the token
    // data — Codex-Manager atomically rewrites them (new inode) to patch the
    // provider on every account/channel switch — so without dedup each switch
    // double-counts the rewritten sessions. `usageDeltaState` is already advanced above,
    // so skipping an already-seen event keeps the cumulative lineage intact
    // while preventing the re-add; genuinely new turns carry new timestamps and
    // are still counted. Key = sessionUUID:eventTimestamp (both stable across the
    // rewrite and across a sessions/ -> archived_sessions/ move).
    //
    // Apply event deduplication only to Codex and Acode. Every Code and other sources
    // reread events to realign models.
    const codexEvents = typeof seenCodexEvents === "function"
      ? seenCodexEvents()
      : seenCodexEvents;
    if (codexEvents && (source === "codex" || source === "acode")) {
      const dedupKey = `${sessionId || filePath}:${tokenTimestamp}`;
      if (codexEvents.has(dedupKey)) continue;
      codexEvents.add(dedupKey);
    }

    const bucket = getHourlyBucket(hourlyState, source, model, bucketStart);
    addTotals(bucket.totals, delta);
    // Only the model bucket gets the subset: the project bucket below carries
    // no model, so no per-model rate can be applied to it anyway.
    if (isPriorityServiceTier(serviceTier)) addPriorityUsage(bucket.totals, delta);
    touchedBuckets.add(bucketKey(source, model, bucketStart));
    if (currentProjectKey && projectState && projectTouchedBuckets) {
      const projectBucket = getProjectBucket(
        projectState,
        currentProjectKey,
        source,
        bucketStart,
        currentProjectRef,
      );
      addTotals(projectBucket.totals, delta);
      projectTouchedBuckets.add(projectBucketKey(currentProjectKey, source, bucketStart));
    }
    eventsAggregated += 1;
  }

  return {
    endOffset: committedEndOffset,
    lastTotal: latestTotal,
    tokenUsageBaselines: snapshotUsageBaselines(usageDeltaState),
    lastModel: model,
    lastServiceTier: serviceTier,
    modelAttributionState: snapshotCodexModelAttributionState(modelAttributionState),
    usageRecordState: usageRecordState
      ? snapshotUsageRecordState(usageRecordState)
      : previousUsageRecordState,
    eventsAggregated,
    projectFileContexts,
  };
}

async function scanRolloutProjectFileContexts({
  filePath,
  fileStat,
  lastTotal,
  tokenUsageBaselines,
  lastModel,
  lastServiceTier,
  modelAttributionState,
  projectState,
  projectMetaCache,
  publicRepoCache,
  publicRepoResolver,
  projectContext,
  invalidRecordPolicy,
}) {
  const st = fileStat || (await fs.stat(filePath));
  const endOffset = st.size;
  const projectFileContexts = [];
  addProjectFileContext(projectFileContexts, projectContext);
  if (!projectState || endOffset <= 0) {
    return {
      endOffset,
      lastTotal,
      tokenUsageBaselines,
      lastModel,
      lastServiceTier: lastServiceTier || null,
      modelAttributionState,
      eventsAggregated: 0,
      projectFileContexts,
    };
  }

  // Compressed archives cannot be scanned as byte streams: JSONL newlines in
  // the compressed payload are not record boundaries. Decode the snapshot,
  // then walk decompressed lines for cwd changes. Pin the cursor to the
  // compressed size so the next sync's unchanged check keeps working.
  if (isCompressedRolloutPath(filePath)) {
    const snapshot = await readCodexRolloutSnapshot(filePath);
    if (!snapshot) {
      if (invalidRecordPolicy === "throw") {
        throw new Error(`unreadable Codex compressed archive: ${filePath}`);
      }
      return {
        endOffset,
        lastTotal,
        tokenUsageBaselines,
        lastModel,
        lastServiceTier: lastServiceTier || null,
        modelAttributionState,
        eventsAggregated: 0,
        projectFileContexts,
        snapshotUnavailable: true,
      };
    }
    let currentCwd = null;
    for (const raw of snapshot.lines) {
      const line = typeof raw === "string" ? raw : "";
      if (
        !line ||
        !(
          line.includes('"turn_context"') ||
          line.includes('"session_meta"')
        ) ||
        !line.includes('"cwd"')
      ) {
        continue;
      }
      let obj;
      try {
        obj = JSON.parse(line);
      } catch (error) {
        if (invalidRecordPolicy === "throw") throw error;
        continue;
      }
      if (
        (obj?.type !== "turn_context" && obj?.type !== "session_meta") ||
        !obj?.payload ||
        typeof obj.payload !== "object" ||
        typeof obj.payload.cwd !== "string"
      ) {
        continue;
      }
      const nextCwd = obj.payload.cwd.trim();
      if (!nextCwd || nextCwd === currentCwd) continue;
      const context = await resolveProjectContextForPath({
        startDir: wsl.mapWslCwdToUnc(nextCwd, filePath),
        projectMetaCache,
        publicRepoCache,
        publicRepoResolver,
        projectState,
      });
      currentCwd = nextCwd;
      addProjectFileContext(projectFileContexts, context);
    }
    return {
      endOffset: snapshot.size,
      lastTotal,
      tokenUsageBaselines,
      lastModel,
      lastServiceTier: lastServiceTier || null,
      modelAttributionState,
      eventsAggregated: 0,
      projectFileContexts,
    };
  }

  const stream = fssync.createReadStream(filePath, { start: 0, end: endOffset - 1 });
  let currentCwd = null;
  let scannedEndOffset = 0;
  let committedEndOffset = 0;

  const invalidUtf8 = invalidRecordPolicy === "throw" ? "throw" : "record";
  for await (const record of physicalJsonlRecords(stream, { invalidUtf8 })) {
    scannedEndOffset += record.physicalBytes;
    if (record.terminated) committedEndOffset = scannedEndOffset;
    if (!record.utf8Valid) {
      if (!record.terminated) break;
      continue;
    }

    const { line } = record;
    if (
      !line ||
      !(
        line.includes('"turn_context"') ||
        line.includes('"session_meta"')
      ) ||
      !line.includes('"cwd"')
    ) {
      if (line && (invalidRecordPolicy === "throw" || !record.terminated)) {
        try {
          JSON.parse(line);
          committedEndOffset = scannedEndOffset;
        } catch (error) {
          if (invalidRecordPolicy === "throw") throw error;
          break;
        }
      }
      continue;
    }

    let obj;
    try {
      obj = JSON.parse(line);
    } catch (error) {
      if (invalidRecordPolicy === "throw") throw error;
      if (!record.terminated) break;
      continue;
    }
    if (!record.terminated) committedEndOffset = scannedEndOffset;
    if (
      (obj?.type !== "turn_context" && obj?.type !== "session_meta") ||
      !obj?.payload ||
      typeof obj.payload !== "object" ||
      typeof obj.payload.cwd !== "string"
    ) {
      continue;
    }

    const nextCwd = obj.payload.cwd.trim();
    if (!nextCwd || nextCwd === currentCwd) continue;
    const context = await resolveProjectContextForPath({
      startDir: wsl.mapWslCwdToUnc(nextCwd, filePath),
      projectMetaCache,
      publicRepoCache,
      publicRepoResolver,
      projectState,
    });
    currentCwd = nextCwd;
    addProjectFileContext(projectFileContexts, context);
  }

  return {
    endOffset: committedEndOffset,
    lastTotal,
    tokenUsageBaselines,
    lastModel,
    lastServiceTier: lastServiceTier || null,
    modelAttributionState,
    eventsAggregated: 0,
    projectFileContexts,
  };
}

// The whole delta belongs to a priority request, so every base column it
// carries is priority usage.
function addPriorityUsage(target, delta) {
  for (const [field, base] of PRIORITY_SUBSET_FIELDS) {
    const value = Number(delta?.[base] || 0);
    if (value > 0) target[field] = (Number(target[field]) || 0) + value;
  }
}

function rolloutDateFromPath(filePath) {
  const match = path.basename(String(filePath || "")).match(/^rollout-(\d{4}-\d{2}-\d{2})T/);
  return match ? match[1] : null;
}

function normalizeIsoDate(value) {
  const raw = typeof value === "string" ? value.trim() : "";
  const match = raw.match(/^(\d{4}-\d{2}-\d{2})/);
  return match ? match[1] : null;
}

// Max inter-row gap (ms) still treated as part of a forked rollout's replay
// burst. Empirically the replay flush spaces rows sub-ms to a few ms apart while
// genuine live turns arrive ≥~4.6s apart and the replay→live break is ≥11s, so
// any value in the ~0.25–2s band separates them with a wide margin; 500ms biases
// toward the low end because merging a real fast turn (under-count) is worse than
// leaving a slow replay counted (bounded over-count). See the skip site in
// parseRolloutFile. (issue #169 follow-up.)
const FORK_REPLAY_GAP_MS = 500;

function isForkedReplayToken({ isForkedRollout, rolloutDate, currentDate }) {
  return Boolean(isForkedRollout && rolloutDate && currentDate && currentDate < rolloutDate);
}

function addProjectFileContext(contexts, projectContext) {
  if (!Array.isArray(contexts) || !projectContext || typeof projectContext !== "object") return;
  const configPath =
    typeof projectContext.configPath === "string" && projectContext.configPath
      ? projectContext.configPath
      : null;
  if (!configPath) return;
  contexts.push(projectContext);
}

function extractTokenCount(obj) {
  const payload = obj?.payload;
  if (!payload) return null;
  if (payload.type === "token_count") {
    return { info: payload.info, timestamp: obj?.timestamp || null };
  }
  const msg = payload.msg;
  if (msg && msg.type === "token_count") {
    return { info: msg.info, timestamp: obj?.timestamp || null };
  }
  return null;
}

function normalizeUsage(u) {
  const out = {};
  for (const k of [
    "input_tokens",
    "cached_input_tokens",
    "cache_creation_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
  ]) {
    const n = Number(u[k] || 0);
    out[k] = Number.isFinite(n) && n >= 0 ? Math.floor(n) : 0;
  }
  // Codex rollouts (and Every Code, which shares the format) report
  // `input_tokens` as the TOTAL prompt, with `cached_input_tokens` as the
  // cached subset — i.e. the cached slice is INSIDE the input count. Our
  // queue schema (CLAUDE.md → Token Normalization Convention) stores
  // `input_tokens` as pure non-cached input and `cached_input_tokens`
  // separately. Without this subtraction the cost formula bills the cached
  // bytes twice: once at the full input rate and again at the cache_read
  // rate, producing ~6–7x cost inflation on cache-heavy Codex sessions
  // (verified against ccusage's per-day numbers on the same rollouts).
  // Preserve the reported total for compatibility with older Codex / Every
  // Code shapes where output_tokens can exclude reasoning or some component
  // fields are absent. The exact all-zero reset sentinel is rejected before
  // normalization by isCodexTotalOnlyResetSentinel().
  out.input_tokens = Math.max(0, out.input_tokens - out.cached_input_tokens);
  return out;
}

function isCodexTotalOnlyResetSentinel(lastUsage, totalUsage) {
  const componentTotal = (usage) => [
    usage?.input_tokens,
    usage?.cached_input_tokens,
    usage?.cache_creation_input_tokens ?? usage?.cache_write_input_tokens,
    usage?.output_tokens,
    usage?.reasoning_output_tokens,
  ].reduce((sum, value) => sum + Math.max(0, Number(value) || 0), 0);
  return Number(lastUsage?.total_tokens || 0) > 0
    && Number(totalUsage?.total_tokens || 0) === 0
    && componentTotal(lastUsage) === 0
    && componentTotal(totalUsage) === 0;
}

module.exports = {
  listRolloutFiles,
  isRolloutFileName,
  isCompressedRolloutFileName,
  isCompressedRolloutPath,
  readCodexRolloutSnapshot,
  listRolloutFilesDeep,
  parseRolloutIncremental,
  filterColdCodexRolloutFiles,
  codexSessionIdFromPath,
};
