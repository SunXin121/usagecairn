// antigravity usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fs = require("node:fs/promises");
const fssync = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const { ensureDir } = require("../fs");
const { readSqliteJsonRows } = require("../sqlite-reader");
const {
  BUCKET_SEPARATOR,
  enqueueTouchedBuckets,
  enqueueTouchedProjectBuckets,
  normalizeHourlyState,
  normalizeProjectState,
  getHourlyBucket,
  getProjectBucket,
  initTotals,
  addTotals,
  subtractTotals,
  toUtcHalfHourStart,
  bucketKey,
  projectBucketKey,
  parseBucketKey,
  normalizeSourceInput,
  resolveProjectContextForFile,
  isUncPath,
  snapshotSqliteDb,
} = require("./core");

function antigravityProjectAssignmentChanged(previous, projectContext) {
  const projectKey = projectContext?.projectKey || null;
  const projectRef = projectContext?.projectRef || null;
  const priorProjectKeys = new Set(
    Object.values(previous?.contributions || {})
      .map((contribution) => contribution?.projectKey)
      .filter((value) => typeof value === "string" && value.length > 0),
  );
  if (typeof previous?.projectKey === "string" && previous.projectKey) {
    priorProjectKeys.add(previous.projectKey);
  }
  const priorProjectRefs = new Set(
    Object.values(previous?.contributions || {})
      .map((contribution) => contribution?.projectRef)
      .filter((value) => typeof value === "string" && value.length > 0),
  );
  if (typeof previous?.projectRef === "string" && previous.projectRef) {
    priorProjectRefs.add(previous.projectRef);
  }
  return (
    priorProjectKeys.size !== (projectKey ? 1 : 0) ||
    (projectKey && !priorProjectKeys.has(projectKey)) ||
    priorProjectRefs.size !== (projectRef ? 1 : 0) ||
    (projectRef && !priorProjectRefs.has(projectRef)) ||
    (typeof previous?.projectStatus === "string" &&
      previous.projectStatus !== (projectContext?.status || null))
  );
}

function resolveAntigravityBrainDirs(geminiHome) {
  if (!geminiHome || typeof geminiHome !== "string") return [];
  return [
    path.join(geminiHome, "antigravity", "brain"),
    path.join(geminiHome, "antigravity-ide", "brain"),
    path.join(geminiHome, "antigravity-cli", "brain"),
  ];
}

function antigravityPathWithin(filePath, root) {
  if (typeof filePath !== "string" || typeof root !== "string") return false;
  const relative = path.relative(root, filePath);
  return Boolean(
    relative &&
      relative !== ".." &&
      !relative.startsWith(`..${path.sep}`) &&
      !path.isAbsolute(relative),
  );
}

async function listAntigravitySessionFilesWithStatus(brainDir, knownFiles = null) {
  const out = [];
  const known = knownFiles instanceof Set ? knownFiles : new Set(knownFiles || []);
  if (!brainDir || typeof brainDir !== "string") {
    return { files: out, complete: false };
  }

  let entries;
  try {
    entries = await fs.readdir(brainDir, { withFileTypes: true });
  } catch (err) {
    // A missing optional install directory is an authoritative empty inventory
    // only when it has never contained a tracked session. If it has, the
    // directory may be temporarily unavailable and must not trigger retraction.
    return {
      files: out,
      complete:
        err?.code === "ENOENT" &&
        !Array.from(known).some((filePath) => antigravityPathWithin(filePath, brainDir)),
    };
  }

  let complete = true;
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const logsDir = path.join(brainDir, entry.name, ".system_generated", "logs");
    const transcriptPath = path.join(logsDir, "transcript.jsonl");
    let st;
    try {
      st = await fs.stat(transcriptPath);
    } catch (err) {
      // ENOENT means this session directory currently has no transcript (or
      // the transcript was deleted). Other errors make the scan incomplete.
      if (err?.code !== "ENOENT") complete = false;
      continue;
    }
    if (st.isFile()) out.push(transcriptPath);
  }

  out.sort((a, b) => a.localeCompare(b));
  return { files: out, complete };
}

async function listAntigravitySessionFiles(brainDir) {
  return (await listAntigravitySessionFilesWithStatus(brainDir)).files;
}

async function listAntigravityTranscriptsWithStatus(geminiHome, knownFiles = null) {
  const dirs = resolveAntigravityBrainDirs(geminiHome);
  const results = await Promise.all(
    dirs.map((dir) => listAntigravitySessionFilesWithStatus(dir, knownFiles)),
  );
  return {
    files: results.flatMap((result) => result.files),
    complete: results.every((result) => result.complete),
  };
}

async function listAntigravityTranscripts(geminiHome) {
  return (await listAntigravityTranscriptsWithStatus(geminiHome)).files;
}

const ANTIGRAVITY_CURSOR_VERSION = 2;
// Bump when extractAntigravityGenInfo starts reading records it used to
// discard. Kept apart from the cursor version: a cursor-version mismatch routes
// to the legacy migration, which treats the cursor as having no per-file ledger.
const ANTIGRAVITY_EXTRACTOR_REVISION = 1;
const ANTIGRAVITY_MAX_CONTRIBUTIONS = 4096;

function hashAntigravityTranscript(buffer) {
  if (!Buffer.isBuffer(buffer)) return null;
  return crypto.createHash("sha256").update(buffer).digest("hex");
}

function antigravityTranscriptPrefixMatches(buffer, previous) {
  if (!Buffer.isBuffer(buffer) || !previous || typeof previous.transcriptHash !== "string") {
    return false;
  }
  const previousSize = Number(previous.size);
  if (!Number.isSafeInteger(previousSize) || previousSize < 0 || buffer.length < previousSize) {
    return false;
  }
  return hashAntigravityTranscript(buffer.subarray(0, previousSize)) === previous.transcriptHash;
}

async function readAntigravityTranscriptBytes(filePath) {
  try {
    return await fs.readFile(filePath);
  } catch (_error) {
    return null;
  }
}

async function statAntigravityDatabase(dbPath) {
  const result = {
    dbExists: false,
    dbInode: 0,
    dbMtimeMs: 0,
    dbCtimeMs: 0,
    dbSize: 0,
    dbWalInode: 0,
    dbWalMtimeMs: 0,
    dbWalSize: 0,
    dbJournalInode: 0,
    dbJournalMtimeMs: 0,
    dbJournalSize: 0,
  };
  if (!dbPath) return result;
  for (const [suffix, prefix] of [
    ["", "db"],
    ["-wal", "dbWal"],
    ["-journal", "dbJournal"],
  ]) {
    const st = await fs.stat(`${dbPath}${suffix}`).catch(() => null);
    if (!st || !st.isFile()) continue;
    if (suffix === "") result.dbExists = true;
    result[`${prefix}MtimeMs`] = Number.isFinite(st.mtimeMs) ? st.mtimeMs : 0;
    if (suffix === "") {
      result.dbInode = Number.isFinite(st.ino) ? st.ino : 0;
      result.dbCtimeMs = Number.isFinite(st.ctimeMs) ? st.ctimeMs : 0;
    } else {
      result[`${prefix}Inode`] = Number.isFinite(st.ino) ? st.ino : 0;
    }
    result[`${prefix}Size`] = Number.isFinite(st.size) ? st.size : 0;
  }
  return result;
}

// Neither the transcript nor the database changes when the extractor does, so
// without this stamp a session read before an extractor fix keeps its totals.
function antigravityExtractorStale(previous, dbPath) {
  return Boolean(
    dbPath &&
      previous &&
      previous.cursorVersion === ANTIGRAVITY_CURSOR_VERSION &&
      previous.extractorRevision !== ANTIGRAVITY_EXTRACTOR_REVISION,
  );
}

function sameAntigravityDatabase(previous, current) {
  if (!previous || !current) return false;
  // SQLite updates the -shm read marks when a reader opens a WAL database.
  // Those mtime changes do not represent new usage metadata and must not force
  // a full transcript reconciliation on every sync.
  return [
    "dbExists",
    "dbInode",
    "dbMtimeMs",
    "dbCtimeMs",
    "dbSize",
    "dbWalInode",
    "dbWalMtimeMs",
    "dbWalSize",
    "dbJournalInode",
    "dbJournalMtimeMs",
    "dbJournalSize",
  ].every((field) => Number(previous[field] || 0) === Number(current[field] || 0));
}

function capAntigravityContributions(contributions) {
  if (!contributions || typeof contributions !== "object") {
    return { contributions: {}, complete: true };
  }
  const entries = Object.entries(contributions);
  if (entries.length <= ANTIGRAVITY_MAX_CONTRIBUTIONS) {
    return { contributions, complete: true };
  }

  entries.sort(([, left], [, right]) => {
    const leftTime = Date.parse(left?.bucketStart || "");
    const rightTime = Date.parse(right?.bucketStart || "");
    if (Number.isFinite(leftTime) && Number.isFinite(rightTime) && leftTime !== rightTime) {
      return rightTime - leftTime;
    }
    if (Number.isFinite(rightTime) !== Number.isFinite(leftTime)) {
      return Number.isFinite(rightTime) ? 1 : -1;
    }
    return 0;
  });

  return {
    contributions: Object.fromEntries(entries.slice(0, ANTIGRAVITY_MAX_CONTRIBUTIONS)),
    complete: false,
  };
}

function resetAntigravityBucketsForRebuild({
  hourlyState,
  projectState,
  sources,
  touchedBuckets,
  projectTouchedBuckets,
}) {
  if (!sources || sources.size === 0) return;

  for (const [key, bucket] of Object.entries(hourlyState?.buckets || {})) {
    if (!bucket || !bucket.totals) continue;
    const bucketSource = normalizeSourceInput(parseBucketKey(key).source);
    if (!sources.has(bucketSource)) continue;
    bucket.totals = initTotals();
    bucket.queuedKey = null;
    bucket.retractedUnknownKey = null;
    touchedBuckets.add(key);
  }

  for (const key of Object.keys(hourlyState?.groupQueued || {})) {
    const groupSource = normalizeSourceInput(key.split(BUCKET_SEPARATOR)[0]);
    if (sources.has(groupSource)) delete hourlyState.groupQueued[key];
  }

  if (!projectState || !projectTouchedBuckets) return;
  for (const [key, bucket] of Object.entries(projectState.buckets || {})) {
    const keyParts = key.split(BUCKET_SEPARATOR);
    const bucketSource = normalizeSourceInput(bucket?.source || keyParts.at(-2));
    if (!sources.has(bucketSource) || !bucket?.totals) continue;
    bucket.totals = initTotals();
    bucket.queuedKey = null;
    projectTouchedBuckets.add(key);
  }
}

async function parseAntigravityIncremental({
  sessionFiles,
  cursors,
  queuePath,
  projectQueuePath,
  onProgress,
  source,
  publicRepoResolver,
  // Source-wide rebuilds may retract sessions absent from sessionFiles. Only
  // permit that when discovery explicitly completed; direct parser callers
  // retain the historical complete-inventory default for compatibility.
  inventoryComplete = true,
}) {
  await ensureDir(path.dirname(queuePath));
  let filesProcessed = 0;
  let eventsAggregated = 0;

  const cb = typeof onProgress === "function" ? onProgress : null;
  const files = Array.isArray(sessionFiles) ? sessionFiles : [];
  const totalFiles = files.length;
  // Keep every mutation private until both queue appends succeed. The generic
  // normalizers intentionally preserve bucket object references for other
  // parsers, so Antigravity must deep-clone its working state here; otherwise a
  // queue failure can leave a committed cursor pointing at already-mutated
  // hourly totals.
  const hourlyState = structuredClone(normalizeHourlyState(cursors?.hourly));
  const projectEnabled = typeof projectQueuePath === "string" && projectQueuePath.length > 0;
  const projectState = projectEnabled
    ? structuredClone(normalizeProjectState(cursors?.projectHourly))
    : null;
  const projectTouchedBuckets = projectEnabled ? new Set() : null;
  const projectMetaCache = projectEnabled ? new Map() : null;
  const publicRepoCache = projectEnabled ? new Map() : null;
  const touchedBuckets = new Set();
  const defaultSource = normalizeSourceInput(source) || "antigravity";
  const fileCursors =
    cursors.files && typeof cursors.files === "object" && !Array.isArray(cursors.files)
      ? structuredClone(cursors.files)
      : {};

  const fileSources = new Set();
  const fileSourceByPath = new Map();
  for (const entry of files) {
    const filePath = typeof entry === "string" ? entry : entry?.path;
    const fileSource =
      typeof entry === "string"
        ? defaultSource
        : normalizeSourceInput(entry?.source) || defaultSource;
    if (filePath) fileSourceByPath.set(filePath, fileSource);
    fileSources.add(fileSource);
  }
  if (fileSources.size === 0) fileSources.add(defaultSource);
  for (const [filePath, prev] of Object.entries(fileCursors)) {
    if (!resolveAntigravityDbPath(filePath)) continue;
    const previousSource = normalizeSourceInput(prev?.source);
    if (previousSource) fileSources.add(previousSource);
  }

  // Legacy cursors have no per-file contribution ledger. Rebuild when every
  // legacy file is still present; if discovery is incomplete, preserve the old
  // aggregate and migrate live files against transcript-only baselines below.
  const liveFilePaths = new Set(
    files
      .map((entry) => (typeof entry === "string" ? entry : entry?.path))
      .filter(Boolean),
  );
  const incompleteContributionPaths = new Set();
  const deferredIncompletePaths = new Set();
  let needsFullRebuild = false;
  for (const [filePath, prev] of Object.entries(fileCursors)) {
    if (!resolveAntigravityDbPath(filePath) || !prev || Number(prev.lastLine || 0) <= 0) {
      continue;
    }
    const isLegacy =
      prev.cursorVersion !== ANTIGRAVITY_CURSOR_VERSION ||
      !prev.contributions ||
      typeof prev.contributions !== "object";
    if (!isLegacy && prev.contributionsComplete === false) {
      incompleteContributionPaths.add(filePath);
    }
  }

  // Project reassignment is another full-ledger change: a capped ledger cannot
  // retract contributions that fell outside its retained window. Resolve
  // incomplete files against a private project state before deciding whether a
  // source-wide rebuild is required.
  const preflightProjectState =
    projectEnabled && incompleteContributionPaths.size > 0
      ? structuredClone(projectState)
      : null;
  const preflightProjectMetaCache = preflightProjectState ? new Map() : null;
  const preflightPublicRepoCache = preflightProjectState ? new Map() : null;

  // A capped contribution ledger is safe while its transcript/SQLite identity
  // is unchanged. If an incomplete ledger needs reconciliation, rebuild the
  // complete source baseline before applying any per-file deltas. When source
  // discovery is incomplete, defer the file instead: resetting the source
  // would erase contributions belonging to sessions omitted by discovery.
  if (!needsFullRebuild && incompleteContributionPaths.size > 0) {
    for (const filePath of incompleteContributionPaths) {
      if (!liveFilePaths.has(filePath)) {
        if (!inventoryComplete) {
          deferredIncompletePaths.add(filePath);
        } else {
          needsFullRebuild = true;
        }
        if (needsFullRebuild) break;
        continue;
      }
      const prev = cursors.files[filePath];
      const st = await fs.stat(filePath).catch(() => null);
      const dbPath = resolveAntigravityDbPath(filePath);
      const dbIdentity = await statAntigravityDatabase(dbPath);
      const sameInode = st && prev.inode === (st.ino || 0);
      const transcriptCanAppend =
        sameInode &&
        (Number.isFinite(st.size) ? st.size : 0) >= Number(prev.size || 0) &&
        antigravityTranscriptPrefixMatches(
          await readAntigravityTranscriptBytes(filePath),
          prev,
        );
      const sameSource =
        !fileSourceByPath.has(filePath) ||
        (prev.source == null ? defaultSource : prev.source) === fileSourceByPath.get(filePath);
      const sameDb = sameAntigravityDatabase(prev, dbIdentity);
      const staleExtractor = antigravityExtractorStale(prev, dbPath);
      let projectChanged = false;
      if (preflightProjectState) {
        const projectContext = await resolveProjectContextForFile({
          filePath,
          projectMetaCache: preflightProjectMetaCache,
          publicRepoCache: preflightPublicRepoCache,
          publicRepoResolver,
          projectState: preflightProjectState,
        });
        projectChanged = antigravityProjectAssignmentChanged(prev, projectContext);
      }
      if (!transcriptCanAppend || !sameDb || staleExtractor || !sameSource || projectChanged) {
        if (!inventoryComplete) {
          deferredIncompletePaths.add(filePath);
        } else {
          needsFullRebuild = true;
        }
        if (needsFullRebuild) break;
      }
    }
  }

  if (needsFullRebuild) {
    for (const filePath of Object.keys(fileCursors)) {
      if (resolveAntigravityDbPath(filePath) && !liveFilePaths.has(filePath)) {
        delete fileCursors[filePath];
      }
    }
    resetAntigravityBucketsForRebuild({
      hourlyState,
      projectState,
      sources: fileSources,
      touchedBuckets,
      projectTouchedBuckets,
    });
  }

  for (let idx = 0; idx < files.length; idx++) {
    const entry = files[idx];
    const filePath = typeof entry === "string" ? entry : entry?.path;
    if (!filePath) continue;
    const fileSource =
      typeof entry === "string"
        ? defaultSource
        : normalizeSourceInput(entry?.source) || defaultSource;
    const st = await fs.stat(filePath).catch(() => null);
    if (!st || !st.isFile()) {
      if (needsFullRebuild) {
        throw new Error(`Antigravity transcript disappeared during rebuild: ${filePath}`);
      }
      continue;
    }

    const key = filePath;
    if (deferredIncompletePaths.has(key)) continue;
    const prev = fileCursors[key] || null;

    const inode = st.ino || 0;
    const size = Number.isFinite(st.size) ? st.size : 0;
    const mtimeMs = Number.isFinite(st.mtimeMs) ? st.mtimeMs : 0;
    const ctimeMs = Number.isFinite(st.ctimeMs) ? st.ctimeMs : 0;

    const isLegacy = Boolean(
      prev &&
        (prev.cursorVersion !== ANTIGRAVITY_CURSOR_VERSION ||
          !prev.contributions ||
          typeof prev.contributions !== "object"),
    );

    const dbPath = resolveAntigravityDbPath(filePath);
    const dbIdentity = await statAntigravityDatabase(dbPath);

    const sameInode = prev && prev.inode === inode;
    const sameTranscriptStats =
      sameInode &&
      prev.size === size &&
      prev.mtimeMs === mtimeMs &&
      Number(prev.ctimeMs || 0) === ctimeMs;
    const sameDb = prev && sameAntigravityDatabase(prev, dbIdentity);
    const sameSource =
      !prev || (prev.source == null ? defaultSource : prev.source) === fileSource;
    const currentVersion = !isLegacy && prev?.cursorVersion === ANTIGRAVITY_CURSOR_VERSION;
    const staleExtractor = !isLegacy && antigravityExtractorStale(prev, dbPath);
    let transcriptBytes = null;
    let transcriptHash = typeof prev?.transcriptHash === "string" ? prev.transcriptHash : null;
    let sameTranscriptContent = sameTranscriptStats;
    let transcriptCanAppend = false;

    // Stat metadata is only a fast hint. On any apparent file change, compare
    // the bytes so atomic replacement, truncation, and edits before the cursor
    // cannot be mistaken for an append. A cursor without this hash is treated
    // conservatively and gets one reconciliation pass.
    const transcriptReadRequired =
      !sameTranscriptStats || !currentVersion || !prev?.transcriptHash || needsFullRebuild;
    if (transcriptReadRequired) {
      transcriptBytes = await readAntigravityTranscriptBytes(filePath);
      const afterReadStat = await fs.stat(filePath).catch(() => null);
      if (
        !Buffer.isBuffer(transcriptBytes) ||
        !afterReadStat ||
        !afterReadStat.isFile() ||
        (afterReadStat.ino || 0) !== inode ||
        transcriptBytes.length !== size ||
        afterReadStat.size !== size ||
        afterReadStat.mtimeMs !== mtimeMs ||
        (Number.isFinite(afterReadStat.ctimeMs) ? afterReadStat.ctimeMs : 0) !== ctimeMs
      ) {
        throw new Error(`Antigravity transcript changed while being read: ${filePath}`);
      }
      transcriptHash = hashAntigravityTranscript(transcriptBytes);
      sameTranscriptContent =
        Boolean(
          (prev?.transcriptHash && transcriptHash && prev.transcriptHash === transcriptHash && prev.size === size) ||
            (!prev?.transcriptHash && sameTranscriptStats),
        );
      transcriptCanAppend =
        Boolean(
          prev &&
            transcriptBytes &&
            size >= Number(prev.size || 0) &&
            (prev.transcriptHash
              ? antigravityTranscriptPrefixMatches(transcriptBytes, prev)
              : sameInode),
        );
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
    const projectChanged =
      projectEnabled && antigravityProjectAssignmentChanged(prev, projectContext);
    const baselineLine = Number(prev?.baselineLine || (isLegacy ? prev?.lastLine || 0 : 0));

    // Legacy migration: "no retroactive claim". The file's history stays in the
    // old aggregate untouched. If unchanged, stamp v2 cursor with contributions: {}
    // and queue no buckets.
    if (!needsFullRebuild && isLegacy && (sameTranscriptStats || sameTranscriptContent)) {
      const contributionState = capAntigravityContributions({});
      fileCursors[key] = {
        ...prev,
        inode,
        size,
        mtimeMs,
        ctimeMs,
        source: fileSource,
        transcriptHash:
          transcriptHash ||
          (transcriptBytes
            ? hashAntigravityTranscript(transcriptBytes)
            : prev?.transcriptHash || null),
        ...dbIdentity,
        projectKey: projectRef ? projectKey : null,
        projectRef: projectRef || null,
        projectStatus: projectContext?.status || null,
        projectConfigPath: projectContext?.configPath || null,
        projectConfigMtimeMs: projectContext?.configMtimeMs ?? null,
        projectConfigSize: projectContext?.configSize ?? null,
        projectReconciliationDeferred: false,
        baselineLine,
        cursorVersion: ANTIGRAVITY_CURSOR_VERSION,
        extractorRevision: ANTIGRAVITY_EXTRACTOR_REVISION,
        lastLine: Number(prev.lastLine || 0),
        contextTokens: Number(prev.contextTokens || 0),
        previousContextTokens: Number(prev.previousContextTokens || 0),
        currentModel: prev.currentModel,
        lastPlannerModel: prev.lastPlannerModel,
        usageSource: prev.usageSource || (dbIdentity ? "sqlite" : "estimated"),
        dbReadOk: prev.dbReadOk !== false,
        contributions: contributionState.contributions,
        contributionsComplete: contributionState.complete,
        updatedAt: new Date().toISOString(),
      };
      filesProcessed += 1;
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

    const metadataRetryNeeded = currentVersion && Boolean(dbPath) && prev?.dbReadOk !== true;
    if (
      sameTranscriptStats &&
      sameTranscriptContent &&
      sameDb &&
      sameSource &&
      currentVersion &&
      !staleExtractor &&
      !metadataRetryNeeded &&
      !projectChanged &&
      !needsFullRebuild
    ) {
      filesProcessed += 1;
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

    // Reconcile historical contributions whenever the cursor needs migration or
    // SQLite changed, even when new transcript lines were appended in the same
    // sync. Otherwise the incremental branch stamps a migrated cursor without
    // repairing its old, incomplete totals. Legacy cursors have no retroactive
    // claim and are not routed through reconciliation.
    const needsReconcile =
      !needsFullRebuild &&
      !isLegacy &&
      prev &&
      (metadataRetryNeeded ||
        !currentVersion ||
        !sameDb ||
        staleExtractor ||
        !sameSource ||
        projectChanged ||
        (!sameTranscriptContent && !transcriptCanAppend));

    // Reconciliation reparses the complete transcript. Even when stat metadata
    // looked unchanged, project/SQLite changes can select this path; obtain a
    // stable snapshot here rather than letting parseAntigravityFile silently
    // turn a transient read failure into an empty transcript.
    if (!transcriptBytes && (needsFullRebuild || needsReconcile)) {
      transcriptBytes = await readAntigravityTranscriptBytes(filePath);
      const afterReadStat = await fs.stat(filePath).catch(() => null);
      if (
        !Buffer.isBuffer(transcriptBytes) ||
        !afterReadStat ||
        !afterReadStat.isFile() ||
        (afterReadStat.ino || 0) !== inode ||
        transcriptBytes.length !== size ||
        afterReadStat.size !== size ||
        afterReadStat.mtimeMs !== mtimeMs ||
        (Number.isFinite(afterReadStat.ctimeMs) ? afterReadStat.ctimeMs : 0) !== ctimeMs
      ) {
        throw new Error(`Antigravity transcript changed while being read: ${filePath}`);
      }
      transcriptHash = hashAntigravityTranscript(transcriptBytes);
      sameTranscriptContent =
        Boolean(transcriptHash && prev?.transcriptHash === transcriptHash && prev.size === size);
      transcriptCanAppend = Boolean(
        prev &&
          size >= Number(prev.size || 0) &&
          antigravityTranscriptPrefixMatches(transcriptBytes, prev),
      );
    }

    const protectExactState = needsReconcile && prev?.usageSource === "sqlite";
    const hourlyStateBeforeReconcile = protectExactState ? structuredClone(hourlyState) : null;
    const projectStateBeforeReconcile =
      protectExactState && projectState ? structuredClone(projectState) : null;
    const touchedBeforeReconcile = protectExactState ? new Set(touchedBuckets) : null;
    const projectTouchedBeforeReconcile =
      protectExactState && projectTouchedBuckets ? new Set(projectTouchedBuckets) : null;
    let result;

    if (needsFullRebuild || needsReconcile) {
      if (
        !needsFullRebuild &&
        prev &&
        prev.contributions &&
        typeof prev.contributions === "object"
      ) {
        for (const c of Object.values(prev.contributions)) {
          if (!c || !c.totals) continue;
          const oldBucket = getHourlyBucket(hourlyState, c.source, c.model, c.bucketStart);
          subtractTotals(oldBucket.totals, c.totals);
          touchedBuckets.add(bucketKey(c.source, c.model, c.bucketStart));
          if (
            projectEnabled &&
            c.projectKey &&
            projectState &&
            projectTouchedBuckets
          ) {
            const oldProjectBucket = getProjectBucket(
              projectState,
              c.projectKey,
              c.source,
              c.bucketStart,
              c.projectRef || null,
            );
            subtractTotals(oldProjectBucket.totals, c.totals);
            projectTouchedBuckets.add(projectBucketKey(c.projectKey, c.source, c.bucketStart));
          }
        }
      }
      result = await parseAntigravityFile({
        filePath,
        ...(transcriptBytes ? { rawBuffer: transcriptBytes } : {}),
        lastLine: 0,
        baselineLine: needsFullRebuild ? 0 : baselineLine,
        watermarkLine: Number(prev?.lastLine || 0),
        initialUsageSource: prev?.usageSource,
        hourlyState,
        touchedBuckets,
        source: fileSource,
        projectState,
        projectTouchedBuckets,
        projectRef,
        projectKey,
      });
    } else {
      const canResume = sameTranscriptContent || transcriptCanAppend;
      const lastLine = canResume ? Number(prev?.lastLine || 0) : 0;
      const initialContextTokens = canResume ? Number(prev?.contextTokens || 0) : 0;
      const initialPrevContext = canResume ? Number(prev?.previousContextTokens || 0) : 0;
      const initialModel =
        canResume && typeof prev?.currentModel === "string" ? prev.currentModel : null;
      const initialLastPlannerModel =
        canResume && typeof prev?.lastPlannerModel === "string"
          ? prev.lastPlannerModel
          : null;
      const initialUsageSource =
        canResume && typeof prev?.usageSource === "string" ? prev.usageSource : null;

      result = await parseAntigravityFile({
        filePath,
        ...(transcriptBytes ? { rawBuffer: transcriptBytes } : {}),
        lastLine,
        baselineLine,
        initialContextTokens,
        initialPrevContext,
        initialModel,
        initialLastPlannerModel,
        initialUsageSource,
        hourlyState,
        touchedBuckets,
        source: fileSource,
        projectState,
        projectTouchedBuckets,
        projectRef,
        projectKey,
      });

      const mergedContributions = { ...(prev?.contributions || {}) };
      for (const [cKey, cVal] of Object.entries(result.contributions || {})) {
        if (!mergedContributions[cKey]) {
          mergedContributions[cKey] = { ...cVal, totals: { ...cVal.totals } };
        } else {
          addTotals(mergedContributions[cKey].totals, cVal.totals);
        }
      }
      result.contributions = mergedContributions;
    }

    if (result.deferred && prev) {
      if (needsFullRebuild && !hourlyStateBeforeReconcile) {
        throw new Error(`Cannot rebuild Antigravity transcript while SQLite is unavailable: ${filePath}`);
      }
      if (hourlyStateBeforeReconcile) {
        for (const field of Object.keys(hourlyState)) delete hourlyState[field];
        Object.assign(hourlyState, hourlyStateBeforeReconcile);
        if (touchedBeforeReconcile) {
          touchedBuckets.clear();
          for (const touched of touchedBeforeReconcile) touchedBuckets.add(touched);
        }
      }
      if (projectState && projectStateBeforeReconcile) {
        for (const field of Object.keys(projectState)) delete projectState[field];
        Object.assign(projectState, projectStateBeforeReconcile);
        if (projectTouchedBeforeReconcile) {
          projectTouchedBuckets.clear();
          for (const touched of projectTouchedBeforeReconcile) projectTouchedBuckets.add(touched);
        }
      }
      fileCursors[key] = {
        ...prev,
        inode,
        size,
        mtimeMs,
        ctimeMs,
        source: fileSource,
        transcriptHash,
        ...dbIdentity,
        dbReadOk: false,
        updatedAt: new Date().toISOString(),
      };
      filesProcessed += 1;
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

    const contributionState = capAntigravityContributions(result.contributions);
    fileCursors[key] = {
      inode,
      size,
      mtimeMs,
      ctimeMs,
      source: fileSource,
      transcriptHash,
      ...dbIdentity,
      projectKey: projectRef ? projectKey : null,
      projectRef: projectRef || null,
      projectStatus: projectContext?.status || null,
      projectConfigPath: projectContext?.configPath || null,
      projectConfigMtimeMs: projectContext?.configMtimeMs ?? null,
      projectConfigSize: projectContext?.configSize ?? null,
      projectReconciliationDeferred: false,
      baselineLine: needsFullRebuild ? 0 : baselineLine,
      cursorVersion: ANTIGRAVITY_CURSOR_VERSION,
      extractorRevision: ANTIGRAVITY_EXTRACTOR_REVISION,
      lastLine: result.lastLine,
      contextTokens: result.contextTokens,
      previousContextTokens: result.previousContextTokens,
      currentModel: result.currentModel,
      lastPlannerModel: result.lastPlannerModel,
      usageSource: result.usageSource,
      dbReadOk: result.dbReadOk !== false,
      contributions: contributionState.contributions,
      contributionsComplete: contributionState.complete,
      updatedAt: new Date().toISOString(),
    };

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
  hourlyState.updatedAt = new Date().toISOString();
  cursors.files = fileCursors;
  cursors.hourly = hourlyState;
  if (projectState) {
    projectState.updatedAt = new Date().toISOString();
    cursors.projectHourly = projectState;
  }

  return { filesProcessed, eventsAggregated, bucketsQueued, projectBucketsQueued };
}

function decodeAntigravityVarint(buf, offset) {
  if (!Buffer.isBuffer(buf) || !Number.isSafeInteger(offset) || offset < 0) {
    throw new RangeError("invalid Antigravity protobuf offset");
  }
  const start = offset;
  let res = 0;
  for (let count = 0; count < 10; count++) {
    if (offset >= buf.length) throw new RangeError("truncated Antigravity protobuf varint");
    const b = buf[offset++];
    // 64 bits is nine 7-bit groups plus one bit of a tenth byte.
    if (count === 9 && b > 1) throw new RangeError("Antigravity protobuf varint exceeds 64 bits");
    res += (b & 0x7f) * 2 ** (count * 7);
    if (!(b & 0x80)) {
      if (Number.isSafeInteger(res)) return [res, offset];
      // Antigravity writes 2^64 - 1 in fields the extractor never reads. The
      // float sum has already lost precision, so re-read the bytes exactly.
      let exact = 0n;
      for (let i = start; i < offset; i++) {
        exact |= BigInt(buf[i] & 0x7f) << BigInt((i - start) * 7);
      }
      return [exact, offset];
    }
  }
  throw new RangeError("overlong Antigravity protobuf varint");
}

function findAntigravityProtoFields(buf) {
  if (!Buffer.isBuffer(buf)) throw new RangeError("invalid Antigravity protobuf payload");
  const fields = [];
  let offset = 0;
  while (offset < buf.length) {
    const [tag, next] = decodeAntigravityVarint(buf, offset);
    if (typeof tag !== "number") throw new RangeError("unsafe Antigravity protobuf field tag");
    offset = next;
    const fieldNum = Math.floor(tag / 8);
    const wireType = tag & 7;
    if (fieldNum <= 0) throw new RangeError("invalid Antigravity protobuf field number");
    if (wireType === 0) {
      // May be a BigInt; token reads use Number.isSafeInteger, so it is never billed.
      const [val, vNext] = decodeAntigravityVarint(buf, offset);
      offset = vNext;
      fields.push({ num: fieldNum, val });
    } else if (wireType === 2) {
      const [len, lNext] = decodeAntigravityVarint(buf, offset);
      offset = lNext;
      if (len > buf.length - offset) {
        throw new RangeError("truncated Antigravity protobuf field");
      }
      fields.push({ num: fieldNum, val: buf.subarray(offset, offset + len) });
      offset += len;
    } else if (wireType === 1) {
      if (buf.length - offset < 8) throw new RangeError("truncated Antigravity protobuf field");
      offset += 8;
    } else if (wireType === 5) {
      if (buf.length - offset < 4) throw new RangeError("truncated Antigravity protobuf field");
      offset += 4;
    } else {
      throw new RangeError("unsupported Antigravity protobuf wire type");
    }
  }
  return fields;
}

function extractAntigravityGenInfo(buf) {
  try {
    const root = findAntigravityProtoFields(buf);
    const f1 = root.find((f) => f.num === 1)?.val;
    if (!f1) return null;

    const inner = findAntigravityProtoFields(f1);
    let model = null;
    const f19 = inner.find((f) => f.num === 19)?.val;
    if (f19) model = Buffer.from(f19).toString("utf8").trim();

    let contextTokens = 0;
    const f9 = inner.find((f) => f.num === 9)?.val;
    if (f9) {
      const f10 = findAntigravityProtoFields(f9).find((f) => f.num === 10)?.val;
      if (f10) {
        const tok = findAntigravityProtoFields(f10).find((f) => f.num === 1)?.val;
        if (Number.isSafeInteger(tok)) contextTokens = tok;
      }
    }

    let lastStepIndex = null;
    for (const f of inner) {
      if (f.num !== 20 || !f.val) continue;
      const kv = findAntigravityProtoFields(f.val);
      const k = kv.find((x) => x.num === 1)?.val;
      const v = kv.find((x) => x.num === 2)?.val;
      if (k && Buffer.from(k).toString("utf8") === "last_step_index" && v) {
        const rawStepIndex = Buffer.from(v).toString("utf8").trim();
        if (/^\d+$/.test(rawStepIndex)) {
          const parsed = Number(rawStepIndex);
          if (Number.isSafeInteger(parsed)) lastStepIndex = parsed;
        }
      }
    }

    let uncachedInput;
    let cachedInput;
    let outputTokens;
    let textOutput;
    let reasoningOutput;
    let hasUsageMetadata = false;

    const f4 = inner.find((f) => f.num === 4)?.val;
    if (f4) {
      // Antigravity internal usage message layout (Field 4):
      // - f4.1: system / tool instruction prefix tokens (per-model fixed prefix)
      // - f4.2: user / prompt tokens for this turn
      // - f4.3: total output tokens checksum (f4.9 text + f4.10 reasoning)
      // - f4.5: cached prompt tokens (cache read)
      // - f4.9: text output tokens
      // - f4.10: reasoning / thought tokens
      const f4fields = findAntigravityProtoFields(f4);
      const sTok = f4fields.find((f) => f.num === 1)?.val;
      const pTok = f4fields.find((f) => f.num === 2)?.val;
      const cTok = f4fields.find((f) => f.num === 5)?.val;
      const oTok = f4fields.find((f) => f.num === 3)?.val;
      const tTok = f4fields.find((f) => f.num === 9)?.val;
      const rTok = f4fields.find((f) => f.num === 10)?.val;
      if (
        Number.isSafeInteger(sTok) ||
        Number.isSafeInteger(pTok) ||
        Number.isSafeInteger(cTok) ||
        Number.isSafeInteger(oTok) ||
        Number.isSafeInteger(tTok) ||
        Number.isSafeInteger(rTok)
      ) {
        hasUsageMetadata = true;
        const sysTokens = Number.isSafeInteger(sTok) ? sTok : 0;
        const promptTokens = Number.isSafeInteger(pTok) ? pTok : 0;
        uncachedInput = sysTokens + promptTokens;
        cachedInput = Number.isSafeInteger(cTok) ? cTok : 0;
        outputTokens = Number.isSafeInteger(oTok) ? oTok : 0;
        textOutput = Number.isSafeInteger(tTok) ? tTok : 0;
        reasoningOutput = Number.isSafeInteger(rTok) ? rTok : 0;
      }
    }

    return {
      model,
      contextTokens,
      lastStepIndex,
      ...(hasUsageMetadata
        ? {
            uncachedInput,
            cachedInput,
            outputTokens,
            textOutput,
            reasoningOutput,
            hasUsageMetadata: true,
          }
        : {}),
    };
  } catch (_error) {
    return null;
  }
}

function resolveAntigravityDbPath(transcriptPath) {
  if (!transcriptPath || typeof transcriptPath !== "string") return null;
  const m = transcriptPath.match(/^(.*)[/\\]brain[/\\]([^/\\]+)[/\\]\.system_generated[/\\]logs[/\\]transcript.*\.jsonl$/);
  if (!m) return null;
  return path.join(m[1], "conversations", `${m[2]}.db`);
}

function readAntigravityConversationDb(dbPath) {
  if (!dbPath) return null;
  let snapshot = null;
  let effectiveDbPath = dbPath;
  // Antigravity keeps active generation metadata in SQLite WAL files. Reading
  // a UNC/WSL database directly is both lock-prone and can miss rows that have
  // not checkpointed into the main file, so copy the database and sidecars as
  // one local read view first.
  if (isUncPath(dbPath)) {
    try {
      snapshot = snapshotSqliteDb(dbPath);
      effectiveDbPath = snapshot.path;
    } catch (_error) {
      // Preserve the existing direct-read fallback for transient snapshot or
      // permission failures.
    }
  }
  try {
    if (!fssync.existsSync(effectiveDbPath)) return null;
    const rows = readSqliteJsonRows(
      effectiveDbPath,
      "SELECT idx, quote(data) as hex FROM gen_metadata ORDER BY idx",
      {
        readOnly: true,
        throwOnReadFailure: true,
        maxBuffer: 64 * 1024 * 1024,
        timeout: 15_000,
      },
    );
    if (!Array.isArray(rows)) return null;

    const stepMap = new Map();
    for (const r of rows) {
      if (!r || typeof r.hex !== "string" || !r.hex.startsWith("X'")) continue;
      const buf = Buffer.from(r.hex.slice(2, -1), "hex");
      const info = extractAntigravityGenInfo(buf);
      if (
        info &&
        info.lastStepIndex != null &&
        (info.hasUsageMetadata || info.contextTokens > 0)
      ) {
        stepMap.set(info.lastStepIndex + 1, info);
      }
    }
    // An empty map means the database was read successfully but has no usable
    // metadata yet. Keep it distinct from null, which signals a transient read
    // failure and must be retried on the next sync.
    return stepMap;
  } catch (_) {
    return null;
  } finally {
    if (snapshot) snapshot.cleanup();
  }
}

async function parseAntigravityFile({
  filePath,
  rawBuffer = null,
  lastLine = 0,
  maxLine = null,
  baselineLine = 0,
  watermarkLine = 0,
  applyBuckets = true,
  initialContextTokens,
  initialPrevContext,
  initialModel,
  initialLastPlannerModel,
  initialUsageSource,
  hourlyState,
  touchedBuckets,
  source,
  projectState,
  projectTouchedBuckets,
  projectRef,
  projectKey,
}) {
  const raw = Buffer.isBuffer(rawBuffer)
    ? rawBuffer.toString("utf8")
    : await fs.readFile(filePath, "utf8").catch(() => null);
  if (raw === null) {
    throw new Error(`Antigravity transcript could not be read: ${filePath}`);
  }
  if (!raw.trim()) {
    return {
      lastLine: 0,
      eventsAggregated: 0,
      contextTokens: 0,
      previousContextTokens: 0,
      currentModel: null,
      lastPlannerModel: null,
      usageSource: "estimated",
      dbReadOk: true,
      contributions: {},
    };
  }

  const allLines = raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  const endLimit =
    Number.isFinite(maxLine) && maxLine >= 0
      ? Math.min(allLines.length, maxLine)
      : allLines.length;
  const lines = allLines.slice(0, endLimit);
  let eventsAggregated = 0;
  const contributions = {};
  const dbPath = resolveAntigravityDbPath(filePath);
  const stepMap = dbPath ? readAntigravityConversationDb(dbPath) : null;
  // Resume cached context-token total + model so historical lines (i < lastLine)
  // don't need to be re-tokenized on every sync. Falls back to a full re-walk
  // when the cached state is missing (legacy cursor) or the file rotated.
  const canResume =
    Number.isFinite(lastLine) && lastLine > 0 && lastLine <= lines.length;
  const cachedTokens = Number.isFinite(initialContextTokens) ? initialContextTokens : 0;
  const cachedPrev = Number.isFinite(initialPrevContext) ? initialPrevContext : 0;
  const cachedModel = typeof initialModel === "string" ? initialModel : null;
  const dbReadFailed = Boolean(dbPath) && stepMap === null;
  if (dbReadFailed && initialUsageSource === "sqlite") {
    return {
      lastLine: Math.min(Number.isFinite(lastLine) ? lastLine : 0, lines.length),
      eventsAggregated: 0,
      contextTokens: cachedTokens,
      previousContextTokens: cachedPrev,
      currentModel: cachedModel,
      lastPlannerModel:
        typeof initialLastPlannerModel === "string" ? initialLastPlannerModel : cachedModel,
      usageSource: "sqlite",
      dbReadOk: false,
      deferred: true,
      contributions: {},
    };
  }
  const sqliteCursor = initialUsageSource === "sqlite";
  const hasSqliteMetadata = Boolean(stepMap && stepMap.size > 0);
  const resumed =
    canResume &&
    (cachedTokens > 0 || cachedModel !== null) &&
    (!hasSqliteMetadata || sqliteCursor);
  const scanStart = resumed ? lastLine : 0;
  let currentModel = resumed ? cachedModel : typeof initialModel === "string" ? initialModel : null;
  if (!currentModel) {
    currentModel = await readAntigravityDefaultModel(filePath);
  }
  let contextTokens = resumed ? cachedTokens : 0;
  // Snapshot of contextTokens at the last PLANNER_RESPONSE we billed for. Only
  // tokens accumulated AFTER that point count as new input on the next planner
  // call — prevents O(N²) double-counting of the full history every turn.
  let previousContextTokens = resumed ? cachedPrev : 0;
  let lastPlannerModel = null;
  if (resumed) {
    lastPlannerModel =
      typeof initialLastPlannerModel === "string" ? initialLastPlannerModel : cachedModel;
  }
  let lastCompletedLine = Math.min(Number.isFinite(lastLine) ? lastLine : 0, lines.length);

  for (let i = scanStart; i < lines.length; i++) {
    const line = lines[i];

    let parsed;
    try {
      parsed = JSON.parse(line);
    } catch (_e) {
      if (i >= lastLine) break;
      continue;
    }

    const isNewEvent = i >= lastLine;

    if (parsed.type === "USER_INPUT" || parsed.type === "USER_SETTINGS_CHANGE") {
      const content = typeof parsed.content === "string" ? parsed.content : "";
      const model = parseAntigravityModelSelection(content);
      if (model) currentModel = model;
    }

    const eventContextTokens = antigravityContextTokens(parsed);
    const dbTurn =
      parsed.type === "PLANNER_RESPONSE" && stepMap && Number.isFinite(parsed.step_index)
        ? stepMap.get(parsed.step_index)
        : null;
    const dbContextTokens = dbTurn && dbTurn.contextTokens > 0 ? dbTurn.contextTokens : 0;
    if (dbTurn && dbTurn.model) {
      const norm = normalizeAntigravityTranscriptModel(dbTurn.model);
      if (norm) currentModel = norm;
    }

    if (!isNewEvent) {
      if (parsed.type === "PLANNER_RESPONSE") {
        if (dbTurn?.hasUsageMetadata) {
          contextTokens =
            dbContextTokens > 0
              ? dbContextTokens
              : (dbTurn.uncachedInput || 0) + (dbTurn.cachedInput || 0);
        } else if (dbContextTokens > 0) {
          contextTokens = dbContextTokens;
        }
        previousContextTokens = contextTokens;
        lastPlannerModel = currentModel;
        contextTokens += eventContextTokens;
      } else {
        contextTokens += eventContextTokens;
      }
      lastCompletedLine = i + 1;
      continue;
    }

    const timestamp = parsed.created_at;
    if (!timestamp) {
      contextTokens += eventContextTokens;
      lastCompletedLine = i + 1;
      continue;
    }

    const bucketStart = toUtcHalfHourStart(timestamp);
    if (!bucketStart) {
      contextTokens += eventContextTokens;
      lastCompletedLine = i + 1;
      continue;
    }

    let model = currentModel || "antigravity-unknown";
    let delta = initTotals();
    let billedPlanner = false;

    if (parsed.type === "PLANNER_RESPONSE") {
      if (dbTurn?.hasUsageMetadata) {
        const uncached = dbTurn.uncachedInput || 0;
        const cached = dbTurn.cachedInput || 0;
        const reasoning = dbTurn.reasoningOutput || 0;
        let output = 0;
        if (Number.isFinite(dbTurn.textOutput) && dbTurn.textOutput > 0) {
          output = dbTurn.textOutput;
        } else if (Number.isFinite(dbTurn.outputTokens)) {
          output = Math.max(0, dbTurn.outputTokens - reasoning);
        }

        delta.input_tokens = uncached;
        delta.cached_input_tokens = cached;
        delta.output_tokens = output;
        delta.reasoning_output_tokens = reasoning;
        delta.total_tokens = uncached + cached + output + reasoning;
        delta.billable_total_tokens = delta.total_tokens;
        delta.conversation_count = 1;
        billedPlanner = delta.total_tokens > 0;

        contextTokens = dbContextTokens > 0 ? dbContextTokens : uncached + cached;
      } else {
        const content = typeof parsed.content === "string" ? parsed.content : "";
        const thinking = typeof parsed.thinking === "string" ? parsed.thinking : "";

        if (dbContextTokens > 0) {
          contextTokens = dbContextTokens;
        }
        if (lastPlannerModel && model !== lastPlannerModel) {
          previousContextTokens = 0;
        }
        const inputDelta = Math.max(0, contextTokens - previousContextTokens);

        const outputTokens =
          antigravityValueTokens(content) + antigravityValueTokens(parsed.tool_calls);
        const reasoningTokens = antigravityValueTokens(thinking);

        delta.input_tokens = inputDelta;
        delta.output_tokens = outputTokens;
        delta.reasoning_output_tokens = reasoningTokens;
        delta.total_tokens = inputDelta + outputTokens + reasoningTokens;
        delta.billable_total_tokens = delta.total_tokens;
        delta.conversation_count = 1;
        billedPlanner = delta.total_tokens > 0;
      }
    }

    if (!billedPlanner || i < baselineLine) {
      if (billedPlanner) {
        previousContextTokens = contextTokens;
        lastPlannerModel = model;
      }
      contextTokens += eventContextTokens;
      lastCompletedLine = i + 1;
      continue;
    }

    const cKey = `${source}|${model}|${bucketStart}|${projectKey || ""}`;
    if (!contributions[cKey]) {
      contributions[cKey] = {
        source,
        model,
        bucketStart,
        projectKey: projectKey || null,
        projectRef: projectRef || null,
        totals: initTotals(),
      };
    }
    addTotals(contributions[cKey].totals, delta);

    if (applyBuckets !== false && hourlyState && touchedBuckets) {
      const bucket = getHourlyBucket(hourlyState, source, model, bucketStart);
      addTotals(bucket.totals, delta);
      touchedBuckets.add(bucketKey(source, model, bucketStart));

      if (projectKey && projectState && projectTouchedBuckets) {
        const projectBucket = getProjectBucket(
          projectState,
          projectKey,
          source,
          bucketStart,
          projectRef,
        );
        addTotals(projectBucket.totals, delta);
        projectTouchedBuckets.add(projectBucketKey(projectKey, source, bucketStart));
      }
    }
    const isNewForCounter = watermarkLine > 0 ? i >= watermarkLine : true;
    if (isNewForCounter) {
      eventsAggregated += 1;
    }
    // Snapshot the pre-planner context first. The planner's own content+tool_calls
    // (eventContextTokens, added below) become part of the next turn's history,
    // so they MUST be billed as input on the next planner — don't fold them into
    // previousContextTokens or that history vanishes from the totals.
    previousContextTokens = contextTokens;
    lastPlannerModel = model;
    contextTokens += eventContextTokens;
    lastCompletedLine = i + 1;
  }

  return {
    lastLine: lastCompletedLine,
    eventsAggregated,
    contextTokens,
    previousContextTokens,
    currentModel,
    lastPlannerModel,
    usageSource: hasSqliteMetadata ? "sqlite" : "estimated",
    dbReadOk: !dbPath || stepMap !== null,
    contributions,
  };
}

async function readAntigravityDefaultModel(filePath) {
  try {
    // filePath: …/antigravity-cli/brain/<uuid>/.system_generated/logs/transcript.jsonl
    // Go up 5 levels to reach the variant root (e.g. …/antigravity-cli/)
    let dir = filePath;
    for (let i = 0; i < 5; i++) dir = path.dirname(dir);
    const raw = await fs.readFile(path.join(dir, "settings.json"), "utf8");
    const settings = JSON.parse(raw);
    if (settings.model && typeof settings.model === "string") {
      return normalizeAntigravityTranscriptModel(settings.model);
    }
  } catch (_) {
  }
  return null;
}

function parseAntigravityModelSelection(content) {
  if (typeof content !== "string" || !content) return null;
  const match = content.match(
    /changed setting `Model Selection` from .*? to ([^`\n]+?)(?:\s*\([^)]*\))?\.(?:\s+|$)/i,
  );
  if (!match) return null;
  return normalizeAntigravityTranscriptModel(match[1]);
}

function normalizeAntigravityTranscriptModel(modelName) {
  if (!modelName || typeof modelName !== "string") return null;
  let slug = modelName
    .trim()
    .replace(/\([^)]*\)/g, " ")
    .replace(/\b(thinking|xhigh|high|medium|low|fast)\b/gi, " ")
    .toLowerCase()
    .replace(/[^a-z0-9.]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/-{2,}/g, "-");
  if (!slug) return null;

  for (const marker of ["gemini", "claude", "gpt"]) {
    const idx = slug.indexOf(marker);
    if (idx >= 0) {
      slug = slug.slice(idx);
      break;
    }
  }
  if (/^(gemini|claude|gpt)-/.test(slug)) return slug;
  return `antigravity-${slug}`;
}

function antigravityContextTokens(event) {
  if (!event || typeof event !== "object") return 0;
  let tokens = antigravityValueTokens(event.content);
  if (event.type === "PLANNER_RESPONSE" && event.tool_calls) {
    tokens += antigravityValueTokens(event.tool_calls);
  }
  return tokens;
}

function antigravityValueTokens(value) {
  if (typeof value === "string") return estimateAntigravityTokens(value);
  if (value == null) return 0;
  try {
    return estimateAntigravityTokens(JSON.stringify(value));
  } catch (_e) {
    return 0;
  }
}

function estimateAntigravityTokens(text) {
  if (typeof text !== "string" || text.length === 0) return 0;
  let cjk = 0;
  let other = 0;
  for (const ch of text) {
    if (isCjkCodePoint(ch.codePointAt(0))) {
      cjk += 1;
    } else {
      other += 1;
    }
  }
  return cjk + Math.ceil(other / 4);
}

function isCjkCodePoint(code) {
  return (
    (code >= 0x3400 && code <= 0x4dbf) ||
    (code >= 0x4e00 && code <= 0x9fff) ||
    (code >= 0x3040 && code <= 0x30ff)
  );
}

module.exports = {
  resolveAntigravityBrainDirs,
  listAntigravitySessionFiles,
  listAntigravityTranscriptsWithStatus,
  listAntigravityTranscripts,
  ANTIGRAVITY_CURSOR_VERSION,
  ANTIGRAVITY_EXTRACTOR_REVISION,
  parseAntigravityIncremental,
  extractAntigravityGenInfo,
  resolveAntigravityDbPath,
  readAntigravityConversationDb,
  estimateAntigravityTokens,
  isCjkCodePoint,
};
