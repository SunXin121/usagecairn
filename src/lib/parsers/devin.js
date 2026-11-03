// devin usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { ensureDir } = require("../fs");
const { readSqliteJsonRowsAsync } = require("../sqlite-reader");
const wsl = require("../wsl-probe");
const { resolveInstallPaths } = require("../install-resolver");
const { DEVIN_TABLE_PROBE_SQL, devinUsageSql, buildDevinUsageEvents } = require("../devin-usage");
const {
  DEFAULT_SOURCE,
  BUCKET_SEPARATOR,
  enqueueTouchedBuckets,
  enqueueTouchedProjectBuckets,
  normalizeHourlyState,
  normalizeProjectState,
  getHourlyBucket,
  getProjectBucket,
  addTotals,
  subtractTotals,
  totalsKey,
  toUtcHalfHourStart,
  bucketKey,
  projectBucketKey,
  parseBucketKey,
  normalizeSourceInput,
  resolveProjectContextForPath,
  isUncPath,
  snapshotSqliteDb,
  devinSqliteFingerprint,
  stageProviderBuckets,
  sameSqliteFingerprint,
} = require("./core");

// ─────────────────────────────────────────────────────────────────────────────
// Devin (Cognition — devin.ai CLI)
//
// Data: SQLite at
//   macOS/Linux: $XDG_DATA_HOME/devin/cli/sessions.db (~/.local/share)
//   Windows:     no evidenced native location; the CLI's XDG data dir inside a
//                WSL distro is probed via the \\wsl$ UNC bridge
//   Override:    $TOKEN_PULSE_DEVIN_DB
//
// Devin rewrites history aggressively: replay, compaction and forks copy
// message_nodes rows, so several nodes share one chat_message
// `.metadata.request_id` (verified on CLI 3000.10.21: ~2 duplicated nodes per
// request). request_id is the billing identity — copies must not add usage.
// Aggregation is a full projection + per-request reconcile keyed by
// request_id: every fingerprint change re-reads the usage-only rows, the
// ledger in cursors.devin.requests subtracts a request's prior contribution
// before adding its current one, and deleted/compacted history keeps its
// (non-refunded) ledger entry. There is no row_id high-water mark because
// in-place metric corrections cannot be detected by one.
//
// Attribution recorded at first observation is authoritative: a request's
// owning session, resolved project and conversation share live in the ledger,
// so deleting the original node while a fork copy survives never migrates its
// spend or refunds its conversation. A fork made only of copies pays nothing;
// the first genuinely new request in a session pays its single
// conversation_count once — the set of sessions that already paid is derived
// from the ledger entries themselves, not persisted separately.
// ─────────────────────────────────────────────────────────────────────────────

const DEVIN_SOURCE = "devin";

function resolveDevinDbPath(env = process.env, deps = {}) {
  const override =
    typeof env.TOKEN_PULSE_DEVIN_DB === "string" && env.TOKEN_PULSE_DEVIN_DB.trim();
  if (override) return path.resolve(env.TOKEN_PULSE_DEVIN_DB.trim());
  const home = env.HOME || env.USERPROFILE || os.homedir();
  const xdgDataHome =
    typeof env.XDG_DATA_HOME === "string" && env.XDG_DATA_HOME.trim()
      ? path.resolve(env.XDG_DATA_HOME.trim())
      : path.join(home, ".local", "share");
  if (process.platform !== "win32") {
    return path.join(xdgDataHome, "devin", "cli", "sessions.db");
  }
  // Devin CLI is not known to write a native-Windows data dir; on Windows its
  // sessions.db lives under the distro's XDG home, reachable over \\wsl$.
  const wslDir = wsl.shouldProbeWsl(env)
    ? wsl.discoverWslHome(".local/share/devin/cli", { ...deps, env })
    : null;
  const wslValue = wslDir ? path.join(wslDir, "sessions.db") : null;
  const paths = resolveInstallPaths({ nativeValue: null, wslValue }, env, deps);
  return paths.native || paths.wsl;
}

// Request ids and conversation keys are untrusted strings — normalize the
// persisted maps into null-prototype dictionaries so a literal "__proto__"
// key stays an own entry that round-trips through cursors.json instead of
// silently mutating the prototype chain (and re-adding that request's usage
// on every rescan).
function devinStringMap(value) {
  const dict = Object.create(null);
  if (value && typeof value === "object" && !Array.isArray(value)) {
    for (const key of Object.keys(value)) dict[key] = value[key];
  }
  return dict;
}

async function readDevinUsageRows(dbPath, sqliteOptions = {}) {
  if (!dbPath || !fssync.existsSync(dbPath)) return [];
  const options = {
    label: "Devin",
    maxBuffer: 32 * 1024 * 1024,
    timeout: 30_000,
    readOnly: true,
    throwOnReadFailure: true,
    ...sqliteOptions,
  };
  let snapshot = null;
  let effectiveDbPath = dbPath;
  if (isUncPath(dbPath)) {
    try {
      snapshot = snapshotSqliteDb(dbPath);
      effectiveDbPath = snapshot.path;
    } catch (_e) { }
  }
  try {
    const tables = new Set(
      (await readSqliteJsonRowsAsync(effectiveDbPath, DEVIN_TABLE_PROBE_SQL, options))
        .map((row) => row?.name)
        .filter(Boolean),
    );
    if (!tables.has("message_nodes")) return [];
    return await readSqliteJsonRowsAsync(
      effectiveDbPath,
      devinUsageSql({ hasSessionsTable: tables.has("sessions") }),
      options,
    );
  } finally {
    if (snapshot) snapshot.cleanup();
  }
}

async function parseDevinIncremental({
  dbPath,
  cursors,
  queuePath,
  projectQueuePath,
  onProgress,
  env,
  sqliteOptions,
  publicRepoResolver,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const resolvedDb = dbPath || resolveDevinDbPath(env || process.env);
  const priorState =
    cursors.devin && typeof cursors.devin === "object" ? cursors.devin : {};
  const requests = devinStringMap(priorState.requests);
  if (!resolvedDb || !fssync.existsSync(resolvedDb)) {
    const nextState = { ...priorState, requests, updatedAt: new Date().toISOString() };
    delete nextState.conversations;
    cursors.devin = nextState;
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0, projectBucketsQueued: 0 };
  }

  // Cheap unchanged check: skip all SQL work when neither the DB nor its WAL
  // moved since the last published state.
  const initialFingerprint = devinSqliteFingerprint(resolvedDb);
  if (sameSqliteFingerprint(initialFingerprint, priorState.fingerprint)) {
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0, projectBucketsQueued: 0 };
  }

  const rows = await readDevinUsageRows(resolvedDb, sqliteOptions);
  const { events } = buildDevinUsageEvents(rows);

  // Stage the normalized working states: bucket-map copies alias the
  // caller's published bucket objects, so give only the devin-owned entries
  // (plus the flat groupQueued map) private copies. Reconciliation and
  // enqueue mutations then land on staged state and are published only after
  // both queue appends below succeed — a failed append leaves `cursors`
  // untouched so a retry re-derives the same contribution and latest-wins
  // rows recover either queue. Unrelated providers' buckets stay shared
  // read-only references (this parser never writes their keys).
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  hourlyState.buckets = stageProviderBuckets(
    hourlyState.buckets,
    (key) =>
      (normalizeSourceInput(parseBucketKey(key).source) || DEFAULT_SOURCE) ===
      DEVIN_SOURCE,
  );
  hourlyState.groupQueued =
    hourlyState.groupQueued && typeof hourlyState.groupQueued === "object"
      ? { ...hourlyState.groupQueued }
      : {};
  const touchedBuckets = new Set();
  const projectEnabled =
    typeof projectQueuePath === "string" && projectQueuePath.length > 0;
  const projectState = projectEnabled
    ? normalizeProjectState(cursors?.projectHourly)
    : null;
  if (projectState) {
    // Project bucket keys are `projectKey|source|hourStart`; this parser only
    // ever looks up keys whose middle segment is the devin source.
    projectState.buckets = stageProviderBuckets(projectState.buckets, (key) => {
      const last = key.lastIndexOf(BUCKET_SEPARATOR);
      const prev = last > 0 ? key.lastIndexOf(BUCKET_SEPARATOR, last - 1) : -1;
      const source = prev >= 0 ? key.slice(prev + 1, last) : "";
      return (
        (normalizeSourceInput(source) || DEFAULT_SOURCE) === DEVIN_SOURCE
      );
    });
  }
  const projectTouchedBuckets = projectEnabled ? new Set() : null;
  const projectMetaCache = projectEnabled ? new Map() : null;
  const publicRepoCache = projectEnabled ? new Map() : null;
  const projectContextBySession = projectEnabled ? new Map() : null;
  const cb = typeof onProgress === "function" ? onProgress : null;
  let eventsAggregated = 0;

  // The counted-conversation index is derived from the retained ledger: the
  // entry that paid a conversation's +1 keeps that share in its own totals,
  // even after every copy of the request vanished — no second persisted map.
  const countedConversations = new Set();
  for (const [requestId, entry] of Object.entries(requests)) {
    const totals = entry && typeof entry === "object" ? entry.totals : null;
    if (totals && Number.isSafeInteger(totals.conversation_count) && totals.conversation_count >= 1) {
      countedConversations.add(
        typeof entry.sessionId === "string" && entry.sessionId
          ? entry.sessionId
          : `request:${requestId}`,
      );
    }
  }

  for (let index = 0; index < events.length; index++) {
    const event = events[index];
    const bucketStart = toUtcHalfHourStart(new Date(event.tsMs).toISOString());
    if (!bucketStart) continue;

    const previous = requests[event.requestId];
    const previousTotals =
      previous?.totals && typeof previous.totals === "object" ? previous.totals : null;

    // Ownership recorded at first observation is authoritative. For a known
    // request the ledger's session/project/conversation share survive copy
    // deletion and fork timelines; for a new request the canonical retained
    // record supplies them exactly once.
    let projectKey = previous ? previous.projectKey || null : null;
    let projectRef = previous ? previous.projectRef || null : null;
    if (previous) {
      const priorConv = previousTotals ? previousTotals.conversation_count : 0;
      event.totals.conversation_count =
        Number.isSafeInteger(priorConv) && priorConv >= 0 ? priorConv : 0;
    } else {
      if (projectEnabled && event.sessionId) {
        let context = projectContextBySession.get(event.sessionId);
        if (context === undefined) {
          const startDir = event.workingDirectory
            ? wsl.mapWslCwdToUnc(event.workingDirectory, resolvedDb)
            : null;
          context = startDir
            ? await resolveProjectContextForPath({
                startDir,
                projectMetaCache,
                publicRepoCache,
                publicRepoResolver,
                projectState,
              })
            : null;
          projectContextBySession.set(event.sessionId, context || null);
        }
        projectKey = context?.projectKey || null;
        projectRef = context?.projectRef || null;
      }
      // A fork made only of copied requests pays no conversation of its own;
      // the first genuinely new request in a conversation pays it once.
      const convKey = event.sessionId || `request:${event.requestId}`;
      if (countedConversations.has(convKey)) {
        event.totals.conversation_count = 0;
      } else {
        event.totals.conversation_count = 1;
        countedConversations.add(convKey);
      }
    }

    const unchanged =
      previousTotals &&
      totalsKey(previousTotals) === totalsKey(event.totals) &&
      previous.bucketStart === bucketStart &&
      previous.model === event.model;
    if (!unchanged) {
      if (previousTotals && previous.bucketStart && previous.model) {
        const oldBucket = getHourlyBucket(
          hourlyState,
          DEVIN_SOURCE,
          previous.model,
          previous.bucketStart,
        );
        subtractTotals(oldBucket.totals, previousTotals);
        touchedBuckets.add(
          bucketKey(DEVIN_SOURCE, previous.model, previous.bucketStart),
        );
        if (projectEnabled && previous.projectKey) {
          const oldProjectBucket = getProjectBucket(
            projectState,
            previous.projectKey,
            DEVIN_SOURCE,
            previous.bucketStart,
            previous.projectRef || null,
          );
          subtractTotals(oldProjectBucket.totals, previousTotals);
          projectTouchedBuckets.add(
            projectBucketKey(previous.projectKey, DEVIN_SOURCE, previous.bucketStart),
          );
        }
      }

      const bucket = getHourlyBucket(hourlyState, DEVIN_SOURCE, event.model, bucketStart);
      addTotals(bucket.totals, event.totals);
      touchedBuckets.add(bucketKey(DEVIN_SOURCE, event.model, bucketStart));
      if (projectEnabled && projectKey) {
        const projectBucket = getProjectBucket(
          projectState,
          projectKey,
          DEVIN_SOURCE,
          bucketStart,
          projectRef,
        );
        addTotals(projectBucket.totals, event.totals);
        projectTouchedBuckets.add(
          projectBucketKey(projectKey, DEVIN_SOURCE, bucketStart),
        );
      }
      // The ledger records only what was added: owning session, model,
      // bucket, totals and the resolved project identity — never request text
      // or raw working paths.
      requests[event.requestId] = {
        sessionId:
          previous && typeof previous.sessionId === "string" && previous.sessionId
            ? previous.sessionId
            : event.sessionId,
        model: event.model,
        bucketStart,
        totals: event.totals,
        projectKey,
        projectRef,
        updatedAt: new Date().toISOString(),
      };
      eventsAggregated += 1;
    }
    if (cb) {
      cb({
        index: index + 1,
        total: events.length,
        recordsProcessed: index + 1,
        eventsAggregated,
        bucketsQueued: touchedBuckets.size,
      });
    }
  }

  const bucketsQueued = await enqueueTouchedBuckets({
    queuePath,
    hourlyState,
    touchedBuckets,
  });
  const projectBucketsQueued = projectEnabled
    ? await enqueueTouchedProjectBuckets({
        projectQueuePath,
        projectState,
        projectTouchedBuckets,
      })
      : 0;
  // If Devin wrote to the DB/WAL while we read, publish the pre-read
  // fingerprint so the next sync re-reads instead of acknowledging a snapshot
  // it never saw (same convention as parseUnslothIncremental).
  const finalFingerprint = devinSqliteFingerprint(resolvedDb);
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.devin = {
    version: 1,
    requests,
    fingerprint: sameSqliteFingerprint(initialFingerprint, finalFingerprint)
      ? finalFingerprint
      : initialFingerprint,
    updatedAt,
  };
  if (projectState) {
    projectState.updatedAt = updatedAt;
    cursors.projectHourly = projectState;
  }
  return {
    recordsProcessed: rows.length,
    eventsAggregated,
    bucketsQueued,
    projectBucketsQueued,
  };
}

module.exports = {
  resolveDevinDbPath,
  readDevinUsageRows,
  parseDevinIncremental,
};
