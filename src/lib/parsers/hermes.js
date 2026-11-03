// hermes usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { ensureDir } = require("../fs");
const { readSqliteJsonRows } = require("../sqlite-reader");
const wsl = require("../wsl-probe");
const { resolveInstallPaths } = require("../install-resolver");
const {
  enqueueTouchedBuckets,
  normalizeHourlyState,
  getHourlyBucket,
  addTotals,
  toUtcHalfHourStart,
  bucketKey,
  normalizeModelInput,
  toNonNegativeInt,
  sqliteStringLiteral,
  isUncPath,
  snapshotSqliteDb,
  sqliteDbContainsIds,
  sampleRecentKeys,
} = require("./core");

// ─────────────────────────────────────────────────────────────────────────────
// Hermes Agent — SQLite-based (sessions table in ~/.hermes/state.db)
// ─────────────────────────────────────────────────────────────────────────────

function resolveHermesPath(env = process.env, deps = {}) {
  const override = env.TOKEN_PULSE_HERMES_HOME;
  if (typeof override === "string" && override.trim().length > 0) {
    return override.trim();
  }
  const home = os.homedir();
  const defaultPath = path.join(home, ".hermes");
  if (process.platform === "win32") {
    const localAppData = typeof env.LOCALAPPDATA === "string" ? env.LOCALAPPDATA.trim() : "";
    const nativeValue = localAppData.length > 0 ? path.join(localAppData, "hermes") : null;
    const paths = resolveInstallPaths({ nativeValue, wslDir: ".hermes" }, env, deps);
    const picked = paths.native || paths.wsl;
    if (picked) return picked;
    const mode = wsl.getWslMode(env);
    if (mode === "wsl-only" || mode === "native-only") return null;
  }
  return defaultPath;
}

// Parse `wsl.exe -l -v` output into [{ name, version, isDefault }]. The default
// distro is prefixed with `*`; the VERSION column (1 or 2) decides which UNC
// alias to try first (simonlpaige, #87).
function parseWslListVerbose(raw) {
  return wsl.parseWslListVerbose(raw);
}

// Default distro first, then listed order — matches what a user expects when
// they have one primary distro plus extras.
function probeWslDistros(deps = {}) {
  return wsl.probeWslDistros(deps);
}

// Probe each WSL distro for ~/.hermes via UNC. The Linux username rarely equals
// %USERNAME%, so we ask the distro directly with `whoami` rather than guessing
// (simonlpaige, #87).
function discoverWslHermesHome(deps = {}) {
  return wsl.discoverWslHome(".hermes", deps);
}

function resolveHermesDbPath(env = process.env) {
  const hermesPath = resolveHermesPath(env);
  return hermesPath ? path.join(hermesPath, "state.db") : null;
}

function resolveAllHermesDBPaths({ hermesPath, dbPath } = {}) {
  const hermesDir = hermesPath ?? (dbPath ? path.dirname(dbPath) : resolveHermesPath());
  if (!hermesDir) return { default: null, profiles: {} };
  const defaultDbPath = dbPath ?? path.join(hermesDir, "state.db");
  const profilePaths = {};
  try {
    const profilesDir = path.join(hermesDir, "profiles");
    const profiles = fssync.readdirSync(profilesDir, { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .sort((a, b) => a.name.localeCompare(b.name));

    for (const entry of profiles) {
      const dbPath = path.join(profilesDir, entry.name, "state.db");
      if (fssync.existsSync(dbPath)) {
        profilePaths[entry.name] = dbPath;
      }
    }
  } catch (_e) { }

  return {
    default: fssync.existsSync(defaultDbPath) ? defaultDbPath : null,
    profiles: profilePaths,
  }
}

function readHermesSessions(dbPath, lastCompletedEpoch, unfinishedSessionIds = [], sqliteOptions = {}) {
  if (!dbPath || !fssync.existsSync(dbPath)) return [];
  const since = Number.isFinite(lastCompletedEpoch) && lastCompletedEpoch > 0 ? lastCompletedEpoch : 0;
  const forceIds = Array.isArray(unfinishedSessionIds)
    ? [...new Set(unfinishedSessionIds.filter((id) => typeof id === "string" && id.length > 0))]
    : [];
  const forceIncludeSql = forceIds.length > 0
    ? ` OR id IN (${forceIds.map(sqliteStringLiteral).join(",")})`
    : "";
  // Fetch sessions that started at/after the cursor, sessions that are still
  // in-progress (ended_at IS NULL), OR sessions that were previously observed
  // unfinished.  Hermes updates token counts in real-time, including a final
  // delta when an active session later gets ended_at set.
  const sql = `SELECT id, model, started_at, ended_at, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, reasoning_tokens, message_count FROM sessions WHERE (started_at >= ${since} OR ended_at IS NULL${forceIncludeSql}) AND (input_tokens > 0 OR output_tokens > 0 OR cache_read_tokens > 0 OR cache_write_tokens > 0 OR reasoning_tokens > 0) ORDER BY started_at ASC`;

  let snapshot = null;
  let effectiveDbPath = dbPath;
  if (isUncPath(dbPath)) {
    try {
      snapshot = snapshotSqliteDb(dbPath);
      effectiveDbPath = snapshot.path;
    } catch (_e) {
      // Snapshot failed — fall through to a direct read so we don't regress
      // the non-locked case (e.g. permissions, transient I/O).
    }
  }

  try {
    return readSqliteJsonRows(effectiveDbPath, sql, {
      label: "Hermes",
      maxBuffer: 10 * 1024 * 1024,
      timeout: 15_000,
      ...sqliteOptions,
    });
  } finally {
    if (snapshot) snapshot.cleanup();
  }
}

function hermesInstallOwnsCursor(hermesPath, flatState, sqliteOptions) {
  const ids = new Set();
  const collect = (state) => {
    if (!state || typeof state !== "object") return;
    for (const key of sampleRecentKeys(state.snapshots)) ids.add(key);
    if (Array.isArray(state.unfinishedSessionIds)) {
      for (const id of state.unfinishedSessionIds) {
        if (typeof id === "string" && id.length > 0) ids.add(id);
      }
    }
  };
  collect(flatState);
  if (flatState?.profiles && typeof flatState.profiles === "object") {
    for (const profileState of Object.values(flatState.profiles)) collect(profileState);
  }
  const sample = [...ids].slice(-16);
  if (sample.length === 0) return false;

  const dbPaths = resolveAllHermesDBPaths({ hermesPath });
  const allDbs = [dbPaths.default, ...Object.values(dbPaths.profiles)].filter(Boolean);
  return allDbs.some((db) => sqliteDbContainsIds(db, "sessions", sample, sqliteOptions));
}

async function parseHermesIncremental({ hermesPath, dbPath, cursors, queuePath, onProgress, sqliteOptions } = {}) {
  await ensureDir(path.dirname(queuePath));
  const hermesState = cursors.hermes && typeof cursors.hermes === "object" ? cursors.hermes : {};

  const dbPaths = resolveAllHermesDBPaths({ hermesPath, dbPath });
  if (dbPaths.default === null && Object.keys(dbPaths.profiles).length === 0) {
    // No state in any profile
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const cb = typeof onProgress === "function" ? onProgress : null;
  const updatedAt = new Date().toISOString();
  let recordsProcessed = 0;
  let eventsAggregated = 0;
  const touchedBuckets = new Set();

  function ingestProfile(dbPath, dbState) {
    const trackedUnfinishedSessionIds = Array.isArray(dbState.unfinishedSessionIds)
      ? dbState.unfinishedSessionIds
      : [];
    const rows = readHermesSessions(
      dbPath,
      dbState.lastCompletedStartedAt,
      trackedUnfinishedSessionIds,
      sqliteOptions,
    );
    recordsProcessed += rows.length;
    if (rows.length === 0) {
      dbState.updatedAt = updatedAt;
      return;
    }

    // Per-session snapshot from the previous sync: { [sessionId]: { in, out, cacheRead, cacheWrite, reasoning } }
    const prevSnapshots = (dbState.snapshots && typeof dbState.snapshots === "object")
      ? dbState.snapshots : {};

    // Only advance past sessions that have fully ended.  Active sessions
    // (ended_at IS NULL) must be re-read every sync because Hermes updates
    // their token counts in real-time after each turn.
    const lastCompletedStartedAt =
      typeof dbState.lastCompletedStartedAt === "number" ? dbState.lastCompletedStartedAt : 0;

    let maxCompletedStartedAt = lastCompletedStartedAt;
    let oldestUnfinishedStartedAt = Infinity;
    const nextUnfinishedSessionIds = new Set();
    const nextSnapshots = {};

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const inputTokens = toNonNegativeInt(row.input_tokens);
      const outputTokens = toNonNegativeInt(row.output_tokens);
      const cacheRead = toNonNegativeInt(row.cache_read_tokens);
      const cacheWrite = toNonNegativeInt(row.cache_write_tokens);
      const reasoning = toNonNegativeInt(row.reasoning_tokens);
      const messageCount = toNonNegativeInt(row.message_count);
      if (
        inputTokens === 0 &&
        outputTokens === 0 &&
        cacheRead === 0 &&
        cacheWrite === 0 &&
        reasoning === 0
      ) continue;

      // Save current snapshot for next sync
      nextSnapshots[row.id] = { in: inputTokens, out: outputTokens, cacheRead, cacheWrite, reasoning, message_count: messageCount };

      const startedAt = Number(row.started_at);
      const endedAt = row.ended_at == null ? null : Number(row.ended_at);
      if (endedAt == null) {
        if (row.id && Number.isFinite(startedAt)) {
          nextUnfinishedSessionIds.add(row.id);
          oldestUnfinishedStartedAt = Math.min(oldestUnfinishedStartedAt, startedAt);
        }
      } else if (Number.isFinite(startedAt) && startedAt > maxCompletedStartedAt) {
        maxCompletedStartedAt = startedAt;
      }

      // Compute delta from previous snapshot (if any) so that we only count
      // new usage since the last sync.  First time we see a session the
      // previous snapshot is absent, so the full amount is the delta.
      const prev = prevSnapshots[row.id];
      let dInput = inputTokens;
      let dOutput = outputTokens;
      let dCacheRead = cacheRead;
      let dCacheWrite = cacheWrite;
      let dReasoning = reasoning;
      let dMessageCount = messageCount;
      if (prev) {
        dInput = Math.max(0, inputTokens - (prev.in || 0));
        dOutput = Math.max(0, outputTokens - (prev.out || 0));
        dCacheRead = Math.max(0, cacheRead - (prev.cacheRead || 0));
        dCacheWrite = Math.max(0, cacheWrite - (prev.cacheWrite || 0));
        dReasoning = Math.max(0, reasoning - (prev.reasoning || 0));
        dMessageCount = Math.max(0, messageCount - (prev.message_count || 0));
      }
      // Skip if delta is zero (session unchanged since last sync)
      if (dInput === 0 && dOutput === 0 && dCacheRead === 0 && dCacheWrite === 0 && dReasoning === 0) continue;

      // Prefer ended_at for bucket placement; fall back to started_at
      const epochSec = endedAt ?? startedAt;
      if (!epochSec || !Number.isFinite(epochSec)) continue;
      const tsIso = new Date(epochSec * 1000).toISOString();
      const bucketStart = toUtcHalfHourStart(tsIso);
      if (!bucketStart) continue;

      const model = normalizeModelInput(row.model) || "hermes-agent";

      const delta = {
        input_tokens: dInput,
        cached_input_tokens: dCacheRead,
        cache_creation_input_tokens: dCacheWrite,
        output_tokens: dOutput,
        reasoning_output_tokens: dReasoning,
        total_tokens: dInput + dOutput + dCacheRead + dCacheWrite + dReasoning,
        conversation_count: dMessageCount,
      };

      const bucket = getHourlyBucket(hourlyState, "hermes", model, bucketStart);
      addTotals(bucket.totals, delta);
      touchedBuckets.add(bucketKey("hermes", model, bucketStart));
      eventsAggregated++;

      if (cb) {
        cb({
          index: i + 1,
          total: rows.length,
          recordsProcessed: i + 1,
          eventsAggregated,
          bucketsQueued: touchedBuckets.size,
        });
      }
    }

    const nextLastCompletedStartedAt = Number.isFinite(oldestUnfinishedStartedAt)
      ? Math.min(maxCompletedStartedAt, oldestUnfinishedStartedAt)
      : maxCompletedStartedAt;

    Object.assign(dbState, {
      lastStartedAt: nextLastCompletedStartedAt,
      lastCompletedStartedAt: nextLastCompletedStartedAt,
      unfinishedSessionIds: Array.from(nextUnfinishedSessionIds),
      snapshots: nextSnapshots,
      updatedAt,
    });
  }

  if (dbPaths.default) {
    ingestProfile(dbPaths.default, hermesState);
  }

  hermesState.profiles = hermesState.profiles && typeof hermesState.profiles === "object" ? hermesState.profiles : {};

  for (const [profileName, dbPath] of Object.entries(dbPaths.profiles)) {
    const profileState = hermesState.profiles[profileName] && typeof hermesState.profiles[profileName] === "object"
      ? hermesState.profiles[profileName]
      : {};
    hermesState.profiles[profileName] = profileState;
    ingestProfile(dbPath, profileState);
  }

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.hermes = {
    ...hermesState,
    updatedAt, // Update the overall profile state timestamp even if the DB doesn't exist for the fast-path check
  };

  return {
    recordsProcessed,
    eventsAggregated,
    bucketsQueued,
  };
}

module.exports = {
  resolveHermesPath,
  parseWslListVerbose,
  probeWslDistros,
  discoverWslHermesHome,
  resolveHermesDbPath,
  hermesInstallOwnsCursor,
  parseHermesIncremental,
};
