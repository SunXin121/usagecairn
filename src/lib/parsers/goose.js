// goose usage parser and install discovery.
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
  snapshotSqliteDb,
  sqliteDbContainsIds,
  sampleRecentKeys,
} = require("./core");

function gooseInstallOwnsCursor(dbPath, flatState, sqliteOptions) {
  return sqliteDbContainsIds(dbPath, "sessions", sampleRecentKeys(flatState?.sessionTotals), sqliteOptions);
}

// ─────────────────────────────────────────────────────────────────────────────
// Goose (Block AI agent — github.com/block/goose)
//
// Data: SQLite at
//   macOS:   ~/Library/Application Support/goose/sessions/sessions.db
//   Linux:   $XDG_DATA_HOME/goose/sessions/sessions.db (~/.local/share)
//   Legacy:  ~/.local/share/Block/goose/sessions/sessions.db
//   Windows: %APPDATA%\goose\sessions\sessions.db
//   Override: $GOOSE_PATH_ROOT/data/sessions/sessions.db
//
// `sessions` table: one row per session, columns:
//   id, model_config_json ({"model_name":"..."}),
//   provider_name, created_at,
//   total_tokens / input_tokens / output_tokens (latest turn),
//   accumulated_total_tokens / accumulated_input_tokens /
//   accumulated_output_tokens (whole-session cumulative).
//
// We prefer accumulated_* (gives lifetime usage), with single-turn fallback.
// Goose has no cache fields; if total > input+output, the excess is treated
// as reasoning_output_tokens (same heuristic as tokscale).
//
// Session rows grow over time → same cumulative-delta pattern as Zed
// (cursors.goose.sessionTotals tracks last-seen per session).
// ─────────────────────────────────────────────────────────────────────────────

function resolveGooseDbPath(env = process.env) {
  if (typeof env.TOKEN_PULSE_GOOSE_DB === "string" && env.TOKEN_PULSE_GOOSE_DB.trim()) {
    return env.TOKEN_PULSE_GOOSE_DB.trim();
  }
  const root = typeof env.GOOSE_PATH_ROOT === "string" ? env.GOOSE_PATH_ROOT.trim() : "";
  if (root) return path.join(root, "data", "sessions", "sessions.db");
  const home = env.HOME || os.homedir();
  const candidates = [];
  if (process.platform === "darwin") {
    candidates.push(
      path.join(home, "Library", "Application Support", "goose", "sessions", "sessions.db"),
    );
  } else if (process.platform === "win32") {
    const appData = env.APPDATA || path.join(home, "AppData", "Roaming");
    const native = path.join(appData, "goose", "sessions", "sessions.db");
    const wslDir = wsl.shouldProbeWsl(env) ? wsl.discoverWslHome(".local/share/goose/sessions", { env }) : null;
    const wslValue = wslDir && fssync.existsSync(path.join(wslDir, "sessions.db"))
      ? path.join(wslDir, "sessions.db") : null;
    const paths = resolveInstallPaths({ nativeValue: native, wslValue }, env);
    const picked = paths.native || paths.wsl;
    if (picked) candidates.push(picked);
    for (const c of candidates) {
      if (fssync.existsSync(c)) return c;
    }
    const mode = wsl.getWslMode(env);
    return mode === "wsl-only" || mode === "native-only" ? null : native;
  }
  const xdg = env.XDG_DATA_HOME || path.join(home, ".local", "share");
  candidates.push(
    path.join(xdg, "goose", "sessions", "sessions.db"),
    path.join(xdg, "Block", "goose", "sessions", "sessions.db"),
  );
  // Default to first existing; if none, return the platform-canonical path so
  // status can report it cleanly without throwing.
  for (const c of candidates) {
    if (fssync.existsSync(c)) return c;
  }
  return candidates[0] || null;
}

function parseGooseModelName(modelConfigJson) {
  if (typeof modelConfigJson !== "string" || !modelConfigJson.trim()) return null;
  try {
    const obj = JSON.parse(modelConfigJson);
    if (obj && typeof obj.model_name === "string") {
      const trimmed = obj.model_name.trim();
      return trimmed || null;
    }
  } catch (_e) { /* ignore */ }
  return null;
}

// Goose stores created_at in multiple formats across versions: RFC3339
// (preferred), "YYYY-MM-DD HH:MM:SS" (naive UTC), or bare "YYYY-MM-DD".
// Return ISO 8601 string, or null on failure.
function parseGooseCreatedAt(s) {
  if (typeof s !== "string" || !s.trim()) return null;
  const trimmed = s.trim();
  // Match naive UTC formats FIRST — otherwise `new Date("2026-05-21 14:30:00")`
  // is interpreted in the local zone, shifting the bucket by ±N hours.
  const dt = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})$/.exec(trimmed);
  if (dt) {
    const d = new Date(Date.UTC(+dt[1], +dt[2] - 1, +dt[3], +dt[4], +dt[5], +dt[6]));
    return d.toISOString();
  }
  const dateOnly = /^(\d{4})-(\d{2})-(\d{2})$/.exec(trimmed);
  if (dateOnly) {
    const d = new Date(Date.UTC(+dateOnly[1], +dateOnly[2] - 1, +dateOnly[3]));
    return d.toISOString();
  }
  // Anything else — RFC3339, "Z"-suffixed, "+HH:MM" — let Date handle it.
  const iso = new Date(trimmed);
  if (!Number.isNaN(iso.getTime())) return iso.toISOString();
  return null;
}

function readGooseSessionsFromSqlite(dbPath, sqliteOptions = {}) {
  // Probe columns: the `accumulated_*` fields were added in a later Goose
  // version; we keep the query forgiving so older installs still work.
  const pragmaRows = readSqliteJsonRows(dbPath, "PRAGMA table_info(sessions)", {
    label: "Goose",
    maxBuffer: 4 * 1024 * 1024,
    timeout: 10_000,
    ...sqliteOptions,
  });
  const columns = new Set(
    pragmaRows
      .map((row) => row?.name)
      .filter(Boolean),
  );
  const optional = (col) => (columns.has(col) ? col : `NULL AS ${col}`);
  const sql = `
    SELECT
      id,
      model_config_json,
      ${optional("provider_name")},
      created_at,
      ${optional("total_tokens")},
      ${optional("input_tokens")},
      ${optional("output_tokens")},
      ${optional("accumulated_total_tokens")},
      ${optional("accumulated_input_tokens")},
      ${optional("accumulated_output_tokens")}
    FROM sessions
    WHERE model_config_json IS NOT NULL
      AND TRIM(model_config_json) != ''
  `.trim();
  return readSqliteJsonRows(dbPath, sql, {
    label: "Goose",
    maxBuffer: 64 * 1024 * 1024,
    timeout: 60_000,
    ...sqliteOptions,
  });
}

async function parseGooseIncremental({
  dbPath,
  cursors,
  queuePath,
  onProgress,
  env,
  sqliteOptions,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const resolvedDb = dbPath || resolveGooseDbPath(env || process.env);
  const gooseState =
    cursors.goose && typeof cursors.goose === "object" ? cursors.goose : {};
  const sessionTotals =
    gooseState.sessionTotals && typeof gooseState.sessionTotals === "object"
      ? { ...gooseState.sessionTotals }
      : {};

  if (!resolvedDb) {
    cursors.goose = { ...gooseState, sessionTotals, updatedAt: new Date().toISOString() };
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  const cursorDbMtime = Number.isFinite(gooseState.lastDbMtimeMs) ? gooseState.lastDbMtimeMs : 0;
  let currentMtime = 0;
  try {
    currentMtime = fssync.statSync(resolvedDb).mtimeMs;
  } catch (e) {
    if (e && e.code === "ENOENT") {
      cursors.goose = { ...gooseState, sessionTotals, updatedAt: new Date().toISOString() };
      return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
    }
    throw e;
  }
  // mtime short-circuit: skip the full sessions table scan when the DB
  // hasn't been touched since the last sync.
  if (currentMtime > 0 && currentMtime === cursorDbMtime) {
    cursors.goose = { ...gooseState, sessionTotals, updatedAt: new Date().toISOString() };
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  // Snapshot via the shared helper to capture WAL/SHM sidecars — Goose
  // writes async, so without them an in-flight session would read stale.
  const snap = snapshotSqliteDb(resolvedDb);
  let rows = [];
  try {
    rows = readGooseSessionsFromSqlite(snap.path, sqliteOptions);
  } finally {
    snap.cleanup();
  }

  if (rows.length === 0) {
    cursors.goose = { ...gooseState, sessionTotals, updatedAt: new Date().toISOString() };
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;
  let recordsProcessed = 0;
  let eventsAggregated = 0;

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    recordsProcessed++;
    if (!row || typeof row.id !== "string") continue;

    const model = parseGooseModelName(row.model_config_json);
    if (!model) continue;

    // Prefer accumulated_*; fall back to single-turn columns.
    const totalNow = Math.max(
      0,
      Number(row.accumulated_total_tokens ?? row.total_tokens ?? 0) || 0,
    );
    const inputNow = Math.max(
      0,
      Number(row.accumulated_input_tokens ?? row.input_tokens ?? 0) || 0,
    );
    const outputNow = Math.max(
      0,
      Number(row.accumulated_output_tokens ?? row.output_tokens ?? 0) || 0,
    );
    if (totalNow === 0 && inputNow === 0 && outputNow === 0) continue;

    const prev = sessionTotals[row.id] || { input: 0, output: 0, total: 0 };
    // Goose can wipe a session and re-create with the same id during
    // database migration. Treat shrinking cumulative as a reset and emit
    // the full curr value, otherwise the next sync's growth would
    // double-count everything from the reset.
    const isReset = totalNow > 0 && totalNow < prev.total;
    const dInput = isReset ? inputNow : Math.max(0, inputNow - prev.input);
    const dOutput = isReset ? outputNow : Math.max(0, outputNow - prev.output);
    const dTotal = isReset ? totalNow : Math.max(0, totalNow - prev.total);
    if (dInput === 0 && dOutput === 0 && dTotal === 0) {
      if (
        prev.input !== inputNow ||
        prev.output !== outputNow ||
        prev.total !== totalNow
      ) {
        sessionTotals[row.id] = { input: inputNow, output: outputNow, total: totalNow };
      }
      continue;
    }

    // If total grew more than (input + output), treat the excess as reasoning
    // — matches Goose's accounting (it lumps reasoning into `total_tokens`).
    const accountedDelta = dInput + dOutput;
    const reasoningDelta = Math.max(0, dTotal - accountedDelta);

    const tsIso = parseGooseCreatedAt(row.created_at) || new Date().toISOString();
    const bucketStart = toUtcHalfHourStart(tsIso);
    if (!bucketStart) continue;

    // Token normalization: input_tokens = non-cached input; Goose has no
    // cache fields → all input lands in input_tokens. Total stays consistent
    // with: input + output + reasoning (no cache columns).
    const bucketDelta = {
      input_tokens: dInput,
      cached_input_tokens: 0,
      cache_creation_input_tokens: 0,
      output_tokens: dOutput,
      reasoning_output_tokens: reasoningDelta,
      total_tokens: dInput + dOutput + reasoningDelta,
      conversation_count: 1,
    };

    const bucket = getHourlyBucket(hourlyState, "goose", model, bucketStart);
    addTotals(bucket.totals, bucketDelta);
    touchedBuckets.add(bucketKey("goose", model, bucketStart));
    sessionTotals[row.id] = { input: inputNow, output: outputNow, total: totalNow };
    eventsAggregated++;

    if (cb) {
      cb({
        index: i + 1,
        total: rows.length,
        recordsProcessed,
        eventsAggregated,
        bucketsQueued: touchedBuckets.size,
      });
    }
  }

  // Cap cursor at 10k sessions (largest by lifetime usage).
  const entries = Object.entries(sessionTotals);
  if (entries.length > 10_000) {
    entries.sort((a, b) => b[1].total - a[1].total);
    const capped = Object.fromEntries(entries.slice(0, 10_000));
    for (const k of Object.keys(sessionTotals)) delete sessionTotals[k];
    Object.assign(sessionTotals, capped);
  }

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.goose = {
    ...gooseState,
    sessionTotals,
    lastDbMtimeMs: currentMtime,
    updatedAt,
  };

  return { recordsProcessed, eventsAggregated, bucketsQueued };
}

module.exports = {
  gooseInstallOwnsCursor,
  resolveGooseDbPath,
  parseGooseModelName,
  parseGooseCreatedAt,
  parseGooseIncremental,
};
