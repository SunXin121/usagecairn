// claude-science usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { ensureDir } = require("../fs");
const { readSqliteJsonRowsAsync } = require("../sqlite-reader");
const wsl = require("../wsl-probe");
const {
  enqueueTouchedBuckets,
  normalizeHourlyState,
  normalizeQoderState,
  normalizeMessageKeyPart,
  getHourlyBucket,
  addTotals,
  subtractTotals,
  totalsKey,
  toUtcHalfHourStart,
  bucketKey,
  normalizeModelInput,
  toNonNegativeInt,
  coerceEpochMs,
  parseIsoTimestampMs,
  isUncPath,
  snapshotSqliteDb,
} = require("./core");

// ── Claude Science (Anthropic's local research workbench, issue #246) ──
//
// Claude Science keeps all of its state in a SQLite database named
// `operon-cli.db`. There is no native Windows build — on Windows the app runs
// INSIDE WSL, so the DB lives on the distro's ext4 home, not under %USERPROFILE%
// (see resolveClaudeScienceDbPaths).
//
// Per-frame token usage lives on the `frames` table. A row is one agent
// session/sub-session and records its OWN counters; Claude Science's own
// per-conversation rollup (`sumTokensForRoot`) plainly sums every row under a
// root, so summing across frames never double-counts a parent's children.
//
// TWO load-bearing quirks, both verified against real frames by reproducing
// Claude Science's own cost function to 6 significant figures:
//
//  1. `input_tokens` is OpenAI-style — it ALREADY INCLUDES cache reads and
//     writes. Claude Science's accumulator adds `totalInputTokens`
//     (= cacheRead + cacheWrite + uncached), not the Anthropic API's
//     `input_tokens`. Treating the column as pure non-cached input prices cache
//     reads at the full input rate and inflates cost ~5.6x, while also counting
//     the cache columns twice in total_tokens (~1.97x). Subtract them back out.
//  2. `aux_*` are real, billable usage from background helper calls (rolling
//     compaction, compaction summaries, artifact provenance extraction,
//     biosecurity screening, memory extraction). They are DISJOINT from the
//     headline columns — the summarizer call is issued with
//     `skip_cost_accumulation: true` so it bypasses the main accumulator — and
//     Claude Science's own rollup adds them in. Dropping them under-counts by
//     ~11.5%. `aux_input_tokens` carries the same cache-inclusive convention.
//
//     Known approximation: aux tokens get priced at the frame's headline model
//     rate, but the helper calls actually run on a cheaper model, and Claude
//     Science does not persist which one (nor a per-aux cache split — the
//     aux_cache_* columns come back NULL even when aux_cost implies cache
//     reads). Measured on real frames, that overprices the aux slice ~2.4x,
//     i.e. ~20% high on the frame total. Counting the tokens and accepting a
//     bounded cost skew beats dropping 11.5% of usage outright, but if Claude
//     Science ever records the aux model, price that slice separately.
//
// `token_class_usage` is deliberately NOT read: it is an attribution *slice* of
// the same tokens (assistant_prose / tool_calls / thinking / …), so its classes
// sum back to the columns above and adding it would double count.
// The cache and aux columns arrived in later drizzle migrations, so build the
// SELECT from PRAGMA table_info rather than naming them blindly — an older
// operon-cli.db must degrade to the columns it has instead of erroring the
// whole provider out.
//
// The usage predicate doubles as the seed-frame filter: a fresh install ships
// four Anthropic demo frames whose token columns are all NULL. Their
// `context_data` JSON still carries the demo run's real numbers (~23.6M tokens
// / $28.5), which is exactly why this parser reads columns and never that blob.
const CLAUDE_SCIENCE_TOKEN_COLUMNS = [
  "input_tokens",
  "output_tokens",
  "cache_read_tokens",
  "cache_write_tokens",
  "aux_input_tokens",
  "aux_output_tokens",
  "aux_cache_read_tokens",
  "aux_cache_write_tokens",
];

function buildClaudeScienceFramesQuery(columnNames) {
  const columns = new Set(columnNames);
  const optional = (col) => (columns.has(col) ? col : `NULL AS ${col}`);
  const present = CLAUDE_SCIENCE_TOKEN_COLUMNS.filter((col) => columns.has(col));
  const where = present.length
    ? `WHERE ${present.map((col) => `COALESCE(${col}, 0) <> 0`).join("\n   OR ")}`
    : "WHERE 0";
  return `
SELECT
  id,
  parent_frame_id,
  model,
  ${CLAUDE_SCIENCE_TOKEN_COLUMNS.map(optional).join(",\n  ")},
  created_at,
  ${optional("updated_at")},
  ${optional("completed_at")}
FROM frames
${where}
ORDER BY created_at, id
`;
}

// `operon.db` is the pre-rename filename; both are still probed.
const CLAUDE_SCIENCE_DB_FILENAMES = ["operon-cli.db", "operon.db"];

function resolveClaudeScienceRoot({ home = os.homedir(), env = process.env } = {}) {
  const override =
    typeof env.CLAUDE_SCIENCE_HOME === "string" && env.CLAUDE_SCIENCE_HOME.trim()
      ? path.resolve(env.CLAUDE_SCIENCE_HOME.trim())
      : null;
  return override || path.join(home, ".claude-science");
}

function claudeScienceDbsUnderRoot(root) {
  const out = [];
  if (!root) return out;
  for (const name of CLAUDE_SCIENCE_DB_FILENAMES) {
    const candidate = path.join(root, name);
    if (fssync.existsSync(candidate)) out.push(candidate);
  }
  // Multi-org installs keep one DB per org under orgs/<slug>/.
  const orgsDir = path.join(root, "orgs");
  let orgs = [];
  try {
    orgs = fssync.readdirSync(orgsDir, { withFileTypes: true });
  } catch (_e) {
    orgs = [];
  }
  for (const org of orgs) {
    if (!org.isDirectory()) continue;
    for (const name of CLAUDE_SCIENCE_DB_FILENAMES) {
      const candidate = path.join(orgsDir, org.name, name);
      if (fssync.existsSync(candidate)) out.push(candidate);
    }
  }
  return out;
}

// Every Claude Science DB on this machine. There is no native Windows build —
// Windows users run the app inside WSL, so on win32 the distro home is probed
// too (same pattern as the other WSL-aware providers). Returns [] when nothing
// is installed.
function resolveClaudeScienceDbPaths({ home = os.homedir(), env = process.env, deps = {} } = {}) {
  const explicit =
    typeof env.CLAUDE_SCIENCE_DB_PATH === "string" && env.CLAUDE_SCIENCE_DB_PATH.trim()
      ? path.resolve(env.CLAUDE_SCIENCE_DB_PATH.trim())
      : null;
  if (explicit) return fssync.existsSync(explicit) ? [explicit] : [];

  const roots = [resolveClaudeScienceRoot({ home, env })];
  const platform = deps.platform || process.platform;
  if (platform === "win32" && !env.CLAUDE_SCIENCE_HOME) {
    const discoverWslHome = deps.discoverWslHome || wsl.discoverWslHome;
    const wslRoot = wsl.shouldProbeWsl(env)
      ? discoverWslHome(".claude-science", { ...deps, env })
      : null;
    if (wslRoot) roots.push(wslRoot);
  }

  const seen = new Set();
  const out = [];
  for (const root of roots) {
    for (const dbPath of claudeScienceDbsUnderRoot(root)) {
      if (seen.has(dbPath)) continue;
      seen.add(dbPath);
      out.push(dbPath);
    }
  }
  return out;
}

// Kept for the default single-install path (and its existing callers/tests):
// the conventional location, whether or not it exists yet.
function resolveClaudeScienceDbPath({ home = os.homedir(), env = process.env } = {}) {
  const explicit =
    typeof env.CLAUDE_SCIENCE_DB_PATH === "string" && env.CLAUDE_SCIENCE_DB_PATH.trim()
      ? env.CLAUDE_SCIENCE_DB_PATH.trim()
      : null;
  if (explicit) return path.resolve(explicit);
  return path.join(resolveClaudeScienceRoot({ home, env }), "operon-cli.db");
}

async function readClaudeScienceFrames(dbPath, sqliteOptions = {}) {
  if (!dbPath || !fssync.existsSync(dbPath)) return [];
  const options = { label: "Claude Science", readOnly: true, ...sqliteOptions };

  // On Windows the DB lives inside WSL and is read over a \\wsl.localhost\ UNC
  // path. operon-cli.db runs in WAL mode (its -wal/-shm sidecars must be read
  // consistently with the main file), and SQLite over the 9p/UNC bridge can
  // fail to open the WAL or read a torn state. Snapshot to a local temp copy
  // first — same guard every other WSL-aware provider uses — and run BOTH the
  // PRAGMA probe and the frames read against that one consistent copy.
  let snapshot = null;
  let effectiveDbPath = dbPath;
  if (isUncPath(dbPath)) {
    try {
      snapshot = snapshotSqliteDb(dbPath);
      effectiveDbPath = snapshot.path;
    } catch (_e) {
      // Snapshot failed (permissions, transient I/O) — fall through to a direct
      // read so the non-UNC case never regresses.
    }
  }

  try {
    const pragmaRows = await readSqliteJsonRowsAsync(
      effectiveDbPath,
      "PRAGMA table_info(frames)",
      options,
    );
    const columnNames = pragmaRows.map((row) => row?.name).filter(Boolean);
    // No `frames` table at all (wrong DB / pre-frames build) — not an error.
    if (columnNames.length === 0) return [];
    return await readSqliteJsonRowsAsync(
      effectiveDbPath,
      buildClaudeScienceFramesQuery(columnNames),
      options,
    );
  } finally {
    if (snapshot) snapshot.cleanup();
  }
}

// `input` here is cache-inclusive (see the header note), so peel the cache
// columns back off to recover pure non-cached input. Main and aux counters are
// clamped independently — mixing them before the subtraction would let one
// group's cache mask the other's shortfall.
function claudeScienceUncachedInput(input, cacheRead, cacheWrite) {
  return Math.max(0, toNonNegativeInt(input) - toNonNegativeInt(cacheRead) - toNonNegativeInt(cacheWrite));
}

function normalizeClaudeScienceTokens(row) {
  const uncachedInput =
    claudeScienceUncachedInput(row?.input_tokens, row?.cache_read_tokens, row?.cache_write_tokens) +
    claudeScienceUncachedInput(
      row?.aux_input_tokens,
      row?.aux_cache_read_tokens,
      row?.aux_cache_write_tokens,
    );
  const output = toNonNegativeInt(row?.output_tokens) + toNonNegativeInt(row?.aux_output_tokens);
  const cacheRead =
    toNonNegativeInt(row?.cache_read_tokens) + toNonNegativeInt(row?.aux_cache_read_tokens);
  const cacheWrite =
    toNonNegativeInt(row?.cache_write_tokens) + toNonNegativeInt(row?.aux_cache_write_tokens);
  const total = uncachedInput + output + cacheRead + cacheWrite;
  if (total <= 0) return null;
  return {
    input_tokens: uncachedInput,
    cached_input_tokens: cacheRead,
    cache_creation_input_tokens: cacheWrite,
    output_tokens: output,
    reasoning_output_tokens: 0,
    total_tokens: total,
    billable_total_tokens: total,
  };
}

function claudeScienceFrameTimestampMs(row) {
  return (
    coerceEpochMs(row?.completed_at) ||
    coerceEpochMs(row?.updated_at) ||
    coerceEpochMs(row?.created_at) ||
    // Tolerate ISO-8601 text timestamps in case a build stores them as strings.
    parseIsoTimestampMs(row?.completed_at) ||
    parseIsoTimestampMs(row?.updated_at) ||
    parseIsoTimestampMs(row?.created_at)
  );
}

// Idempotent, subtract-on-change aggregation keyed by frame id (mirrors the
// Qoder DB parser): frames are re-read in full on every sync, and a frame whose
// counters grew between syncs has its previous contribution removed from its old
// bucket before the new totals are added, so repeated syncs never inflate.
async function parseClaudeScienceIncremental({
  dbRows,
  cursors,
  queuePath,
  onProgress,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const rows = Array.isArray(dbRows) ? dbRows : [];
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const state = normalizeQoderState(cursors?.claudeScience);
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;
  let recordsProcessed = 0;
  let eventsAggregated = 0;

  for (let index = 0; index < rows.length; index++) {
    const row = rows[index];
    const frameId = normalizeMessageKeyPart(row?.id == null ? "" : String(row.id));
    const base = normalizeClaudeScienceTokens(row);
    const timestampMs = claudeScienceFrameTimestampMs(row);
    const bucketStart = timestampMs
      ? toUtcHalfHourStart(new Date(timestampMs).toISOString())
      : null;
    if (!frameId || !base || !bucketStart) {
      recordsProcessed += 1;
      continue;
    }

    // A root frame (no parent) is one user-facing conversation; nested child
    // frames are sub-agent turns of the same conversation and must not inflate
    // the conversation count.
    const isRoot = !normalizeMessageKeyPart(
      row?.parent_frame_id == null ? "" : String(row.parent_frame_id),
    );
    const currentTotals = { ...base, conversation_count: isRoot ? 1 : 0 };
    const model = normalizeModelInput(row?.model) || "claude-science";

    const previous = state.messages[frameId];
    const previousTotals =
      previous?.totals && typeof previous.totals === "object" ? previous.totals : null;
    const unchanged =
      previousTotals &&
      totalsKey(previousTotals) === totalsKey(currentTotals) &&
      previous.bucketStart === bucketStart &&
      previous.model === model;

    if (!unchanged) {
      if (previousTotals && previous.bucketStart && previous.model) {
        const oldBucket = getHourlyBucket(
          hourlyState,
          "claude-science",
          previous.model,
          previous.bucketStart,
        );
        subtractTotals(oldBucket.totals, previousTotals);
        touchedBuckets.add(bucketKey("claude-science", previous.model, previous.bucketStart));
      }
      const bucket = getHourlyBucket(hourlyState, "claude-science", model, bucketStart);
      addTotals(bucket.totals, currentTotals);
      touchedBuckets.add(bucketKey("claude-science", model, bucketStart));
      state.messages[frameId] = {
        totals: currentTotals,
        bucketStart,
        model,
        updatedAt: new Date().toISOString(),
      };
      eventsAggregated += 1;
    }

    recordsProcessed += 1;
    if (cb) {
      cb({
        index: index + 1,
        total: rows.length,
        recordsProcessed,
        eventsAggregated,
        bucketsQueued: touchedBuckets.size,
      });
    }
  }

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  if (bucketsQueued > 0) hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.claudeScience = state;
  return { recordsProcessed, eventsAggregated, bucketsQueued };
}

module.exports = {
  buildClaudeScienceFramesQuery,
  resolveClaudeScienceDbPaths,
  resolveClaudeScienceDbPath,
  readClaudeScienceFrames,
  parseClaudeScienceIncremental,
};
