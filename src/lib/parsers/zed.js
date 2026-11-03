// zed usage parser and install discovery.
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

function zedInstallOwnsCursor(dbPath, flatState, sqliteOptions) {
  return sqliteDbContainsIds(dbPath, "threads", sampleRecentKeys(flatState?.threadTotals), sqliteOptions);
}

// ─────────────────────────────────────────────────────────────────────────────
// Zed Agent (all model providers — hosted "zed.dev" and bring-your-own alike)
//
// Data: SQLite at
//   macOS:    ~/Library/Application Support/Zed/threads/threads.db
//   Linux:    $XDG_DATA_HOME/zed/threads/threads.db (defaults to ~/.local/share)
//   Windows:  %LOCALAPPDATA%\Zed\threads\threads.db
//
// `threads` table stores one row per thread with a BLOB `data` column —
// either raw JSON or zstd-compressed JSON (governed by `data_type`). Each
// thread's JSON carries `cumulative_token_usage` and/or
// `request_token_usage` (a map or array of per-request usages with
// input_tokens / output_tokens / cache_read_input_tokens /
// cache_creation_input_tokens).
//
// Threads grow over multiple turns — the row is rewritten with a larger
// cumulative on every send, so naive dedup-by-id would freeze our count at
// whatever the thread looked like the first time we saw it. We mirror the
// antigravity cumulative-delta pattern: keep last-seen totals per thread in
// `cursors.zed.threadTotals`, emit (current - previous) on each sync.
//
// Providers already reported by a dedicated parser are skipped to avoid
// double-counting (see ZED_DOUBLE_COUNTED_PROVIDERS — empty today). Model names
// are normalized for pricing in the matcher (normalizeZedModel), not here, so
// the real Zed model name is preserved for display.
// ─────────────────────────────────────────────────────────────────────────────

// Providers whose usage is ALSO captured by a dedicated UsageCairn parser, so
// counting them via the Zed thread store would double-count. Zed's native model
// providers (zed.dev, copilot_chat, openai*, anthropic, google, ollama,
// lmstudio, …) do NOT overlap: e.g. Zed's copilot_chat talks to the Copilot API
// directly and never writes ~/.copilot/otel, which is what the Copilot parser
// reads. The set is therefore empty today; it's the extension point if Zed ever
// persists external-ACP-agent usage (Claude Code / Codex run inside Zed) into
// threads.db with a recognizable provider id.
const ZED_DOUBLE_COUNTED_PROVIDERS = new Set();
const MAX_ZED_THREAD_JSON_BYTES = 32 * 1024 * 1024;

function resolveZedDbPath(env = process.env) {
  if (typeof env.TOKEN_PULSE_ZED_DB === "string" && env.TOKEN_PULSE_ZED_DB.trim()) {
    return env.TOKEN_PULSE_ZED_DB.trim();
  }
  const home = env.HOME || os.homedir();
  if (process.platform === "darwin") {
    return path.join(home, "Library", "Application Support", "Zed", "threads", "threads.db");
  }
  if (process.platform === "win32") {
    const local = env.LOCALAPPDATA || path.join(home, "AppData", "Local");
    const native = path.join(local, "Zed", "threads", "threads.db");
    const wslThreadsDir = wsl.shouldProbeWsl(env) ? wsl.discoverWslHome(".local/share/zed/threads", { env }) : null;
    const wslDbPath = wslThreadsDir && fssync.existsSync(path.join(wslThreadsDir, "threads.db"))
      ? path.join(wslThreadsDir, "threads.db") : null;
    const paths = resolveInstallPaths({ nativeValue: native, wslValue: wslDbPath }, env);
    const picked = paths.native || paths.wsl;
    if (picked) return picked;
    const mode = wsl.getWslMode(env);
    return mode === "wsl-only" || mode === "native-only" ? null : native;
  }
  const xdg = env.XDG_DATA_HOME || path.join(home, ".local", "share");
  return path.join(xdg, "zed", "threads", "threads.db");
}

// Decode a row's BLOB payload into UTF-8 JSON text. Zed marks zstd-compressed
// blobs with data_type="zstd"; older / smaller threads use data_type="json"
// and store the bytes verbatim. Node 24+ has native zstd; Node 20 needs the
// @mongodb-js/zstd fallback. Cap decoded size to mirror tokscale's safety net.
async function decodeZedThreadBlob({ dataType, data }) {
  const type = (dataType || "").trim().toLowerCase();
  if (type === "json") {
    if (data.length > MAX_ZED_THREAD_JSON_BYTES) {
      throw new Error(`json blob exceeds ${MAX_ZED_THREAD_JSON_BYTES} bytes`);
    }
    return data.toString("utf8");
  }
  if (type === "zstd") {
    const zlib = require("node:zlib");
    const out =
      typeof zlib.zstdDecompressSync === "function"
        ? zlib.zstdDecompressSync(data)
        : Buffer.from(await require("@mongodb-js/zstd").decompress(data));
    if (out.length > MAX_ZED_THREAD_JSON_BYTES) {
      throw new Error(`decoded zstd blob exceeds ${MAX_ZED_THREAD_JSON_BYTES} bytes`);
    }
    return out.toString("utf8");
  }
  throw new Error(`unsupported data_type: ${dataType}`);
}

// Pull the 4-tuple (input/output/cache_read/cache_write) out of one Zed
// TokenUsage shape. Zed stores integers but some historical rows used
// strings — match tokscale's permissive coercion.
function readZedUsage(value) {
  if (!value || typeof value !== "object") return null;
  const coerce = (v) => {
    if (typeof v === "number") return Math.max(0, Math.floor(v));
    if (typeof v === "string") {
      const n = Number.parseInt(v, 10);
      return Number.isFinite(n) && n > 0 ? n : 0;
    }
    return 0;
  };
  return {
    input: coerce(value.input_tokens),
    output: coerce(value.output_tokens),
    cache_read: coerce(value.cache_read_input_tokens),
    cache_write: coerce(value.cache_creation_input_tokens),
  };
}

function sumZedRequestUsage(value) {
  const total = { input: 0, output: 0, cache_read: 0, cache_write: 0 };
  if (!value) return total;
  const iter =
    Array.isArray(value)
      ? value
      : typeof value === "object"
      ? Object.values(value)
      : [];
  for (const entry of iter) {
    const u = readZedUsage(entry);
    if (!u) continue;
    total.input += u.input;
    total.output += u.output;
    total.cache_read += u.cache_read;
    total.cache_write += u.cache_write;
  }
  return total;
}

// Extract token totals from a parsed Zed thread object. Prefer summed
// request_token_usage (per-turn breakdown) and fall back to
// cumulative_token_usage when the per-turn map is empty.
function extractZedTotals(thread) {
  if (!thread || thread.imported === true) return null;
  const model = thread.model;
  if (!model || typeof model !== "object") return null;
  const provider = typeof model.provider === "string" ? model.provider.trim() : "";
  // Count usage for ALL providers — Zed-hosted (zed.dev) and bring-your-own
  // (copilot_chat, openai-subscribed, anthropic, lmstudio, …) alike. Only skip
  // providers whose usage a dedicated parser already reports (see
  // ZED_DOUBLE_COUNTED_PROVIDERS).
  if (provider && ZED_DOUBLE_COUNTED_PROVIDERS.has(provider.toLowerCase())) return null;
  const modelId = typeof model.model === "string" ? model.model.trim() : "";
  if (!modelId) return null;

  const request = sumZedRequestUsage(thread.request_token_usage);
  if (request.input + request.output + request.cache_read + request.cache_write > 0) {
    return { totals: request, model: modelId };
  }
  const cumulative = readZedUsage(thread.cumulative_token_usage);
  if (
    cumulative &&
    cumulative.input + cumulative.output + cumulative.cache_read + cumulative.cache_write > 0
  ) {
    return { totals: cumulative, model: modelId };
  }
  return null;
}

// Build a SELECT that only references columns we know exist — Zed has shipped
// several `threads` schemas; older versions may omit created_at /
// folder_paths. We dynamically detect via PRAGMA so the query never fails on
// a missing column.
function buildZedThreadsQuery(dbPath, cursorUpdatedAt, sqliteOptions = {}) {
  const pragmaRows = readSqliteJsonRows(dbPath, "PRAGMA table_info(threads)", {
    label: "Zed",
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
  // Incremental: only fetch threads updated after the last sync watermark.
  // Without this we'd zstd-decode every thread on every sync (~250MB for a
  // 5k-thread DB on every menu-bar tick). Empty cursor → full scan (first
  // sync). updated_at is stored as ISO 8601 text, so lexical comparison ==
  // chronological comparison.
  const escaped = typeof cursorUpdatedAt === "string" && cursorUpdatedAt
    ? cursorUpdatedAt.replace(/'/g, "''")
    : null;
  const where = escaped ? ` WHERE updated_at > '${escaped}'` : "";
  return `SELECT id, updated_at, ${optional("created_at")}, data_type, hex(data) AS data_hex FROM threads${where}`;
}

function readZedThreadRowsFromSqlite(dbPath, cursorUpdatedAt, sqliteOptions = {}) {
  const query = buildZedThreadsQuery(dbPath, cursorUpdatedAt, sqliteOptions);
  return readSqliteJsonRows(dbPath, query, {
    label: "Zed",
    maxBuffer: 256 * 1024 * 1024,
    timeout: 60_000,
    ...sqliteOptions,
  });
}

async function parseZedIncremental({
  dbPath,
  cursors,
  queuePath,
  onProgress,
  env,
  sqliteOptions,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const resolvedDb = dbPath || resolveZedDbPath(env || process.env);
  if (!resolvedDb) {
    cursors.zed = { ...cursors.zed, updatedAt: new Date().toISOString() };
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }
  const zedState =
    cursors.zed && typeof cursors.zed === "object" ? cursors.zed : {};
  const threadTotals =
    zedState.threadTotals && typeof zedState.threadTotals === "object"
      ? { ...zedState.threadTotals }
      : {};
  const cursorUpdatedAt = typeof zedState.lastUpdatedAt === "string" ? zedState.lastUpdatedAt : null;
  const cursorDbMtime = Number.isFinite(zedState.lastDbMtimeMs) ? zedState.lastDbMtimeMs : 0;

  // mtime short-circuit: if the SQLite file hasn't been touched since the
  // last sync there's nothing to read — skip the ~250MB copyFile + zstd
  // round-trip entirely. We still re-stat on the next call, so a Zed write
  // is picked up within one sync interval.
  let currentMtime = 0;
  try {
    currentMtime = fssync.statSync(resolvedDb).mtimeMs;
  } catch (e) {
    if (e && e.code === "ENOENT") {
      cursors.zed = { ...zedState, threadTotals, updatedAt: new Date().toISOString() };
      return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
    }
    throw e;
  }
  if (currentMtime > 0 && currentMtime === cursorDbMtime) {
    cursors.zed = { ...zedState, threadTotals, updatedAt: new Date().toISOString() };
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  // Snapshot via the shared helper so we get WAL/SHM/journal sidecar copies
  // too. Without sidecars, an active Zed write that's still in the WAL
  // would be missed (the .db has older pages until checkpoint).
  const snap = snapshotSqliteDb(resolvedDb);
  let rows = [];
  try {
    rows = readZedThreadRowsFromSqlite(snap.path, cursorUpdatedAt, sqliteOptions);
  } finally {
    snap.cleanup();
  }

  if (rows.length === 0) {
    cursors.zed = { ...zedState, threadTotals, updatedAt: new Date().toISOString() };
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
    if (!row || typeof row.id !== "string" || !row.data_hex) continue;

    let blob;
    try { blob = Buffer.from(row.data_hex, "hex"); } catch { continue; }

    let jsonText;
    try { jsonText = await decodeZedThreadBlob({ dataType: row.data_type, data: blob }); }
    catch { continue; }

    let thread;
    try { thread = JSON.parse(jsonText); } catch { continue; }

    const extracted = extractZedTotals(thread);
    if (!extracted) continue;

    const prev = threadTotals[row.id] || { input: 0, output: 0, cache_read: 0, cache_write: 0 };
    const curr = extracted.totals;
    const prevSum = prev.input + prev.output + prev.cache_read + prev.cache_write;
    const currSum = curr.input + curr.output + curr.cache_read + curr.cache_write;
    // Detect cumulative reset: a thread can be re-created with the same id
    // but lower totals (rare — Zed may purge & rewrite on import/export).
    // Naive `Math.max(0, curr - prev)` would clamp the delta to 0 and quietly
    // update the cursor to the smaller `curr`, so the next sync sees growth
    // from the reset and re-counts everything since. Treat reset as a
    // fresh-start emit of `curr`.
    const isReset = currSum > 0 && currSum < prevSum;
    const delta = isReset
      ? { ...curr }
      : {
          input: Math.max(0, curr.input - prev.input),
          output: Math.max(0, curr.output - prev.output),
          cache_read: Math.max(0, curr.cache_read - prev.cache_read),
          cache_write: Math.max(0, curr.cache_write - prev.cache_write),
        };
    const totalDelta = delta.input + delta.output + delta.cache_read + delta.cache_write;
    if (totalDelta <= 0) {
      if (
        curr.input !== prev.input ||
        curr.output !== prev.output ||
        curr.cache_read !== prev.cache_read ||
        curr.cache_write !== prev.cache_write
      ) {
        threadTotals[row.id] = curr;
      }
      continue;
    }

    const tsIso =
      (typeof row.updated_at === "string" && row.updated_at) ||
      (typeof row.created_at === "string" && row.created_at) ||
      (typeof thread.updated_at === "string" && thread.updated_at) ||
      new Date().toISOString();
    const bucketStart = toUtcHalfHourStart(tsIso);
    if (!bucketStart) continue;

    const bucketDelta = {
      input_tokens: delta.input,
      cached_input_tokens: delta.cache_read,
      cache_creation_input_tokens: delta.cache_write,
      output_tokens: delta.output,
      reasoning_output_tokens: 0,
      total_tokens: totalDelta,
      conversation_count: 1,
    };

    const bucket = getHourlyBucket(hourlyState, "zed", extracted.model, bucketStart);
    addTotals(bucket.totals, bucketDelta);
    touchedBuckets.add(bucketKey("zed", extracted.model, bucketStart));
    threadTotals[row.id] = curr;
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

  // Compute nextCursor BEFORE the 10k cap. If we capped first, a low-volume
  // zed.dev thread evicted in the cap step would no longer be in
  // threadTotals, so its updated_at would not advance the cursor — and the
  // next sync's WHERE filter would re-read & re-decode the same blob forever.
  // We record everything we touched this run regardless of post-cap eviction.
  let nextCursor = cursorUpdatedAt;
  for (const r of rows) {
    if (
      typeof r.updated_at === "string" &&
      threadTotals[r.id] !== undefined &&
      (nextCursor == null || r.updated_at > nextCursor)
    ) {
      nextCursor = r.updated_at;
    }
  }

  const entries = Object.entries(threadTotals);
  if (entries.length > 10_000) {
    entries.sort((a, b) => {
      const ta = a[1].input + a[1].output + a[1].cache_read + a[1].cache_write;
      const tb = b[1].input + b[1].output + b[1].cache_read + b[1].cache_write;
      return tb - ta;
    });
    const capped = Object.fromEntries(entries.slice(0, 10_000));
    for (const k of Object.keys(threadTotals)) delete threadTotals[k];
    Object.assign(threadTotals, capped);
  }

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.zed = {
    ...zedState,
    threadTotals,
    lastUpdatedAt: nextCursor,
    lastDbMtimeMs: currentMtime,
    updatedAt,
  };

  return { recordsProcessed, eventsAggregated, bucketsQueued };
}

module.exports = {
  zedInstallOwnsCursor,
  resolveZedDbPath,
  decodeZedThreadBlob,
  readZedUsage,
  sumZedRequestUsage,
  extractZedTotals,
  parseZedIncremental,
};
