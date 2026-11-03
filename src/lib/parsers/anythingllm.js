// anythingllm usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const path = require("node:path");
const { ensureDir } = require("../fs");
const { readSqliteJsonRows } = require("../sqlite-reader");
const {
  enqueueTouchedBuckets,
  normalizeHourlyState,
  getHourlyBucket,
  addTotals,
  toUtcHalfHourStart,
  bucketKey,
  normalizeModelInput,
  toNonNegativeInt,
  isUncPath,
  snapshotSqliteDb,
} = require("./core");

// ─────────────────────────────────────────────────────────────────────────────
// AnythingLLM Desktop (Mintplex Labs)
//
// Data: SQLite at
//   macOS:   ~/Library/Application Support/anythingllm-desktop/storage/anythingllm.db
//   Linux:   $XDG_CONFIG_HOME/anythingllm-desktop/storage/anythingllm.db
//   Windows: %APPDATA%\anythingllm-desktop\storage\anythingllm.db
//   Override: $TOKEN_PULSE_ANYTHINGLLM_DB
//
// AnythingLLM >= 1.7.1 stores per-message prompt/completion/total counts in
// workspace_chats.response.metrics. The SQL projection below extracts only
// those numeric metrics and the model identifier — prompts, response
// text, sources, and attachments never leave SQLite.
// ─────────────────────────────────────────────────────────────────────────────

const ANYTHINGLLM_PENDING_MAX_AGE_MS = 24 * 60 * 60 * 1000;

function resolveAnythingllmDbPath(env = process.env, platform = process.platform) {
  const override = typeof env.TOKEN_PULSE_ANYTHINGLLM_DB === "string"
    ? env.TOKEN_PULSE_ANYTHINGLLM_DB.trim()
    : "";
  if (override) return override;

  const os = require("node:os");
  if (platform === "win32") {
    const home = env.USERPROFILE || os.homedir();
    const appData = env.APPDATA || path.join(home, "AppData", "Roaming");
    return path.join(appData, "anythingllm-desktop", "storage", "anythingllm.db");
  }

  const home = env.HOME || os.homedir();
  if (platform === "darwin") {
    return path.join(
      home,
      "Library",
      "Application Support",
      "anythingllm-desktop",
      "storage",
      "anythingllm.db",
    );
  }

  const configHome = env.XDG_CONFIG_HOME || path.join(home, ".config");
  return path.join(configHome, "anythingllm-desktop", "storage", "anythingllm.db");
}

function parseAnythingllmTimestamp(value) {
  const epochIso = (epochValue) => {
    if (!Number.isFinite(epochValue)) return null;
    // Prisma stores SQLite DateTime values as epoch milliseconds. Accept
    // seconds as well for compatibility with databases created by other
    // SQLite clients.
    const epochMs = Math.abs(epochValue) < 100_000_000_000
      ? epochValue * 1000
      : epochValue;
    const parsed = new Date(epochMs);
    return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
  };
  if (typeof value === "number") return epochIso(value);
  if (typeof value !== "string" || !value.trim()) return null;
  const trimmed = value.trim();
  if (/^-?\d+(?:\.\d+)?$/.test(trimmed)) return epochIso(Number(trimmed));
  const naive = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?$/.exec(trimmed);
  if (naive) {
    const millis = String(naive[7] || "0").padEnd(3, "0");
    return new Date(Date.UTC(
      +naive[1],
      +naive[2] - 1,
      +naive[3],
      +naive[4],
      +naive[5],
      +naive[6],
      +millis,
    )).toISOString();
  }
  const parsed = new Date(trimmed);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function readAnythingllmUsageRowsWhere(dbPath, whereClause, sqliteOptions = {}) {
  if (!dbPath || !fssync.existsSync(dbPath)) return [];
  const metric = (jsonPath, alias) =>
    `CASE WHEN json_valid(response) THEN json_extract(response, '${jsonPath}') ELSE NULL END AS ${alias}`;
  const sql = `
    SELECT
      id,
      include,
      createdAt,
      lastUpdatedAt,
      ${metric("$.metrics.prompt_tokens", "prompt_tokens")},
      ${metric("$.metrics.completion_tokens", "completion_tokens")},
      ${metric("$.metrics.total_tokens", "total_tokens")},
      ${metric("$.metrics.model", "model")}
    FROM workspace_chats
    WHERE ${whereClause}
    ORDER BY id ASC
  `.trim();

  let snapshot = null;
  let effectiveDbPath = dbPath;
  if (isUncPath(dbPath)) {
    try {
      snapshot = snapshotSqliteDb(dbPath);
      effectiveDbPath = snapshot.path;
    } catch (_e) { }
  }

  try {
    return readSqliteJsonRows(effectiveDbPath, sql, {
      label: "AnythingLLM",
      maxBuffer: 32 * 1024 * 1024,
      timeout: 30_000,
      readOnly: true,
      ...sqliteOptions,
    });
  } finally {
    if (snapshot) snapshot.cleanup();
  }
}

function readAnythingllmUsageRows(dbPath, sinceId = 0, sqliteOptions = {}) {
  const safeSinceId = Math.max(0, Math.trunc(Number(sinceId) || 0));
  return readAnythingllmUsageRowsWhere(dbPath, `id > ${safeSinceId}`, sqliteOptions);
}

function readAnythingllmUsageRowsByIds(dbPath, ids, sqliteOptions = {}) {
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
      ...readAnythingllmUsageRowsWhere(
        dbPath,
        `id IN (${chunk.join(",")})`,
        sqliteOptions,
      ),
    );
  }
  return rows.sort((a, b) => toNonNegativeInt(a?.id) - toNonNegativeInt(b?.id));
}

async function parseAnythingllmIncremental({
  dbPath,
  cursors,
  queuePath,
  onProgress,
  env,
  sqliteOptions,
  nowMs = Date.now(),
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const resolvedDb = dbPath || resolveAnythingllmDbPath(env || process.env);
  const priorState = cursors.anythingllm && typeof cursors.anythingllm === "object"
    ? cursors.anythingllm
    : {};
  const lastChatId = Math.max(0, Math.trunc(Number(priorState.lastChatId) || 0));
  const pendingChatIds = new Set(
    (Array.isArray(priorState.pendingChatIds) ? priorState.pendingChatIds : [])
      .map(toNonNegativeInt)
      .filter((id) => id > 0),
  );
  const pendingCutoffMs = (Number.isFinite(nowMs) ? nowMs : Date.now())
    - ANYTHINGLLM_PENDING_MAX_AGE_MS;

  if (!resolvedDb || !fssync.existsSync(resolvedDb)) {
    cursors.anythingllm = {
      ...priorState,
      lastChatId,
      pendingChatIds: [...pendingChatIds].sort((a, b) => a - b),
      updatedAt: new Date().toISOString(),
    };
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  const incrementalRows = readAnythingllmUsageRows(resolvedDb, lastChatId, sqliteOptions);
  const pendingRetryRows = readAnythingllmUsageRowsByIds(
    resolvedDb,
    [...pendingChatIds],
    sqliteOptions,
  );
  const pendingRetryIds = new Set(
    pendingRetryRows.map((row) => toNonNegativeInt(row?.id)),
  );
  for (const pendingId of pendingChatIds) {
    if (!pendingRetryIds.has(pendingId)) pendingChatIds.delete(pendingId);
  }
  const rowsById = new Map();
  for (const row of [...pendingRetryRows, ...incrementalRows]) {
    rowsById.set(toNonNegativeInt(row?.id), row);
  }
  const rows = Array.from(rowsById.values()).sort(
    (a, b) => toNonNegativeInt(a?.id) - toNonNegativeInt(b?.id),
  );
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;
  let maxSeenId = lastChatId;
  let recordsProcessed = 0;
  let eventsAggregated = 0;
  const reportProgress = (index) => {
    if (!cb) return;
    cb({
      index,
      total: rows.length,
      recordsProcessed,
      eventsAggregated,
      bucketsQueued: touchedBuckets.size,
    });
  };

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    recordsProcessed += 1;
    const rowId = Math.max(0, Math.trunc(Number(row?.id) || 0));
    if (rowId > maxSeenId) maxSeenId = rowId;

    const inputTokens = toNonNegativeInt(row?.prompt_tokens);
    const outputTokens = toNonNegativeInt(row?.completion_tokens);
    const reportedTotal = toNonNegativeInt(row?.total_tokens);
    if (inputTokens === 0 && outputTokens === 0) {
      const pendingTimestamp = parseAnythingllmTimestamp(row?.lastUpdatedAt)
        || parseAnythingllmTimestamp(row?.createdAt);
      const pendingTimestampMs = pendingTimestamp ? Date.parse(pendingTimestamp) : NaN;
      const shouldRetry =
        toNonNegativeInt(row?.include) === 0
        && Number.isFinite(pendingTimestampMs)
        && pendingTimestampMs >= pendingCutoffMs;
      if (rowId > 0 && shouldRetry) pendingChatIds.add(rowId);
      else if (rowId > 0) pendingChatIds.delete(rowId);
      reportProgress(i + 1);
      continue;
    }

    const timestamp = parseAnythingllmTimestamp(row?.createdAt)
      || parseAnythingllmTimestamp(row?.lastUpdatedAt);
    const bucketStart = timestamp ? toUtcHalfHourStart(timestamp) : null;
    if (!bucketStart) {
      if (rowId > 0) pendingChatIds.delete(rowId);
      reportProgress(i + 1);
      continue;
    }
    if (rowId > 0) pendingChatIds.delete(rowId);

    const model = normalizeModelInput(row?.model) || "anythingllm-unknown";
    const accounted = inputTokens + outputTokens;
    const reasoningTokens = Math.max(0, reportedTotal - accounted);
    const delta = {
      input_tokens: inputTokens,
      cached_input_tokens: 0,
      cache_creation_input_tokens: 0,
      output_tokens: outputTokens,
      reasoning_output_tokens: reasoningTokens,
      total_tokens: accounted + reasoningTokens,
      conversation_count: 1,
    };

    const bucket = getHourlyBucket(hourlyState, "anythingllm", model, bucketStart);
    addTotals(bucket.totals, delta);
    touchedBuckets.add(bucketKey("anythingllm", model, bucketStart));
    eventsAggregated += 1;

    reportProgress(i + 1);
  }

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.anythingllm = {
    ...priorState,
    lastChatId: maxSeenId,
    pendingChatIds: [...pendingChatIds].sort((a, b) => a - b),
    updatedAt,
  };

  return { recordsProcessed, eventsAggregated, bucketsQueued };
}

module.exports = {
  resolveAnythingllmDbPath,
  parseAnythingllmTimestamp,
  readAnythingllmUsageRows,
  parseAnythingllmIncremental,
};
