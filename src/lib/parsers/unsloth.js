// unsloth usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { ensureDir } = require("../fs");
const { readSqliteJsonRows } = require("../sqlite-reader");
const {
  DEFAULT_MODEL,
  enqueueTouchedBuckets,
  normalizeHourlyState,
  normalizeMessageKeyPart,
  toUtcHalfHourStart,
  normalizeModelInput,
  coerceEpochMs,
  parseIsoTimestampMs,
  sqliteStringLiteral,
  normalizeLocalStudioTokens,
  reconcilePassiveUsageEvent,
  sqliteSidecarFingerprint,
  sameSqliteFingerprint,
} = require("./core");

// ─────────────────────────────────────────────────────────────────────────────
// Unsloth Studio
//
// Durable inference usage lives in `studio.db`. The SQL projections below
// extract only stable IDs, timestamps, model identifiers, and scalar counters.
// Message content, attachments, API subjects, credentials, and training data
// never leave SQLite.
// ─────────────────────────────────────────────────────────────────────────────

const UNSLOTH_SOURCE = "unsloth";
const UNSLOTH_SQL_PAGE_SIZE = 500;
const UNSLOTH_CURSOR_OVERLAP_ROWS = 256;
const UNSLOTH_METERED_PROVIDER_TYPES = new Set([
  "anthropic",
  "deepseek",
  "gemini",
  "huggingface",
  "kimi",
  "mistral",
  "openai",
  "openrouter",
  "qwen",
]);

function resolveUnslothDbPath(env = process.env) {
  const override = typeof env.TOKEN_PULSE_UNSLOTH_DB === "string"
    ? env.TOKEN_PULSE_UNSLOTH_DB.trim()
    : "";
  if (override) return path.resolve(override);
  const studioHome = typeof env.UNSLOTH_STUDIO_HOME === "string"
    ? env.UNSLOTH_STUDIO_HOME.trim()
    : "";
  if (studioHome) return path.join(path.resolve(studioHome), "studio.db");
  return path.join(env.HOME || env.USERPROFILE || os.homedir(), ".unsloth", "studio", "studio.db");
}

function unslothSqliteFingerprint(dbPath) {
  const fingerprint = sqliteSidecarFingerprint(dbPath);
  delete fingerprint["-shm"];
  return fingerprint;
}

function unslothRowPosition(row) {
  const createdAt = row?.created_at == null ? "" : String(row.created_at);
  const id = row?.id == null ? "" : String(row.id);
  return createdAt && id ? { createdAt, id } : null;
}

function unslothPositionClause(rowAlias, position, inclusive) {
  if (!position?.createdAt || !position?.id) return null;
  const createdAt = sqliteStringLiteral(position.createdAt);
  const id = sqliteStringLiteral(position.id);
  const idOperator = inclusive ? ">=" : ">";
  return `(${rowAlias}.created_at > ${createdAt} OR ` +
    `(${rowAlias}.created_at = ${createdAt} AND ${rowAlias}.id ${idOperator} ${id}))`;
}

function readUnslothRowsPaged({
  dbPath,
  select,
  from,
  where,
  rowAlias,
  lowerBound,
  options,
}) {
  const requestedPageSize = Number(options?.pageSize);
  const pageSize = Number.isInteger(requestedPageSize) && requestedPageSize > 0
    ? Math.min(requestedPageSize, UNSLOTH_SQL_PAGE_SIZE)
    : UNSLOTH_SQL_PAGE_SIZE;
  const rows = [];
  let after = null;

  while (true) {
    const clauses = [where, `${rowAlias}.created_at IS NOT NULL`, `${rowAlias}.id IS NOT NULL`];
    const positionClause = unslothPositionClause(rowAlias, after || lowerBound, !after);
    if (positionClause) clauses.push(positionClause);
    const page = readSqliteJsonRows(dbPath, `
      SELECT
        ${select}
      FROM ${from}
      WHERE ${clauses.filter(Boolean).join(" AND ")}
      ORDER BY ${rowAlias}.created_at, ${rowAlias}.id
      LIMIT ${pageSize}
    `.trim(), options);
    rows.push(...page);
    if (page.length < pageSize) break;

    const next = unslothRowPosition(page[page.length - 1]);
    if (!next || (
      after && next.createdAt === after.createdAt && next.id === after.id
    )) {
      throw new Error("Unsloth Studio pagination did not advance");
    }
    after = next;
  }

  return rows;
}

function readUnslothUsageRows(dbPath, sqliteOptions = {}, scanState = {}) {
  if (!dbPath || !fssync.existsSync(dbPath)) return [];
  const options = {
    label: "Unsloth Studio",
    maxBuffer: 32 * 1024 * 1024,
    timeout: 30_000,
    readOnly: true,
    throwOnReadFailure: true,
    ...sqliteOptions,
  };
  const tables = new Set(
    readSqliteJsonRows(
      dbPath,
      "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('chat_messages','chat_threads','api_usage_events')",
      options,
    ).map((row) => row?.name).filter(Boolean),
  );
  const rows = [];

  if (tables.has("chat_messages")) {
    const joinThread = tables.has("chat_threads")
      ? "LEFT JOIN chat_threads t ON t.id = m.thread_id"
      : "";
    const threadModel = tables.has("chat_threads") ? "t.model_id" : "NULL";
    const field = (jsonPath, alias) =>
      `CASE WHEN json_valid(m.metadata_json) THEN json_extract(m.metadata_json, '${jsonPath}') ELSE NULL END AS ${alias}`;
    // Re-read a bounded tail because Studio finalizes recent rows in place and
    // does not expose an updated_at column. Older immutable history stays behind
    // the persisted timestamp/ID overlap cursor.
    const chatRows = readUnslothRowsPaged({
      dbPath,
      select: `
        'chat' AS usage_kind,
        m.id,
        m.created_at,
        ${field("$.responseDetails.responseModelId", "response_model")},
        ${field("$.contextUsage.modelId", "requested_model")},
        ${field("$.responseDetails.providerType", "provider_type")},
        ${threadModel} AS fallback_model,
        ${field("$.contextUsage.promptTokens", "prompt_tokens")},
        ${field("$.contextUsage.completionTokens", "completion_tokens")},
        ${field("$.contextUsage.totalTokens", "total_tokens")},
        ${field("$.contextUsage.cachedTokens", "cached_tokens")},
        ${field("$.contextUsage.cacheWriteTokens", "cache_write_tokens")},
        ${field("$.contextUsage.reasoningTokens", "reasoning_tokens")}
      `.trim(),
      from: `chat_messages m ${joinThread}`,
      where: "m.role = 'assistant'",
      rowAlias: "m",
      lowerBound: scanState?.chat?.overlap,
      options,
    });
    rows.push(...chatRows);
  }

  if (tables.has("api_usage_events")) {
    const columns = new Set(
      readSqliteJsonRows(dbPath, "PRAGMA table_info(api_usage_events)", options)
        .map((row) => row?.name)
        .filter(Boolean),
    );
    const required = ["id", "model", "prompt_tokens", "completion_tokens", "total_tokens", "created_at"];
    if (required.every((column) => columns.has(column))) {
      rows.push(...readUnslothRowsPaged({
        dbPath,
        select: `
          'api' AS usage_kind,
          e.id,
          e.created_at,
          e.model AS response_model,
          e.model AS requested_model,
          'local' AS provider_type,
          NULL AS fallback_model,
          e.prompt_tokens,
          e.completion_tokens,
          e.total_tokens,
          0 AS cached_tokens,
          0 AS cache_write_tokens,
          0 AS reasoning_tokens
        `.trim(),
        from: "api_usage_events e",
        where: "",
        rowAlias: "e",
        lowerBound: scanState?.api?.overlap,
        options,
      }));
    }
  }
  return rows;
}

function qualifyUnslothModel(row) {
  const rawModel = normalizeModelInput(row?.response_model)
    || normalizeModelInput(row?.requested_model)
    || normalizeModelInput(row?.fallback_model)
    || DEFAULT_MODEL;
  const providerType = normalizeMessageKeyPart(row?.provider_type).toLowerCase();
  if (row?.usage_kind === "api" || providerType === "local") {
    return `local/${rawModel}`;
  }
  if (!UNSLOTH_METERED_PROVIDER_TYPES.has(providerType)) {
    return `unpriced/${providerType || "unknown"}/${rawModel}`;
  }
  const lowerModel = rawModel.toLowerCase();
  return lowerModel.startsWith(`${providerType}/`)
    ? rawModel
    : `${providerType}/${rawModel}`;
}

function normalizeUnslothUsageRow(row) {
  const totals = normalizeLocalStudioTokens({
    prompt_tokens: row?.prompt_tokens,
    completion_tokens: row?.completion_tokens,
    total_tokens: row?.total_tokens,
    cached_tokens: row?.cached_tokens,
    cache_creation_input_tokens: row?.cache_write_tokens,
    reasoning_tokens: row?.reasoning_tokens,
  });
  const id = normalizeMessageKeyPart(row?.id == null ? "" : String(row.id));
  const timestampMs = coerceEpochMs(row?.created_at) || parseIsoTimestampMs(row?.created_at);
  const bucketStart = timestampMs
    ? toUtcHalfHourStart(new Date(timestampMs).toISOString())
    : null;
  if (!id || !totals || !bucketStart) return null;
  const kind = row?.usage_kind === "api" ? "api" : "chat";
  return {
    key: `unsloth:${kind}:${id}`,
    model: qualifyUnslothModel(row),
    bucketStart,
    totals,
  };
}

async function parseUnslothIncremental({
  dbPath,
  cursors,
  queuePath,
  onProgress,
  env,
  sqliteOptions,
  overlapRows = UNSLOTH_CURSOR_OVERLAP_ROWS,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const resolvedDb = dbPath || resolveUnslothDbPath(env || process.env);
  const priorState = cursors.unsloth && typeof cursors.unsloth === "object"
    ? cursors.unsloth
    : {};
  const messages = priorState.messages && typeof priorState.messages === "object"
    ? priorState.messages
    : {};
  if (!resolvedDb || !fssync.existsSync(resolvedDb)) {
    cursors.unsloth = { ...priorState, messages, updatedAt: new Date().toISOString() };
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  const initialFingerprint = unslothSqliteFingerprint(resolvedDb);
  const hasBoundedScan = priorState.scan
    && typeof priorState.scan === "object"
    && Object.keys(messages).length <= UNSLOTH_CURSOR_OVERLAP_ROWS * 2;
  if (hasBoundedScan && sameSqliteFingerprint(initialFingerprint, priorState.fingerprint)) {
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  const rows = readUnslothUsageRows(resolvedDb, sqliteOptions, priorState.scan);
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;
  let recordsProcessed = 0;
  let eventsAggregated = 0;
  for (let index = 0; index < rows.length; index++) {
    const event = normalizeUnslothUsageRow(rows[index]);
    recordsProcessed += 1;
    if (event && reconcilePassiveUsageEvent({
      event,
      source: UNSLOTH_SOURCE,
      messages,
      hourlyState,
      touchedBuckets,
    })) eventsAggregated += 1;
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

  const retention = Number.isInteger(overlapRows) && overlapRows > 0
    ? Math.min(overlapRows, UNSLOTH_CURSOR_OVERLAP_ROWS)
    : UNSLOTH_CURSOR_OVERLAP_ROWS;
  const nextScan = {};
  const retainedMessageKeys = new Set();
  for (const kind of ["chat", "api"]) {
    const kindRows = rows.filter((row) => (row?.usage_kind === "api" ? "api" : "chat") === kind);
    if (kindRows.length === 0) {
      if (priorState.scan?.[kind]) nextScan[kind] = priorState.scan[kind];
      for (const key of Object.keys(messages)) {
        if (key.startsWith(`unsloth:${kind}:`)) retainedMessageKeys.add(key);
      }
      continue;
    }
    const tail = kindRows.slice(-retention);
    const overlap = unslothRowPosition(tail[0]);
    const latest = unslothRowPosition(kindRows[kindRows.length - 1]);
    if (overlap && latest) nextScan[kind] = { overlap, latest };
    for (const row of tail) {
      const event = normalizeUnslothUsageRow(row);
      if (event) retainedMessageKeys.add(event.key);
    }
  }
  for (const key of Object.keys(messages)) {
    if (key.startsWith("unsloth:") && !retainedMessageKeys.has(key)) delete messages[key];
  }

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const finalFingerprint = unslothSqliteFingerprint(resolvedDb);
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.unsloth = {
    messages,
    scan: nextScan,
    fingerprint: sameSqliteFingerprint(initialFingerprint, finalFingerprint)
      ? finalFingerprint
      : initialFingerprint,
    updatedAt,
  };
  return { recordsProcessed, eventsAggregated, bucketsQueued };
}

module.exports = {
  resolveUnslothDbPath,
  readUnslothUsageRows,
  normalizeUnslothUsageRow,
  parseUnslothIncremental,
};
