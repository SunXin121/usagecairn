// qoder usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fs = require("node:fs/promises");
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { ensureDir } = require("../fs");
const { readSqliteJsonRowsAsync } = require("../sqlite-reader");
const wsl = require("../wsl-probe");
const {
  enqueueTouchedBuckets,
  enqueueTouchedProjectBuckets,
  normalizeHourlyState,
  normalizeProjectState,
  normalizeQoderState,
  normalizeMessageKeyPart,
  getHourlyBucket,
  getProjectBucket,
  addTotals,
  subtractTotals,
  totalsKey,
  toUtcHalfHourStart,
  bucketKey,
  projectBucketKey,
  normalizeModelInput,
  resolveProjectContextForPath,
  coerceEpochMs,
  safeReadDir,
  parseIsoTimestampMs,
} = require("./core");

const QODER_USAGE_SQL = `
SELECT
  cm.rowid AS row_id,
  cm.id,
  cm.session_id,
  cm.request_id,
  cm.token_info,
  cm.model_info,
  cm.gmt_create,
  cr.extra AS record_extra,
  cs.preferred_model_info,
  cs.project_uri,
  cs.project_name
FROM chat_message AS cm
LEFT JOIN chat_record AS cr ON cr.request_id = cm.request_id
LEFT JOIN chat_session AS cs ON cs.session_id = cm.session_id
WHERE cm.role = 'assistant'
  AND cm.token_info IS NOT NULL
  AND trim(cm.token_info) NOT IN ('', '{}')
ORDER BY cm.gmt_create, cm.rowid
`;

function resolveQoderDbPath({
  home = os.homedir(),
  env = process.env,
  platform = process.platform,
  appDir = "Qoder",
  envPrefix = "QODER",
} = {}) {
  const dbPathKey = `${envPrefix}_DB_PATH`;
  const homeKey = `${envPrefix}_HOME`;
  if (typeof env[dbPathKey] === "string" && env[dbPathKey].trim()) {
    return path.resolve(env[dbPathKey].trim());
  }
  let root;
  if (typeof env[homeKey] === "string" && env[homeKey].trim()) {
    root = path.resolve(env[homeKey].trim());
  } else if (platform === "darwin") {
    root = path.join(home, "Library", "Application Support", appDir);
  } else if (platform === "win32") {
    root = path.join(
      env.APPDATA || path.join(home, "AppData", "Roaming"),
      appDir,
    );
  } else {
    root = path.join(home, ".config", appDir);
  }
  return path.join(root, "SharedClientCache", "cache", "db", "local.db");
}

function resolveQoderDbPaths({
  home = os.homedir(),
  env = process.env,
  platform = process.platform,
  appDir = "Qoder",
  envPrefix = "QODER",
  deps = {},
} = {}) {
  const dbPathKey = `${envPrefix}_DB_PATH`;
  const homeKey = `${envPrefix}_HOME`;
  const nativeValue = resolveQoderDbPath({ home, env, platform, appDir, envPrefix });
  if (platform !== "win32" || env[dbPathKey] || env[homeKey]) {
    return { native: nativeValue, wsl: null };
  }
  const existsSync = deps.existsSync || fssync.existsSync;
  const native = wsl.shouldProbeNative(env) && existsSync(nativeValue) ? nativeValue : null;
  const discoverWslHome = deps.discoverWslHome || wsl.discoverWslHome;
  const wslRoot = wsl.shouldProbeWsl(env)
    ? discoverWslHome(`.config/${appDir}`, { ...deps, env })
    : null;
  const wslValue = wslRoot
    ? path.join(wslRoot, "SharedClientCache", "cache", "db", "local.db")
    : null;
  const wslDb = wslValue && existsSync(wslValue) ? wslValue : null;
  return wsl.resolveAllWin32Paths({
    nativeValue: native,
    wslValue: wslDb,
    env,
    platform: "win32",
  });
}

// Qoder CN keeps its own data directory (Application Support/QoderCN on macOS,
// AppData/Roaming/QoderCN on Windows, .config/QoderCN on Linux), so it needs
// its own path resolution. The token schema is identical to the international
// edition — the same QODER_USAGE_SQL and parser are reused with a distinct
// source/cursor namespace to avoid rowid collisions between the two DBs. CN
// honors its own env overrides (QODER_CN_DB_PATH / QODER_CN_HOME) so that
// QODER_HOME/QODER_DB_PATH, which point at the international install, never
// redirect the CN resolver onto the same database (that would double-count).
function resolveQoderCnDbPaths({
  home = os.homedir(),
  env = process.env,
  platform = process.platform,
  deps = {},
} = {}) {
  return resolveQoderDbPaths({ home, env, platform, appDir: "QoderCN", envPrefix: "QODER_CN", deps });
}

async function readQoderDbMessages(dbPath, sqliteOptions = {}) {
  if (!dbPath || !fssync.existsSync(dbPath)) return [];
  return readSqliteJsonRowsAsync(dbPath, QODER_USAGE_SQL, {
    label: "Qoder",
    ...sqliteOptions,
  });
}

function parseJsonObject(value) {
  if (!value) return null;
  if (value && typeof value === "object" && !Buffer.isBuffer(value)) return value;
  try {
    const parsed = JSON.parse(String(value));
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch (_error) {
    return null;
  }
}

function normalizeQoderTokens(tokenInfo) {
  const tokens = parseJsonObject(tokenInfo);
  if (!tokens) return null;
  const prompt = Number(tokens.prompt_tokens);
  const cached = Number(tokens.cached_tokens || 0);
  const completion = Number(tokens.completion_tokens);
  if (
    !Number.isFinite(prompt) ||
    !Number.isFinite(cached) ||
    !Number.isFinite(completion) ||
    prompt < 0 ||
    cached < 0 ||
    completion < 0
  ) {
    return null;
  }
  // Qoder's prompt_tokens already includes cached_tokens. Keep cached input in
  // its own column and report only the remainder as ordinary input, otherwise
  // cached context is counted twice in dashboards and cost calculations.
  const input = Math.max(0, Math.trunc(prompt) - Math.trunc(cached));
  const cachedInput = Math.min(Math.trunc(prompt), Math.trunc(cached));
  const output = Math.trunc(completion);
  return {
    input_tokens: input,
    cached_input_tokens: cachedInput,
    cache_creation_input_tokens: 0,
    output_tokens: output,
    reasoning_output_tokens: 0,
    total_tokens: Math.trunc(prompt) + output,
    billable_total_tokens: Math.trunc(prompt) + output,
  };
}

function qoderModelFromRow(row) {
  const direct = parseJsonObject(row?.model_info);
  const recordExtra = parseJsonObject(row?.record_extra);
  const preferred = parseJsonObject(row?.preferred_model_info);
  return (
    normalizeModelInput(direct?.model_key || direct?.modelKey) ||
    normalizeModelInput(recordExtra?.modelConfig?.key || recordExtra?.model_config?.key) ||
    normalizeModelInput(
      preferred?.model_key ||
      preferred?.modelKey ||
      preferred?.preferred_model ||
      preferred?.preferredModel,
    ) ||
    "qoder-agent"
  );
}

function qoderMessageKey(row) {
  const id = normalizeMessageKeyPart(row?.id);
  const sessionId = normalizeMessageKeyPart(row?.session_id);
  if (sessionId && id) return `${sessionId}|${id}`;
  if (id) return id;
  const rowId = row?.row_id;
  return rowId === null || rowId === undefined ? null : `row:${rowId}`;
}

function qoderProjectPath(row) {
  const raw = typeof row?.project_uri === "string" ? row.project_uri.trim() : "";
  if (!raw) return null;
  if (!raw.startsWith("file://")) return raw;
  try {
    return decodeURIComponent(new URL(raw).pathname);
  } catch (_error) {
    return null;
  }
}

async function parseQoderDbIncremental({
  dbMessages,
  dbPath,
  cursors,
  queuePath,
  projectQueuePath,
  onProgress,
  publicRepoResolver,
  sourceKey = "qoder",
  cursorKey = "qoder",
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const rows = Array.isArray(dbMessages) ? dbMessages : [];
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const qoderState = normalizeQoderState(cursors?.[cursorKey]);
  const touchedBuckets = new Set();
  const projectEnabled = typeof projectQueuePath === "string" && projectQueuePath.length > 0;
  const projectState = projectEnabled ? normalizeProjectState(cursors?.projectHourly) : null;
  const projectTouchedBuckets = projectEnabled ? new Set() : null;
  const projectMetaCache = projectEnabled ? new Map() : null;
  const publicRepoCache = projectEnabled ? new Map() : null;
  const requestOwners = new Map();
  for (const row of rows) {
    const messageKey = qoderMessageKey(row);
    if (
      !messageKey ||
      !normalizeQoderTokens(row?.token_info) ||
      !coerceEpochMs(row?.gmt_create)
    ) {
      continue;
    }
    const requestKey =
      normalizeMessageKeyPart(row?.request_id) ||
      normalizeMessageKeyPart(row?.session_id) ||
      messageKey;
    if (!requestOwners.has(requestKey)) requestOwners.set(requestKey, messageKey);
  }
  const cb = typeof onProgress === "function" ? onProgress : null;
  let messagesProcessed = 0;
  let eventsAggregated = 0;

  for (let index = 0; index < rows.length; index++) {
    const row = rows[index];
    const messageKey = qoderMessageKey(row);
    const currentBase = normalizeQoderTokens(row?.token_info);
    const timestampMs = coerceEpochMs(row?.gmt_create);
    const bucketStart = timestampMs
      ? toUtcHalfHourStart(new Date(timestampMs).toISOString())
      : null;
    if (!messageKey || !currentBase || !bucketStart) {
      messagesProcessed += 1;
      continue;
    }

    const requestKey =
      normalizeMessageKeyPart(row?.request_id) ||
      normalizeMessageKeyPart(row?.session_id) ||
      messageKey;
    const previous = qoderState.messages[messageKey];
    // Recompute request ownership from the complete ordered DB snapshot on
    // every pass. Qoder can attach token_info to an earlier assistant row only
    // after a later row was already counted; retaining the old per-message
    // owner in that case inflates one request to two conversations.
    const conversationCount = requestOwners.get(requestKey) === messageKey ? 1 : 0;

    const currentTotals = {
      ...currentBase,
      conversation_count: conversationCount,
    };
    const model = qoderModelFromRow(row);
    let projectKey = null;
    let projectRef = null;
    if (projectEnabled) {
      const projectPath = qoderProjectPath(row);
      if (projectPath) {
        const context = await resolveProjectContextForPath({
          startDir: wsl.mapWslCwdToUnc(projectPath, dbPath),
          projectMetaCache,
          publicRepoCache,
          publicRepoResolver,
          projectState,
        });
        projectKey = context?.projectKey || null;
        projectRef = context?.projectRef || null;
      }
    }

    const previousTotals =
      previous?.totals && typeof previous.totals === "object" ? previous.totals : null;
    const unchanged =
      previousTotals &&
      totalsKey(previousTotals) === totalsKey(currentTotals) &&
      previous.bucketStart === bucketStart &&
      previous.model === model &&
      (previous.projectKey || null) === projectKey;
    if (!unchanged) {
      if (previousTotals && previous.bucketStart && previous.model) {
        const oldBucket = getHourlyBucket(
          hourlyState,
          sourceKey,
          previous.model,
          previous.bucketStart,
        );
        subtractTotals(oldBucket.totals, previousTotals);
        touchedBuckets.add(bucketKey(sourceKey, previous.model, previous.bucketStart));
        if (projectEnabled && previous.projectKey) {
          const oldProjectBucket = getProjectBucket(
            projectState,
            previous.projectKey,
            sourceKey,
            previous.bucketStart,
            previous.projectRef || null,
          );
          subtractTotals(oldProjectBucket.totals, previousTotals);
          projectTouchedBuckets.add(
            projectBucketKey(previous.projectKey, sourceKey, previous.bucketStart),
          );
        }
      }

      const bucket = getHourlyBucket(hourlyState, sourceKey, model, bucketStart);
      addTotals(bucket.totals, currentTotals);
      touchedBuckets.add(bucketKey(sourceKey, model, bucketStart));
      if (projectEnabled && projectKey) {
        const projectBucket = getProjectBucket(
          projectState,
          projectKey,
          sourceKey,
          bucketStart,
          projectRef,
        );
        addTotals(projectBucket.totals, currentTotals);
        projectTouchedBuckets.add(projectBucketKey(projectKey, sourceKey, bucketStart));
      }
      qoderState.messages[messageKey] = {
        totals: currentTotals,
        conversationCount,
        requestKey,
        bucketStart,
        model,
        projectKey,
        projectRef,
        updatedAt: new Date().toISOString(),
      };
      eventsAggregated += 1;
    }

    messagesProcessed += 1;
    if (cb) {
      cb({
        index: index + 1,
        total: rows.length,
        messagesProcessed,
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
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  qoderState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors[cursorKey] = qoderState;
  if (projectState) {
    projectState.updatedAt = updatedAt;
    cursors.projectHourly = projectState;
  }
  return { messagesProcessed, eventsAggregated, bucketsQueued, projectBucketsQueued };
}

// ── Qoder (new) — ~/.qoder/projects JSONL (com.qoder.app.stable, 2026-08+) ──
//
// Qoder 2026-08 (app 0.1.2+) migrated from SharedClientCache/cache/db/local.db
// to Electron main.sqlite + ~/.qoder/projects/<slug>/<sessionId>.jsonl.
// The new transcript's message.usage no longer carries prompt_tokens — it is a
// credit-billed SDK: {input_tokens:0, output_tokens:0, credits:3.2, billable:true}.
// Only rows with authoritative token fields are counted: credit-only usage
// without tokens is intentionally not counted (usage stays unsupported, no
// token delta) because there is no first-party evidence for a credit→token
// rate. Cost was never estimated — no authoritative credit→USD rate is
// published, so total_cost_usd stays 0.
// Old local.db is kept as a legacy fallback; both sources now use distinct
// cursor namespaces (qoder vs qoderNew) via disjoint messageKey prefixes
// (row: vs jsonl:) but upload under the same source="qoder".

function resolveQoderProjectsDir({ home = os.homedir(), env = process.env, platform = process.platform, deps = {} } = {}) {
  const override = typeof env.QODER_PROJECTS_DIR === "string" && env.QODER_PROJECTS_DIR.trim()
    ? path.resolve(env.QODER_PROJECTS_DIR.trim())
    : null;
  if (override) return override;
  // QODER_HOME points at the app support dir for the legacy DB; the new
  // projects dir is always ~/.qoder regardless of QODER_HOME.
  // On Windows also probe WSL distro home (same pattern as other providers).
  if (platform === "win32" && !env.QODER_PROJECTS_DIR) {
    const discoverWslHome = deps.discoverWslHome || wsl.discoverWslHome;
    const wslRoot = wsl.shouldProbeWsl(env) ? discoverWslHome(".qoder", { ...deps, env }) : null;
    if (wslRoot) {
      const wslProjects = path.join(wslRoot, "projects");
      if ((deps.existsSync || fssync.existsSync)(wslProjects)) return wslProjects;
    }
  }
  return path.join(home, ".qoder", "projects");
}

function resolveQoderCnProjectsDir({ home = os.homedir(), env = process.env, platform = process.platform, deps = {} } = {}) {
  const override = typeof env.QODER_CN_PROJECTS_DIR === "string" && env.QODER_CN_PROJECTS_DIR.trim()
    ? path.resolve(env.QODER_CN_PROJECTS_DIR.trim())
    : null;
  if (override) return override;
  // The new CN app (com.qodercn.app.stable, 2026-08+) keeps its sessions in
  // ~/.qoder-cn/projects — a sibling of the international ~/.qoder, not a
  // shared directory. Pointing CN at ~/.qoder/projects made the "CN dir
  // diverges from international" guards in sync.js/status.js always false,
  // so new-version CN JSONL usage was silently never parsed (and on
  // international-only installs would have double-counted under qoder-cn).
  if (platform === "win32" && !env.QODER_CN_PROJECTS_DIR) {
    const discoverWslHome = deps.discoverWslHome || wsl.discoverWslHome;
    const wslRoot = wsl.shouldProbeWsl(env) ? discoverWslHome(".qoder-cn", { ...deps, env }) : null;
    if (wslRoot) {
      const wslProjects = path.join(wslRoot, "projects");
      if ((deps.existsSync || fssync.existsSync)(wslProjects)) return wslProjects;
    }
  }
  return path.join(home, ".qoder-cn", "projects");
}

async function listQoderNewSessionFiles(projectsDir) {
  const out = [];
  if (!projectsDir || !fssync.existsSync(projectsDir)) return out;
  async function walk(dir) {
    const entries = await safeReadDir(dir);
    for (const e of entries) {
      const p = path.join(dir, e.name);
      if (e.isDirectory()) {
        await walk(p);
      } else if (e.isFile() && e.name.endsWith(".jsonl")) {
        out.push(p);
      }
    }
  }
  await walk(projectsDir);
  out.sort((a, b) => a.localeCompare(b));
  return out;
}

function qoderNewModelFromRecord(record) {
  const msgModel = record?.message?.model;
  const direct = typeof msgModel === "string" ? msgModel.trim() : "";
  // CN BYOK routes embed an install-local provider UUID in the model id
  // ("qoder-custom-<uuid>/glm-5.3-flash"). Keep the bare model id so bucket
  // keys stay stable across reinstalls and don't fragment per user; official
  // ids (e.g. "qmodel_38max") have no prefix and pass through unchanged.
  const stripped = direct.replace(
    /^qoder-custom-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\//i,
    "",
  );
  return normalizeModelInput(stripped) || "qoder-agent";
}

function qoderNewMessageKey(record, filePath, lineIndex = 0) {
  const msgId = normalizeMessageKeyPart(record?.message?.id)
    || normalizeMessageKeyPart(record?.uuid)
    || normalizeMessageKeyPart(record?.id)
    || null;
  const sessionId = normalizeMessageKeyPart(record?.sessionId || record?.session_id);
  // Prefix to avoid collision with legacy row: keys; fallback includes line index
  // so multiple no-id records in the same file remain distinct.
  const fallbackSuffix = Number.isFinite(lineIndex) ? `${filePath}:${lineIndex}` : filePath;
  if (sessionId && msgId) return `jsonl:${sessionId}|${msgId}`;
  if (msgId) return `jsonl:${msgId}`;
  if (sessionId) return `jsonl:${sessionId}|${record?.uuid || fallbackSuffix}`;
  return `jsonl:${fallbackSuffix}|${record?.uuid || ""}`;
}

function qoderNewTimestampMs(record) {
  return coerceEpochMs(record?.timestamp)
    || coerceEpochMs(record?.message?.timestamp)
    || parseIsoTimestampMs(record?.timestamp)
    || parseIsoTimestampMs(record?.message?.timestamp)
    || 0;
}

function normalizeQoderNewTokens(usage) {
  if (!usage || typeof usage !== "object") return null;
  const credits = Number(usage.credits ?? usage.original_credits ?? 0);
  let input = Number(usage.input_tokens ?? 0);
  let cached = Number(usage.cache_read_input_tokens ?? usage.cached_tokens ?? 0);
  let cacheCreation = Number(usage.cache_creation_input_tokens ?? 0);
  let output = Number(usage.output_tokens ?? 0);
  // Guard against malformed numbers (NaN/Infinity/negative) — align with
  // legacy normalizeQoderTokens which returns null on such input.
  if (!Number.isFinite(input) || input < 0) input = 0;
  if (!Number.isFinite(cached) || cached < 0) cached = 0;
  if (!Number.isFinite(cacheCreation) || cacheCreation < 0) cacheCreation = 0;
  if (!Number.isFinite(output) || output < 0) output = 0;
  // Only rows with authoritative token fields are counted; anything else
  // falls through to null (unsupported, no token delta).
  if (input > 0 || cached > 0 || cacheCreation > 0 || output > 0) {
    const inp = Math.max(0, Math.trunc(input));
    const cach = Math.max(0, Math.trunc(cached));
    const out = Math.max(0, Math.trunc(output));
    const cc = Math.max(0, Math.trunc(cacheCreation));
    return {
      input_tokens: inp,
      cached_input_tokens: cach,
      cache_creation_input_tokens: cc,
      output_tokens: out,
      reasoning_output_tokens: 0,
      total_tokens: inp + cach + cc + out,
      billable_total_tokens: inp + cach + cc + out,
      credits: Number.isFinite(credits) && credits > 0 ? credits : 0,
      usage_precision: null,
    };
  }
  // Credit-only usage without authoritative token fields is intentionally
  // not counted (no token delta): there is no first-party evidence for a
  // credit→token rate. The caller still counts billable messages as
  // conversation activity.
  return null;
}

async function parseQoderNewIncremental({
  sessionFiles,
  cursors,
  queuePath,
  projectQueuePath,
  onProgress,
  sourceKey = "qoder",
  cursorKey = "qoderNew",
  publicRepoResolver,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const files = Array.isArray(sessionFiles) ? sessionFiles : [];
  // One-time migration: pre-#549 stored JSONL keys under the legacy "qoder"
  // cursor (same namespace as SQLite). Move them to the new isolated namespace
  // so history is not double-counted and legacy multi-install state is preserved.
  if (cursorKey === "qoderNew" && cursors?.qoder && !cursors?.qoderNew) {
    const legacy = normalizeQoderState(cursors.qoder);
    const jsonlEntries = Object.entries(legacy.messages).filter(([k]) => k.startsWith("jsonl:"));
    if (jsonlEntries.length > 0) {
      const migrated = {};
      for (const [k, v] of jsonlEntries) {
        migrated[k] = v;
        delete legacy.messages[k];
      }
      cursors.qoderNew = { messages: migrated, updatedAt: legacy.updatedAt || new Date().toISOString() };
    }
  }
  if (cursorKey === "qoderCnNew" && cursors?.["qoder-cn"] && !cursors?.["qoderCnNew"]) {
    const legacy = normalizeQoderState(cursors["qoder-cn"]);
    const jsonlEntries = Object.entries(legacy.messages).filter(([k]) => k.startsWith("jsonl:"));
    if (jsonlEntries.length > 0) {
      const migrated = {};
      for (const [k, v] of jsonlEntries) {
        migrated[k] = v;
        delete legacy.messages[k];
      }
      cursors["qoderCnNew"] = { messages: migrated, updatedAt: legacy.updatedAt || new Date().toISOString() };
    }
  }
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const qoderState = normalizeQoderState(cursors?.[cursorKey]);
  const projectEnabled = typeof projectQueuePath === "string" && projectQueuePath.length > 0;
  const projectState = projectEnabled ? normalizeProjectState(cursors?.projectHourly) : null;
  const projectTouchedBuckets = projectEnabled ? new Set() : null;
  const projectMetaCache = projectEnabled ? new Map() : null;
  const publicRepoCache = projectEnabled ? new Map() : null;
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;

  // Build current snapshot from all JSONL files
  const currentByKey = new Map();
  const fileCount = files.length;
  for (let fileIdx = 0; fileIdx < files.length; fileIdx++) {
    const filePath = files[fileIdx];
    let raw;
    try {
      raw = await fs.readFile(filePath, "utf8");
    } catch (_e) {
      continue;
    }
    const lines = raw.split("\n");
    for (let lineIdx = 0; lineIdx < lines.length; lineIdx++) {
      const line = lines[lineIdx];
      if (!line.trim()) continue;
      let record;
      try {
        record = JSON.parse(line);
      } catch (_e) {
        continue;
      }
      if (record?.type !== "assistant") continue;
      const msg = record?.message;
      if (!msg || msg.role !== "assistant") continue;
      // Skip synthetic sub-agent streaming chunks that carry no usage
      const usage = msg.usage;
      if (!usage || typeof usage !== "object") continue;
      const base = normalizeQoderNewTokens(usage);
      // Allow billable zero-token messages to still count conversation (no token delta)
      const isBillable = usage.billable !== false;
      if (!base && !isBillable) continue;
      const timestampMs = qoderNewTimestampMs(record);
      if (!timestampMs) continue;
      const bucketStart = toUtcHalfHourStart(new Date(timestampMs).toISOString());
      if (!bucketStart) continue;
      const messageKey = qoderNewMessageKey(record, filePath, lineIdx);
      if (!messageKey) continue;
      const model = qoderNewModelFromRecord(record);
      const totals = base ? {
        input_tokens: base.input_tokens,
        cached_input_tokens: base.cached_input_tokens,
        cache_creation_input_tokens: base.cache_creation_input_tokens,
        output_tokens: base.output_tokens,
        reasoning_output_tokens: 0,
        total_tokens: base.total_tokens,
        billable_total_tokens: base.billable_total_tokens,
        total_cost_usd: 0,
        usage_precision: base.usage_precision || undefined,
        conversation_count: 1,
      } : {
        input_tokens: 0,
        cached_input_tokens: 0,
        cache_creation_input_tokens: 0,
        output_tokens: 0,
        reasoning_output_tokens: 0,
        total_tokens: 0,
        billable_total_tokens: 0,
        total_cost_usd: 0,
        conversation_count: 1,
      };
      let projectKey = null;
      let projectRef = null;
      if (projectEnabled) {
        const rawCwd = typeof record?.cwd === "string" ? record.cwd.trim() : "";
        if (rawCwd) {
          const startDir = wsl.mapWslCwdToUnc(rawCwd, filePath);
          const context = await resolveProjectContextForPath({
            startDir,
            projectMetaCache,
            publicRepoCache,
            publicRepoResolver,
            projectState,
          });
          projectKey = context?.projectKey || null;
          projectRef = context?.projectRef || null;
        }
      }
      currentByKey.set(messageKey, {
        totals,
        bucketStart,
        model,
        projectKey,
        projectRef,
        filePath,
      });
    }
    if (cb && (fileIdx % 50 === 0 || fileIdx === files.length - 1)) {
      cb({
        index: fileIdx + 1,
        total: fileCount,
        messagesProcessed: currentByKey.size,
        eventsAggregated: 0,
        bucketsQueued: 0,
      });
    }
  }

  let messagesProcessed = currentByKey.size;
  let eventsAggregated = 0;

  // Subtract contributions that disappeared or changed
  for (const [key, prev] of Object.entries(qoderState.messages)) {
    if (!key.startsWith("jsonl:")) continue;
    const cur = currentByKey.get(key);
    const unchanged = cur
      && totalsKey(prev.totals) === totalsKey(cur.totals)
      && prev.bucketStart === cur.bucketStart
      && prev.model === cur.model
      && (prev.projectKey || null) === (cur.projectKey || null)
      && (prev.totals?.total_cost_usd || 0) === (cur.totals.total_cost_usd || 0);
    if (unchanged) continue;
    if (prev.totals && prev.bucketStart && prev.model) {
      const oldBucket = getHourlyBucket(hourlyState, sourceKey, prev.model, prev.bucketStart);
      subtractTotals(oldBucket.totals, prev.totals);
      touchedBuckets.add(bucketKey(sourceKey, prev.model, prev.bucketStart));
      if (projectEnabled && prev.projectKey) {
        const oldProjectBucket = getProjectBucket(projectState, prev.projectKey, sourceKey, prev.bucketStart, prev.projectRef || null);
        subtractTotals(oldProjectBucket.totals, prev.totals);
        projectTouchedBuckets.add(projectBucketKey(prev.projectKey, sourceKey, prev.bucketStart));
      }
    }
    if (cur) {
      const bucket = getHourlyBucket(hourlyState, sourceKey, cur.model, cur.bucketStart);
      addTotals(bucket.totals, cur.totals);
      if (cur.totals.usage_precision) bucket.usage_precision = cur.totals.usage_precision;
      // addTotals handles total_cost_usd via USD_TICKS, but credits cost is small; ensure it accumulates
      touchedBuckets.add(bucketKey(sourceKey, cur.model, cur.bucketStart));
      if (projectEnabled && cur.projectKey) {
        const projectBucket = getProjectBucket(projectState, cur.projectKey, sourceKey, cur.bucketStart, cur.projectRef);
        addTotals(projectBucket.totals, cur.totals);
        if (cur.totals.usage_precision) projectBucket.usage_precision = cur.totals.usage_precision;
        projectTouchedBuckets.add(projectBucketKey(cur.projectKey, sourceKey, cur.bucketStart));
      }
      qoderState.messages[key] = {
        totals: cur.totals,
        conversationCount: cur.totals.conversation_count,
        bucketStart: cur.bucketStart,
        model: cur.model,
        projectKey: cur.projectKey,
        projectRef: cur.projectRef,
        updatedAt: new Date().toISOString(),
      };
      eventsAggregated += 1;
    } else {
      delete qoderState.messages[key];
      eventsAggregated += 1;
    }
  }

  // Add brand-new keys
  for (const [key, cur] of currentByKey.entries()) {
    if (qoderState.messages[key]) continue;
    const bucket = getHourlyBucket(hourlyState, sourceKey, cur.model, cur.bucketStart);
    addTotals(bucket.totals, cur.totals);
    if (cur.totals.usage_precision) bucket.usage_precision = cur.totals.usage_precision;
    touchedBuckets.add(bucketKey(sourceKey, cur.model, cur.bucketStart));
    if (projectEnabled && cur.projectKey) {
      const projectBucket = getProjectBucket(projectState, cur.projectKey, sourceKey, cur.bucketStart, cur.projectRef);
      addTotals(projectBucket.totals, cur.totals);
      if (cur.totals.usage_precision) projectBucket.usage_precision = cur.totals.usage_precision;
      projectTouchedBuckets.add(projectBucketKey(cur.projectKey, sourceKey, cur.bucketStart));
    }
    qoderState.messages[key] = {
      totals: cur.totals,
      conversationCount: cur.totals.conversation_count,
      bucketStart: cur.bucketStart,
      model: cur.model,
      projectKey: cur.projectKey,
      projectRef: cur.projectRef,
      updatedAt: new Date().toISOString(),
    };
    eventsAggregated += 1;
  }

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const projectBucketsQueued = projectEnabled
    ? await enqueueTouchedProjectBuckets({ projectQueuePath, projectState, projectTouchedBuckets })
    : 0;
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  qoderState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors[cursorKey] = qoderState;
  if (projectState) {
    projectState.updatedAt = updatedAt;
    cursors.projectHourly = projectState;
  }
  return { messagesProcessed, eventsAggregated, bucketsQueued, projectBucketsQueued };
}

module.exports = {
  resolveQoderDbPath,
  resolveQoderDbPaths,
  resolveQoderCnDbPaths,
  readQoderDbMessages,
  normalizeQoderTokens,
  parseQoderDbIncremental,
  resolveQoderProjectsDir,
  resolveQoderCnProjectsDir,
  listQoderNewSessionFiles,
  normalizeQoderNewTokens,
  parseQoderNewIncremental,
};
