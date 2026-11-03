// kiro usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fs = require("node:fs/promises");
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const readline = require("node:readline");
const { ensureDir, writeJson, chmod600IfPossible } = require("../fs");
const { readKiroJsonlTokens } = require("../kiro-jsonl");
const { readSqliteJsonRows } = require("../sqlite-reader");
const {
  enqueueTouchedBuckets,
  normalizeHourlyState,
  getHourlyBucket,
  addTotals,
  toUtcHalfHourStart,
  bucketKey,
  toNonNegativeInt,
  sqliteStringLiteral,
  isUncPath,
  snapshotSqliteDb,
  sqliteDbContainsIds,
  sampleRecentKeys,
} = require("./core");

// ---------------------------------------------------------------------------
// Kiro token tracking (reads from devdata.sqlite or tokens_generated.jsonl)
// ---------------------------------------------------------------------------

// Kiro IDE (VS Code fork) globalStorage lives under the editor config root,
// which differs per platform: %APPDATA% on Windows, ~/.config on Linux/WSL,
// ~/Library/Application Support on macOS.
function resolveKiroBasePath(env = process.env) {
  const home = os.homedir();
  const suffix = ["Kiro", "User", "globalStorage", "kiro.kiroagent"];
  if (process.platform === "win32") {
    const appData = typeof env.APPDATA === "string" && env.APPDATA.trim().length > 0
      ? env.APPDATA.trim()
      : path.join(home, "AppData", "Roaming");
    return path.join(appData, ...suffix);
  }
  if (process.platform === "linux") {
    const configHome = typeof env.XDG_CONFIG_HOME === "string" && env.XDG_CONFIG_HOME.trim().length > 0
      ? env.XDG_CONFIG_HOME.trim()
      : path.join(home, ".config");
    return path.join(configHome, ...suffix);
  }
  return path.join(home, "Library", "Application Support", ...suffix);
}

function resolveKiroDbPath(basePath) {
  return path.join(basePath || resolveKiroBasePath(), "dev_data", "devdata.sqlite");
}

function resolveKiroJsonlPath(basePath) {
  return path.join(basePath || resolveKiroBasePath(), "dev_data", "tokens_generated.jsonl");
}

function readKiroDbTokens(dbPath, sinceId, sqliteOptions = {}) {
  if (!dbPath || !fssync.existsSync(dbPath)) return [];
  const minId = Number.isFinite(sinceId) && sinceId > 0 ? sinceId : 0;
  const sql = `SELECT id, model, provider, tokens_prompt, tokens_generated, timestamp FROM tokens_generated WHERE id > ${minId} ORDER BY id ASC`;

  // WSL installs are read over the \\wsl$ UNC bridge; sqlite3 cannot safely
  // open WAL databases there, so copy db (+ sidecars) to tmp first.
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
      label: "Kiro",
      maxBuffer: 10 * 1024 * 1024,
      timeout: 15_000,
      ...sqliteOptions,
    });
  } finally {
    if (snapshot) snapshot.cleanup();
  }
}

// Build a sorted timeline of model usage from Kiro .chat metadata files
function buildKiroModelTimeline(basePath) {
  const timeline = []; // [{ startMs, endMs, model }]
  if (!basePath || !fssync.existsSync(basePath)) return timeline;
  let dirs;
  try {
    dirs = fssync.readdirSync(basePath, { withFileTypes: true });
  } catch (_e) {
    return timeline;
  }
  for (const entry of dirs) {
    if (!entry.isDirectory()) continue;
    const dirPath = path.join(basePath, entry.name);
    let files;
    try {
      files = fssync.readdirSync(dirPath).filter((f) => f.endsWith(".chat"));
    } catch (_e) {
      continue;
    }
    for (const file of files) {
      try {
        const raw = fssync.readFileSync(path.join(dirPath, file), "utf8");
        const data = JSON.parse(raw);
        const meta = data?.metadata;
        if (!meta?.modelId || !meta?.startTime) continue;
        timeline.push({
          startMs: meta.startTime,
          endMs: meta.endTime || meta.startTime,
          model: String(meta.modelId),
        });
      } catch (_e) {}
    }
  }
  timeline.sort((a, b) => a.startMs - b.startMs);
  return timeline;
}

// Find the model for a given UTC timestamp string using the .chat timeline
function resolveKiroModel(timeline, utcTimestamp) {
  if (!timeline.length || !utcTimestamp) return null;
  const ts = new Date(utcTimestamp).getTime();
  if (!Number.isFinite(ts)) return null;

  // Binary search for the closest .chat entry
  let lo = 0;
  let hi = timeline.length - 1;
  let best = null;
  let bestDist = Infinity;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    const entry = timeline[mid];
    // Check if timestamp falls within the .chat execution window
    if (ts >= entry.startMs && ts <= entry.endMs) return entry.model;
    const dist = Math.min(Math.abs(ts - entry.startMs), Math.abs(ts - entry.endMs));
    if (dist < bestDist) {
      bestDist = dist;
      best = entry.model;
    }
    if (ts < entry.startMs) hi = mid - 1;
    else lo = mid + 1;
  }
  // Only match if within 10 minutes
  return bestDist < 10 * 60 * 1000 ? best : null;
}

// Normalize Kiro internal model IDs to readable names
// e.g. "CLAUDE_SONNET_4_20250514_V1_0" → "claude-sonnet-4"
function normalizeKiroModelName(raw) {
  if (!raw || typeof raw !== "string") return null;
  let name = raw.trim();
  if (!name) return null;
  // Already lowercase with dashes (e.g. "claude-opus-4.5") → keep as-is
  if (name === name.toLowerCase() && name.includes("-")) return name;
  // UPPER_SNAKE_CASE internal names: strip date/version suffixes, convert to lowercase-dash
  name = name
    .replace(/_\d{8}_V\d+_\d+$/i, "") // remove _20250514_V1_0
    .replace(/_V\d+$/i, "") // remove _V1
    .toLowerCase()
    .replace(/_/g, "-");
  return name || null;
}

async function parseKiroIncremental({ basePath, dbPath, jsonlPath, cursors, queuePath, onProgress, sqliteOptions } = {}) {
  await ensureDir(path.dirname(queuePath));
  const kiroState = cursors.kiro && typeof cursors.kiro === "object" ? cursors.kiro : {};
  const lastDbId = typeof kiroState.lastDbId === "number"
    ? kiroState.lastDbId
    : (typeof kiroState.lastId === "number" ? kiroState.lastId : 0);
  const jsonlState = kiroState.jsonl && typeof kiroState.jsonl === "object" ? kiroState.jsonl : {};
  const lastJsonlLine = typeof jsonlState.lastLine === "number" ? jsonlState.lastLine : 0;

  const resolvedDbPath = dbPath || resolveKiroDbPath(basePath);
  const resolvedJsonlPath = jsonlPath || resolveKiroJsonlPath(basePath);

  // Try SQLite first, fall back to JSONL.
  let rows = [];
  let nextDbId = lastDbId;
  let nextJsonlLine = lastJsonlLine;
  let nextJsonlCursor = jsonlState;
  let usingDb = false;
  if (fssync.existsSync(resolvedDbPath)) {
    rows = readKiroDbTokens(resolvedDbPath, lastDbId, sqliteOptions);
    usingDb = true;
    // DB and JSONL are siblings for the same usage events. If the DB ever
    // disappears (corrupted / wiped) and we fall back to JSONL in a later
    // run, we must not re-read lines that the DB path already consumed.
    // Advance the JSONL line cursor to the current file tail.
    if (fssync.existsSync(resolvedJsonlPath)) {
      const tail = await readKiroJsonlTokens(resolvedJsonlPath, jsonlState, { countOnly: true });
      nextJsonlCursor = tail.cursor;
      nextJsonlLine = tail.lineCount;
    }
  } else if (fssync.existsSync(resolvedJsonlPath)) {
    const jsonlResult = await readKiroJsonlTokens(resolvedJsonlPath, jsonlState);
    rows = jsonlResult.rows;
    nextJsonlLine = jsonlResult.lineCount;
    nextJsonlCursor = jsonlResult.cursor;
    if (jsonlResult.reset) {
      cursors.kiro = {
        ...kiroState,
        lastDbId,
        jsonl: { ...nextJsonlCursor, updatedAt: new Date().toISOString() },
        updatedAt: new Date().toISOString(),
      };
      return {
        recordsProcessed: 0,
        eventsAggregated: 0,
        bucketsQueued: 0,
      };
    }
  } else {
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }
  if (rows.length === 0) {
    cursors.kiro = {
      ...kiroState,
      lastDbId,
      jsonl: { ...nextJsonlCursor, updatedAt: new Date().toISOString() },
      updatedAt: new Date().toISOString(),
    };
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  // Build model timeline from .chat files for model name resolution.
  // Must come from the SAME install as the rows being parsed (dual-install:
  // a WSL install's rows must not be resolved against native .chat files).
  const timelineBase = basePath
    || (dbPath ? path.dirname(path.dirname(dbPath)) : resolveKiroBasePath());
  const modelTimeline = buildKiroModelTimeline(timelineBase);

  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;
  let eventsAggregated = 0;
  let maxId = lastDbId;

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const inputTokens = toNonNegativeInt(row.tokens_prompt);
    const outputTokens = toNonNegativeInt(row.tokens_generated);
    if (inputTokens === 0 && outputTokens === 0) continue;

    // timestamp format: "2026-01-09 15:25:30" (UTC from SQLite DEFAULT CURRENT_TIMESTAMP)
    const ts = row.timestamp ? row.timestamp.replace(" ", "T") + "Z" : null;
    const bucketStart = ts ? toUtcHalfHourStart(ts) : null;
    if (!bucketStart) continue;

    // Resolve actual model from .chat timeline, fallback to "kiro-agent"
    const resolvedModel = resolveKiroModel(modelTimeline, ts);
    const model = normalizeKiroModelName(resolvedModel) || "kiro-agent";

    const delta = {
      input_tokens: inputTokens,
      cached_input_tokens: 0,
      output_tokens: outputTokens,
      reasoning_output_tokens: 0,
      total_tokens: inputTokens + outputTokens,
      conversation_count: 1,
    };

    const bucket = getHourlyBucket(hourlyState, "kiro", model, bucketStart);
    addTotals(bucket.totals, delta);
    touchedBuckets.add(bucketKey("kiro", model, bucketStart));
    eventsAggregated++;

    if (usingDb && row.id && row.id > maxId) maxId = row.id;

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

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.kiro = {
    ...kiroState,
    lastId: maxId,
    lastDbId: maxId,
    jsonl: { ...nextJsonlCursor, updatedAt },
    updatedAt,
  };

  return { recordsProcessed: rows.length, eventsAggregated, bucketsQueued };
}

function kiroInstallOwnsCursor(dbPath, flatState, sqliteOptions) {
  // The flat cursor tracks the max AUTOINCREMENT id it consumed. An install
  // whose tokens_generated table never reached that id cannot be the flat
  // cursor's host. Both installs containing the id (short/equal histories)
  // yields two hits upstream → caller seeds every namespace (safe).
  const lastDbId = typeof flatState?.lastDbId === "number" && Number.isInteger(flatState.lastDbId) && flatState.lastDbId > 0
    ? flatState.lastDbId
    : (typeof flatState?.lastId === "number" && Number.isInteger(flatState.lastId) && flatState.lastId > 0
      ? flatState.lastId
      : 0);
  if (!lastDbId) return false;
  return sqliteDbContainsIds(dbPath, "tokens_generated", [String(lastDbId)], sqliteOptions);
}

// Kiro CLI request_ids live inside the conversations_v2 JSON payload, not in
// an id column — probe via json_each instead of sqliteDbContainsIds.
function kiroCliDbContainsRequestIds(dbPath, requestIds, sqliteOptions = {}) {
  if (!dbPath || !Array.isArray(requestIds) || requestIds.length === 0) return false;
  if (!fssync.existsSync(dbPath)) return false;
  const inList = requestIds.map(sqliteStringLiteral).join(",");
  // Alias both tables: conversations_v2.value and json_each's own `value`
  // column collide — a bare `value` is "ambiguous column name" and sqlite
  // errors out (which readSqliteJsonRows swallows into []).
  const sql =
    "SELECT 1 FROM conversations_v2 AS c, " +
    "json_each(json_extract(c.value, '$.user_turn_metadata.requests')) AS je " +
    `WHERE json_extract(je.value, '$.request_id') IN (${inList}) LIMIT 1`;

  let snapshot = null;
  let effectiveDbPath = dbPath;
  if (isUncPath(dbPath)) {
    try {
      snapshot = snapshotSqliteDb(dbPath);
      effectiveDbPath = snapshot.path;
    } catch (_e) { }
  }
  try {
    const rows = readSqliteJsonRows(effectiveDbPath, sql, {
      label: "InstallProbe",
      maxBuffer: 1024 * 1024,
      timeout: 10_000,
      ...sqliteOptions,
    });
    return Array.isArray(rows) && rows.length > 0;
  } catch (_e) {
    return false;
  } finally {
    if (snapshot) snapshot.cleanup();
  }
}

function kiroCliInstallOwnsCursor(dbPath, flatState, sqliteOptions) {
  // Only SQLite-origin request_id UUIDs are probe evidence: session-file
  // entries synthesize `${sessionId}:${loopRand}` keys or carry a session_id
  // tag, and neither appears in conversations_v2. Filter BEFORE sampling so
  // recent session-file churn cannot blind the probe to older SQLite ids.
  const requests =
    flatState?.requests && typeof flatState.requests === "object" ? flatState.requests : {};
  const sqliteKeyed = {};
  for (const [k, v] of Object.entries(requests)) {
    if (k.includes(":")) continue;
    if (v && typeof v === "object" && typeof v.session_id === "string" && v.session_id) continue;
    sqliteKeyed[k] = v;
  }
  const sample = sampleRecentKeys(sqliteKeyed);
  return kiroCliDbContainsRequestIds(dbPath, sample, sqliteOptions);
}
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// Kiro CLI — reads historical conversation state from
// ~/Library/Application Support/kiro-cli/data.sqlite3 (table conversations_v2).
// Kiro CLI does NOT store explicit token counts locally. Each request row
// carries: user_prompt_length (chars), response_size (chars), model_id,
// request_start_timestamp_ms, message_id. We approximate tokens at 4 chars /
// token. Source is merged with Kiro IDE (source='kiro') and canonicalized
// model names are used so CLI and IDE rows collapse when they refer to the
// same underlying Bedrock model. Cursor state is per-request-id so mutable
// requests can be reprocessed (subtract-old/add-new on fingerprint change).
// ─────────────────────────────────────────────────────────────────────────────

const KIRO_CLI_CHARS_PER_TOKEN = 4;
const KIRO_CLI_CREDITS_SIDECAR = "kiro-credits.json";

function resolveKiroCliDbPath(env = process.env) {
  if (env.KIRO_CLI_DB_PATH) return env.KIRO_CLI_DB_PATH;
  const home = env.HOME || os.homedir();
  if (process.platform === "win32") {
    // Speculative: Kiro CLI descends from Amazon Q CLI, which ships
    // macOS/Linux only — Windows users run it inside WSL (handled by the
    // dual-install path in sync). Kept as the conventional %LOCALAPPDATA%
    // location so a future native build is picked up; harmless when absent.
    const localAppData = typeof env.LOCALAPPDATA === "string" && env.LOCALAPPDATA.trim().length > 0
      ? env.LOCALAPPDATA.trim()
      : path.join(home, "AppData", "Local");
    return path.join(localAppData, "kiro-cli", "data.sqlite3");
  }
  if (process.platform === "linux") {
    const dataHome = typeof env.XDG_DATA_HOME === "string" && env.XDG_DATA_HOME.trim().length > 0
      ? env.XDG_DATA_HOME.trim()
      : path.join(home, ".local", "share");
    return path.join(dataHome, "kiro-cli", "data.sqlite3");
  }
  return path.join(home, "Library", "Application Support", "kiro-cli", "data.sqlite3");
}

// Bug-4: canonical UUID shape — 8-4-4-4-12 hex groups. The looser
// /^[0-9a-f-]{36}\.json$/ form accepted `36 hyphens`.json or 36 hex with
// no hyphens. kiro-cli writes proper UUIDs; lock to the canonical shape.
const KIRO_CLI_SESSION_FILE_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.json$/i;
// The directory prefix and direct messages.jsonl child are the stable
// contract. Do not assume the suffix will always remain a UUID: Kiro has
// already changed this layout once, and an opaque non-empty session id is
// sufficient to keep discovery both bounded and forward-compatible.
const KIRO_CLI_V2_SESSION_DIR_RE = /^sess_.+$/;

// Lists both Kiro CLI session layouts:
//   legacy: ~/.kiro/sessions/cli/{uuid}.json
//   2.13+:  ~/.kiro/sessions/{workspaceHash}/sess_{uuid}/messages.jsonl
//
// Legacy .json files are rewritten atomically per turn. The 2.13+ JSONL is
// append-only and may be read while a session is active. Only the direct
// messages.jsonl child is accepted; sub-executions and arbitrary nested JSONL
// files are intentionally excluded.
//
// TASK-014: env.HOME is honored (symmetric with resolveKiroCliDbPath) so
// callers can redirect to a tmp home for hermetic tests/CI.
function resolveKiroCliSessionFiles(env = process.env) {
  const home = env.HOME || os.homedir();
  const kiroHome = env.KIRO_HOME || path.join(home, ".kiro");
  const sessionsRoot = path.join(kiroHome, "sessions");
  if (!fssync.existsSync(sessionsRoot)) return [];
  const files = [];

  const legacyDir = path.join(sessionsRoot, "cli");
  try {
    for (const entry of fssync.readdirSync(legacyDir)) {
      // TASK-003: only canonical {uuid}.json files; backups, scratch,
      // typos are skipped so they don't feed JSON.parse garbage.
      if (!KIRO_CLI_SESSION_FILE_RE.test(entry)) continue;
      files.push(path.join(legacyDir, entry));
    }
  } catch {
    // ignore read errors
  }

  try {
    const workspaceDirs = fssync.readdirSync(sessionsRoot, {
      withFileTypes: true,
    });
    for (const workspace of workspaceDirs) {
      if (!workspace.isDirectory() || workspace.name === "cli") continue;
      const workspacePath = path.join(sessionsRoot, workspace.name);
      let sessionDirs;
      try {
        sessionDirs = fssync.readdirSync(workspacePath, {
          withFileTypes: true,
        });
      } catch {
        continue;
      }
      for (const session of sessionDirs) {
        if (
          !session.isDirectory() ||
          !KIRO_CLI_V2_SESSION_DIR_RE.test(session.name)
        ) {
          continue;
        }
        const messagesPath = path.join(
          workspacePath,
          session.name,
          "messages.jsonl",
        );
        if (fssync.existsSync(messagesPath)) files.push(messagesPath);
      }
    }
  } catch {
    // ignore read errors
  }

  return files.sort();
}

// Build char-count maps from a .jsonl sibling file. Lets us approximate
// per-turn tokens when the live session's input_token_count /
// output_token_count fields are 0 (kiro-cli does not persist real token
// counts; billing is credit-based).
//
// Returns:
//   byMessage:       message_id -> assistant+toolUse char count
//   messageKind:     message_id -> jsonl event kind
//   turnPromptChars: turn_index -> input chars attributed to that turn
//
// Input attribution: Kiro CLI's turn.message_ids only records
// AssistantMessage / ToolResults ids, NEVER the user Prompt id. So the
// Prompt event is invisible if you look it up by message_id. To recover
// the per-turn user input, we walk the jsonl in timestamp order and buffer
// Prompt chars until the next AssistantMessage that belongs to a turn
// (turnMessageIds provides that mapping). The first such AssistantMessage
// "claims" the buffered Prompt chars for its turn, and the buffer resets.
// Later cycles within the same turn (Assistant → ToolResults → Assistant)
// do not re-attribute.
async function readKiroCliMessageChars(jsonlPath, turnMessageIds) {
  const result = {
    byMessage: new Map(),
    messageKind: new Map(),
    turnPromptChars: new Map(),
  };
  if (!jsonlPath || !fssync.existsSync(jsonlPath)) return result;
  // TASK-005: stream via readline so multi-MB .jsonl files (heavy tool-use
  // sessions) don't block the sync event loop by buffering whole-file.
  let stream;
  try {
    stream = fssync.createReadStream(jsonlPath, { encoding: "utf8" });
  } catch {
    return result;
  }
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  const midToTurn =
    turnMessageIds instanceof Map ? turnMessageIds : new Map();
  const attributedTurns = new Set();
  let pendingPromptChars = 0;
  // Bug-5: wrap the streamed iteration. Mid-read errors (file deleted or
  // truncated while kiro-cli is writing) would otherwise propagate up and
  // crash the whole sync pass. On error we return the partial result and
  // let the next sync re-read fresh.
  try {
    for await (const line of rl) {
      if (!line || !line.trim()) continue;
      let evt;
      try {
        evt = JSON.parse(line);
      } catch {
        continue;
      }
      const data = evt && evt.data;
      if (!data || typeof data !== "object") continue;
      const mid = data.message_id;
      if (!mid) continue;
      const content = Array.isArray(data.content) ? data.content : [];
      let chars = 0;
      for (const c of content) {
        if (!c || typeof c !== "object") continue;
        if (c.kind === "text" && typeof c.data === "string") {
          chars += c.data.length;
        } else if (c.kind === "toolUse" && c.data && typeof c.data === "object") {
          try {
            chars += JSON.stringify(c.data.input || {}).length;
          } catch {
            // ignore
          }
        }
      }
      result.byMessage.set(mid, (result.byMessage.get(mid) || 0) + chars);
      if (!result.messageKind.has(mid)) result.messageKind.set(mid, evt.kind);

      if (evt.kind === "Prompt") {
        pendingPromptChars += chars;
      } else if (evt.kind === "AssistantMessage" && midToTurn.has(mid)) {
        const turnIdx = midToTurn.get(mid);
        if (!attributedTurns.has(turnIdx)) {
          result.turnPromptChars.set(turnIdx, pendingPromptChars);
          attributedTurns.add(turnIdx);
          pendingPromptChars = 0;
        }
      }
    }
  } catch {
    // partial data — return what we have.
  }
  return result;
}

// Extract flat per-turn records from a live session .json + its .jsonl
// sibling. Returns [{ request_id, model_id, request_start_timestamp_ms,
// input_tokens, output_tokens }]. We use the same request_id dedup slot as
// the SQLite path so mutations (turn rewritten on next flush) go through
// the subtract-old/add-new path in parseKiroCliIncremental.
async function readKiroCliSessionTurns(jsonPath) {
  if (!jsonPath || !fssync.existsSync(jsonPath)) return [];
  let parsed;
  try {
    parsed = JSON.parse(fssync.readFileSync(jsonPath, "utf8"));
  } catch {
    return [];
  }
  if (!parsed || typeof parsed !== "object") return [];
  const turns = Array.isArray(
    parsed?.session_state?.conversation_metadata?.user_turn_metadatas,
  )
    ? parsed.session_state.conversation_metadata.user_turn_metadatas
    : [];
  if (turns.length === 0) return [];

  const modelInfo = parsed?.session_state?.rts_model_state?.model_info || null;
  const sessionModelId =
    (modelInfo && (modelInfo.model_id || modelInfo.model_name)) || null;
  const sessionId =
    typeof parsed.session_id === "string" ? parsed.session_id : path.basename(jsonPath, ".json");

  // Build turn_index -> Set(message_id) so the jsonl walker can attribute
  // orphaned Prompt events (not referenced by turn.message_ids) to the
  // right turn. The turn.message_ids list only contains AssistantMessage
  // and ToolResults ids; Prompt ids appear in the jsonl stream only.
  const turnMessageIds = new Map();
  for (let i = 0; i < turns.length; i++) {
    const t = turns[i];
    if (!t || !Array.isArray(t.message_ids)) continue;
    for (const mid of t.message_ids) {
      if (typeof mid === "string" && mid) turnMessageIds.set(mid, i);
    }
  }

  // Load sibling .jsonl for char-count fallback.
  const jsonlPath = jsonPath.replace(/\.json$/, ".jsonl");
  const charMap = await readKiroCliMessageChars(jsonlPath, turnMessageIds);

  const flat = [];
  for (let turnIdx = 0; turnIdx < turns.length; turnIdx++) {
    const turn = turns[turnIdx];
    if (!turn || typeof turn !== "object") continue;
    // TASK-001: preserve the integer 0. `|| null` would coerce a valid
    // loop_id.rand=0 into a message_id fallback, splitting the dedup
    // namespace across runs that see 0 vs runs that don't.
    const loopRand =
      turn.loop_id && typeof turn.loop_id === "object"
        ? turn.loop_id.rand ?? turn.loop_id.seed ?? null
        : null;
    const messageIds = Array.isArray(turn.message_ids) ? turn.message_ids : [];
    const requestId = loopRand != null ? `${sessionId}:${loopRand}` : (messageIds[0] || null);
    if (!requestId) continue;

    // Prefer real token counts if kiro-cli populated them.
    let inputTokens = toNonNegativeInt(turn.input_token_count);
    let outputTokens = toNonNegativeInt(turn.output_token_count);

    if (inputTokens === 0 && outputTokens === 0) {
      // Fall back to char-count approximation. Input chars come from the
      // sequential Prompt attribution (see readKiroCliMessageChars);
      // output chars come from AssistantMessage+toolUse bodies referenced
      // by turn.message_ids.
      const promptChars = charMap.turnPromptChars.get(turnIdx) || 0;
      let assistantChars = 0;
      for (const mid of messageIds) {
        const chars = charMap.byMessage.get(mid) || 0;
        const kind = charMap.messageKind.get(mid);
        if (kind === "AssistantMessage") assistantChars += chars;
      }
      inputTokens = Math.floor(promptChars / KIRO_CLI_CHARS_PER_TOKEN);
      outputTokens = Math.floor(assistantChars / KIRO_CLI_CHARS_PER_TOKEN);
    }

    // TASK-006: timestamp precedence matches SQLite's
    // request_start_timestamp_ms so a turn that migrates SQLite ↔
    // session-file buckets identically across tiers (previously a turn
    // straddling a half-hour boundary bucketed differently per source
    // because session files use end_timestamp while SQLite uses start).
    //   1. turn.request_start_timestamp_ms   (numeric ms, SQLite shape)
    //   2. turn.start_timestamp              (ISO string)
    //   3. turn.end_timestamp                (ISO string, legacy fallback)
    let tsMs = NaN;
    if (Number.isFinite(Number(turn.request_start_timestamp_ms))) {
      tsMs = Number(turn.request_start_timestamp_ms);
    } else if (turn.start_timestamp) {
      tsMs = Date.parse(turn.start_timestamp);
    } else if (turn.end_timestamp) {
      tsMs = Date.parse(turn.end_timestamp);
    }
    if (!Number.isFinite(tsMs) || tsMs <= 0) continue;

    flat.push({
      request_id: requestId,
      session_model_id: sessionModelId,
      message_id: messageIds[0] || null,
      // Turn-granular migration match: surface the full list so the
      // cross-source retraction in parseKiroCliIncremental can drop this
      // specific turn iff any of its assistant/tool_result message_ids
      // appears in SQLite. Session-level matching over-retracts newer
      // turns in an active session whose older turns have already
      // flushed to SQLite.
      all_message_ids: messageIds.slice(),
      model_id: turn.model_id || sessionModelId,
      request_start_timestamp_ms: tsMs,
      // D-1 / Bug-2: tag with session_id so the retraction pass can match
      // session-origin entries even when the requestId format has no
      // colon (no-loop_id fallback uses a bare message_id UUID that would
      // otherwise be indistinguishable from SQLite's UUID keys).
      session_id: sessionId,
      // For the parser, we feed the ALREADY-approximated tokens directly via
      // a special sentinel field. The parser will divide chars by
      // KIRO_CLI_CHARS_PER_TOKEN; bypass that by pre-multiplying here.
      user_prompt_length: inputTokens * KIRO_CLI_CHARS_PER_TOKEN,
      response_size: outputTokens * KIRO_CLI_CHARS_PER_TOKEN,
    });
  }
  return flat;
}

function kiroCliV2ContentChars(value) {
  if (typeof value === "string") return value.length;
  if (Array.isArray(value)) {
    return value.reduce(
      (sum, item) => sum + kiroCliV2ContentChars(item),
      0,
    );
  }
  if (!value || typeof value !== "object") return 0;
  for (const key of ["content", "text", "value", "parts", "entries"]) {
    if (value[key] !== undefined) {
      return kiroCliV2ContentChars(value[key]);
    }
  }
  return 0;
}

function kiroCliV2UsageCredits(payload) {
  const summaries = Array.isArray(payload?.promptTurnSummaries)
    ? payload.promptTurnSummaries
    : [];
  let totalCredits = 0;
  let hasCreditEntry = false;
  for (const summary of summaries) {
    if (!summary || typeof summary !== "object") continue;
    const unit = String(summary.unit || "").trim().toLowerCase();
    if (unit !== "credit" && unit !== "credits") continue;
    const usage = Number(summary.usage);
    if (!Number.isFinite(usage) || usage < 0) continue;
    totalCredits += usage;
    hasCreditEntry = true;
  }
  return {
    totalCredits,
    recordCount: hasCreditEntry ? 1 : 0,
  };
}

// Kiro CLI 2.13+ writes event-sourced sessions under
// ~/.kiro/sessions/<workspaceHash>/sess_<uuid>/messages.jsonl. The records do
// not carry token counts, but user/assistant text, tool_result payloads, turn
// boundaries, timestamps, and assistant reasoningModelId are sufficient for
// the same 4 chars/token approximation used by the legacy CLI reader.
// tool_result content counts as INPUT: tool output (file reads, command
// output, search results) is fed back to the model as context on the next
// request — skipping it undercounted tool-heavy sessions by ~67% (#366).
async function readKiroCliV2SessionTurns(messagesPath) {
  if (
    !messagesPath ||
    path.basename(messagesPath) !== "messages.jsonl" ||
    !fssync.existsSync(messagesPath)
  ) {
    return {
      turns: [],
      credits: { totalCredits: 0, recordCount: 0, latestAt: null },
    };
  }

  const sessionDir = path.dirname(messagesPath);
  let sessionMeta = {};
  try {
    sessionMeta = JSON.parse(
      fssync.readFileSync(path.join(sessionDir, "session.json"), "utf8"),
    );
  } catch {
    // messages.jsonl is self-contained enough to parse without session.json.
  }

  const sessionId =
    typeof sessionMeta?.id === "string" && sessionMeta.id
      ? sessionMeta.id
      : path.basename(sessionDir);
  const fallbackModel =
    typeof sessionMeta?.modelId === "string" ? sessionMeta.modelId : null;
  const fallbackTimestamp = Date.parse(
    sessionMeta?.createdAt || sessionMeta?.lastModifiedAt || "",
  );

  const flat = [];
  let stream;
  try {
    stream = fssync.createReadStream(messagesPath, { encoding: "utf8" });
  } catch {
    return {
      turns: flat,
      credits: { totalCredits: 0, recordCount: 0, latestAt: null },
    };
  }
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let pendingUserChars = 0;
  let pendingUserTimestampMs = NaN;
  let turn = null;
  let fallbackTurnIndex = 0;
  const seenCreditEvents = new Set();
  let totalCredits = 0;
  let creditRecordCount = 0;
  let latestCreditAt = null;

  const flushTurn = () => {
    if (!turn) return;
    const hasUsage =
      turn.outputChars > 0 ||
      turn.reasoningChars > 0;
    const tsMs = Number.isFinite(turn.timestampMs)
      ? turn.timestampMs
      : Number.isFinite(pendingUserTimestampMs)
        ? pendingUserTimestampMs
        : fallbackTimestamp;
    if (hasUsage && Number.isFinite(tsMs) && tsMs > 0) {
      const requestId =
        turn.executionId ||
        `${sessionId}:v2:${fallbackTurnIndex}`;
      const messageIds = Array.from(turn.messageIds);
      flat.push({
        request_id: requestId,
        message_id: messageIds[0] || turn.executionId || null,
        all_message_ids: messageIds,
        session_id: sessionId,
        session_model_id: fallbackModel,
        model_id: turn.modelId || fallbackModel,
        request_start_timestamp_ms: tsMs,
        user_prompt_length: turn.inputChars,
        response_size: turn.outputChars,
        reasoning_size: turn.reasoningChars,
      });
    }
    fallbackTurnIndex++;
    turn = null;
  };

  const startTurn = (payload, timestampMs) => {
    flushTurn();
    turn = {
      executionId:
        typeof payload?.executionId === "string" ? payload.executionId : null,
      timestampMs:
        Number.isFinite(timestampMs)
          ? timestampMs
          : pendingUserTimestampMs,
      inputChars: pendingUserChars,
      outputChars: 0,
      reasoningChars: 0,
      modelId: null,
      messageIds: new Set(),
    };
    pendingUserChars = 0;
    pendingUserTimestampMs = NaN;
  };

  try {
    for await (const line of rl) {
      if (!line || !line.trim()) continue;
      let event;
      try {
        event = JSON.parse(line);
      } catch {
        continue;
      }
      const payload = event?.payload;
      if (!payload || typeof payload !== "object") continue;
      const type =
        typeof payload.type === "string"
          ? payload.type.toLowerCase()
          : "";
      const timestampMs = Date.parse(event.timestamp || "");

      if (type === "user") {
        if (turn) flushTurn();
        pendingUserChars = kiroCliV2ContentChars(payload.content);
        pendingUserTimestampMs = timestampMs;
        continue;
      }

      if (type === "turn_start") {
        startTurn(payload, timestampMs);
        continue;
      }

      if (type === "usage_summary") {
        const creditEventKey =
          typeof event.id === "string" && event.id
            ? event.id
            : `${payload.executionId || ""}:${event.timestamp || ""}:${creditRecordCount}`;
        if (!seenCreditEvents.has(creditEventKey)) {
          seenCreditEvents.add(creditEventKey);
          const usage = kiroCliV2UsageCredits(payload);
          if (usage.recordCount > 0) {
            totalCredits += usage.totalCredits;
            creditRecordCount += usage.recordCount;
            if (
              Number.isFinite(timestampMs) &&
              (
                !latestCreditAt ||
                timestampMs > Date.parse(latestCreditAt)
              )
            ) {
              latestCreditAt = new Date(timestampMs).toISOString();
            }
          }
        }
        // Billing summaries are independent metadata. They can arrive after
        // turn_end, so they must not create or mutate token turns.
        continue;
      }

      if (
        ["assistant", "tool_call", "tool_result"].includes(
          type,
        ) &&
        !turn
      ) {
        startTurn(payload, timestampMs);
      }
      if (!turn) continue;

      if (
        typeof payload.executionId === "string" &&
        payload.executionId &&
        !turn.executionId
      ) {
        turn.executionId = payload.executionId;
      }
      if (typeof event.id === "string" && event.id) {
        turn.messageIds.add(event.id);
      }

      if (type === "assistant") {
        const chars = kiroCliV2ContentChars(payload.content);
        if (
          typeof payload.operationType === "string" &&
          payload.operationType.toLowerCase() === "reasoning"
        ) {
          turn.reasoningChars += chars;
        } else {
          turn.outputChars += chars;
        }
        if (
          typeof payload.reasoningModelId === "string" &&
          payload.reasoningModelId
        ) {
          turn.modelId = payload.reasoningModelId;
        }
      } else if (type === "tool_result") {
        // #366: tool output re-enters the model as input context on the
        // next request within the turn. Same ÷4 heuristic as user text.
        turn.inputChars += kiroCliV2ContentChars(payload.content);
      } else if (type === "turn_end") {
        flushTurn();
      }
    }
  } catch {
    // Return complete turns parsed before a concurrent truncate/delete.
  }
  flushTurn();
  return {
    turns: flat,
    credits: {
      totalCredits,
      recordCount: creditRecordCount,
      latestAt: latestCreditAt,
    },
  };
}

async function writeKiroCliCreditsSidecar({
  queuePath,
  installKey,
  totalCredits,
  recordCount,
  sessionCount,
  fileCount,
  latestAt,
} = {}) {
  if (!queuePath || !(fileCount > 0)) return;
  const sidecarPath = path.join(
    path.dirname(queuePath),
    KIRO_CLI_CREDITS_SIDECAR,
  );
  try {
    // Dual-install (#306): keep one entry per install and aggregate at the
    // top level, so a second install's write never clobbers the first. The
    // top-level fields keep the v1 shape readKiroCreditsSummary consumes.
    let installs = {};
    try {
      const prev = JSON.parse(await fs.readFile(sidecarPath, "utf8"));
      if (prev?.version === 1 && prev.installs && typeof prev.installs === "object") {
        installs = prev.installs;
      }
    } catch {
      // first write or pre-dual-install sidecar — start fresh
    }
    // Expire entries whose install stopped syncing (deleted WSL distro,
    // distro rename / UNC alias flip leaving a duplicate key, mode switched
    // to *-only). Without this the read-modify-write merge would inflate the
    // aggregate with phantom installs forever; a 30-day grace keeps entries
    // alive across transient wsl.exe probe failures.
    const staleMs = 30 * 24 * 60 * 60 * 1000;
    for (const [k, entry] of Object.entries(installs)) {
      const ts = Date.parse(entry?.updated_at || "");
      if (!Number.isFinite(ts) || Date.now() - ts > staleMs) delete installs[k];
    }
    const key = typeof installKey === "string" && installKey ? installKey : "default";
    installs[key] = {
      total_credits: Number(totalCredits.toFixed(12)),
      record_count: recordCount,
      session_count: sessionCount,
      file_count: fileCount,
      latest_at: latestAt,
      updated_at: new Date().toISOString(),
    };
    let aggCredits = 0;
    let aggRecords = 0;
    let aggSessions = 0;
    let aggFiles = 0;
    let aggLatestAt = null;
    for (const entry of Object.values(installs)) {
      aggCredits += Number(entry.total_credits) || 0;
      aggRecords += Number(entry.record_count) || 0;
      aggSessions += Number(entry.session_count) || 0;
      aggFiles += Number(entry.file_count) || 0;
      if (
        typeof entry.latest_at === "string" &&
        (!aggLatestAt || Date.parse(entry.latest_at) > Date.parse(aggLatestAt))
      ) {
        aggLatestAt = entry.latest_at;
      }
    }
    await writeJson(sidecarPath, {
      version: 1,
      source: "kiro-cli-usage-summary",
      total_credits: Number(aggCredits.toFixed(12)),
      record_count: aggRecords,
      session_count: aggSessions,
      file_count: aggFiles,
      latest_at: aggLatestAt,
      updated_at: new Date().toISOString(),
      installs,
    });
    await chmod600IfPossible(sidecarPath);
  } catch {
    // Credits are supplemental billing metadata. A sidecar write failure must
    // never block the canonical token queue.
  }
}

// Canonicalize a Kiro-CLI-emitted model id so IDE and CLI rows collapse when
// they refer to the same underlying Bedrock model. Examples:
//   anthropic.claude-sonnet-4-20250514-v1:0  -> claude-sonnet-4
//   claude-opus-4.6                           -> claude-opus-4.6
//   claude-sonnet-4.5                         -> claude-sonnet-4.5
//   auto                                      -> null (caller uses 'kiro-cli-agent')
//   <unknown/falsy>                           -> null (caller falls back to 'kiro-cli-agent')
//
// "auto" is treated as unknown because Kiro CLI's auto-routing does not
// expose the underlying Bedrock model id in the session file. Returning
// null lets pricing fall into the kiro-cli-agent bucket (sonnet-4 rates)
// rather than the literal "auto" string which matches Cursor's composer-1
// pricing by accident.
function canonicalizeKiroCliModelId(raw) {
  if (!raw || typeof raw !== "string") return null;
  let name = raw.trim();
  if (!name) return null;
  name = name.toLowerCase();
  if (name === "auto") return null;
  name = name.replace(/^(?:qdev|kiro)::/, "");
  // Strip provider prefix (anthropic., aws., openai., or a full Bedrock ARN).
  name = name.replace(
    /^(?:arn:aws:bedrock:[^:]*:[^:]*:(?:foundation-model\/)?|anthropic\.|openai\.|aws\.)/,
    "",
  );
  // Strip Bedrock revision suffix `:N`.
  name = name.replace(/:\d+$/, "");
  // Strip date + vN suffix (e.g. "-20250514-v1"), or lone "-vN", or lone date.
  name = name.replace(/-\d{8}-v\d+$/i, "");
  name = name.replace(/-v\d+$/i, "");
  name = name.replace(/-\d{8}$/, "");
  // Strip trailing ".v1" or similar Anthropic-on-Bedrock tails if present.
  name = name.replace(/\.v\d+$/i, "");
  return name || null;
}

// Read Kiro CLI requests using SQL-side json_extract so we don't pull the
// full (93 MB-ish) conversations_v2 blob back through sqlite3 -json.
//
// D-1: also surfaces `user_turn_metadata.continuation_id` so the cross-
// source retraction pass (parseKiroCliIncremental) can match whichever
// UUID kiro-cli used as the session link. The SQL column
// `conversation_id` and the inner JSON `continuation_id` are different
// UUIDs on observed data; covering both means retraction fires whichever
// side matches the live session's `session_id`.
function readKiroCliRequests(dbPath, env = process.env, sqliteOptions = {}) {
  if (!dbPath || !fssync.existsSync(dbPath)) return [];
  const sql =
    "SELECT conversation_id, " +
    "json_extract(value, '$.model_info.model_id') AS session_model_id, " +
    "json_extract(value, '$.user_turn_metadata.continuation_id') AS continuation_id, " +
    "json_extract(value, '$.user_turn_metadata.requests') AS requests_json " +
    "FROM conversations_v2 " +
    "WHERE json_extract(value, '$.user_turn_metadata.requests') IS NOT NULL";

  // WSL installs are read over the \\wsl$ UNC bridge; snapshot to tmp so
  // sqlite3 never opens a WAL database across the 9p bridge.
  let snapshot = null;
  let effectiveDbPath = dbPath;
  if (isUncPath(dbPath)) {
    try {
      snapshot = snapshotSqliteDb(dbPath);
      effectiveDbPath = snapshot.path;
    } catch (_e) { }
  }
  let rows;
  try {
    rows = readSqliteJsonRows(effectiveDbPath, sql, {
      label: "Kiro CLI",
      env,
      maxBuffer: 128 * 1024 * 1024,
      timeout: 120_000,
      ...sqliteOptions,
    });
  } finally {
    if (snapshot) snapshot.cleanup();
  }
  const flat = [];
  for (const row of rows) {
    let requests;
    try {
      requests = JSON.parse(row.requests_json || "[]");
    } catch {
      continue;
    }
    if (!Array.isArray(requests)) continue;
    for (const r of requests) {
      if (!r || typeof r !== "object") continue;
      flat.push({
        conversation_id: row.conversation_id,
        continuation_id: row.continuation_id || null,
        session_model_id: row.session_model_id || null,
        request_id: r.request_id || null,
        message_id: r.message_id || null,
        user_prompt_length: r.user_prompt_length,
        response_size: r.response_size,
        model_id: r.model_id || null,
        request_start_timestamp_ms: r.request_start_timestamp_ms,
      });
    }
  }
  return flat;
}

async function parseKiroCliIncremental({ sessionFiles, cursors, queuePath, onProgress, env, sqliteOptions } = {}) {
  await ensureDir(path.dirname(queuePath));
  const kiroCliState =
    cursors.kiroCli && typeof cursors.kiroCli === "object" ? cursors.kiroCli : {};
  const seenIds = new Set(Array.isArray(kiroCliState.seenIds) ? kiroCliState.seenIds : []);

  // Back-compat branch: if caller explicitly passes sessionFiles (an array of
  // per-session .json paths, the old contract used in tests/fixtures), read
  // them as user_turn_metadatas. New default path below reads the SQLite DB.
  if (Array.isArray(sessionFiles)) {
    return parseKiroCliFromSessionFiles({
      sessionFiles,
      cursors,
      queuePath,
      onProgress,
      env,
      kiroCliState,
      seenIds,
    });
  }

  const resolvedEnv = env || process.env;
  const dbPath = resolveKiroCliDbPath(resolvedEnv);

  // Combine three sources under the same (source='kiro', cursors.kiroCli)
  // namespace: historical rows from the SQLite DB, legacy live session state
  // from ~/.kiro/sessions/cli/{uuid}.json, and Kiro CLI 2.13+ event logs at
  // ~/.kiro/sessions/<workspaceHash>/sess_<uuid>/messages.jsonl. Request ID
  // shapes differ:
  // SQLite carries a persisted request_id UUID; session files synthesize
  // `${sessionId}:${loop_id.rand}`. When kiro-cli migrates a live session
  // into SQLite the same turn lands under a new request_id — the cross-
  // source retraction pass below (D-1 + TASK-007) matches session_id ↔
  // SQLite conversation_id OR continuation_id to subtract the orphan
  // session-file cursor entry before the new SQLite row is processed.
  const flatDb = fssync.existsSync(dbPath)
    ? readKiroCliRequests(dbPath, resolvedEnv, sqliteOptions)
    : [];
  const sessionFilesList = resolveKiroCliSessionFiles(resolvedEnv);
  let flatSessions = [];
  let kiroCreditTotal = 0;
  let kiroCreditRecords = 0;
  let kiroCreditSessions = 0;
  let kiroCreditFiles = 0;
  let latestKiroCreditAt = null;
  for (const sessionPath of sessionFilesList) {
    let turns;
    if (path.basename(sessionPath) === "messages.jsonl") {
      kiroCreditFiles++;
      const parsed = await readKiroCliV2SessionTurns(sessionPath);
      turns = parsed.turns;
      const credits = parsed.credits;
      kiroCreditTotal += credits.totalCredits;
      kiroCreditRecords += credits.recordCount;
      if (credits.recordCount > 0) kiroCreditSessions++;
      if (
        credits.latestAt &&
        (
          !latestKiroCreditAt ||
          Date.parse(credits.latestAt) > Date.parse(latestKiroCreditAt)
        )
      ) {
        latestKiroCreditAt = credits.latestAt;
      }
    } else {
      turns = await readKiroCliSessionTurns(sessionPath);
    }
    for (const turn of turns) flatSessions.push(turn);
  }
  await writeKiroCliCreditsSidecar({
    queuePath,
    // Install identity mirrors resolveKiroCliSessionFiles' sessions home so
    // dual installs keep separate sidecar entries.
    installKey:
      resolvedEnv.KIRO_HOME ||
      path.join(resolvedEnv.HOME || os.homedir(), ".kiro"),
    totalCredits: kiroCreditTotal,
    recordCount: kiroCreditRecords,
    sessionCount: kiroCreditSessions,
    fileCount: kiroCreditFiles,
    latestAt: latestKiroCreditAt,
  });
  // Per-request state replaces the old seenIds set. Each entry captures
  // what we contributed for that request_id last time, so a later mutation
  // (same request_id, different fingerprint) can subtract-old/add-new
  // instead of being skipped forever.
  const requestState =
    kiroCliState.requests && typeof kiroCliState.requests === "object"
      ? { ...kiroCliState.requests }
      : {};

  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  const debugEnabled = ["1", "true"].includes(
    String(resolvedEnv.TOKEN_PULSE_DEBUG || "").toLowerCase(),
  );

  // ── TASK-007 + D-1: cross-source retraction. When a conversation has
  //    migrated from the session-file tier into SQLite, the cursor's
  //    prior session-file entry (keyed `${sessionId}:${loopRand}` OR a
  //    bare message_id UUID when loop_id is absent) never matches the
  //    new SQLite request_id. Without retraction the old contribution
  //    stays in the bucket absolute and the new SQLite row is added on
  //    top — permanent double-count. D-6: typed non-empty check so a
  //    corrupt NULL/empty conv_id can't poison the match set.
  //
  // Two match sets are built:
  //   • migratedConvIds  — session_id → any row in SQLite. Used to scope
  //                        cursor retraction (coarse but safe because
  //                        un-migrated turns still present in the session
  //                        file are re-added later in this same run).
  //   • migratedMsgIds   — r.message_id → exact turn in SQLite. Used to
  //                        filter flatSessions at TURN granularity. An
  //                        active session with older migrated turns +
  //                        newer session-file-only turns must keep the
  //                        newer turns; session-level filtering dropped
  //                        them and caused Kiro CLI under-count.
  const migratedConvIds = new Set();
  const migratedMsgIds = new Set();
  for (const row of flatDb) {
    if (!row) continue;
    if (typeof row.conversation_id === "string" && row.conversation_id)
      migratedConvIds.add(row.conversation_id);
    if (typeof row.continuation_id === "string" && row.continuation_id)
      migratedConvIds.add(row.continuation_id);
    if (typeof row.message_id === "string" && row.message_id)
      migratedMsgIds.add(row.message_id);
  }
  if (migratedConvIds.size > 0) {
    // Pre-collect to retract so mutation during iteration is safe.
    // Retraction stays session-level: for every cursor entry whose
    // session_id has any row in SQLite, subtract its prior contribution.
    // This is provably correct because turns still live in the session
    // file get re-added in this same run via the (turn-granular) filter
    // below, producing a net delta of zero for un-migrated turns.
    const toRetract = [];
    for (const [reqId, prev] of Object.entries(requestState)) {
      if (!prev || typeof prev !== "object") continue;
      // Bug-2: prefer the stored session_id tag (new schema); fall back
      // to colon-split for legacy cursors pre-dating this change.
      let sid = null;
      if (typeof prev.session_id === "string" && prev.session_id) {
        sid = prev.session_id;
      } else {
        const colon = reqId.indexOf(":");
        if (colon > 0) sid = reqId.slice(0, colon);
      }
      if (!sid || !migratedConvIds.has(sid)) continue;
      toRetract.push([reqId, prev, sid]);
    }
    for (const [reqId, prev, sid] of toRetract) {
      if (
        prev.input_tokens ||
        prev.output_tokens ||
        prev.reasoning_output_tokens
      ) {
        const prevReasoning = toNonNegativeInt(
          prev.reasoning_output_tokens,
        );
        const prevBucket = getHourlyBucket(
          hourlyState,
          "kiro",
          prev.model,
          prev.bucketStart,
        );
        addTotals(prevBucket.totals, {
          input_tokens: -prev.input_tokens,
          cached_input_tokens: 0,
          cache_creation_input_tokens: 0,
          output_tokens: -prev.output_tokens,
          reasoning_output_tokens: -prevReasoning,
          total_tokens: -(
            prev.input_tokens +
            prev.output_tokens +
            prevReasoning
          ),
          conversation_count: -1,
        });
        touchedBuckets.add(bucketKey("kiro", prev.model, prev.bucketStart));
      }
      delete requestState[reqId];
      if (debugEnabled) {
        process.stderr.write(
          `[kiro-cli] retracted migrated session entry (conv ${sid})\n`,
        );
      }
    }
    // Turn-granular filter: drop a session-file turn only when at least
    // one of its assistant/tool_result message_ids is present in SQLite
    // (i.e. this specific turn has been flushed). Newer turns in the
    // same session that haven't yet landed in SQLite survive.
    //
    // Edge: a turn with no message_ids at all cannot be matched. We keep
    // it — preferring a rare potential double-count (narrow, since such
    // a turn would also have no request_id under the no-loop_id path and
    // be discarded upstream) over the reported regression of dropping
    // legitimate newer turns wholesale. D-14: still O(N) single-pass.
    const before = flatSessions.length;
    flatSessions = flatSessions.filter((s) => {
      if (!s) return false;
      const mids = Array.isArray(s.all_message_ids)
        ? s.all_message_ids
        : s.message_id
        ? [s.message_id]
        : [];
      for (const mid of mids) {
        if (typeof mid === "string" && mid && migratedMsgIds.has(mid)) {
          return false;
        }
      }
      return true;
    });
    if (debugEnabled && flatSessions.length !== before) {
      process.stderr.write(
        `[kiro-cli] dropped ${before - flatSessions.length} migrated session-file turn(s)\n`,
      );
    }
  }

  const flat = flatDb.concat(flatSessions);

  if (flat.length === 0) {
    // Bug-1: retraction may have touched buckets even with empty flat.
    // Clamp + cap BEFORE flushing so the early-return path applies the
    // same guarantees as the main path (fixes a skip that flushed
    // negative conversation_counts and left the cap unapplied).
    const cappedEarly = clampAndCapKiroCliState({
      requestState,
      hourlyState,
      touchedBuckets,
    });
    const bucketsQueued = await enqueueTouchedBuckets({
      queuePath,
      hourlyState,
      touchedBuckets,
    });
    const updatedAt = new Date().toISOString();
    hourlyState.updatedAt = updatedAt;
    cursors.hourly = hourlyState;
    cursors.kiroCli = {
      ...kiroCliState,
      requests: cappedEarly.requests,
      watermarkMs: Math.max(Number(kiroCliState.watermarkMs) || 0, cappedEarly.watermarkMs),
      updatedAt,
    };
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued };
  }
  const cb = typeof onProgress === "function" ? onProgress : null;
  let recordsProcessed = 0;
  let eventsAggregated = 0;

  // 2026-06 audit fix: requests older than the persisted prune watermark were
  // already counted once and had their cursor entry pruned by
  // clampAndCapKiroCliState — re-processing them (prev === undefined) re-ADDED
  // their tokens to the same bucket on every sync, inflating old buckets
  // without bound. Skip them; the watermark only ever advances, and starts at
  // 0 so a first-ever parse still ingests the full DB history.
  const kiroCliWatermarkMs = Number(kiroCliState.watermarkMs) || 0;

  for (let i = 0; i < flat.length; i++) {
    const r = flat[i];
    recordsProcessed++;

    const requestId = r.request_id || r.message_id;
    if (!requestId) continue;

    const promptChars = toNonNegativeInt(r.user_prompt_length);
    const responseChars = toNonNegativeInt(r.response_size);
    const reasoningChars = toNonNegativeInt(r.reasoning_size);
    const approxInput = Math.floor(promptChars / KIRO_CLI_CHARS_PER_TOKEN);
    const approxOutput = Math.floor(responseChars / KIRO_CLI_CHARS_PER_TOKEN);
    const approxReasoning = Math.floor(
      reasoningChars / KIRO_CLI_CHARS_PER_TOKEN,
    );

    const tsMs = Number(r.request_start_timestamp_ms);
    if (!Number.isFinite(tsMs) || tsMs <= 0) continue;
    if (tsMs < kiroCliWatermarkMs) continue;
    const bucketStart = toUtcHalfHourStart(new Date(tsMs).toISOString());
    if (!bucketStart) continue;

    const rawModel = r.model_id || r.session_model_id;
    const canonical = canonicalizeKiroCliModelId(rawModel);
    const model = canonical || "kiro-cli-agent";

    // Fingerprint captures every field whose change should cause a re-bucket.
    const fingerprint =
      `${promptChars}:${responseChars}:${reasoningChars}:${model}:${tsMs}`;
    const prev = requestState[requestId];
    if (prev && prev.fingerprint === fingerprint) continue; // unchanged

    // Subtract the prior contribution (if any) from its prior bucket so the
    // bucket's absolute totals reflect the CURRENT truth, not the historical
    // truth. enqueueTouchedBuckets will emit the net delta at flush time.
    if (
      prev &&
      (
        prev.input_tokens ||
        prev.output_tokens ||
        prev.reasoning_output_tokens
      )
    ) {
      const prevReasoning = toNonNegativeInt(
        prev.reasoning_output_tokens,
      );
      const prevBucket = getHourlyBucket(hourlyState, "kiro", prev.model, prev.bucketStart);
      addTotals(prevBucket.totals, {
        input_tokens: -prev.input_tokens,
        cached_input_tokens: 0,
        cache_creation_input_tokens: 0,
        output_tokens: -prev.output_tokens,
        reasoning_output_tokens: -prevReasoning,
        total_tokens: -(
          prev.input_tokens +
          prev.output_tokens +
          prevReasoning
        ),
        conversation_count: -1,
      });
      touchedBuckets.add(bucketKey("kiro", prev.model, prev.bucketStart));
    }

    // Add the new contribution.
    if (approxInput > 0 || approxOutput > 0 || approxReasoning > 0) {
      const bucket = getHourlyBucket(hourlyState, "kiro", model, bucketStart);
      addTotals(bucket.totals, {
        input_tokens: approxInput,
        cached_input_tokens: 0,
        cache_creation_input_tokens: 0,
        output_tokens: approxOutput,
        reasoning_output_tokens: approxReasoning,
        total_tokens: approxInput + approxOutput + approxReasoning,
        conversation_count: 1,
      });
      touchedBuckets.add(bucketKey("kiro", model, bucketStart));
      eventsAggregated++;
    }

    // Always record the cursor entry (even for zero-token requests) so we
    // don't re-count later if Kiro rewrites this request with real data.
    // Bug-2: tag session-origin entries with session_id so the retraction
    // pass can identify them regardless of request_id format (the
    // no-loop_id fallback produces a bare UUID with no colon, which would
    // otherwise be indistinguishable from SQLite's UUID keys).
    requestState[requestId] = {
      fingerprint,
      bucketStart,
      model,
      input_tokens: approxInput,
      output_tokens: approxOutput,
      reasoning_output_tokens: approxReasoning,
      ...(r.session_id ? { session_id: r.session_id } : {}),
    };

    if (cb && i % 50 === 0) {
      cb({
        index: i + 1,
        total: flat.length,
        recordsProcessed,
        eventsAggregated,
        bucketsQueued: touchedBuckets.size,
      });
    }
  }

  const cappedState = clampAndCapKiroCliState({
    requestState,
    hourlyState,
    touchedBuckets,
  });

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.kiroCli = {
    ...kiroCliState,
    requests: cappedState.requests,
    watermarkMs: Math.max(kiroCliWatermarkMs, cappedState.watermarkMs),
    updatedAt,
  };

  return { recordsProcessed, eventsAggregated, bucketsQueued };
}

// TASK-004 + TASK-010 + Bug-1: shared end-of-run clamp + cap for
// parseKiroCliIncremental. Centralized so the main path AND the
// retraction-only early-return path both apply the same guarantees.
// Mutates hourlyState bucket totals in place (clamp) and returns a new
// capped requestState object (cap).
const KIRO_CLI_CURSOR_MAX_AGE_MS = 90 * 24 * 60 * 60 * 1000;
const KIRO_CLI_CURSOR_MAX_ENTRIES = 20_000;

function clampAndCapKiroCliState({ requestState, hourlyState, touchedBuckets }) {
  // TASK-010: clamp conversation_count to >= 0 on Kiro-touched buckets
  // only. The shared enqueueTouchedBuckets is left untouched so
  // legitimate negatives from the 10 other parsers are not masked. Kiro
  // negatives come from the subtract-old pass on mutation or retraction.
  for (const key of touchedBuckets) {
    const bucket = hourlyState.buckets && hourlyState.buckets[key];
    if (bucket && bucket.totals && bucket.totals.conversation_count < 0) {
      bucket.totals.conversation_count = 0;
    }
  }
  // TASK-004: cap cursors.kiroCli.requests by age + count. Runs LAST so
  // nothing active or just-retracted is pruned mid-flight.
  //
  // 2026-06 audit fix: pruning an entry while readKiroCliRequests has no
  // time floor meant the same request came back next sync with
  // `prev === undefined` and was re-ADDED to its (old) bucket — every sync,
  // forever. The returned watermarkMs records how far this prune reached;
  // the parse loop skips any request older than the persisted watermark, so
  // a pruned request can never be re-counted. First-ever parse still counts
  // arbitrarily old history (watermark starts at 0).
  const ageCutoffMs = Date.now() - KIRO_CLI_CURSOR_MAX_AGE_MS;
  const cappedEntries = [];
  for (const [reqId, entry] of Object.entries(requestState)) {
    if (!entry || typeof entry !== "object") continue;
    const ts = entry.bucketStart ? Date.parse(entry.bucketStart) : NaN;
    if (!Number.isFinite(ts) || ts < ageCutoffMs) continue;
    cappedEntries.push([reqId, entry, ts]);
  }
  // +30min margin: entries are pruned by bucketStart (half-hour floor) while
  // the parse loop skips by raw request ts, which can sit up to 30 minutes
  // after its bucketStart. Without the margin a request whose bucket just
  // crossed the cutoff would be pruned yet still pass the skip, re-adding for
  // a few syncs until the watermark catches up.
  let watermarkMs = ageCutoffMs + 30 * 60 * 1000;
  if (cappedEntries.length > KIRO_CLI_CURSOR_MAX_ENTRIES) {
    cappedEntries.sort((a, b) => b[2] - a[2]); // newest first
    // Newest EVICTED entry sits at index MAX_ENTRIES after the sort; the
    // watermark must clear it so count-capped evictions can't re-add either.
    watermarkMs = Math.max(watermarkMs, cappedEntries[KIRO_CLI_CURSOR_MAX_ENTRIES][2] + 1);
    cappedEntries.length = KIRO_CLI_CURSOR_MAX_ENTRIES;
  }
  const capped = {};
  for (const [reqId, entry] of cappedEntries) capped[reqId] = entry;
  return { requests: capped, watermarkMs };
}

// Back-compat path: per-session .json files (the old fixture shape). Emits
// exact tokens if the fixture happens to carry them (which the test fixture
// does). Used only by the test/rollout-parser.test.js fixture tests.
async function parseKiroCliFromSessionFiles({
  sessionFiles,
  cursors,
  queuePath,
  onProgress,
  env,
  kiroCliState,
  seenIds,
}) {
  const fileOffsets =
    kiroCliState.fileOffsets && typeof kiroCliState.fileOffsets === "object"
      ? { ...kiroCliState.fileOffsets }
      : {};
  if (sessionFiles.length === 0) {
    cursors.kiroCli = {
      ...kiroCliState,
      seenIds: Array.from(seenIds),
      fileOffsets,
      updatedAt: new Date().toISOString(),
    };
    return {
      recordsProcessed: 0,
      eventsAggregated: 0,
      bucketsQueued: 0,
    };
  }

  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;
  let recordsProcessed = 0;
  let eventsAggregated = 0;

  for (let fileIdx = 0; fileIdx < sessionFiles.length; fileIdx++) {
    const filePath = sessionFiles[fileIdx];
    let stat;
    try {
      stat = fssync.statSync(filePath);
    } catch {
      continue;
    }

    const prevEntry = fileOffsets[filePath] || {};
    const prevMtime = Number(prevEntry.mtimeMs) || 0;
    const prevLastIndex = Number.isFinite(Number(prevEntry.lastIndex))
      ? Number(prevEntry.lastIndex)
      : -1;
    if (prevMtime && stat.mtimeMs <= prevMtime) continue;

    let parsed;
    try {
      parsed = JSON.parse(fssync.readFileSync(filePath, "utf8"));
    } catch {
      continue;
    }
    if (!parsed || typeof parsed !== "object") continue;

    const turns = Array.isArray(
      parsed?.session_state?.conversation_metadata?.user_turn_metadatas,
    )
      ? parsed.session_state.conversation_metadata.user_turn_metadatas
      : [];
    const sessionId = typeof parsed.session_id === "string" ? parsed.session_id : filePath;
    const sessionModelId =
      (parsed?.session_state?.rts_model_state?.model_info &&
        (parsed.session_state.rts_model_state.model_info.model_id ||
          parsed.session_state.rts_model_state.model_info.modelId)) ||
      null;

    let maxIndex = prevLastIndex;
    for (let i = 0; i < turns.length; i++) {
      if (i <= prevLastIndex) continue;
      const turn = turns[i];
      if (!turn || typeof turn !== "object") continue;
      recordsProcessed++;

      const input = toNonNegativeInt(turn.input_tokens);
      const output = toNonNegativeInt(turn.output_tokens);
      const cacheRead = toNonNegativeInt(
        turn.cache_read_input_tokens ?? turn.cached_input_tokens,
      );
      const cacheCreation = toNonNegativeInt(
        turn.cache_creation_input_tokens ?? turn.cache_write_input_tokens,
      );
      const reasoning = toNonNegativeInt(turn.reasoning_output_tokens);
      if (input === 0 && output === 0 && cacheRead === 0 && cacheCreation === 0) {
        maxIndex = i;
        continue;
      }

      const ts = turn.timestamp || turn.created_at || turn.updated_at;
      if (!ts) continue;
      const bucketStart = toUtcHalfHourStart(ts);
      if (!bucketStart) continue;

      const turnMessageId =
        typeof turn.message_id === "string" && turn.message_id ? turn.message_id : null;
      const dedupKey = turnMessageId ? `${sessionId}:${turnMessageId}` : null;
      if (dedupKey && seenIds.has(dedupKey)) {
        maxIndex = i;
        continue;
      }

      const rawModel =
        turn.model_id ||
        turn.modelId ||
        (turn.model_info && (turn.model_info.model_id || turn.model_info.modelId)) ||
        sessionModelId;
      const normalized = rawModel ? normalizeKiroModelName(rawModel) : null;
      const model = normalized || "kiro-cli-agent";

      const delta = {
        input_tokens: input,
        cached_input_tokens: cacheRead,
        cache_creation_input_tokens: cacheCreation,
        output_tokens: output,
        reasoning_output_tokens: reasoning,
        total_tokens: input + output + cacheRead + cacheCreation + reasoning,
        conversation_count: 1,
      };

      const bucket = getHourlyBucket(hourlyState, "kiro", model, bucketStart);
      addTotals(bucket.totals, delta);
      touchedBuckets.add(bucketKey("kiro", model, bucketStart));
      if (dedupKey) seenIds.add(dedupKey);
      maxIndex = i;
      eventsAggregated++;

      if (cb) {
        cb({
          index: fileIdx + 1,
          total: sessionFiles.length,
          recordsProcessed,
          eventsAggregated,
          bucketsQueued: touchedBuckets.size,
        });
      }
    }

    fileOffsets[filePath] = {
      mtimeMs: stat.mtimeMs,
      size: stat.size,
      lastIndex: maxIndex,
    };
  }

  const seenArr = Array.from(seenIds);
  const cappedSeen = seenArr.length > 10_000 ? seenArr.slice(seenArr.length - 10_000) : seenArr;

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.kiroCli = { ...kiroCliState, seenIds: cappedSeen, fileOffsets, updatedAt };

  return {
    recordsProcessed,
    eventsAggregated,
    bucketsQueued,
  };
}

module.exports = {
  resolveKiroBasePath,
  resolveKiroDbPath,
  resolveKiroJsonlPath,
  parseKiroIncremental,
  kiroInstallOwnsCursor,
  kiroCliInstallOwnsCursor,
  resolveKiroCliDbPath,
  resolveKiroCliSessionFiles,
  parseKiroCliIncremental,
};
