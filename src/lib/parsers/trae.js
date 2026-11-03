// trae usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fs = require("node:fs/promises");
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const crypto = require("node:crypto");
const { ensureDir } = require("../fs");
const { resolveTraeDbPaths, readTraeUsageRows } = require("../trae-db");
const { normalizeTraeUsage, normalizeTraeModel, traeTimestamp } = require("../trae-usage");
const {
  enqueueTouchedBuckets,
  normalizeHourlyState,
  getHourlyBucket,
  addTotals,
  subtractTotals,
  totalsKey,
  toUtcHalfHourStart,
  bucketKey,
  devinSqliteFingerprint,
  sameSqliteFingerprint,
  mergeGrokUsagePrecision,
  clearSourceHourlyBuckets,
  retractStaleSourceQueueRows,
} = require("./core");

// ── Trae SOLO (ByteDance AI IDE) ─────────────────────────────────────────────
// https://www.trae.ai
//
// International TRAE usage comes from local encrypted chat_turn metadata.
//
// The ledger keeps one compact tuple per turn, keyed by a 128-bit digest, so a
// correction can retract exactly what the turn contributed even after TRAE
// deletes its history. total_tokens is the five token columns plus any input
// without a cache split, which is counted but unpriced (normalizeTraeUsage).
const TRAE_CURSOR_VERSION = 2;
const HALF_HOUR_MS = 30 * 60 * 1000;
const [T_SESSION, T_MODEL, T_BUCKET, T_MODIFIED, T_STORE, T_INPUT, T_CACHED, T_WRITTEN,
  T_OUTPUT, T_REASONING, T_CONVERSATIONS, T_ESTIMATED, T_UNPRICED] = Array.from({ length: 13 }, (_, i) => i);

function packTraeTurn({ session, model, bucketStart, totals, modifiedAt, store }) {
  return [session, model, Date.parse(bucketStart) / HALF_HOUR_MS, modifiedAt, store,
    totals.input_tokens, totals.cached_input_tokens, totals.cache_creation_input_tokens,
    totals.output_tokens, totals.reasoning_output_tokens, totals.conversation_count,
    totals.usage_precision === "estimated" ? 1 : 0, totals.unpriced_input_tokens || 0];
}

function unpackTraeTotals(turn) {
  const totals = {
    input_tokens: turn[T_INPUT],
    cached_input_tokens: turn[T_CACHED],
    cache_creation_input_tokens: turn[T_WRITTEN],
    output_tokens: turn[T_OUTPUT],
    reasoning_output_tokens: turn[T_REASONING],
    total_tokens: turn[T_INPUT] + turn[T_CACHED] + turn[T_WRITTEN] + turn[T_OUTPUT] + turn[T_REASONING]
      + turn[T_UNPRICED],
    conversation_count: turn[T_CONVERSATIONS],
  };
  if (turn[T_ESTIMATED]) totals.usage_precision = "estimated";
  return totals;
}

function traeTurnBucketStart(turn) {
  return new Date(turn[T_BUCKET] * HALF_HOUR_MS).toISOString();
}

async function parseTraeIncremental({
  dbPaths,
  cursors,
  queuePath,
  env = process.env,
  onProgress,
  readUsageRows = readTraeUsageRows,
} = {}) {
  const paths = dbPaths || resolveTraeDbPaths(env);
  const hourlyState = normalizeHourlyState(cursors.hourly);
  // Queue writes and reads may fail. Publish cursor changes only after the
  // append succeeds, including copies of the mutable bucket totals.
  hourlyState.buckets = Object.fromEntries(Object.entries(hourlyState.buckets).map(([key, bucket]) => [
    key, key.startsWith("trae|") ? { ...bucket, totals: { ...bucket.totals } } : bucket,
  ]));
  hourlyState.groupQueued = { ...hourlyState.groupQueued };
  // A ledger with another TRAE_CURSOR_VERSION cannot retract what it
  // contributed, so rebuild the TRAE buckets from the stores instead of
  // adding every turn on top of them.
  const prior = cursors.trae?.version === TRAE_CURSOR_VERSION ? cursors.trae : null;
  const rebuilding = !prior && Boolean(cursors.trae);
  if (rebuilding) clearSourceHourlyBuckets(hourlyState, "trae");
  const turns = { ...prior?.turns };
  const databases = { ...prior?.databases };
  const stores = [...(prior?.stores || [])];
  const touchedBuckets = new Set();
  const conversations = new Set(Object.values(turns)
    .filter((turn) => turn[T_CONVERSATIONS] > 0).map((turn) => turn[T_SESSION]));
  // 128-bit digests: identifiers are hashed before they are stored.
  const digest = (value) => crypto.createHash("sha256").update(value).digest().subarray(0, 16).toString("base64url");
  let recordsProcessed = 0;
  let eventsAggregated = 0;
  let recordsSkipped = 0;
  let estimatedRecords = 0;
  let unpricedRecords = 0;
  const errors = [];
  for (const dbPath of [...new Set(paths)]) {
    const databaseKey = digest(path.resolve(dbPath));
    let store = stores.indexOf(databaseKey);
    if (store < 0) store = stores.push(databaseKey) - 1;
    let rows;
    let fingerprint;
    let finalFingerprint;
    try {
      // existsSync also returns false for permission errors. Let stat/read
      // report inaccessible stores instead of treating them as absent.
      fssync.statSync(dbPath);
      fingerprint = devinSqliteFingerprint(dbPath);
      if (sameSqliteFingerprint(fingerprint, databases[databaseKey])) continue;
      rows = await readUsageRows(dbPath, { env });
      finalFingerprint = devinSqliteFingerprint(dbPath);
    } catch (err) {
      // One install can be locked, corrupt, or use a different key while
      // another remains readable. Its prior fingerprint stays retryable;
      // successful stores still publish together after the queue append.
      errors.push({ database: dbPath, message: err?.message || String(err) });
      continue;
    }
    for (const row of rows) {
      recordsProcessed += 1;
      const totals = normalizeTraeUsage(row.usage, { model: row.model });
      const timestamp = traeTimestamp(row.created_at);
      const bucketStart = timestamp && toUtcHalfHourStart(timestamp);
      const rowId = row.turn_id || row.id;
      if (!totals || !bucketStart || rowId == null || rowId === "") {
        recordsSkipped += 1;
        continue;
      }
      const session = digest(String(row.session_id || `${databaseKey}:${rowId}`));
      // Stable turn ids deduplicate copies across the two international app
      // stores. Numeric SQLite ids are only unique within their database.
      const identity = digest(JSON.stringify(row.turn_id
        ? [session, row.turn_id] : [databaseKey, rowId]));
      const previous = turns[identity];
      const modifiedAt = Date.parse(traeTimestamp(row.updated_at) || timestamp);
      // A copied turn in another install can lag behind the original. Never
      // let an unrelated rescan of that stale store retract newer usage.
      // Without a newer timestamp, conflicting copies retain their owner.
      if (previous && previous[T_STORE] !== store && modifiedAt <= previous[T_MODIFIED]) continue;
      if (!previous && totals.total_tokens === 0) continue;
      if (totals.usage_precision === "estimated") estimatedRecords += 1;
      if (totals.unpriced_input_tokens > 0) unpricedRecords += 1;
      totals.conversation_count = previous
        ? previous[T_CONVERSATIONS]
        : conversations.has(session) ? 0 : 1;
      conversations.add(session);
      const model = normalizeTraeModel(row.model);
      const next = packTraeTurn({ session, model, bucketStart, totals, modifiedAt, store });
      if (previous && next.every((value, i) => i === T_MODIFIED || i === T_STORE || value === previous[i])) {
        turns[identity] = next;
        continue;
      }
      if (previous) {
        const previousBucket = traeTurnBucketStart(previous);
        const old = getHourlyBucket(hourlyState, "trae", previous[T_MODEL], previousBucket);
        subtractTotals(old.totals, unpackTraeTotals(previous));
        touchedBuckets.add(bucketKey("trae", previous[T_MODEL], previousBucket));
      }
      const bucket = getHourlyBucket(hourlyState, "trae", model, bucketStart);
      addTotals(bucket.totals, totals);
      touchedBuckets.add(bucketKey("trae", model, bucketStart));
      turns[identity] = next;
      eventsAggregated += 1;
    }
    // A read that races with a writer must be retried on the next sync.
    databases[databaseKey] = sameSqliteFingerprint(fingerprint, finalFingerprint)
      ? fingerprint : null;
    if (typeof onProgress === "function") {
      onProgress({ recordsProcessed, eventsAggregated, bucketsQueued: touchedBuckets.size });
    }
  }
  // Rebuild precision from the retained contributions, including unchanged
  // stores. A correction can replace the last estimated turn with reported
  // usage without changing numeric totals, or move it to another bucket.
  const bucketPrecisions = new Map();
  for (const turn of Object.values(turns)) {
    const key = bucketKey("trae", turn[T_MODEL], traeTurnBucketStart(turn));
    if (!touchedBuckets.has(key) || unpackTraeTotals(turn).total_tokens === 0) continue;
    bucketPrecisions.set(key, mergeGrokUsagePrecision(
      bucketPrecisions.get(key), turn[T_ESTIMATED] ? "estimated" : "reported",
    ));
  }
  for (const key of touchedBuckets) {
    const precision = bucketPrecisions.get(key);
    hourlyState.buckets[key].usage_precision = precision === "reported" ? null : precision || null;
  }
  // A rebuild is all or nothing. Buckets rebuilt from a subset of the stores
  // would replace shared buckets with partial totals, and each retry would
  // append every row again. Keep the old hourly state and ledger instead.
  if (rebuilding && errors.length) {
    return { recordsProcessed, eventsAggregated, bucketsQueued: 0, recordsSkipped, estimatedRecords, unpricedRecords, errors };
  }
  await ensureDir(path.dirname(queuePath));
  let bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  cursors.hourly = hourlyState;
  if (rebuilding) {
    // The old ledger may have filed a turn under another bucket key (model or
    // half-hour); retract those rows so the dashboard does not count it twice.
    const keepKeys = new Set(Object.keys(hourlyState.buckets).filter((key) => key.startsWith("trae|")));
    bucketsQueued += await retractStaleSourceQueueRows(queuePath, "trae", keepKeys);
  }
  cursors.trae = { version: TRAE_CURSOR_VERSION, stores, databases, turns };
  return { recordsProcessed, eventsAggregated, bucketsQueued, recordsSkipped, estimatedRecords, unpricedRecords, errors };
}

// Ordered candidate app-dir names. The CN IDE build installs as "Trae CN" on
// Windows; the international build installs as "TRAE SOLO". Both expose the same
// iCubeServerData entitlement key, so either is a valid Trae IDE install.
function traeCandidateAppDirs(env = process.env) {
  const override = env.TOKEN_PULSE_TRAE_HOME;
  if (typeof override === "string" && override.trim().length > 0) {
    return [override.trim()];
  }
  const home = os.homedir();
  if (process.platform === "darwin") {
    return [path.join(home, "Library", "Application Support", "TRAE SOLO")];
  }
  if (process.platform === "win32") {
    // A Windows box with no APPDATA still keeps Trae under the standard
    // roaming profile — falling through to the dot-dir below would point at
    // a location Trae never writes.
    const appData =
      typeof env.APPDATA === "string" && env.APPDATA.trim()
        ? env.APPDATA.trim()
        : path.join(home, "AppData", "Roaming");
    return [
      path.join(appData, "TRAE SOLO"),
      path.join(appData, "Trae CN"),
    ];
  }
  // Linux and friends: TRAE SOLO ships official builds for macOS/Windows
  // only, so there is no verified app-data layout to default to. Fall back to
  // a deterministic home-dir path (best-effort detection);
  // TOKEN_PULSE_TRAE_HOME always wins for unusual installs.
  return [path.join(home, ".trae-solo")];
}

// Best-effort: the first candidate that exists on disk (used for display/init
// detection). When no install is present, returns the primary candidate so
// callers still get a deterministic path to report.
function resolveTraePath(env = process.env) {
  const candidates = traeCandidateAppDirs(env);
  for (const dir of candidates) {
    try { if (fssync.existsSync(dir)) return dir; } catch (_error) {}
  }
  return candidates.length ? candidates[0] : null;
}

function resolveTraeStoragePath(env = process.env) {
  for (const dir of traeCandidateAppDirs(env)) {
    const p = path.join(dir, "User", "globalStorage", "storage.json");
    try { if (fssync.existsSync(p)) return p; } catch (_error) {}
  }
  return null;
}

/**
 * Extract the Trae SOLO entitlement snapshot from parsed storage.json
 * serverData (the value under iCubeServerData://icube.cloudide).
 * Returns a normalized entitlement object, or null when the serverData
 * carries no valid entitlementInfo. Shared by the status read path
 * (readTraeEntitlementFromStorage).
 */
function normalizeTraeEntitlement(serverData) {
  let ent;
  try {
    ent = typeof serverData === "string" ? JSON.parse(serverData) : serverData;
  } catch {
    return null;
  }
  // The parsed serverData may legitimately be JSON `null` (e.g. the string
  // "null") or another non-object — treat it as no valid snapshot.
  if (!ent || typeof ent !== "object" || Array.isArray(ent)) return null;
  const entitlementInfo = ent.entitlementInfo;
  if (!entitlementInfo || typeof entitlementInfo !== "object") return null;
  const detail = entitlementInfo.detail || {};
  return {
    identity: entitlementInfo.identityStr,
    identity_code: entitlementInfo.identity,
    has_package: entitlementInfo.hasPackage,
    is_dollar_billing: entitlementInfo.isDollarUsageBilling,
    pro_period: entitlementInfo.proPeriod,
    enable_solo_builder: entitlementInfo.enableSoloBuilder,
    enable_solo_coder: entitlementInfo.enableSoloCoder,
    fast_request_per: detail.fastRequestPer,
    in_waitlist: detail.inWaitlist,
  };
}

/**
 * Read the current Trae SOLO entitlement snapshot straight from the Trae
 * Local State storage.json, without touching the token-count-only queue
 * (CLAUDE.md privacy contract). Returns a normalized entitlement object
 * with a captured_at timestamp, or null when storage.json is missing,
 * unparseable, or carries no valid entitlement snapshot.
 */
function readTraeEntitlementFromStorage(storagePath) {
  if (!storagePath) return null;
  let fd;
  try {
    fd = fssync.openSync(storagePath, "r");
  } catch {
    return null;
  }
  let stat = null;
  let raw;
  try {
    stat = fssync.fstatSync(fd);
    raw = fssync.readFileSync(fd, "utf8");
  } catch {
    return null;
  } finally {
    try {
      fssync.closeSync(fd);
    } catch {
      // fd already closed; nothing to do.
    }
  }
  let storage;
  try {
    storage = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!storage || typeof storage !== "object" || Array.isArray(storage)) {
    return null;
  }
  const serverKey = "iCubeServerData://icube.cloudide";
  const serverData = storage[serverKey];
  if (!serverData) return null;
  const entitlement = normalizeTraeEntitlement(serverData);
  if (!entitlement) return null;
  return {
    ...entitlement,
    captured_at: stat ? new Date(stat.mtimeMs).toISOString() : null,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Trae Work CN (国内版) — usage API incremental parser.
//
// TRAE Work CN's usage API reports per-session rows that are NOT append-only:
// a session can be re-reported with corrected token totals, a different model,
// or a shifted time bucket. This parser keeps the last normalized contribution
// per session_id under `cursors.traeCn` and reconciles by subtracting the
// previous contribution from its old bucket before adding the new one, so
// corrections retract the stale tuple instead of stacking. Only the normalized
// contribution is persisted — no raw API rows, cost/credits, auth, refresh
// tokens, or prompt previews. Source is always `trae-cn`.
const TRAE_CN_SOURCE = "trae-cn";
const TRAE_CN_STATE_VERSION = 1;
const TRAE_CN_UNKNOWN_MODEL = "trae-cn-unknown";

function normalizeTraeCnState(raw) {
  if (raw === undefined || raw === null) {
    return {
      version: TRAE_CN_STATE_VERSION,
      sessions: {},
      prunedBeforeMs: 0,
      updatedAt: null,
    };
  }
  if (typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error("Trae CN cursor state is malformed.");
  }
  if (raw.version !== TRAE_CN_STATE_VERSION) {
    throw new Error(`Trae CN cursor state version ${raw.version} is not supported.`);
  }
  if (!raw.sessions || typeof raw.sessions !== "object" || Array.isArray(raw.sessions)) {
    throw new Error("Trae CN cursor sessions are malformed.");
  }
  return {
    version: TRAE_CN_STATE_VERSION,
    sessions: { ...raw.sessions },
    prunedBeforeMs:
      Number.isFinite(raw.prunedBeforeMs) && raw.prunedBeforeMs > 0 ? raw.prunedBeforeMs : 0,
    updatedAt: typeof raw.updatedAt === "string" ? raw.updatedAt : null,
  };
}

function isValidTraeCnTotals(totals) {
  if (!totals || typeof totals !== "object") return false;
  for (const key of [
    "input_tokens",
    "cached_input_tokens",
    "cache_creation_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
    "billable_total_tokens",
    "conversation_count",
  ]) {
    if (!Number.isSafeInteger(totals[key]) || totals[key] < 0) return false;
  }
  return true;
}

// Canonical UTC half-hour bucketStart produced by toUtcHalfHourStart:
// "YYYY-MM-DDTHH:00:00.000Z" or "YYYY-MM-DDTHH:30:00.000Z".
function isValidTraeCnBucketStart(value) {
  if (typeof value !== "string") return false;
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:(00|30):00\.000Z$/.test(value)) return false;
  const dt = new Date(value);
  return Number.isFinite(dt.getTime()) && dt.toISOString() === value;
}

// Validate a stored prior contribution before it is ever subtracted, so a
// tampered or corrupt cursor fails closed instead of silently rewinding buckets.
// Enforces this provider's fixed canonical invariant (all totals safe
// nonnegative integers, reasoning=0, conversation_count=1,
// total=input+cached+cacheCreation+output, billable=total) plus a canonical
// half-hour UTC bucketStart.
function validateTraeCnStoredContribution(entry) {
  if (!entry || typeof entry !== "object") {
    throw new Error("Trae CN stored session contribution is malformed.");
  }
  if (
    typeof entry.model !== "string" ||
    !entry.model.trim() ||
    entry.model.includes("|")
  ) {
    throw new Error("Trae CN stored session model is malformed.");
  }
  if (!isValidTraeCnBucketStart(entry.bucketStart)) {
    throw new Error("Trae CN stored session bucket is malformed.");
  }
  if (!isValidTraeCnTotals(entry.totals)) {
    throw new Error("Trae CN stored session totals are malformed.");
  }
  const { totals } = entry;
  if (totals.reasoning_output_tokens !== 0 || totals.conversation_count !== 1) {
    throw new Error("Trae CN stored session totals are malformed.");
  }
  const sum =
    totals.input_tokens +
    totals.cached_input_tokens +
    totals.cache_creation_input_tokens +
    totals.output_tokens;
  if (totals.total_tokens !== sum || totals.billable_total_tokens !== totals.total_tokens) {
    throw new Error("Trae CN stored session totals are malformed.");
  }
}

// Token fields may live in `row.extra_info` (object or JSON string) or at the
// top level of the row. Missing values are NOT coerced to zero.
function traeCnExtraInfo(row) {
  const extra = row?.extra_info;
  if (extra && typeof extra === "object" && !Array.isArray(extra)) return extra;
  if (typeof extra === "string") {
    try {
      const parsed = JSON.parse(extra);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) return parsed;
    } catch (_e) {}
  }
  return null;
}

function traeCnTokenField(row, extra, key) {
  if (extra && extra[key] !== undefined && extra[key] !== null) return extra[key];
  if (row && row[key] !== undefined && row[key] !== null) return row[key];
  return undefined;
}

// Normalize one raw session row into the canonical contribution, or throw
// before any cursor/bucket mutation happens. Returns
// { sessionId, model, bucketStart, totals }.
function normalizeTraeCnSession(row) {
  const sessionId = typeof row?.session_id === "string" ? row.session_id.trim() : "";
  if (!sessionId) {
    throw new Error("Trae CN session row is missing session_id.");
  }
  let model;
  if (row?.model_name === undefined || row?.model_name === null) {
    model = TRAE_CN_UNKNOWN_MODEL;
  } else if (typeof row.model_name === "string") {
    model = row.model_name.trim() || TRAE_CN_UNKNOWN_MODEL;
  } else {
    throw new Error("Trae CN session row has an invalid model_name.");
  }
  if (model.includes("|")) {
    throw new Error("Trae CN session row has an unsupported model name.");
  }
  if (!Number.isSafeInteger(row?.usage_time) || row.usage_time <= 0) {
    throw new Error("Trae CN session row has an invalid usage_time.");
  }
  const bucketStart = toUtcHalfHourStart(row.usage_time * 1000);
  if (!bucketStart) {
    throw new Error("Trae CN session row has an invalid usage_time.");
  }
  const extra = traeCnExtraInfo(row);
  // Cache fields may be absent for models without a prompt-cache concept
  // (Doubao / DeepSeek on TRAE CN) — an absent cache field means "no cache
  // activity", i.e. 0, NOT a malformed row. input/output stay mandatory.
  //
  // `input_token` is cache-INCLUSIVE: verifying real rows against TRAE CN's
  // own credit billing, credits = fresh_input*p + cache_read*(p/4) + output*q
  // fits every row with zero residual only under this reading (same
  // convention as Codex / Qoder). Peel the cache subsets off into their own
  // columns and report only the remainder as ordinary input, otherwise cached
  // context is double-counted in dashboards and cost calculations. cache_write
  // was 0 across all observed rows; it is treated as a further subset of the
  // cache-inclusive input (clamped so the buckets always sum back to it).
  const cacheRead = traeCnTokenField(row, extra, "cache_read_token");
  const cacheWrite = traeCnTokenField(row, extra, "cache_write_token");
  const fields = [
    ["input_token", traeCnTokenField(row, extra, "input_token")],
    ["output_token", traeCnTokenField(row, extra, "output_token")],
    ["cache_read_token", cacheRead === undefined || cacheRead === null ? 0 : cacheRead],
    ["cache_write_token", cacheWrite === undefined || cacheWrite === null ? 0 : cacheWrite],
  ];
  for (const [label, value] of fields) {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error(`Trae CN session row has an invalid ${label}.`);
    }
  }
  const rawInput = fields[0][1];
  const outputTokens = fields[1][1];
  const cachedInput = Math.min(rawInput, fields[2][1]);
  const cacheCreation = Math.min(rawInput - cachedInput, fields[3][1]);
  const total = rawInput + outputTokens;
  if (!Number.isSafeInteger(total)) {
    throw new Error("Trae CN session row token totals overflow.");
  }
  return {
    sessionId,
    model,
    bucketStart,
    totals: {
      input_tokens: rawInput - cachedInput - cacheCreation,
      output_tokens: outputTokens,
      cached_input_tokens: cachedInput,
      cache_creation_input_tokens: cacheCreation,
      reasoning_output_tokens: 0,
      total_tokens: total,
      billable_total_tokens: total,
      conversation_count: 1,
    },
  };
}

// Fail closed instead of letting subtractTotals clamp a stored prior
// contribution that exceeds its source bucket (a corruption signal).
function assertTraeCnBucketCovers(bucketTotals, previousTotals) {
  for (const key of [
    "input_tokens",
    "cached_input_tokens",
    "cache_creation_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
    "billable_total_tokens",
    "conversation_count",
  ]) {
    if ((bucketTotals[key] || 0) < (previousTotals[key] || 0)) {
      throw new Error("Trae CN state corruption: stored contribution exceeds its bucket totals.");
    }
  }
}

// Account session state: a queue record (kind: "account_session_state")
// carrying the CANONICAL observation of ONE provider-side TRAE CN session.
// Cloud truth for trae-cn lives at the session level
// (token-pulse_account_session_states; ingest edge upserts via
// token-pulse_upsert_account_session_states): identity is
// (user_id, source, session_id) - device_id is NOT identity (the usage API
// request carries no device discriminator). Evidence split (2026-08-17, one
// account, three real fetches 137 -> 141 -> 164): repeated-fetch id
// stability VERIFIED (137/137 persisted, corrections KEPT ids), cross-window
// stability VERIFIED (exact subsets), no duplicate ids OBSERVED. Cross-device
// same-account id stability is NOT DIRECTLY VERIFIED - no device
// discriminator in the request body is necessary but not sufficient (a
// device/login context could ride inside the JWT / server auth context), and
// no second independent device/auth experiment was run.
//
// The three correction classes collapse into ONE whole-row replace:
//   downward  S tokens 100 -> 60
//   model     S model A -> B
//   bucket    S bucket 10:00 -> 10:30
// A fresh device with no cursor history uploads every session it observes;
// the cloud LWW guard reconciles versions. ABSENCE is NOT PROVEN to mean
// deletion, so nothing is ever emitted for sessions missing from a
// non-empty snapshot, and an empty payload emits nothing at all.
//
// snapshot_verified_at is the CLIENT logical fetch stamp (the API exposes no
// provider-side ordering signal - headers carry only CDN trace ids, rows
// carry no revision). It is stamped once per real fetch and replayed
// verbatim by this append-only queue; the cloud upsert applies strictly
// newer (>) stamps only, so replays are idempotent and a transport retry of
// an older observation cannot displace a newer one. Cross-device ordering
// under clock skew is a documented residual risk, NOT strict correctness.
//
// Appended AFTER the bucket rows of the same sync (queue order guarantees a
// device's rows land before the states describing them). Bucket-row readers
// skip this record via its kind field (it carries no hour_start and is not
// a usage row).
async function appendTraeCnSessionStates({ queuePath, observations, verifiedAtMs }) {
  if (!Array.isArray(observations) || observations.length === 0) return 0;
  if (!Number.isFinite(verifiedAtMs)) {
    throw new Error("Trae CN session states require a finite verification stamp.");
  }
  const verifiedAt = new Date(verifiedAtMs).toISOString();
  const lines = [];
  for (const obs of observations) {
    const t = obs.totals;
    lines.push(
      JSON.stringify({
        kind: "account_session_state",
        source: TRAE_CN_SOURCE,
        session_id: obs.sessionId,
        model: obs.model,
        bucket_start: obs.bucketStart,
        input_tokens: t.input_tokens,
        output_tokens: t.output_tokens,
        cached_input_tokens: t.cached_input_tokens,
        cache_creation_input_tokens: t.cache_creation_input_tokens,
        reasoning_output_tokens: t.reasoning_output_tokens,
        total_tokens: t.total_tokens,
        snapshot_verified_at: verifiedAt,
      }),
    );
  }
  await fs.appendFile(queuePath, lines.join("\n") + "\n", "utf8");
  return lines.length;
}

async function parseTraeCnApiIncremental({
  sessions,
  cursors,
  queuePath,
  onProgress,
  windowStartMs,
  windowEndMs,
  snapshotVerifiedAtMs,
} = {}) {
  if (!Array.isArray(sessions)) {
    throw new Error("Trae CN sessions must be an array.");
  }
  if (!cursors || typeof cursors !== "object" || Array.isArray(cursors)) {
    throw new Error("Trae CN cursors must be a writable object.");
  }
  await ensureDir(path.dirname(queuePath));

  // Validate persisted state up front (fresh absent state initializes version
  // 1; malformed state / unexpected version fails closed instead of resetting).
  // Deep-clone the normalized working states so reconciliation and enqueue
  // mutations never touch `cursors` before the final assignment succeeds.
  const hourlyState = structuredClone(normalizeHourlyState(cursors?.hourly));
  const traeCnState = structuredClone(normalizeTraeCnState(cursors?.traeCn));

  if (sessions.length === 0) {
    // Empty payload is a successful no-op: no usage mutation, no session
    // state records.
    //
    // Evidence check (2026-08-16, two real fetches 17min apart over the same
    // account: 137 -> 141 sessions, 0 disappeared, 0 rows changed) shows the
    // session set is stable, but NOTHING proves 'absent from the response'
    // means 'authoritatively deleted / zero' — so the absence contract is
    // NOT PROVEN and must stay symmetric everywhere: a session missing from
    // a non-empty snapshot never emits a retraction, and an empty response
    // asserts nothing at all. Canonical corrections ride on explicit
    // observations only.
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  // Full prevalidation + in-payload dedupe BEFORE any cursor/bucket mutation.
  //
  // ANY malformed row fails the whole snapshot closed. Canonical session
  // states assert this device actually understood the snapshot, so a
  // partially understood snapshot (99 valid + 1 uninterpretable row) must
  // never be enqueued: it would become the canonical cloud state. The
  // provider-level try/catch in cmdSync keeps the failure
  // isolated (other providers sync on) and the error message carries only
  // the count + reason - no session ids, tokens, or credentials. Confirmed
  // LEGAL variations must not land here: absent cache fields are already
  // coerced to 0 inside normalizeTraeCnSession (models without a prompt
  // cache concept); only truly uninterpretable rows (bad session_id /
  // usage_time / token numbers / model, impossible schema) throw.
  const bySession = new Map();
  let skippedRows = 0;
  let firstSkipReason = "";
  for (const row of sessions) {
    let normalized;
    try {
      normalized = normalizeTraeCnSession(row);
    } catch (error) {
      skippedRows += 1;
      if (!firstSkipReason) firstSkipReason = error?.message || "unknown";
      continue;
    }
    const existing = bySession.get(normalized.sessionId);
    if (existing) {
      const identical =
        existing.model === normalized.model &&
        existing.bucketStart === normalized.bucketStart &&
        totalsKey(existing.totals) === totalsKey(normalized.totals);
      if (!identical) {
        throw new Error("Trae CN session rows contain conflicting contributions.");
      }
      continue; // exact duplicate within one payload is accepted once
    }
    bySession.set(normalized.sessionId, normalized);
  }
  if (skippedRows > 0) {
    // Partial snapshots are not authoritative: one uninterpretable row means
    // the window was NOT fully verified, so fail closed - no bucket rows, no
    // session states, no cursor commit. The next sync replays from the same
    // cursor state (idempotent).
    throw new Error(
      `Trae CN snapshot is not authoritative: ${skippedRows} malformed row${skippedRows !== 1 ? "s" : ""} (first: ${firstSkipReason}).`,
    );
  }
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;
  const total = sessions.length;
  let index = 0;
  let eventsAggregated = 0;

  // Validate EVERY existing stored contribution up front — not just sessions
  // present in the incoming payload — so an unrelated malformed entry fails
  // closed before any reconciliation mutation.
  for (const sessionId of Object.keys(traeCnState.sessions)) {
    validateTraeCnStoredContribution(traeCnState.sessions[sessionId]);
  }

  // Monotonic prune (same watermark pattern as the kiroCli cap above): the
  // sync caller fetches a fixed trailing window, so a session whose entire
  // half-hour bucket sits before windowStartMs can never reappear in a later
  // payload — its stored contribution is dead reconciliation weight. Prune
  // only when the window start moved FORWARD past the persisted watermark:
  // an entry evicted under window W1 and re-fetched under an earlier window
  // would find previousEntry undefined and count twice. The +30min margin
  // matches bucketStart's half-hour floor, which can trail the row's
  // usage_time by up to 30 minutes.
  if (Number.isFinite(windowStartMs) && windowStartMs > traeCnState.prunedBeforeMs) {
    const pruneCutoffMs = windowStartMs - 30 * 60 * 1000;
    for (const sessionId of Object.keys(traeCnState.sessions)) {
      const bucketMs = Date.parse(traeCnState.sessions[sessionId].bucketStart);
      if (Number.isFinite(bucketMs) && bucketMs <= pruneCutoffMs) {
        delete traeCnState.sessions[sessionId];
      }
    }
    traeCnState.prunedBeforeMs = windowStartMs;
  }

  // Deterministic: process unique session ids in sorted order. Sessions
  // whose canonical observation changed (or that were never seen) are also
  // collected for the account_session_state queue records.
  const sessionIds = [...bySession.keys()].sort();
  const changedObservations = [];
  for (const sessionId of sessionIds) {
    const current = bySession.get(sessionId);
    const previousEntry = traeCnState.sessions[sessionId];

    const unchanged =
      previousEntry &&
      totalsKey(previousEntry.totals) === totalsKey(current.totals) &&
      previousEntry.bucketStart === current.bucketStart &&
      previousEntry.model === current.model;
    if (unchanged) {
      index += 1;
      if (cb) cb({ index, total, eventsAggregated, bucketsQueued: touchedBuckets.size });
      continue;
    }

    if (previousEntry) {
      const oldBucket = getHourlyBucket(
        hourlyState,
        TRAE_CN_SOURCE,
        previousEntry.model,
        previousEntry.bucketStart,
      );
      assertTraeCnBucketCovers(oldBucket.totals, previousEntry.totals);
      subtractTotals(oldBucket.totals, previousEntry.totals);
      touchedBuckets.add(bucketKey(TRAE_CN_SOURCE, previousEntry.model, previousEntry.bucketStart));
    }

    const bucket = getHourlyBucket(hourlyState, TRAE_CN_SOURCE, current.model, current.bucketStart);
    addTotals(bucket.totals, current.totals);
    touchedBuckets.add(bucketKey(TRAE_CN_SOURCE, current.model, current.bucketStart));

    traeCnState.sessions[sessionId] = {
      model: current.model,
      bucketStart: current.bucketStart,
      totals: { ...current.totals },
      updatedAt: new Date().toISOString(),
    };
    changedObservations.push({
      sessionId,
      model: current.model,
      bucketStart: current.bucketStart,
      totals: current.totals,
    });
    eventsAggregated += 1;
    index += 1;
    if (cb) cb({ index, total, eventsAggregated, bucketsQueued: touchedBuckets.size });
  }

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  // Emit canonical session observations only after the reconciled bucket
  // rows are durably queued; a failure here aborts before cursor commit, so
  // the next sync replays the (idempotent) reconciliation and re-appends
  // both. snapshot_verified_at is stamped once per real fetch; the
  // append-only queue replays it verbatim, so transport retries never fake
  // freshness. Unchanged sessions emit nothing - a fixed-now no-change sync
  // stays byte-identical.
  const verifiedAtMs = Number.isFinite(snapshotVerifiedAtMs) ? snapshotVerifiedAtMs : Date.now();
  await appendTraeCnSessionStates({
    queuePath,
    observations: changedObservations,
    verifiedAtMs,
  });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  traeCnState.updatedAt = updatedAt;
  // Assign cursor state only after enqueue succeeds.
  cursors.hourly = hourlyState;
  cursors.traeCn = traeCnState;

  return { recordsProcessed: total, eventsAggregated, bucketsQueued, skippedRows };
}

module.exports = {
  parseTraeIncremental,
  resolveTraePath,
  resolveTraeStoragePath,
  readTraeEntitlementFromStorage,
  parseTraeCnApiIncremental,
};
