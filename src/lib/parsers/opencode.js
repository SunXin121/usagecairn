// opencode usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fs = require("node:fs/promises");
const fssync = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const { ensureDir } = require("../fs");
const { readSqliteJsonRows } = require("../sqlite-reader");
const wsl = require("../wsl-probe");
const { resolveInstallPaths } = require("../install-resolver");
const {
  DEFAULT_MODEL,
  enqueueTouchedBuckets,
  enqueueTouchedProjectBuckets,
  normalizeHourlyState,
  normalizeProjectState,
  normalizeMessageKeyPart,
  getHourlyBucket,
  getProjectBucket,
  addTotals,
  subtractTotals,
  toUtcHalfHourStart,
  bucketKey,
  projectBucketKey,
  normalizeSourceInput,
  normalizeModelInput,
  resolveProjectContextForFile,
  resolveProjectContextForPath,
  sameGeminiTotals,
  diffGeminiTotals,
  isAllZeroUsage,
  toNonNegativeInt,
  coerceEpochMs,
  safeReadDir,
  isUncPath,
  snapshotSqliteDb,
} = require("./core");

async function listOpencodeMessageFiles(storageDir) {
  const out = [];
  const messageDir = path.join(storageDir, "message");
  await walkOpencodeMessages(messageDir, out);
  out.sort((a, b) => a.localeCompare(b));
  return out;
}

async function parseOpencodeIncremental({
  messageFiles,
  cursors,
  queuePath,
  projectQueuePath,
  onProgress,
  source,
  publicRepoResolver,
}) {
  await ensureDir(path.dirname(queuePath));
  let filesProcessed = 0;
  let eventsAggregated = 0;

  const cb = typeof onProgress === "function" ? onProgress : null;
  const files = Array.isArray(messageFiles) ? messageFiles : [];
  const totalFiles = files.length;
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const projectEnabled = typeof projectQueuePath === "string" && projectQueuePath.length > 0;
  const projectState = projectEnabled ? normalizeProjectState(cursors?.projectHourly) : null;
  const projectTouchedBuckets = projectEnabled ? new Set() : null;
  const projectMetaCache = projectEnabled ? new Map() : null;
  const publicRepoCache = projectEnabled ? new Map() : null;
  const opencodeState = normalizeOpencodeState(cursors?.opencode);
  const messageIndex = opencodeState.messages;
  const fingerprintIndex = buildOpencodeFingerprintIndex(messageIndex);
  const touchedBuckets = new Set();
  const defaultSource = normalizeSourceInput(source) || "opencode";

  if (!cursors.files || typeof cursors.files !== "object") {
    cursors.files = {};
  }

  for (let idx = 0; idx < files.length; idx++) {
    const entry = files[idx];
    const filePath = typeof entry === "string" ? entry : entry?.path;
    if (!filePath) continue;
    const fileSource =
      typeof entry === "string"
        ? defaultSource
        : normalizeSourceInput(entry?.source) || defaultSource;
    const st = await fs.stat(filePath).catch(() => null);
    if (!st || !st.isFile()) continue;

    const key = filePath;
    const prev = cursors.files[key] || null;
    const inode = st.ino || 0;
    const size = Number.isFinite(st.size) ? st.size : 0;
    const mtimeMs = Number.isFinite(st.mtimeMs) ? st.mtimeMs : 0;
    const unchanged =
      prev &&
      prev.inode === inode &&
      prev.size === size &&
      prev.mtimeMs === mtimeMs &&
      prev.opencodeForkRepairVersion === 1;
    if (unchanged) {
      filesProcessed += 1;
      if (cb) {
        cb({
          index: idx + 1,
          total: totalFiles,
          filePath,
          filesProcessed,
          eventsAggregated,
          bucketsQueued: touchedBuckets.size,
        });
      }
      continue;
    }

    const fallbackTotals = prev && typeof prev.lastTotals === "object" ? prev.lastTotals : null;
    const fallbackMessageKey =
      prev && typeof prev.messageKey === "string" && prev.messageKey.trim()
        ? prev.messageKey.trim()
        : null;
    const projectContext = projectEnabled
      ? await resolveProjectContextForFile({
          filePath,
          projectMetaCache,
          publicRepoCache,
          publicRepoResolver,
          projectState,
        })
      : null;
    const projectRef = projectContext?.projectRef || null;
    const projectKey = projectContext?.projectKey || null;

    const result = await parseOpencodeMessageFile({
      filePath,
      messageIndex,
      fingerprintIndex,
      fallbackTotals,
      fallbackMessageKey,
      hourlyState,
      touchedBuckets,
      source: fileSource,
      projectState,
      projectTouchedBuckets,
      projectRef,
      projectKey,
    });

    cursors.files[key] = {
      inode,
      size,
      mtimeMs,
      lastTotals: result.lastTotals,
      messageKey: result.messageKey || null,
      opencodeForkRepairVersion: 1,
      updatedAt: new Date().toISOString(),
    };

    filesProcessed += 1;
    eventsAggregated += result.eventsAggregated;

    if (result.messageKey && result.shouldUpdate) {
      recordOpencodeMessage({
        messageIndex,
        fingerprintIndex,
        messageKey: result.messageKey,
        totals: result.lastTotals,
        fingerprint: result.fingerprint || null,
        dedupedForkCopy: result.dedupedForkCopy === true,
      });
    }

    if (cb) {
      cb({
        index: idx + 1,
        total: totalFiles,
        filePath,
        filesProcessed,
        eventsAggregated,
        bucketsQueued: touchedBuckets.size,
      });
    }
  }

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const projectBucketsQueued = projectEnabled
    ? await enqueueTouchedProjectBuckets({ projectQueuePath, projectState, projectTouchedBuckets })
    : 0;
  hourlyState.updatedAt = new Date().toISOString();
  cursors.hourly = hourlyState;
  opencodeState.updatedAt = new Date().toISOString();
  cursors.opencode = opencodeState;
  if (projectState) {
    projectState.updatedAt = new Date().toISOString();
    cursors.projectHourly = projectState;
  }

  return { filesProcessed, eventsAggregated, bucketsQueued, projectBucketsQueued };
}

async function parseOpencodeMessageFile({
  filePath,
  messageIndex,
  fingerprintIndex,
  fallbackTotals,
  fallbackMessageKey,
  hourlyState,
  touchedBuckets,
  source,
  projectState,
  projectTouchedBuckets,
  projectRef,
  projectKey,
}) {
  const fallbackKey =
    typeof fallbackMessageKey === "string" && fallbackMessageKey.trim()
      ? fallbackMessageKey.trim()
      : null;
  const legacyTotals = fallbackTotals && typeof fallbackTotals === "object" ? fallbackTotals : null;
  const fallbackEntry = messageIndex && fallbackKey ? messageIndex[fallbackKey] : null;
  const fallbackLastTotals =
    fallbackEntry && typeof fallbackEntry.lastTotals === "object"
      ? fallbackEntry.lastTotals
      : legacyTotals;

  const raw = await fs.readFile(filePath, "utf8").catch(() => "");
  if (!raw.trim()) {
    return {
      messageKey: fallbackKey,
      lastTotals: fallbackLastTotals,
      eventsAggregated: 0,
      shouldUpdate: false,
    };
  }

  let msg;
  try {
    msg = JSON.parse(raw);
  } catch (_e) {
    return {
      messageKey: fallbackKey,
      lastTotals: fallbackLastTotals,
      eventsAggregated: 0,
      shouldUpdate: false,
    };
  }

  const messageKey = deriveOpencodeMessageKey(msg, filePath);
  const prev = messageIndex && messageKey ? messageIndex[messageKey] : null;
  const indexTotals = prev && typeof prev.lastTotals === "object" ? prev.lastTotals : null;
  const fallbackMatch = !fallbackKey || fallbackKey === messageKey;
  const lastTotals = indexTotals || (fallbackMatch ? fallbackLastTotals : null);

  const currentTotals = normalizeOpencodeTokens(msg?.tokens);
  if (!currentTotals) {
    return { messageKey, lastTotals, eventsAggregated: 0, shouldUpdate: false };
  }

  // `Session.fork` re-materialises the parent's prefix under new message ids in
  // a new session — count it once (issue #426, see deriveOpencodeMessageFingerprint).
  const fingerprint = deriveOpencodeMessageFingerprint({ msg, totals: currentTotals, source });
  if (isOpencodeForkCopy(fingerprintIndex, fingerprint, messageKey)) {
    if (lastTotals && prev?.dedupedForkCopy !== true) {
      repairCountedOpencodeForkCopy({
        msg,
        totals: lastTotals,
        source,
        hourlyState,
        touchedBuckets,
        projectState,
        projectTouchedBuckets,
        projectRef,
        projectKey,
      });
    }
    return {
      messageKey,
      lastTotals: currentTotals,
      fingerprint,
      dedupedForkCopy: true,
      eventsAggregated: 0,
      shouldUpdate: prev?.dedupedForkCopy !== true || !lastTotals,
    };
  }

  // A formerly deduped copy can become a genuinely distinct in-place update.
  // Its prior totals were removed from the buckets, so re-add the full current
  // snapshot instead of only the delta from the suppressed value.
  const effectiveLastTotals = prev?.dedupedForkCopy === true ? null : lastTotals;
  const delta = diffGeminiTotals(currentTotals, effectiveLastTotals);
  if (!delta || isAllZeroUsage(delta)) {
    return {
      messageKey,
      lastTotals: currentTotals,
      fingerprint,
      dedupedForkCopy: false,
      eventsAggregated: 0,
      shouldUpdate: true,
    };
  }
  delta.conversation_count = 1;

  const timestampMs = coerceEpochMs(msg?.time?.completed) || coerceEpochMs(msg?.time?.created);
  if (!timestampMs) {
    return {
      messageKey,
      lastTotals,
      eventsAggregated: 0,
      shouldUpdate: Boolean(lastTotals),
    };
  }

  const tsIso = new Date(timestampMs).toISOString();
  const bucketStart = toUtcHalfHourStart(tsIso);
  if (!bucketStart) {
    return {
      messageKey,
      lastTotals,
      eventsAggregated: 0,
      shouldUpdate: Boolean(lastTotals),
    };
  }

  const { modelId: fileModelId } = normalizeOpencodeModelFields(msg);
  const model = fileModelId || DEFAULT_MODEL;
  const bucket = getHourlyBucket(hourlyState, source, model, bucketStart);
  addTotals(bucket.totals, delta);
  touchedBuckets.add(bucketKey(source, model, bucketStart));
  if (projectKey && projectState && projectTouchedBuckets) {
    const projectBucket = getProjectBucket(
      projectState,
      projectKey,
      source,
      bucketStart,
      projectRef,
    );
    addTotals(projectBucket.totals, delta);
    projectTouchedBuckets.add(projectBucketKey(projectKey, source, bucketStart));
  }
  return {
    messageKey,
    lastTotals: currentTotals,
    fingerprint,
    dedupedForkCopy: false,
    eventsAggregated: 1,
    shouldUpdate: true,
  };
}

function normalizeOpencodeState(raw) {
  const state = raw && typeof raw === "object" ? raw : {};
  const messages = state.messages && typeof state.messages === "object" ? state.messages : {};
  return {
    messages,
    dbCursor: state.dbCursor && typeof state.dbCursor === "object" ? state.dbCursor : null,
    updatedAt: typeof state.updatedAt === "string" ? state.updatedAt : null,
  };
}

function deriveOpencodeMessageKey(msg, fallback) {
  const sessionId = normalizeMessageKeyPart(msg?.sessionID || msg?.sessionId || msg?.session_id);
  const messageId = normalizeMessageKeyPart(msg?.id || msg?.messageID || msg?.messageId);
  if (sessionId && messageId) return `${sessionId}|${messageId}`;
  return fallback;
}

// ── OpenCode fork-copy dedup (issue #426) ──────────────────────────────────
//
// `Session.fork` copies every parent message up to the fork point into a BRAND
// NEW session. Verified against the shipped opencode binary: the copy is
// `{ ...parentMessage.info, sessionID: newSession.id, id: newMessageId }` — so
// `time.created`, `time.completed`, `modelID`, `providerID` and the entire
// `tokens` payload survive verbatim and only the two identity fields change.
// The forked session is created WITHOUT a parentID (confirmed empirically:
// POST /session/:id/fork returns parentID=null), so the copies cannot be linked
// to their origin by ancestry — content is the only available discriminator.
// Because our dedup key is `sessionID|messageID`, every copied prefix used to be
// counted a second time (a real fork of a 170-message session re-added 20.3M
// tokens; the reporter measured ~2.0B across ~20 forks).
//
// The fingerprint is deliberately narrow — issue #187 is the standing reminder
// that a loose dedup predicate deletes real usage. A collision requires the SAME
// millisecond for BOTH created and completed, byte-equal values in all five
// token columns, the same model, the same provider AND the same source. On top
// of that we only ever drop a match that lives in a DIFFERENT session, since
// forking always mints a new one — two turns inside one session are never
// treated as copies of each other.
function deriveOpencodeMessageFingerprint({ msg, totals, source }) {
  if (!totals || isAllZeroUsage(totals)) return null;
  const created = coerceEpochMs(msg?.time?.created) || 0;
  const completed = coerceEpochMs(msg?.time?.completed) || 0;
  if (!created && !completed) return null;
  // v1 keeps flat modelID/providerID strings; v2 nests them in
  // `model: { id, providerID }` — normalize both (see normalizeOpencodeModelFields).
  const { modelId, providerId } = normalizeOpencodeModelFields(msg);
  const model = modelId || DEFAULT_MODEL;
  const provider = providerId;
  const raw = [
    normalizeSourceInput(source) || "opencode",
    created,
    completed,
    totals.input_tokens,
    totals.output_tokens,
    totals.cached_input_tokens,
    totals.cache_creation_input_tokens,
    totals.reasoning_output_tokens,
    model,
    provider,
  ].join("\u0000");
  // Hashed rather than stored raw: the fingerprint is persisted per message in
  // cursors.json, and heavy OpenCode users carry tens of thousands of entries.
  return crypto.createHash("sha256").update(raw).digest("base64url").slice(0, 22);
}

// `fingerprint -> Set<messageKey>` for counted messages in this cursor namespace.
// Same-session duplicates are all retained as owners; the first session remains
// canonical for cross-session fork copies. Tombstoned copies never claim ownership.
function buildOpencodeFingerprintIndex(messageIndex, wantedFingerprints = null) {
  const byFingerprint = new Map();
  if (!messageIndex || typeof messageIndex !== "object") return byFingerprint;
  for (const key in messageIndex) {
    const entry = messageIndex[key];
    if (entry?.dedupedForkCopy === true) continue;
    const fingerprint = entry && typeof entry.fingerprint === "string" ? entry.fingerprint : null;
    if (fingerprint && (!wantedFingerprints || wantedFingerprints.has(fingerprint))) {
      if (!byFingerprint.has(fingerprint)) byFingerprint.set(fingerprint, new Set());
      byFingerprint.get(fingerprint).add(key);
    }
  }
  return byFingerprint;
}

function opencodeMessageKeySession(messageKey) {
  if (typeof messageKey !== "string") return null;
  const idx = messageKey.indexOf("|");
  return idx > 0 ? messageKey.slice(0, idx) : null;
}

// True only when an already-counted message in ANOTHER session carries the exact
// same fingerprint — i.e. this row is a fork copy. Keys without a resolvable
// session (the JSON reader's file-path fallback) never dedup: unproven identity
// must not delete usage.
function isOpencodeForkCopy(fingerprintIndex, fingerprint, messageKey) {
  if (!fingerprint || !fingerprintIndex) return false;
  const owners = fingerprintIndex.get(fingerprint);
  if (!owners) return false;
  const session = opencodeMessageKeySession(messageKey);
  if (!session) return false;
  let hasCrossSessionOwner = false;
  for (const owner of owners instanceof Set ? owners : [owners]) {
    const ownerSession = opencodeMessageKeySession(owner);
    if (!ownerSession) continue;
    if (owner === messageKey) return false;
    if (ownerSession !== session) hasCrossSessionOwner = true;
  }
  return hasCrossSessionOwner;
}

function normalizeOpencodeAttribution(raw) {
  if (typeof raw === "string") {
    const [bucketStart = "", model = "", projectKey = "", projectRef = ""] = raw.split("\t");
    if (!bucketStart || !model) return null;
    return {
      bucketStart,
      model,
      projectKey: projectKey || null,
      projectRef: projectRef || null,
    };
  }
  if (!raw || typeof raw !== "object") return null;
  const bucketStart = typeof raw.bucketStart === "string" ? raw.bucketStart : "";
  const model = typeof raw.model === "string" ? raw.model : "";
  if (!bucketStart || !model) return null;
  return {
    bucketStart,
    model,
    projectKey: typeof raw.projectKey === "string" ? raw.projectKey : null,
    projectRef: typeof raw.projectRef === "string" ? raw.projectRef : null,
  };
}

function encodeOpencodeAttribution(raw) {
  const attribution = normalizeOpencodeAttribution(raw);
  if (!attribution) return null;
  return [
    attribution.bucketStart,
    attribution.model,
    attribution.projectKey || "",
    attribution.projectRef || "",
  ].join("\t");
}

function sameOpencodeAttribution(a, b) {
  const left = normalizeOpencodeAttribution(a);
  const right = normalizeOpencodeAttribution(b);
  if (!left || !right) return left === right;
  return (
    left.bucketStart === right.bucketStart &&
    left.model === right.model &&
    left.projectKey === right.projectKey &&
    left.projectRef === right.projectRef
  );
}

// Persist a message's snapshot + fingerprint and claim the fingerprint for it.
// Writes only when something actually changed so a steady-state sync leaves the
// cursor untouched. A falsy `fingerprint` means "unknown" and preserves whatever
// the entry already carried; a new one releases the old claim so a stale
// mid-stream snapshot cannot shadow an unrelated message later.
function recordOpencodeMessage({
  messageIndex,
  fingerprintIndex,
  messageKey,
  totals,
  fingerprint,
  attribution,
  dedupedForkCopy = false,
}) {
  if (!messageIndex || !messageKey) return;
  const prev = messageIndex[messageKey];
  const prevTotals = prev && typeof prev.lastTotals === "object" ? prev.lastTotals : null;
  const prevFingerprint = prev && typeof prev.fingerprint === "string" ? prev.fingerprint : null;
  const nextFingerprint = fingerprint || prevFingerprint;
  const prevAttribution = normalizeOpencodeAttribution(prev?.attribution);
  const nextAttribution = normalizeOpencodeAttribution(attribution) || prevAttribution;
  const prevDeduped = prev?.dedupedForkCopy === true;
  if (
    sameGeminiTotals(totals, prevTotals) &&
    prevFingerprint === nextFingerprint &&
    sameOpencodeAttribution(prevAttribution, nextAttribution) &&
    prevDeduped === Boolean(dedupedForkCopy)
  ) return;

  if (
    fingerprintIndex &&
    prevFingerprint &&
    (prevFingerprint !== nextFingerprint || dedupedForkCopy)
  ) {
    const owners = fingerprintIndex.get(prevFingerprint);
    if (owners instanceof Set) {
      owners.delete(messageKey);
      if (owners.size === 0) fingerprintIndex.delete(prevFingerprint);
    } else if (owners === messageKey) {
      fingerprintIndex.delete(prevFingerprint);
    }
  }
  const entry = { lastTotals: totals, updatedAt: new Date().toISOString() };
  if (nextFingerprint) entry.fingerprint = nextFingerprint;
  if (nextAttribution) entry.attribution = encodeOpencodeAttribution(nextAttribution);
  if (dedupedForkCopy) entry.dedupedForkCopy = true;
  messageIndex[messageKey] = entry;
  if (fingerprintIndex && nextFingerprint && !dedupedForkCopy) {
    if (!fingerprintIndex.has(nextFingerprint)) {
      fingerprintIndex.set(nextFingerprint, new Set());
    }
    const owners = fingerprintIndex.get(nextFingerprint);
    if (owners instanceof Set) owners.add(messageKey);
  }
}

function subtractCountedOpencodeMessage({
  attribution,
  totals,
  source,
  hourlyState,
  touchedBuckets,
  projectState,
  projectTouchedBuckets,
}) {
  const countedAt = normalizeOpencodeAttribution(attribution);
  if (!countedAt) return false;
  const counted = { ...totals, conversation_count: 1 };
  const bucket = getHourlyBucket(
    hourlyState,
    source,
    countedAt.model,
    countedAt.bucketStart,
  );
  subtractTotals(bucket.totals, counted);
  touchedBuckets.add(bucketKey(source, countedAt.model, countedAt.bucketStart));
  if (countedAt.projectKey && projectState && projectTouchedBuckets) {
    const projectBucket = getProjectBucket(
      projectState,
      countedAt.projectKey,
      source,
      countedAt.bucketStart,
      countedAt.projectRef,
    );
    subtractTotals(projectBucket.totals, counted);
    projectTouchedBuckets.add(
      projectBucketKey(countedAt.projectKey, source, countedAt.bucketStart),
    );
  }
  return true;
}

function repairCountedOpencodeForkCopy({
  msg,
  attribution,
  totals,
  source,
  hourlyState,
  touchedBuckets,
  projectState,
  projectTouchedBuckets,
  projectRef,
  projectKey,
}) {
  let countedAt = normalizeOpencodeAttribution(attribution);
  if (!countedAt) {
    const timestampMs = coerceEpochMs(msg?.time?.completed) || coerceEpochMs(msg?.time?.created);
    if (!timestampMs) return false;
    const bucketStart = toUtcHalfHourStart(new Date(timestampMs).toISOString());
    if (!bucketStart) return false;
    const { modelId: repairModelId } = normalizeOpencodeModelFields(msg);
    countedAt = {
      bucketStart,
      model: repairModelId || DEFAULT_MODEL,
      projectKey,
      projectRef,
    };
  }
  return subtractCountedOpencodeMessage({
    attribution: countedAt,
    totals,
    source,
    hourlyState,
    touchedBuckets,
    projectState,
    projectTouchedBuckets,
  });
}

// OpenCode v1 stores the model as flat strings on every assistant message
// (`modelID`, `providerID`), while OpenCode v2 (the `opencode2` beta, which
// moved messages into the `session_message` table) nests them inside a single
// object: `model: { id, providerID, variant }`. Some forks also wrote a plain
// string into `model`. Resolve both shapes to { modelId, providerId } so
// bucket keys and fork fingerprints stay identical across versions.
function normalizeOpencodeModelFields(msg) {
  const directModel =
    normalizeModelInput(msg?.modelID) ||
    normalizeModelInput(typeof msg?.model === "string" ? msg.model : null) ||
    normalizeModelInput(msg?.modelId);
  if (directModel) {
    return {
      modelId: directModel,
      providerId: normalizeMessageKeyPart(msg?.providerID || msg?.provider || msg?.providerId),
    };
  }
  const nested = msg?.model;
  if (nested && typeof nested === "object") {
    return {
      modelId: normalizeModelInput(nested.id),
      providerId: normalizeMessageKeyPart(nested.providerID),
    };
  }
  return { modelId: null, providerId: "" };
}

function normalizeOpencodeTokens(tokens) {
  if (!tokens || typeof tokens !== "object") return null;
  const input = toNonNegativeInt(tokens.input);
  const output = toNonNegativeInt(tokens.output);
  const reasoning = toNonNegativeInt(tokens.reasoning);
  const cached = toNonNegativeInt(tokens.cache?.read);
  const cacheWrite = toNonNegativeInt(tokens.cache?.write);
  const total = input + output + reasoning + cached + cacheWrite;

  return {
    input_tokens: input,
    cached_input_tokens: cached,
    cache_creation_input_tokens: cacheWrite,
    output_tokens: output,
    reasoning_output_tokens: reasoning,
    total_tokens: total,
  };
}

// OpenCode rows are authoritative snapshots of one message and can be
// corrected downward or move tokens between cache/output columns. Signed
// deltas replace the prior contribution instead of treating a correction as a
// fresh cumulative reset. Gemini keeps its provider-specific reset behavior.
function diffOpencodeTotals(current, previous) {
  if (!current || typeof current !== "object") return null;
  if (!previous || typeof previous !== "object") return current;
  if (sameGeminiTotals(current, previous)) return null;

  const delta = {};
  for (const key of [
    "input_tokens",
    "cached_input_tokens",
    "cache_creation_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
  ]) {
    delta[key] = Number(current[key] || 0) - Number(previous[key] || 0);
  }
  return isAllZeroUsage(delta) ? null : delta;
}

async function walkOpencodeMessages(dir, out) {
  const entries = await safeReadDir(dir);
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      await walkOpencodeMessages(fullPath, out);
      continue;
    }
    if (entry.isFile() && entry.name.startsWith("msg_") && entry.name.endsWith(".json"))
      out.push(fullPath);
  }
}

// ---------------------------------------------------------------------------
// OpenCode SQLite DB reader
//
// Four real database shapes exist in the wild (see PR #501 / opencode2):
//   A  pure v1:        `message`(data) + `session_message`(empty) [+ `session`]
//   B  beta-17887:     `session_message`(data) + `session_v2`
//   C  upstream v2:    `session_message`(data) + `session`
//   D  transitional:   `message`(old data) + `session_message`(new data)
//
// The reader must serve all four without firing a doomed SQL at type A (an
// empty `session_message` table still EXISTS — querying it with a JOIN against
// `session_v2` that does not exist throws "no such table" every sync, and the
// old code silently swallowed that error). The combined probe below answers
// both "is there v2 data?" and "which session table exists?" in one round-trip.
// ---------------------------------------------------------------------------

const OPENCODE_DB_CURSOR_VERSION = 1;

function normalizeOpencodeDbWatermark(raw) {
  if (!raw || typeof raw !== "object") return null;
  const maxRowId = Math.max(0, Math.floor(Number(raw.maxRowId) || 0));
  const maxUpdatedAt = Math.max(0, Math.floor(Number(raw.maxUpdatedAt) || 0));
  const anchor = typeof raw.anchor === "string" && raw.anchor ? raw.anchor : null;
  return maxRowId || maxUpdatedAt ? { maxRowId, maxUpdatedAt, anchor } : null;
}

function opencodeDbIdentity(dbPath) {
  try {
    const stat = fssync.statSync(dbPath);
    const resolved = path.resolve(dbPath);
    return {
      pathHash: crypto.createHash("sha256").update(resolved).digest("hex"),
      dev: String(stat.dev || 0),
      ino: String(stat.ino || 0),
    };
  } catch (_e) {
    return null;
  }
}

function sameOpencodeDbIdentity(a, b) {
  return Boolean(
    a &&
    b &&
    a.pathHash === b.pathHash &&
    a.dev === b.dev &&
    a.ino === b.ino,
  );
}

function opencodeDbRowAnchor(row) {
  if (!row || typeof row !== "object") return null;
  const rowId = Math.max(0, Math.floor(Number(row.row_id) || 0));
  const id = typeof row.id === "string" ? row.id : "";
  const created = Math.max(0, Math.floor(Number(row.time_created) || 0));
  if (!rowId || !id) return null;
  return crypto.createHash("sha256").update(`${rowId}\0${id}\0${created}`).digest("base64url");
}

function opencodeDbIncrementalPredicate(alias, watermark) {
  const cursor = normalizeOpencodeDbWatermark(watermark);
  if (!cursor) return "";
  const clauses = [];
  if (cursor.maxRowId) clauses.push(`${alias}rowid > ${cursor.maxRowId}`);
  // Re-read the boundary timestamp so simultaneous updates cannot fall through
  // a strict greater-than watermark. The duplicate is removed by messageIndex.
  if (cursor.maxUpdatedAt) clauses.push(`${alias}time_updated >= ${cursor.maxUpdatedAt}`);
  return clauses.length > 0 ? ` AND (${clauses.join(" OR ")})` : "";
}

function buildV1Sql(watermark = null) {
  return (
    `SELECT rowid AS row_id, id, session_id, time_updated, data FROM message ` +
    `WHERE json_extract(data, '$.role') = 'assistant'` +
    `${opencodeDbIncrementalPredicate("", watermark)} ORDER BY time_created ASC`
  );
}

// Build the v2 query. When a session table exists the LEFT JOIN restores the
// project directory for downstream attribution; when it does not (type B
// without session_v2, or an exotic fork) the join and directory column are
// omitted and tokens are counted as-is.
function buildV2Sql(sessionTable, watermark = null) {
  const joinClause = sessionTable
    ? `LEFT JOIN ${sessionTable} s ON s.id = sm.session_id `
    : "";
  const directorySelect = sessionTable
    ? `s.directory AS directory, `
    : "";
  return (
    `SELECT sm.rowid AS row_id, sm.id AS id, sm.session_id AS session_id, sm.time_updated AS time_updated, ` +
    `${directorySelect}sm.data AS data ` +
    `FROM session_message sm ${joinClause}` +
    `WHERE sm.type = 'assistant'` +
    `${opencodeDbIncrementalPredicate("sm.", watermark)} ORDER BY sm.time_created ASC`
  );
}

// One combined probe: hasRows (is there any data in session_message?) plus
// sessionTable (which session table exists, session_v2 preferred over session
// via DESC). Returns null when the probe itself fails — callers treat that as
// v1-only, matching the pre-existing silent-degradation contract.
function detectOpencodeMessageLayout(dbPath, sqliteOptions = {}, { propagateReadFailure = false } = {}) {
  let rows;
  try {
    rows = readSqliteJsonRows(
      dbPath,
      `SELECT (SELECT 1 FROM session_message LIMIT 1) AS hasRows,
              (SELECT name FROM sqlite_master WHERE name IN ('session','session_v2') ORDER BY name DESC LIMIT 1) AS sessionTable`,
      { label: "OpenCode", timeout: 10_000, maxBuffer: 1024 * 1024, ...sqliteOptions, throwOnReadFailure: true },
    );
  } catch (error) {
    // A missing session_message table is the normal v1-only miss; with
    // propagateReadFailure a real read failure must reach the caller — the
    // session scanner would otherwise skip the v2 generation silently and
    // cache a zero-token inventory over a readable-in-principle database.
    if (propagateReadFailure && !isMissingSqliteTableError(error)) throw error;
    return null;
  }
  if (!Array.isArray(rows) || rows.length === 0) return null;
  const row = rows[0];
  return {
    hasRows: Boolean(row?.hasRows),
    sessionTable: typeof row?.sessionTable === "string" && row.sessionTable.trim()
      ? row.sessionTable.trim()
      : null,
  };
}

// Shared provider resolver: v1 rows carry a top-level providerID, v2 rows nest
// it under model.providerID. Used by the mimo/zcode discriminators so they work
// against both generations without duplicating the resolution logic.
function opencodeMessageProvider(data) {
  return data?.providerID || data?.model?.providerID || "";
}

// readSqliteJsonRows wraps its two backend errors into the thrown Error, so a
// "table genuinely absent" miss and a "database unreadable" failure look the
// same to a caller that only catches. Both backends spell a missing table
// "no such table" — use that to keep the normal miss on the tolerant path
// while real read failures can propagate.
function isMissingSqliteTableError(error) {
  const messages = [error?.message, error?.cliError?.message, error?.nodeSqliteError?.message];
  return messages.some((m) => typeof m === "string" && m.includes("no such table"));
}

function readOpencodeDbMessagesIncremental(dbPath, previousCursor = null, sqliteOptions = {}, { propagateReadFailure = false, includeZeroMessage } = {}) {
  if (!dbPath || !fssync.existsSync(dbPath)) return { messages: [], cursor: null };

  const identity = opencodeDbIdentity(dbPath);
  const canResume =
    previousCursor?.version === OPENCODE_DB_CURSOR_VERSION &&
    sameOpencodeDbIdentity(previousCursor.identity, identity);

  let snapshot = null;
  let effectiveDbPath = dbPath;
  if (isUncPath(dbPath)) {
    try {
      snapshot = snapshotSqliteDb(dbPath);
      effectiveDbPath = snapshot.path;
    } catch (_e) { }
  }

  // Parse raw SQL rows into the shared { id, sessionID, timeUpdated, data }
  // shape; optionally restore v2's session-level project directory as the
  // per-message `path.cwd` the rest of the pipeline expects.
  //
  // D-state (transitional) union: both the legacy `message` table and the new
  // `session_message` table may carry rows. Cross-table duplicates are caught
  // by the existing cursor-key + #426 fingerprint dedup in
  // parseOpencodeDbIncremental, so unioning here is safe — the downstream
  // parser collapses identical sessionID|messageID pairs before bucketing.
  const appendRows = (rows, isV2, out) => {
    for (const row of rows) {
      if (!row || typeof row.data !== "string") continue;
      let data;
      try {
        data = JSON.parse(row.data);
      } catch (_e) {
        continue;
      }
      const tokens = data?.tokens;
      if (!tokens || typeof tokens !== "object") continue;
      // Skip messages with no meaningful token data
      const hasTokens =
        toNonNegativeInt(tokens.input) > 0 ||
        toNonNegativeInt(tokens.output) > 0 ||
        toNonNegativeInt(tokens.reasoning) > 0 ||
        toNonNegativeInt(tokens.cache?.read) > 0 ||
        toNonNegativeInt(tokens.cache?.write) > 0;
      const message = {
        id: row.id || data.id,
        sessionID: row.session_id || data.sessionID,
        timeUpdated: row.time_updated || 0,
        data,
      };
      // Providers can retain zero corrections for previously counted messages.
      // New empty records remain excluded unless the caller recognizes them.
      if (!hasTokens && !(typeof includeZeroMessage === "function" && includeZeroMessage(message))) continue;
      if (isV2 && typeof row.directory === "string" && row.directory.trim()) {
        data.path = { ...(typeof data.path === "object" && data.path ? data.path : {}), cwd: row.directory };
      }
      out.push(message);
    }
  };

  const readGeneration = (sql) => {
    try {
      const rows = readSqliteJsonRows(effectiveDbPath, sql, {
        label: "OpenCode",
        maxBuffer: 50 * 1024 * 1024,
        timeout: 30_000,
        ...sqliteOptions,
        throwOnReadFailure: true,
      });
      return Array.isArray(rows) ? rows : [];
    } catch (error) {
      // A missing table is the normal "other generation's layout" miss; an
      // unreadable database is not — with propagateReadFailure the session
      // scan aborts instead of caching a zero-token inventory over it.
      if (propagateReadFailure && !isMissingSqliteTableError(error)) throw error;
      return null; // query-level failure (e.g. wrong generation's table)
    }
  };

  const readMaxRowId = (table) => {
    const rows = readGeneration(`SELECT MAX(rowid) AS max_row_id FROM ${table}`);
    if (!rows) return null;
    return Math.max(0, Math.floor(Number(rows[0]?.max_row_id) || 0));
  };

  const readRowAnchor = (table, rowId) => {
    if (!rowId) return null;
    const rows = readGeneration(
      `SELECT rowid AS row_id, id, time_created FROM ${table} WHERE rowid = ${rowId}`,
    );
    return rows ? opencodeDbRowAnchor(rows[0]) : null;
  };

  const readCursorGeneration = ({ table, isV2, sql }) => {
    const previous = canResume
      ? normalizeOpencodeDbWatermark(previousCursor?.[isV2 ? "v2" : "v1"])
      : null;
    let forceReplay = false;
    let lastRows = [];

    // Each query uses a separate SQLite reader. Verify the same immutable head
    // row before and after the data query so an in-place database replacement
    // cannot make us persist a cursor from a generation we did not read.
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const currentMaxRowId = readMaxRowId(table);
      if (currentMaxRowId === null) return null;
      const currentAnchor = readRowAnchor(table, currentMaxRowId);
      const previousAnchor = previous
        ? readRowAnchor(table, previous.maxRowId)
        : null;
      const watermark =
        !forceReplay &&
        previous?.anchor &&
        currentMaxRowId >= previous.maxRowId &&
        previousAnchor === previous.anchor
          ? previous
          : null;
      const rows = readGeneration(sql(watermark));
      if (!rows) return null;
      lastRows = rows;

      if (readRowAnchor(table, currentMaxRowId) !== currentAnchor) {
        forceReplay = true;
        continue;
      }

      let maxUpdatedAt = watermark?.maxUpdatedAt || 0;
      for (const row of rows) {
        maxUpdatedAt = Math.max(maxUpdatedAt, Math.floor(Number(row?.time_updated) || 0));
      }
      return {
        rows,
        // Advance only to the head whose anchor bracketed this query. Rows
        // appended concurrently may be returned now and harmlessly reread on
        // the next sync; advancing to them would require an unverified anchor.
        cursor: {
          maxRowId: currentMaxRowId,
          maxUpdatedAt,
          anchor: currentAnchor,
        },
      };
    }

    // Keep parsed usage but refuse to advance while the database is unstable.
    return { rows: lastRows, cursor: null };
  };

  try {
    // Combined probe drives the orchestration: v1 always runs (it is the
    // baseline every database carries or degrades to), v2 runs only when the
    // probe confirms session_message has rows — this avoids firing a JOIN
    // against a non-existent session table on type-A databases.
    const probe = detectOpencodeMessageLayout(effectiveDbPath, sqliteOptions, { propagateReadFailure });
    const out = [];
    const nextCursor = {
      version: OPENCODE_DB_CURSOR_VERSION,
      identity,
      v1: null,
      v2: null,
    };

    const v1 = readCursorGeneration({
      table: "message",
      isV2: false,
      sql: buildV1Sql,
    });
    if (v1) {
      appendRows(v1.rows, false, out);
      nextCursor.v1 = v1.cursor;
    }

    if (probe?.hasRows) {
      const v2 = readCursorGeneration({
        table: "session_message",
        isV2: true,
        sql: (watermark) => buildV2Sql(probe.sessionTable, watermark),
      });
      if (v2) {
        appendRows(v2.rows, true, out);
        nextCursor.v2 = v2.cursor;
      }
    }

    return {
      messages: out,
      cursor: nextCursor.v1 || nextCursor.v2 ? nextCursor : null,
    };
  } finally {
    if (snapshot) snapshot.cleanup();
  }
}

function readOpencodeDbMessages(dbPath, sqliteOptions = {}, { propagateReadFailure = false, includeZeroMessage } = {}) {
  return readOpencodeDbMessagesIncremental(dbPath, null, sqliteOptions, { propagateReadFailure, includeZeroMessage }).messages;
}

// mimocode mirrors the user's Claude Code + claude-mem history into its own
// `message` table — via an explicit `claude_import` AND a live observer /
// session sync — so the overwhelming majority of rows are anthropic-endpoint
// turns (providerID="anthropic") the Claude parser ALREADY counts as
// source=claude. On the dev's box that's ~3.9B mirrored tokens vs ~22M genuine
// mimo tokens. Counting the mirror under "mimo" double-counts and mislabels
// Claude usage (user saw claude-* rows under the MIMO provider).
//
// The discriminator is providerID, NOT the model id. mimo's own runtime tags
// turns providerID="mimo" (its auto router) or "xiaomi". providerID="anthropic"
// means the turn went through a Claude-compatible endpoint — plain Claude Code,
// OR a mimo-named model the user picked IN Claude Code (e.g. model=mimo-v2.5-pro
// run inside Claude Code, logged in ~/.claude, counted as source=claude). Keying
// off the model id would wrongly re-count that mimo-v2.5-pro. claude_import is
// irrelevant — it never covers the observer/session mirror; the provider rule
// subsumes it.
function isMimoNativeMessage(data) {
  if (!data) return false;
  const provider = String(opencodeMessageProvider(data)).toLowerCase();
  return provider === "mimo" || provider === "xiaomi";
}

// Read only genuine mimo assistant messages (mimo's own models), dropping the
// mirrored Claude/claude-mem rows. See isMimoNativeMessage for why.
function readMimoDbMessages(dbPath, sqliteOptions = {}) {
  if (!dbPath || !fssync.existsSync(dbPath)) return [];
  const all = readOpencodeDbMessages(dbPath, sqliteOptions);
  return all.filter((m) => isMimoNativeMessage(m.data));
}

// ZCode is Z.ai's (Zhipu) coding agent — another OpenCode-fork that stores
// assistant messages in the identical `message` table schema
// (~/.zcode/cli/db/db.sqlite). Its own agent runs GLM models through Z.ai /
// BigModel endpoints (providerID "builtin:zai-start-plan",
// "builtin:bigmodel-coding-plan", …), but ZCode also lets users add CUSTOM
// providers (a built-in feature beyond the Z.ai plan subscription) that point at
// ANY model — MiMo, Sakana Fugu, any OpenAI-compatible proxy. A user-defined
// provider is assigned a random UUID as its providerID (observed on a real box:
// model "mimo-v2.5-pro" under providerID "265956bf-…"), so an allowlist of known
// vendor keywords can NEVER match it and silently drops every custom-provider
// turn (issue #216). Those turns live ONLY in this DB, so we must keep them or
// they go uncounted entirely. The exception is the bundled
// claude-code / codex / gemini-cli sub-agents ZCode can orchestrate: those carry
// providerID "anthropic"/"openai"/"google" and write to ~/.claude / ~/.codex /
// ~/.gemini, so the standalone Claude/Codex/Gemini parsers already count them —
// DROP those here to avoid double-counting. Hence a blocklist (exclude the three
// direct vendors), not an allowlist that would silently miss every third-party
// model. Key off providerID, NEVER the model id — a GLM/Claude model the user
// ran *inside* Claude Code is source=claude, so matching the model name would
// re-count it. Mirrors the Mimo discipline.
function isZcodeNativeMessage(data) {
  if (!data) return false;
  const provider = String(opencodeMessageProvider(data)).toLowerCase();
  if (!provider) return false;
  return !(
    provider.includes("anthropic") ||
    provider.includes("openai") ||
    provider.includes("google")
  );
}

// ZCode persists inclusive parent counters in both its legacy OpenCode tables
// and the newer model_usage table: cache read/write are already included in
// input, and reasoning is already included in output. The shared OpenCode
// parser expects disjoint columns, so split the subsets before it computes
// queue totals and cost (issue #554).
function normalizeZcodeInclusiveTokens(tokens) {
  if (!tokens || typeof tokens !== "object") return tokens;
  const rawInput = toNonNegativeInt(tokens.input);
  const rawOutput = toNonNegativeInt(tokens.output);
  const cacheRead = toNonNegativeInt(tokens.cache?.read);
  const cacheWrite = toNonNegativeInt(tokens.cache?.write);
  const reasoning = toNonNegativeInt(tokens.reasoning);
  return {
    ...tokens,
    input: Math.max(0, rawInput - cacheRead - cacheWrite),
    output: Math.max(0, rawOutput - reasoning),
    reasoning,
    cache: {
      ...(tokens.cache && typeof tokens.cache === "object" ? tokens.cache : {}),
      read: cacheRead,
      write: cacheWrite,
    },
  };
}

function normalizeZcodeLegacyMessage(message) {
  if (!message?.data?.tokens) return message;
  return {
    ...message,
    data: {
      ...message.data,
      tokens: normalizeZcodeInclusiveTokens(message.data.tokens),
    },
  };
}

const ZCODE_NATIVE_USAGE_COLUMNS = new Set([
  "id",
  "logical_request_id",
  "attempt_index",
  "session_id",
  "provider_id",
  "model_id",
  "status",
  "started_at",
  "input_tokens",
  "output_tokens",
  "reasoning_tokens",
  "cache_creation_input_tokens",
  "cache_read_input_tokens",
]);

function detectZcodeNativeUsageLayout(dbPath, sqliteOptions = {}, { propagateReadFailure = false } = {}) {
  if (!dbPath || !fssync.existsSync(dbPath)) return null;
  let rows;
  try {
    rows = readSqliteJsonRows(
      dbPath,
      `SELECT 'model_usage' AS table_name, name FROM pragma_table_info('model_usage')
       UNION ALL
       SELECT 'session' AS table_name, name FROM pragma_table_info('session')`,
      {
        label: "ZCode",
        timeout: 10_000,
        maxBuffer: 1024 * 1024,
        ...sqliteOptions,
        throwOnReadFailure: true,
      },
    );
  } catch (error) {
    // propagateReadFailure: an unreadable db is a failure, not "no native
    // layout" — the session scanner must skip the file, not fall back to an
    // empty legacy read and cache zero tokens.
    if (propagateReadFailure) throw error;
    return null;
  }
  const modelUsageColumns = new Set(
    rows
      .filter((row) => !row?.table_name || row.table_name === "model_usage")
      .map((row) => String(row?.name || "")),
  );
  if (![...ZCODE_NATIVE_USAGE_COLUMNS].every((name) => modelUsageColumns.has(name))) {
    return null;
  }
  const sessionColumns = new Set(
    rows
      .filter((row) => row?.table_name === "session")
      .map((row) => String(row?.name || "")),
  );
  return {
    hasSessionDirectory: sessionColumns.has("id") && sessionColumns.has("directory"),
  };
}

function hasZcodeNativeUsageSchema(dbPath, sqliteOptions = {}) {
  if (!dbPath || !fssync.existsSync(dbPath)) return false;
  let snapshot = null;
  let effectiveDbPath = dbPath;
  if (isUncPath(dbPath)) {
    try {
      snapshot = snapshotSqliteDb(dbPath);
      effectiveDbPath = snapshot.path;
    } catch (_error) {
      // Fall through to the direct read: some UNC servers support SQLite's
      // read-only locking semantics even when the WSL bridge does not.
    }
  }
  try {
    return detectZcodeNativeUsageLayout(effectiveDbPath, sqliteOptions) !== null;
  } finally {
    if (snapshot) snapshot.cleanup();
  }
}

// true: readable with content; false: confirmed empty; null: unavailable or
// unreadable. Only a confirmed empty database is safe to replace during discovery.
function zcodeDbHasContent(dbPath, sqliteOptions = {}) {
  if (!dbPath || !fssync.existsSync(dbPath)) return null;
  let snapshot = null;
  let effectiveDbPath = dbPath;
  if (isUncPath(dbPath)) {
    try {
      snapshot = snapshotSqliteDb(dbPath);
      effectiveDbPath = snapshot.path;
    } catch (_error) {
      // Preserve the existing direct-read fallback for transient snapshot
      // failures and UNC implementations that support SQLite locking.
    }
  }
  try {
    const rows = readSqliteJsonRows(
      effectiveDbPath,
      "SELECT (SELECT COUNT(*) FROM session) AS sessions, (SELECT COUNT(*) FROM model_usage) AS requests",
      {
        label: "ZCode",
        timeout: 10_000,
        maxBuffer: 1024 * 1024,
        ...sqliteOptions,
        throwOnReadFailure: true,
      },
    );
    const row = Array.isArray(rows) ? rows[0] : null;
    if (!row) return null;
    return (Number(row.sessions) || 0) > 0 || (Number(row.requests) || 0) > 0;
  } catch (_error) {
    return null;
  } finally {
    if (snapshot) snapshot.cleanup();
  }
}

function hasZcodeCursorState(cursors) {
  return [cursors?.zcode, cursors?.zcode?.native, cursors?.zcode?.wsl]
    .some((state) => Object.keys(state?.messages || {}).length > 0) ||
    Object.keys(cursors?.hourly?.buckets || {}).some((key) => key.startsWith("zcode|")) ||
    Object.keys(cursors?.hourly?.groupQueued || {}).some((key) => key.startsWith("zcode|")) ||
    Object.keys(cursors?.projectHourly?.buckets || {}).some((key) => key.includes("|zcode|"));
}

// Sync, session discovery and status must select the same installs. Once sync
// records a priority-mode choice, retain it: switching databases underneath a
// flat cursor would accumulate both installs in the same usage ledger.
function resolveZcodeInstallPaths({ nativeValue, wslValue } = {}, env = process.env, deps = {}) {
  const mode = wsl.getWslMode(env);
  const priorityMode = (deps.platform || process.platform) === "win32" && ["native-first", "wsl-first"].includes(mode);
  const selection = deps.selection || deps.cursors?.zcodeInstallSelection;
  if (priorityMode && selection?.mode === mode &&
      selection.zcodeHome === (env.ZCODE_HOME?.trim() || null) &&
      typeof selection.dbPath === "string" && selection.dbPath) {
    // A newly discovered or temporarily unavailable install must not change
    // the cursor's owner. Only an explicit mode/home override releases it.
    return { native: selection.dbPath, wsl: null };
  }
  const paths = resolveInstallPaths({ nativeValue, wslValue }, env, deps);
  if (!priorityMode || (!selection && hasZcodeCursorState(deps.cursors))) {
    // Legacy cursors followed the original install resolver. Keep that choice
    // rather than feeding another database into an already-owned flat ledger.
    return paths;
  }
  const picked = paths.native || paths.wsl;
  const alternate = picked === nativeValue ? wslValue : nativeValue;
  if (picked && alternate && zcodeDbHasContent(picked) === false && zcodeDbHasContent(alternate) === true) {
    return { native: alternate, wsl: null };
  }
  return paths;
}

function buildZcodeNativeUsageSql({ hasSessionDirectory }) {
  const directorySelect = hasSessionDirectory ? ", s.directory AS directory" : "";
  const directoryJoin = hasSessionDirectory
    ? " LEFT JOIN session AS s ON s.id = mu.session_id"
    : "";
  return `SELECT
    mu.id,
    mu.logical_request_id,
    mu.attempt_index,
    mu.session_id,
    mu.provider_id,
    mu.model_id,
    mu.started_at,
    mu.input_tokens,
    mu.output_tokens,
    mu.reasoning_tokens,
    mu.cache_creation_input_tokens,
    mu.cache_read_input_tokens
    ${directorySelect}
    FROM model_usage AS mu${directoryJoin}
    WHERE mu.status = 'completed'
      AND trim(mu.model_id) != ''
      AND (
        mu.input_tokens > 0 OR mu.output_tokens > 0 OR mu.reasoning_tokens > 0 OR
        mu.cache_creation_input_tokens > 0 OR mu.cache_read_input_tokens > 0
      )
    ORDER BY mu.started_at ASC, mu.id ASC`;
}

// `propagateReadFailure` (off by default): when the native usage layout is
// present but its row query fails, throw instead of returning null. The usage
// queue treats that fallthrough as "no native table, use legacy" — correct for
// its own resilience — but the session scanner must not cache a zero-token
// inventory over a failed read (the browser then hides the sessions entirely
// instead of reporting the db as skipped).
function readZcodeNativeUsageMessages(dbPath, sqliteOptions = {}, { propagateReadFailure = false } = {}) {
  let snapshot = null;
  let effectiveDbPath = dbPath;
  if (isUncPath(dbPath)) {
    try {
      snapshot = snapshotSqliteDb(dbPath);
      effectiveDbPath = snapshot.path;
    } catch (_error) {
      // Preserve the existing direct-read fallback for transient snapshot
      // failures and UNC implementations that support SQLite locking.
    }
  }

  let rows;
  try {
    const layout = detectZcodeNativeUsageLayout(effectiveDbPath, sqliteOptions, { propagateReadFailure });
    if (!layout) return null;
    rows = readSqliteJsonRows(effectiveDbPath, buildZcodeNativeUsageSql(layout), {
      label: "ZCode",
      maxBuffer: 50 * 1024 * 1024,
      timeout: 30_000,
      ...sqliteOptions,
      throwOnReadFailure: true,
    });
  } catch (error) {
    if (propagateReadFailure) throw error;
    return null;
  } finally {
    if (snapshot) snapshot.cleanup();
  }

  const messages = [];
  for (const row of rows) {
    const providerID = String(row?.provider_id || "").trim();
    const modelID = String(row?.model_id || "").trim();
    const sessionID = String(row?.session_id || "").trim();
    const logicalRequestId = String(row?.logical_request_id || "").trim();
    const attemptIndex = toNonNegativeInt(row?.attempt_index);
    const id = String(row?.id || "").trim() ||
      (logicalRequestId ? `${logicalRequestId}#${attemptIndex}` : "");
    const startedAt = coerceEpochMs(row?.started_at);
    if (!providerID || !modelID || !sessionID || !id || !startedAt) continue;

    const data = {
      id,
      sessionID,
      role: "assistant",
      providerID,
      modelID,
      time: { created: startedAt, completed: startedAt },
      tokens: normalizeZcodeInclusiveTokens({
        input: row?.input_tokens,
        output: row?.output_tokens,
        reasoning: row?.reasoning_tokens,
        cache: {
          read: row?.cache_read_input_tokens,
          write: row?.cache_creation_input_tokens,
        },
      }),
    };
    if (typeof row?.directory === "string" && row.directory.trim()) {
      data.path = { cwd: row.directory.trim() };
    }
    if (!isZcodeNativeMessage(data)) continue;
    messages.push({ id, sessionID, timeUpdated: startedAt, data });
  }
  return messages;
}

// Read only genuine ZCode assistant messages (its own GLM models via Z.ai /
// BigModel), dropping any bundled sub-agent turns. See isZcodeNativeMessage.
//
// ZCode started writing model_usage after many installations had already
// accumulated months of history in the OpenCode message tables. The native
// table is authoritative from its first completed row onward, but it is not a
// historical backfill. Keep legacy rows before that boundary so merely adding
// the new table cannot make older usage disappear from UsageCairn.
function readZcodeDbMessages(dbPath, sqliteOptions = {}, { propagateReadFailure = false } = {}) {
  if (!dbPath || !fssync.existsSync(dbPath)) return [];
  const nativeMessages = readZcodeNativeUsageMessages(dbPath, sqliteOptions, { propagateReadFailure });
  const legacyMessages = readOpencodeDbMessages(dbPath, sqliteOptions, { propagateReadFailure })
    .filter((message) => isZcodeNativeMessage(message.data))
    .map(normalizeZcodeLegacyMessage);
  if (nativeMessages === null) return legacyMessages;

  const nativeStartMs = nativeMessages.reduce((earliest, message) => {
    const timestampMs = coerceEpochMs(message?.timeUpdated);
    return timestampMs > 0 ? Math.min(earliest, timestampMs) : earliest;
  }, Number.POSITIVE_INFINITY);
  if (!Number.isFinite(nativeStartMs)) return legacyMessages;

  const historicalMessages = legacyMessages.filter((message) => {
    const timestampMs = coerceEpochMs(message?.timeUpdated);
    return timestampMs > 0 && timestampMs < nativeStartMs;
  });
  return [...historicalMessages, ...nativeMessages];
}

async function parseOpencodeDbIncremental({
  dbMessages,
  dbPath,
  dbCursor,
  cursors,
  queuePath,
  projectQueuePath,
  onProgress,
  source,
  cursorKey,
  publicRepoResolver,
  opencodeCursorStore,
  opencodeCursorNamespace = "flat",
}) {
  await ensureDir(path.dirname(queuePath));
  let messagesProcessed = 0;
  let eventsAggregated = 0;

  const cb = typeof onProgress === "function" ? onProgress : null;
  const messages = Array.isArray(dbMessages) ? dbMessages : [];
  const totalMessages = messages.length;
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const projectEnabled = typeof projectQueuePath === "string" && projectQueuePath.length > 0;
  const projectState = projectEnabled ? normalizeProjectState(cursors?.projectHourly) : null;
  const projectTouchedBuckets = projectEnabled ? new Set() : null;
  const projectMetaCache = projectEnabled ? new Map() : null;
  const publicRepoCache = projectEnabled ? new Map() : null;
  const cursorNamespace = typeof cursorKey === "string" && cursorKey.length > 0 ? cursorKey : "opencode";
  const touchedBuckets = new Set();
  const defaultSource = normalizeSourceInput(source) || "opencode";
  const candidateMessageKeys = new Set();
  const candidateFingerprints = new Set();
  for (const entry of messages) {
    const msgForKey = { ...entry?.data };
    if (entry?.id && !msgForKey.id) msgForKey.id = entry.id;
    if (entry?.sessionID && !msgForKey.sessionID) msgForKey.sessionID = entry.sessionID;
    const messageKey = deriveOpencodeMessageKey(msgForKey, null);
    if (messageKey) candidateMessageKeys.add(messageKey);
    const totals = normalizeOpencodeTokens(entry?.data?.tokens);
    const fingerprint = totals
      ? deriveOpencodeMessageFingerprint({ msg: entry.data, totals, source: defaultSource })
      : null;
    if (fingerprint) candidateFingerprints.add(fingerprint);
  }
  const loaded = cursorNamespace === "opencode" && opencodeCursorStore
    ? await opencodeCursorStore.loadOpencodeMessagesForBatch({
        namespace: opencodeCursorNamespace,
        messageKeys: candidateMessageKeys,
        fingerprints: candidateFingerprints,
      })
    : null;
  const opencodeState = normalizeOpencodeState(cursors?.[cursorNamespace]);
  const messageIndex = opencodeState.messages;
  // Direct parser callers and the legacy single-file store retain the in-memory
  // fallback; cursor-store generations provide the bounded reverse index.
  const fingerprintIndex = loaded?.fingerprintIndex ||
    buildOpencodeFingerprintIndex(messageIndex, candidateFingerprints);

  for (let idx = 0; idx < messages.length; idx++) {
    const entry = messages[idx];
    const msg = entry.data;
    if (!msg) continue;

    // DB stores id/sessionID as separate columns; inject into msg for key derivation
    const msgForKey = { ...msg };
    if (entry.id && !msgForKey.id) msgForKey.id = entry.id;
    if (entry.sessionID && !msgForKey.sessionID) msgForKey.sessionID = entry.sessionID;
    const messageKey = deriveOpencodeMessageKey(msgForKey, null);
    if (!messageKey) {
      messagesProcessed += 1;
      continue;
    }

    // Skip messages already indexed (from prior JSON-file parsing or previous DB sync)
    const prev = messageIndex[messageKey];
    const lastTotals = prev && typeof prev.lastTotals === "object" ? prev.lastTotals : null;

    const currentTotals = normalizeOpencodeTokens(msg?.tokens);
    if (!currentTotals) {
      messagesProcessed += 1;
      continue;
    }

    // A fork copy of an already-counted turn contributes nothing. Existing
    // pre-#426 cursor entries were already added to hourly/project buckets;
    // retract those once, then persist a tombstone so a later sync is a no-op.
    const fingerprint = deriveOpencodeMessageFingerprint({
      msg,
      totals: currentTotals,
      source: defaultSource,
    });
    if (isOpencodeForkCopy(fingerprintIndex, fingerprint, messageKey)) {
      let projectContext = null;
      if (lastTotals && prev?.dedupedForkCopy !== true && projectEnabled) {
        projectContext = await resolveProjectContextForDb({
          msg,
          dbPath,
          projectMetaCache,
          publicRepoCache,
          publicRepoResolver,
          projectState,
        });
      }
      if (lastTotals && prev?.dedupedForkCopy !== true) {
        repairCountedOpencodeForkCopy({
          msg,
          attribution: prev?.attribution,
          totals: lastTotals,
          source: defaultSource,
          hourlyState,
          touchedBuckets,
          projectState,
          projectTouchedBuckets,
          projectRef: projectContext?.projectRef || null,
          projectKey: projectContext?.projectKey || null,
        });
      }
      recordOpencodeMessage({
        messageIndex,
        fingerprintIndex,
        messageKey,
        totals: currentTotals,
        fingerprint,
        dedupedForkCopy: true,
      });
      messagesProcessed += 1;
      continue;
    }

    const effectiveLastTotals = prev?.dedupedForkCopy === true ? null : lastTotals;
    const delta = diffOpencodeTotals(currentTotals, effectiveLastTotals);
    const timestampMs = coerceEpochMs(msg?.time?.completed) || coerceEpochMs(msg?.time?.created);
    if (!timestampMs) {
      messagesProcessed += 1;
      continue;
    }

    const tsIso = new Date(timestampMs).toISOString();
    const bucketStart = toUtcHalfHourStart(tsIso);
    if (!bucketStart) {
      messagesProcessed += 1;
      continue;
    }

    const { modelId: dbModelId } = normalizeOpencodeModelFields(msg);
    const model = dbModelId || DEFAULT_MODEL;
    const projectContext = projectEnabled
      ? await resolveProjectContextForDb({
          msg,
          dbPath,
          projectMetaCache,
          publicRepoCache,
          publicRepoResolver,
          projectState,
        })
      : null;
    const attribution = {
      bucketStart,
      model,
      projectKey: projectContext?.projectKey || null,
      projectRef: projectContext?.projectRef || null,
    };

    const previousAttribution = effectiveLastTotals
      ? normalizeOpencodeAttribution(prev?.attribution)
      : null;
    const moved = Boolean(
      effectiveLastTotals &&
      previousAttribution &&
      !sameOpencodeAttribution(previousAttribution, attribution),
    );
    if ((!delta || isAllZeroUsage(delta)) && !moved) {
      // Refresh the index even without a delta: normalization may have changed,
      // and pre-#426 entries need their fingerprint backfilled. Preserve an
      // existing contribution location without bloating every legacy cursor.
      recordOpencodeMessage({
        messageIndex,
        fingerprintIndex,
        messageKey,
        totals: currentTotals,
        fingerprint,
        attribution: prev?.attribution ? attribution : null,
        dedupedForkCopy: false,
      });
      messagesProcessed += 1;
      if (cb) {
        cb({
          index: idx + 1,
          total: totalMessages,
          messagesProcessed,
          eventsAggregated,
          bucketsQueued: touchedBuckets.size,
        });
      }
      continue;
    }

    if (moved) {
      subtractCountedOpencodeMessage({
        attribution: previousAttribution,
        totals: effectiveLastTotals,
        source: defaultSource,
        hourlyState,
        touchedBuckets,
        projectState,
        projectTouchedBuckets,
      });
    }
    const contribution = moved
      ? { ...currentTotals, conversation_count: 1 }
      : { ...delta, conversation_count: effectiveLastTotals ? 0 : 1 };
    const bucket = getHourlyBucket(hourlyState, defaultSource, model, bucketStart);
    addTotals(bucket.totals, contribution);
    touchedBuckets.add(bucketKey(defaultSource, model, bucketStart));

    if (attribution.projectKey && projectState && projectTouchedBuckets) {
      const projectBucket = getProjectBucket(
        projectState,
        attribution.projectKey,
        defaultSource,
        bucketStart,
        attribution.projectRef,
      );
      addTotals(projectBucket.totals, contribution);
      projectTouchedBuckets.add(
        projectBucketKey(attribution.projectKey, defaultSource, bucketStart),
      );
    }

    recordOpencodeMessage({
      messageIndex,
      fingerprintIndex,
      messageKey,
      totals: currentTotals,
      fingerprint,
      attribution,
      dedupedForkCopy: false,
    });
    messagesProcessed += 1;
    if (delta && !isAllZeroUsage(delta)) eventsAggregated += 1;

    if (cb) {
      cb({
        index: idx + 1,
        total: totalMessages,
        messagesProcessed,
        eventsAggregated,
        bucketsQueued: touchedBuckets.size,
      });
    }
  }

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const projectBucketsQueued = projectEnabled
    ? await enqueueTouchedProjectBuckets({ projectQueuePath, projectState, projectTouchedBuckets })
    : 0;
  hourlyState.updatedAt = new Date().toISOString();
  cursors.hourly = hourlyState;
  if (dbCursor && typeof dbCursor === "object") opencodeState.dbCursor = dbCursor;
  opencodeState.updatedAt = new Date().toISOString();
  cursors[cursorNamespace] = opencodeState;
  if (projectState) {
    projectState.updatedAt = new Date().toISOString();
    cursors.projectHourly = projectState;
  }

  return { messagesProcessed, eventsAggregated, bucketsQueued, projectBucketsQueued };
}

// Resolve project context from DB message (no file path available)
async function resolveProjectContextForDb({
  msg,
  dbPath,
  projectMetaCache,
  publicRepoCache,
  publicRepoResolver,
  projectState,
}) {
  const cwd = msg?.path?.cwd;
  if (!cwd || typeof cwd !== "string") return null;
  return resolveProjectContextForPath({
    // The DB itself is read over the distro's UNC bridge on Windows, so its
    // path carries the prefix the recorded POSIX cwd needs (#374).
    startDir: wsl.mapWslCwdToUnc(cwd, dbPath),
    projectMetaCache,
    publicRepoCache,
    publicRepoResolver,
    projectState,
  });
}

module.exports = {
  listOpencodeMessageFiles,
  parseOpencodeIncremental,
  deriveOpencodeMessageFingerprint,
  normalizeOpencodeModelFields,
  normalizeOpencodeTokens,
  readOpencodeDbMessagesIncremental,
  readOpencodeDbMessages,
  readMimoDbMessages,
  hasZcodeNativeUsageSchema,
  zcodeDbHasContent,
  hasZcodeCursorState,
  resolveZcodeInstallPaths,
  readZcodeDbMessages,
  parseOpencodeDbIncremental,
};
