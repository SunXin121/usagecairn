// command-code usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fs = require("node:fs/promises");
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { isDeepStrictEqual } = require("node:util");
const { ensureDir } = require("../fs");
const wsl = require("../wsl-probe");
const {
  DEFAULT_MODEL,
  enqueueTouchedBuckets,
  enqueueTouchedProjectBuckets,
  normalizeHourlyState,
  normalizeProjectState,
  getHourlyBucket,
  getProjectBucket,
  addTotals,
  subtractTotals,
  totalsKey,
  toUtcHalfHourStart,
  bucketKey,
  projectBucketKey,
  resolveProjectContextForPath,
  toNonNegativeInt,
  findDshJsonProperty,
  parseDshJsonString,
} = require("./core");

// ── Command Code (`cmd`, commandcode.ai) — passive session-log reader (issue #630) ──
//
// Command Code keeps one JSONL transcript per conversation under
// `~/.commandcode/projects/<cwd-slug>/<session-id>.jsonl`. The first line is a
// session header carrying the launch `cwd`; every completed assistant turn
// appends a record whose TOP-LEVEL `model` and `usage` are Command Code's own
// accounting for that request:
//
//   {"type":"message","id":"…","parentId":"…","timestamp":"…","effort":"…",
//    "model":"deepseek/deepseek-v4.1-flash",
//    "usage":{"inputTokens":…,"outputTokens":…,"cacheReadTokens":…,
//             "cacheWriteTokens":…,"costUsd":…},"message":{…}}
//
// Two accounting conventions are load-bearing:
//
//  1. AI SDK-normalized `inputTokens` ALREADY INCLUDES cache reads and writes.
//     `uncached = inputTokens - cacheReadTokens - cacheWriteTokens`; keeping
//     either cache category in input double counts it in `total_tokens`.
//  2. `costUsd` is Command Code's display-rate estimate, not a billed amount.
//     Ignore it and emit the zero cost sentinel; local readers and cloud
//     endpoints estimate cost from the shared model price table.
//
// Transcripts are append-only in practice, but a resume/compaction REWRITES the
// file, so byte offsets are the wrong cursor shape here. This reader rebuilds a
// per-file snapshot and reconciles it against a durable observation ledger keyed
// by `recordId|timestamp`. Forks and clones copy those fields into transcripts
// with new session headers, so the header cannot be part of the key. Files whose
// (size, mtime) pair is unchanged can reuse their owned ledger records. A
// non-owning duplicate is re-read rather than storing a second full ledger.
const COMMAND_CODE_SOURCE = "command-code";
const COMMAND_CODE_STATE_VERSION = 3;
const COMMAND_CODE_FILE_CACHE_VERSION = 3;
const COMMAND_CODE_HEADER_MAX_BYTES = 65536;
const COMMAND_CODE_HOME_DIR = ".commandcode";
const COMMAND_CODE_PROJECTS_DIR = "projects";

function isCommandCodeSessionLogName(name) {
  return (
    typeof name === "string" &&
    name.endsWith(".jsonl") &&
    !name.includes(".prompts.") &&
    !name.endsWith(".checkpoints.jsonl")
  );
}

// Precedence mirrors the other passive readers: an explicit UsageCairn
// override first, then the CLI's own `~/.commandcode`.
function resolveCommandCodeHome(env = process.env) {
  const explicit = env?.TOKEN_PULSE_COMMANDCODE_HOME;
  if (typeof explicit === "string" && explicit.trim()) return path.resolve(explicit.trim());
  return path.join(os.homedir(), COMMAND_CODE_HOME_DIR);
}

// Windows users commonly run the `cmd` CLI inside WSL while UsageCairn itself
// runs natively. Respect the repository-wide WSL mode contract and keep explicit
// home overrides authoritative: an override is a complete user choice, not one
// half of an automatic native/WSL discovery pair.
function resolveCommandCodeHomes(env = process.env, deps = {}) {
  const override = env?.TOKEN_PULSE_COMMANDCODE_HOME;
  const overridden = typeof override === "string" && override.trim().length > 0;
  const nativeHome = deps.nativeHome || resolveCommandCodeHome(env);
  const platform = deps.platform || process.platform;
  if (overridden || platform !== "win32") return [nativeHome];

  const probePath = deps.existsSync || ((candidate) => fssync.statSync(candidate).isDirectory());
  const existsSync = (candidate) => {
    try { return probePath(candidate); }
    catch (error) {
      if (isCommandCodePathMissing(error)) return false;
      throw error;
    }
  };
  // Native errors remain visible. WSL is optional and must not suppress native
  // observations when its executable, identity or UNC existence probes fail.
  const nativeValue = wsl.shouldProbeNative(env) && existsSync(nativeHome) ? nativeHome : null;
  const discoverWslHome = deps.discoverWslHome || wsl.discoverWslHome;
  let wslValue = null;
  if (wsl.shouldProbeWsl(env)) {
    try {
      wslValue = discoverWslHome(COMMAND_CODE_HOME_DIR, { ...deps, env, existsSync, strict: true });
    } catch (_error) { }
  }
  const resolved = wsl.resolveAllWin32Paths({
    nativeValue,
    wslValue,
    env,
    platform,
  });
  return [...new Set([resolved.native, resolved.wsl].filter(Boolean))];
}

// Walk `<home>/projects/<cwd-slug>/` for `<session-id>.jsonl` transcripts. The
// sibling checkpoint snapshots and `.prompts.` sidecars are not transcripts
// and must not inflate discovery or status session counts.
async function resolveCommandCodeSessionFiles(env = process.env, deps = {}) {
  const out = new Set();
  const homes = resolveCommandCodeHomes(env, deps);
  const nativeHome = deps.nativeHome || resolveCommandCodeHome(env);
  const overridden = typeof env?.TOKEN_PULSE_COMMANDCODE_HOME === "string" && env.TOKEN_PULSE_COMMANDCODE_HOME.trim();
  for (const home of homes) {
    const rootFiles = [];
    try {
      const projectsRoot = path.join(home, COMMAND_CODE_PROJECTS_DIR);
      for (const project of await readCommandCodeDirectory(projectsRoot)) {
        if (!project.isDirectory()) continue;
        const projectDir = path.join(projectsRoot, project.name);
        for (const entry of await readCommandCodeDirectory(projectDir)) {
          if (!entry.isFile() || !isCommandCodeSessionLogName(entry.name)) continue;
          rootFiles.push(path.join(projectDir, entry.name));
        }
      }
    } catch (error) {
      const optionalWsl = !overridden && (deps.platform || process.platform) === "win32" && home !== nativeHome;
      if (!optionalWsl) throw error;
      // A selected wsl-first root may become inaccessible after discovery.
      // Fall back to native without changing shared WSL callers' mode rules.
      if (wsl.shouldProbeNative(env) && !homes.includes(nativeHome)) homes.push(nativeHome);
      continue;
    }
    for (const file of rootFiles) out.add(file);
  }
  return [...out].sort((a, b) => a.localeCompare(b));
}

function isCommandCodePathMissing(error) {
  return error?.code === "ENOENT" || error?.code === "ENOTDIR" || error?.code === "EISDIR";
}

async function readCommandCodeDirectory(directory) {
  try {
    return await fs.readdir(directory, { withFileTypes: true });
  } catch (error) {
    if (isCommandCodePathMissing(error)) return [];
    // Native errors stay visible; optional WSL errors are isolated by the caller.
    throw error;
  }
}

// Row models are provider-qualified ("deepseek/deepseek-v4.1-flash"); the
// pricing tables and bucket keys use the bare id, matching the dsh reader.
function normalizeCommandCodeModelName(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const slash = trimmed.lastIndexOf("/");
  const name = slash >= 0 ? trimmed.slice(slash + 1) : trimmed;
  return name || null;
}

// Map Command Code's usage object onto disjoint queue columns. `inputTokens`
// already includes cache reads and writes (see the section comment), so subtract
// both back out first. Explicit numeric zero usage can correct an existing
// observation; empty placeholders are not usage. Cost uses model-table pricing.
function commandCodeUsageToTotals(usage) {
  if (!usage || typeof usage !== "object") return null;
  const inclusiveInput = toNonNegativeInt(usage.inputTokens);
  const cachedInput = toNonNegativeInt(usage.cacheReadTokens);
  const cacheWrite = toNonNegativeInt(usage.cacheWriteTokens);
  const output = toNonNegativeInt(usage.outputTokens);
  const input = Math.max(0, inclusiveInput - cachedInput - cacheWrite);
  const total = input + cachedInput + cacheWrite + output;
  const explicitZero = ["inputTokens", "outputTokens"].every((field) =>
    typeof usage[field] === "number" && Number.isFinite(usage[field]) && usage[field] === 0,
  ) && ["cacheReadTokens", "cacheWriteTokens"].every((field) =>
    usage[field] === undefined || (typeof usage[field] === "number" && usage[field] === 0),
  );
  if (total === 0 && !explicitZero) return null;
  return {
    input_tokens: input,
    cached_input_tokens: cachedInput,
    cache_creation_input_tokens: cacheWrite,
    output_tokens: output,
    reasoning_output_tokens: 0,
    total_tokens: total,
    billable_total_tokens: total,
    total_cost_usd: 0,
    conversation_count: total > 0 ? 1 : 0,
  };
}

// Check record framing without JSON-decoding its body. Finding an early usage
// field is not proof that an interrupted append completed the enclosing object.
function isCompleteCommandCodeRecord(raw) {
  const text = raw.trim();
  if (text[0] !== "{") return false;
  const closers = [];
  let inString = false;
  for (let index = 0; index < text.length; index++) {
    const char = text[index];
    if (inString) {
      if (char === "\\") index += 1;
      else if (char === '"') inString = false;
      continue;
    }
    if (char === '"') inString = true;
    else if (char === "{") closers.push("}");
    else if (char === "[") closers.push("]");
    else if (char === "}" || char === "]") {
      if (char !== closers.pop()) return false;
      if (closers.length === 0) return index === text.length - 1;
    }
  }
  return false;
}

// Extract selected top-level metadata from transcript text scanned locally.
// findDshJsonProperty slices the needed fields; JSON decoding is limited to
// selected metadata rather than whole message records or their bodies.
// Raw transcript text is present in memory while scanning; prompt, reply and
// code bodies are not persisted or uploaded by this reader. The parser test's
// JSON.parse guard checks that body content is not JSON-decoded.
function extractCommandCodeLine(line) {
  const raw = String(line || "");
  if (!isCompleteCommandCodeRecord(raw)) return null;
  const type = parseDshJsonString(findDshJsonProperty(raw, "type"));
  if (type === "session") {
    return {
      kind: "session",
      sessionId: parseDshJsonString(findDshJsonProperty(raw, "id")),
      cwd: parseDshJsonString(findDshJsonProperty(raw, "cwd")),
    };
  }
  if (type !== "message") return null;
  const id = parseDshJsonString(findDshJsonProperty(raw, "id"));
  const timestamp = parseDshJsonString(findDshJsonProperty(raw, "timestamp"));
  if (!id || !timestamp) return null;
  const usageRaw = findDshJsonProperty(raw, "usage");
  if (!usageRaw) return null;
  let usage = null;
  try {
    usage = JSON.parse(usageRaw);
  } catch (_error) {
    return null;
  }
  const totals = commandCodeUsageToTotals(usage);
  if (!totals) return null;
  const bucketStart = toUtcHalfHourStart(timestamp);
  if (!bucketStart) return null;
  const model = normalizeCommandCodeModelName(
    parseDshJsonString(findDshJsonProperty(raw, "model")),
  );
  return {
    kind: "message",
    id,
    timestamp,
    model: model || DEFAULT_MODEL,
    totals,
    bucketStart,
  };
}

// Parse one transcript. A torn tail simply contributes nothing — the rebuild
// reconciliation picks it up on the next sync once the record is complete.
function extractCommandCodeSessionUsage(text) {
  const records = [];
  const headerRanges = [];
  let sessionId = null;
  let cwd = null;
  let byteOffset = 0;
  for (const line of String(text || "").split("\n")) {
    const length = Buffer.byteLength(line);
    const start = byteOffset;
    byteOffset += length + 1;
    const parsed = extractCommandCodeLine(line);
    if (!parsed) continue;
    if (parsed.kind === "session") {
      const headerLength = Buffer.byteLength(line.trimStart());
      headerRanges.push({ start: start + length - headerLength, length: headerLength });
      if (!sessionId && parsed.sessionId) sessionId = parsed.sessionId;
      if (!cwd && parsed.cwd) cwd = parsed.cwd;
      continue;
    }
    records.push(parsed);
  }
  return { sessionId, cwd, records, headerRanges };
}

// Four independent counters are sufficient for current observations. Keep
// legacy totals verbatim until a surviving session/id proves their replacement.
function compactCommandCodeMessage(value, legacy = false) {
  const totals = legacy ? { ...value.totals } : {
    input_tokens: value.totals.input_tokens || 0,
    cached_input_tokens: value.totals.cached_input_tokens || 0,
    cache_creation_input_tokens: value.totals.cache_creation_input_tokens || 0,
    output_tokens: value.totals.output_tokens || 0,
  };
  return {
    totals, bucketStart: value.bucketStart, model: value.model,
    projectKey: value.projectKey || null, projectRef: value.projectRef || null,
    ...(legacy ? { legacy: true } : {}),
  };
}

function expandCommandCodeTotals(totals) {
  const total = totals.input_tokens + totals.cached_input_tokens + totals.cache_creation_input_tokens + totals.output_tokens;
  return {
    ...totals,
    reasoning_output_tokens: totals.reasoning_output_tokens || 0,
    total_tokens: totals.total_tokens ?? total,
    billable_total_tokens: totals.billable_total_tokens ?? totals.total_tokens ?? total,
    total_cost_usd: 0,
    conversation_count: totals.conversation_count ?? (total > 0 ? 1 : 0),
  };
}

function normalizeCommandCodeState(raw) {
  const messages = {};
  if (raw?.messages && typeof raw.messages === "object") {
    for (const [key, value] of Object.entries(raw.messages)) {
      if (!value?.totals || !value.bucketStart || !value.model) continue;
      messages[key] = compactCommandCodeMessage(value, !raw.version || raw.version < 2 || value.legacy === true);
    }
  }
  const files = {};
  // v2 already uses exact id|timestamp accounting. Legacy session/id caches
  // must be reread once, but absent legacy ledger rows are never discarded.
  if (raw?.version >= 2 && raw.files && typeof raw.files === "object") {
    for (const [key, value] of Object.entries(raw.files)) {
      if (!value || typeof value !== "object") continue;
      const size = Number(value.size);
      const mtimeMs = Number(value.mtimeMs);
      if (!Number.isFinite(size) || !Number.isFinite(mtimeMs)) continue;
      files[key] = { size, mtimeMs };
    }
  }
  const fileIndex = {};
  // Older caches omitted uncounted zero identities and accepted torn records.
  // Re-read them once while retaining the durable ledger above.
  if (raw?.fileCacheVersion === COMMAND_CODE_FILE_CACHE_VERSION && raw.fileIndex) {
    for (const [key, value] of Object.entries(raw.fileIndex)) {
      if (!files[key] || !Array.isArray(value?.messageKeys) || !Array.isArray(value.headerRanges)) continue;
      if (value.messageKeys.some((entry) => typeof entry !== "string")) continue;
      if (value.zeroKeys && (!Array.isArray(value.zeroKeys) || value.zeroKeys.some((entry) => typeof entry !== "string"))) continue;
      let end = 0;
      const validRanges = value.headerRanges.every((range) => {
        if (!Number.isSafeInteger(range?.start) || !Number.isSafeInteger(range.length)) return false;
        if (range.start < end || range.length < 0 || range.start + range.length > files[key].size) return false;
        end = range.start + range.length;
        return true;
      });
      if (!validRanges) continue;
      fileIndex[key] = {
        messageKeys: [...new Set(value.messageKeys)],
        headerRanges: value.headerRanges.map(({ start, length }) => ({ start, length })),
        completeOwnedSnapshot: value.completeOwnedSnapshot === true,
        ...(value.zeroKeys?.length ? { zeroKeys: [...new Set(value.zeroKeys)] } : {}),
      };
    }
  }
  return { version: COMMAND_CODE_STATE_VERSION, fileCacheVersion: COMMAND_CODE_FILE_CACHE_VERSION, messages, files, fileIndex };
}

// Snapshot one transcript through a single descriptor: the change check and the
// read share one handle, so a writer cannot swap the file between them (the
// TOCTOU shape CodeQL reports as js/file-system-race). Returns null for a
// missing or non-file path while preserving durable history. Other observation
// failures propagate before any queues or cursors are published.
async function readCommandCodeSessionSnapshot(filePath, previous = null, headerRanges = null) {
  let handle;
  try {
    handle = await fs.open(filePath, "r");
    let stat = await handle.stat();
    if (!stat.isFile()) return null;
    const metadata = { size: stat.size, mtimeMs: stat.mtimeMs };
    if (
      previous &&
      previous.size === metadata.size &&
      previous.mtimeMs === metadata.mtimeMs &&
      (!headerRanges || headerRanges.reduce((sum, range) => sum + range.length, 0) <= COMMAND_CODE_HEADER_MAX_BYTES)
    ) {
      if (!headerRanges) return { ...metadata, unchanged: true, text: null };
      // Store byte ranges, never cwd. Positional reads through this same handle
      // refresh only header lines, preserving Unicode and leading whitespace
      // without materializing any of the cached message bodies.
      const headers = [];
      let complete = true;
      for (const { start, length } of headerRanges) {
        const data = Buffer.alloc(length);
        let offset = 0;
        while (offset < length) {
          const { bytesRead } = await handle.read(data, offset, length - offset, start + offset);
          if (!bytesRead) { complete = false; break; }
          offset += bytesRead;
        }
        if (!complete) break;
        headers.push(data.toString("utf8"));
      }
      const finalStat = await handle.stat();
      if (complete && finalStat.size === stat.size && finalStat.mtimeMs === stat.mtimeMs) {
        return { ...metadata, unchanged: true, text: headers.join("\n") };
      }
      // The file changed during the header read. Rebuild through this handle;
      // positional reads above did not advance its full-read file position.
      stat = finalStat;
    }
    const data = await handle.readFile();
    const finalStat = await handle.stat();
    const finalMetadata = { size: finalStat.size, mtimeMs: finalStat.mtimeMs };
    // Appends and equal-size rewrites during a full read both need a retry:
    // old bytes must not be acknowledged under the rewritten file's metadata.
    if (
      finalMetadata.size !== data.length ||
      finalMetadata.size !== stat.size ||
      finalMetadata.mtimeMs !== stat.mtimeMs
    ) finalMetadata.mtimeMs = -1;
    return { ...finalMetadata, unchanged: false, text: data.toString("utf8") };
  } catch (error) {
    if (isCommandCodePathMissing(error)) return null;
    throw error;
  } finally {
    if (handle) await handle.close().catch(() => {});
  }
}

// Rebuild-and-diff sync for `~/.commandcode/projects/**/*.jsonl`. A record's
// identity is `recordId|timestamp`, so a rewritten or forked transcript
// reconciles instead of double counting. Missing records retain their history;
// only surviving observations can correct it. Cursor state is committed after
// both queue appends succeed, so a failed write retries without losing or
// inflating usage.
async function parseCommandCodeIncremental({
  sessionFiles,
  cursors,
  queuePath,
  projectQueuePath,
  onProgress,
  publicRepoResolver,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const files = Array.isArray(sessionFiles) ? sessionFiles : [];
  // Normalizers retain nested bucket objects. Isolate this provider's working
  // state so an append failure cannot publish totals or queuedKey changes
  // through the original cursors before both queues have succeeded.
  const hourlyState = normalizeHourlyState(structuredClone(cursors?.hourly));
  const state = normalizeCommandCodeState(cursors?.commandCode);
  const schemaMigrated = Boolean(cursors?.commandCode &&
    (cursors.commandCode.version !== COMMAND_CODE_STATE_VERSION ||
      cursors.commandCode.fileCacheVersion !== COMMAND_CODE_FILE_CACHE_VERSION));
  const projectEnabled = typeof projectQueuePath === "string" && projectQueuePath.length > 0;
  const projectState = projectEnabled
    ? normalizeProjectState(structuredClone(cursors?.projectHourly))
    : null;
  const projectTouchedBuckets = projectEnabled ? new Set() : null;
  const projectMetaCache = projectEnabled ? new Map() : null;
  const publicRepoCache = projectEnabled ? new Map() : null;
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;

  const currentByKey = new Map();
  const currentOwners = new Map();
  const observedLegacyKeys = new Set();
  const nextFiles = {};
  const nextFileIndex = {};
  let recordsProcessed = 0;

  for (let fileIdx = 0; fileIdx < files.length; fileIdx++) {
    const filePath = files[fileIdx];
    const index = state.fileIndex[filePath];
    const ownsSnapshot = index?.completeOwnedSnapshot &&
      index.messageKeys.every((key) => state.messages[key] && !state.messages[key].legacy) &&
      // A previously uncounted zero can become a correction when another copy
      // introduces the same identity. Re-read its actual model and counters.
      !(index.zeroKeys || []).some((key) => state.messages[key] || currentByKey.has(key));
    const snapshot = await readCommandCodeSessionSnapshot(
      filePath,
      ownsSnapshot ? state.files[filePath] : null,
      projectEnabled && ownsSnapshot ? index.headerRanges : null,
    );
    if (!snapshot) continue;
    nextFiles[filePath] = { size: snapshot.size, mtimeMs: snapshot.mtimeMs };

    const parsed = extractCommandCodeSessionUsage(snapshot.text);

    let projectKey = null;
    let projectRef = null;
    if (projectEnabled) {
      const rawCwd = typeof parsed.cwd === "string" ? parsed.cwd.trim() : "";
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

    if (snapshot.unchanged) {
      nextFileIndex[filePath] = { ...index };
      for (const key of index.messageKeys) {
        const value = state.messages[key];
        currentByKey.set(key, projectEnabled ? { ...value, projectKey, projectRef } : value);
        currentOwners.set(key, filePath);
      }
      continue;
    }

    const messageKeys = new Set();
    for (const record of parsed.records) {
      recordsProcessed += 1;
      const key = `${COMMAND_CODE_SOURCE}:${record.id}|${record.timestamp}`;
      messageKeys.add(key);
      if (parsed.sessionId) observedLegacyKeys.add(`${COMMAND_CODE_SOURCE}:${parsed.sessionId}|${record.id}`);
      currentOwners.set(key, filePath);
      currentByKey.set(key, compactCommandCodeMessage({
        totals: record.totals,
        bucketStart: record.bucketStart,
        model: record.model,
        projectKey,
        projectRef,
      }));
    }
    nextFileIndex[filePath] = { messageKeys: [...messageKeys], headerRanges: parsed.headerRanges };

    if (cb && (fileIdx % 25 === 0 || fileIdx === files.length - 1)) {
      cb({
        index: fileIdx + 1,
        total: files.length,
        messagesProcessed: currentByKey.size,
        eventsAggregated: 0,
        bucketsQueued: 0,
      });
    }
  }

  let eventsAggregated = 0;

  function applyObservation(value, subtract = false) {
    const totals = expandCommandCodeTotals(value.totals);
    const apply = subtract ? subtractTotals : addTotals;
    apply(getHourlyBucket(hourlyState, COMMAND_CODE_SOURCE, value.model, value.bucketStart).totals, totals);
    touchedBuckets.add(bucketKey(COMMAND_CODE_SOURCE, value.model, value.bucketStart));
    if (projectEnabled && value.projectKey) {
      apply(getProjectBucket(projectState, value.projectKey, COMMAND_CODE_SOURCE, value.bucketStart, value.projectRef).totals, totals);
      projectTouchedBuckets.add(projectBucketKey(value.projectKey, COMMAND_CODE_SOURCE, value.bucketStart));
    }
  }

  // Legacy session-based identities can only be removed when that session/id
  // actually survives. An empty or partially compacted source proves nothing.
  for (const [key, prev] of Object.entries(state.messages)) {
    if (!prev.legacy || !observedLegacyKeys.has(key)) continue;
    applyObservation(prev, true);
    delete state.messages[key];
    eventsAggregated += 1;
  }

  for (const [key, prev] of Object.entries(state.messages)) {
    if (!currentByKey.has(key)) continue;
    const cur = currentByKey.get(key);
    const unchanged =
      totalsKey(expandCommandCodeTotals(prev.totals)) === totalsKey(expandCommandCodeTotals(cur.totals)) &&
      prev.bucketStart === cur.bucketStart && prev.model === cur.model &&
      prev.projectKey === cur.projectKey;
    if (!unchanged) {
      applyObservation(prev, true);
      applyObservation(cur);
      eventsAggregated += 1;
    }
    state.messages[key] = cur;
  }

  for (const [key, cur] of currentByKey) {
    if (state.messages[key]) continue;
    // A standalone zero is an empty turn, not a new accounting event. Retain
    // zeros only as corrections to a previously counted exact-key record.
    if (expandCommandCodeTotals(cur.totals).total_tokens === 0) continue;
    applyObservation(cur);
    state.messages[key] = cur;
    eventsAggregated += 1;
  }

  for (const [filePath, index] of Object.entries(nextFileIndex)) {
    // Check ownership before dropping uncounted keys: a positive record that
    // lost to a zero in another file is still a non-owning snapshot.
    const ownsAllRecords = index.messageKeys.every((key) => currentOwners.get(key) === filePath);
    index.completeOwnedSnapshot = ownsAllRecords;
    // Retain the identities of uncounted zeros without full message metadata.
    // They need no body reread until a conflicting observation appears.
    const zeroKeys = [...new Set([
      ...(index.zeroKeys || []),
      ...index.messageKeys.filter((key) => !state.messages[key]),
    ])];
    if (zeroKeys.length) index.zeroKeys = zeroKeys;
    index.messageKeys = index.messageKeys.filter((key) => state.messages[key]);
  }

  // Ignore cost-only changes (including old display estimates). The zero
  // sentinel is written only when actual accounting otherwise queues a bucket.
  function prepareBuckets(touched, next, previous) {
    for (const key of touched) {
      const totals = next.buckets[key].totals;
      const old = previous?.buckets?.[key]?.totals;
      if (old && totalsKey({ ...old, total_cost_usd: 0 }) === totalsKey({ ...totals, total_cost_usd: 0 })) {
        touched.delete(key);
        continue;
      }
      totals.total_cost_usd = 0;
    }
  }
  prepareBuckets(touchedBuckets, hourlyState, cursors?.hourly);
  if (projectEnabled) prepareBuckets(projectTouchedBuckets, projectState, cursors?.projectHourly);

  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const projectBucketsQueued = projectEnabled
    ? await enqueueTouchedProjectBuckets({ projectQueuePath, projectState, projectTouchedBuckets })
    : 0;

  const updatedAt = new Date().toISOString();
  if (bucketsQueued > 0) hourlyState.updatedAt = updatedAt;
  state.files = nextFiles;
  state.fileIndex = nextFileIndex;
  // Non-owning fork copies are reread to resolve conflicts, but those reads
  // alone need not rewrite an identical persistent ledger and file index.
  const cursorUnchanged = isDeepStrictEqual(state, cursors.commandCode);
  cursors.hourly = hourlyState;
  cursors.commandCode = state;
  if (projectState) {
    if (projectBucketsQueued > 0) projectState.updatedAt = updatedAt;
    cursors.projectHourly = projectState;
  }

  return {
    recordsProcessed,
    messagesProcessed: currentByKey.size,
    eventsAggregated,
    bucketsQueued,
    projectBucketsQueued,
    schemaMigrated,
    cursorUnchanged,
  };
}

module.exports = {
  isCommandCodeSessionLogName,
  resolveCommandCodeHome,
  resolveCommandCodeHomes,
  resolveCommandCodeSessionFiles,
  normalizeCommandCodeModelName,
  commandCodeUsageToTotals,
  extractCommandCodeSessionUsage,
  parseCommandCodeIncremental,
};
