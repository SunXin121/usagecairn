// omp usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { ensureDir } = require("../fs");
const wsl = require("../wsl-probe");
const {
  enqueueTouchedBuckets,
  enqueueTouchedProjectBuckets,
  normalizeHourlyState,
  normalizeProjectState,
  getHourlyBucket,
  getProjectBucket,
  addTotals,
  toUtcHalfHourStart,
  bucketKey,
  projectBucketKey,
  normalizeModelInput,
  resolveProjectContextForPath,
  toNonNegativeInt,
  pickWin32ProviderPath,
  expandHomePath,
  resolveOmpAgentDir,
  readPiStyleSessionCwd,
  resolveOmpFileCwd,
  OMP_LIKE_MAX_RECORD_BYTES,
  scanOmpLikeTranscript,
} = require("./core");

// Session-file discovery is shared with OmO (omo), which persists the exact
// same on-disk layout under its own agent dir:
//   <agentDir>/sessions/--<cwd-encoded>--/<timestamp>_<sessionId>.jsonl
function collectPiStyleSessionFiles(agentDir) {
  if (!agentDir) return [];
  const sessionsDir = path.join(agentDir, "sessions");
  if (!fssync.existsSync(sessionsDir)) return [];
  const files = [];
  try {
    for (const cwdDir of fssync.readdirSync(sessionsDir)) {
      const cwdPath = path.join(sessionsDir, cwdDir);
      let stat;
      try { stat = fssync.statSync(cwdPath); } catch { continue; }
      if (!stat.isDirectory()) continue;
      let entries;
      try { entries = fssync.readdirSync(cwdPath); } catch { continue; }
      for (const entry of entries) {
        if (!entry.endsWith(".jsonl")) continue;
        files.push(path.join(cwdPath, entry));
      }
    }
  } catch {
    // ignore — return what we have
  }
  files.sort((a, b) => a.localeCompare(b));
  return files;
}

function resolveOmpSessionFiles(env = process.env) {
  return collectPiStyleSessionFiles(resolveOmpAgentDir(env));
}

// Subagent transcripts live in a directory named after the session file:
//   ~/.omp/agent/sessions/<cwd>/<session>.jsonl            (main agent)
//   ~/.omp/agent/sessions/<cwd>/<session>/<Agent>.jsonl    (task subagent)
//   ~/.omp/agent/sessions/<cwd>/<session>/<sub>/<A>.jsonl  (nested advisor)
// oh-my-pi's own stats indexer classifies by depth (packages/stats/parser.ts:
// rel path <= 2 segments → main, deeper → subagent/advisor), so we mirror
// that: everything below the cwd level is subagent traffic. Session dirs also
// hold non-JSONL artefacts (*.bash-original.log, *.md) — skipped by extension.
// OmO nests the same way, so it reuses this walker.
function collectPiStyleSubagentFiles(agentDir) {
  if (!agentDir) return [];
  const sessionsDir = path.join(agentDir, "sessions");
  if (!fssync.existsSync(sessionsDir)) return [];
  const files = [];
  const walk = (dir) => {
    let entries;
    try { entries = fssync.readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const entry of entries) {
      const full = path.join(dir, entry.name);
      let isDir = entry.isDirectory();
      let isFile = entry.isFile();
      // Resolve symlinks defensively (Dirent flags are false for symlinks).
      if (!isDir && !isFile) {
        try {
          const st = fssync.statSync(full);
          isDir = st.isDirectory();
          isFile = st.isFile();
        } catch { continue; }
      }
      if (isDir) walk(full);
      else if (isFile && entry.name.endsWith(".jsonl")) files.push(full);
    }
  };
  try {
    for (const cwdDir of fssync.readdirSync(sessionsDir)) {
      const cwdPath = path.join(sessionsDir, cwdDir);
      let stat;
      try { stat = fssync.statSync(cwdPath); } catch { continue; }
      if (!stat.isDirectory()) continue;
      let entries;
      try { entries = fssync.readdirSync(cwdPath, { withFileTypes: true }); } catch { continue; }
      for (const entry of entries) {
        const full = path.join(cwdPath, entry.name);
        let isDir = entry.isDirectory();
        if (!isDir && !entry.isFile()) {
          try { isDir = fssync.statSync(full).isDirectory(); } catch { continue; }
        }
        if (isDir) walk(full);
      }
    }
  } catch {
    // ignore — return what we have
  }
  files.sort((a, b) => a.localeCompare(b));
  return files;
}

function resolveOmpSubagentFiles(env = process.env) {
  return collectPiStyleSubagentFiles(resolveOmpAgentDir(env));
}

function resolveOmpDefaultModel() {
  // oh-my-pi has no global default model setting; model is per-message.
  return "omp-unknown";
}

// ─────────────────────────────────────────────────────────────────────────────
// OmO (omo) — passive JSONL reader (~/.omo/agent/sessions/**/*.jsonl)
//
// OmO shares oh-my-pi's session persistence format, so it reuses the same
// collectors and parser. Layout:
//   ~/.omo/agent/sessions/--<cwd-encoded>--/<timestamp>_<sessionId>.jsonl
//
// First line is the type:"session" header (carries the real, unencoded cwd).
// Only type:"message" lines with message.role=="assistant" carry usage:
//
//   {
//     "type": "message",
//     "id": "7ae3734f",              ← 8-char dedup key
//     "timestamp": "2026-08-27T23:28:16.699Z",
//     "message": {
//       "role": "assistant",
//       "provider": "xai",
//       "model": "grok-4.6",
//       "usage": {
//         "input": 28646, "output": 630, "cacheRead": 512, "cacheWrite": 0,
//         "reasoning": 218, "totalTokens": 29788,
//         "cost": { ... }            ← OmO's own estimate; ignored, we price it
//       },
//       "timestamp": 1787873284870   ← ms epoch, preferred for bucketing
//     }
//   }
//
// Two deliberate differences from oh-my-pi:
//   1. The reasoning field is `reasoning`, not `reasoningTokens`.
//   2. Reasoning is a SUBSET of `output`, and `totalTokens` excludes it —
//      verified across a 2,586-message corpus where
//      input+output+cacheRead+cacheWrite === totalTokens for every row, and
//      `usage.cost` bills no separate reasoning component. So omo follows the
//      Codex convention: reasoning_output_tokens is informational and must not
//      be billed on top of output (see computeRowCost in lib/pricing/index.js).
//
// OmO is a router — the upstream model name is recorded per message and there
// is no global default (fallback: "omo-unknown").
//
// Path overrides are UsageCairn-only (TOKEN_PULSE_OMO_AGENT_DIR /
// TOKEN_PULSE_OMO_HOME / OMO_HOME). PI_CONFIG_DIR and PI_CODING_AGENT_DIR are
// deliberately NOT honored here: those belong to pi/omp, and routing them to a
// third provider would reintroduce the ambiguity decidePiCodingAgentDirOwner
// exists to resolve.
// ─────────────────────────────────────────────────────────────────────────────

function resolveOmoHome(env = process.env) {
  if (env.TOKEN_PULSE_OMO_HOME) return expandHomePath(env.TOKEN_PULSE_OMO_HOME, env);
  if (env.OMO_HOME) return expandHomePath(env.OMO_HOME, env);
  const home = env.HOME || os.homedir();
  if (process.platform === "win32") {
    return pickWin32ProviderPath({
      env,
      nativeValue: path.join(home, ".omo"),
      wslProviderDir: ".omo",
    });
  }
  return path.join(home, ".omo");
}

function resolveOmoAgentDir(env = process.env) {
  if (env.TOKEN_PULSE_OMO_AGENT_DIR) {
    return expandHomePath(env.TOKEN_PULSE_OMO_AGENT_DIR, env);
  }
  const omoHome = resolveOmoHome(env);
  return omoHome ? path.join(omoHome, "agent") : null;
}

function resolveOmoSessionFiles(env = process.env) {
  return collectPiStyleSessionFiles(resolveOmoAgentDir(env));
}

function resolveOmoSubagentFiles(env = process.env) {
  return collectPiStyleSubagentFiles(resolveOmoAgentDir(env));
}

function resolveOmoDefaultModel() {
  // OmO has no global default model setting; model is per-message.
  return "omo-unknown";
}

async function resolveOmoFileCwd(filePath) {
  return readPiStyleSessionCwd(filePath);
}

// Reads the first present numeric field. oh-my-pi emits `reasoningTokens`;
// OmO emits `reasoning`. Listing the accepted keys per provider keeps each
// one's accounting exact instead of guessing across both spellings.
function pickUsageInt(usage, fields) {
  for (const field of fields) {
    const raw = usage?.[field];
    if (raw == null) continue;
    const value = Number(raw);
    if (Number.isFinite(value)) return toNonNegativeInt(value);
  }
  return 0;
}

// Prefer the message-level timestamp (ms epoch), then the entry-level ISO
// string. Both must be real dates: a message stamped with a small placeholder
// number was accepted by the old `> 0` check and landed in the 1970-01-01
// bucket (seen in the cloud from three omp users).
const MIN_OMP_TIMESTAMP_MS = Date.UTC(2020, 0, 1);
function ompEntryTimestampMs(msg, entry) {
  const direct = Number(msg.timestamp);
  if (Number.isFinite(direct) && direct >= MIN_OMP_TIMESTAMP_MS) return direct;
  if (typeof entry.timestamp === "string" && entry.timestamp) {
    const parsed = Date.parse(entry.timestamp);
    if (Number.isFinite(parsed) && parsed >= MIN_OMP_TIMESTAMP_MS) return parsed;
  }
  return null;
}

// Shared implementation for the oh-my-pi session format. omp and omo both
// persist it verbatim, so they differ only in where the sessions live, which
// cursor namespace they own, and how reasoning tokens are spelled.
async function parseOmpLikeIncremental({
  sessionFiles,
  subagentFiles,
  cursors,
  queuePath,
  projectQueuePath,
  publicRepoResolver,
  onProgress,
  env,
  defaultModel,
  stateKey,
  source,
  resolveSessionFiles,
  resolveSubagentFiles,
  resolveDefaultModel,
  resolveFileCwd,
  reasoningFields,
  reasoningIncludedInOutput = false,
  maxRecordBytes = OMP_LIKE_MAX_RECORD_BYTES,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const projectEnabled = typeof projectQueuePath === "string" && projectQueuePath.length > 0;
  const providerState =
    cursors[stateKey] && typeof cursors[stateKey] === "object" ? cursors[stateKey] : {};
  const seenIds = new Set(Array.isArray(providerState.seenIds) ? providerState.seenIds : []);
  const projectSeenIds = new Set(
    Array.isArray(providerState.projectSeenIds) ? providerState.projectSeenIds : [],
  );
  const fileOffsets =
    providerState.fileOffsets && typeof providerState.fileOffsets === "object"
      ? { ...providerState.fileOffsets }
      : {};
  const projectFileOffsets =
    providerState.projectFileOffsets && typeof providerState.projectFileOffsets === "object"
      ? { ...providerState.projectFileOffsets }
      : {};

  const mainFiles = Array.isArray(sessionFiles)
    ? sessionFiles
    : resolveSessionFiles(env || process.env);
  // Subagent transcripts share the session format and count toward the same
  // provider totals; they're discovered separately because they nest below the
  // cwd level. When the caller supplies explicit sessionFiles (tests), don't
  // auto-resolve — keep the parse hermetic.
  const subFiles = Array.isArray(subagentFiles)
    ? subagentFiles
    : Array.isArray(sessionFiles)
      ? []
      : resolveSubagentFiles(env || process.env);
  const files = [...mainFiles, ...subFiles];
  const fallbackModel = defaultModel || resolveDefaultModel();

  if (files.length === 0) {
    cursors[stateKey] = {
      ...providerState,
      seenIds: Array.from(seenIds),
      fileOffsets,
      ...(projectEnabled
        ? {
            projectSeenIds: Array.from(projectSeenIds),
            projectFileOffsets,
          }
        : {}),
      updatedAt: new Date().toISOString(),
    };
    return {
      recordsProcessed: 0,
      eventsAggregated: 0,
      bucketsQueued: 0,
      projectBucketsQueued: 0,
    };
  }

  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  const projectState = projectEnabled ? normalizeProjectState(cursors?.projectHourly) : null;
  const projectTouchedBuckets = projectEnabled ? new Set() : null;
  const projectMetaCache = projectEnabled ? new Map() : null;
  const publicRepoCache = projectEnabled ? new Map() : null;
  const cb = typeof onProgress === "function" ? onProgress : null;
  let recordsProcessed = 0;
  let eventsAggregated = 0;

  for (let fileIdx = 0; fileIdx < files.length; fileIdx++) {
    const filePath = files[fileIdx];
    let stat;
    try { stat = fssync.statSync(filePath); } catch { continue; }

    const prevEntry = fileOffsets[filePath] || {};
    const prevSize = Number(prevEntry.size) || 0;
    const prevIno = prevEntry.ino;
    // Re-read from start if file shrunk (truncate/rewrite) or inode changed.
    const inodeChanged = typeof prevIno === "number" && prevIno !== stat.ino;
    const startOffset = stat.size < prevSize || inodeChanged ? 0 : prevSize;
    if (stat.size <= startOffset) continue;

    const committedEndOffset = await scanOmpLikeTranscript(filePath, startOffset, maxRecordBytes, (entry) => {
      // First line of each file is type:"session" (header) — skip all
      // non-message records.
      if (!entry || entry.type !== "message") return;

      // Only assistant messages carry token usage.
      const msg = entry.message;
      if (!msg || msg.role !== "assistant") return;

      const usage = msg.usage;
      if (!usage || typeof usage !== "object") return;

      // Dedup by top-level entry id (8-char string assigned by oh-my-pi).
      const entryId = typeof entry.id === "string" && entry.id ? entry.id : null;
      if (!entryId) return;
      if (seenIds.has(entryId)) return;

      recordsProcessed++;

      const input = toNonNegativeInt(usage.input);
      const output = toNonNegativeInt(usage.output);
      const cacheRead = toNonNegativeInt(usage.cacheRead);
      const cacheWrite = toNonNegativeInt(usage.cacheWrite);
      const reasoningTokens = pickUsageInt(usage, reasoningFields);

      if (
        input === 0 &&
        output === 0 &&
        cacheRead === 0 &&
        cacheWrite === 0 &&
        reasoningTokens === 0
      ) {
        seenIds.add(entryId);
        return;
      }

      // Entries with no resolvable timestamp are skipped — they cannot be
      // placed in a bucket.
      const tsMs = ompEntryTimestampMs(msg, entry);
      if (tsMs == null) {
        seenIds.add(entryId);
        return;
      }

      const tsIso = new Date(tsMs).toISOString();
      const bucketStart = toUtcHalfHourStart(tsIso);
      if (!bucketStart) return;

      // Use provided totalTokens when available; otherwise sum all components.
      // OmO folds reasoning into output, so the fallback must not add it again.
      const totalTokens =
        Number.isFinite(Number(usage.totalTokens)) && Number(usage.totalTokens) > 0
          ? toNonNegativeInt(usage.totalTokens)
          : input + output + cacheRead + cacheWrite +
            (reasoningIncludedInOutput ? 0 : reasoningTokens);

      const model = normalizeModelInput(msg.model) || fallbackModel;

      const delta = {
        input_tokens: input,
        cached_input_tokens: cacheRead,
        cache_creation_input_tokens: cacheWrite,
        output_tokens: output,
        reasoning_output_tokens: reasoningTokens,
        total_tokens: totalTokens,
        conversation_count: 1,
      };

      const bucket = getHourlyBucket(hourlyState, source, model, bucketStart);
      addTotals(bucket.totals, delta);
      touchedBuckets.add(bucketKey(source, model, bucketStart));
      seenIds.add(entryId);
      eventsAggregated++;

      if (cb) {
        cb({
          index: fileIdx + 1,
          total: files.length,
          recordsProcessed,
          eventsAggregated,
          bucketsQueued: touchedBuckets.size,
        });
      }
    });

    fileOffsets[filePath] = {
      size: committedEndOffset,
      mtimeMs: stat.mtimeMs,
      ino: stat.ino,
    };
  }

  // Project attribution has an independent cursor so upgrading an existing
  // installation can backfill already-consumed OMP sessions without adding
  // those messages to the total-usage buckets a second time. Current OMP
  // session headers persist the real cwd; unlike the encoded session folder,
  // it is lossless even when path components contain dashes.
  if (projectEnabled) {
    for (const filePath of files) {
      let stat;
      try { stat = fssync.statSync(filePath); } catch { continue; }

      const prevEntry = projectFileOffsets[filePath] || {};
      const prevSize = Number(prevEntry.size) || 0;
      const prevIno = prevEntry.ino;
      const inodeChanged = typeof prevIno === "number" && prevIno !== stat.ino;
      const startOffset = stat.size < prevSize || inodeChanged ? 0 : prevSize;
      if (stat.size <= startOffset) continue;

      const cwd = await resolveFileCwd(filePath);
      const projectContext = cwd
        ? await resolveProjectContextForPath({
            startDir: wsl.mapWslCwdToUnc(cwd, filePath),
            projectMetaCache,
            publicRepoCache,
            publicRepoResolver,
            projectState,
          })
        : null;
      const projectRef = projectContext?.projectRef || null;
      const projectKey = projectContext?.projectKey || null;
      let committedEndOffset = stat.size;

      if (projectKey && projectRef) {
        committedEndOffset = await scanOmpLikeTranscript(filePath, startOffset, maxRecordBytes, (entry) => {
          const msg = entry?.type === "message" ? entry.message : null;
          const usage = msg?.role === "assistant" ? msg.usage : null;
          if (!usage || typeof usage !== "object") return;

          const entryId = typeof entry.id === "string" && entry.id ? entry.id : null;
          if (!entryId || projectSeenIds.has(entryId)) return;

          const input = toNonNegativeInt(usage.input);
          const output = toNonNegativeInt(usage.output);
          const cacheRead = toNonNegativeInt(usage.cacheRead);
          const cacheWrite = toNonNegativeInt(usage.cacheWrite);
          const reasoningTokens = pickUsageInt(usage, reasoningFields);
          if (
            input === 0 &&
            output === 0 &&
            cacheRead === 0 &&
            cacheWrite === 0 &&
            reasoningTokens === 0
          ) {
            projectSeenIds.add(entryId);
            return;
          }

          const tsMs = ompEntryTimestampMs(msg, entry);
          const bucketStart = tsMs == null
            ? null
            : toUtcHalfHourStart(new Date(tsMs).toISOString());
          if (!bucketStart) {
            projectSeenIds.add(entryId);
            return;
          }

          const totalTokens =
            Number.isFinite(Number(usage.totalTokens)) && Number(usage.totalTokens) > 0
              ? toNonNegativeInt(usage.totalTokens)
              : input + output + cacheRead + cacheWrite +
                (reasoningIncludedInOutput ? 0 : reasoningTokens);
          const delta = {
            input_tokens: input,
            cached_input_tokens: cacheRead,
            cache_creation_input_tokens: cacheWrite,
            output_tokens: output,
            reasoning_output_tokens: reasoningTokens,
            total_tokens: totalTokens,
            conversation_count: 1,
          };
          const projectBucket = getProjectBucket(
            projectState,
            projectKey,
            source,
            bucketStart,
            projectRef,
          );
          addTotals(projectBucket.totals, delta);
          projectTouchedBuckets.add(projectBucketKey(projectKey, source, bucketStart));
          projectSeenIds.add(entryId);
        });
      }

      projectFileOffsets[filePath] = {
        size: committedEndOffset,
        mtimeMs: stat.mtimeMs,
        ino: stat.ino,
      };
    }
  }

  // Cap dedup set to last 10k IDs to bound cursor state size — same convention
  // as Kimi/CodeBuddy/Copilot so cursors.json doesn't grow unbounded.
  const seenArr = Array.from(seenIds);
  const cappedSeen =
    seenArr.length > 10_000 ? seenArr.slice(seenArr.length - 10_000) : seenArr;
  const projectSeenArr = Array.from(projectSeenIds);
  const cappedProjectSeen =
    projectSeenArr.length > 10_000
      ? projectSeenArr.slice(projectSeenArr.length - 10_000)
      : projectSeenArr;

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
  cursors.hourly = hourlyState;
  if (projectState) {
    projectState.updatedAt = updatedAt;
    cursors.projectHourly = projectState;
  }
  cursors[stateKey] = {
    ...providerState,
    seenIds: cappedSeen,
    fileOffsets,
    ...(projectEnabled
      ? {
          projectSeenIds: cappedProjectSeen,
          projectFileOffsets,
        }
      : {}),
    updatedAt,
  };

  return { recordsProcessed, eventsAggregated, bucketsQueued, projectBucketsQueued };
}

async function parseOmpIncremental(options = {}) {
  return parseOmpLikeIncremental({
    ...options,
    stateKey: "omp",
    source: "omp",
    resolveSessionFiles: resolveOmpSessionFiles,
    resolveSubagentFiles: resolveOmpSubagentFiles,
    resolveDefaultModel: resolveOmpDefaultModel,
    resolveFileCwd: resolveOmpFileCwd,
    reasoningFields: ["reasoningTokens"],
  });
}

async function parseOmoIncremental(options = {}) {
  return parseOmpLikeIncremental({
    ...options,
    stateKey: "omo",
    source: "omo",
    resolveSessionFiles: resolveOmoSessionFiles,
    resolveSubagentFiles: resolveOmoSubagentFiles,
    resolveDefaultModel: resolveOmoDefaultModel,
    resolveFileCwd: resolveOmoFileCwd,
    reasoningFields: ["reasoningTokens", "reasoning"],
    reasoningIncludedInOutput: true,
  });
}

function omoAgentDirCollidesWithOmp(env = process.env) {
  const omoAgentDir = resolveOmoAgentDir(env);
  const ompAgentDir = resolveOmpAgentDir(env);
  if (!omoAgentDir || !ompAgentDir) return false;
  return path.resolve(omoAgentDir) === path.resolve(ompAgentDir);
}

module.exports = {
  resolveOmpSessionFiles,
  resolveOmpSubagentFiles,
  resolveOmpDefaultModel,
  resolveOmoHome,
  resolveOmoAgentDir,
  resolveOmoSessionFiles,
  resolveOmoSubagentFiles,
  resolveOmoDefaultModel,
  parseOmpIncremental,
  parseOmoIncremental,
  omoAgentDirCollidesWithOmp,
};
