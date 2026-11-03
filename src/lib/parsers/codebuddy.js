// codebuddy usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const readline = require("node:readline");
const { ensureDir } = require("../fs");
const { physicalJsonlRecords } = require("../jsonl-lines");
const { readSqliteJsonRowsAsync } = require("../sqlite-reader");
const wsl = require("../wsl-probe");
const { expandHome } = require("../scan-roots");
const {
  DEFAULT_SOURCE,
  BUCKET_SEPARATOR,
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
  normalizeSourceInput,
  normalizeModelInput,
  resolveProjectContextForPath,
  toNonNegativeInt,
  pickWin32ProviderPath,
  snapshotSqliteDb,
  stageProviderBuckets,
  OMP_LIKE_MAX_RECORD_BYTES,
  scanOmpLikeTranscript,
} = require("./core");

function firstPresentNonNegativeInt(values) {
  for (const value of values) {
    if (value !== undefined && value !== null) {
      return toNonNegativeInt(value);
    }
  }
  return 0;
}

function firstPositiveOrPresentNonNegativeInt(values) {
  let firstPresent = null;
  for (const value of values) {
    if (value === undefined || value === null) continue;
    const n = toNonNegativeInt(value);
    if (firstPresent === null) firstPresent = n;
    if (n > 0) return n;
  }
  return firstPresent === null ? 0 : firstPresent;
}

// ─────────────────────────────────────────────────────────────────────────────
// CodeBuddy CLI — passive JSONL reader (~/.codebuddy/projects/<cwd>/<sid>.jsonl)
//
// Tencent's CodeBuddy CLI is structurally cloned from Claude Code:
//   ~/.codebuddy/projects/<encoded-cwd>/<sessionId>.jsonl  — conversation log
//   ~/.codebuddy/sessions/<pid>.json                       — session metadata
//   ~/.codebuddy/settings.json                             — `{"model": "..."}`
//
// CodeBuddy ships NO hook system — we incrementally tail the JSONL files on
// each sync (passive scan only, same shape as Kimi's wire.jsonl reader).
//
// Per-line record types: message, reasoning, topic, file-history-snapshot.
// Only `type=="message" and role=="assistant"` carry token usage. The shape:
//
//   providerData.rawUsage = {
//     prompt_tokens: 22223,           // OpenAI-style — INCLUDES cached
//     completion_tokens: 250,
//     prompt_tokens_details: { cached_tokens: 512, reasoning_tokens?: number },
//     cache_read_input_tokens: 0,     // Anthropic-style mirror (often 0)
//     cache_creation_input_tokens: 0,
//   }
//
// Token math (matches the repo's queue convention; do NOT pass prompt_tokens
// through unchanged — that double-counts cached input):
//   input_tokens               = prompt_tokens - prompt_tokens_details.cached_tokens
//   cached_input_tokens        = prompt_tokens_details.cached_tokens
//   cache_creation_input_tokens = cache_creation_input_tokens (often 0)
//   output_tokens              = completion_tokens
//   reasoning_output_tokens    = prompt_tokens_details.reasoning_tokens || 0
//   total_tokens               = sum of the above
// ─────────────────────────────────────────────────────────────────────────────

function resolveCodebuddyHome(env = process.env) {
  const home = env.HOME || os.homedir();
  if (env.CODEBUDDY_HOME) return env.CODEBUDDY_HOME;
  if (process.platform === "win32") {
    return pickWin32ProviderPath({
      env,
      nativeValue: path.join(home, ".codebuddy"),
      wslProviderDir: ".codebuddy",
    });
  }
  return path.join(home, ".codebuddy");
}

function resolveCodebuddyDefaultModel(env = process.env) {
  const fallback = "codebuddy-unknown";
  try {
    const codebuddyHome = resolveCodebuddyHome(env);
    if (!codebuddyHome) return fallback;
    const settingsPath = path.join(codebuddyHome, "settings.json");
    const raw = fssync.readFileSync(settingsPath, "utf8");
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object" && typeof parsed.model === "string" && parsed.model.trim()) {
      return parsed.model.trim();
    }
  } catch (_e) {
    // settings missing or malformed — fall through
  }
  return fallback;
}

// Modern CodeBuddy writes token usage into providerData.rawUsage on JSONL
// records. Older IDE installs may have no usable JSONL usage and rely on the
// extension log fallback below. Keep the sniff bounded: it is used by the
// guarded historical migration to decide whether a JSONL rebuild is safe.
function codebuddyJsonlHasUsage(filePath) {
  let fd;
  try {
    fd = fssync.openSync(filePath, "r");
    const buffer = Buffer.alloc(64 * 1024);
    const bytesRead = fssync.readSync(fd, buffer, 0, buffer.length, 0);
    const lines = buffer.toString("utf8", 0, bytesRead).split("\n");
    const limit = Math.min(lines.length, 100);
    for (let i = 0; i < limit; i += 1) {
      const line = lines[i];
      if (!line || (!line.includes("rawUsage") && !line.includes("\"usage\""))) continue;
      let entry;
      try { entry = JSON.parse(line); } catch { continue; }
      if (!entry || typeof entry !== "object") continue;
      const provider = entry.providerData;
      if (provider && typeof provider === "object" && provider.rawUsage && typeof provider.rawUsage === "object") {
        return true;
      }
      if (entry.usage && typeof entry.usage === "object") return true;
    }
  } catch {
    return false;
  } finally {
    if (fd !== undefined) {
      try { fssync.closeSync(fd); } catch {}
    }
  }
  return false;
}

// Legacy IDE extension logs and modern JSONL records do not share an id. Keep
// a bounded, source-neutral fingerprint ledger so a log line can be matched to
// the JSONL round-trip it mirrors without collapsing legitimate same-second
// requests. The ledger is persisted in the CodeBuddy cursor because the two
// sources can arrive on different syncs.
function codebuddyUsageFingerprint({ model, timestampMs, inputTokens, cacheRead, cacheCreation, outputTokens, reasoningTokens }) {
  if (!Number.isFinite(Number(timestampMs)) || Number(timestampMs) <= 0) return null;
  const second = Math.floor(Number(timestampMs) / 1000);
  return [
    second,
    normalizeModelInput(model) || "unknown",
    toNonNegativeInt(inputTokens),
    toNonNegativeInt(cacheRead),
    toNonNegativeInt(cacheCreation),
    toNonNegativeInt(outputTokens),
    toNonNegativeInt(reasoningTokens),
  ].join(":");
}

function restoreCodebuddyUsageFingerprints(raw) {
  const state = new Map();
  if (!raw || typeof raw !== "object") return state;
  for (const [key, value] of Object.entries(raw)) {
    const jsonl = toNonNegativeInt(value?.jsonl);
    const log = toNonNegativeInt(value?.log);
    if (jsonl > 0 || log > 0) state.set(key, { jsonl, log });
  }
  return state;
}

function addCodebuddyJsonlFingerprint(state, fingerprint) {
  if (!fingerprint) return false;
  const current = state.get(fingerprint) || { jsonl: 0, log: 0 };
  const mirrored = current.log > current.jsonl;
  current.jsonl += 1;
  state.set(fingerprint, current);
  return mirrored;
}

function consumeCodebuddyLogFingerprint(state, fingerprint) {
  if (!fingerprint) return false;
  const current = state.get(fingerprint) || { jsonl: 0, log: 0 };
  const mirrored = current.jsonl > current.log;
  current.log += 1;
  state.set(fingerprint, current);
  return mirrored;
}

function capCodebuddyUsageFingerprints(state, maxEntries = 10_000) {
  const entries = Array.from(state.entries());
  if (entries.length <= maxEntries) return Object.fromEntries(entries);
  return Object.fromEntries(entries.slice(entries.length - maxEntries));
}

function resolveCodebuddyProjectFiles(env = process.env) {
  const codebuddyHome = resolveCodebuddyHome(env);
  const jsonlFiles = [];
  const logFiles = [];

  // 1. Recursive JSONL scan in codebuddyHome/projects
  if (codebuddyHome) {
    const projectsDir = path.join(codebuddyHome, "projects");
    if (fssync.existsSync(projectsDir)) {
      const walkJsonl = (dir) => {
        let entries;
        try { entries = fssync.readdirSync(dir, { withFileTypes: true }); } catch { return; }
        for (const entry of entries) {
          const full = path.join(dir, entry.name);
          let isDir = entry.isDirectory();
          let isFile = entry.isFile();
          if (!isDir && !isFile) {
            try {
              const st = fssync.statSync(full);
              isDir = st.isDirectory();
              isFile = st.isFile();
            } catch { continue; }
          }
          if (isDir) walkJsonl(full);
          else if (isFile && entry.name.endsWith(".jsonl")) jsonlFiles.push(full);
        }
      };
      walkJsonl(projectsDir);
    }
  }

  // 2. Active IDE extension logs scan
  const logRoots = [];
  const home = env.HOME || os.homedir();

  if (process.platform === "darwin") {
    const appSupport = path.join(home, "Library", "Application Support");
    logRoots.push({ dir: path.join(appSupport, "CodeBuddy CN", "logs"), pattern: "codebuddy-extension-log" });
    logRoots.push({ dir: path.join(appSupport, "Code", "logs"), pattern: "codebuddy-extension-log" });
    logRoots.push({ dir: path.join(appSupport, "CodeBuddyExtension", "Logs", "CodeBuddyIDE"), pattern: "*.log" });
    logRoots.push({ dir: path.join(appSupport, "CodeBuddyExtension", "Logs", "VSCode"), pattern: "*.log" });
  } else if (process.platform === "win32") {
    const appData = env.APPDATA || path.join(home, "AppData", "Roaming");
    const localAppData = env.LOCALAPPDATA || path.join(home, "AppData", "Local");
    logRoots.push({ dir: path.join(appData, "CodeBuddy CN", "logs"), pattern: "codebuddy-extension-log" });
    logRoots.push({ dir: path.join(appData, "Code", "logs"), pattern: "codebuddy-extension-log" });
    logRoots.push({ dir: path.join(localAppData, "CodeBuddyExtension", "Logs", "CodeBuddyIDE"), pattern: "*.log" });
    logRoots.push({ dir: path.join(localAppData, "CodeBuddyExtension", "Logs", "VSCode"), pattern: "*.log" });
  } else {
    const xdgConfig = env.XDG_CONFIG_HOME || path.join(home, ".config");
    const xdgData = env.XDG_DATA_HOME || path.join(home, ".local", "share");
    logRoots.push({ dir: path.join(xdgConfig, "CodeBuddy CN", "logs"), pattern: "codebuddy-extension-log" });
    logRoots.push({ dir: path.join(xdgConfig, "Code", "logs"), pattern: "codebuddy-extension-log" });
    logRoots.push({ dir: path.join(xdgData, "CodeBuddyExtension", "Logs", "CodeBuddyIDE"), pattern: "*.log" });
    logRoots.push({ dir: path.join(xdgData, "CodeBuddyExtension", "Logs", "VSCode"), pattern: "*.log" });
  }

  for (const root of logRoots) {
    if (!fssync.existsSync(root.dir)) continue;
    const walkLogs = (dir) => {
      let entries;
      try { entries = fssync.readdirSync(dir, { withFileTypes: true }); } catch { return; }
      for (const entry of entries) {
        const full = path.join(dir, entry.name);
        let isDir = entry.isDirectory();
        let isFile = entry.isFile();
        if (!isDir && !isFile) {
          try {
            const st = fssync.statSync(full);
            isDir = st.isDirectory();
            isFile = st.isFile();
          } catch { continue; }
        }
        if (isDir) {
          walkLogs(full);
        } else if (isFile && entry.name.endsWith(".log")) {
          if (root.pattern === "*.log") {
            logFiles.push(full);
          } else if (root.pattern === "codebuddy-extension-log") {
            if (full.toLowerCase().includes("tencent-cloud.coding-copilot")) {
              logFiles.push(full);
            }
          }
        }
      }
    };
    walkLogs(root.dir);
  }

  const fallbackMode = String(env.TOKEN_PULSE_CODEBUDDY_LOG_FALLBACK || "").trim();
  // Auto mode keeps both sources available. The parser performs bounded,
  // count-aware cross-source fingerprint matching, so mixed histories retain
  // legacy-only log sessions while mirrored rounds are counted once. Env=0 is
  // still a supported escape hatch for users who want to suppress extension
  // logs entirely; env=1 remains an explicit force-on value.
  const includeLogs = fallbackMode !== "0";
  const files = [...jsonlFiles, ...(includeLogs ? logFiles : [])];
  files.sort((a, b) => a.localeCompare(b));
  return files;
}

function getBracketValueAfter(line, marker) {
  const parts = line.split(marker);
  if (parts.length < 2) return null;
  const after = parts[1];
  const start = after.indexOf("[");
  if (start === -1) return null;
  const afterOpen = after.slice(start + 1);
  const end = afterOpen.indexOf("]");
  if (end === -1) return null;
  return afterOpen.slice(0, end).trim();
}

function parseLogTimestampMs(line, fallbackTs) {
  let raw = "";
  if (line.startsWith("[")) {
    const endIdx = line.indexOf("]");
    if (endIdx !== -1) {
      raw = line.slice(1, endIdx).trim();
    }
  } else {
    const idx = line.indexOf(" [");
    if (idx !== -1) {
      raw = line.slice(0, idx).trim();
    } else {
      raw = line.slice(0, 23).trim();
    }
  }
  let ts = Date.parse(raw);
  if (isNaN(ts)) {
    const normalized = raw.replace(/\//g, '-');
    ts = Date.parse(normalized);
  }
  return isNaN(ts) ? fallbackTs : ts;
}

async function parseCodebuddyIncremental({
  projectFiles,
  cursors,
  queuePath,
  onProgress,
  env,
  defaultModel,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const codebuddyState =
    cursors.codebuddy && typeof cursors.codebuddy === "object" ? cursors.codebuddy : {};
  const seenIds = new Set(
    Array.isArray(codebuddyState.seenIds) ? codebuddyState.seenIds : [],
  );
  const fileOffsets =
    codebuddyState.fileOffsets && typeof codebuddyState.fileOffsets === "object"
      ? { ...codebuddyState.fileOffsets }
      : {};
  const logModelsByAgent =
    codebuddyState.logModelsByAgent && typeof codebuddyState.logModelsByAgent === "object"
      ? { ...codebuddyState.logModelsByAgent }
      : {};
  const usageFingerprints = restoreCodebuddyUsageFingerprints(codebuddyState.usageFingerprints);

  const discoveredFiles = Array.isArray(projectFiles)
    ? projectFiles
    : resolveCodebuddyProjectFiles(env || process.env);
  // JSONL must be parsed before extension logs so the fingerprint ledger can
  // match mirrored rounds in the same sync. Keep the legacy bare-string API
  // accepted for callers and tests.
  const files = [...discoveredFiles].sort((a, b) => {
    const aPath = typeof a === "string" ? a : a?.path || "";
    const bPath = typeof b === "string" ? b : b?.path || "";
    const aLog = aPath.endsWith(".log") ? 1 : 0;
    const bLog = bPath.endsWith(".log") ? 1 : 0;
    return aLog - bLog || aPath.localeCompare(bPath);
  });
  const fallbackModel = defaultModel || resolveCodebuddyDefaultModel(env || process.env);

  if (files.length === 0) {
    cursors.codebuddy = {
      ...codebuddyState,
      seenIds: Array.from(seenIds),
      fileOffsets,
      usageFingerprints: capCodebuddyUsageFingerprints(usageFingerprints),
      updatedAt: new Date().toISOString(),
    };
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
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
    const inodeChanged = typeof prevIno === "number" && prevIno !== stat.ino;
    const startOffset = stat.size < prevSize || inodeChanged ? 0 : prevSize;
    if (stat.size <= startOffset) continue;

    let stream;
    try {
      stream = fssync.createReadStream(filePath, {
        encoding: "utf8",
        start: startOffset,
      });
    } catch { continue; }
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

    const isLogFile = filePath.endsWith(".log");
    const modelsByAgent = new Map();
    const agentModelKey = (agentId) => `${filePath}::${agentId}`;

    for await (const line of rl) {
      if (!line || !line.trim()) continue;

      if (isLogFile) {
        if (line.includes("[CraftInvokableAgent]") && line.includes("Model prepared:")) {
          const agentId = getBracketValueAfter(line, "[CraftInvokableAgent]");
          const marker = "Model prepared:";
          const idx = line.indexOf(marker);
          if (agentId && idx !== -1) {
            const afterMarker = line.slice(idx + marker.length).trim();
            let modelId = afterMarker;
            const openParen = afterMarker.lastIndexOf("(");
            if (openParen !== -1) {
              const tail = afterMarker.slice(openParen + 1);
              const closeParen = tail.indexOf(")");
              if (closeParen !== -1) {
                const inner = tail.slice(0, closeParen).trim();
                if (inner) modelId = inner;
              }
            }
            modelsByAgent.set(agentId, modelId);
            logModelsByAgent[agentModelKey(agentId)] = modelId;
          }
          continue;
        }

        if (!line.includes("[AgentReporter]") || !line.includes("Agent execution successful with usage:")) {
          continue;
        }

        const agentId = getBracketValueAfter(line, "[AgentReporter]");
        if (!agentId) continue;

        const marker = "Agent execution successful with usage:";
        const usageParts = line.split(marker);
        if (usageParts.length < 2) continue;

        const usageJsonRaw = usageParts[1].trim();
        const endBrace = usageJsonRaw.lastIndexOf("}");
        if (endBrace === -1) continue;

        let usage;
        try {
          usage = JSON.parse(usageJsonRaw.slice(0, endBrace + 1));
        } catch { continue; }

        if (!usage || typeof usage !== "object") continue;

        const inputRaw = firstPresentNonNegativeInt([
          usage.cachedMissTokens,
          usage.cacheMissTokens,
          usage.input_tokens,
          usage.inputTokens,
          usage.prompt_tokens,
        ]);
        const outputRaw = firstPresentNonNegativeInt([
          usage.output_tokens,
          usage.outputTokens,
          usage.completion_tokens,
        ]);
        const cacheReadRaw = firstPositiveOrPresentNonNegativeInt([
          usage.cache_read_input_tokens,
          usage.cacheReadInputTokens,
          usage.cacheTokens,
          usage.prompt_cache_hit_tokens,
          usage.cached_tokens,
        ]);
        const cacheCreationRaw = firstPositiveOrPresentNonNegativeInt([
          usage.cache_creation_input_tokens,
          usage.cacheCreationInputTokens,
          usage.cachedWriteTokens,
          usage.prompt_cache_write_tokens,
        ]);
        const reasoningRaw = firstPresentNonNegativeInt([
          usage.completion_thinking_tokens,
          usage.completionThinkingTokens,
          usage.reasoningTokens,
        ]);

        const tsMs = parseLogTimestampMs(line, stat.mtimeMs);
        const dedupSecond = Math.floor(tsMs / 1000);
        const messageId = `codebuddy:extension-log:${agentId}:${dedupSecond}:${inputRaw}:${outputRaw}:${cacheReadRaw}:${cacheCreationRaw}:${reasoningRaw}`;

        if (seenIds.has(messageId)) continue;
        recordsProcessed++;

        let inputTokens = toNonNegativeInt(inputRaw);
        const completionTokens = toNonNegativeInt(outputRaw);
        const cacheRead = toNonNegativeInt(cacheReadRaw);
        const cacheCreation = toNonNegativeInt(cacheCreationRaw);
        const reasoningTokens = toNonNegativeInt(reasoningRaw);

        const isFromTotalField =
          usage.cachedMissTokens === undefined &&
          usage.cacheMissTokens === undefined &&
          usage.prompt_cache_miss_tokens === undefined;
        if (isFromTotalField && cacheRead > 0) {
          inputTokens = Math.max(0, inputTokens - cacheRead);
        }

        if (inputTokens === 0 && completionTokens === 0 && cacheRead === 0 && cacheCreation === 0) {
          seenIds.add(messageId);
          continue;
        }

        const tsIso = new Date(tsMs).toISOString();
        const bucketStart = toUtcHalfHourStart(tsIso);
        if (!bucketStart) continue;

        const rawModel =
          modelsByAgent.get(agentId) ||
          logModelsByAgent[agentModelKey(agentId)] ||
          fallbackModel;
        const model = normalizeModelInput(rawModel);

        const fingerprint = codebuddyUsageFingerprint({
          model,
          timestampMs: tsMs,
          inputTokens,
          cacheRead,
          cacheCreation,
          outputTokens: completionTokens,
          reasoningTokens,
        });
        if (consumeCodebuddyLogFingerprint(usageFingerprints, fingerprint)) {
          seenIds.add(messageId);
          continue;
        }

        const delta = {
          input_tokens: inputTokens,
          cached_input_tokens: cacheRead,
          cache_creation_input_tokens: cacheCreation,
          output_tokens: completionTokens,
          reasoning_output_tokens: reasoningTokens,
          total_tokens: inputTokens + completionTokens + cacheRead + cacheCreation + reasoningTokens,
          conversation_count: 1,
        };

        const bucket = getHourlyBucket(hourlyState, "codebuddy", model, bucketStart);
        addTotals(bucket.totals, delta);
        touchedBuckets.add(bucketKey("codebuddy", model, bucketStart));
        seenIds.add(messageId);
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
      } else {
        let entry;
        try { entry = JSON.parse(line); } catch { continue; }
        if (!entry || typeof entry !== "object") continue;

        // Usage is carried on ANY record with providerData.rawUsage — assistant
        // messages AND function_call records. Each LLM round-trip (whether it
        // ends in a text reply or a tool call) carries its own usage; on real
        // installs function_call records are ~93% of round-trips and ~14x the
        // assistant-message token volume, so filtering by record type drops
        // the majority of usage. Aggregate them all; dedup per round-trip.
        // (Same convention as the WorkBuddy reader — same transcript format.)
        const provider = entry.providerData;
        const rawUsage = provider && typeof provider === "object" ? provider.rawUsage : null;
        if (!rawUsage || typeof rawUsage !== "object") continue;

        const sessionId =
          typeof entry.sessionId === "string" && entry.sessionId
            ? entry.sessionId
            : path.basename(filePath, ".jsonl");
        const tsMs =
          Number.isFinite(Number(entry.timestamp)) && Number(entry.timestamp) > 0
            ? Number(entry.timestamp)
            : null;
        // One usage record per LLM round-trip; providerData.messageId is the
        // response-level id shared by the function_call/message pair of the
        // same round-trip, so it is the most stable dedup key. entry.id is the
        // per-record append id (unique per line, NOT per round-trip) and must
        // NOT be preferred — preferring it would count a round-trip once per
        // record type instead of once total.
        const messageId =
          typeof provider?.messageId === "string" && provider?.messageId
            ? provider.messageId
            : typeof entry.uuid === "string" && entry.uuid
              ? entry.uuid
              : typeof entry.id === "string" && entry.id
                ? entry.id
                : tsMs != null
                  ? `${sessionId}:${tsMs}`
                  : null;
        if (!messageId) continue;
        if (seenIds.has(messageId)) continue;

        recordsProcessed++;

        const promptTokens = toNonNegativeInt(rawUsage.prompt_tokens);
        const completionTokensRaw = toNonNegativeInt(rawUsage.completion_tokens);
        const details =
          rawUsage.prompt_tokens_details && typeof rawUsage.prompt_tokens_details === "object"
            ? rawUsage.prompt_tokens_details
            : {};
        const completionDetails =
          rawUsage.completion_tokens_details && typeof rawUsage.completion_tokens_details === "object"
            ? rawUsage.completion_tokens_details
            : {};
        // Cache-read mirrors three ways depending on upstream: Anthropic-style
        // cache_read_input_tokens, OpenAI-style prompt_tokens_details.cached_tokens,
        // DeepSeek-style prompt_cache_hit_tokens. Take the max (they mirror the
        // same quantity; on real data exactly one is non-zero).
        const cachedTokens = Math.max(
          toNonNegativeInt(details.cached_tokens),
          toNonNegativeInt(rawUsage.prompt_cache_hit_tokens),
        );
        const cacheReadAlt = toNonNegativeInt(rawUsage.cache_read_input_tokens);
        const cacheCreation = Math.max(
          toNonNegativeInt(rawUsage.cache_creation_input_tokens),
          toNonNegativeInt(rawUsage.prompt_cache_write_tokens),
        );
        // reasoning_tokens lives in completion_tokens_details (verified on real
        // CodeBuddy data: prompt_tokens_details.reasoning_tokens is always 0).
        // completion_tokens INCLUDES reasoning, so subtract to avoid double-count.
        const reasoningTokens = Math.min(completionTokensRaw, toNonNegativeInt(completionDetails.reasoning_tokens));
        const completionTokens = Math.max(0, completionTokensRaw - reasoningTokens);

        const cacheRead = Math.max(cachedTokens, cacheReadAlt);
        const inputTokens = Math.max(0, promptTokens - cacheRead - cacheCreation);

        if (
          inputTokens === 0 &&
          completionTokens === 0 &&
          cacheRead === 0 &&
          cacheCreation === 0 &&
          reasoningTokens === 0
        ) {
          seenIds.add(messageId);
          continue;
        }

        if (tsMs == null) {
          seenIds.add(messageId);
          continue;
        }
        const tsIso = new Date(tsMs).toISOString();
        const bucketStart = toUtcHalfHourStart(tsIso);
        if (!bucketStart) continue;

        const model =
          normalizeModelInput(provider?.model) ||
          normalizeModelInput(entry.model) ||
          fallbackModel;

        const fingerprint = codebuddyUsageFingerprint({
          model,
          timestampMs: tsMs,
          inputTokens,
          cacheRead,
          cacheCreation,
          outputTokens: completionTokens,
          reasoningTokens,
        });
        if (addCodebuddyJsonlFingerprint(usageFingerprints, fingerprint)) {
          seenIds.add(messageId);
          continue;
        }

        const delta = {
          input_tokens: inputTokens,
          cached_input_tokens: cacheRead,
          cache_creation_input_tokens: cacheCreation,
          output_tokens: completionTokens,
          reasoning_output_tokens: reasoningTokens,
          total_tokens:
            inputTokens + completionTokens + cacheRead + cacheCreation + reasoningTokens,
          conversation_count: 1,
        };

        const bucket = getHourlyBucket(hourlyState, "codebuddy", model, bucketStart);
        addTotals(bucket.totals, delta);
        touchedBuckets.add(bucketKey("codebuddy", model, bucketStart));
        seenIds.add(messageId);
        // Preserve the pre-fingerprint cursor key as well. Older releases
        // keyed JSONL rows by entry.id; retaining it prevents the first
        // post-upgrade sync from replaying an already-counted row when
        // providerData.messageId is now preferred.
        if (typeof entry.id === "string" && entry.id && entry.id !== messageId) {
          seenIds.add(entry.id);
        }
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
      }
    }

    let postStat = stat;
    try { postStat = fssync.statSync(filePath); } catch {}
    fileOffsets[filePath] = {
      size: postStat.size,
      mtimeMs: postStat.mtimeMs,
      ino: postStat.ino,
    };
  }

  // Cap dedup set to last 10k IDs to bound cursor state size — same convention
  // as Kimi/Copilot so cursors.json doesn't grow unbounded.
  const seenArr = Array.from(seenIds);
  const cappedSeen =
    seenArr.length > 10_000 ? seenArr.slice(seenArr.length - 10_000) : seenArr;
  const logModelEntries = Object.entries(logModelsByAgent);
  const cappedLogModelsByAgent =
    logModelEntries.length > 10_000
      ? Object.fromEntries(logModelEntries.slice(logModelEntries.length - 10_000))
      : logModelsByAgent;

  const bucketsQueued = await enqueueTouchedBuckets({
    queuePath,
    hourlyState,
    touchedBuckets,
  });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.codebuddy = {
    ...codebuddyState,
    seenIds: cappedSeen,
    fileOffsets,
    logModelsByAgent: cappedLogModelsByAgent,
    usageFingerprints: capCodebuddyUsageFingerprints(usageFingerprints),
    updatedAt,
  };

  return { recordsProcessed, eventsAggregated, bucketsQueued };
}

// ─────────────────────────────────────────────────────────────────────────────
// WorkBuddy — passive JSONL reader (~/.workbuddy/projects/<cwd>/**/*.jsonl)
//
// Tencent's WorkBuddy is a Claude-Code fork in the same "buddy" family as
// CodeBuddy, but it differs from CodeBuddy's reader in three load-bearing ways
// (each verified against real ~/.workbuddy logs, NOT assumed from CodeBuddy):
//
//   1. Usage lives on `function_call` records too — not only on
//      `type=="message" and role=="assistant"`. Each LLM round-trip (whether it
//      ends in a tool call or a text reply) carries its own providerData.rawUsage.
//      We therefore aggregate EVERY record that has providerData.rawUsage and
//      dedup per response id, instead of filtering by record type.
//
//   2. Sub-agent traffic is nested two levels deeper:
//        ~/.workbuddy/projects/<cwd>/<sessionId>.jsonl                  (main)
//        ~/.workbuddy/projects/<cwd>/<sessionId>/subagents/agent-*.jsonl (sub)
//      CodeBuddy's resolver only globs the top level, so we recurse to pick up
//      sub-agent usage (tool-results/*.txt are naturally skipped — not .jsonl).
//
//   3. rawUsage is OpenAI/DeepSeek-shaped and prompt_tokens is the FULL prompt
//      (cache reads + cache writes + genuinely-new input). The cache split is
//      mirrored two ways depending on which upstream the auto-router picked:
//        • Anthropic-style: cache_read_input_tokens / cache_creation_input_tokens
//        • DeepSeek/OpenAI-style: prompt_tokens_details.cached_tokens /
//          prompt_cache_hit_tokens  (cache_creation_input_tokens then 0)
//      Reasoning is reported inside completion_tokens (verified:
//      rawUsage.total_tokens === prompt_tokens + completion_tokens).
//
//   Token math (matches the repo's queue convention; subtract BOTH cache reads
//   AND cache writes from prompt_tokens — CodeBuddy's "prompt_tokens - cacheRead"
//   only works because its cache_creation is always 0; WorkBuddy writes cache
//   heavily, so the naive formula double-counts cache writes ~2x):
//     cacheRead   = max(cache_read_input_tokens, prompt_tokens_details.cached_tokens,
//                       prompt_cache_hit_tokens)
//     cacheCreate = cache_creation_input_tokens
//     input_tokens               = prompt_tokens - cacheRead - cacheCreate
//     cached_input_tokens        = cacheRead
//     cache_creation_input_tokens = cacheCreate
//     reasoning_output_tokens    = completion_tokens_details.reasoning_tokens
//     output_tokens              = completion_tokens - reasoning_output_tokens
//     total_tokens               = sum of the above (== prompt_tokens + completion_tokens)
//
//   model is the auto-router placeholder ("auto") — WorkBuddy does not expose
//   the underlying model in the log, so we emit it verbatim.
//
//   4. Completed ~/.workbuddy/traces/**/trace_*.json summaries expose aggregate
//      modelInfo input/output/cache totals. They are a fallback only when the
//      session has no detailed JSONL usage. The current SQLite session_usage
//      table exposes bounded context-window state (`used`/`size`) plus credits,
//      not billable token columns, so SQLite-only rows are ignored rather than
//      mislabelled as input usage. A future schema with explicit token columns
//      is accepted by workbuddySqliteUsageSnapshot.
// ─────────────────────────────────────────────────────────────────────────────

// WorkBuddy ships two regional builds that keep their data in sibling homes:
//   ~/.workbuddy     — Tencent CN build (models: hy*/glm*/deepseek-*/auto)
//   ~/.workbuddy-ai  — international build (models: gpt-5.6-*, gpt-6-*)
// Both write byte-identical JSONL, so every reader below is shared and only
// the home directory (and the bucket source key) differs. `variant` is also
// the source key written into hourly buckets, so the two builds stay
// separable in the dashboard and never share dedup state.
const WORKBUDDY_VARIANTS = {
  workbuddy: { dir: ".workbuddy", envVar: "WORKBUDDY_HOME" },
  "workbuddy-ai": { dir: ".workbuddy-ai", envVar: "WORKBUDDY_AI_HOME" },
};

function resolveWorkbuddyVariant(variant) {
  return Object.prototype.hasOwnProperty.call(WORKBUDDY_VARIANTS, variant)
    ? variant
    : "workbuddy";
}

function resolveWorkbuddyHome(env = process.env, variant = "workbuddy") {
  const spec = WORKBUDDY_VARIANTS[resolveWorkbuddyVariant(variant)];
  const home = env.HOME || env.USERPROFILE || os.homedir();
  const configured = expandHome(env[spec.envVar], home);
  if (configured) return configured;
  if (process.platform === "win32") {
    return pickWin32ProviderPath({
      env,
      nativeValue: path.join(home, spec.dir),
      wslProviderDir: spec.dir,
    });
  }
  return path.join(home, spec.dir);
}

function resolveWorkbuddyDefaultModel(env = process.env, variant = "workbuddy") {
  const fallback = "auto";
  try {
    const workbuddyHome = resolveWorkbuddyHome(env, variant);
    if (!workbuddyHome) return fallback;
    const settingsPath = path.join(workbuddyHome, "settings.json");
    const raw = fssync.readFileSync(settingsPath, "utf8");
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object" && typeof parsed.model === "string" && parsed.model.trim()) {
      return parsed.model.trim();
    }
  } catch (_e) {
    // settings missing or malformed — fall through
  }
  return fallback;
}

// Recursively collect every *.jsonl under ~/.workbuddy/projects so that
// per-session conversation logs AND their nested subagents/agent-*.jsonl files
// are both discovered. WorkBuddy also writes completed trace summaries under
// ~/.workbuddy/traces/<pid>/trace_*.json. Those summaries are useful only when
// the detailed JSONL for a session is absent, so they are returned as typed
// entries and parsed after JSONL (JSONL remains authoritative when both exist).
function resolveWorkbuddyProjectFiles(env = process.env, variant = "workbuddy") {
  const workbuddyHome = resolveWorkbuddyHome(env, variant);
  if (!workbuddyHome) return [];
  const projectsDir = path.join(workbuddyHome, "projects");
  const files = [];
  const walkJsonl = (dir) => {
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
      if (isDir) walkJsonl(full);
      else if (isFile && entry.name.endsWith(".jsonl")) files.push(full);
    }
  };
  if (fssync.existsSync(projectsDir)) walkJsonl(projectsDir);

  const tracesDir = path.join(workbuddyHome, "traces");
  const walkTraces = (dir) => {
    let entries;
    try { entries = fssync.readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const entry of entries) {
      const full = path.join(dir, entry.name);
      let isDir = entry.isDirectory();
      let isFile = entry.isFile();
      if (!isDir && !isFile) {
        try {
          const st = fssync.statSync(full);
          isDir = st.isDirectory();
          isFile = st.isFile();
        } catch { continue; }
      }
      if (isDir) walkTraces(full);
      else if (isFile && /^trace_[^/]+\.json$/i.test(entry.name)) {
        files.push({ path: full, kind: "trace" });
      }
    }
  };
  if (fssync.existsSync(tracesDir)) walkTraces(tracesDir);
  files.sort((a, b) => {
    const aPath = typeof a === "string" ? a : a?.path || "";
    const bPath = typeof b === "string" ? b : b?.path || "";
    // JSONL first is intentional: trace summaries are a lossy fallback and
    // must not win over a detailed rawUsage record on the same session.
    const aKind = typeof a === "string" ? 0 : 1;
    const bKind = typeof b === "string" ? 0 : 1;
    return aKind - bKind || aPath.localeCompare(bPath);
  });
  return files;
}

// WorkBuddy's current session_usage table stores context-window state in
// `used`/`size` and model credit dollars in `credit_json`; neither is a token
// accounting breakdown. Only consume a SQLite row when a future schema (or a
// newer installation) exposes explicit cumulative token columns. This keeps
// the fallback forward-compatible without treating a context snapshot as
// billable usage.
function workbuddySqliteUsageSnapshot(row) {
  if (!row || typeof row !== "object") return null;
  const hasAny = (keys) => keys.some((key) => Object.prototype.hasOwnProperty.call(row, key));
  const inputKeys = ["input_tokens", "inputTokens", "prompt_tokens", "promptTokens"];
  const cacheReadKeys = [
    "cached_input_tokens",
    "cachedInputTokens",
    "cache_read_input_tokens",
    "cacheReadInputTokens",
    "cache_read_tokens",
    "cacheReadTokens",
  ];
  const cacheCreationKeys = [
    "cache_creation_input_tokens",
    "cacheCreationInputTokens",
    "prompt_cache_write_tokens",
    "promptCacheWriteTokens",
  ];
  const outputKeys = ["output_tokens", "outputTokens", "completion_tokens", "completionTokens"];
  const reasoningKeys = [
    "reasoning_output_tokens",
    "reasoningOutputTokens",
    "reasoning_tokens",
    "reasoningTokens",
  ];
  if (!hasAny([...inputKeys, ...cacheReadKeys, ...cacheCreationKeys, ...outputKeys, ...reasoningKeys])) {
    return null;
  }
  return {
    input: firstPresentNonNegativeInt(inputKeys.map((key) => row[key])),
    cacheRead: firstPresentNonNegativeInt(cacheReadKeys.map((key) => row[key])),
    cacheCreation: firstPresentNonNegativeInt(cacheCreationKeys.map((key) => row[key])),
    output: firstPresentNonNegativeInt(outputKeys.map((key) => row[key])),
    reasoning: firstPresentNonNegativeInt(reasoningKeys.map((key) => row[key])),
  };
}

// WorkBuddy writes OpenAI/DeepSeek-shaped rawUsage on every LLM round-trip.
// The totals pass and the project pass below need the same breakdown, so the
// arithmetic lives here once. Callers treat total_tokens === 0 as "no billable
// usage": every component is non-negative, so that is equivalent to testing
// each field for zero.
function workbuddyUsageDelta(rawUsage) {
  const promptTokens = toNonNegativeInt(rawUsage.prompt_tokens);
  const completionTokens = toNonNegativeInt(rawUsage.completion_tokens);
  const promptDetails =
    rawUsage.prompt_tokens_details && typeof rawUsage.prompt_tokens_details === "object"
      ? rawUsage.prompt_tokens_details
      : {};
  const completionDetails =
    rawUsage.completion_tokens_details && typeof rawUsage.completion_tokens_details === "object"
      ? rawUsage.completion_tokens_details
      : {};

  // Cache reads are mirrored across up to three fields depending on which
  // upstream the auto-router used; take the largest non-zero mirror.
  const cacheRead = Math.max(
    toNonNegativeInt(rawUsage.cache_read_input_tokens),
    toNonNegativeInt(promptDetails.cached_tokens),
    toNonNegativeInt(rawUsage.prompt_cache_hit_tokens),
  );
  const cacheCreation = toNonNegativeInt(rawUsage.cache_creation_input_tokens);
  // prompt_tokens is the FULL prompt: subtract BOTH reads and writes so
  // input_tokens is pure non-cached input (no double-counting cache writes).
  const inputTokens = Math.max(0, promptTokens - cacheRead - cacheCreation);
  // completion_tokens INCLUDES reasoning (verified: total == prompt+completion).
  const reasoningTokens = Math.min(
    completionTokens,
    toNonNegativeInt(completionDetails.reasoning_tokens),
  );
  const outputTokens = Math.max(0, completionTokens - reasoningTokens);

  return {
    input_tokens: inputTokens,
    cached_input_tokens: cacheRead,
    cache_creation_input_tokens: cacheCreation,
    output_tokens: outputTokens,
    reasoning_output_tokens: reasoningTokens,
    total_tokens: inputTokens + outputTokens + cacheRead + cacheCreation + reasoningTokens,
    conversation_count: 1,
  };
}

// WorkBuddy stamps `cwd` on its records rather than in a session header, and a
// session can open with a record that lacks it while later appends omit it
// altogether. Resolve it from a bounded prefix of the file so every record in
// the incremental window is attributed, not just the ones that happen to carry
// their own cwd. A file with no recoverable cwd stays outside project
// attribution — there is nothing to attribute it to.
async function resolveWorkbuddyFileCwd(filePath, maxScanBytes = 8 * 1024 * 1024) {
  let stream;
  try {
    const stat = fssync.statSync(filePath);
    if (stat.size <= 0) return null;
    stream = fssync.createReadStream(filePath, {
      start: 0,
      end: Math.min(stat.size, maxScanBytes) - 1,
    });
  } catch {
    return null;
  }
  let cwd = null;
  try {
    for await (const record of physicalJsonlRecords(stream, {
      invalidUtf8: "record",
      maxRecordBytes: OMP_LIKE_MAX_RECORD_BYTES,
    })) {
      if (!record.line || !record.line.trim()) continue;
      let entry;
      try { entry = JSON.parse(record.line); } catch { continue; }
      if (entry && typeof entry === "object" && typeof entry.cwd === "string" && entry.cwd) {
        cwd = entry.cwd;
        break;
      }
    }
  } catch {
    // No recoverable cwd — the file stays un-attributed.
  } finally {
    try { stream.destroy(); } catch {}
  }
  return cwd;
}

async function parseWorkbuddyIncremental({
  projectFiles,
  cursors,
  queuePath,
  projectQueuePath,
  onProgress,
  env,
  defaultModel,
  source = "workbuddy",
  publicRepoResolver,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  // `source` doubles as the variant selector and the bucket source key, so the
  // CN install (~/.workbuddy) and the international install (~/.workbuddy-ai)
  // get independent cursor namespaces and never share dedup state.
  const variant = resolveWorkbuddyVariant(source);
  const workbuddyState =
    cursors[variant] && typeof cursors[variant] === "object" ? cursors[variant] : {};
  const seenIds = new Set(
    Array.isArray(workbuddyState.seenIds) ? workbuddyState.seenIds : [],
  );
  const fileOffsets =
    workbuddyState.fileOffsets && typeof workbuddyState.fileOffsets === "object"
      ? { ...workbuddyState.fileOffsets }
      : {};
  const sqliteSessions =
    workbuddyState.sqliteSessions && typeof workbuddyState.sqliteSessions === "object"
      ? { ...workbuddyState.sqliteSessions }
      : {};
  const detailedSessions =
    workbuddyState.detailedSessions && typeof workbuddyState.detailedSessions === "object"
      ? { ...workbuddyState.detailedSessions }
      : {};
  const seenTraceIds = new Set(
    Array.isArray(workbuddyState.seenTraceIds) ? workbuddyState.seenTraceIds : [],
  );
  const tracedSessionIds = new Set(
    Array.isArray(workbuddyState.tracedSessionIds) ? workbuddyState.tracedSessionIds : [],
  );
  const detailedSessionsWithUsage = new Set();

  // The shared normalizers copy the bucket maps but alias each bucket/totals
  // object, and the enqueue helpers stamp queuedKey before appendFile runs.
  // Give the buckets this parser can mutate private copies so a failed append
  // leaves the caller's published state untouched — the same staging contract
  // the Devin reader uses. Callers retry from unchanged offsets, so a polluted
  // published state would add the same usage twice.
  const ownsWorkbuddyBucket = (key) =>
    (normalizeSourceInput(key.split(BUCKET_SEPARATOR)[0]) || DEFAULT_SOURCE) === variant;
  const ownsWorkbuddyProjectBucket = (key) => {
    // Project keys are `projectKey|source|hour`; a projectKey may itself
    // contain the separator, so read the source segment from the right.
    const last = key.lastIndexOf(BUCKET_SEPARATOR);
    const prev = last > 0 ? key.lastIndexOf(BUCKET_SEPARATOR, last - 1) : -1;
    const source = prev >= 0 ? key.slice(prev + 1, last) : "";
    return (normalizeSourceInput(source) || DEFAULT_SOURCE) === variant;
  };

  // Project attribution runs as a second, independent pass over the same JSONL
  // files (see "Project pass" below). It keeps its own offsets and seen-set, so
  // an install that already has totals backfills project history on upgrade
  // without re-counting a single token in the totals queue.
  const projectEnabled = typeof projectQueuePath === "string" && projectQueuePath.length > 0;
  const projectState = projectEnabled ? normalizeProjectState(cursors?.projectHourly) : null;
  if (projectState) {
    projectState.buckets = stageProviderBuckets(projectState.buckets, ownsWorkbuddyProjectBucket);
  }
  const projectTouchedBuckets = projectEnabled ? new Set() : null;
  const projectMetaCache = projectEnabled ? new Map() : null;
  const publicRepoCache = projectEnabled ? new Map() : null;
  const projectSeenIds = new Set(
    Array.isArray(workbuddyState.projectSeenIds) ? workbuddyState.projectSeenIds : [],
  );
  const projectFileOffsets =
    workbuddyState.projectFileOffsets && typeof workbuddyState.projectFileOffsets === "object"
      ? { ...workbuddyState.projectFileOffsets }
      : {};
  let projectBucketsQueued = 0;
  let projectCursorUpdated = false;

  const allFiles = Array.isArray(projectFiles)
    ? projectFiles
    : resolveWorkbuddyProjectFiles(env || process.env, variant);
  const jsonlFiles = allFiles.filter((entry) => typeof entry === "string");
  const traceFiles = allFiles.filter(
    (entry) => entry && typeof entry === "object" && typeof entry.path === "string" && entry.kind === "trace",
  );
  const files = [...jsonlFiles, ...traceFiles];
  const fallbackModel = defaultModel || resolveWorkbuddyDefaultModel(env || process.env, variant);

  const workbuddyHome = resolveWorkbuddyHome(env || process.env, variant);
  const dbPath = workbuddyHome ? path.join(workbuddyHome, "workbuddy.db") : null;
  const dbExists = Boolean(dbPath && fssync.existsSync(dbPath));

  if (files.length === 0 && !dbExists) {
    cursors[variant] = {
      ...workbuddyState,
      seenIds: Array.from(seenIds),
      seenTraceIds: Array.from(seenTraceIds),
      tracedSessionIds: Array.from(tracedSessionIds),
      fileOffsets,
      sqliteSessions,
      detailedSessions,
      updatedAt: new Date().toISOString(),
    };
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0, projectBucketsQueued: 0, projectCursorUpdated: false };
  }

  const hourlyState = normalizeHourlyState(cursors?.hourly);
  hourlyState.buckets = stageProviderBuckets(hourlyState.buckets, ownsWorkbuddyBucket);
  hourlyState.groupQueued =
    hourlyState.groupQueued && typeof hourlyState.groupQueued === "object"
      ? { ...hourlyState.groupQueued }
      : {};
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;
  let recordsProcessed = 0;
  let eventsAggregated = 0;

  for (let fileIdx = 0; fileIdx < jsonlFiles.length; fileIdx++) {
    const filePath = jsonlFiles[fileIdx];
    let stat;
    try { stat = fssync.statSync(filePath); } catch { continue; }

    const prevEntry = fileOffsets[filePath] || {};
    const prevSize = Number(prevEntry.size) || 0;
    const prevIno = prevEntry.ino;
    // Re-read from start if file shrunk (truncate/rewrite) or inode changed
    // (file deleted + recreated). Otherwise pick up after the last read offset.
    const inodeChanged = typeof prevIno === "number" && prevIno !== stat.ino;
    const startOffset = stat.size < prevSize || inodeChanged ? 0 : prevSize;
    if (stat.size <= startOffset) continue;

    let stream;
    try {
      stream = fssync.createReadStream(filePath, {
        encoding: "utf8",
        start: startOffset,
      });
    } catch { continue; }
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

    for await (const line of rl) {
      if (!line || !line.trim()) continue;
      let entry;
      try { entry = JSON.parse(line); } catch { continue; }
      if (!entry || typeof entry !== "object") continue;

      // Usage is carried on ANY record with providerData.rawUsage — assistant
      // messages AND function_call records. Aggregate them all; dedup per id.
      const provider = entry.providerData;
      const rawUsage = provider && typeof provider === "object" ? provider.rawUsage : null;
      if (!rawUsage || typeof rawUsage !== "object") continue;

      const sessionId =
        typeof entry.sessionId === "string" && entry.sessionId
          ? entry.sessionId
          : path.basename(filePath, ".jsonl");
      const sqliteSession = sqliteSessions[sessionId];
      if (sqliteSession?.detailed || tracedSessionIds.has(sessionId)) {
        continue;
      }
      const tsMs =
        Number.isFinite(Number(entry.timestamp)) && Number(entry.timestamp) > 0
          ? Number(entry.timestamp)
          : null;
      // One usage record per LLM round-trip; providerData.messageId is the
      // response-level id shared by function_call/message records and must win
      // over the per-record append id. Fall back to entry.uuid/id only when the
      // response-level id is absent.
      const messageId =
        typeof provider?.messageId === "string" && provider.messageId
            ? provider.messageId
            : typeof entry.uuid === "string" && entry.uuid
              ? entry.uuid
              : typeof entry.id === "string" && entry.id
                ? entry.id
                : tsMs != null
                  ? `${sessionId}:${tsMs}`
                  : null;
      if (!messageId) continue;
      if (seenIds.has(messageId)) continue;

      recordsProcessed++;

      const delta = workbuddyUsageDelta(rawUsage);
      if (delta.total_tokens === 0) {
        seenIds.add(messageId);
        continue;
      }

      if (tsMs == null) {
        seenIds.add(messageId);
        continue;
      }
      const tsIso = new Date(tsMs).toISOString();
      const bucketStart = toUtcHalfHourStart(tsIso);
      if (!bucketStart) continue;

      const model =
        normalizeModelInput(provider.model) ||
        normalizeModelInput(provider.requestModelId) ||
        normalizeModelInput(entry.model) ||
        fallbackModel;

      const bucket = getHourlyBucket(hourlyState, variant, model, bucketStart);
      addTotals(bucket.totals, delta);
      touchedBuckets.add(bucketKey(variant, model, bucketStart));
      seenIds.add(messageId);
      // Keep legacy entry-id keys alongside the response id so upgrading a
      // cursor cannot replay a row that an older build already counted.
      if (typeof entry.id === "string" && entry.id && entry.id !== messageId) {
        seenIds.add(entry.id);
      }
      detailedSessions[sessionId] = true;
      detailedSessionsWithUsage.add(sessionId);
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
    }

    let postStat = stat;
    try { postStat = fssync.statSync(filePath); } catch {}
    fileOffsets[filePath] = {
      size: postStat.size,
      mtimeMs: postStat.mtimeMs,
      ino: postStat.ino,
    };
  }

  // ── Project pass ───────────────────────────────────────────────────────
  // WorkBuddy stamps `cwd` on its records, so unlike the trace summaries below
  // there is nothing to infer: attribute each round-trip to the git repository
  // its cwd belongs to. This is a second loop over the same files with its own
  // cursor, so an install that already has totals backfills project history on
  // upgrade without touching (or double-counting) them.
  if (projectEnabled) {
    for (let fileIdx = 0; fileIdx < jsonlFiles.length; fileIdx++) {
      const filePath = jsonlFiles[fileIdx];
      let stat;
      try { stat = fssync.statSync(filePath); } catch { continue; }

      const prevEntry = projectFileOffsets[filePath] || {};
      const prevSize = Number(prevEntry.size) || 0;
      const prevIno = prevEntry.ino;
      const inodeChanged = typeof prevIno === "number" && prevIno !== stat.ino;
      const truncated = stat.size < prevSize;
      const startOffset = truncated || inodeChanged ? 0 : prevSize;
      if (stat.size <= startOffset) continue;

      // cwd is fixed for a session file's lifetime. Reuse the resolved value
      // while it is the same file, otherwise read it from the file head — not
      // from the incremental window, which may hold only appends that omit it.
      // Truncation (same inode, smaller size) must drop the cached cwd: the
      // replacement transcript may belong to a different checkout.
      let cwd =
        !inodeChanged && !truncated && typeof prevEntry.cwd === "string"
          ? prevEntry.cwd
          : null;

      // No recoverable cwd yet: do not advance past history. A later append
      // may stamp cwd, and early records must still be attributable then.
      // Check the unchanged-probe cache BEFORE the head scan so idle syncs do
      // not re-read up to 8 MiB of JSONL on every pass.
      if (
        !cwd &&
        Number(prevEntry.noCwdProbedSize) === stat.size &&
        prevEntry.ino === stat.ino &&
        Number(prevEntry.size) === startOffset
      ) {
        continue;
      }
      if (!cwd) cwd = await resolveWorkbuddyFileCwd(filePath);
      if (!cwd) {
        projectCursorUpdated = true;
        projectFileOffsets[filePath] = {
          size: startOffset,
          mtimeMs: stat.mtimeMs,
          ino: stat.ino,
          noCwdProbedSize: stat.size,
        };
        continue;
      }

      let projectRef = null;
      let projectKey = null;
      const projectContext = await resolveProjectContextForPath({
        startDir: wsl.mapWslCwdToUnc(cwd, filePath),
        projectMetaCache,
        publicRepoCache,
        publicRepoResolver,
        projectState,
      });
      projectRef = projectContext?.projectRef || null;
      projectKey = projectContext?.projectKey || null;

      // cwd resolved but the repo is not yet public_verified (or has no git
      // checkout): keep the offset unadvanced so a later sync can retry, but
      // do not JSON-parse the transcript on every pass — there is nothing to
      // attribute until both identifiers exist.
      if (!projectKey || !projectRef) {
        projectCursorUpdated = true;
        projectFileOffsets[filePath] = {
          size: startOffset,
          mtimeMs: stat.mtimeMs,
          ino: stat.ino,
          cwd,
        };
        continue;
      }

      const committedEndOffset = await scanOmpLikeTranscript(
        filePath,
        startOffset,
        OMP_LIKE_MAX_RECORD_BYTES,
        (entry) => {
          // Mirror the totals pass: a JSON null / non-object line must not
          // abort the whole WorkBuddy parse (and strand both cursors).
          if (!entry || typeof entry !== "object") return;

          const provider = entry.providerData;
          const rawUsage = provider && typeof provider === "object" ? provider.rawUsage : null;
          if (!rawUsage || typeof rawUsage !== "object") return;

          // Mirror the totals pass's suppression rules so the project queue
          // never reports a session the totals queue deliberately skipped.
          const sessionId =
            typeof entry.sessionId === "string" && entry.sessionId
              ? entry.sessionId
              : path.basename(filePath, ".jsonl");
          const sqliteSession = sqliteSessions[sessionId];
          if (sqliteSession?.detailed || tracedSessionIds.has(sessionId)) return;

          const tsMs =
            Number.isFinite(Number(entry.timestamp)) && Number(entry.timestamp) > 0
              ? Number(entry.timestamp)
              : null;
          if (tsMs == null) return;

          // Same response-level id preference as the totals pass, so both
          // queues dedup on identical keys.
          const messageId =
            typeof provider?.messageId === "string" && provider.messageId
              ? provider.messageId
              : typeof entry.uuid === "string" && entry.uuid
                ? entry.uuid
                : typeof entry.id === "string" && entry.id
                  ? entry.id
                  : `${sessionId}:${tsMs}`;
          if (projectSeenIds.has(messageId)) return;
          projectSeenIds.add(messageId);
          // The totals pass also registers the raw entry id so an older or
          // rewritten copy of the same record cannot be counted twice; the
          // project pass has to keep the same alias.
          if (typeof entry.id === "string" && entry.id && entry.id !== messageId) {
            projectSeenIds.add(entry.id);
          }

          const delta = workbuddyUsageDelta(rawUsage);
          if (delta.total_tokens === 0) return;

          const bucketStart = toUtcHalfHourStart(new Date(tsMs).toISOString());
          if (!bucketStart) return;

          const projectBucket = getProjectBucket(
            projectState,
            projectKey,
            source,
            bucketStart,
            projectRef,
          );
          addTotals(projectBucket.totals, delta);
          projectTouchedBuckets.add(projectBucketKey(projectKey, source, bucketStart));
        },
      );

      // Commit only past records that were read in full: a record appended
      // while this pass was running, or an unfinished tail line, has to stay
      // visible to the next sync. Only advance after successful attribution.
      projectCursorUpdated = true;
      projectFileOffsets[filePath] = {
        size: committedEndOffset,
        mtimeMs: stat.mtimeMs,
        ino: stat.ino,
        cwd,
      };
    }
  }

  // Trace summaries are a fallback for sessions whose JSONL has no detailed
  // providerData.rawUsage. A trace's modelInfo totals include the cache read
  // portion of input, so split input into uncached input + cached input. Do
  // not merge a trace with the same session's JSONL (or SQLite) data: the
  // JSONL path has per-round detail and the trace summary can be partial on
  // older WorkBuddy versions.
  for (let fileIdx = 0; fileIdx < traceFiles.length; fileIdx++) {
    const entry = traceFiles[fileIdx];
    const filePath = entry.path;
    let stat;
    let traceDoc;
    let traceFd;
    try {
      traceFd = fssync.openSync(filePath, "r");
      stat = fssync.fstatSync(traceFd);
      if (!stat.isFile()) continue;
      const prevEntry = fileOffsets[filePath] || {};
      const prevSize = Number(prevEntry.size) || 0;
      const prevIno = prevEntry.ino;
      const inodeChanged = typeof prevIno === "number" && prevIno !== stat.ino;
      const startOffset = stat.size < prevSize || inodeChanged ? 0 : prevSize;
      if (stat.size <= startOffset) continue;
      traceDoc = JSON.parse(fssync.readFileSync(traceFd, "utf8"));
    } catch {
      continue;
    } finally {
      if (traceFd !== undefined) fssync.closeSync(traceFd);
    }
    const trace = traceDoc && typeof traceDoc === "object" && traceDoc.trace && typeof traceDoc.trace === "object"
      ? traceDoc.trace
      : null;
    const traceId = typeof trace?.traceId === "string" && trace.traceId
      ? trace.traceId
      : path.basename(filePath, ".json");
    const metadata = trace?.metadata && typeof trace.metadata === "object" ? trace.metadata : {};
    const modelInfo = trace?.modelInfo && typeof trace.modelInfo === "object"
      ? trace.modelInfo
      : metadata.modelInfo && typeof metadata.modelInfo === "object"
        ? metadata.modelInfo
        : {};
    const sessionId =
      (typeof trace?.sessionId === "string" && trace.sessionId) ||
      (typeof metadata.sessionId === "string" && metadata.sessionId) ||
      traceId;
    const startedAtRaw = trace?.startedAt || metadata.startedAt;
    const tsMs = typeof startedAtRaw === "number" && Number.isFinite(startedAtRaw)
      ? (startedAtRaw > 10000000000 ? startedAtRaw : startedAtRaw * 1000)
      : Date.parse(String(startedAtRaw || ""));
    const totalInput = toNonNegativeInt(modelInfo.totalInputTokens);
    const totalOutput = toNonNegativeInt(modelInfo.totalOutputTokens);
    const totalCached = Math.min(totalInput, toNonNegativeInt(modelInfo.totalCachedTokens));
    if (seenTraceIds.has(traceId) || detailedSessions[sessionId] || detailedSessionsWithUsage.has(sessionId) || tracedSessionIds.has(sessionId)) {
      seenTraceIds.add(traceId);
      fileOffsets[filePath] = { size: stat.size, mtimeMs: stat.mtimeMs, ino: stat.ino };
      continue;
    }
    if (!Number.isFinite(tsMs) || tsMs <= 0 || (totalInput <= 0 && totalOutput <= 0)) {
      // Keep the file offset but do not permanently suppress a trace that may
      // still be finalized in place by WorkBuddy.
      fileOffsets[filePath] = { size: stat.size, mtimeMs: stat.mtimeMs, ino: stat.ino };
      continue;
    }

    const bucketStart = toUtcHalfHourStart(new Date(tsMs).toISOString());
    if (!bucketStart) continue;
    const model =
      normalizeModelInput(Array.isArray(modelInfo.models) ? modelInfo.models[0] : modelInfo.model) ||
      fallbackModel;
    const inputTokens = Math.max(0, totalInput - totalCached);
    const delta = {
      input_tokens: inputTokens,
      cached_input_tokens: totalCached,
      cache_creation_input_tokens: 0,
      output_tokens: totalOutput,
      reasoning_output_tokens: 0,
      total_tokens: inputTokens + totalCached + totalOutput,
      conversation_count: 1,
    };
    const bucket = getHourlyBucket(hourlyState, variant, model, bucketStart);
    addTotals(bucket.totals, delta);
    touchedBuckets.add(bucketKey(variant, model, bucketStart));
    seenTraceIds.add(traceId);
    tracedSessionIds.add(sessionId);
    recordsProcessed++;
    eventsAggregated++;
    fileOffsets[filePath] = { size: stat.size, mtimeMs: stat.mtimeMs, ino: stat.ino };
    if (cb) {
      cb({
        index: jsonlFiles.length + fileIdx + 1,
        total: files.length,
        recordsProcessed,
        eventsAggregated,
        bucketsQueued: touchedBuckets.size,
      });
    }
  }

  if (dbExists) {
    const query = `
      SELECT
        su.*,
        s.model AS session_model,
        s.cwd AS session_cwd
      FROM session_usage su
      LEFT JOIN sessions s ON s.id = su.session_id
      WHERE su.used IS NOT NULL
        AND su.used > 0
        AND su.updated_at IS NOT NULL
        AND su.updated_at > 0
    `.trim();
    let rows = [];
    const snap = snapshotSqliteDb(dbPath);
    try {
      rows = await readSqliteJsonRowsAsync(snap.path, query, {
        label: "WorkBuddy",
        timeout: 10_000,
        maxBuffer: 16 * 1024 * 1024,
      });
    } finally {
      snap.cleanup();
    }

    try {
      for (const row of rows) {
        if (!row || typeof row !== "object") continue;
        const sessionId = typeof row.session_id === "string" ? row.session_id.trim() : "";
        if (!sessionId) continue;
        if (
          detailedSessions[sessionId] ||
          detailedSessionsWithUsage.has(sessionId) ||
          tracedSessionIds.has(sessionId)
        ) continue;

        const updatedAtRaw = toNonNegativeInt(row.updated_at);
        const rawModel = typeof row.session_model === "string"
          ? row.session_model.trim()
          : typeof row.model === "string" ? row.model.trim() : "";

        if (updatedAtRaw <= 0) continue;

        const snapshot = workbuddySqliteUsageSnapshot(row);
        const usedNow = toNonNegativeInt(row.used);
        const prev = sqliteSessions[sessionId] || {};

        // Current WorkBuddy releases expose only context state (`used` versus
        // `size`) here. Do not turn that bounded snapshot into a fake usage
        // delta; detailed JSONL/trace sources remain the authoritative path.
        if (!snapshot) {
          sqliteSessions[sessionId] = {
            ...prev,
            used: usedNow,
            updatedAt: updatedAtRaw,
            model: rawModel || prev.model || fallbackModel,
            detailed: false,
          };
          continue;
        }

        const previousTokens = prev.tokens && typeof prev.tokens === "object"
          ? prev.tokens
          : { input: 0, cacheRead: 0, cacheCreation: 0, output: 0, reasoning: 0 };
        const isReset = ["input", "cacheRead", "cacheCreation", "output", "reasoning"]
          .some((key) => snapshot[key] < toNonNegativeInt(previousTokens[key]));
        const deltaTokens = {
          input: isReset ? snapshot.input : Math.max(0, snapshot.input - toNonNegativeInt(previousTokens.input)),
          cacheRead: isReset ? snapshot.cacheRead : Math.max(0, snapshot.cacheRead - toNonNegativeInt(previousTokens.cacheRead)),
          cacheCreation: isReset ? snapshot.cacheCreation : Math.max(0, snapshot.cacheCreation - toNonNegativeInt(previousTokens.cacheCreation)),
          output: isReset ? snapshot.output : Math.max(0, snapshot.output - toNonNegativeInt(previousTokens.output)),
          reasoning: isReset ? snapshot.reasoning : Math.max(0, snapshot.reasoning - toNonNegativeInt(previousTokens.reasoning)),
        };
        if (
          deltaTokens.input === 0 &&
          deltaTokens.cacheRead === 0 &&
          deltaTokens.cacheCreation === 0 &&
          deltaTokens.output === 0 &&
          deltaTokens.reasoning === 0
        ) {
          sqliteSessions[sessionId] = {
            ...prev,
            used: usedNow,
            updatedAt: updatedAtRaw,
            model: rawModel || prev.model || fallbackModel,
            tokens: snapshot,
            detailed: true,
          };
          continue;
        }

        recordsProcessed++;

        const inputTokens = deltaTokens.input;
        const completionTokens = deltaTokens.output;
        const cacheRead = deltaTokens.cacheRead;
        const cacheCreation = deltaTokens.cacheCreation;
        const reasoningTokens = deltaTokens.reasoning;

        const tsMs = updatedAtRaw > 10000000000 ? updatedAtRaw : updatedAtRaw * 1000;
        const tsIso = new Date(tsMs).toISOString();
        const bucketStart = toUtcHalfHourStart(tsIso);
        if (!bucketStart) continue;

        const model = normalizeModelInput(rawModel) || fallbackModel;

        const delta = {
          input_tokens: inputTokens,
          cached_input_tokens: cacheRead,
          cache_creation_input_tokens: cacheCreation,
          output_tokens: completionTokens,
          reasoning_output_tokens: reasoningTokens,
        total_tokens: inputTokens + completionTokens + cacheRead + cacheCreation + reasoningTokens,
          conversation_count: Object.keys(previousTokens).every((key) => toNonNegativeInt(previousTokens[key]) === 0) || isReset ? 1 : 0,
        };

        const bucket = getHourlyBucket(hourlyState, variant, model, bucketStart);
        addTotals(bucket.totals, delta);
        touchedBuckets.add(bucketKey(variant, model, bucketStart));
        sqliteSessions[sessionId] = {
          used: usedNow,
          updatedAt: updatedAtRaw,
          model,
          tokens: snapshot,
          detailed: true,
        };
        eventsAggregated++;
      }
    } catch (err) {
      // SQLite fallback is best effort; detailed JSONL remains authoritative.
    }
  }

  // Cap dedup set to last 10k IDs to bound cursor state size — same convention
  // as CodeBuddy/Kimi/Copilot so cursors.json doesn't grow unbounded.
  const seenArr = Array.from(seenIds);
  const cappedSeen =
    seenArr.length > 10_000 ? seenArr.slice(seenArr.length - 10_000) : seenArr;
  const sqliteSessionEntries = Object.entries(sqliteSessions);
  const cappedSqliteSessions =
    sqliteSessionEntries.length > 10_000
      ? Object.fromEntries(
          sqliteSessionEntries
            .sort((a, b) => toNonNegativeInt(b[1]?.used) - toNonNegativeInt(a[1]?.used))
            .slice(0, 10_000),
        )
      : sqliteSessions;
  const detailedSessionEntries = Object.entries(detailedSessions);
  const cappedDetailedSessions =
    detailedSessionEntries.length > 10_000
      ? Object.fromEntries(detailedSessionEntries.slice(detailedSessionEntries.length - 10_000))
      : detailedSessions;
  const traceArr = Array.from(seenTraceIds);
  const cappedSeenTraceIds =
    traceArr.length > 10_000 ? traceArr.slice(traceArr.length - 10_000) : traceArr;
  const tracedSessionArr = Array.from(tracedSessionIds);
  const cappedTracedSessionIds =
    tracedSessionArr.length > 10_000
      ? tracedSessionArr.slice(tracedSessionArr.length - 10_000)
      : tracedSessionArr;

  const projectSeenArr = Array.from(projectSeenIds);
  const cappedProjectSeenIds =
    projectSeenArr.length > 10_000
      ? projectSeenArr.slice(projectSeenArr.length - 10_000)
      : projectSeenArr;

  // Publish only after both appends succeed: a throw here leaves `cursors`
  // untouched, so a retry re-derives the same contribution from the same
  // offsets instead of adding it on top of a half-applied state.
  projectBucketsQueued = projectEnabled
    ? await enqueueTouchedProjectBuckets({
        projectQueuePath,
        projectState,
        projectTouchedBuckets,
      })
    : 0;

  const bucketsQueued = await enqueueTouchedBuckets({
    queuePath,
    hourlyState,
    touchedBuckets,
  });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  if (projectState) {
    projectState.updatedAt = updatedAt;
    cursors.projectHourly = projectState;
  }
  cursors[variant] = {
    ...workbuddyState,
    seenIds: cappedSeen,
    seenTraceIds: cappedSeenTraceIds,
    tracedSessionIds: cappedTracedSessionIds,
    fileOffsets,
    sqliteSessions: cappedSqliteSessions,
    detailedSessions: cappedDetailedSessions,
    ...(projectEnabled
      ? {
          projectSeenIds: cappedProjectSeenIds,
          projectFileOffsets,
        }
      : {}),
    updatedAt,
  };

  return {
    recordsProcessed,
    eventsAggregated,
    bucketsQueued,
    projectBucketsQueued,
    projectCursorUpdated: projectEnabled && projectCursorUpdated,
  };
}

module.exports = {
  resolveCodebuddyHome,
  resolveCodebuddyDefaultModel,
  codebuddyJsonlHasUsage,
  resolveCodebuddyProjectFiles,
  parseCodebuddyIncremental,
  WORKBUDDY_VARIANTS,
  resolveWorkbuddyVariant,
  resolveWorkbuddyHome,
  resolveWorkbuddyDefaultModel,
  resolveWorkbuddyProjectFiles,
  parseWorkbuddyIncremental,
};
