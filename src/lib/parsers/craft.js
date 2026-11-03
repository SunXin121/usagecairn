// craft usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const readline = require("node:readline");
const { ensureDir } = require("../fs");
const {
  enqueueTouchedBuckets,
  normalizeHourlyState,
  getHourlyBucket,
  addTotals,
  toUtcHalfHourStart,
  bucketKey,
  normalizeModelInput,
  toNonNegativeInt,
  pickWin32ProviderPath,
} = require("./core");

// ─────────────────────────────────────────────────────────────────────────────
// Craft Agents (lukilabs/craft-agents-oss) — passive JSONL reader
//
// Craft is a desktop Electron agent that wraps the Claude Agent SDK plus
// multiple LLM backends (Anthropic, OpenAI, Google, GitHub Copilot, OpenRouter,
// Groq, Mistral, DeepSeek, xAI, Bedrock, Vertex). It writes per-session JSONL
// files with a pre-aggregated SessionTokenUsage block on the FIRST line:
//
//   line 1: SessionHeader
//     {
//       "id": "260430-swift-river",
//       "model": "claude-sonnet-4-6",
//       "llmConnection": "anthropic-default",
//       "lastMessageAt": 1745003600000,
//       "tokenUsage": {
//         "inputTokens": 1234,            ← pure non-cached input
//         "outputTokens": 567,
//         "totalTokens": 9876,
//         "cacheReadTokens": 5500,
//         "cacheCreationTokens": 1100
//       }
//     }
//   line 2..N: StoredMessage records (we do not need them for token totals)
//
// Disk layout:
//   ~/.craft-agent/                    ← config dir (override: CRAFT_CONFIG_DIR)
//     config.json                      ← workspaces[].rootPath list
//     workspaces/<id>/sessions/<sid>/session.jsonl  (default)
//   <user-chosen-rootPath>/sessions/<sid>/session.jsonl  (custom workspaces)
//
// Workspaces can be relocated outside ~/.craft-agent, so we MUST read
// config.json to enumerate every rootPath rather than just globbing the
// default directory.
//
// Token semantics map directly onto UsageCairn conventions — `inputTokens`
// is already pure non-cached input (no Codex-style trap, see
// feedback_rollout_input_semantics.md). Re-parses are idempotent: the header
// is rewritten as the session grows, and we dedup by sessionId combined with
// the most-recent header byte length so a growing total replaces the old
// snapshot instead of double-counting.
// ─────────────────────────────────────────────────────────────────────────────

function resolveCraftConfigDir(env = process.env) {
  if (env.CRAFT_CONFIG_DIR) return env.CRAFT_CONFIG_DIR;
  const home = env.HOME || os.homedir();
  if (process.platform === "win32") {
    return pickWin32ProviderPath({
      env,
      nativeValue: path.join(home, ".craft-agent"),
      wslProviderDir: ".craft-agent",
    });
  }
  return path.join(home, ".craft-agent");
}

function resolveCraftWorkspaceRoots(env = process.env) {
  const configDir = resolveCraftConfigDir(env);
  if (!configDir) return [];
  const roots = new Set();
  // Always include the default workspaces directory so a fresh install (no
  // config.json yet) still gets discovered.
  const defaultWorkspaces = path.join(configDir, "workspaces");
  if (fssync.existsSync(defaultWorkspaces)) {
    try {
      for (const entry of fssync.readdirSync(defaultWorkspaces)) {
        const wsPath = path.join(defaultWorkspaces, entry);
        let stat;
        try { stat = fssync.statSync(wsPath); } catch { continue; }
        if (stat.isDirectory()) roots.add(wsPath);
      }
    } catch {
      // ignore
    }
  }
  // Layer in user-relocated workspaces from config.json.
  const configPath = path.join(configDir, "config.json");
  if (fssync.existsSync(configPath)) {
    try {
      const raw = fssync.readFileSync(configPath, "utf8");
      const cfg = JSON.parse(raw);
      const list = Array.isArray(cfg?.workspaces) ? cfg.workspaces : [];
      for (const ws of list) {
        const root = ws && typeof ws.rootPath === "string" ? ws.rootPath : null;
        if (root && fssync.existsSync(root)) roots.add(root);
      }
    } catch {
      // malformed config.json — fall back to default discovery only
    }
  }
  return Array.from(roots).sort((a, b) => a.localeCompare(b));
}

function resolveCraftSessionFiles(env = process.env) {
  const roots = resolveCraftWorkspaceRoots(env);
  if (roots.length === 0) return [];
  const files = [];
  for (const root of roots) {
    const sessionsDir = path.join(root, "sessions");
    if (!fssync.existsSync(sessionsDir)) continue;
    let entries;
    try { entries = fssync.readdirSync(sessionsDir); } catch { continue; }
    for (const sessionId of entries) {
      const sessionDir = path.join(sessionsDir, sessionId);
      let stat;
      try { stat = fssync.statSync(sessionDir); } catch { continue; }
      if (!stat.isDirectory()) continue;
      const filePath = path.join(sessionDir, "session.jsonl");
      if (fssync.existsSync(filePath)) files.push(filePath);
    }
  }
  files.sort((a, b) => a.localeCompare(b));
  return files;
}

function resolveCraftDefaultModel() {
  // Craft is a router. Per-session header carries the actual model.
  return "craft-unknown";
}

async function parseCraftIncremental({
  sessionFiles,
  cursors,
  queuePath,
  onProgress,
  env,
  defaultModel,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const craftState = cursors.craft && typeof cursors.craft === "object" ? cursors.craft : {};
  // Per-session previous totals so each re-parse only contributes the delta
  // of the running token totals (the header rewrites in place as the session
  // grows). Shape: { [sessionId]: { input, output, cacheRead, cacheWrite, total } }
  const sessionTotals =
    craftState.sessionTotals && typeof craftState.sessionTotals === "object"
      ? { ...craftState.sessionTotals }
      : {};

  const files = Array.isArray(sessionFiles)
    ? sessionFiles
    : resolveCraftSessionFiles(env || process.env);
  const fallbackModel = defaultModel || resolveCraftDefaultModel();

  if (files.length === 0) {
    cursors.craft = {
      ...craftState,
      sessionTotals,
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

    // Read only the FIRST line — the SessionHeader carries the running totals.
    // Streaming the whole file would be wasted work since we don't use
    // per-message records for token accounting. We cap at 1 MiB to bound
    // memory if the first line is unexpectedly huge; real headers observed
    // in v0.9.0 are ~1–2 KiB so this is generous.
    let header = null;
    let parseError = null;
    let stream;
    try {
      stream = fssync.createReadStream(filePath, {
        encoding: "utf8",
        end: 1024 * 1024 - 1,
      });
    } catch { continue; }
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
    for await (const line of rl) {
      if (!line || !line.trim()) continue;
      try {
        header = JSON.parse(line);
      } catch (e) {
        parseError = e;
        header = null;
      }
      break;
    }
    rl.close();
    try { stream.destroy(); } catch {}

    if (!header || typeof header !== "object") {
      if (parseError && process.env.TOKEN_PULSE_DEBUG) {
        process.stderr.write(
          `[craft] header parse failed for ${filePath}: ${parseError.message}\n`,
        );
      }
      continue;
    }
    const usage = header.tokenUsage;
    if (!usage || typeof usage !== "object") continue;

    const sessionId =
      typeof header.id === "string" && header.id
        ? header.id
        : (typeof header.sdkSessionId === "string" && header.sdkSessionId
            ? header.sdkSessionId
            : null);
    if (!sessionId) continue;

    recordsProcessed++;

    const totalInput = toNonNegativeInt(usage.inputTokens);
    const totalOutput = toNonNegativeInt(usage.outputTokens);
    const totalCacheRead = toNonNegativeInt(usage.cacheReadTokens);
    const totalCacheWrite = toNonNegativeInt(usage.cacheCreationTokens);
    const totalReported =
      Number.isFinite(Number(usage.totalTokens)) && Number(usage.totalTokens) > 0
        ? toNonNegativeInt(usage.totalTokens)
        : totalInput + totalOutput + totalCacheRead + totalCacheWrite;

    const prev = sessionTotals[sessionId] || {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      total: 0,
    };

    // Compute the delta since the last sync. Negative deltas mean the session
    // was reset/truncated — clamp to 0 and replace the snapshot.
    const dInput = Math.max(0, totalInput - prev.input);
    const dOutput = Math.max(0, totalOutput - prev.output);
    const dCacheRead = Math.max(0, totalCacheRead - prev.cacheRead);
    const dCacheWrite = Math.max(0, totalCacheWrite - prev.cacheWrite);
    const dTotal = Math.max(0, totalReported - prev.total);

    const nowMs = Date.now();

    if (dInput === 0 && dOutput === 0 && dCacheRead === 0 && dCacheWrite === 0) {
      // No new usage since last parse — but still update the snapshot in case
      // an earlier truncate left it stale, and refresh lastSeenAt so the
      // eviction policy treats the session as live.
      sessionTotals[sessionId] = {
        input: totalInput,
        output: totalOutput,
        cacheRead: totalCacheRead,
        cacheWrite: totalCacheWrite,
        total: totalReported,
        lastSeenAt: nowMs,
      };
      continue;
    }

    // Bucket on lastMessageAt (preferred) or createdAt — both ms epoch.
    let tsMs = null;
    const tsCandidates = [header.lastMessageAt, header.lastUsedAt, header.createdAt];
    for (const cand of tsCandidates) {
      if (Number.isFinite(Number(cand)) && Number(cand) > 0) {
        tsMs = Number(cand);
        break;
      }
    }
    if (tsMs == null) tsMs = stat.mtimeMs;
    if (!Number.isFinite(tsMs) || tsMs <= 0) continue;

    const tsIso = new Date(tsMs).toISOString();
    const bucketStart = toUtcHalfHourStart(tsIso);
    if (!bucketStart) continue;

    const model = normalizeModelInput(header.model) || fallbackModel;

    // conversation_count: 1 the first time we see a session, 0 on subsequent
    // syncs of the same session. NOTE: this differs from omp/Claude which
    // count one-per-assistant-message. Cross-provider "conversations" totals
    // are therefore not directly comparable — Craft's are per-session.
    const delta = {
      input_tokens: dInput,
      cached_input_tokens: dCacheRead,
      cache_creation_input_tokens: dCacheWrite,
      output_tokens: dOutput,
      reasoning_output_tokens: 0,
      total_tokens: dTotal > 0 ? dTotal : dInput + dOutput + dCacheRead + dCacheWrite,
      conversation_count: prev.total === 0 ? 1 : 0,
    };

    const bucket = getHourlyBucket(hourlyState, "craft", model, bucketStart);
    addTotals(bucket.totals, delta);
    touchedBuckets.add(bucketKey("craft", model, bucketStart));
    eventsAggregated++;

    sessionTotals[sessionId] = {
      input: totalInput,
      output: totalOutput,
      cacheRead: totalCacheRead,
      cacheWrite: totalCacheWrite,
      total: totalReported,
      lastSeenAt: nowMs,
    };

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

  // Cap session-totals map at 5k entries to bound cursor state size. Evict by
  // lastSeenAt (least-recently-seen first) so that long-lived sessions stay
  // tracked even when many newer one-shot sessions cycle through. Insertion
  // order would silently re-zero a long-running session and double-count its
  // total on the next sync.
  const entries = Object.entries(sessionTotals);
  let capped = sessionTotals;
  if (entries.length > 5000) {
    entries.sort((a, b) => (a[1]?.lastSeenAt || 0) - (b[1]?.lastSeenAt || 0));
    capped = Object.fromEntries(entries.slice(entries.length - 5000));
  }

  const bucketsQueued = await enqueueTouchedBuckets({
    queuePath,
    hourlyState,
    touchedBuckets,
  });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.craft = {
    ...craftState,
    sessionTotals: capped,
    updatedAt,
  };

  return { recordsProcessed, eventsAggregated, bucketsQueued };
}

module.exports = {
  resolveCraftConfigDir,
  resolveCraftWorkspaceRoots,
  resolveCraftSessionFiles,
  resolveCraftDefaultModel,
  parseCraftIncremental,
};
