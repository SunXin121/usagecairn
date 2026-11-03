// Per-provider collection steps of `usagecairn sync`. cmdSync (sync.js) runs
// them in order while holding sync.lock; each step discovers one provider's
// local data, parses it into queue.jsonl and returns its counters.
const os = require("node:os");
const path = require("node:path");
const fs = require("node:fs/promises");
const fssync = require("node:fs");
const {
  resolveInstallPaths,
  resolveZcodeNativeDbPath,
  resolveMimoNativeDbPath,
  ensureFlatCursor,
} = require("../lib/install-resolver");
const { multiInstallParse, mergeBothFileSources } = require("../lib/multi-install-parser");
const { resolveDumateDbPaths, parseDumateIncremental } = require("../lib/dumate");
const wsl = require("../lib/wsl-probe");
const { ensureDir, readJson, writeJson } = require("../lib/fs");
const {
  listClaudeProjectFiles,
  listGeminiSessionFiles,
  listOpencodeMessageFiles,
  readOpencodeDbMessages,
  readOpencodeDbMessagesIncremental,
  readMimoDbMessages,
  readZcodeDbMessages,
  resolveQoderDbPaths,
  resolveQoderCnDbPaths,
  readQoderDbMessages,
  resolveQoderProjectsDir,
  resolveQoderCnProjectsDir,
  listQoderNewSessionFiles,
  parseQoderNewIncremental,
  resolveKiroDbPath,
  resolveKiroJsonlPath,
  resolveKiroBasePath,
  parseClaudeIncremental,
  parseGeminiIncremental,
  parseOpencodeIncremental,
  parseOpencodeDbIncremental,
  parseQoderDbIncremental,
  parseOpenclawIncremental,
  openclawCursorKey,
  resolveClaudeScienceDbPaths,
  readClaudeScienceFrames,
  parseClaudeScienceIncremental,
  parseCursorApiIncremental,
  parseKiroIncremental,
  parseHermesIncremental,
  gooseInstallOwnsCursor,
  zedInstallOwnsCursor,
  hermesInstallOwnsCursor,
  kiroInstallOwnsCursor,
  kiroCliInstallOwnsCursor,
  resolveKimiWireFiles,
  parseKimiIncremental,
  resolveKimiCodeWireFiles,
  parseKimiCodeIncremental,
  resolveOmpSessionFiles,
  resolveOmpSubagentFiles,
  parseOmpIncremental,
  resolveOmoSessionFiles,
  resolveOmoSubagentFiles,
  parseOmoIncremental,
  omoAgentDirCollidesWithOmp,
  resolvePiSessionFiles,
  parsePiIncremental,
  piAgentDirCollidesWithOmp,
  resolvePiDesktopSessionFiles,
  parsePiDesktopIncremental,
  resolvePrimeAgentSessionFiles,
  parsePrimeAgentIncremental,
  resolveMinimaxCodeSessionFiles,
  parseMinimaxCodeIncremental,
  resolveCraftSessionFiles,
  parseCraftIncremental,
  resolveReasonixTelemetryFiles,
  parseReasonixIncremental,
  resolveMuseSessionFiles,
  parseMuseIncremental,
  resolveAntigravityDbPath,
  listAntigravityTranscriptsWithStatus,
  parseAntigravityIncremental,
  resolveCodebuddyProjectFiles,
  parseCodebuddyIncremental,
  resolveWorkbuddyProjectFiles,
  parseWorkbuddyIncremental,
  resolveKiroCliSessionFiles,
  resolveKiroCliDbPath,
  parseKiroCliIncremental,
  resolveKilocodeTaskFiles,
  parseKilocodeIncremental,
  resolveRoocodeTaskFiles,
  parseRoocodeIncremental,
  resolveClineSessionFilesWithStatus,
  parseClineIncremental,
  resolveZedDbPath,
  parseZedIncremental,
  resolveLmstudioLogFiles,
  parseLmstudioIncremental,
  resolveUnslothDbPath,
  parseUnslothIncremental,
  resolveAnythingllmDbPath,
  parseAnythingllmIncremental,
  resolveDevinDbPath,
  parseDevinIncremental,
  resolveGooseDbPath,
  parseGooseIncremental,
  listDroidSettingsFiles,
  parseDroidIncremental,
  resolveDshSessionFiles,
  parseDshIncremental,
  resolveCommandCodeSessionFiles,
  parseCommandCodeIncremental,
  parseTraeCnApiIncremental,
  parseTraeIncremental,
  hasZcodeCursorState,
  resolveZcodeInstallPaths,
} = require("../lib/rollout");
const { renderBar, formatNumber } = require("../lib/progress");
const {
  isCursorInstalled,
  extractCursorSessionToken,
  fetchCursorUsageCsv,
  parseCursorCsv,
} = require("../lib/cursor-config");
const { fetchTraeCnUsageWithAuth, resolveTraeCnStoragePath, isTraeCnUsageEnabled } = require("../lib/trae-cn-config");
const { isCursorStoreRetry } = require("../lib/cursor-store");

function warnProviderParseFailure(label, err, opts) {
  if (opts?.auto) return;
  process.stderr.write(`${label} sync: ${err && err.message ? err.message : err}\n`);
}

function mergeParseResult(total, next) {
  return {
    recordsProcessed: total.recordsProcessed + next.recordsProcessed,
    eventsAggregated: total.eventsAggregated + next.eventsAggregated,
    bucketsQueued: total.bucketsQueued + next.bucketsQueued,
  };
}

// The pre-merge Harness prototype briefly emitted source="deepseek" before
// settling on the official CLI id, source="dsh". Relabel the local history,
// append explicit zero rows for every old cloud key, and replay from offset 0.
// That preserves the latest canonical totals while making remote whole-row
// upserts remove the stale alias instead of displaying both sources or summing
// the same session twice.

function normalizeString(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

async function applyOpenclawTotalsFallback({
  trackerDir,
  signal,
  cursors,
  queuePath,
  projectQueuePath,
}) {
  const totalTokens = Number(signal?.prevTotals?.totalTokens || 0);
  if (!trackerDir || !signal || totalTokens <= 0) {
    return { filesProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  // The passive/plugin transcript parse is authoritative: if this session's
  // transcript has ever yielded real per-event usage, synthesizing sessions.json
  // totals on top would count the same tokens twice (issue #264 review). Defer
  // to the real events and advance the fallback baseline so no stale delta lingers.
  const transcriptCursor =
    signal.sessionFile && cursors?.files
      ? cursors.files[openclawCursorKey(signal.sessionFile)]
      : null;
  const transcriptHasRealUsage = Boolean(transcriptCursor?.hasRealUsage);

  const sessionKey = `${signal.agentId}:${signal.sessionId}`;
  const statePath = path.join(trackerDir, "openclaw.fallback.state.json");
  const fallbackFilePath = path.join(trackerDir, "openclaw.fallback.jsonl");
  const state = (await readJson(statePath)) || { version: 1, sessions: {} };
  const sessions = state.sessions && typeof state.sessions === "object" ? state.sessions : {};
  const prev =
    sessions[sessionKey] && typeof sessions[sessionKey] === "object" ? sessions[sessionKey] : null;

  if (transcriptHasRealUsage) {
    sessions[sessionKey] = {
      totalTokens: normalizeNonNegativeInt(signal?.prevTotals?.totalTokens) || 0,
      inputTokens: normalizeNonNegativeInt(signal?.prevTotals?.inputTokens) || 0,
      outputTokens: normalizeNonNegativeInt(signal?.prevTotals?.outputTokens) || 0,
      model: normalizeString(signal?.prevTotals?.model) || "unknown",
      updatedAt: normalizeIsoOrEpoch(signal?.prevTotals?.updatedAt) || new Date().toISOString(),
      seenAt: new Date().toISOString(),
      coveredByEvents: true,
    };
    state.version = 1;
    state.sessions = sessions;
    await writeJson(statePath, state);
    return { filesProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  const current = {
    totalTokens: normalizeNonNegativeInt(signal?.prevTotals?.totalTokens) || 0,
    inputTokens: normalizeNonNegativeInt(signal?.prevTotals?.inputTokens) || 0,
    outputTokens: normalizeNonNegativeInt(signal?.prevTotals?.outputTokens) || 0,
    model: normalizeString(signal?.prevTotals?.model) || "unknown",
    updatedAt: normalizeIsoOrEpoch(signal?.prevTotals?.updatedAt) || new Date().toISOString(),
    seenAt: new Date().toISOString(),
  };

  let deltaTotal = current.totalTokens;
  let deltaInput = current.inputTokens;
  let deltaOutput = current.outputTokens;
  if (prev) {
    deltaTotal = Math.max(
      0,
      current.totalTokens - (normalizeNonNegativeInt(prev.totalTokens) || 0),
    );
    deltaInput = Math.max(
      0,
      current.inputTokens - (normalizeNonNegativeInt(prev.inputTokens) || 0),
    );
    deltaOutput = Math.max(
      0,
      current.outputTokens - (normalizeNonNegativeInt(prev.outputTokens) || 0),
    );
  }

  if (deltaTotal > 0 && deltaInput + deltaOutput === 0) {
    deltaInput = deltaTotal;
  }

  sessions[sessionKey] = current;
  state.version = 1;
  state.sessions = sessions;

  if (deltaTotal <= 0) {
    await writeJson(statePath, state);
    return { filesProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }

  await ensureDir(path.dirname(fallbackFilePath));
  const syntheticMessage = {
    type: "message",
    timestamp: current.updatedAt,
    message: {
      role: "assistant",
      model: current.model,
      usage: {
        input: deltaInput,
        output: deltaOutput,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: deltaTotal,
      },
    },
  };
  await fs.appendFile(fallbackFilePath, `${JSON.stringify(syntheticMessage)}\n`, "utf8");
  await writeJson(statePath, state);

  return parseOpenclawIncremental({
    sessionFiles: [{ path: fallbackFilePath, source: "openclaw" }],
    cursors,
    queuePath,
    projectQueuePath,
    source: "openclaw",
  });
}

function normalizeNonNegativeInt(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return null;
  return Math.floor(n);
}

function normalizeIsoOrEpoch(value) {
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed.length > 0 && !Number.isNaN(Date.parse(trimmed))) return trimmed;
    const numeric = Number(trimmed);
    if (Number.isFinite(numeric) && numeric > 0) {
      const ms = numeric < 1e12 ? Math.floor(numeric * 1000) : Math.floor(numeric);
      const iso = new Date(ms).toISOString();
      if (!Number.isNaN(Date.parse(iso))) return iso;
    }
  }

  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  const ms = n < 1e12 ? Math.floor(n * 1000) : Math.floor(n);
  const dt = new Date(ms);
  if (Number.isNaN(dt.getTime())) return null;
  return dt.toISOString();
}

async function syncOpenclaw({
  opts,
  trackerDir,
  queuePath,
  projectQueuePath,
  cursors,
  openclawSignal,
  sourceAllowed,
  openclawFiles,
}) {
  let openclawResult = { filesProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("openclaw") && openclawFiles.length > 0) {
    // Parses plugin-triggered and/or passively discovered session files.
    try {
      openclawResult = await parseOpenclawIncremental({
        sessionFiles: openclawFiles,
        cursors,
        queuePath,
        projectQueuePath,
        source: "openclaw",
      });
    } catch (err) {
      warnProviderParseFailure("OpenClaw", err, opts);
    }
  }

  let openclawFallback = { filesProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("openclaw")) {
    try {
      openclawFallback = await applyOpenclawTotalsFallback({
        trackerDir,
        signal: openclawSignal,
        cursors,
        queuePath,
        projectQueuePath,
      });
    } catch (err) {
      warnProviderParseFailure("OpenClaw", err, opts);
    }
  }
  openclawResult.filesProcessed += openclawFallback.filesProcessed;
  openclawResult.eventsAggregated += openclawFallback.eventsAggregated;
  openclawResult.bucketsQueued += openclawFallback.bucketsQueued;

  return { openclawResult };
}

async function syncClaude({
  opts,
  progress,
  queuePath,
  projectQueuePath,
  cursors,
  claudeProjectsDirs,
  sourceAllowed,
}) {
  let claudeFiles = [];
  if (sourceAllowed("claude")) {
    const seenClaudeFiles = new Set();
    for (const dir of claudeProjectsDirs) {
      for (const f of await listClaudeProjectFiles(dir)) {
        if (!seenClaudeFiles.has(f)) {
          seenClaudeFiles.add(f);
          claudeFiles.push(f);
        }
      }
    }
  }
  let claudeResult = { filesProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (claudeFiles.length > 0) {
    if (progress?.enabled) {
      progress.start(
        `Parsing Claude ${renderBar(0)} 0/${formatNumber(claudeFiles.length)} files | buckets 0`,
      );
    }
    try {
      claudeResult = await parseClaudeIncremental({
        projectFiles: claudeFiles,
        cursors,
        queuePath,
        projectQueuePath,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
              `Parsing Claude ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(
                p.bucketsQueued,
            )}`,
          );
        },
        source: "claude",
      });
    } catch (err) {
      warnProviderParseFailure("Claude", err, opts);
    }
  }

  return { claudeResult };
}

async function syncGemini({
  opts,
  home,
  progress,
  queuePath,
  projectQueuePath,
  cursors,
  sourceAllowed,
}) {
  let geminiPaths = null;
  if (sourceAllowed("gemini") || sourceAllowed("antigravity")) {
    const geminiNativeValue = process.env.GEMINI_HOME || path.join(home, ".gemini");
    const wslGeminiDir = process.platform === "win32" && wsl.shouldProbeWsl(process.env)
      ? wsl.discoverWslHome(".gemini")
      : null;
    geminiPaths = resolveInstallPaths({ nativeValue: geminiNativeValue, wslValue: wslGeminiDir });
  }

  let geminiFiles = [];
  if (sourceAllowed("gemini") && geminiPaths) {
    const fileSets = [];
    if (geminiPaths.native) {
      fileSets.push(await listGeminiSessionFiles(path.join(geminiPaths.native, "tmp")));
    }
    if (geminiPaths.wsl) {
      fileSets.push(await listGeminiSessionFiles(path.join(geminiPaths.wsl, "tmp")));
    }
    const seen = new Set();
    for (const set of fileSets) {
      for (const f of set) {
        if (!seen.has(f)) {
          seen.add(f);
          geminiFiles.push(f);
        }
      }
    }
  }
  let geminiResult = { filesProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (geminiFiles.length > 0) {
    if (progress?.enabled) {
      progress.start(
        `Parsing Gemini ${renderBar(0)} 0/${formatNumber(geminiFiles.length)} files | buckets 0`,
      );
    }
    try {
      geminiResult = await parseGeminiIncremental({
        sessionFiles: geminiFiles,
        cursors,
        queuePath,
        projectQueuePath,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
              `Parsing Gemini ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(
                p.bucketsQueued,
            )}`,
          );
        },
        source: "gemini",
      });
    } catch (err) {
      warnProviderParseFailure("Gemini", err, opts);
    }
  }

  return { geminiPaths, geminiResult };
}

async function syncAntigravity({
  opts,
  progress,
  queuePath,
  projectQueuePath,
  cursors,
  sourceAllowed,
  geminiPaths,
}) {
  let antigravityFiles = [];
  let antigravityInventoryComplete = true;
  if (sourceAllowed("antigravity") && geminiPaths) {
    const knownAntigravityFiles = new Set(
      Object.keys(cursors.files || {}).filter((filePath) => resolveAntigravityDbPath(filePath)),
    );
    const fileSets = [];
    if (geminiPaths.native) {
      fileSets.push(
        await listAntigravityTranscriptsWithStatus(geminiPaths.native, knownAntigravityFiles),
      );
    }
    if (geminiPaths.wsl) {
      fileSets.push(
        await listAntigravityTranscriptsWithStatus(geminiPaths.wsl, knownAntigravityFiles),
      );
    }
    antigravityInventoryComplete = fileSets.every((set) => set.complete);
    const seen = new Set();
    for (const set of fileSets) {
      for (const f of set.files) {
        if (!seen.has(f)) {
          seen.add(f);
          antigravityFiles.push(f);
        }
      }
    }
  }
  let antigravityResult = { filesProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (antigravityFiles.length > 0) {
    if (progress?.enabled) {
      progress.start(
        `Parsing Antigravity ${renderBar(0)} 0/${formatNumber(antigravityFiles.length)} files | buckets 0`,
      );
    }
    try {
      antigravityResult = await parseAntigravityIncremental({
        sessionFiles: antigravityFiles,
        cursors,
        queuePath,
        projectQueuePath,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
              `Parsing Antigravity ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(
                p.bucketsQueued,
            )}`,
          );
        },
        source: "antigravity",
        inventoryComplete: antigravityInventoryComplete,
      });
    } catch (err) {
      warnProviderParseFailure("Antigravity", err, opts);
    }
  }

  return { antigravityResult };
}

async function syncOpencode({
  progress,
  queuePath,
  projectQueuePath,
  cursorStore,
  cursors,
  xdgDataHome,
  sourceAllowed,
}) {
  let opencodeResult = { filesProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("opencode")) {
    const opencodeStorageNativeValue = process.env.OPENCODE_HOME || path.join(xdgDataHome, "opencode");
    const wslOpencodeStorageDir = process.platform === "win32" && wsl.shouldProbeWsl(process.env)
      ? wsl.discoverWslHome(".local/share/opencode")
      : null;
    const storagePaths = resolveInstallPaths({
      nativeValue: opencodeStorageNativeValue,
      wslValue: wslOpencodeStorageDir,
    });

    const opencodeDbNativeValue = process.env.OPENCODE_HOME || path.join(xdgDataHome, "opencode");
    const wslOpencodeDbDir = process.platform === "win32" && wsl.shouldProbeWsl(process.env)
      ? wsl.discoverWslHome(".local/share/opencode")
      : null;
    const dbPaths = resolveInstallPaths({
      nativeValue: opencodeDbNativeValue,
      wslValue: wslOpencodeDbDir,
    });

    const parseOpencodeForInstall = async (options) => {
      const { dbDir, cursors, messageFiles } = options;
      let filesResult = { filesProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
      if (messageFiles.length > 0) {
        filesResult = await parseOpencodeIncremental({
          ...options,
          messageFiles,
        });
      }

      let dbResult = { messagesProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
      if (dbDir) {
        const dbPath = path.join(dbDir, "opencode.db");
        const dbRead = readOpencodeDbMessagesIncremental(
          dbPath,
          cursors?.opencode?.dbCursor,
        );
        if (dbRead.messages.length > 0 || dbRead.cursor) {
          dbResult = await parseOpencodeDbIncremental({
            ...options,
            dbMessages: dbRead.messages,
            dbCursor: dbRead.cursor,
            dbPath,
            opencodeCursorStore: cursorStore,
          });
        }
      }

      return {
        recordsProcessed: filesResult.filesProcessed + dbResult.messagesProcessed,
        eventsAggregated: filesResult.eventsAggregated + dbResult.eventsAggregated,
        bucketsQueued: filesResult.bucketsQueued + dbResult.bucketsQueued,
      };
    };

    const opencodePaths = {
      native: storagePaths.native || dbPaths.native,
      wsl: storagePaths.wsl || dbPaths.wsl,
    };
    const installKeys = Object.keys(opencodePaths).filter((key) => opencodePaths[key]);
    const messageFilesByInstall = {};
    for (const key of installKeys) {
      messageFilesByInstall[key] = storagePaths[key]
        ? await listOpencodeMessageFiles(path.join(storagePaths[key], "storage"))
        : [];
    }
    const storedOpencode = cursors.opencode;
    const storedNamespaced = Boolean(
      storedOpencode &&
      typeof storedOpencode === "object" &&
      (storedOpencode.native !== undefined || storedOpencode.wsl !== undefined),
    );
    if (
      storedNamespaced !== (installKeys.length > 1) ||
      Object.values(messageFilesByInstall).some((files) => files.length > 0)
    ) {
      await cursorStore.materializeAllOpencodeState();
    }

    let multiResult;
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        multiResult = await multiInstallParse({
          paths: opencodePaths,
          parserFn: parseOpencodeForInstall,
          providerName: "opencode",
          cursors,
          queuePath,
          projectQueuePath,
          getParams: (_p, key) => ({
            dbDir: dbPaths[key],
            messageFiles: messageFilesByInstall[key] || [],
            opencodeCursorNamespace: installKeys.length > 1 ? key : "flat",
          }),
          onProgress: (p) => {
            if (!progress?.enabled) return;
            const pct = p.total > 0 ? p.index / p.total : 1;
            progress.update(
                `Parsing Opencode (${p.install || "default"}) ${renderBar(pct)} ${formatNumber(
                  p.index,
              )}/${formatNumber(p.total)} | buckets ${formatNumber(p.bucketsQueued)}`,
            );
          },
          source: "opencode",
        });
        break;
      } catch (error) {
        if (!isCursorStoreRetry(error) || attempt > 0) throw error;
      }
    }

    opencodeResult = {
      filesProcessed: multiResult.recordsProcessed,
      eventsAggregated: multiResult.eventsAggregated,
      bucketsQueued: multiResult.bucketsQueued,
    };
  }

  return { opencodeResult };
}

async function syncQoder({
  opts,
  home,
  progress,
  queuePath,
  projectQueuePath,
  cursors,
  sourceAllowed,
  makeProviderProgress,
}) {
  let qoderResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("qoder")) {
    const qoderPaths = resolveQoderDbPaths({
      home,
      env: process.env,
      platform: process.platform,
    });
    if (qoderPaths.native || qoderPaths.wsl) {
      if (progress?.enabled) {
        progress.start(`Parsing Qoder ${renderBar(0)} | buckets 0`);
      }
      try {
        const result = await multiInstallParse({
          paths: qoderPaths,
          parserFn: async ({ dbPath, ...rest }) => {
            const dbMessages = await readQoderDbMessages(dbPath);
            const parsed = await parseQoderDbIncremental({ dbMessages, dbPath, ...rest });
            return {
              recordsProcessed: parsed.messagesProcessed || 0,
              eventsAggregated: parsed.eventsAggregated || 0,
              bucketsQueued: parsed.bucketsQueued || 0,
            };
          },
          providerName: "qoder",
          cursors,
          queuePath,
          projectQueuePath,
          getParams: (dbPath) => ({ dbPath }),
          onProgress: makeProviderProgress("Qoder"),
        });
        qoderResult = {
          recordsProcessed: result.recordsProcessed || 0,
          eventsAggregated: result.eventsAggregated || 0,
          bucketsQueued: result.bucketsQueued || 0,
        };
      } catch (err) {
        warnProviderParseFailure("Qoder", err, opts);
      }
    }
  }

  // Qoder new (JSONL) — com.qoder.app.stable / ~/.qoder/projects (2026-08+)
  // The new Electron app no longer writes SharedClientCache/local.db; all
  // recent sessions live in ~/.qoder/projects as flat JSONL with credit-based
  // usage. Parse them into the same "qoder" source so history is continuous.
  // The legacy DB block above remains as a fallback for pre-migration rows.
  if (sourceAllowed("qoder")) {
    try {
      const projectsDir = resolveQoderProjectsDir({ home, env: process.env });
      const sessionFiles = await listQoderNewSessionFiles(projectsDir);
      if (sessionFiles.length > 0) {
        if (progress?.enabled) {
          progress.start(`Parsing Qoder (new) ${renderBar(0)} | buckets 0`);
        }
        const parsed = await parseQoderNewIncremental({
          sessionFiles,
          cursors,
          queuePath,
          projectQueuePath,
          onProgress: makeProviderProgress("Qoder (new)"),
          sourceKey: "qoder",
          cursorKey: "qoderNew",
        });
        qoderResult = {
          recordsProcessed: qoderResult.recordsProcessed + (parsed.messagesProcessed || 0),
          eventsAggregated: qoderResult.eventsAggregated + (parsed.eventsAggregated || 0),
          bucketsQueued: qoderResult.bucketsQueued + (parsed.bucketsQueued || 0),
        };
      }
    } catch (err) {
      warnProviderParseFailure("Qoder (new)", err, opts);
    }
  }

  return { qoderResult };
}

async function syncQoderCn({
  opts,
  home,
  progress,
  queuePath,
  projectQueuePath,
  cursors,
  sourceAllowed,
  makeProviderProgress,
}) {
  // ── Qoder CN (国内版) — same SharedClientCache/local.db schema, separate
  // Application Support/QoderCN data directory. Tracked as its own source
  // with its own cursor namespace: the two DBs each number rowids from 1, so
  // a shared cursor would mis-dedup across installs.
  let qoderCnResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("qoder-cn")) {
    const qoderCnPaths = resolveQoderCnDbPaths({
      home,
      env: process.env,
      platform: process.platform,
    });
    if (qoderCnPaths.native || qoderCnPaths.wsl) {
      if (progress?.enabled) {
        progress.start(`Parsing Qoder CN ${renderBar(0)} | buckets 0`);
      }
      try {
        const result = await multiInstallParse({
          paths: qoderCnPaths,
          parserFn: async ({ dbPath, ...rest }) => {
            const dbMessages = await readQoderDbMessages(dbPath, { label: "Qoder CN" });
            const parsed = await parseQoderDbIncremental({
              dbMessages,
              dbPath,
              sourceKey: "qoder-cn",
              cursorKey: "qoder-cn",
              ...rest,
            });
            return {
              recordsProcessed: parsed.messagesProcessed || 0,
              eventsAggregated: parsed.eventsAggregated || 0,
              bucketsQueued: parsed.bucketsQueued || 0,
            };
          },
          providerName: "qoder-cn",
          cursors,
          queuePath,
          projectQueuePath,
          getParams: (dbPath) => ({ dbPath }),
          onProgress: makeProviderProgress("Qoder CN"),
        });
        qoderCnResult = {
          recordsProcessed: result.recordsProcessed || 0,
          eventsAggregated: result.eventsAggregated || 0,
          bucketsQueued: result.bucketsQueued || 0,
        };
      } catch (err) {
        warnProviderParseFailure("Qoder CN", err, opts);
      }
    }
  }

  // Qoder CN new (JSONL) — the new CN app (com.qodercn.app.stable) writes
  // ~/.qoder-cn/projects, a sibling of the international ~/.qoder/projects.
  // Keep the divergence guard: if a user (or a future app build) points both
  // resolvers at the same directory, the same JSONL files must not count
  // under two sources.
  if (sourceAllowed("qoder-cn")) {
    try {
      const cnProjectsDir = resolveQoderCnProjectsDir({ home, env: process.env });
      const intlProjectsDir = resolveQoderProjectsDir({ home, env: process.env });
      const projectsDirKey = (p) => {
        const normalized = path.normalize(p);
        return process.platform === "win32" ? normalized.toLowerCase() : normalized;
      };
      if (projectsDirKey(cnProjectsDir) !== projectsDirKey(intlProjectsDir)) {
        const sessionFiles = await listQoderNewSessionFiles(cnProjectsDir);
        if (sessionFiles.length > 0) {
          if (progress?.enabled) {
            progress.start(`Parsing Qoder CN (new) ${renderBar(0)} | buckets 0`);
          }
          const parsed = await parseQoderNewIncremental({
            sessionFiles,
            cursors,
            queuePath,
            projectQueuePath,
            onProgress: makeProviderProgress("Qoder CN (new)"),
            sourceKey: "qoder-cn",
            cursorKey: "qoderCnNew",
          });
          qoderCnResult = {
            recordsProcessed: qoderCnResult.recordsProcessed + (parsed.messagesProcessed || 0),
            eventsAggregated: qoderCnResult.eventsAggregated + (parsed.eventsAggregated || 0),
            bucketsQueued: qoderCnResult.bucketsQueued + (parsed.bucketsQueued || 0),
          };
        }
      }
    } catch (err) {
      warnProviderParseFailure("Qoder CN (new)", err, opts);
    }
  }

  return { qoderCnResult };
}

async function syncClaudeScience({
  opts,
  home,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
  makeProviderProgress,
}) {
  // ── Claude Science (Anthropic's local research workbench, issue #246) ──
  // Per-frame token usage lives on the `frames` table of operon-cli.db.
  // Passive, incremental, subtract-on-change. There can be more than one DB:
  // multi-org installs keep one per org, and on Windows the app runs inside
  // WSL so the DB is on the distro home.
  let claudeScienceResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("claude-science")) {
    // Resolution is inside the error isolation too: it touches the filesystem
    // and (on Windows) probes WSL, and a throw here would abort cmdSync before
    // any provider's buckets are written, not just this one's.
    let claudeScienceDbPaths = [];
    try {
      claudeScienceDbPaths = resolveClaudeScienceDbPaths({ home, env: process.env });
    } catch (err) {
      warnProviderParseFailure("Claude Science", err, opts);
    }
    if (claudeScienceDbPaths.length > 0 && progress?.enabled) {
      progress.start(`Parsing Claude Science ${renderBar(0)} | buckets 0`);
    }
    for (const claudeScienceDbPath of claudeScienceDbPaths) {
      try {
        const dbRows = await readClaudeScienceFrames(claudeScienceDbPath);
        const parsed = await parseClaudeScienceIncremental({
          dbRows,
          cursors,
          queuePath,
          onProgress: makeProviderProgress("Claude Science"),
        });
        claudeScienceResult = mergeParseResult(claudeScienceResult, {
          recordsProcessed: parsed.recordsProcessed || 0,
          eventsAggregated: parsed.eventsAggregated || 0,
          bucketsQueued: parsed.bucketsQueued || 0,
        });
      } catch (err) {
        warnProviderParseFailure("Claude Science", err, opts);
      }
    }
  }

  return { claudeScienceResult };
}

async function syncKilo({
  opts,
  progress,
  queuePath,
  projectQueuePath,
  cursors,
  kiloHome,
  sourceAllowed,
  parseOpencodeDbForInstall,
  makeProviderProgress,
}) {
  // ── Kilo CLI (kilo.ai @kilocode/plugin — OpenCode-fork SQLite) ──
  let kiloResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("kilo-cli")) {
    const kiloNativeValue = process.platform === "win32" && typeof process.env.APPDATA === "string"
      ? path.join(process.env.APPDATA.trim(), "kilo", "kilo.db")
      : path.join(kiloHome, "kilo.db");
    const wslKiloDir = process.platform === "win32" && wsl.shouldProbeWsl(process.env)
      ? wsl.discoverWslHome(".local/share/kilo")
      : null;
    const kiloPaths = resolveInstallPaths({ nativeValue: kiloNativeValue, wslValue: wslKiloDir ? path.join(wslKiloDir, "kilo.db") : null });
    if (kiloPaths.native || kiloPaths.wsl) {
      if (progress?.enabled) progress.start(`Parsing Kilo CLI ${renderBar(0)} | buckets 0`);
      try {
        kiloResult = await multiInstallParse({
          paths: kiloPaths, parserFn: parseOpencodeDbForInstall, providerName: "kiloCli",
          cursors, getParams: (p) => ({ dbPath: p, readFn: readOpencodeDbMessages, source: "kilo-cli", cursorKey: "kiloCli" }),
          queuePath, projectQueuePath, onProgress: makeProviderProgress("Kilo CLI"),
        });
      } catch (err) { warnProviderParseFailure("Kilo CLI", err, opts); }
    }
  }

  return { kiloResult };
}

async function syncDumate({
  opts,
  home,
  progress,
  queuePath,
  projectQueuePath,
  cursors,
  sourceAllowed,
  makeProviderProgress,
}) {
  let dumateResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("dumate")) {
    const dumateDbPaths = resolveDumateDbPaths({ home });
    if (dumateDbPaths.length > 0) {
      if (progress?.enabled) progress.start(`Parsing DuMate ${renderBar(0)} | buckets 0`);
      try {
        dumateResult = await parseDumateIncremental({
          dbPaths: dumateDbPaths, cursors, queuePath, projectQueuePath,
          onProgress: makeProviderProgress("DuMate"),
        });
      } catch (err) { warnProviderParseFailure("DuMate", err, opts); }
    }
  }

  return { dumateResult };
}

async function syncMimo({
  opts,
  home,
  progress,
  queuePath,
  projectQueuePath,
  cursors,
  sourceAllowed,
  parseOpencodeDbForInstall,
  makeProviderProgress,
}) {
  // ── Mimo (mimocode — OpenCode-fork SQLite) ──
  // readMimoDbMessages filters out mirrored Claude Code rows to avoid
  // double-counting usage already counted as source=claude.
  let mimoResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("mimo")) {
    const mimoNativeValue = resolveMimoNativeDbPath({ home });
    const wslMimoDir = process.platform === "win32" && wsl.shouldProbeWsl(process.env)
      ? wsl.discoverWslHome(".local/share/mimocode")
      : null;
    const mimoPaths = resolveInstallPaths({ nativeValue: mimoNativeValue, wslValue: wslMimoDir ? path.join(wslMimoDir, "mimocode.db") : null });
    if (mimoPaths.native || mimoPaths.wsl) {
      if (progress?.enabled) progress.start(`Parsing Mimo ${renderBar(0)} | buckets 0`);
      try {
        mimoResult = await multiInstallParse({
          paths: mimoPaths, parserFn: parseOpencodeDbForInstall, providerName: "mimo",
          cursors, getParams: (p) => ({ dbPath: p, readFn: readMimoDbMessages, source: "mimo", cursorKey: "mimo" }),
          queuePath, projectQueuePath, onProgress: makeProviderProgress("Mimo"),
        });
      } catch (err) { warnProviderParseFailure("Mimo", err, opts); }
    }
  }

  return { mimoResult };
}

async function syncZcode({
  opts,
  home,
  progress,
  queuePath,
  projectQueuePath,
  cursorStore,
  cursors,
  sourceAllowed,
  parseOpencodeDbForInstall,
  makeProviderProgress,
}) {
  // ── ZCode (Z.ai's coding agent — OpenCode-fork SQLite) ──
  let zcodeResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("zcode")) {
    const zcodeNativeValue = resolveZcodeNativeDbPath({ home });
    const wslZcodeDir = process.platform === "win32" && wsl.shouldProbeWsl(process.env)
      ? wsl.discoverWslHome(".zcode")
      : null;
    const zcodeWslValue = wslZcodeDir ? path.join(wslZcodeDir, "cli", "db", "db.sqlite") : null;
    const zcodePaths = resolveZcodeInstallPaths(
      { nativeValue: zcodeNativeValue, wslValue: zcodeWslValue }, process.env,
      { cursors },
    );
    if (zcodePaths.native || zcodePaths.wsl) {
      const mode = wsl.getWslMode(process.env);
      if (process.platform === "win32" && ["native-first", "wsl-first"].includes(mode) &&
          !cursors.zcodeInstallSelection && hasZcodeCursorState(cursors)) {
        // Preserve legacy ownership before a repair can clear old buckets,
        // even when the selected database currently has no rows to parse.
        cursors.zcodeInstallSelection = {
          mode, zcodeHome: process.env.ZCODE_HOME?.trim() || null,
          dbPath: zcodePaths.native || zcodePaths.wsl,
        };
        cursorStore.requiresCommit = true;
      }
      if (progress?.enabled) progress.start(`Parsing ZCode ${renderBar(0)} | buckets 0`);
      try {
        zcodeResult = await multiInstallParse({
          paths: zcodePaths, parserFn: parseOpencodeDbForInstall, providerName: "zcode",
          cursors, getParams: (p) => ({ dbPath: p, readFn: readZcodeDbMessages, source: "zcode", cursorKey: "zcode" }),
          queuePath, projectQueuePath, onProgress: makeProviderProgress("ZCode"),
        });
        if (zcodeResult.recordsProcessed > 0) {
          if (process.platform === "win32" && ["native-first", "wsl-first"].includes(mode)) {
            cursors.zcodeInstallSelection = {
              mode, zcodeHome: process.env.ZCODE_HOME?.trim() || null,
              dbPath: zcodePaths.native || zcodePaths.wsl,
            };
          } else {
            delete cursors.zcodeInstallSelection;
          }
        }
      } catch (err) { warnProviderParseFailure("ZCode", err, opts); }
    }
  }

  return { zcodeResult };
}

async function syncDsh({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
  makeProviderProgress,
}) {
  let dshResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0, deferredMigrations: 0 };
  if (sourceAllowed("dsh")) {
    const dshSessionFiles = await resolveDshSessionFiles(process.env);
    if (dshSessionFiles.length > 0) {
      if (progress?.enabled) {
        progress.start(
            `Parsing DeepSeek Harness ${renderBar(0)} 0/${formatNumber(
              dshSessionFiles.length,
          )} sessions | buckets 0`,
        );
      }
      try {
        dshResult = await parseDshIncremental({
          sessionFiles: dshSessionFiles,
          cursors,
          queuePath,
          onProgress: makeProviderProgress("DeepSeek Harness"),
        });
        if (dshResult.deferredMigrations > 0) {
          warnProviderParseFailure(
            "DeepSeek Harness",
            new Error(
              `${dshResult.deferredMigrations} artifact migration(s) deferred; inspect cursors.dsh.deferredMigrations and retry after the replacement is complete`,
            ),
            opts,
          );
        }
      } catch (err) {
        warnProviderParseFailure("DeepSeek Harness", err, opts);
      }
    }
  }

  return { dshResult };
}

async function syncCommandCode({
  opts,
  progress,
  queuePath,
  projectQueuePath,
  cursors,
  sourceAllowed,
  makeProviderProgress,
}) {
  // ── Command Code (`cmd`) — passive read of ~/.commandcode session logs ──
  let commandCodeResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("command-code")) {
    try {
      const commandCodeSessionFiles = await resolveCommandCodeSessionFiles(process.env);
      // Empty discovery preserves durable usage history. Native I/O failures
      // remain visible; optional WSL failures do not suppress native usage.
      if (commandCodeSessionFiles.length > 0 || cursors.commandCode) {
        if (progress?.enabled) {
          progress.start(
              `Parsing Command Code ${renderBar(0)} 0/${formatNumber(
                commandCodeSessionFiles.length,
            )} sessions | buckets 0`,
          );
        }
        commandCodeResult = await parseCommandCodeIncremental({
          sessionFiles: commandCodeSessionFiles,
          cursors,
          queuePath,
          projectQueuePath,
          onProgress: makeProviderProgress("Command Code"),
        });
      }
    } catch (err) {
      warnProviderParseFailure("Command Code", err, opts);
    }
  }

  return { commandCodeResult };
}

async function syncLmstudio({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
  makeProviderProgress,
}) {
  // ── LM Studio and Unsloth Studio — passive inference usage ──
  let lmstudioResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("lmstudio")) {
    const lmstudioLogFiles = await resolveLmstudioLogFiles(process.env);
    if (lmstudioLogFiles.length > 0) {
      if (progress?.enabled) {
        progress.start(
          `Parsing LM Studio ${renderBar(0)} 0/${formatNumber(lmstudioLogFiles.length)} logs | buckets 0`,
        );
      }
      try {
        lmstudioResult = await parseLmstudioIncremental({
          logFiles: lmstudioLogFiles,
          cursors,
          queuePath,
          onProgress: makeProviderProgress("LM Studio"),
        });
      } catch (err) {
        warnProviderParseFailure("LM Studio", err, opts);
      }
    }
  }

  return { lmstudioResult };
}

async function syncUnsloth({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
  makeProviderProgress,
}) {
  let unslothResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("unsloth")) {
    const unslothDbPath = resolveUnslothDbPath(process.env);
    if (unslothDbPath && fssync.existsSync(unslothDbPath)) {
      if (progress?.enabled) progress.start(`Parsing Unsloth ${renderBar(0)} | buckets 0`);
      try {
        unslothResult = await parseUnslothIncremental({
          dbPath: unslothDbPath,
          cursors,
          queuePath,
          onProgress: makeProviderProgress("Unsloth"),
        });
      } catch (err) {
        warnProviderParseFailure("Unsloth", err, opts);
      }
    }
  }

  return { unslothResult };
}

async function syncAnythingllm({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
  makeProviderProgress,
}) {
  // ── AnythingLLM Desktop (workspace_chats.response.metrics) ──
  let anythingllmResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("anythingllm")) {
    const anythingllmDbPath = resolveAnythingllmDbPath(process.env);
    if (anythingllmDbPath && fssync.existsSync(anythingllmDbPath)) {
      if (progress?.enabled) progress.start(`Parsing AnythingLLM ${renderBar(0)} | buckets 0`);
      try {
        anythingllmResult = await parseAnythingllmIncremental({
          dbPath: anythingllmDbPath,
          cursors,
          queuePath,
          onProgress: makeProviderProgress("AnythingLLM"),
        });
      } catch (err) {
        warnProviderParseFailure("AnythingLLM", err, opts);
      }
    }
  }

  return { anythingllmResult };
}

async function syncDevin({
  opts,
  progress,
  queuePath,
  projectQueuePath,
  cursors,
  sourceAllowed,
  makeProviderProgress,
}) {
  // ── Devin CLI (Cognition) — SQLite message_nodes chat_message metrics ──
  let devinResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("devin")) {
    const devinDbPath = resolveDevinDbPath(process.env);
    if (devinDbPath && fssync.existsSync(devinDbPath)) {
      if (progress?.enabled) progress.start(`Parsing Devin ${renderBar(0)} | buckets 0`);
      try {
        devinResult = await parseDevinIncremental({
          dbPath: devinDbPath,
          cursors,
          queuePath,
          projectQueuePath,
          onProgress: makeProviderProgress("Devin"),
        });
      } catch (err) {
        warnProviderParseFailure("Devin", err, opts);
      }
    }
  }

  return { devinResult };
}

async function syncKilocode({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
}) {
  // ── Kilo Code VS Code extension (Cline-style ui_messages.json) ──
  const kilocodeTaskFiles = sourceAllowed("kilocode")
    ? mergeBothFileSources({ resolveFiles: resolveKilocodeTaskFiles, env: process.env })
    : [];
  let kilocodeResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (kilocodeTaskFiles.length > 0) {
    if (progress?.enabled) {
      progress.start(
        `Parsing Kilo Code ${renderBar(0)} 0/${formatNumber(kilocodeTaskFiles.length)} tasks | buckets 0`,
      );
    }
    try {
      kilocodeResult = await parseKilocodeIncremental({
        taskFiles: kilocodeTaskFiles,
        cursors,
        queuePath,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
              `Parsing Kilo Code ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(
                p.total,
            )} tasks | buckets ${formatNumber(p.bucketsQueued)}`,
          );
        },
      });
    } catch (err) {
      warnProviderParseFailure("Kilo Code", err, opts);
    }
  }

  return { kilocodeResult };
}

async function syncGoose({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
  gooseOnProgress,
}) {
  // ── Goose (Block) — SQLite sessions with cumulative tokens per session ──
  const gooseDbPath = resolveGooseDbPath(process.env);
  let gooseResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("goose")) {
    const gooseMode = wsl.getWslMode(process.env);
    if (gooseMode === "both" && process.platform === "win32") {
      const home = os.homedir();
      const appData = process.env.APPDATA || path.join(home, "AppData", "Roaming");
      const nativeDb = path.join(appData, "goose", "sessions", "sessions.db");
      const wslDir = wsl.shouldProbeWsl(process.env) ? wsl.discoverWslHome(".local/share/goose/sessions") : null;
      const wslDb = wslDir ? path.join(wslDir, "sessions.db") : null;
      const goosePaths = resolveInstallPaths({ nativeValue: nativeDb, wslValue: wslDb });
      if (goosePaths.native || goosePaths.wsl) {
        if (progress?.enabled) progress.start(`Parsing Goose ${renderBar(0)} 0 sessions | buckets 0`);
        try {
          gooseResult = await multiInstallParse({
            paths: goosePaths, parserFn: parseGooseIncremental, providerName: "goose",
            cursors, getParams: (p) => ({ dbPath: p }), queuePath, onProgress: gooseOnProgress,
            detectInstall: gooseInstallOwnsCursor,
          });
        } catch (err) { warnProviderParseFailure("Goose", err, opts); }
      }
    } else if (gooseDbPath && fssync.existsSync(gooseDbPath)) {
      if (progress?.enabled) progress.start(`Parsing Goose ${renderBar(0)} 0 sessions | buckets 0`);
      ensureFlatCursor(cursors, "goose", process.env);
      try {
        gooseResult = await parseGooseIncremental({
          dbPath: gooseDbPath, cursors, queuePath, onProgress: gooseOnProgress,
        });
      } catch (err) { warnProviderParseFailure("Goose", err, opts); }
    }
  }

  return { gooseResult };
}

async function syncDroid({
  opts,
  progress,
  queuePath,
  projectQueuePath,
  cursors,
  sourceAllowed,
}) {
  // ── Droid (Factory CLI) — passive reader for ~/.factory/sessions/*.settings.json ──
  const droidSettingsFiles = sourceAllowed("droid")
    ? mergeBothFileSources({ resolveFiles: listDroidSettingsFiles, env: process.env })
    : [];
  let droidResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (droidSettingsFiles.length > 0) {
    if (progress?.enabled) {
      progress.start(
        `Parsing Droid ${renderBar(0)} 0/${formatNumber(droidSettingsFiles.length)} sessions | buckets 0`,
      );
    }
    try {
      droidResult = await parseDroidIncremental({
        settingsFiles: droidSettingsFiles,
        cursors,
        queuePath,
        projectQueuePath,
        // Full-scan sync: drop cursor entries for any session whose
        // settings.json has disappeared off disk so cursors.droid stays
        // bounded by the actual on-disk session count.
        prune: true,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
              `Parsing Droid ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(
                p.total,
            )} sessions | buckets ${formatNumber(p.bucketsQueued)}`,
          );
        },
      });
    } catch (err) {
      warnProviderParseFailure("Droid", err, opts);
    }
  }

  return { droidResult };
}

async function syncZed({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
  zedOnProgress,
}) {
  // ── Zed Agent (all providers; cumulative-delta over SQLite threads) ──
  const zedDbPath = resolveZedDbPath(process.env);
  let zedResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("zed")) {
    const zedMode = wsl.getWslMode(process.env);
    if (zedMode === "both" && process.platform === "win32") {
      const home = os.homedir();
      const local = process.env.LOCALAPPDATA || path.join(home, "AppData", "Local");
      const nativeDb = path.join(local, "Zed", "threads", "threads.db");
      const wslThreadsDir = wsl.shouldProbeWsl(process.env) ? wsl.discoverWslHome(".local/share/zed/threads") : null;
      const wslDb = wslThreadsDir ? path.join(wslThreadsDir, "threads.db") : null;
      const zedPaths = resolveInstallPaths({ nativeValue: nativeDb, wslValue: wslDb });
      if (zedPaths.native || zedPaths.wsl) {
        if (progress?.enabled) progress.start(`Parsing Zed Agent ${renderBar(0)} 0 threads | buckets 0`);
        try {
          zedResult = await multiInstallParse({
            paths: zedPaths, parserFn: parseZedIncremental, providerName: "zed",
            cursors, getParams: (p) => ({ dbPath: p }), queuePath, onProgress: zedOnProgress,
            detectInstall: zedInstallOwnsCursor,
          });
        } catch (err) { warnProviderParseFailure("Zed Agent", err, opts); }
      }
    } else if (zedDbPath && fssync.existsSync(zedDbPath)) {
      if (progress?.enabled) progress.start(`Parsing Zed Agent ${renderBar(0)} 0 threads | buckets 0`);
      ensureFlatCursor(cursors, "zed", process.env);
      try {
        zedResult = await parseZedIncremental({
          dbPath: zedDbPath, cursors, queuePath, onProgress: zedOnProgress,
        });
      } catch (err) { warnProviderParseFailure("Zed Agent", err, opts); }
    }
  }

  return { zedResult };
}

async function syncRoocode({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
}) {
  // ── Roo Code VS Code extension (Cline-derived; rooveterinaryinc.roo-cline) ──
  const roocodeTaskFiles = sourceAllowed("roocode") ? resolveRoocodeTaskFiles(process.env) : [];
  let roocodeResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (roocodeTaskFiles.length > 0) {
    if (progress?.enabled) {
      progress.start(
        `Parsing Roo Code ${renderBar(0)} 0/${formatNumber(roocodeTaskFiles.length)} tasks | buckets 0`,
      );
    }
    try {
      roocodeResult = await parseRoocodeIncremental({
        taskFiles: roocodeTaskFiles,
        cursors,
        queuePath,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
              `Parsing Roo Code ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(
                p.total,
            )} tasks | buckets ${formatNumber(p.bucketsQueued)}`,
          );
        },
      });
    } catch (err) {
      warnProviderParseFailure("Roo Code", err, opts);
    }
  }

  return { roocodeResult };
}

async function syncCline({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
}) {
  // ── Cline (CLI v3 / desktop app — ~/.cline/data/sessions) ──
  let clineResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("cline")) {
    try {
      const clineScan = resolveClineSessionFilesWithStatus(process.env);
      const clineSessionFiles = clineScan.files;
      for (const failure of clineScan.errors) {
        warnProviderParseFailure("Cline", failure.error, opts);
      }
      if (progress?.enabled && clineSessionFiles.length > 0) {
        progress.start(
          `Parsing Cline ${renderBar(0)} 0/${formatNumber(clineSessionFiles.length)} transcripts | buckets 0`,
        );
      }
      clineResult = await parseClineIncremental({
        sessionFiles: clineSessionFiles,
        scanCompleteRoots: clineScan.completedRoots,
        cursors,
        queuePath,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
              `Parsing Cline ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(
                p.total,
            )} transcripts | buckets ${formatNumber(p.bucketsQueued)}`,
          );
        },
      });
    } catch (err) {
      warnProviderParseFailure("Cline", err, opts);
    }
  }

  return { clineResult };
}

async function syncCursor({
  opts,
  cursorSyncDeps,
  home,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
}) {
  // ── Cursor (API-based) ──
  let cursorResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  const cursorInstalled = cursorSyncDeps.isInstalled || isCursorInstalled;
  const cursorAuthExtractor = cursorSyncDeps.extractAuth || extractCursorSessionToken;
  const cursorUsageFetcher = cursorSyncDeps.fetchUsageCsv || fetchCursorUsageCsv;
  if (sourceAllowed("cursor") && cursorInstalled({ home })) {
    const cursorAuth = cursorAuthExtractor({ home });
    if (cursorAuth) {
      try {
        if (progress?.enabled) {
          progress.start(`Fetching Cursor usage...`);
        }
        const csvText = await cursorUsageFetcher({ cookie: cursorAuth.cookie });
        const records = parseCursorCsv(csvText);
        if (records.length > 0) {
          if (progress?.enabled) {
            progress.start(
              `Parsing Cursor ${renderBar(0)} 0/${formatNumber(records.length)} records | buckets 0`,
            );
          }
          cursorResult = await parseCursorApiIncremental({
            records,
            cursors,
            queuePath,
            onProgress: (p) => {
              if (!progress?.enabled) return;
              const pct = p.total > 0 ? p.index / p.total : 1;
              progress.update(
                  `Parsing Cursor ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(
                    p.total,
                )} records | buckets ${formatNumber(p.bucketsQueued)}`,
              );
            },
            source: "cursor",
          });
        }
      } catch (err) {
        if (!opts.auto) {
          process.stderr.write(`Cursor sync: ${err.message}\n`);
        }
      }
    }
  }

  return { cursorResult };
}

async function syncTrae({
  opts,
  queuePath,
  cursors,
  sourceAllowed,
  makeProviderProgress,
}) {
  let traeResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("trae")) {
    try {
      traeResult = await parseTraeIncremental({
        cursors, queuePath, onProgress: makeProviderProgress("TRAE"),
      });
      for (const { database, message } of traeResult.errors) {
        process.stderr.write(`TRAE sync: could not read ${database}: ${message}. Will retry on the next sync.\n`);
      }
      if (traeResult.recordsSkipped > 0 && !opts.auto) {
        process.stderr.write(`TRAE sync: skipped ${traeResult.recordsSkipped} records with unsupported usage metadata.\n`);
      }
      if (traeResult.estimatedRecords > 0 && !opts.auto) {
        process.stderr.write(`TRAE sync: ${traeResult.estimatedRecords} Gemini records have repaired thought or cache counters, marked as estimated.\n`);
      }
      if (traeResult.unpricedRecords > 0 && !opts.auto) {
        process.stderr.write(`TRAE sync: ${traeResult.unpricedRecords} multi-request turns include earlier input without a cache split; it is counted in token totals but left out of cost.\n`);
      }
    } catch (err) { warnProviderParseFailure("TRAE", err); }
  }

  return { traeResult };
}

async function syncTraeCn({
  opts,
  traeCnFetchImpl,
  traeCnNowMs,
  home,
  progress,
  queuePath,
  cursors,
  isBackgroundLightweightSync,
  sourceAllowed,
  makeProviderProgress,
}) {
  // ── Trae Work CN (国内版) — account-level usage API ──
  // A simple explicit rolling 30-day window captured once per sync (no
  // cursor checkpoints / full-history import / page retries / background
  // requests). Runs only when the user has opted in via
  // TOKEN_PULSE_TRAE_CN_USAGE=1 (the read transmits the locally stored
  // sign-in JWT to TRAE's official endpoint, so it is never default-on),
  // the sync is non-lightweight, the source is allowed, and the CN storage
  // file exists — which excludes both ordinary background and `--auto
  // --background --all-local-sources`. Fetching goes through the
  // storage-backed helper so its single 401/403 reread/retry is used; an
  // empty response is a successful no-op; each window is bounded to 100
  // pages/2,000 rows and an over-capacity window is split into staggered
  // sub-windows (still over capacity at the finest allowed granularity, or
  // any other fetch/page/schema/auth failure) skips the parser and leaves
  // prior data untouched while unrelated providers continue.
  let traeCnResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (
    !isBackgroundLightweightSync &&
    isTraeCnUsageEnabled(process.env) &&
    sourceAllowed("trae-cn")
  ) {
    const nowMs = Number.isFinite(traeCnNowMs) ? traeCnNowMs : Date.now();
    const fetchImpl = typeof traeCnFetchImpl === "function" ? traeCnFetchImpl : fetch;
    const endTime = Math.floor(nowMs / 1000);
    // Align the REAL fetch start down to a half-hour boundary so the API
    // range fully contains every bucket it can touch (a raw 08:37 start
    // would only partially cover the 08:30 bucket) - session bucket floors
    // never straddle the queried range.
    const HALF_HOUR_SEC = 30 * 60;
    const startTime = Math.max(
      1,
      Math.floor((endTime - 30 * 24 * 60 * 60) / HALF_HOUR_SEC) * HALF_HOUR_SEC,
    );
    try {
      // Absent storage means "not signed in" — a silent skip, not an error.
      const traeCnStoragePath = resolveTraeCnStoragePath({ env: process.env, home });
      if (traeCnStoragePath && fssync.existsSync(traeCnStoragePath)) {
        if (progress?.enabled) {
          progress.start(`Fetching TRAE Work CN usage...`);
        }
        const traeCnUsage = await fetchTraeCnUsageWithAuth({
          start_time: startTime,
          end_time: endTime,
          fetchImpl,
          env: process.env,
          home,
        });
        const traeCnSessions = Array.isArray(traeCnUsage?.sessions) ? traeCnUsage.sessions : [];
        // An empty payload parses as a pure no-op: the TRAE absence
        // contract is NOT PROVEN (no evidence that a missing session means
        // deleted/zero), so an empty response asserts nothing - no usage
        // mutation and no session states. Non-empty snapshots append one
        // canonical session-state observation per CHANGED session (the
        // cloud LWW upsert reconciles cross-device versions).
        {
          if (progress?.enabled && traeCnSessions.length > 0) {
            progress.start(
                `Parsing TRAE Work CN ${renderBar(0)} 0/${formatNumber(
                  traeCnSessions.length,
              )} records | buckets 0`,
            );
          }
          traeCnResult = await parseTraeCnApiIncremental({
            sessions: traeCnSessions,
            cursors,
            queuePath,
            onProgress: makeProviderProgress("TRAE Work CN"),
            windowStartMs: startTime * 1000,
            windowEndMs: endTime * 1000,
            // The LOGICAL fetch stamp carried into every session state's
            // snapshot_verified_at: the same clock that produced the query
            // range, not the later enqueue moment (append-only queue
            // replays it verbatim, so transport retries never fake
            // freshness).
            snapshotVerifiedAtMs: nowMs,
          });
          // A partially malformed snapshot throws inside the parser (fail
          // closed - it must not become the authoritative window state);
          // warnProviderParseFailure below reports it without sensitive
          // data while unrelated providers continue.
        }
      }
    } catch (err) {
      warnProviderParseFailure("TRAE Work CN", err, opts);
    }
  }

  return { traeCnResult };
}

async function syncKiro({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
  makeProviderProgress,
}) {
  // ── Kiro (SQLite-based, with JSONL fallback; dual-install aware) ──
  let kiroResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("kiro")) {
    const kiroNativeBase = resolveKiroBasePath(process.env);
    const wslKiroBase = process.platform === "win32" && wsl.shouldProbeWsl(process.env)
      ? wsl.discoverWslHome(".config/Kiro/User/globalStorage/kiro.kiroagent")
      : null;
    const kiroPaths = resolveInstallPaths({ nativeValue: kiroNativeBase, wslValue: wslKiroBase });
    // resolveInstallPaths only checks the base dir (and skips the check off
    // win32); keep the original per-install db/jsonl presence gate so empty
    // installs never spin up a parse or seed a cursor namespace.
    const kiroHasData = (base) => Boolean(base)
      && (fssync.existsSync(resolveKiroDbPath(base)) || fssync.existsSync(resolveKiroJsonlPath(base)));
    if (!kiroHasData(kiroPaths.native)) kiroPaths.native = null;
    if (!kiroHasData(kiroPaths.wsl)) kiroPaths.wsl = null;
    if (kiroPaths.native || kiroPaths.wsl) {
      if (progress?.enabled) {
        progress.start(`Parsing Kiro ${renderBar(0)} | buckets 0`);
      }
      try {
        kiroResult = await multiInstallParse({
          paths: kiroPaths,
          parserFn: parseKiroIncremental,
          providerName: "kiro",
          cursors,
          getParams: (base) => ({
            basePath: base,
            dbPath: resolveKiroDbPath(base),
            jsonlPath: resolveKiroJsonlPath(base),
          }),
          queuePath,
          onProgress: makeProviderProgress("Kiro"),
          detectInstall: (base, flatState) =>
            kiroInstallOwnsCursor(resolveKiroDbPath(base), flatState),
        });
      } catch (err) {
        warnProviderParseFailure("Kiro", err, opts);
      }
    }
  }

  return { kiroResult };
}

async function syncHermes({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
  hermesOnProgress,
}) {
  // ── Hermes Agent (SQLite-based) ──
  let hermesResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("hermes")) {
    const override = process.env.TOKEN_PULSE_HERMES_HOME;
    const overridePath = typeof override === "string" && override.trim().length > 0 ? override.trim() : null;
    if (overridePath) {
      if (fssync.existsSync(overridePath)) {
        if (progress?.enabled) {
          progress.start(`Parsing Hermes ${renderBar(0)} | buckets 0`);
        }
        ensureFlatCursor(cursors, "hermes", process.env);
        try {
          hermesResult = await parseHermesIncremental({
            hermesPath: overridePath,
            cursors,
            queuePath,
            onProgress: hermesOnProgress,
          });
        } catch (err) {
          warnProviderParseFailure("Hermes", err, opts);
        }
      }
    } else {
      const home = os.homedir();
      const defaultPath = path.join(home, ".hermes");
      const nativeValue = process.platform === "win32" && typeof process.env.LOCALAPPDATA === "string"
        ? path.join(process.env.LOCALAPPDATA.trim(), "hermes") : defaultPath;
      const hermesPaths = resolveInstallPaths({ nativeValue, wslDir: ".hermes" });
      if (hermesPaths.native || hermesPaths.wsl) {
        if (progress?.enabled) {
          progress.start(`Parsing Hermes ${renderBar(0)} | buckets 0`);
        }
        try {
          hermesResult = await multiInstallParse({
            paths: hermesPaths,
            parserFn: parseHermesIncremental,
            providerName: "hermes",
            cursors,
            getParams: (path) => ({ hermesPath: path }),
            queuePath,
            onProgress: hermesOnProgress,
            detectInstall: hermesInstallOwnsCursor,
          });
        } catch (err) {
          warnProviderParseFailure("Hermes", err, opts);
        }
      }
    }
  }

  return { hermesResult };
}

async function syncKiroCli({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
}) {
  // ── Kiro CLI (reads ~/Library/Application Support/kiro-cli/data.sqlite3,
  //    legacy ~/.kiro/sessions/cli/{uuid}.json, and Kiro CLI 2.13+
  //    ~/.kiro/sessions/{workspace}/sess_{uuid}/messages.jsonl) ──
  // Runs IN PARALLEL with the Kiro IDE branch above — NOT instead of it.
  // Both emit source='kiro' so totals merge transparently; cursor state
  // is isolated in cursors.kiroCli. Kiro CLI does not persist explicit
  // token counts (billing is credit-based on Bedrock); we approximate at
  // 4 chars/token from user prompt chars and assistant response chars.
  let kiroCliResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (sourceAllowed("kiro")) {
    const kiroCliDb = resolveKiroCliDbPath(process.env);
    const kiroCliSessionFiles = resolveKiroCliSessionFiles(process.env);
    const nativeCliPresent = fssync.existsSync(kiroCliDb) || kiroCliSessionFiles.length > 0;

    // Explicit overrides pin a single install — never mix them with WSL
    // auto-discovery (mirrors the Hermes TOKEN_PULSE_HERMES_HOME branch).
    const kiroCliOverride = Boolean(process.env.KIRO_CLI_DB_PATH || process.env.KIRO_HOME);
    let wslKiroCliEnv = null;
    let wslKiroCliMarker = null;
    if (!kiroCliOverride && process.platform === "win32" && wsl.shouldProbeWsl(process.env)) {
      // A WSL install owns BOTH a data dir (~/.local/share/kiro-cli) and a
      // sessions home (~/.kiro). Derive both from whichever probe hits so
      // the per-install env never falls back to native paths.
      const wslKiroHomeDir = wsl.discoverWslHome(".kiro");
      const wslCliDataDir = wsl.discoverWslHome(".local/share/kiro-cli");
      const wslHomeRoot = wslKiroHomeDir
        ? path.dirname(wslKiroHomeDir)
        : (wslCliDataDir ? path.dirname(path.dirname(path.dirname(wslCliDataDir))) : null);
      if (wslHomeRoot) {
        const wslCliDb = path.join(wslHomeRoot, ".local", "share", "kiro-cli", "data.sqlite3");
        wslKiroCliEnv = {
          ...process.env,
          KIRO_CLI_DB_PATH: wslCliDb,
          KIRO_HOME: path.join(wslHomeRoot, ".kiro"),
        };
        const wslCliPresent = fssync.existsSync(wslCliDb)
          || resolveKiroCliSessionFiles(wslKiroCliEnv).length > 0;
        if (wslCliPresent) wslKiroCliMarker = wslCliDb;
      }
    }

    // Paths here are install markers only — the parser resolves its DB and
    // session files from the per-install env (KIRO_CLI_DB_PATH/KIRO_HOME).
    const kiroCliPaths = process.platform === "win32"
      ? wsl.resolveAllWin32Paths({
        nativeValue: nativeCliPresent ? kiroCliDb : null,
        wslValue: wslKiroCliMarker,
        env: process.env,
        platform: "win32",
      })
      : { native: nativeCliPresent ? kiroCliDb : null, wsl: null };
    const kiroCliEnvFor = (p) =>
      wslKiroCliEnv && p === wslKiroCliMarker ? wslKiroCliEnv : process.env;
    if (kiroCliPaths.native || kiroCliPaths.wsl) {
      if (progress?.enabled) {
        progress.start(`Parsing Kiro CLI ${renderBar(0)} | buckets 0`);
      }
      try {
        kiroCliResult = await multiInstallParse({
          paths: kiroCliPaths,
          parserFn: parseKiroCliIncremental,
          providerName: "kiroCli",
          cursors,
          // Per-install env MUST come from getParams: multiInstallParse
          // spreads shared params over it, so a top-level env would clobber
          // the per-install one.
          getParams: (p) => ({ env: kiroCliEnvFor(p) }),
          queuePath,
          onProgress: (p) => {
            if (!progress?.enabled) return;
            const pct = p.total > 0 ? p.index / p.total : 1;
            progress.update(
              `Parsing Kiro CLI ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} sessions | buckets ${formatNumber(p.bucketsQueued)}`,
            );
          },
          detectInstall: (p, flatState) =>
            kiroCliInstallOwnsCursor(kiroCliEnvFor(p).KIRO_CLI_DB_PATH || kiroCliDb, flatState),
        });
      } catch (err) {
        if (!opts.auto) {
          process.stderr.write(`Kiro CLI sync: ${err.message}\n`);
        }
      }
    }
  }

  return { kiroCliResult };
}

async function syncKimi({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
}) {
  // ── Kimi (passive wire.jsonl reader) ──
  let kimiResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  const kimiWireFiles = sourceAllowed("kimi")
    ? mergeBothFileSources({ resolveFiles: resolveKimiWireFiles, env: process.env })
    : [];
  const kimiCodeWireFiles = sourceAllowed("kimi-code")
    ? mergeBothFileSources({ resolveFiles: resolveKimiCodeWireFiles, env: process.env })
    : [];
  if (kimiWireFiles.length > 0) {
    if (progress?.enabled) {
      progress.start(`Parsing Kimi Code ${renderBar(0)} | buckets 0`);
    }
    try {
      kimiResult = await parseKimiIncremental({
        wireFiles: kimiWireFiles,
        cursors,
        queuePath,
        env: process.env,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
            `Parsing Kimi Code ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(p.bucketsQueued)}`,
          );
        },
      });
    } catch (err) {
      warnProviderParseFailure("Kimi Code", err, opts);
    }
  }

  return { kimiResult, kimiCodeWireFiles };
}

async function syncKimiCode({
  opts,
  progress,
  queuePath,
  cursors,
  kimiCodeWireFiles,
}) {
  // ── Kimi Code official (@moonshot-ai/kimi-code, ~/.kimi-code) ──
  let kimiCodeResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  if (kimiCodeWireFiles.length > 0) {
    if (progress?.enabled) {
      progress.start(`Parsing Kimi Code (official) ${renderBar(0)} | buckets 0`);
    }
    try {
      kimiCodeResult = await parseKimiCodeIncremental({
        wireFiles: kimiCodeWireFiles,
        cursors,
        queuePath,
        env: process.env,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
            `Parsing Kimi Code (official) ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(p.bucketsQueued)}`,
          );
        },
      });
    } catch (err) {
      warnProviderParseFailure("Kimi Code (official)", err, opts);
    }
  }

  return { kimiCodeResult };
}

async function syncCodebuddy({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
}) {
  // ── CodeBuddy CLI (passive ~/.codebuddy/projects/**/*.jsonl reader) ──
  // Tencent's CodeBuddy CLI is a Claude Code clone; no hook system, so we
  // tail the per-session JSONL conversation logs incrementally on each sync.
  let codebuddyResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  const codebuddyFiles = sourceAllowed("codebuddy")
    ? mergeBothFileSources({ resolveFiles: resolveCodebuddyProjectFiles, env: process.env })
    : [];
  if (codebuddyFiles.length > 0) {
    if (progress?.enabled) {
      progress.start(`Parsing CodeBuddy ${renderBar(0)} | buckets 0`);
    }
    try {
      codebuddyResult = await parseCodebuddyIncremental({
        projectFiles: codebuddyFiles,
        cursors,
        queuePath,
        env: process.env,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
            `Parsing CodeBuddy ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(p.bucketsQueued)}`,
          );
        },
      });
    } catch (err) {
      warnProviderParseFailure("CodeBuddy", err, opts);
    }
  }

  return { codebuddyResult };
}

async function syncWorkbuddy({
  opts,
  progress,
  queuePath,
  projectQueuePath,
  cursors,
  sourceAllowed,
}) {
  // ── WorkBuddy (passive ~/.workbuddy/projects/**/*.jsonl reader) ──
  // Tencent's WorkBuddy is a Claude Code fork in the same family as CodeBuddy;
  // usage rides on function_call records too (not only assistant messages) and
  // sub-agent logs nest one level deeper, so the resolver recurses. See the
  // parser comment in rollout.js for the cache-aware token math.
  let workbuddyResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0, projectBucketsQueued: 0, projectCursorUpdated: false };
  const workbuddyFiles = sourceAllowed("workbuddy")
    ? mergeBothFileSources({ resolveFiles: resolveWorkbuddyProjectFiles, env: process.env })
    : [];
  if (sourceAllowed("workbuddy")) {
    if (progress?.enabled) {
      progress.start(`Parsing WorkBuddy ${renderBar(0)} | buckets 0`);
    }
    try {
      workbuddyResult = await parseWorkbuddyIncremental({
        projectFiles: workbuddyFiles,
        cursors,
        queuePath,
        projectQueuePath,
        env: process.env,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
            `Parsing WorkBuddy ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(p.bucketsQueued)}`,
          );
        },
      });
    } catch (err) {
      warnProviderParseFailure("WorkBuddy", err, opts);
    }
  }

  return { workbuddyResult };
}

async function syncWorkbuddyAi({
  opts,
  progress,
  queuePath,
  projectQueuePath,
  cursors,
  sourceAllowed,
}) {
  // ── WorkBuddy AI — international build (~/.workbuddy-ai/projects/**/*.jsonl) ──
  // Same Claude-fork family and byte-identical JSONL as the CN build above;
  // only the home directory differs, so it reuses the WorkBuddy parser and
  // gets its own source key (and cursor namespace) so the two builds stay
  // separable instead of merging into one provider row.
  let workbuddyAiResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0, projectBucketsQueued: 0, projectCursorUpdated: false };
  const workbuddyAiFiles = sourceAllowed("workbuddy-ai")
    ? mergeBothFileSources({
        resolveFiles: (resolveEnv) => resolveWorkbuddyProjectFiles(resolveEnv, "workbuddy-ai"),
        env: process.env,
      })
    : [];
  if (sourceAllowed("workbuddy-ai")) {
    if (progress?.enabled) {
      progress.start(`Parsing WorkBuddy AI ${renderBar(0)} | buckets 0`);
    }
    try {
      workbuddyAiResult = await parseWorkbuddyIncremental({
        projectFiles: workbuddyAiFiles,
        cursors,
        queuePath,
        projectQueuePath,
        env: process.env,
        source: "workbuddy-ai",
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
            `Parsing WorkBuddy AI ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(p.bucketsQueued)}`,
          );
        },
      });
    } catch (err) {
      warnProviderParseFailure("WorkBuddy AI", err, opts);
    }
  }

  return { workbuddyAiResult };
}

async function syncOmp({
  opts,
  progress,
  queuePath,
  projectQueuePath,
  cursors,
  sourceAllowed,
}) {
  // ── oh-my-pi (passive ~/.omp/agent/sessions/**/*.jsonl reader) ──
  // Task-subagent transcripts (nested below the cwd level) are scanned too,
  // so their usage counts toward the omp totals.
  let ompResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  const ompFiles = sourceAllowed("omp")
    ? mergeBothFileSources({ resolveFiles: resolveOmpSessionFiles, env: process.env })
    : [];
  const ompSubagentFiles = sourceAllowed("omp")
    ? mergeBothFileSources({ resolveFiles: resolveOmpSubagentFiles, env: process.env })
    : [];
  if (ompFiles.length > 0 || ompSubagentFiles.length > 0) {
    if (progress?.enabled) {
      progress.start(`Parsing oh-my-pi ${renderBar(0)} | buckets 0`);
    }
    try {
      ompResult = await parseOmpIncremental({
        sessionFiles: ompFiles,
        subagentFiles: ompSubagentFiles,
        cursors,
        queuePath,
        projectQueuePath,
        env: process.env,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
            `Parsing oh-my-pi ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(p.bucketsQueued)}`,
          );
        },
      });
    } catch (err) {
      warnProviderParseFailure("oh-my-pi", err, opts);
    }
  }

  return { ompResult };
}

async function syncOmo({
  opts,
  progress,
  queuePath,
  projectQueuePath,
  cursors,
  sourceAllowed,
}) {
  // ── OmO (passive ~/.omo/agent/sessions/**/*.jsonl reader) ──
  // Same session format as oh-my-pi, but a separate install root, cursor
  // namespace and source label, so the two never shadow each other.
  let omoResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  // Skip OmO when its agent dir resolves to the same path as omp's, so an
  // explicit TOKEN_PULSE_OMO_AGENT_DIR pointing at ~/.omp/agent cannot
  // double-count the same transcripts under two source labels.
  const omoCollidesWithOmp = omoAgentDirCollidesWithOmp(process.env);
  const omoFiles = !sourceAllowed("omo") || omoCollidesWithOmp
    ? []
    : mergeBothFileSources({ resolveFiles: resolveOmoSessionFiles, env: process.env });
  const omoSubagentFiles = !sourceAllowed("omo") || omoCollidesWithOmp
    ? []
    : mergeBothFileSources({ resolveFiles: resolveOmoSubagentFiles, env: process.env });
  if (omoFiles.length > 0 || omoSubagentFiles.length > 0) {
    if (progress?.enabled) {
      progress.start(`Parsing OmO ${renderBar(0)} | buckets 0`);
    }
    try {
      omoResult = await parseOmoIncremental({
        sessionFiles: omoFiles,
        subagentFiles: omoSubagentFiles,
        cursors,
        queuePath,
        projectQueuePath,
        env: process.env,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
            `Parsing OmO ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(p.bucketsQueued)}`,
          );
        },
      });
    } catch (err) {
      warnProviderParseFailure("OmO", err, opts);
    }
  }

  return { omoResult };
}

async function syncPi({
  opts,
  progress,
  queuePath,
  projectQueuePath,
  cursors,
  sourceAllowed,
}) {
  // ── pi (@mariozechner/pi-coding-agent) — passive ~/.pi/agent/sessions/**/*.jsonl reader ──
  // Skip pi parse if its agent dir resolves to the same path as omp's. This
  // prevents double-counting when explicit overrides (TOKEN_PULSE_OMP_AGENT_DIR /
  // TOKEN_PULSE_PI_AGENT_DIR) bypass the install-signal disambiguator.
  let piResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  const piFiles = !sourceAllowed("pi") || piAgentDirCollidesWithOmp(process.env)
    ? []
    : mergeBothFileSources({ resolveFiles: resolvePiSessionFiles, env: process.env });
  if (piFiles.length > 0) {
    if (progress?.enabled) {
      progress.start(`Parsing pi ${renderBar(0)} | buckets 0`);
    }
    try {
      piResult = await parsePiIncremental({
        sessionFiles: piFiles,
        cursors,
        queuePath,
        projectQueuePath,
        env: process.env,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
            `Parsing pi ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(p.bucketsQueued)}`,
          );
        },
      });
    } catch (err) {
      warnProviderParseFailure("pi", err, opts);
    }
  }

  return { piResult };
}

async function syncPiDesktop({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
}) {
  // ── PI-Desktop — passive ~/.pi-desktop/sessions/*.jsonl metadata reader ──
  let piDesktopResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  const piDesktopFiles = sourceAllowed("pi-desktop")
    ? resolvePiDesktopSessionFiles(process.env)
    : [];
  if (piDesktopFiles.length > 0) {
    if (progress?.enabled) progress.start(`Parsing PI-Desktop ${renderBar(0)} | buckets 0`);
    try {
      piDesktopResult = await parsePiDesktopIncremental({
        sessionFiles: piDesktopFiles,
        cursors,
        queuePath,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(`Parsing PI-Desktop ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(p.bucketsQueued)}`);
        },
      });
    } catch (err) {
      warnProviderParseFailure("PI-Desktop", err, opts);
    }
  }

  return { piDesktopResult };
}

async function syncPrimeAgent({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
}) {
  // ── Prime Agent — passive ~/.prime/agent/sessions/*.jsonl usage reader ──
  let primeAgentResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  const primeAgentFiles = sourceAllowed("prime-agent")
    ? mergeBothFileSources({ resolveFiles: resolvePrimeAgentSessionFiles, env: process.env })
    : [];
  if (primeAgentFiles.length > 0) {
    if (progress?.enabled) {
      progress.start(`Parsing Prime Agent ${renderBar(0)} | buckets 0`);
    }
    try {
      primeAgentResult = await parsePrimeAgentIncremental({
        sessionFiles: primeAgentFiles,
        cursors,
        queuePath,
        env: process.env,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
            `Parsing Prime Agent ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(p.bucketsQueued)}`,
          );
        },
      });
    } catch (err) {
      warnProviderParseFailure("Prime Agent", err, opts);
    }
  }

  return { primeAgentResult };
}

async function syncMinimaxCode({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
}) {
  // ── MiniMax Code — passive ~/.minimax/v2/sessions/**/messages.jsonl usage reader ──
  let minimaxCodeResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  const minimaxCodeFiles = sourceAllowed("minimax-code")
    ? mergeBothFileSources({ resolveFiles: resolveMinimaxCodeSessionFiles, env: process.env })
    : [];
  if (minimaxCodeFiles.length > 0) {
    if (progress?.enabled) {
      progress.start(`Parsing MiniMax Code ${renderBar(0)} | buckets 0`);
    }
    try {
      minimaxCodeResult = await parseMinimaxCodeIncremental({
        sessionFiles: minimaxCodeFiles,
        cursors,
        queuePath,
        env: process.env,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
            `Parsing MiniMax Code ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(p.bucketsQueued)}`,
          );
        },
      });
    } catch (err) {
      warnProviderParseFailure("MiniMax Code", err, opts);
    }
  }

  return { minimaxCodeResult };
}

async function syncCraft({
  opts,
  progress,
  queuePath,
  cursors,
  sourceAllowed,
}) {
  // ── Craft Agents (passive ~/.craft-agent + workspaces session.jsonl reader) ──
  let craftResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  const craftFiles = sourceAllowed("craft")
    ? mergeBothFileSources({ resolveFiles: resolveCraftSessionFiles, env: process.env })
    : [];
  if (craftFiles.length > 0) {
    if (progress?.enabled) {
      progress.start(`Parsing Craft ${renderBar(0)} | buckets 0`);
    }
    try {
      craftResult = await parseCraftIncremental({
        sessionFiles: craftFiles,
        cursors,
        queuePath,
        env: process.env,
        onProgress: (p) => {
          if (!progress?.enabled) return;
          const pct = p.total > 0 ? p.index / p.total : 1;
          progress.update(
            `Parsing Craft ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(p.bucketsQueued)}`,
          );
        },
      });
    } catch (err) {
      warnProviderParseFailure("Craft", err, opts);
    }
  }

  return { craftResult };
}

async function syncReasonix({
  opts,
  queuePath,
  cursors,
  sourceAllowed,
  makeProviderProgress,
}) {
  // Reasonix — content-free cumulative telemetry sidecars only.
  let reasonixResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  const reasonixFiles = sourceAllowed("reasonix")
    ? mergeBothFileSources({ resolveFiles: resolveReasonixTelemetryFiles, env: process.env })
    : [];
  if (reasonixFiles.length > 0) {
    try {
      reasonixResult = await parseReasonixIncremental({
        telemetryFiles: reasonixFiles,
        cursors,
        queuePath,
        env: process.env,
        onProgress: makeProviderProgress("Reasonix"),
      });
    } catch (err) {
      warnProviderParseFailure("Reasonix", err, opts);
    }
  }

  return { reasonixResult };
}

async function syncMuse({
  opts,
  queuePath,
  cursors,
  sourceAllowed,
  makeProviderProgress,
}) {
  // Muse Code (Meta) — usage counts from per-session session.jsonl logs.
  let museResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  const museFiles = sourceAllowed("muse")
    ? mergeBothFileSources({ resolveFiles: resolveMuseSessionFiles, env: process.env })
    : [];
  if (museFiles.length > 0) {
    try {
      museResult = await parseMuseIncremental({
        sessionFiles: museFiles,
        cursors,
        queuePath,
        env: process.env,
        onProgress: makeProviderProgress("Muse"),
      });
    } catch (err) {
      warnProviderParseFailure("Muse", err, opts);
    }
  }

  return { museResult };
}

module.exports = {
  syncOpenclaw,
  syncClaude,
  syncGemini,
  syncAntigravity,
  syncOpencode,
  syncQoder,
  syncQoderCn,
  syncClaudeScience,
  syncKilo,
  syncDumate,
  syncMimo,
  syncZcode,
  syncDsh,
  syncCommandCode,
  syncLmstudio,
  syncUnsloth,
  syncAnythingllm,
  syncDevin,
  syncKilocode,
  syncGoose,
  syncDroid,
  syncZed,
  syncRoocode,
  syncCline,
  syncCursor,
  syncTrae,
  syncTraeCn,
  syncKiro,
  syncHermes,
  syncKiroCli,
  syncKimi,
  syncKimiCode,
  syncCodebuddy,
  syncWorkbuddy,
  syncWorkbuddyAi,
  syncOmp,
  syncOmo,
  syncPi,
  syncPiDesktop,
  syncPrimeAgent,
  syncMinimaxCode,
  syncCraft,
  syncReasonix,
  syncMuse,
  mergeParseResult,
  applyOpenclawTotalsFallback,
  normalizeNonNegativeInt,
  normalizeIsoOrEpoch,
  normalizeString,
  warnProviderParseFailure,
};
