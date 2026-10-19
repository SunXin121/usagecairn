const os = require("node:os");
const path = require("node:path");
const fs = require("node:fs/promises");
const fssync = require("node:fs");
const cp = require("node:child_process");
const readline = require("node:readline");
const { uploadedOffset } = require("./upload");

const { resolveInstallPaths, resolveZcodeNativeDbPath, resolveMimoNativeDbPath, ensureFlatCursor } = require("../lib/install-resolver");
const { multiInstallParse, mergeBothFileSources } = require("../lib/multi-install-parser");
const { resolveDumateDbPaths, parseDumateIncremental } = require("../lib/dumate");
const wsl = require("../lib/wsl-probe");
const {
  ensureDir,
  readJson,
  writeJson,
  chmod600IfPossible,
  openLock,
  inspectLock,
  updateJsonLocked,
} = require("../lib/fs");
const { countRecordOnlyFiles, formatRecordOnlyWarning } = require("../lib/codex-usage-record");
const {
  listRolloutFiles,
  listRolloutFilesDeep,
  readCodexRolloutSnapshot,
  codexSessionIdFromPath,
  filterColdCodexRolloutFiles,
  listClaudeProjectFiles,
  listGeminiSessionFiles,
  listOpencodeMessageFiles,
  readOpencodeDbMessages,
  readOpencodeDbMessagesIncremental,
  readMimoDbMessages,
  readZcodeDbMessages,
  hasZcodeNativeUsageSchema,
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
  resolveHermesPath,
  resolveCopilotOtelPaths,
  normalizeCopilotDbPath,
  uniqueCopilotDbPaths,
  coalesceCopilotDbStatesByIdentity,
  resolveCopilotSessionStorePaths,
  getCopilotSqliteFingerprint,
  resolveCopilotAppDbPaths,
  parseRolloutIncremental,
  parseClaudeIncremental,
  parseGeminiIncremental,
  parseOpencodeIncremental,
  parseOpencodeDbIncremental,
  parseQoderDbIncremental,
  parseOpenclawIncremental,
  resolveOpenclawSessionFiles,
  resolveOpenclawHome,
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
  copilotOtelCursorHasLegacyCliUsage,
  pruneCopilotUsageClaims,
  parseCopilotIncremental,
  parseCopilotSessionStoreIncremental,
  parseCopilotAppDbIncremental,
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
  resolveGrokBuildSessions,
  parseGrokBuildIncremental,
  resolveAntigravityDbPath,
  listAntigravityTranscriptsWithStatus,
  parseAntigravityIncremental,
  resolveCodebuddyProjectFiles,
  codebuddyJsonlHasUsage,
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
  droidSessionIdFromPath,
  resolveDroidModel,
  resolveDshSessionFiles,
  parseDshIncremental,
  resolveCommandCodeSessionFiles,
  parseCommandCodeIncremental,
  parseTraeCnApiIncremental,
  parseTraeIncremental,
  bucketKey,
  toUtcHalfHourStart,
  totalsKey,
  claudeMessageDedupKey,
  coerceEpochMs,
  hasZcodeCursorState,
  resolveZcodeInstallPaths,
} = require("../lib/rollout");
const { computeClaudeGroundTruthBuckets } = require("../lib/claude-categorizer");
const { createProgress, renderBar, formatNumber, formatBytes } = require("../lib/progress");
const {
  isCursorInstalled,
  extractCursorSessionToken,
  fetchCursorUsageCsv,
  parseCursorCsv,
} = require("../lib/cursor-config");
const {
  fetchTraeCnUsageWithAuth,
  resolveTraeCnStoragePath,
  isTraeCnUsageEnabled,
} = require("../lib/trae-cn-config");
const {
  isCodexSessionCursorPath,
  isCursorStoreRetry,
  openCursorStore,
} = require("../lib/cursor-store");
const { resolveTrackerPaths } = require("../lib/pulse-paths");
const {
  appendUniqueDirs,
  extraScanRootPaths,
  hasAnyScanChild,
  resolveEnvRoot,
  resolveScanRoots,
  scanRootDirState,
} = require("../lib/scan-roots");
const {
  consumeUsageDelta,
  createUsageDeltaState,
} = require("../lib/codex-token-usage");

// v1 had a cursor-format bug (wrote plain integer instead of {inode, offset,
// updatedAt}), which made parseClaudeIncremental reread every jsonl from
// byte 0 on the next sync and double everything. v2 fixed the format.
// v3 fixes two latent issues caught by adversarial review:
//   (a) v2 wrote `cursors.hourly.groupQueued[claude|<hour>]` for every
//       repaired bucket. enqueueTouchedBuckets uses presence of that key
//       as the legacy-group marker, so any later sync that touched a
//       claude hour (even just a user-message conv-count++) would re-emit
//       the entire hour as one aggregate row under model=DEFAULT_MODEL,
//       causing a different inflation path. v3 leaves groupQueued alone.
//   (b) v2 only repaired the main queue.jsonl. project.queue.jsonl still
//       carried historical claude-mem observer rows (project_key=
//       "claude-mem/observer-sessions") and the project totals on the
//       Project Usage panel stayed inflated. v3 drops every claude /
//       claude-mem row from project.queue.jsonl too, and resets the
//       matching cursors.projectHourly + project.queue.state offset.
// v4 fixes the dedup short-circuit (issue #64): v3's ground-truth scan
// itself used `if (msgId && reqId)` to build the dedup key, which silently
// disabled dedup for any provider whose jsonl entries lack `requestId`
// (DeepSeek/Kimi/Mimo/MiniMax anthropic-compatible endpoints, plus Claude
// Code's sub-agent / thinking transport paths). The repaired ground truth
// was therefore inflated by 1.6–3.7x on those providers — v3 left it that
// way.
// v6 was bumped in 0.26.3 to re-run the repair with the zero-usage dedup fix
// applied. SHIPPING 0.26.3 caused catastrophic data loss on every upgrader
// whose ~/.claude session jsonls had been pruned by Claude Code's own
// cleanup: the repair does atomic-drop + rescan, so any hour_start no longer
// represented in the on-disk logs is silently removed from queue.jsonl. On
// the reporter's machine this wiped 2.17B claude tokens (-1.27B opus-4-7,
// -474M opus-4-6, -376M sonnet-4-5, -48M haiku, -6M sonnet-4-6). The
// upload-offset reset also propagated the damage to the cloud.
// 0.26.4 HALTS back at v4 so the buggy atomic-rewrite path stops auto-firing
// on existing installs. The dedup fixes in parseClaudeFile /
// categorizeSessionFile / computeClaudeGroundTruthBuckets are KEPT — they are
// correct in isolation, and any future repair will produce the right answer
// for whatever data is actually on disk. A targeted, log-gap-safe mimo
// migration will ship later under its own key.
// One-time full re-upload: the cloud ingest dropped `conversation_count` to 0
// from 2026-04-18 until the 2026-06-10 field-mapping fix (it read
// `b.conversations`; queue rows carry `conversation_count`). Historical cloud
// rows can only be repaired from each user's local queue.jsonl — resetting the
// upload offset replays the full queue and the ingest's whole-row upsert
// overwrites every historical bucket with the correct conversation counts
// (token columns replay to the same final values: last emission per key wins,
// exactly how the cloud rows were built the first time).
// One-time repair (#187): until the codexHashes event-dedup landed, a Codex
// session file rewritten with a new inode (Codex-Manager atomically rewrites
// sessions/ files to patch the provider on every account switch) was re-scanned
// from offset 0 and its tokens re-added to the persistent hourly buckets. This
// rebuilds the codex buckets from disk (event-deduped), atomically drops the
// inflated codex rows from queue.jsonl, and resets the upload offset so the
// corrected values overwrite the cloud. GUARDED: skips if any codex session
// file that previously contributed is no longer on disk (deleted, or moved to
// ~/.codex/archived_sessions/ which sync does not scan) — clearing its bucket
// would lose that history (ref the v6 ground-truth-repair data-loss incident).
// One-time repair (#169 follow-up): forked Codex rollouts replay the parent
// session's token history into the child file, and until the same-day burst
// detector landed in rollout.js the parser counted those replayed rows as live
// usage (PR #169's date guard only caught cross-day forks). Cursors sit at EOF
// on every already-parsed file, so the forward fix alone never corrects the
// inflated history. This re-runs the exact #187 guarded rebuild under a new key
// — the rebuild re-parses every codex file with the CURRENT (fork-aware)
// parser, so replay rows are excluded — and inherits all its safety properties
// (unreproducible-session skip, atomic throwaway rebuild, queue strip, upload
// offset reset for the cloud overwrite). Pre-gated: installs with no forked
// rollout on disk carry no fork phantom and mark done without the rebuild.
// One-time repair for Codex rollouts that contain token counters from multiple
// interleaved SessionState instances. Older parsers subtracted each cumulative
// total from the most recently observed total, even when it belonged to another
// state, which could turn an ordinary turn into a multi-billion-token delta.
// v2 removes the unreliable multi_agent_version pre-gate and validates every
// contributing rollout before committing the rebuild. The new key is required
// so installs that finalized the original migration on a false negative retry.
// Keep the one escalated desktop refresh bounded; explicit full syncs can retry
// the same migration without this ceiling when the history needs a deeper scan.
const CODEX_COLD_SCAN_AUDIT_INTERVAL_MS = 24 * 60 * 60 * 1000;
const NOTIFY_LOCK_WAIT_MS = 130_000;
const NOTIFY_LOCK_POLL_MS = 5_000;
// Local API sync requests have a 120-second child-process budget. Reserve at
// least 30 seconds for parsing, persistence, and the response after contention.
const PRIORITY_LOCK_WAIT_MS = 90_000;
const PRIORITY_LOCK_POLL_MS = 250;
const CODEX_COLD_SCAN_AUDIT_MAX_SYNCS = 288;
// 0.57.0 mis-attributed mimocode's mirrored Claude/claude-mem history to
// source=mimo (read the whole DB instead of only providerID=mimo rows). This
// one-time repair purges all source=mimo data from the local queues + cursor
// state so the next sync (providerID-filtered reader) rebuilds it correctly.
// #828: ZCode CLI prunes model_usage/turn_usage to ~30 days. The wipe+rescan
// repairs above destroyed pre-window history for late upgraders (and anyone
// whose prune line advanced before the one-shot key fired). This follow-up
// restores pre-window peaks from queue.jsonl.bak.* when the live ledger is
// lower, and the wipe helpers below now preserve hours older than the live
// rebuild floor so a future re-key cannot repeat the loss.
const AUTO_SYNC_SOURCE_ALIASES = new Map([
  ["code", "every-code"],
  ["commandcode", "command-code"],
  ["deepseek", "dsh"],
  ["everycode", "every-code"],
  ["kilo", "kilo-cli"],
  ["kilo-code", "kilocode"],
  ["kimi_code", "kimi-code"],
  ["roo-code", "roocode"],
]);
const AUTO_SYNC_SOURCES = new Set([
  "acode",
  "antigravity",
  "anythingllm",
  "claude",
  "claude-science",
  "cline",
  "codebuddy",
  "codex",
  "command-code",
  "copilot",
  "craft",
  "cursor",
  "devin",
  "droid",
  "dumate",
  "dsh",
  "every-code",
  "gemini",
  "goose",
  "grok",
  "hermes",
  "kilo-cli",
  "kilocode",
  "kiro",
  "kimi",
  "kimi-code",
  "lmstudio",
  "mimo",
  "minimax-code",
  "muse",
  "omo",
  "omp",
  "opencode",
  "openclaw",
  "pi",
  "qoder",
  "qoder-cn",
  "reasonix",
  "roocode",
  "trae-cn",
  "trae",
  "unsloth",
  "workbuddy",
  "workbuddy-ai",
  "zcode",
  "zed",
]);
const BACKGROUND_AUTO_SYNC_SOURCES = new Set([
  // Keep unscoped native 5-minute syncs bounded to dated local session trees.
  "acode",
  "codex",
  "every-code",
  "muse",
  "reasonix",
]);

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

function copilotAppBaselineTotal(baseline) {
  return (
    Math.max(0, Number(baseline?.input) || 0) +
    Math.max(0, Number(baseline?.output) || 0) +
    Math.max(0, Number(baseline?.cached) || 0) +
    Math.max(0, Number(baseline?.reasoning) || 0)
  );
}

function mergeCopilotAppDbStates(primary = {}, alias = {}) {
  const primaryUpdatedAt = Date.parse(primary?.updatedAt || "") || 0;
  const aliasUpdatedAt = Date.parse(alias?.updatedAt || "") || 0;
  const newer = aliasUpdatedAt > primaryUpdatedAt ? alias : primary;
  const sessionTotals = {};
  for (const state of [primary, alias]) {
    for (const [sessionId, baseline] of Object.entries(
      state?.sessionTotals || {},
    )) {
      const current = sessionTotals[sessionId];
      const baselineTotal = copilotAppBaselineTotal(baseline);
      const currentTotal = copilotAppBaselineTotal(current);
      const baselineUpdatedAt = Date.parse(baseline?.updatedAt || "") || 0;
      const currentUpdatedAt = Date.parse(current?.updatedAt || "") || 0;
      if (
        !current ||
        baselineTotal > currentTotal ||
        (baselineTotal === currentTotal &&
          baselineUpdatedAt > currentUpdatedAt)
      ) {
        sessionTotals[sessionId] = baseline;
      }
    }
  }
  return { ...primary, ...newer, sessionTotals };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function acquireSyncLock(
  lockPath,
  opts,
  {
    notifyWaitMs = NOTIFY_LOCK_WAIT_MS,
    notifyPollMs = NOTIFY_LOCK_POLL_MS,
    priorityWaitMs = PRIORITY_LOCK_WAIT_MS,
    priorityPollMs = PRIORITY_LOCK_POLL_MS,
  } = {},
) {
  const waitsForPriority = Boolean(opts.waitForLock);
  let lock = await openLock(lockPath, {
    quietIfLocked: opts.auto || waitsForPriority,
  });
  if (lock) return lock;

  if (waitsForPriority) {
    const deadline = Date.now() + Math.max(0, priorityWaitMs);
    while (!lock && Date.now() < deadline) {
      const remaining = deadline - Date.now();
      await sleep(Math.min(Math.max(1, priorityPollMs), remaining));
      lock = await openLock(lockPath, { quietIfLocked: true });
    }
    if (!lock) {
      const error = new Error(
        "SYNC_BUSY: another sync is still running; no refresh was performed",
      );
      error.code = "SYNC_BUSY";
      throw error;
    }
    return lock;
  }

  if (!opts.fromNotify || notifyWaitMs <= 0) return null;

  // One low-frequency waiter coalesces every notify that lands behind an
  // active native/background sync. This avoids both losing the final Codex
  // turn and spawning a fleet of idle retry processes during a long scan.
  const waiter = await openLock(`${lockPath}.notify-wait`, {
    quietIfLocked: true,
  });
  if (!waiter) return null;

  try {
    const deadline = Date.now() + notifyWaitMs;
    while (!lock && Date.now() < deadline) {
      const remaining = deadline - Date.now();
      await sleep(Math.min(Math.max(1, notifyPollMs), remaining));
      lock = await openLock(lockPath, { quietIfLocked: true });
    }
    return lock;
  } finally {
    await waiter.release();
  }
}

const SYNC_SKIP_MARKER = "sync.skip.json";

// A skipped sync used to exit 0 with no output whatsoever, so lock debris left
// by a killed run stalled every parse for a day with no visible signal and
// nothing for `token-pulse status` to report (issue #431). Leave a queryable
// marker that separates ordinary contention (`lock_busy`) from a lease whose
// owner is gone (`lock_debris`, the state that needs attention).
async function recordSyncSkip(trackerDir, lockPath) {
  const holder = await inspectLock(lockPath);
  const reason = holder.exists && holder.pid && !holder.alive ? "lock_debris" : "lock_busy";
  const detail = !holder.exists
    ? "sync lock could not be acquired"
    : holder.pid
      ? `sync lock held by pid ${holder.pid} (${holder.alive ? "running" : "not running"})`
      : "sync lock has no owner record";
  await writeJson(path.join(trackerDir, SYNC_SKIP_MARKER), {
    reason,
    detail,
    at: new Date().toISOString(),
    lockPath,
  });
  return `Sync skipped: ${reason} — ${detail}\n`;
}

async function clearSyncSkip(trackerDir) {
  try {
    await fs.unlink(path.join(trackerDir, SYNC_SKIP_MARKER));
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
}

async function cmdSync(argv, context = {}) {
  context = context && typeof context === "object" ? context : {};
  const opts = parseArgs(argv);
  const diagnostics = context && typeof context === "object" ? context.diagnostics : null;
  const cursorStoreOptions = context && typeof context === "object"
    ? context.cursorStoreOptions
    : null;
  const lockWaitOptions = context && typeof context === "object"
    ? context.lockWaitOptions
    : undefined;
  // Narrow test-only seam for TRAE Work CN: deterministic injected fetch + a
  // fixed "now" for the rolling range. Production keeps the global fetch and
  // current time; no public CLI flag/config is introduced.
  const traeCnFetchImpl = context && typeof context === "object"
    ? context.traeCnFetchImpl
    : undefined;
  const traeCnNowMs = context && typeof context === "object"
    ? context.traeCnNowMs
    : undefined;
  const cursorSyncDeps = context && typeof context.cursorSyncDeps === "object"
    ? context.cursorSyncDeps
    : {};
  const syncDiagnostics = diagnostics && typeof diagnostics === "object" ? diagnostics : null;
  const home = context.home || os.homedir();
  const trackerDir = context.trackerDataDir || (await resolveTrackerPaths({ home })).trackerDir;

  await ensureDir(trackerDir);
  if (opts.fromOpenclaw) {
    await writeOpenclawSignal(trackerDir);
  }

  const lockPath = path.join(trackerDir, "sync.lock");
  const lock = await acquireSyncLock(lockPath, opts, lockWaitOptions);
  if (!lock) {
    // Warn on stderr so the notice reaches interactive and background runs
    // alike without disturbing anything that parses sync's stdout.
    process.stderr.write(await recordSyncSkip(trackerDir, lockPath));
    return;
  }
  let progress = null;
  try {
    // Marker cleanup can fail for reasons other than ENOENT (permissions, or a
    // directory replacing the expected file). Keep it inside the lease's
    // try/finally so an auxiliary diagnostic never strands the sync lock.
    await clearSyncSkip(trackerDir);
    progress = !opts.auto ? createProgress({ stream: process.stdout }) : null;
    const configPath = path.join(trackerDir, "config.json");
    const cursorsPath = path.join(trackerDir, "cursors.json");
    const queuePath = path.join(trackerDir, "queue.jsonl");
    // The server accepts aggregate buckets only. Keep parser-level project
    // support for legacy data/tests, but do not produce an unconsumed ledger.
    const projectQueuePath = null;
    const grokSignalPath = path.join(trackerDir, "grok-last-session.json");
    const legacyGrokSignalPath = path.join(trackerDir, "tracker", "grok-last-session.json");

    const config = await readJson(configPath);
    // Scan roots (#657). Every producer — hook-fired sync, native background
    // refresh, CLI — must derive the SAME root list, and the cursor store's
    // codexRoots must come from that list: a rollout under a root the store
    // does not know is filed in core.json instead of its per-day shard, so two
    // producers with different roots never see each other's cursor and re-parse
    // the file from byte 0 on every alternation (#639). CODEX_HOME keeps its
    // existing meaning (replaces ~/.codex for this process) but is normalized
    // once by resolveEnvRoot — a relative value is anchored to home, not cwd —
    // and that single value feeds discovery AND the cursor store below.
    // config.scanRoots adds roots for every process.
    const codexNativeValue = resolveEnvRoot("codex", { env: process.env, home }) || path.join(home, ".codex");
    const scanRoots = resolveScanRoots({
      home,
      env: process.env,
      config,
      base: { codex: [codexNativeValue], claude: [path.join(home, ".claude")] },
    });
    const codexCursorRoots = scanRoots.codex.map((entry) => entry.path);
    const cursorStore = await openCursorStore({
      trackerDir,
      cursorsPath,
      codexRoots: codexCursorRoots,
      ...(cursorStoreOptions && typeof cursorStoreOptions === "object"
        ? cursorStoreOptions
        : {}),
    });
    const cursors = cursorStore.cursors;
    let grokHookSignal = null;
    let grokHookSignalPath = null;
    for (const candidate of [grokSignalPath, legacyGrokSignalPath]) {
      const signal = await readJson(candidate);
      if (signal && typeof signal === "object") {
        grokHookSignal = signal;
        grokHookSignalPath = candidate;
        break;
      }
    }
    let grokHookSignalConsumed = false;

    // Claude Code home — dual-install aware (#307): a Windows host may carry
    // a native ~/.claude plus a WSL install reachable over \\wsl$. Unlike the
    // SQLite providers, Claude UNIONS every allowed install instead of taking
    // resolveAllWin32Paths' single pick: under the default wsl-first mode a
    // WSL ~/.claude would otherwise silently evict the native one from
    // collection — and from the "disk is truth" ground-truth repair below,
    // which would then erase the native history. The file-hash and
    // message-hash dedup layers make the union safe. Native is listed first
    // so the cross-environment file dedup keeps the native copy as primary.
    const claudeNativeHome = path.join(home, ".claude");
    const wslClaudeHome = process.platform === "win32" && wsl.shouldProbeWsl(process.env)
      ? wsl.discoverWslHome(".claude")
      : null;
    const claudeInstallHomes = [];
    if (process.platform !== "win32" || wsl.shouldProbeNative(process.env)) {
      claudeInstallHomes.push(claudeNativeHome);
    }
    if (wslClaudeHome) claudeInstallHomes.push(wslClaudeHome);
    // Extra Claude roots (#657): CLAUDE_CONFIG_DIR of the spawning process and
    // config.scanRoots.claude, additive to the homes above so coverage does not
    // depend on which process spawned this sync. Deduped by realpath at the
    // projects/ level: two profiles may symlink one projects/ dir, and reading
    // it under both spellings would double-parse every file, leaving
    // correctness to the bounded claudeHashes layer.
    const claudeProjectsDirs = appendUniqueDirs(
      claudeInstallHomes.map((h) => path.join(h, "projects")),
      extraScanRootPaths(scanRoots.claude).map((h) => path.join(h, "projects")),
    );
    const xdgDataHome = process.env.XDG_DATA_HOME || path.join(home, ".local", "share");
    const kiloHome = process.env.KILO_HOME || path.join(xdgDataHome, "kilo");

    // OpenClaw session plugin integration: lifecycle hooks request an
    // OpenClaw-only auto sync so unrelated providers do not get walked.
    const openclawSignal = opts.fromOpenclaw
      ? resolveOpenclawSignal({ env: process.env })
      : null;

    const autoSourceScope = resolveAutoSourceScope(opts);
    // --background narrows the local scan; a plain sync scans every source.
    const isBackgroundLightweightSync = opts.auto && opts.background;
    const isBackgroundAllLocalSync = isBackgroundLightweightSync && opts.allLocalSources;
    const isFullSourceScan = !autoSourceScope && !isBackgroundLightweightSync;
    const sourceAllowed = (...sources) => {
      if (Array.isArray(context.scanSources) && !sources.some(source => context.scanSources.includes(source))) return false;
      if (isBackgroundLightweightSync) {
        if (autoSourceScope) {
          return (
            (isBackgroundAllLocalSync || BACKGROUND_AUTO_SYNC_SOURCES.has(autoSourceScope)) &&
            sources.includes(autoSourceScope)
          );
        }
        if (isBackgroundAllLocalSync) {
          return sources.some((source) => AUTO_SYNC_SOURCES.has(source));
        }
        return sources.some((source) => BACKGROUND_AUTO_SYNC_SOURCES.has(source));
      }
      if (autoSourceScope) return sources.includes(autoSourceScope);
      return true;
    };


    const sources = [];
    if (sourceAllowed("codex")) {
      // resolveInstallPaths stays the single authority for wsl-first /
      // native-first / wsl-only / native-only / both selection; requireAnyChild
      // makes it validate that a candidate actually holds sessions/ or
      // archived_sessions/ so an empty WSL ~/.codex shell cannot shadow the
      // native install (issue #codex-wsl-shadow).
      //
      // Pass `wslDir` rather than a pre-resolved home so requireAnyChild is
      // folded INTO the discovery probe: discoverWslHome returns the first
      // distro whose ~/.codex merely exists, so resolving it here first meant a
      // bare shell in an earlier distro won, got rejected as unpopulated, and
      // the populated later distro was never looked at — the same shadowing bug
      // one level down, and a silent disagreement with status.js.
      //
      // union: both installs are real usage on this machine, so a preference
      // mode must not delete one of them. Before this, a populated WSL
      // ~/.codex evicted the native install entirely under the default
      // wsl-first — the session browser (which walks every root, see
      // session-analytics.js providerRoots) showed those Codex sessions while
      // the dashboard counted none of their tokens. Safe because the parser
      // dedups Codex events by sessionUUID:eventTimestamp, so the same session
      // seen under two path spellings collapses instead of double-counting.
      const codexPaths = resolveInstallPaths({
        nativeValue: codexNativeValue,
        wslDir: ".codex",
        requireAnyChild: ["sessions", "archived_sessions"],
        union: true,
      });
      if (codexPaths.native) {
        sources.push({ source: "codex", sessionsDir: path.join(codexPaths.native, "sessions"), inventoryCacheKey: "codexDayInventoryCache" });
        if (!isBackgroundLightweightSync) {
          sources.push({ source: "codex", sessionsDir: path.join(codexPaths.native, "archived_sessions"), deep: true });
        }
      }
      if (codexPaths.wsl) {
        sources.push({ source: "codex", sessionsDir: path.join(codexPaths.wsl, "sessions"), inventoryCacheKey: "codexDayInventoryCache" });
        if (!isBackgroundLightweightSync) {
          sources.push({ source: "codex", sessionsDir: path.join(codexPaths.wsl, "archived_sessions"), deep: true });
        }
      }
      // Extra Codex roots (#657) from config.scanRoots.codex. Same populated-
      // root rule as requireAnyChild above so an empty shell dir is not walked;
      // the day-inventory cache is keyed by day directory, so sharing it across
      // roots is safe.
      for (const extraRoot of extraScanRootPaths(scanRoots.codex)) {
        if (!hasAnyScanChild(extraRoot, ["sessions", "archived_sessions"])) continue;
        sources.push({ source: "codex", sessionsDir: path.join(extraRoot, "sessions"), inventoryCacheKey: "codexDayInventoryCache" });
        if (!isBackgroundLightweightSync) {
          sources.push({ source: "codex", sessionsDir: path.join(extraRoot, "archived_sessions"), deep: true });
        }
      }
    }
    if (sourceAllowed("acode")) {
      const acodeNativeValue =
        process.env.TOKEN_PULSE_ACODE_HOME || path.join(home, ".acode");
      const acodePaths = resolveInstallPaths({
        nativeValue: acodeNativeValue,
        wslDir: ".acode",
        requireAnyChild: ["sessions", "archived_sessions"],
        union: true,
      });
      if (acodePaths.native) {
        sources.push({
          source: "acode",
          sessionsDir: path.join(acodePaths.native, "sessions"),
          inventoryCacheKey: "acodeDayInventoryCache",
        });
        if (!isBackgroundLightweightSync) {
          sources.push({
            source: "acode",
            sessionsDir: path.join(acodePaths.native, "archived_sessions"),
            deep: true,
          });
        }
      }
      if (acodePaths.wsl) {
        sources.push({
          source: "acode",
          sessionsDir: path.join(acodePaths.wsl, "sessions"),
          inventoryCacheKey: "acodeDayInventoryCache",
        });
        if (!isBackgroundLightweightSync) {
          sources.push({
            source: "acode",
            sessionsDir: path.join(acodePaths.wsl, "archived_sessions"),
            deep: true,
          });
        }
      }
    }
    if (sourceAllowed("every-code")) {
      const codeNativeValue = process.env.CODE_HOME || path.join(home, ".code");
      const wslCodeDir = process.platform === "win32" && wsl.shouldProbeWsl(process.env)
        ? wsl.discoverWslHome(".code")
        : null;
      const codePaths = resolveInstallPaths({ nativeValue: codeNativeValue, wslValue: wslCodeDir });
      if (codePaths.native) {
        sources.push({ source: "every-code", sessionsDir: path.join(codePaths.native, "sessions") });
      }
      if (codePaths.wsl) {
        sources.push({ source: "every-code", sessionsDir: path.join(codePaths.wsl, "sessions") });
      }
    }

    const rolloutFiles = [];
    const seenSessions = new Set();
    const uniqueSources = sources.filter((entry) => {
      if (seenSessions.has(entry.sessionsDir)) return false;
      seenSessions.add(entry.sessionsDir);
      return true;
    });
    const inventoryCaches = new Map();
    const inventoryCacheKeys = new Set(
      uniqueSources.map((entry) => entry.inventoryCacheKey).filter(Boolean),
    );
    for (const cursorKey of inventoryCacheKeys) {
      const inventoryCache =
        cursors[cursorKey] && typeof cursors[cursorKey] === "object"
          ? cursors[cursorKey]
          : { version: 1, days: {} };
      cursors[cursorKey] = inventoryCache;
      inventoryCaches.set(cursorKey, inventoryCache);
    }
    const sourceFileGroups = await Promise.all(uniqueSources.map((entry) => (
      entry.deep
        ? listRolloutFilesDeep(entry.sessionsDir)
        : listRolloutFiles(entry.sessionsDir, entry.inventoryCacheKey
          ? { dayInventoryCache: inventoryCaches.get(entry.inventoryCacheKey) }
          : undefined)
    )));
    for (let sourceIndex = 0; sourceIndex < uniqueSources.length; sourceIndex++) {
      const entry = uniqueSources[sourceIndex];
      const files = sourceFileGroups[sourceIndex];
      for (const filePath of files) {
        rolloutFiles.push({ path: filePath, source: entry.source });
      }
    }
    if (isFullSourceScan) {
      await cursorStore.materializeAllCodexState(cursors);
    }

    const codexColdSkipEnabled = opts.auto && sourceAllowed("codex");
    const deferredCodexAuditSyncs = codexColdSkipEnabled
      ? await cursorStore.readDeferredCodexAuditSyncs()
      : 0;
    const codexColdAuditDue = codexColdSkipEnabled
      ? isCodexColdScanAuditDue(cursors, Date.now(), deferredCodexAuditSyncs)
      : false;
    let codexColdFilter = codexColdSkipEnabled
      ? await filterColdCodexRolloutFiles({
          rolloutFiles,
          cursors,
          codexCursorStore: cursorStore,
          projectEnabled: true,
          auditDue: codexColdAuditDue,
          diagnostics: syncDiagnostics,
        })
      : { rolloutFiles, skipped: 0 };
    const rolloutFilesForParse = codexColdFilter.rolloutFiles;
    let codexCursorLoadRestarted = false;
    if (!codexColdSkipEnabled && sourceAllowed("codex")) {
      const loadResult = await cursorStore.loadCodexFilesForPaths(
        rolloutFilesForParse,
        cursors,
      );
      codexCursorLoadRestarted = Boolean(loadResult?.restarted);
    }

    // Plugin-triggered sync points at one specific session file; a normal full
    // sync also passively scans every on-disk OpenClaw transcript so usage is
    // captured even when the session plugin never fires (issue #264). The scan
    // is gated to full syncs except for gateway lifecycle signals. A scoped
    // agent_end reads only its transcript or its session in the agent database,
    // and the ordinary background tick stays cheap. The
    // event-identity dedup makes the plugin and passive paths idempotent, so
    // overlap on the same file is safe.
    //
    // Keyed by openclawCursorKey, not the raw path: that is the same identity
    // the parser assigns cursors by, so a plugin-supplied path and a scanned
    // path that differ only in Windows casing collapse to one entry instead of
    // being parsed twice.
    const openclawSessionFiles = new Map();
    if (openclawSignal?.sessionFile) {
      openclawSessionFiles.set(openclawCursorKey(openclawSignal.sessionFile), {
        path: openclawSignal.sessionFile,
        source: "openclaw",
      });
      for (const dbPath of [
        path.join(openclawSignal.openclawHome, "agents", openclawSignal.agentId, "agent", "openclaw-agent.sqlite"),
        path.join(openclawSignal.openclawHome, "agents", openclawSignal.agentId, "openclaw-agent.sqlite"),
      ]) {
        if (!(await fs.stat(dbPath).catch(() => null))?.isFile()) continue;
        openclawSessionFiles.set(openclawCursorKey(dbPath), {
          path: dbPath, source: "openclaw", sessionId: openclawSignal.sessionId,
        });
        break;
      }
    }
    if ((isFullSourceScan || (opts.fromOpenclaw && !openclawSignal)) && sourceAllowed("openclaw")) {
      try {
        for (const f of await resolveOpenclawSessionFiles(process.env)) {
          const key = openclawCursorKey(f);
          if (!openclawSessionFiles.has(key) || f.endsWith(".sqlite")) {
            openclawSessionFiles.set(key, { path: f, source: "openclaw" });
          }
        }
      } catch (err) {
        warnProviderParseFailure("OpenClaw", err, opts);
      }
    }
    const openclawFiles = Array.from(openclawSessionFiles.values());

    if (progress?.enabled) {
      progress.start(
        `Parsing ${renderBar(0)} 0/${formatNumber(rolloutFilesForParse.length)} files | buckets 0`,
      );
    }

    let parseResult = { filesProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
    let codexParseSucceeded = false;
    let codexFallbackRetryRan = Boolean(
      codexColdFilter.restarted || codexCursorLoadRestarted,
    );
    const runCodexParse = (files) => parseRolloutIncremental({
      rolloutFiles: files,
      cursors,
      codexEventStore: Array.isArray(cursors.codexHashes)
        ? null
        : cursorStore.codexEventStore,
      queuePath,
      projectQueuePath,
      diagnostics: syncDiagnostics,
      onProgress: (p) => {
        if (!progress?.enabled) return;
        const pct = p.total > 0 ? p.index / p.total : 1;
        progress.update(
          `Parsing ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(
            p.bucketsQueued,
          )}`,
        );
      },
    });
    try {
      parseResult = await runCodexParse(rolloutFilesForParse);
      codexParseSucceeded = true;
    } catch (err) {
      if (isCursorStoreRetry(err)) {
        await cursorStore.loadCodexFilesForPaths(rolloutFiles, cursors);
        codexColdFilter = { rolloutFiles, skipped: 0, restarted: true };
        codexFallbackRetryRan = true;
        if (syncDiagnostics) {
          syncDiagnostics.cold_skipped = 0;
          syncDiagnostics.parse_candidates = rolloutFiles.reduce(
            (count, entry) => count + (
              typeof entry === "string" || !entry?.source || entry.source === "codex"
                ? 1
                : 0
            ),
            0,
          );
        }
        parseResult = await runCodexParse(rolloutFiles);
        codexParseSucceeded = true;
      } else if (err?.code === "TOKEN_PULSE_CURSOR_STORE_CORRUPT") {
        throw err;
      } else {
        warnProviderParseFailure("Codex", err, opts);
      }
    }
    const codexAuditRecorded = codexColdSkipEnabled && codexParseSucceeded;
    if (codexAuditRecorded) {
      recordCodexColdScanAudit(cursors, {
        fullAudit: codexColdAuditDue || codexFallbackRetryRan,
        skipped: codexColdFilter.skipped,
        deferredSyncs: deferredCodexAuditSyncs,
      });
    }

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

    async function parseOpencodeDbForInstall({ dbPath, readFn, source, cursorKey, ...rest }) {
      if (!dbPath || !fssync.existsSync(dbPath)) {
        return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
      }
      const dbMessages = readFn(dbPath);
      if (dbMessages.length === 0) return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
      const result = await parseOpencodeDbIncremental({ dbMessages, dbPath, source, cursorKey, ...rest });
      return {
        recordsProcessed: result.messagesProcessed || 0,
        eventsAggregated: result.eventsAggregated || 0,
        bucketsQueued: result.bucketsQueued || 0,
      };
    }

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

    function makeProviderProgress(label) {
      return (p) => {
        if (!progress?.enabled) return;
        const pct = p.total > 0 ? p.index / p.total : 1;
        progress.update(
          `Parsing ${label} ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} records | buckets ${formatNumber(p.bucketsQueued)}`,
        );
      };
    }

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

    function hermesOnProgress(p) {
      if (!progress?.enabled) return;
      const pct = p.total > 0 ? p.index / p.total : 1;
      progress.update(
        `Parsing Hermes ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} sessions | buckets ${formatNumber(p.bucketsQueued)}`,
      );
    }

    function gooseOnProgress(p) {
      if (!progress?.enabled) return;
      const pct = p.total > 0 ? p.index / p.total : 1;
      progress.update(
        `Parsing Goose ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} sessions | buckets ${formatNumber(p.bucketsQueued)}`,
      );
    }

    function zedOnProgress(p) {
      if (!progress?.enabled) return;
      const pct = p.total > 0 ? p.index / p.total : 1;
      progress.update(
        `Parsing Zed Agent ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} threads | buckets ${formatNumber(p.bucketsQueued)}`,
      );
    }

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

    // ── Grok Build (xAI) ──
    let grokResult = {
      recordsProcessed: 0,
      eventsAggregated: 0,
      bucketsQueued: 0,
      projectBucketsQueued: 0,
    };
    // Full passive scan of all Grok sessions (historical + any not covered by hook)
    const grokSessions = sourceAllowed("grok") ? resolveGrokBuildSessions(process.env) : [];
    const grokSessionInputs = [...grokSessions];
    if (sourceAllowed("grok") && grokHookSignal && typeof grokHookSignal === "object") {
      const hookSessionId =
        typeof grokHookSignal.sessionId === "string" && grokHookSignal.sessionId.trim()
          ? grokHookSignal.sessionId.trim()
          : null;
      if (hookSessionId) {
        const hookContextTokens =
          grokHookSignal.contextTokensUsed != null
            ? grokHookSignal.contextTokensUsed
            : grokHookSignal.totalTokens;
        const hookTotalTokens =
          grokHookSignal.totalTokens != null
            ? grokHookSignal.totalTokens
            : hookContextTokens;
        grokSessionInputs.unshift({
          sessionId: hookSessionId,
          sessionDir:
            typeof grokHookSignal.sessionDir === "string" ? grokHookSignal.sessionDir : undefined,
          cwd: typeof grokHookSignal.cwd === "string" ? grokHookSignal.cwd : undefined,
          encodedCwd:
            typeof grokHookSignal.sessionDir === "string" && grokHookSignal.sessionDir
              ? path.basename(path.dirname(grokHookSignal.sessionDir))
              : undefined,
          updatesPath:
            typeof grokHookSignal.updatesPath === "string" ? grokHookSignal.updatesPath : undefined,
          signalsPath:
            typeof grokHookSignal.signalsPath === "string" ? grokHookSignal.signalsPath : undefined,
          summaryPath:
            typeof grokHookSignal.summaryPath === "string" ? grokHookSignal.summaryPath : undefined,
          signals: {
            contextTokensUsed: hookContextTokens,
            totalTokens: hookTotalTokens,
            totalTokensBeforeCompaction: grokHookSignal.totalTokensBeforeCompaction,
            assistantMessageCount: grokHookSignal.messageCount,
            primaryModelId: grokHookSignal.model,
            lastActiveAt: grokHookSignal.lastActive,
          },
          summary: { updated_at: grokHookSignal.lastActive },
        });
        grokHookSignalConsumed = true;
      }
    }
    if (grokSessionInputs.length > 0) {
      if (progress?.enabled) {
        progress.start(`Parsing Grok Build ${renderBar(0)} | buckets 0`);
      }
      let grokScanResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
      try {
        grokScanResult = await parseGrokBuildIncremental({
          sessions: grokSessionInputs,
          cursors,
          queuePath,
          projectQueuePath,
          env: process.env,
          onProgress: (p) => {
            if (!progress?.enabled) return;
            const pct = p.total > 0 ? p.index / p.total : 1;
            progress.update(
              `Parsing Grok Build ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} sessions | buckets ${formatNumber(p.bucketsQueued)}`,
            );
          },
        });
      } catch (err) {
        warnProviderParseFailure("Grok Build", err, opts);
      }
      grokResult = {
        recordsProcessed: grokResult.recordsProcessed + grokScanResult.recordsProcessed,
        eventsAggregated: grokResult.eventsAggregated + grokScanResult.eventsAggregated,
        bucketsQueued: grokResult.bucketsQueued + grokScanResult.bucketsQueued,
        projectBucketsQueued:
          (grokResult.projectBucketsQueued || 0) + (grokScanResult.projectBucketsQueued || 0),
      };
    }
    // ── GitHub Copilot App / CLI unified local runtime ──
    //
    // First adoption deliberately runs legacy OTEL + App DB once before the
    // session-store parser records its max-id barrier. Subsequent scans use
    // session-store as canonical for CLI/App requests: OTEL keeps only Chat
    // extension LogRecords, while App DB remains an observe-only baseline.
    let copilotResult = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
    const copilotSourceAllowed = sourceAllowed("copilot");
    const adoptedCopilotStorePaths = new Set(
      Object.entries(cursors?.copilotStore?.dbs || {})
        .filter(([, state]) => state?.adoptedAt)
        .map(([dbPath]) => dbPath),
    );
    const copilotStorePaths = copilotSourceAllowed
      ? Array.from(
          new Set([
            ...resolveCopilotSessionStorePaths(process.env),
            ...adoptedCopilotStorePaths,
          ]),
        ).filter((dbPath) => {
          if (adoptedCopilotStorePaths.has(dbPath)) return true;
          try { return fssync.existsSync(dbPath); } catch (_e) { return false; }
        })
      : [];
    const copilotStoreWasActive = cursors?.copilotStore?.active === true;
    const copilotStoreAdoptionFingerprints = {};
    if (!copilotStoreWasActive) {
      for (const dbPath of copilotStorePaths) {
        try {
          copilotStoreAdoptionFingerprints[dbPath] =
            getCopilotSqliteFingerprint(dbPath);
        } catch (_e) {}
      }
    }
    let copilotHadLegacyCliOtelHistory = false;
    if (!copilotStoreWasActive) {
      copilotHadLegacyCliOtelHistory =
        await copilotOtelCursorHasLegacyCliUsage(cursors);
      if (cursors?.copilot && typeof cursors.copilot === "object") {
        if (copilotHadLegacyCliOtelHistory) {
          cursors.copilot.legacyCliHistory = true;
        } else {
          cursors.copilot.usageClaimsComplete = true;
        }
      }
    }
    let copilotLegacyHealthy = true;
    let copilotOtelUsageClaims =
      cursors?.copilot?.recentUsageEvents || [];
    let copilotStoreResult = {
      active: copilotStoreWasActive,
      healthy: false,
      adoptedThisRun: false,
      canonicalDbPaths: [],
      recordsProcessed: 0,
      eventsAggregated: 0,
      bucketsQueued: 0,
      usageClaims: cursors?.copilotStore?.recentEvents || [],
    };
    if (copilotSourceAllowed && copilotStoreWasActive) {
      if (progress?.enabled) {
        progress.start(`Parsing Copilot App/CLI ${renderBar(0)} | buckets 0`);
      }
      try {
        copilotStoreResult = await parseCopilotSessionStoreIncremental({
          dbPaths: copilotStorePaths,
          cursors,
          queuePath,
          env: process.env,
          otelUsageEvents: cursors?.copilot?.recentUsageEvents,
          onProgress: (p) => {
            if (!progress?.enabled) return;
            const pct = p.total > 0 ? p.index / p.total : 1;
            progress.update(
              `Parsing Copilot App/CLI ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} events | buckets ${formatNumber(p.bucketsQueued)}`,
            );
          },
        });
        copilotResult = mergeParseResult(copilotResult, copilotStoreResult);
      } catch (err) {
        warnProviderParseFailure("Copilot App/CLI session store", err, opts);
      }
    }
    const copilotPathKey = (dbPath) => {
      const normalized = path.normalize(dbPath);
      return process.platform === "win32" ? normalized.toLowerCase() : normalized;
    };
    const canonicalCopilotStorePaths = new Set(
      (copilotStoreResult.canonicalDbPaths || []).map(copilotPathKey),
    );
    const adoptedCopilotStorePathKeys = new Set(
      Object.entries(cursors?.copilotStore?.dbs || {})
        .filter(([, state]) => state?.adoptedAt)
        .map(([dbPath]) => copilotPathKey(dbPath)),
    );
    const explicitCopilotStorePath =
      typeof process.env.TOKEN_PULSE_COPILOT_SESSION_STORE_DB === "string" &&
      process.env.TOKEN_PULSE_COPILOT_SESSION_STORE_DB.trim()
        ? copilotPathKey(
            normalizeCopilotDbPath(
              process.env.TOKEN_PULSE_COPILOT_SESSION_STORE_DB,
              process.env,
            ),
          )
        : null;
    const explicitStoreIsCanonical =
      explicitCopilotStorePath !== null &&
      canonicalCopilotStorePaths.has(explicitCopilotStorePath);
    const explicitStoreIsAdopted =
      explicitCopilotStorePath !== null &&
      adoptedCopilotStorePathKeys.has(explicitCopilotStorePath);

    // ── GitHub Copilot CLI / Chat extension (OTEL JSONL files) ──
    const copilotPaths = copilotSourceAllowed ? resolveCopilotOtelPaths(process.env) : [];
    if (copilotPaths.length > 0) {
      if (progress?.enabled) {
        progress.start(`Parsing Copilot ${renderBar(0)} | buckets 0`);
      }
      try {
        const copilotOtelResult = await parseCopilotIncremental({
          otelPaths: copilotPaths,
          cursors,
          queuePath,
          env: process.env,
          storeUsageEvents:
            copilotStoreResult.usageClaims ||
            cursors?.copilotStore?.recentEvents ||
            [],
          skipCliSpans: copilotStoreWasActive,
          onProgress: (p) => {
            if (!progress?.enabled) return;
            const pct = p.total > 0 ? p.index / p.total : 1;
            progress.update(
              `Parsing Copilot ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} files | buckets ${formatNumber(p.bucketsQueued)}`,
            );
          },
        });
        copilotOtelUsageClaims =
          copilotOtelResult.usageClaims ||
          cursors?.copilot?.recentUsageEvents ||
          [];
        copilotResult = mergeParseResult(copilotResult, copilotOtelResult);
      } catch (err) {
        copilotLegacyHealthy = false;
        warnProviderParseFailure("Copilot", err, opts);
      }
    }

    // ── GitHub Copilot App (passive data.db session summaries) ──
    const copilotAppDbStates =
      cursors?.copilotApp?.dbs && typeof cursors.copilotApp.dbs === "object"
        ? cursors.copilotApp.dbs
        : null;
    const initialTrackedCopilotAppDbPaths = new Set(
      Object.keys(copilotAppDbStates || {}),
    );
    const copilotAppDbCandidates = uniqueCopilotDbPaths(
      [
        ...resolveCopilotAppDbPaths(process.env),
        ...initialTrackedCopilotAppDbPaths,
      ],
      process.env,
      Array.from(initialTrackedCopilotAppDbPaths),
    );
    coalesceCopilotDbStatesByIdentity(
      copilotAppDbStates,
      copilotAppDbCandidates,
      mergeCopilotAppDbStates,
    );
    const explicitStoreOwnsSingleApp =
      copilotAppDbCandidates.length === 1 &&
      explicitStoreIsAdopted;
    if (copilotAppDbStates && (copilotStoreWasActive || copilotStorePaths.length > 0)) {
      for (const dbPath of Object.keys(copilotAppDbStates)) {
        try {
          fssync.statSync(dbPath);
        } catch (err) {
          const matchingStorePath = copilotPathKey(
            path.join(path.dirname(dbPath), "session-store.db"),
          );
          if (err?.code === "ENOENT") {
            if (
              canonicalCopilotStorePaths.has(matchingStorePath) ||
              (explicitStoreOwnsSingleApp && explicitStoreIsCanonical)
            ) {
              delete copilotAppDbStates[dbPath];
            }
          } else {
            if (!copilotStoreWasActive) copilotLegacyHealthy = false;
          }
        }
      }
    }
    const trackedCopilotAppDbPaths = new Set(
      Object.keys(cursors?.copilotApp?.dbs || {}),
    );
    const rawCopilotAppDbPaths = copilotSourceAllowed
      ? Array.from(new Set([
          ...copilotAppDbCandidates,
          ...trackedCopilotAppDbPaths,
        ])).filter((dbPath) => {
          if (trackedCopilotAppDbPaths.has(dbPath)) return true;
          try { return fssync.existsSync(dbPath); } catch (_e) { return false; }
        })
      : [];
    const copilotAppDbPaths = uniqueCopilotDbPaths(
      rawCopilotAppDbPaths,
      process.env,
      Array.from(trackedCopilotAppDbPaths),
    );
    if (copilotAppDbPaths.length > 0) {
      if (progress?.enabled) {
        progress.start(`Parsing Copilot App ${renderBar(0)} | buckets 0`);
      }
      for (const appDbPath of copilotAppDbPaths) {
        const storePathKey = copilotPathKey(
          path.join(path.dirname(appDbPath), "session-store.db"),
        );
        const observeOnly =
          copilotStoreWasActive ||
          adoptedCopilotStorePathKeys.has(storePathKey) ||
          (copilotAppDbPaths.length === 1 && explicitStoreIsAdopted);
        const catchupRelevant =
          !observeOnly &&
          !copilotStoreWasActive &&
          copilotStorePaths.length > 0;
        try {
          const copilotAppResult = await parseCopilotAppDbIncremental({
            dbPaths: [appDbPath],
            cursors,
            queuePath,
            env: process.env,
            observeOnly,
            onProgress: (p) => {
              if (!progress?.enabled) return;
              const pct = p.total > 0 ? p.index / p.total : 1;
              progress.update(
                `Parsing Copilot App ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} sessions | buckets ${formatNumber(p.bucketsQueued)}`,
              );
            },
          });
          copilotResult = mergeParseResult(copilotResult, copilotAppResult);
          if (catchupRelevant && copilotAppResult.dbErrors > 0) {
            copilotLegacyHealthy = false;
          }
        } catch (err) {
          if (catchupRelevant) copilotLegacyHealthy = false;
          warnProviderParseFailure("Copilot App", err, opts);
        }
      }
    }

    if (
      copilotSourceAllowed &&
      !copilotStoreWasActive &&
      copilotStorePaths.length > 0 &&
      copilotLegacyHealthy
    ) {
      if (progress?.enabled) {
        progress.start(`Adopting Copilot App/CLI store ${renderBar(0)} | buckets 0`);
      }
      try {
        const copilotHasOtelCursorHistory =
          (Array.isArray(cursors?.copilot?.seenIds) &&
            cursors.copilot.seenIds.length > 0) ||
          (cursors?.copilot?.fileOffsets &&
            Object.keys(cursors.copilot.fileOffsets).length > 0);
        const copilotOtelClaimsIncomplete =
          copilotHadLegacyCliOtelHistory ||
          (copilotHasOtelCursorHistory &&
            cursors?.copilot?.usageClaimsComplete !== true);
        const appSessionIds = [];
        const appSessionTotals = {};
        for (const dbState of Object.values(cursors?.copilotApp?.dbs || {})) {
          for (const [sessionId, totals] of Object.entries(
            dbState?.sessionTotals || {},
          )) {
            const total =
              Number(totals?.input || 0) +
              Number(totals?.output || 0) +
              Number(totals?.cached || 0) +
              Number(totals?.reasoning || 0);
            if (total <= 0) continue;
            appSessionIds.push(sessionId);
            const prior = appSessionTotals[sessionId];
            const priorTotal =
              Number(prior?.input || 0) +
              Number(prior?.output || 0) +
              Number(prior?.cached || 0) +
              Number(prior?.reasoning || 0);
            if (total > priorTotal) appSessionTotals[sessionId] = totals;
          }
        }
        copilotStoreResult = await parseCopilotSessionStoreIncremental({
          dbPaths: copilotStorePaths,
          cursors,
          queuePath,
          env: process.env,
          backfillOnFirstRun: !copilotOtelClaimsIncomplete,
          excludeSessionIdsOnFirstRun: appSessionIds,
          excludeSessionTotalsOnFirstRun: appSessionTotals,
          expectedFingerprints: copilotStoreAdoptionFingerprints,
          otelUsageEvents: copilotOtelUsageClaims,
        });
        copilotResult = mergeParseResult(copilotResult, copilotStoreResult);
      } catch (err) {
        warnProviderParseFailure("Copilot App/CLI session store", err, opts);
      }
    }

    if (copilotSourceAllowed) {
      if (Array.isArray(cursors?.copilot?.recentUsageEvents)) {
        cursors.copilot.recentUsageEvents = pruneCopilotUsageClaims(
          cursors.copilot.recentUsageEvents,
        );
      }
      if (Array.isArray(cursors?.copilotStore?.recentEvents)) {
        cursors.copilotStore.recentEvents = pruneCopilotUsageClaims(
          cursors.copilotStore.recentEvents,
        );
      }
    }


    const totalParsed =
      parseResult.filesProcessed +
      openclawResult.filesProcessed +
      claudeResult.filesProcessed +
      geminiResult.filesProcessed +
      antigravityResult.filesProcessed +
      opencodeResult.filesProcessed +
      qoderResult.recordsProcessed +
      qoderCnResult.recordsProcessed +
      claudeScienceResult.recordsProcessed +
      cursorResult.recordsProcessed +
      traeCnResult.recordsProcessed +
      traeResult.recordsProcessed +
      kiroResult.recordsProcessed +
      kiroCliResult.recordsProcessed +
      hermesResult.recordsProcessed +
      kimiResult.recordsProcessed +
      kimiCodeResult.recordsProcessed +
      codebuddyResult.recordsProcessed +
      workbuddyResult.recordsProcessed +
      workbuddyAiResult.recordsProcessed +
      ompResult.recordsProcessed +
      omoResult.recordsProcessed +
      piResult.recordsProcessed +
      primeAgentResult.recordsProcessed +
      minimaxCodeResult.recordsProcessed +
      craftResult.recordsProcessed +
      reasonixResult.recordsProcessed +
      museResult.recordsProcessed +
      grokResult.recordsProcessed +
      copilotResult.recordsProcessed +
      lmstudioResult.recordsProcessed +
      unslothResult.recordsProcessed +
      anythingllmResult.recordsProcessed +
      devinResult.recordsProcessed +
      kiloResult.recordsProcessed +
      dumateResult.recordsProcessed +
      mimoResult.recordsProcessed +
      zcodeResult.recordsProcessed +
      kilocodeResult.recordsProcessed +
      roocodeResult.recordsProcessed +
      clineResult.recordsProcessed +
      zedResult.recordsProcessed +
      gooseResult.recordsProcessed +
      dshResult.recordsProcessed +
      commandCodeResult.recordsProcessed +
      droidResult.recordsProcessed;
    const totalBuckets =
      parseResult.bucketsQueued +
      openclawResult.bucketsQueued +
      claudeResult.bucketsQueued +
      geminiResult.bucketsQueued +
      antigravityResult.bucketsQueued +
      opencodeResult.bucketsQueued +
      qoderResult.bucketsQueued +
      qoderCnResult.bucketsQueued +
      claudeScienceResult.bucketsQueued +
      cursorResult.bucketsQueued +
      traeCnResult.bucketsQueued +
      traeResult.bucketsQueued +
      kiroResult.bucketsQueued +
      kiroCliResult.bucketsQueued +
      hermesResult.bucketsQueued +
      kimiResult.bucketsQueued +
      kimiCodeResult.bucketsQueued +
      codebuddyResult.bucketsQueued +
      workbuddyResult.bucketsQueued +
      workbuddyAiResult.bucketsQueued +
      ompResult.bucketsQueued +
      omoResult.bucketsQueued +
      piResult.bucketsQueued +
      primeAgentResult.bucketsQueued +
      minimaxCodeResult.bucketsQueued +
      craftResult.bucketsQueued +
      reasonixResult.bucketsQueued +
      museResult.bucketsQueued +
      grokResult.bucketsQueued +
      copilotResult.bucketsQueued +
      lmstudioResult.bucketsQueued +
      unslothResult.bucketsQueued +
      anythingllmResult.bucketsQueued +
      devinResult.bucketsQueued +
      kiloResult.bucketsQueued +
      dumateResult.bucketsQueued +
      mimoResult.bucketsQueued +
      zcodeResult.bucketsQueued +
      kilocodeResult.bucketsQueued +
      roocodeResult.bucketsQueued +
      clineResult.bucketsQueued +
      zedResult.bucketsQueued +
      gooseResult.bucketsQueued +
      dshResult.bucketsQueued +
      commandCodeResult.bucketsQueued +
      droidResult.bucketsQueued;
    const skipNoOpCursorCommit =
      opts.auto &&
      !isFullSourceScan &&
      cursorStore.mode === "v2" &&
      cursorStore.requiresCommit !== true &&
      totalParsed === (commandCodeResult.cursorUnchanged ? commandCodeResult.recordsProcessed : 0) &&
      totalBuckets === 0 &&
      !(workbuddyResult.projectBucketsQueued > 0) &&
      !(workbuddyAiResult.projectBucketsQueued > 0) &&
      !workbuddyResult.projectCursorUpdated &&
      !workbuddyAiResult.projectCursorUpdated &&
      !(grokResult.projectBucketsQueued > 0) &&
      !(commandCodeResult.projectBucketsQueued > 0) &&
      !commandCodeResult.schemaMigrated &&
      !codexColdAuditDue &&
      !codexFallbackRetryRan &&
      !grokHookSignalConsumed;

    if (syncDiagnostics) {
      syncDiagnostics.cursor_commits = Number(syncDiagnostics.cursor_commits || 0);
      syncDiagnostics.cursor_bytes = Number(syncDiagnostics.cursor_bytes || 0);
      syncDiagnostics.cursor_path = cursorStore.currentCorePath;
    }
    if (!skipNoOpCursorCommit) {
      cursors.updatedAt = new Date().toISOString();
      await cursorStore.commit(cursors);
      if (codexAuditRecorded) {
        await cursorStore.clearDeferredCodexAuditSyncs();
      }
      if (syncDiagnostics) {
        const cursorStat = await fs.stat(cursorStore.currentCorePath);
        syncDiagnostics.cursor_commits += 1;
        syncDiagnostics.cursor_bytes += cursorStat.size;
        syncDiagnostics.cursor_path = cursorStore.currentCorePath;
      }
    } else if (codexAuditRecorded) {
      await cursorStore.writeDeferredCodexAuditSyncs(
        deferredCodexAuditSyncs + 1,
      );
    }
    if (grokHookSignalConsumed && grokHookSignalPath) {
      await fs.unlink(grokHookSignalPath).catch(() => {});
    }

    progress?.stop();

    // Report backlog for the current destination and queue identity.
    const pendingBytes = Math.max(0, (await safeStatSize(queuePath)) - await uploadedOffset(queuePath));

    if (!opts.auto) {
      const codexRecordOnlyWarning = formatRecordOnlyWarning(countRecordOnlyFiles(cursors));
      process.stdout.write(
        [
          "Sync finished:",
          `- Parsed files: ${totalParsed}`,
          `- New 30-min buckets queued: ${totalBuckets}`,
          pendingBytes > 0
            ? `- Local backlog: ${formatBytes(pendingBytes)} awaiting upload (run: token-pulse upload)`
            : "- Local backlog: none",
          codexRecordOnlyWarning ? `- Warning: ${codexRecordOnlyWarning}` : null,
          "",
        ]
          .filter(Boolean)
          .join("\n"),
      );
    }

  } finally {
    progress?.stop();
    await lock.release();
  }
}

function parseArgs(argv) {
  const out = {
    auto: false,
    fromNotify: false,
    fromRetry: false,
    fromOpenclaw: false,
    source: null,
    waitForLock: false,
    background: false,
    allLocalSources: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--auto") out.auto = true;
    else if (a === "--from-notify") out.fromNotify = true;
    else if (a === "--from-retry") out.fromRetry = true;
    else if (a === "--from-openclaw") out.fromOpenclaw = true;
    else if (a === "--source") {
      out.source = normalizeSyncSource(argv[i + 1]);
      i += 1;
    }
    else if (a.startsWith("--source=")) out.source = normalizeSyncSource(a.slice("--source=".length));
    else if (a === "--wait-for-lock") out.waitForLock = true;
    else if (a === "--background" || a === "--lightweight") out.background = true;
    else if (a === "--all-local-sources") out.allLocalSources = true;
    else throw new Error(`Unknown option: ${a}`);
  }
  return out;
}

function resolveAutoSourceScope(opts) {
  if (!opts?.auto) return null;
  if (opts.fromOpenclaw) return "openclaw";
  if (opts.fromRetry) return normalizeSyncSource(opts.source);
  if (!opts.fromNotify) return null;
  return normalizeSyncSource(opts.source);
}

function normalizeSyncSource(value) {
  if (typeof value !== "string") return null;
  const normalized = value.trim().toLowerCase();
  if (!normalized) return null;
  const aliased = AUTO_SYNC_SOURCE_ALIASES.get(normalized) || normalized;
  return AUTO_SYNC_SOURCES.has(aliased) ? aliased : null;
}

function isCodexColdScanAuditDue(
  cursors,
  nowMs = Date.now(),
  deferredSyncs = 0,
) {
  const state = cursors?.codexColdScanAudit;
  if (!state || typeof state !== "object" || state.version !== 1) return true;
  const lastFullScanAtMs = Number(state.lastFullScanAtMs);
  if (!Number.isFinite(lastFullScanAtMs) || lastFullScanAtMs <= 0) return true;
  if (Number.isFinite(nowMs) && lastFullScanAtMs - nowMs > 5 * 60 * 1000) return true;
  if (Number.isFinite(nowMs) && nowMs - lastFullScanAtMs >= CODEX_COLD_SCAN_AUDIT_INTERVAL_MS) {
    return true;
  }
  const syncsSinceFullScan =
    Number(state.syncsSinceFullScan || 0) +
    Math.max(0, Number(deferredSyncs) || 0);
  return (
    Number.isFinite(syncsSinceFullScan) &&
    syncsSinceFullScan >= CODEX_COLD_SCAN_AUDIT_MAX_SYNCS
  );
}

function recordCodexColdScanAudit(
  cursors,
  { fullAudit = false, skipped = 0, deferredSyncs = 0 } = {},
  nowMs = Date.now(),
) {
  if (!cursors || typeof cursors !== "object") return;
  const prev =
    cursors.codexColdScanAudit && typeof cursors.codexColdScanAudit === "object"
      ? cursors.codexColdScanAudit
      : {};
  const previousSyncs = Number(prev.syncsSinceFullScan || 0);
  const lastFullScanAtMs = Number(prev.lastFullScanAtMs);
  const next = {
    version: 1,
    lastFullScanAtMs: Number.isFinite(lastFullScanAtMs) && lastFullScanAtMs > 0
      ? lastFullScanAtMs
      : nowMs,
    syncsSinceFullScan:
      (Number.isFinite(previousSyncs) && previousSyncs > 0 ? previousSyncs : 0) +
      Math.max(0, Number(deferredSyncs) || 0),
    lastSkippedFiles: Math.max(0, Number(skipped) || 0),
    updatedAt: new Date(nowMs).toISOString(),
  };
  if (fullAudit) {
    next.lastFullScanAtMs = nowMs;
    next.lastFullScanAt = new Date(nowMs).toISOString();
    next.syncsSinceFullScan = 0;
  } else {
    next.lastFullScanAt = Number.isFinite(next.lastFullScanAtMs)
      ? new Date(next.lastFullScanAtMs).toISOString()
      : null;
    next.syncsSinceFullScan += 1;
  }
  cursors.codexColdScanAudit = next;
}

module.exports = {
  cmdSync,
  acquireSyncLock,
  isCodexColdScanAuditDue,
  recordCodexColdScanAudit,
};

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

function resolveOpenclawSignal({ env } = {}) {
  if (!env) return null;

  const agentId = normalizeString(env.TOKEN_PULSE_OPENCLAW_AGENT_ID);
  const sessionId = normalizeString(env.TOKEN_PULSE_OPENCLAW_PREV_SESSION_ID);
  if (!agentId || !sessionId) return null;

  // Resolve the OpenClaw home identically to the passive scanner so the
  // plugin-triggered file and passively discovered files share one path
  // spelling — otherwise the same transcript could get two cursors and be
  // counted twice (issue #264 review).
  const openclawHome = resolveOpenclawHome(env);
  const sessionFile = path.join(openclawHome, "agents", agentId, "sessions", `${sessionId}.jsonl`);

  const prevTotals = {
    totalTokens: normalizeNonNegativeInt(env.TOKEN_PULSE_OPENCLAW_PREV_TOTAL_TOKENS),
    inputTokens: normalizeNonNegativeInt(env.TOKEN_PULSE_OPENCLAW_PREV_INPUT_TOKENS),
    outputTokens: normalizeNonNegativeInt(env.TOKEN_PULSE_OPENCLAW_PREV_OUTPUT_TOKENS),
    model: normalizeString(env.TOKEN_PULSE_OPENCLAW_PREV_MODEL),
    updatedAt: normalizeIsoOrEpoch(env.TOKEN_PULSE_OPENCLAW_PREV_UPDATED_AT),
  };

  return {
    agentId,
    sessionId,
    sessionKey: normalizeString(env.TOKEN_PULSE_OPENCLAW_SESSION_KEY),
    openclawHome,
    sessionFile,
    prevTotals,
  };
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

async function safeStatSize(p) {
  try {
    const st = await fs.stat(p);
    return st && st.isFile() ? st.size : 0;
  } catch (_e) {
    return 0;
  }
}


async function writeOpenclawSignal(trackerDir) {
  const openclawSignalPath = path.join(trackerDir, "openclaw.signal");
  try {
    await fs.writeFile(openclawSignalPath, new Date().toISOString(), "utf8");
  } catch (_e) {
    // best-effort marker
  }
}
