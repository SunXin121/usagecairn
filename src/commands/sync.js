const os = require("node:os");
const path = require("node:path");
const fs = require("node:fs/promises");
const fssync = require("node:fs");
const { uploadedOffset, compactQueueBacklog } = require("./upload");
const {
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
  normalizeNonNegativeInt,
  normalizeIsoOrEpoch,
  normalizeString,
  warnProviderParseFailure,
} = require("./sync-sources");

const { resolveInstallPaths } = require("../lib/install-resolver");
const wsl = require("../lib/wsl-probe");
const {
  ensureDir,
  readJson,
  writeJson,
  openLock,
  inspectLock,
} = require("../lib/fs");
const { countRecordOnlyFiles, formatRecordOnlyWarning } = require("../lib/codex-usage-record");
const {
  listRolloutFiles,
  listRolloutFilesDeep,
  filterColdCodexRolloutFiles,
  resolveCopilotOtelPaths,
  normalizeCopilotDbPath,
  uniqueCopilotDbPaths,
  coalesceCopilotDbStatesByIdentity,
  resolveCopilotSessionStorePaths,
  getCopilotSqliteFingerprint,
  resolveCopilotAppDbPaths,
  parseRolloutIncremental,
  parseOpencodeDbIncremental,
  resolveOpenclawSessionFiles,
  resolveOpenclawHome,
  openclawCursorKey,
  copilotOtelCursorHasLegacyCliUsage,
  pruneCopilotUsageClaims,
  parseCopilotIncremental,
  parseCopilotSessionStoreIncremental,
  parseCopilotAppDbIncremental,
  resolveGrokBuildSessions,
  parseGrokBuildIncremental,
} = require("../lib/rollout");
const { createProgress, renderBar, formatNumber, formatBytes } = require("../lib/progress");
const { isCursorStoreRetry, openCursorStore } = require("../lib/cursor-store");
const { resolveTrackerPaths } = require("../lib/pulse-paths");
const {
  appendUniqueDirs,
  extraScanRootPaths,
  hasAnyScanChild,
  resolveEnvRoot,
  resolveScanRoots,
} = require("../lib/scan-roots");

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
  "pi-desktop",
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
// nothing for `usagecairn status` to report (issue #431). Leave a queryable
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

    function makeProviderProgress(label) {
      return (p) => {
        if (!progress?.enabled) return;
        const pct = p.total > 0 ? p.index / p.total : 1;
        progress.update(
          `Parsing ${label} ${renderBar(pct)} ${formatNumber(p.index)}/${formatNumber(p.total)} records | buckets ${formatNumber(p.bucketsQueued)}`,
        );
      };
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
    const { openclawResult } = await syncOpenclaw({
      opts,
      trackerDir,
      queuePath,
      projectQueuePath,
      cursors,
      openclawSignal,
      sourceAllowed,
      openclawFiles,
    });
    const { claudeResult } = await syncClaude({
      opts,
      progress,
      queuePath,
      projectQueuePath,
      cursors,
      claudeProjectsDirs,
      sourceAllowed,
    });
    const { geminiPaths, geminiResult } = await syncGemini({
      opts,
      home,
      progress,
      queuePath,
      projectQueuePath,
      cursors,
      sourceAllowed,
    });
    const { antigravityResult } = await syncAntigravity({
      opts,
      progress,
      queuePath,
      projectQueuePath,
      cursors,
      sourceAllowed,
      geminiPaths,
    });
    const { opencodeResult } = await syncOpencode({
      progress,
      queuePath,
      projectQueuePath,
      cursorStore,
      cursors,
      xdgDataHome,
      sourceAllowed,
    });
    const { qoderResult } = await syncQoder({
      opts,
      home,
      progress,
      queuePath,
      projectQueuePath,
      cursors,
      sourceAllowed,
      makeProviderProgress,
    });
    const { qoderCnResult } = await syncQoderCn({
      opts,
      home,
      progress,
      queuePath,
      projectQueuePath,
      cursors,
      sourceAllowed,
      makeProviderProgress,
    });
    const { claudeScienceResult } = await syncClaudeScience({
      opts,
      home,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
      makeProviderProgress,
    });
    const { kiloResult } = await syncKilo({
      opts,
      progress,
      queuePath,
      projectQueuePath,
      cursors,
      kiloHome,
      sourceAllowed,
      parseOpencodeDbForInstall,
      makeProviderProgress,
    });
    const { dumateResult } = await syncDumate({
      opts,
      home,
      progress,
      queuePath,
      projectQueuePath,
      cursors,
      sourceAllowed,
      makeProviderProgress,
    });
    const { mimoResult } = await syncMimo({
      opts,
      home,
      progress,
      queuePath,
      projectQueuePath,
      cursors,
      sourceAllowed,
      parseOpencodeDbForInstall,
      makeProviderProgress,
    });
    const { zcodeResult } = await syncZcode({
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
    });
    const { dshResult } = await syncDsh({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
      makeProviderProgress,
    });
    const { commandCodeResult } = await syncCommandCode({
      opts,
      progress,
      queuePath,
      projectQueuePath,
      cursors,
      sourceAllowed,
      makeProviderProgress,
    });
    const { lmstudioResult } = await syncLmstudio({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
      makeProviderProgress,
    });
    const { unslothResult } = await syncUnsloth({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
      makeProviderProgress,
    });
    const { anythingllmResult } = await syncAnythingllm({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
      makeProviderProgress,
    });
    const { devinResult } = await syncDevin({
      opts,
      progress,
      queuePath,
      projectQueuePath,
      cursors,
      sourceAllowed,
      makeProviderProgress,
    });
    const { kilocodeResult } = await syncKilocode({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
    });
    const { gooseResult } = await syncGoose({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
      gooseOnProgress,
    });
    const { droidResult } = await syncDroid({
      opts,
      progress,
      queuePath,
      projectQueuePath,
      cursors,
      sourceAllowed,
    });
    const { zedResult } = await syncZed({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
      zedOnProgress,
    });
    const { roocodeResult } = await syncRoocode({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
    });
    const { clineResult } = await syncCline({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
    });
    const { cursorResult } = await syncCursor({
      opts,
      cursorSyncDeps,
      home,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
    });
    const { traeResult } = await syncTrae({
      opts,
      queuePath,
      cursors,
      sourceAllowed,
      makeProviderProgress,
    });
    const { traeCnResult } = await syncTraeCn({
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
    });
    const { kiroResult } = await syncKiro({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
      makeProviderProgress,
    });
    const { hermesResult } = await syncHermes({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
      hermesOnProgress,
    });
    const { kiroCliResult } = await syncKiroCli({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
    });
    const { kimiResult, kimiCodeWireFiles } = await syncKimi({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
    });
    const { kimiCodeResult } = await syncKimiCode({
      opts,
      progress,
      queuePath,
      cursors,
      kimiCodeWireFiles,
    });
    const { codebuddyResult } = await syncCodebuddy({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
    });
    const { workbuddyResult } = await syncWorkbuddy({
      opts,
      progress,
      queuePath,
      projectQueuePath,
      cursors,
      sourceAllowed,
    });
    const { workbuddyAiResult } = await syncWorkbuddyAi({
      opts,
      progress,
      queuePath,
      projectQueuePath,
      cursors,
      sourceAllowed,
    });
    const { ompResult } = await syncOmp({
      opts,
      progress,
      queuePath,
      projectQueuePath,
      cursors,
      sourceAllowed,
    });
    const { omoResult } = await syncOmo({
      opts,
      progress,
      queuePath,
      projectQueuePath,
      cursors,
      sourceAllowed,
    });
    const { piResult } = await syncPi({
      opts,
      progress,
      queuePath,
      projectQueuePath,
      cursors,
      sourceAllowed,
    });
    const { piDesktopResult } = await syncPiDesktop({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
    });
    const { primeAgentResult } = await syncPrimeAgent({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
    });
    const { minimaxCodeResult } = await syncMinimaxCode({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
    });
    const { craftResult } = await syncCraft({
      opts,
      progress,
      queuePath,
      cursors,
      sourceAllowed,
    });
    const { reasonixResult } = await syncReasonix({
      opts,
      queuePath,
      cursors,
      sourceAllowed,
      makeProviderProgress,
    });
    const { museResult } = await syncMuse({
      opts,
      queuePath,
      cursors,
      sourceAllowed,
      makeProviderProgress,
    });

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
      piDesktopResult.recordsProcessed +
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
      piDesktopResult.bucketsQueued +
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

    // Still under sync.lock, so no parser is appending. Compaction is an
    // optimization: a failure leaves the queue as it was and must not fail sync.
    try {
      await compactQueueBacklog(queuePath);
    } catch (error) {
      if (!opts.auto) process.stderr.write(`Warning: queue compaction skipped: ${error.message}\n`);
    }

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
            ? `- Local backlog: ${formatBytes(pendingBytes)} awaiting upload (run: usagecairn upload)`
            : "- Local backlog: none",
          codexRecordOnlyWarning ? `- Warning: ${codexRecordOnlyWarning}` : null,
        ]
          .filter(Boolean)
          .join("\n") + "\n",
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
