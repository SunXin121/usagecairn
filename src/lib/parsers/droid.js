// droid usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const readline = require("node:readline");
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
  buildProjectFileContext,
  isProjectFileContextFresh,
  resolveProjectContextForPath,
  pickWin32ProviderPath,
  expandHomePath,
} = require("./core");

// ─────────────────────────────────────────────────────────────────────────────
// Droid (Factory CLI) — passive reader for ~/.factory/sessions/**/*.settings.json
//
// Each Droid session has two sibling files:
//   <session-id>.jsonl           — per-message transcript (no token counts)
//   <session-id>.settings.json   — JSON object whose tokenUsage holds the
//                                  CUMULATIVE session-level total:
//     {
//       "model": "custom:GLM-5.1-[Proxy]-0",
//       "providerLock": "anthropic",
//       "providerLockTimestamp": "2026-05-21T12:34:56.000Z",
//       "tokenUsage": {
//         "inputTokens": 12345,         // already excludes cached reads
//         "outputTokens": 678,
//         "cacheCreationTokens": 0,
//         "cacheReadTokens": 0,
//         "thinkingTokens": 0
//       }
//     }
//
// Droid records totals at session granularity (not per message). We treat each
// settings file as a cumulative counter and emit (current - previous) deltas,
// the same cumulative-delta pattern as Goose/Cursor. Bucket timestamp is the
// settings file's mtime — the file is rewritten each turn, so mtime is the
// most accurate "when did these new tokens land" signal we have.
// ─────────────────────────────────────────────────────────────────────────────

function resolveDroidSessionsDirs(env = process.env) {
  if (typeof env.DROID_SESSIONS_DIR === "string" && env.DROID_SESSIONS_DIR.trim()) {
    return env.DROID_SESSIONS_DIR.split(",")
      .map((d) => expandHomePath(d.trim(), env))
      .filter(Boolean);
  }
  if (typeof env.FACTORY_DIR === "string" && env.FACTORY_DIR.trim()) {
    return [path.join(expandHomePath(env.FACTORY_DIR.trim(), env), "sessions")];
  }
  const home = env.HOME || os.homedir();
  if (process.platform === "win32") {
    const picked = pickWin32ProviderPath({
      env,
      nativeValue: path.join(home, ".factory", "sessions"),
      wslProviderDir: ".factory/sessions",
    });
    return picked ? [picked] : [];
  }
  return [path.join(home, ".factory", "sessions")];
}

function resolveDroidSessionsDir(env = process.env) {
  return resolveDroidSessionsDirs(env)[0];
}

function listDroidSettingsFiles(env = process.env) {
  const dirs = resolveDroidSessionsDirs(env);
  const out = [];
  const walk = (dir) => {
    let entries;
    try {
      entries = fssync.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(full);
      } else if (entry.isFile() && entry.name.endsWith(".settings.json")) {
        out.push(full);
      }
    }
  };
  for (const dir of dirs) {
    if (!fssync.existsSync(dir)) continue;
    walk(dir);
  }
  out.sort((a, b) => a.localeCompare(b));
  return out;
}

// Strip Droid's wrapper to leave a comparable model id. Mirrors ccusage's
// `normalize_droid_model_name` (rust/crates/ccusage/src/adapter/droid/parser.rs)
// so the same input produces the same bucket key across both tools:
//   "custom:GLM-5.1-[Proxy]-0"        -> "glm-5-1-0"
//   "anthropic/claude-sonnet-4-5"     -> "anthropic/claude-sonnet-4-5"
//   "glm_5_1"                          -> "glm_5_1"   (underscore preserved)
// IMPORTANT: only whitespace, `.`, and existing dashes collapse to a single
// `-`. Underscores are kept verbatim — diverging here would split `glm_5_1`
// rows from ccusage's equivalent rows in cross-tool comparisons.
function normalizeDroidModelName(raw) {
  if (typeof raw !== "string") return "";
  let s = raw.startsWith("custom:") ? raw.slice("custom:".length) : raw;
  s = s.replace(/\[[^\]]*\]/g, "");
  s = s.toLowerCase();
  s = s.replace(/[\s.]+/g, "-");
  s = s.replace(/-+/g, "-");
  s = s.replace(/^-+|-+$/g, "");
  return s;
}

// Mirror ccusage's `normalize_droid_provider`: collapse aliases for the four
// known upstream families. Anything else falls through to the literal value
// (or "unknown" when the input is empty/garbage).
function normalizeDroidProvider(raw) {
  if (typeof raw !== "string") return "unknown";
  const v = raw.trim().toLowerCase().replace(/-/g, "_");
  if (!v) return "unknown";
  if (v === "claude" || v === "anthropic") return "anthropic";
  if (v === "openai") return "openai";
  if (
    v === "google" ||
    v === "google_ai" ||
    v === "gemini" ||
    v === "vertex" ||
    v === "vertex_ai"
  )
    return "google";
  if (v === "xai" || v === "x_ai" || v === "grok") return "xai";
  return v;
}

// When `providerLock` is missing, ccusage infers the family from the model
// name itself. We replicate the same heuristic so empty-providerLock sessions
// still bucket into `claude-unknown` / `gpt-unknown` / etc. rather than a
// generic "unknown".
function inferDroidProviderFromModel(model) {
  if (typeof model !== "string" || !model) return "unknown";
  const m = model.toLowerCase();
  if (
    m.includes("claude") ||
    m.includes("opus") ||
    m.includes("sonnet") ||
    m.includes("haiku")
  )
    return "anthropic";
  if (
    m.startsWith("gpt-") ||
    m.includes("-gpt-") ||
    m.includes("chatgpt") ||
    /^o\d/.test(m)
  )
    return "openai";
  if (m.includes("gemini")) return "google";
  if (m.includes("grok")) return "xai";
  return "unknown";
}

function defaultDroidModelForProvider(provider) {
  switch (provider) {
    case "anthropic":
      return "claude-unknown";
    case "openai":
      return "gpt-unknown";
    case "google":
      return "gemini-unknown";
    case "xai":
      return "grok-unknown";
    default:
      return "unknown";
  }
}

// When `settings.model` is missing, ccusage scans the sibling `<id>.jsonl`
// transcript for a line containing `Model:` and pulls the name from there.
// We mirror that exactly — same first-500-lines cap, same terminator chars
// (`"`, `\`, `[`) — so empty-model droid sessions don't all bucket under
// "unknown".
function extractDroidModelFromSidecarJsonl(settingsPath) {
  if (typeof settingsPath !== "string") return "";
  if (!settingsPath.endsWith(".settings.json")) return "";
  const sidecar = settingsPath.slice(0, -".settings.json".length) + ".jsonl";
  let raw;
  try {
    raw = fssync.readFileSync(sidecar, "utf8");
  } catch {
    return "";
  }
  const lines = raw.split("\n");
  const limit = Math.min(lines.length, 500);
  for (let i = 0; i < limit; i++) {
    const idx = lines[i].indexOf("Model:");
    if (idx < 0) continue;
    const tail = lines[i].slice(idx + "Model:".length);
    // Stop at the first quote, backslash, or bracket — mirrors ccusage.
    let cut = tail.length;
    for (const ch of ['"', "\\", "["]) {
      const p = tail.indexOf(ch);
      if (p >= 0 && p < cut) cut = p;
    }
    const candidate = tail.slice(0, cut).trim();
    if (!candidate) continue;
    const normalized = normalizeDroidModelName(candidate);
    if (normalized) return normalized;
  }
  return "";
}

// ccusage's `apply_total_token_fallback`: if the five detail counters
// underflow the session's `totalTokens`, attribute the gap. Prefer assigning
// it to output (the field most likely to be missing on older settings.json
// schemas); if output is already populated, fold the extra into the thinking
// (reasoning_output_tokens) channel so total stays consistent. Mirrors
// rust/crates/ccusage/src/utils.rs verbatim.
function applyDroidTotalFallback(usage) {
  const known =
    usage.input + usage.output + usage.cacheCreation + usage.cacheRead + usage.thinking;
  const total = usage.totalTokens || 0;
  const missing = total > known ? total - known : 0;
  if (missing === 0) return usage;
  if (usage.output === 0) {
    return { ...usage, output: missing };
  }
  return { ...usage, thinking: usage.thinking + missing };
}

// Session id = basename minus `.settings.json`, mirroring ccusage's keying.
// Stable across FACTORY_DIR / HOME / mount-point moves because Droid uses
// UUID-style session ids (collision risk between projects is negligible).
function droidSessionIdFromPath(filePath) {
  if (typeof filePath !== "string" || !filePath) return "";
  const base = path.basename(filePath);
  if (!base.endsWith(".settings.json")) return "";
  return base.slice(0, -".settings.json".length);
}

// Droid's workspace directory slug is lossy: path separators become `-`, which
// collides with literal dashes in directory names. The sibling transcript keeps
// the launch cwd losslessly in its session_start record, so project attribution
// must read that value instead of trying to decode the directory name.
const DROID_CWD_SCAN_MAX_BYTES = 65536;

async function resolveDroidFileCwd(settingsPath) {
  if (typeof settingsPath !== "string" || !settingsPath.endsWith(".settings.json")) {
    return null;
  }
  const sidecarPath = settingsPath.slice(0, -".settings.json".length) + ".jsonl";
  let stream;
  try {
    stream = fssync.createReadStream(sidecarPath, {
      encoding: "utf8",
      start: 0,
      end: DROID_CWD_SCAN_MAX_BYTES,
    });
  } catch {
    return null;
  }
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  try {
    for await (const line of rl) {
      if (!line || !line.includes('"cwd"')) continue;
      let entry;
      try {
        entry = JSON.parse(line);
      } catch {
        continue;
      }
      if (
        (entry?.type === "session_start" || entry?.type === "session") &&
        typeof entry.cwd === "string" &&
        entry.cwd.trim()
      ) {
        return entry.cwd.trim();
      }
    }
  } catch {
    return null;
  } finally {
    rl.close();
    stream.close?.();
  }
  return null;
}

// Resolve a Droid bucket model id. ccusage's chain: settings.model → sidecar
// <id>.jsonl scrape → `<provider>-unknown` derived from providerLock or inferred
// from the model fragment. Extracted so the dup-session repair migration can
// reproduce the exact same bucket key a settings file would have emitted under.
function resolveDroidModel(settings, filePath) {
  let model = normalizeDroidModelName(settings.model);
  if (!model) model = extractDroidModelFromSidecarJsonl(filePath);
  if (!model) {
    let provider = normalizeDroidProvider(settings.providerLock);
    if (provider === "unknown") {
      provider = inferDroidProviderFromModel(settings.model || "");
    }
    model = defaultDroidModelForProvider(provider);
  }
  return model;
}

// When the SAME Droid session id (the basename, which is the cursor key) appears
// in more than one folder under ~/.factory/sessions, every such file shares one
// sessionTotals[sessionId] entry. Processing them in a single parse loop makes the
// lower-count file look like a session reset and re-emit the full cumulative on
// every sync — unbounded inflation (issue #204). De-dupe to ONE canonical file per
// session id BEFORE the loop. Canonical = the most complete cumulative snapshot:
// largest max(five-field sum, totalTokens) (applyDroidTotalFallback already spills
// totalTokens into the five fields, so summing the filled fields IS that max);
// ties go to the newest mtime, then the lexicographically smaller path. A session
// id with a single file passes through untouched (no disk read).
function dedupeDroidSettingsFilesBySession(files) {
  const list = Array.isArray(files) ? files : [];
  const groups = new Map();
  for (const filePath of list) {
    if (typeof filePath !== "string") continue;
    const sessionId = droidSessionIdFromPath(filePath);
    if (!sessionId) continue;
    if (!groups.has(sessionId)) groups.set(sessionId, []);
    groups.get(sessionId).push(filePath);
  }
  const out = [];
  for (const group of groups.values()) {
    if (group.length === 1) {
      out.push(group[0]);
      continue;
    }
    let best = null;
    let bestMetric = -1;
    let bestMtime = -1;
    for (const filePath of group) {
      let mtimeMs = 0;
      try {
        mtimeMs = fssync.statSync(filePath).mtimeMs;
      } catch {
        continue;
      }
      let settings;
      try {
        settings = JSON.parse(fssync.readFileSync(filePath, "utf8"));
      } catch {
        continue;
      }
      const usage =
        settings && typeof settings === "object" && settings.tokenUsage
          ? settings.tokenUsage
          : {};
      const filled = applyDroidTotalFallback({
        input: Math.max(0, Number(usage.inputTokens || 0)),
        output: Math.max(0, Number(usage.outputTokens || 0)),
        cacheCreation: Math.max(0, Number(usage.cacheCreationTokens || 0)),
        cacheRead: Math.max(0, Number(usage.cacheReadTokens || 0)),
        thinking: Math.max(0, Number(usage.thinkingTokens || 0)),
        totalTokens: Math.max(0, Number(usage.totalTokens || 0)),
      });
      const metric =
        filled.input +
        filled.output +
        filled.cacheCreation +
        filled.cacheRead +
        filled.thinking;
      const better =
        metric > bestMetric ||
        (metric === bestMetric && mtimeMs > bestMtime) ||
        (metric === bestMetric &&
          mtimeMs === bestMtime &&
          (best === null || filePath.localeCompare(best) < 0));
      if (better) {
        best = filePath;
        bestMetric = metric;
        bestMtime = mtimeMs;
      }
    }
    out.push(best || group[0]);
  }
  out.sort((a, b) => a.localeCompare(b));
  return out;
}

async function parseDroidIncremental({
  settingsFiles,
  cursors,
  queuePath,
  projectQueuePath,
  publicRepoResolver,
  onProgress,
  env,
  // `prune: true` (the production default) drops cursor entries whose session
  // id was not observed this run — handles `.settings.json` files removed
  // off disk so the cursor doesn't grow unbounded. Tests that pass an
  // intentionally partial `settingsFiles` list should set `prune: false` to
  // keep unobserved entries.
  prune = true,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const projectEnabled = typeof projectQueuePath === "string" && projectQueuePath.length > 0;
  const droidState =
    cursors.droid && typeof cursors.droid === "object" ? cursors.droid : {};
  const sessionTotals =
    droidState.sessionTotals && typeof droidState.sessionTotals === "object"
      ? { ...droidState.sessionTotals }
      : {};
  const projectSessionTotals =
    droidState.projectSessionTotals && typeof droidState.projectSessionTotals === "object"
      ? { ...droidState.projectSessionTotals }
      : {};

  const files = dedupeDroidSettingsFilesBySession(
    Array.isArray(settingsFiles)
      ? settingsFiles
      : listDroidSettingsFiles(env || process.env),
  );

  if (files.length === 0) {
    cursors.droid = {
      ...droidState,
      sessionTotals,
      ...(projectEnabled ? { projectSessionTotals } : {}),
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
  const projectFreshnessCache = projectEnabled ? new Map() : null;
  const cb = typeof onProgress === "function" ? onProgress : null;
  let recordsProcessed = 0;
  let eventsAggregated = 0;

  // Track which session ids we observed this run so we can prune cursor
  // entries for files that disappeared off disk — keeps the cursor bounded
  // by actual session count without the false-first-sight re-emit bug that
  // a fixed-N cap would introduce (evicted-but-still-on-disk entries would
  // resurrect as zero-prev on the next sync and re-count their cumulative).
  const seenSessionIds = new Set();

  for (let i = 0; i < files.length; i++) {
    const filePath = files[i];
    recordsProcessed++;

    let mtimeMs = 0;
    try {
      mtimeMs = fssync.statSync(filePath).mtimeMs;
    } catch (e) {
      if (e && e.code === "ENOENT") continue;
      throw e;
    }

    // Key by session id (the UUID-style filename without `.settings.json`)
    // so the cursor survives FACTORY_DIR / HOME / mount-point migrations.
    // Mirrors ccusage's session_id derivation (parser.rs::load_settings_file).
    const sessionId = droidSessionIdFromPath(filePath);
    if (!sessionId) continue;
    seenSessionIds.add(sessionId);

    const prev = sessionTotals[sessionId] || {
      input: 0,
      output: 0,
      cacheCreation: 0,
      cacheRead: 0,
      thinking: 0,
      mtimeMs: 0,
    };
    const projectPrev = projectSessionTotals[sessionId] || {
      input: 0,
      output: 0,
      cacheCreation: 0,
      cacheRead: 0,
      thinking: 0,
      mtimeMs: 0,
      filePath: null,
      attributed: false,
      projectFileContext: null,
    };
    const isFirstSeenSession = !sessionTotals[sessionId];
    const globalNeedsUpdate = !(mtimeMs && mtimeMs === prev.mtimeMs);
    let projectNeedsUpdate = false;
    if (projectEnabled) {
      const projectFileChanged =
        !mtimeMs || mtimeMs !== projectPrev.mtimeMs || projectPrev.filePath !== filePath;
      if (!projectSessionTotals[sessionId] || projectFileChanged) {
        projectNeedsUpdate = true;
      } else if (projectPrev.attributed !== true) {
        projectNeedsUpdate = !(await isProjectFileContextFresh(
          projectPrev.projectFileContext,
          { freshnessCache: projectFreshnessCache },
        ));
      }
    }
    if (!globalNeedsUpdate && !projectNeedsUpdate) continue;

    let raw;
    try {
      raw = fssync.readFileSync(filePath, "utf8");
    } catch {
      continue;
    }
    let settings;
    try {
      settings = JSON.parse(raw);
    } catch {
      continue;
    }
    if (!settings || typeof settings !== "object") continue;
    const tokenUsage = settings.tokenUsage;
    if (!tokenUsage || typeof tokenUsage !== "object") continue;

    const filled = applyDroidTotalFallback({
      input: Math.max(0, Number(tokenUsage.inputTokens || 0)),
      output: Math.max(0, Number(tokenUsage.outputTokens || 0)),
      cacheCreation: Math.max(0, Number(tokenUsage.cacheCreationTokens || 0)),
      cacheRead: Math.max(0, Number(tokenUsage.cacheReadTokens || 0)),
      thinking: Math.max(0, Number(tokenUsage.thinkingTokens || 0)),
      totalTokens: Math.max(0, Number(tokenUsage.totalTokens || 0)),
    });
    const inputNow = filled.input;
    const outputNow = filled.output;
    const cacheCreationNow = filled.cacheCreation;
    const cacheReadNow = filled.cacheRead;
    const thinkingNow = filled.thinking;
    const sumNow =
      inputNow + outputNow + cacheCreationNow + cacheReadNow + thinkingNow;
    const sumPrev =
      prev.input + prev.output + prev.cacheCreation + prev.cacheRead + prev.thinking;
    const projectSumPrev =
      projectPrev.input +
      projectPrev.output +
      projectPrev.cacheCreation +
      projectPrev.cacheRead +
      projectPrev.thinking;

    // Transient empty: settings.json was observed with zero tokens (mid-write
    // or a brief wipe before the next turn restores totals). Do NOT clobber
    // the existing per-field baseline — only bump mtimeMs so we don't re-read
    // the same empty payload next sync. If we overwrote prev with zeros, a
    // later non-empty read would emit the full cumulative as a fresh delta.
    if (sumNow === 0) {
      if (globalNeedsUpdate) {
        if (sumPrev > 0) {
          sessionTotals[sessionId] = { ...prev, mtimeMs };
        } else {
          sessionTotals[sessionId] = {
            input: 0,
            output: 0,
            cacheCreation: 0,
            cacheRead: 0,
            thinking: 0,
            mtimeMs,
          };
        }
      }
      if (projectNeedsUpdate) {
        projectSessionTotals[sessionId] =
          projectSumPrev > 0
            ? { ...projectPrev, mtimeMs, filePath }
            : {
                input: 0,
                output: 0,
                cacheCreation: 0,
                cacheRead: 0,
                thinking: 0,
                mtimeMs,
                filePath,
                attributed: false,
                projectFileContext: buildProjectFileContext(null),
              };
      }
      continue;
    }

    const bucketStart = toUtcHalfHourStart(
      new Date(mtimeMs || Date.now()).toISOString(),
    );
    if (!bucketStart) continue;

    // Model resolution mirrors ccusage's chain: settings.model → sidecar
    // <id>.jsonl scrape → `<provider>-unknown` derived from providerLock or
    // inferred from the model fragment we did find. Same fallback string set
    // (claude-unknown / gpt-unknown / gemini-unknown / grok-unknown) so
    // empty-model sessions bucket identically across both tools.
    const model = resolveDroidModel(settings, filePath);

    if (globalNeedsUpdate) {
      // Reset only when the TOTAL shrinks — a real session reuse (Droid wiped
      // tokenUsage and started over). A single field dropping while the sum
      // grows is a schema change or cache eviction; clamping per-field deltas
      // to >=0 is the right behavior for those.
      const isReset = sumNow < sumPrev;
      const dInput = isReset ? inputNow : Math.max(0, inputNow - prev.input);
      const dOutput = isReset ? outputNow : Math.max(0, outputNow - prev.output);
      const dCacheCreation = isReset
        ? cacheCreationNow
        : Math.max(0, cacheCreationNow - prev.cacheCreation);
      const dCacheRead = isReset
        ? cacheReadNow
        : Math.max(0, cacheReadNow - prev.cacheRead);
      const dThinking = isReset
        ? thinkingNow
        : Math.max(0, thinkingNow - prev.thinking);

      if (dInput + dOutput + dCacheCreation + dCacheRead + dThinking > 0) {
        // Token normalization: inputTokens already excludes cache reads (matches
        // Anthropic API convention), so cache columns slot in directly. Thinking
        // is reasoning_output_tokens — folded into cost via existing pricing path.
        const bucketDelta = {
          input_tokens: dInput,
          cached_input_tokens: dCacheRead,
          cache_creation_input_tokens: dCacheCreation,
          output_tokens: dOutput,
          reasoning_output_tokens: dThinking,
          total_tokens: dInput + dOutput + dCacheCreation + dCacheRead + dThinking,
          conversation_count: isFirstSeenSession || isReset ? 1 : 0,
        };
        const bucket = getHourlyBucket(hourlyState, "droid", model, bucketStart);
        addTotals(bucket.totals, bucketDelta);
        touchedBuckets.add(bucketKey("droid", model, bucketStart));
        eventsAggregated++;
      }

      sessionTotals[sessionId] = {
        input: inputNow,
        output: outputNow,
        cacheCreation: cacheCreationNow,
        cacheRead: cacheReadNow,
        thinking: thinkingNow,
        mtimeMs,
      };
    }

    if (projectNeedsUpdate) {
      const cwd = await resolveDroidFileCwd(filePath);
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
      const projectFileContext = buildProjectFileContext(projectContext);

      // Keep this baseline independent from the global cursor so project
      // attribution can be backfilled once without replaying global usage.
      // If the repo is not public/verified yet, leave it unadvanced so a later
      // sync can retry after project metadata or network availability changes.
      if (projectKey && projectRef) {
        const projectReset = sumNow < projectSumPrev;
        const projectDInput = projectReset
          ? inputNow
          : Math.max(0, inputNow - projectPrev.input);
        const projectDOutput = projectReset
          ? outputNow
          : Math.max(0, outputNow - projectPrev.output);
        const projectDCacheCreation = projectReset
          ? cacheCreationNow
          : Math.max(0, cacheCreationNow - projectPrev.cacheCreation);
        const projectDCacheRead = projectReset
          ? cacheReadNow
          : Math.max(0, cacheReadNow - projectPrev.cacheRead);
        const projectDThinking = projectReset
          ? thinkingNow
          : Math.max(0, thinkingNow - projectPrev.thinking);
        const projectTotal =
          projectDInput +
          projectDOutput +
          projectDCacheCreation +
          projectDCacheRead +
          projectDThinking;
        if (projectTotal > 0) {
          const projectDelta = {
            input_tokens: projectDInput,
            cached_input_tokens: projectDCacheRead,
            cache_creation_input_tokens: projectDCacheCreation,
            output_tokens: projectDOutput,
            reasoning_output_tokens: projectDThinking,
            total_tokens: projectTotal,
            conversation_count: !projectSessionTotals[sessionId] || projectReset ? 1 : 0,
          };
          const projectBucket = getProjectBucket(
            projectState,
            projectKey,
            "droid",
            bucketStart,
            projectRef,
          );
          addTotals(projectBucket.totals, projectDelta);
          projectTouchedBuckets.add(
            projectBucketKey(projectKey, "droid", bucketStart),
          );
        }
        projectSessionTotals[sessionId] = {
          input: inputNow,
          output: outputNow,
          cacheCreation: cacheCreationNow,
          cacheRead: cacheReadNow,
          thinking: thinkingNow,
          mtimeMs,
          filePath,
          projectKey,
          projectRef,
          attributed: true,
          projectFileContext,
        };
      } else {
        projectSessionTotals[sessionId] = {
          input: 0,
          output: 0,
          cacheCreation: 0,
          cacheRead: 0,
          thinking: 0,
          mtimeMs,
          filePath,
          projectKey: null,
          projectRef,
          attributed: false,
          projectFileContext,
        };
      }
    }

    if (cb) {
      cb({
        index: i + 1,
        total: files.length,
        recordsProcessed,
        eventsAggregated,
        bucketsQueued: touchedBuckets.size,
      });
    }
  }

  // Prune cursor entries for sessions that no longer appear on disk. Driven
  // by an explicit `prune` flag (default true) — not by the shape of
  // `settingsFiles` — so production callers that pass an explicit file list
  // still get pruning, while tests passing an intentionally partial subset
  // can opt out with `prune: false`.
  if (prune) {
    for (const id of Object.keys(sessionTotals)) {
      if (!seenSessionIds.has(id)) delete sessionTotals[id];
    }
    for (const id of Object.keys(projectSessionTotals)) {
      if (!seenSessionIds.has(id)) delete projectSessionTotals[id];
    }
  }

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
  cursors.droid = {
    ...droidState,
    sessionTotals,
    ...(projectEnabled ? { projectSessionTotals } : {}),
    updatedAt,
  };

  return { recordsProcessed, eventsAggregated, bucketsQueued, projectBucketsQueued };
}

module.exports = {
  resolveDroidSessionsDirs,
  resolveDroidSessionsDir,
  listDroidSettingsFiles,
  normalizeDroidModelName,
  normalizeDroidProvider,
  inferDroidProviderFromModel,
  defaultDroidModelForProvider,
  extractDroidModelFromSidecarJsonl,
  applyDroidTotalFallback,
  droidSessionIdFromPath,
  resolveDroidModel,
  dedupeDroidSettingsFilesBySession,
  parseDroidIncremental,
};
