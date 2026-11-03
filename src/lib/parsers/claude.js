// claude usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const { claudeUserIdentity, collectClaudeForkAliases } = require("../claude-user-identity");
const fs = require("node:fs/promises");
const fssync = require("node:fs");
const path = require("node:path");
const readline = require("node:readline");
const crypto = require("node:crypto");
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
  toUtcHalfHourStart,
  bucketKey,
  projectBucketKey,
  normalizeSourceInput,
  normalizeModelInput,
  buildProjectFileContext,
  isProjectFileContextFresh,
  resolveProjectContextForPath,
  isAllZeroUsage,
  toNonNegativeInt,
  safeReadDir,
  isUncPath,
} = require("./core");

async function listClaudeProjectFiles(projectsDir) {
  const out = [];
  await walkClaudeProjects(projectsDir, out);
  out.sort((a, b) => a.localeCompare(b));
  return out;
}

// Identity hash for cross-environment duplicate session files (native and
// \\wsl$ views of a synced ~/.claude). sha256 of the first 64KB plus the
// file size: Claude session headers carry sessionId/cwd so the prefix is
// discriminating, and folding in the size keeps a grown file distinct from
// a stale copy. Deliberately excludes inode (unstable across the 9p bridge)
// and mtime (rewritten by sync tools).
const CLAUDE_FILE_ID_PREFIX_BYTES = 64 * 1024;
async function claudeFileIdentityHash(filePath, st) {
  const length = Math.min(CLAUDE_FILE_ID_PREFIX_BYTES, st.size);
  if (length <= 0) return null;
  let fh = null;
  try {
    fh = await fs.open(filePath, "r");
    const buf = Buffer.alloc(length);
    const { bytesRead } = await fh.read(buf, 0, length, 0);
    const hash = crypto.createHash("sha256").update(buf.subarray(0, bytesRead)).digest("hex");
    return `${hash}:${st.size}`;
  } catch (_e) {
    return null;
  } finally {
    if (fh) await fh.close().catch(() => { });
  }
}

async function parseClaudeIncremental({
  projectFiles,
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
  const files = Array.isArray(projectFiles) ? projectFiles : [];
  const totalFiles = files.length;
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const projectEnabled = typeof projectQueuePath === "string" && projectQueuePath.length > 0;
  const projectState = projectEnabled ? normalizeProjectState(cursors?.projectHourly) : null;
  const projectTouchedBuckets = projectEnabled ? new Set() : null;
  const projectMetaCache = projectEnabled ? new Map() : null;
  const publicRepoCache = projectEnabled ? new Map() : null;
  // Sessions cluster into few repos, so the per-file freshness checks below
  // keep stat-ing the same handful of .git/config paths. Share one stat per
  // config per run, as the Codex path does — it matters most for WSL installs,
  // where every stat crosses the \\wsl$ bridge (#374).
  const projectFreshnessCache = projectEnabled ? new Map() : null;
  const touchedBuckets = new Set();
  // Persist seenMessageHashes across syncs to prevent cross-file duplicates
  // (e.g. subagent file created after main session was already parsed).
  const prevHashes = Array.isArray(cursors.claudeHashes) ? cursors.claudeHashes : [];
  const seenMessageHashes = new Set(prevHashes);
  const defaultSource = normalizeSourceInput(source) || "claude";
  // Resolve all changed-file lineage before counting: fork files may sort first.
  const forkTimes = new Map();
  const forkAliases = await collectClaudeForkAliases(files, cursors,
    (obj) => { if (typeof obj.timestamp === "string") forkTimes.set(obj.uuid, obj.timestamp); });
  {
    // Correct only copies whose old UUID identities prove they were counted.
    // Preserve all token totals and history whose source files were removed.
    const families = new Map();
    const familyTimes = new Map();
    for (const [uuid, ts] of forkTimes) familyTimes.set(claudeUserIdentity({ uuid }, forkAliases), ts);
    for (const hash of prevHashes) {
      if (typeof hash !== "string" || !hash.startsWith("u:")) continue;
      const canonical = claudeUserIdentity({ uuid: hash.slice(2) }, forkAliases);
      families.set(canonical, (families.get(canonical) || 0) + 1);
      seenMessageHashes.delete(hash);
      seenMessageHashes.add(canonical);
    }
    for (const [canonical, count] of families) {
      if (count < 2) continue;
      const ts = familyTimes.get(canonical);
      const hour = ts ? toUtcHalfHourStart(ts) : null;
      if (!hour) continue;
      const key = bucketKey(defaultSource, DEFAULT_MODEL, hour);
      const bucket = hourlyState.buckets[key];
      if (!bucket || bucket.totals.conversation_count < count - 1) continue;
      bucket.totals.conversation_count -= count - 1;
      touchedBuckets.add(key);
    }
  }

  if (!cursors.files || typeof cursors.files !== "object") {
    cursors.files = {};
  }

  // Cross-environment file dedup (#307): when the scan list mixes native and
  // \\wsl$ paths, a session file synced between the environments shows up
  // twice under different paths. Identify never-parsed files by content hash
  // and skip the duplicate wholesale — the message-hash layer alone is capped
  // at 100k entries, and eviction would let bulk history double count.
  let sawUncFile = false;
  let sawLocalFile = false;
  for (const entry of files) {
    const p = typeof entry === "string" ? entry : entry?.path;
    if (typeof p !== "string" || !p) continue;
    if (isUncPath(p)) sawUncFile = true;
    else sawLocalFile = true;
    if (sawUncFile && sawLocalFile) break;
  }
  const seenFileIds = sawUncFile && sawLocalFile ? new Map() : null;

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
    const sameInode = prev && prev.inode === inode;
    const prevOffset = sameInode ? prev.offset || 0 : 0;
    const truncated = sameInode && prevOffset > st.size;
    const startOffset = sameInode && !truncated ? prevOffset : 0;

    let fileId = null;
    if (seenFileIds) {
      const cachedFileId =
        sameInode && typeof prev?.fileId === "string" && prev?.fileIdSize === st.size
          ? prev.fileId
          : null;
      fileId = cachedFileId || (await claudeFileIdentityHash(filePath, st));
      if (fileId) {
        // Cache on the live cursor object so idle files don't re-hash on
        // every sync (the idle short-circuit below never rewrites cursors).
        if (prev && (prev.fileId !== fileId || prev.fileIdSize !== st.size)) {
          prev.fileId = fileId;
          prev.fileIdSize = st.size;
        }
        const primary = seenFileIds.get(fileId);
        if (primary === undefined) {
          seenFileIds.set(fileId, filePath);
        } else if (!prev && primary !== filePath) {
          // Never-parsed duplicate of a file already accounted this run (the
          // native copy sorts first). Mark it caught-up so future syncs only
          // look at genuinely new tail bytes — message hashes still guard the
          // tail if the copies diverge later.
          cursors.files[key] = {
            inode,
            offset: st.size,
            updatedAt: new Date().toISOString(),
            fileId,
            fileIdSize: st.size,
            duplicateOf: primary,
            ...(projectEnabled
              ? {
                  claudeCwd: null,
                  projectFileContext: buildProjectFileContext(null),
                  projectRef: null,
                  projectKey: null,
                }
              : {}),
          };
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
      }
    }

    // Claude's launch cwd is fixed for a session file's lifetime, so once
    // resolved (found, or confirmed absent) from content it never needs
    // re-extracting. Cache it — and the resolved project's git-config
    // freshness fingerprint — on the cursor so idle files with project
    // tracking enabled don't re-walk the filesystem on every sync.
    //
    // "No cwd found" / "cwd not inside a git checkout" only count as settled
    // for genuinely idle files (no new bytes) — there's nothing new to
    // attribute either way. A file that's still growing gets retried: an
    // early sync can catch a session file before its cwd-bearing line lands,
    // and treating that as permanent would silently drop the rest of the
    // session's usage from the project queue.
    const idle = sameInode && !truncated && startOffset >= st.size;
    const cachedCwd = sameInode && !truncated ? prev?.claudeCwd : undefined;
    const cachedProjectFileContext =
      sameInode && !truncated ? prev?.projectFileContext || null : null;
    const cachedContextAbsent = cachedProjectFileContext?.absent === true;
    const projectContextFresh =
      projectEnabled && cachedProjectFileContext && !cachedContextAbsent
        ? await isProjectFileContextFresh(cachedProjectFileContext, {
            freshnessCache: projectFreshnessCache,
          })
        : false;
    const projectKnownAbsent = cachedCwd === null || cachedContextAbsent;
    const projectSettled =
      !projectEnabled || (idle && projectKnownAbsent) || projectContextFresh;

    if (idle && projectSettled) {
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

    let projectRef = sameInode && !truncated ? prev?.projectRef || null : null;
    let projectKey = sameInode && !truncated ? prev?.projectKey || null : null;
    let nextCwd = cachedCwd;
    let nextProjectFileContext = cachedProjectFileContext;

    if (projectEnabled && !projectSettled) {
      if (nextCwd === undefined || nextCwd === null) {
        nextCwd = await resolveClaudeFileCwd(filePath);
      }
      if (nextCwd) {
        const projectContext = await resolveProjectContextForPath({
          startDir: wsl.mapWslCwdToUnc(nextCwd, filePath),
          projectMetaCache,
          publicRepoCache,
          publicRepoResolver,
          projectState,
        });
        projectRef = projectContext?.projectRef || null;
        projectKey = projectContext?.projectKey || null;
        nextProjectFileContext = buildProjectFileContext(projectContext);
      } else {
        projectRef = null;
        projectKey = null;
        nextProjectFileContext = buildProjectFileContext(null);
      }
    }

    const result = await parseClaudeFile({
      filePath,
      fileStat: st,
      startOffset,
      hourlyState,
      touchedBuckets,
      source: fileSource,
      projectState,
      projectTouchedBuckets,
      projectRef,
      projectKey,
      seenMessageHashes,
      forkAliases,
    });

    cursors.files[key] = {
      inode,
      offset: result.endOffset,
      claudeForkIndexed: true,
      updatedAt: new Date().toISOString(),
      ...(fileId ? { fileId, fileIdSize: st.size } : {}),
      ...(projectEnabled
        ? {
            claudeCwd: nextCwd === undefined ? null : nextCwd,
            projectFileContext: nextProjectFileContext,
            projectRef,
            projectKey,
          }
        : {}),
    };

    filesProcessed += 1;
    eventsAggregated += result.eventsAggregated;

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
  if (projectState) {
    projectState.updatedAt = new Date().toISOString();
    cursors.projectHourly = projectState;
  }
  // Persist message hashes for cross-sync dedup; cap at 100k entries to bound size.
  cursors.claudeForkAliases = forkAliases;
  const allHashes = Array.from(seenMessageHashes);
  cursors.claudeHashes =
    allHashes.length > 100_000 ? allHashes.slice(allHashes.length - 100_000) : allHashes;

  return { filesProcessed, eventsAggregated, bucketsQueued, projectBucketsQueued };
}

async function parseClaudeFile({
  filePath,
  fileStat,
  startOffset,
  hourlyState,
  touchedBuckets,
  source,
  projectState,
  projectTouchedBuckets,
  projectRef,
  projectKey,
  seenMessageHashes,
  forkAliases,
}) {
  const st = fileStat || (await fs.stat(filePath).catch(() => null));
  if (!st || !st.isFile()) return { endOffset: startOffset, eventsAggregated: 0 };

  const endOffset = st.size;
  if (startOffset >= endOffset) return { endOffset, eventsAggregated: 0 };

  const stream = fssync.createReadStream(filePath, { encoding: "utf8", start: startOffset });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let eventsAggregated = 0;
  // Separator-agnostic: WSL installs are scanned over \\wsl$ UNC paths where
  // path.join produces backslashes, and a forward-slash-only check would
  // misclassify subagent transcripts as main sessions (#307).
  const isMainSession = !/[\\/]subagents[\\/]/.test(filePath);
  for await (const line of rl) {
    if (!line) continue;

    // Count user-typed messages as conversations (main sessions only).
    // Exclude tool_result messages — those are auto-generated by tool calls,
    // not manually typed by the user. Only count messages with a "text" block.
    if (isMainSession && line.includes('"type":"user"')) {
      let userObj;
      try {
        userObj = JSON.parse(line);
      } catch (_e) {
        /* skip */
      }
      if (userObj?.type === "user") {
        const content = userObj?.message?.content;
        const hasText =
          typeof content === "string" ||
          (Array.isArray(content) && content.some((b) => b?.type === "text"));
        if (hasText) {
          // Copied user rows share their original identity through the full
          // fork lineage. Token dedup remains independent.
          const userKey = seenMessageHashes ? claudeUserIdentity(userObj, forkAliases) : null;
          if (!userKey || !seenMessageHashes.has(userKey)) {
            if (userKey) seenMessageHashes.add(userKey);
            const userTs = typeof userObj?.timestamp === "string" ? userObj.timestamp : null;
            const userBucketStart = userTs ? toUtcHalfHourStart(userTs) : null;
            if (userBucketStart) {
              const userModel = DEFAULT_MODEL;
              const userBucket = getHourlyBucket(hourlyState, source, userModel, userBucketStart);
              userBucket.totals.conversation_count += 1;
              touchedBuckets.add(bucketKey(source, userModel, userBucketStart));
            }
          }
        }
      }
    }

    if (!line.includes('"usage"')) continue;
    let obj;
    try {
      obj = JSON.parse(line);
    } catch (_e) {
      continue;
    }

    if (isOmOClaudeSdkMirror(obj)) continue;

    const usage = obj?.message?.usage || obj?.usage;
    if (!usage || typeof usage !== "object") continue;

    const dedupHash = seenMessageHashes ? claudeMessageDedupKey(obj) : null;
    if (dedupHash && seenMessageHashes.has(dedupHash)) continue;

    const model = normalizeModelInput(obj?.message?.model || obj?.model) || DEFAULT_MODEL;
    const tokenTimestamp = typeof obj?.timestamp === "string" ? obj.timestamp : null;
    if (!tokenTimestamp) continue;

    const delta = normalizeClaudeUsage(usage);
    if (!delta || isAllZeroUsage(delta)) continue;

    if (dedupHash) seenMessageHashes.add(dedupHash);
    delta.conversation_count = 0;

    const bucketStart = toUtcHalfHourStart(tokenTimestamp);
    if (!bucketStart) continue;

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
    eventsAggregated += 1;
  }

  rl.close();
  stream.close?.();
  return { endOffset, eventsAggregated };
}

// Claude Code session files log the launch cwd on message lines (top-level
// "cwd" field) — unlike the file's on-disk location under
// ~/.claude/projects/<dash-encoded-cwd>/, which is not itself inside the
// real project's git checkout and can never resolve a project via directory
// walk. The encoded name can't be decoded reliably either (dashes in the
// real path collide with the path-separator encoding), so read the real cwd
// from content instead. It's set once at session start and bounded scan is
// cheap since it's a top-level field on the first several lines.
const CLAUDE_CWD_SCAN_MAX_BYTES = 65536;

async function resolveClaudeFileCwd(filePath) {
  const stream = fssync.createReadStream(filePath, {
    encoding: "utf8",
    start: 0,
    end: CLAUDE_CWD_SCAN_MAX_BYTES,
  });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  try {
    for await (const line of rl) {
      if (!line || !line.includes('"cwd"')) continue;
      let obj;
      try {
        obj = JSON.parse(line);
      } catch (_e) {
        continue;
      }
      if (typeof obj?.cwd === "string" && obj.cwd.trim()) return obj.cwd.trim();
    }
  } finally {
    rl.close();
    stream.close?.();
  }
  return null;
}

// Stable dedup key for one Claude jsonl entry. Anthropic's official protocol
// guarantees `message.id` is globally unique per response, so msgId alone is a
// valid dedup key. Older code required both msgId AND requestId, which short-
// circuited dedup entirely for jsonl entries where `requestId` is absent
// (DeepSeek/Kimi/Mimo/MiniMax anthropic-compatible endpoints don't return the
// `request-id` HTTP header, and Claude Code's sub-agent / thinking transport
// paths drop the field too). The short-circuit caused 1.6–3.7x overcounting on
// every affected provider — see issue #64. Falling back to msgId-only keeps
// backward compatibility for the (msgId, reqId) format already persisted in
// cursors.claudeHashes (msgId strings don't contain `:`, so the two formats
// share the same Set without collision).
// OmO Native routes Anthropic-subscription turns through the Claude Agent SDK,
// which mirrors the same usage into ~/.claude/projects with entrypoint "sdk-ts"
// and userType "external". OmO's passive reader already counts those turns
// under source=omo — skip the mirrored Claude transcript copy (#726).
function isOmOClaudeSdkMirror(obj) {
  return obj?.entrypoint === "sdk-ts" && obj?.userType === "external";
}

function claudeMessageDedupKey(obj) {
  const msgId = typeof obj?.message?.id === "string" && obj.message.id ? obj.message.id : null;
  if (!msgId) return null;
  const reqId = typeof obj?.requestId === "string" && obj.requestId ? obj.requestId : null;
  return reqId ? `${msgId}:${reqId}` : msgId;
}

function normalizeClaudeUsage(u) {
  const inputTokens = toNonNegativeInt(u?.input_tokens);
  const outputTokens = toNonNegativeInt(u?.output_tokens);
  const reasoningTokens = Math.min(outputTokens, toNonNegativeInt(
    u?.output_tokens_details?.thinking_tokens ?? u?.output_tokens_details?.reasoning_tokens,
  ));
  const cacheCreation = toNonNegativeInt(u?.cache_creation_input_tokens);
  const cacheRead = toNonNegativeInt(u?.cache_read_input_tokens);
  const totalTokens = inputTokens + outputTokens + cacheCreation + cacheRead;
  return {
    input_tokens: inputTokens,
    cached_input_tokens: cacheRead,
    cache_creation_input_tokens: cacheCreation,
    // Claude rows price reasoning separately; it is already included in usage.output_tokens.
    output_tokens: outputTokens - reasoningTokens,
    reasoning_output_tokens: reasoningTokens,
    total_tokens: totalTokens,
  };
}

async function walkClaudeProjects(dir, out) {
  const entries = await safeReadDir(dir);
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      await walkClaudeProjects(fullPath, out);
      continue;
    }
    if (entry.isFile() && entry.name.endsWith(".jsonl")) out.push(fullPath);
  }
}

module.exports = {
  listClaudeProjectFiles,
  parseClaudeIncremental,
  claudeMessageDedupKey,
  normalizeClaudeUsage,
};
