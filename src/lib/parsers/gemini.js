// gemini usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fs = require("node:fs/promises");
const path = require("node:path");
const { ensureDir } = require("../fs");
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
  normalizeSourceInput,
  normalizeModelInput,
  resolveProjectContextForFile,
  diffGeminiTotals,
  isAllZeroUsage,
  toNonNegativeInt,
  safeReadDir,
} = require("./core");

async function listGeminiSessionFiles(tmpDir) {
  const out = [];
  const roots = await safeReadDir(tmpDir);
  for (const root of roots) {
    if (!root.isDirectory()) continue;
    const chatsDir = path.join(tmpDir, root.name, "chats");
    const chats = await safeReadDir(chatsDir);
    for (const entry of chats) {
      if (!entry.isFile()) continue;
      if (!entry.name.startsWith("session-") || !entry.name.endsWith(".json")) continue;
      out.push(path.join(chatsDir, entry.name));
    }
  }
  out.sort((a, b) => a.localeCompare(b));
  return out;
}

async function parseGeminiIncremental({
  sessionFiles,
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
  const files = Array.isArray(sessionFiles) ? sessionFiles : [];
  const totalFiles = files.length;
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const projectEnabled = typeof projectQueuePath === "string" && projectQueuePath.length > 0;
  const projectState = projectEnabled ? normalizeProjectState(cursors?.projectHourly) : null;
  const projectTouchedBuckets = projectEnabled ? new Set() : null;
  const projectMetaCache = projectEnabled ? new Map() : null;
  const publicRepoCache = projectEnabled ? new Map() : null;
  const touchedBuckets = new Set();
  const defaultSource = normalizeSourceInput(source) || "gemini";

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
    const sameFileMetadata =
      prev &&
      prev.inode === inode &&
      prev.size === size &&
      prev.mtimeMs === mtimeMs;
    if (!projectEnabled && sameFileMetadata) {
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
    let startIndex = prev && prev.inode === inode ? Number(prev.lastIndex || -1) : -1;
    let lastTotals = prev && prev.inode === inode ? prev.lastTotals || null : null;
    let lastModel = prev && prev.inode === inode ? prev.lastModel || null : null;

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
    const projectCursor =
      projectKey &&
      prev?.project &&
      prev.project.projectKey === projectKey &&
      prev.project.projectRef === projectRef
        ? prev.project
        : null;
    const projectUpToDate =
      projectKey &&
      sameFileMetadata &&
      projectCursor &&
      Number(projectCursor.lastIndex ?? -1) >= Number(prev?.lastIndex ?? -1);
    if (projectUpToDate) {
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

    const result = await parseGeminiFile({
      filePath,
      startIndex,
      lastTotals,
      lastModel,
      projectCursor,
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
      lastIndex: result.lastIndex,
      lastTotals: result.lastTotals,
      lastModel: result.lastModel,
      updatedAt: new Date().toISOString(),
    };
    if (projectKey) {
      cursors.files[key].project = {
        projectKey,
        projectRef,
        lastIndex: result.projectLastIndex,
        lastTotals: result.projectLastTotals,
        lastModel: result.projectLastModel,
        updatedAt: new Date().toISOString(),
      };
    } else if (prev?.project) {
      cursors.files[key].project = prev.project;
    }

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

  return { filesProcessed, eventsAggregated, bucketsQueued, projectBucketsQueued };
}

async function parseGeminiFile({
  filePath,
  startIndex,
  lastTotals,
  lastModel,
  projectCursor,
  hourlyState,
  touchedBuckets,
  source,
  projectState,
  projectTouchedBuckets,
  projectRef,
  projectKey,
}) {
  const raw = await fs.readFile(filePath, "utf8").catch(() => "");
  if (!raw.trim()) return { lastIndex: startIndex, lastTotals, lastModel, eventsAggregated: 0 };

  let session;
  try {
    session = JSON.parse(raw);
  } catch (_e) {
    return { lastIndex: startIndex, lastTotals, lastModel, eventsAggregated: 0 };
  }

  const messages = Array.isArray(session?.messages) ? session.messages : [];
  if (startIndex >= messages.length) {
    startIndex = -1;
    lastTotals = null;
    lastModel = null;
  }

  let eventsAggregated = 0;
  let model = typeof lastModel === "string" ? lastModel : null;
  let totals = lastTotals && typeof lastTotals === "object" ? lastTotals : null;
  const projectActive = Boolean(projectKey && projectState && projectTouchedBuckets);
  let projectStartIndex =
    projectActive && Number.isFinite(projectCursor?.lastIndex)
      ? Number(projectCursor.lastIndex)
      : -1;
  let projectModel =
    projectActive && typeof projectCursor?.lastModel === "string"
      ? projectCursor.lastModel
      : null;
  let projectTotals =
    projectActive && projectCursor?.lastTotals && typeof projectCursor.lastTotals === "object"
      ? projectCursor.lastTotals
      : null;
  if (projectActive && projectStartIndex >= messages.length) {
    projectStartIndex = -1;
    projectTotals = null;
    projectModel = null;
  }
  const hourlyBegin = Number.isFinite(startIndex) ? startIndex + 1 : 0;
  const projectBegin = projectActive ? projectStartIndex + 1 : Number.POSITIVE_INFINITY;
  const begin = Math.min(hourlyBegin, projectBegin);

  for (let idx = begin; idx < messages.length; idx++) {
    const msg = messages[idx];
    if (!msg || typeof msg !== "object") continue;

    const normalizedModel = normalizeModelInput(msg.model);
    if (normalizedModel && idx >= hourlyBegin) model = normalizedModel;
    if (normalizedModel && idx >= projectBegin) projectModel = normalizedModel;

    const timestamp = typeof msg.timestamp === "string" ? msg.timestamp : null;
    const currentTotals = normalizeGeminiTokens(msg.tokens);
    if (idx >= hourlyBegin && (!timestamp || !currentTotals)) {
      totals = currentTotals || totals;
    }
    if (idx >= projectBegin && (!timestamp || !currentTotals)) {
      projectTotals = currentTotals || projectTotals;
    }
    if (!timestamp || !currentTotals) {
      continue;
    }

    let bucketStart = null;
    if (idx >= hourlyBegin) {
      const delta = diffGeminiTotals(currentTotals, totals);
      if (!delta || isAllZeroUsage(delta)) {
        totals = currentTotals;
      } else {
        delta.conversation_count = 1;
        bucketStart = toUtcHalfHourStart(timestamp);
        if (bucketStart) {
          const bucket = getHourlyBucket(hourlyState, source, model, bucketStart);
          addTotals(bucket.totals, delta);
          touchedBuckets.add(bucketKey(source, model, bucketStart));
          eventsAggregated += 1;
        }
        totals = currentTotals;
      }
    }

    if (idx >= projectBegin) {
      const projectDelta = diffGeminiTotals(currentTotals, projectTotals);
      if (!projectDelta || isAllZeroUsage(projectDelta)) {
        projectTotals = currentTotals;
        continue;
      }
      projectDelta.conversation_count = 1;
      bucketStart = bucketStart || toUtcHalfHourStart(timestamp);
      if (bucketStart) {
        const projectBucket = getProjectBucket(
          projectState,
          projectKey,
          source,
          bucketStart,
          projectRef,
        );
        addTotals(projectBucket.totals, projectDelta);
        projectTouchedBuckets.add(projectBucketKey(projectKey, source, bucketStart));
      }
      projectTotals = currentTotals;
    }
  }

  return {
    lastIndex: messages.length - 1,
    lastTotals: totals,
    lastModel: model,
    projectLastIndex: projectActive ? messages.length - 1 : null,
    projectLastTotals: projectTotals,
    projectLastModel: projectModel,
    eventsAggregated,
  };
}

function normalizeGeminiTokens(tokens) {
  if (!tokens || typeof tokens !== "object") return null;
  const input = toNonNegativeInt(tokens.input);
  const cached = toNonNegativeInt(tokens.cached);
  const output = toNonNegativeInt(tokens.output);
  const tool = toNonNegativeInt(tokens.tool);
  const thoughts = toNonNegativeInt(tokens.thoughts);
  const reportedTotal = toNonNegativeInt(tokens.total);
  const computedTotal = input + cached + output + tool + thoughts;
  const total = Math.max(reportedTotal, computedTotal);

  return {
    input_tokens: input,
    cached_input_tokens: cached,
    cache_creation_input_tokens: 0,
    output_tokens: output + tool,
    reasoning_output_tokens: thoughts,
    total_tokens: total,
  };
}

module.exports = {
  listGeminiSessionFiles,
  parseGeminiIncremental,
  normalizeGeminiTokens,
};
