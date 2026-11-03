// lmstudio usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const crypto = require("node:crypto");
const { ensureDir } = require("../fs");
const {
  DEFAULT_MODEL,
  enqueueTouchedBuckets,
  cloneTotals,
  normalizeHourlyState,
  initTotals,
  totalsKey,
  toUtcHalfHourStart,
  parseBucketKey,
  normalizeModelInput,
  normalizeLocalStudioTokens,
  reconcilePassiveUsageEvent,
} = require("./core");

// ─────────────────────────────────────────────────────────────────────────────
// LM Studio
//
// Data: pretty-printed OpenAI-compatible final responses beneath
// `~/.lmstudio/server-logs/`. The reader recognizes both Chat Completions and
// Responses API usage shapes. It scans only response identity, model, timestamp,
// and the balanced `usage` object; prompt and response bodies are never parsed
// or persisted.
// ─────────────────────────────────────────────────────────────────────────────

const LMSTUDIO_SOURCE = "lmstudio";
const LMSTUDIO_WINDOW_BYTES = 8 * 1024 * 1024;
const LMSTUDIO_CHUNK_BYTES = 64 * 1024;
const LMSTUDIO_IDENTITY_OVERLAP_BYTES = 512;
const LMSTUDIO_MESSAGE_LIMIT = 10_000;
const LMSTUDIO_USAGE_MARKER = Buffer.from('"usage"');

function resolveLmstudioHome(env = process.env) {
  const override = typeof env.TOKEN_PULSE_LMSTUDIO_HOME === "string"
    ? env.TOKEN_PULSE_LMSTUDIO_HOME.trim()
    : "";
  if (override) return path.resolve(override);
  const nativeHome = typeof env.LM_STUDIO_HOME === "string"
    ? env.LM_STUDIO_HOME.trim()
    : "";
  if (nativeHome) return path.resolve(nativeHome);
  return path.join(env.HOME || env.USERPROFILE || os.homedir(), ".lmstudio");
}

async function resolveLmstudioLogFiles(env = process.env) {
  const root = path.join(resolveLmstudioHome(env), "server-logs");
  const files = [];
  const pending = [root];
  while (pending.length > 0) {
    const dir = pending.pop();
    let entries;
    try {
      entries = await fs.readdir(dir, { withFileTypes: true });
    } catch (_e) {
      continue;
    }
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) pending.push(fullPath);
      else if (entry.isFile() && entry.name.toLowerCase().endsWith(".log")) files.push(fullPath);
    }
  }
  return files.sort((a, b) => a.localeCompare(b));
}

function skipLmstudioWhitespace(buffer, index) {
  while (index < buffer.length && /\s/.test(String.fromCharCode(buffer[index]))) index += 1;
  return index;
}

function scanLmstudioUsageObjectStart(buffer, from = 0) {
  let cursor = Math.max(0, from);
  while (cursor < buffer.length) {
    const marker = buffer.indexOf(LMSTUDIO_USAGE_MARKER, cursor);
    if (marker < 0) break;
    cursor = marker + LMSTUDIO_USAGE_MARKER.length;
    if (marker > 0 && buffer[marker - 1] === 0x5c) continue;
    const colon = skipLmstudioWhitespace(buffer, cursor);
    if (colon >= buffer.length) return { found: null, certainTo: marker };
    if (buffer[colon] !== 0x3a) continue;
    const brace = skipLmstudioWhitespace(buffer, colon + 1);
    if (brace >= buffer.length) return { found: null, certainTo: marker };
    if (buffer[brace] === 0x7b) return { found: { marker, objectStart: brace }, certainTo: marker };
  }
  return {
    found: null,
    certainTo: Math.max(0, buffer.length - Math.max(0, LMSTUDIO_USAGE_MARKER.length - 1)),
  };
}

function lmstudioBalancedObjectEnd(buffer, start) {
  if (buffer[start] !== 0x7b) return -1;
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let index = start; index < buffer.length; index++) {
    const byte = buffer[index];
    if (inString) {
      if (escaped) escaped = false;
      else if (byte === 0x5c) escaped = true;
      else if (byte === 0x22) inString = false;
      continue;
    }
    if (byte === 0x22) inString = true;
    else if (byte === 0x7b) depth += 1;
    else if (byte === 0x7d) {
      depth -= 1;
      if (depth === 0) return index + 1;
    }
  }
  return -1;
}

function lmstudioJsonStringEnd(buffer, start) {
  if (buffer[start] !== 0x22) return -1;
  let escaped = false;
  for (let index = start + 1; index < buffer.length; index++) {
    const byte = buffer[index];
    if (escaped) escaped = false;
    else if (byte === 0x5c) escaped = true;
    else if (byte === 0x22) return index + 1;
  }
  return -1;
}

function lastLmstudioJsonStringField(buffer, field) {
  const markerBuffer = Buffer.from(`"${field}"`);
  let cursor = 0;
  let found = null;
  while (cursor < buffer.length) {
    const index = buffer.indexOf(markerBuffer, cursor);
    if (index < 0) break;
    cursor = index + markerBuffer.length;
    if (index > 0 && buffer[index - 1] === 0x5c) continue;
    let valueStart = skipLmstudioWhitespace(buffer, cursor);
    if (buffer[valueStart] !== 0x3a) continue;
    valueStart = skipLmstudioWhitespace(buffer, valueStart + 1);
    const valueEnd = lmstudioJsonStringEnd(buffer, valueStart);
    if (valueEnd < 0) continue;
    try {
      const value = JSON.parse(buffer.subarray(valueStart, valueEnd).toString("utf8"));
      if (typeof value === "string") found = value;
    } catch (_e) { }
  }
  return found;
}

function lastLmstudioTimestamp(buffer) {
  const text = buffer.toString("utf8");
  let timestamp = null;
  for (const rawLine of text.split("\n")) {
    const line = rawLine.startsWith("\r") ? rawLine.slice(1) : rawLine;
    const match = /^\[(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\]\[/.exec(line);
    if (!match) continue;
    const date = new Date(
      Number(match[1]),
      Number(match[2]) - 1,
      Number(match[3]),
      Number(match[4]),
      Number(match[5]),
      Number(match[6]),
    );
    if (!Number.isNaN(date.getTime())) timestamp = date.getTime();
  }
  return timestamp;
}

function lmstudioResponseId(buffer) {
  const id = lastLmstudioJsonStringField(buffer, "id");
  return typeof id === "string" && ["chatcmpl-", "cmpl-", "resp_"].some((prefix) => id.startsWith(prefix))
    ? id
    : null;
}

function absorbLmstudioIdentity(identity, buffer) {
  const responseId = lmstudioResponseId(buffer);
  const model = lastLmstudioJsonStringField(buffer, "model");
  const timestamp = lastLmstudioTimestamp(buffer);
  if (responseId) identity.responseId = responseId;
  if (model && model.trim()) identity.model = model.trim();
  if (timestamp) identity.timestamp = timestamp;
}

function normalizeLmstudioResumeIdentity(value) {
  const identity = {};
  const responseId = typeof value?.responseId === "string" ? value.responseId : "";
  if (["chatcmpl-", "cmpl-", "resp_"].some((prefix) => responseId.startsWith(prefix))) {
    identity.responseId = responseId;
  }
  const model = typeof value?.model === "string" ? value.model.trim() : "";
  if (model) identity.model = model;
  const timestamp = Number(value?.timestamp);
  if (Number.isFinite(timestamp) && timestamp > 0) identity.timestamp = timestamp;
  return identity;
}

function lmstudioRecordFromSlices({ usageBuffer, metadataBuffer, carried, filePath, marker, fallbackTimestamp }) {
  let usage;
  try {
    usage = JSON.parse(usageBuffer.toString("utf8"));
  } catch (_e) {
    return null;
  }
  const totals = normalizeLocalStudioTokens(usage);
  if (!totals) return null;
  const responseId = lmstudioResponseId(metadataBuffer) || carried.responseId || null;
  const model = normalizeModelInput(
    lastLmstudioJsonStringField(metadataBuffer, "model") || carried.model,
  ) || DEFAULT_MODEL;
  const timestampMs = lastLmstudioTimestamp(metadataBuffer) || carried.timestamp || fallbackTimestamp;
  const bucketStart = timestampMs
    ? toUtcHalfHourStart(new Date(timestampMs).toISOString())
    : null;
  if (!bucketStart) return null;
  const fallback = crypto.createHash("sha256")
    .update(filePath)
    .update("\0")
    .update(String(marker))
    .update("\0")
    .update(model)
    .update("\0")
    .update(totalsKey(totals))
    .digest("base64url");
  return {
    key: responseId ? `lmstudio:${responseId}` : `lmstudio:${fallback}`,
    model,
    bucketStart,
    totals,
    marker,
  };
}

async function readLmstudioFileRecords(
  filePath,
  {
    windowBytes = LMSTUDIO_WINDOW_BYTES,
    chunkBytes = LMSTUDIO_CHUNK_BYTES,
    startOffset = 0,
    resumeIdentity,
  } = {},
) {
  const handle = await fs.open(filePath, "r");
  try {
    const initialStat = await handle.stat();
    const fallbackTimestamp = initialStat.mtimeMs;
    const records = [];
    const initialOffset = Math.min(
      initialStat.size,
      Math.max(0, Number.isFinite(startOffset) ? Math.floor(startOffset) : 0),
    );
    let window = Buffer.alloc(0);
    let windowStart = initialOffset;
    let metadataStart = initialOffset;
    let scannedTo = initialOffset;
    let position = initialOffset;
    let carried = normalizeLmstudioResumeIdentity(resumeIdentity);
    const chunk = Buffer.alloc(Math.max(1, chunkBytes));

    while (true) {
      const { bytesRead } = await handle.read(chunk, 0, chunk.length, position);
      if (bytesRead > 0) {
        window = Buffer.concat([window, chunk.subarray(0, bytesRead)]);
        position += bytesRead;
      }
      const atEof = bytesRead === 0;

      while (true) {
        const scanFrom = Math.min(window.length, Math.max(0, scannedTo - windowStart));
        const scan = scanLmstudioUsageObjectStart(window, scanFrom);
        if (!scan.found) {
          scannedTo = windowStart + scan.certainTo;
          break;
        }
        const objectEnd = lmstudioBalancedObjectEnd(window, scan.found.objectStart);
        if (objectEnd < 0) {
          scannedTo = atEof
            ? windowStart + window.length
            : windowStart + scan.found.marker;
          break;
        }
        scannedTo = windowStart + objectEnd;
        const absoluteMarker = windowStart + scan.found.marker;
        const absoluteEnd = windowStart + objectEnd;
        const metadataFrom = Math.min(
          scan.found.marker,
          Math.max(0, metadataStart - windowStart),
        );
        const record = lmstudioRecordFromSlices({
          usageBuffer: window.subarray(scan.found.objectStart, objectEnd),
          metadataBuffer: window.subarray(metadataFrom, scan.found.marker),
          carried,
          filePath,
          marker: absoluteMarker,
          fallbackTimestamp,
        });
        if (record) records.push(record);
        metadataStart = absoluteEnd;
        carried = {};
      }

      const keepFrom = Math.min(window.length, Math.max(0, metadataStart - windowStart));
      if (keepFrom > 0) {
        window = window.subarray(keepFrom);
        windowStart += keepFrom;
      }

      if (window.length > windowBytes) {
        const overflow = window.length - windowBytes;
        const absorbTo = Math.min(
          window.length,
          overflow + LMSTUDIO_IDENTITY_OVERLAP_BYTES,
        );
        absorbLmstudioIdentity(carried, window.subarray(0, absorbTo));
        window = window.subarray(overflow);
        windowStart += overflow;
        metadataStart = Math.max(metadataStart, windowStart);
        scannedTo = Math.max(scannedTo, windowStart);
      }

      if (atEof) {
        const finalStat = await handle.stat().catch(() => initialStat);
        const nextIdentity = normalizeLmstudioResumeIdentity(carried);
        return {
          records,
          stat: {
            dev: finalStat.dev,
            ino: finalStat.ino,
            size: finalStat.size,
            mtimeMs: finalStat.size === position ? finalStat.mtimeMs : -1,
            resumeOffset: Math.max(metadataStart, position - windowBytes),
            ...(Object.keys(nextIdentity).length > 0
              ? { resumeIdentity: nextIdentity }
              : {}),
          },
        };
      }
    }
  } finally {
    await handle.close().catch(() => {});
  }
}

function retainNewestPassiveMessages(messages, maxEntries) {
  const limit = Number.isInteger(maxEntries) && maxEntries > 0
    ? maxEntries
    : LMSTUDIO_MESSAGE_LIMIT;
  const entries = Object.entries(messages);
  if (entries.length <= limit) return messages;
  entries.sort((left, right) => {
    const leftTime = Date.parse(left[1]?.updatedAt || left[1]?.bucketStart || "") || 0;
    const rightTime = Date.parse(right[1]?.updatedAt || right[1]?.bucketStart || "") || 0;
    if (leftTime !== rightTime) return rightTime - leftTime;
    return right[0].localeCompare(left[0]);
  });
  return Object.fromEntries(entries.slice(0, limit));
}

async function parseLmstudioIncremental({
  logFiles,
  cursors,
  queuePath,
  onProgress,
  messageLimit = LMSTUDIO_MESSAGE_LIMIT,
  readerOptions,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const files = Array.isArray(logFiles) ? [...logFiles].sort((a, b) => a.localeCompare(b)) : [];
  const priorState = cursors.lmstudio && typeof cursors.lmstudio === "object"
    ? cursors.lmstudio
    : {};
  let fileState = priorState.files && typeof priorState.files === "object"
    ? { ...priorState.files }
    : {};
  let messages = priorState.messages && typeof priorState.messages === "object"
    ? priorState.messages
    : {};
  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  const presentFiles = new Set(files);
  const cb = typeof onProgress === "function" ? onProgress : null;
  const effectiveMessageLimit = Number.isInteger(messageLimit) && messageLimit > 0
    ? messageLimit
    : LMSTUDIO_MESSAGE_LIMIT;
  let recordsProcessed = 0;
  let eventsAggregated = 0;
  let stateChanged = false;
  const readOptions = readerOptions && typeof readerOptions === "object" ? readerOptions : {};
  const filePlans = [];
  let allFilesReadable = true;

  for (const filePath of files) {
    const previousFile = fileState[filePath];
    let stat;
    try {
      stat = await fs.stat(filePath);
    } catch (_e) {
      allFilesReadable = false;
      continue;
    }
    const unchanged = Boolean(previousFile
      && previousFile.dev === stat.dev
      && previousFile.ino === stat.ino
      && previousFile.size === stat.size
      && previousFile.mtimeMs === stat.mtimeMs
      && Number.isFinite(previousFile.resumeOffset));
    const canResume = Boolean(previousFile
      && previousFile.dev === stat.dev
      && previousFile.ino === stat.ino
      && (stat.size > previousFile.size || previousFile.mtimeMs === -1)
      && Number.isFinite(previousFile.resumeOffset)
      && previousFile.resumeOffset >= 0
      && previousFile.resumeOffset <= stat.size);
    filePlans.push({ filePath, previousFile, unchanged, canResume });
  }

  const requiresRebuild = filePlans.some(
    ({ previousFile, unchanged, canResume }) => previousFile && !unchanged && !canResume,
  );
  if (requiresRebuild) {
    // Bounded message retention cannot safely deduplicate a replay from byte
    // zero. Rebuild every current LM Studio log in isolation, then replace only
    // this source's buckets while preserving their queue fingerprints.
    if (!allFilesReadable || filePlans.length !== files.length) {
      return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
    }
    const rebuiltHourly = normalizeHourlyState(null);
    const rebuiltMessages = {};
    const rebuiltFiles = {};
    const rebuiltTouched = new Set();
    let rebuiltRecords = 0;
    let rebuiltEvents = 0;
    try {
      for (let index = 0; index < filePlans.length; index++) {
        const { filePath } = filePlans[index];
        const parsed = await readLmstudioFileRecords(filePath, {
          ...readOptions,
          startOffset: 0,
          resumeIdentity: undefined,
        });
        for (const event of parsed.records) {
          if (reconcilePassiveUsageEvent({
            event,
            source: LMSTUDIO_SOURCE,
            messages: rebuiltMessages,
            hourlyState: rebuiltHourly,
            touchedBuckets: rebuiltTouched,
          })) rebuiltEvents += 1;
        }
        rebuiltFiles[filePath] = { ...parsed.stat, updatedAt: new Date().toISOString() };
        rebuiltRecords += 1;
        if (cb) {
          cb({
            index: index + 1,
            total: files.length,
            recordsProcessed: rebuiltRecords,
            eventsAggregated: rebuiltEvents,
            bucketsQueued: rebuiltTouched.size,
          });
        }
      }
    } catch (error) {
      if (process.env.TOKEN_PULSE_DEBUG) {
        process.stderr.write(`[lmstudio] rebuild deferred: ${error?.message || error}\n`);
      }
      return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
    }

    for (const [key, bucket] of Object.entries(hourlyState.buckets || {})) {
      if (parseBucketKey(key).source !== LMSTUDIO_SOURCE || !bucket?.totals) continue;
      bucket.totals = initTotals();
      touchedBuckets.add(key);
    }
    for (const [key, bucket] of Object.entries(rebuiltHourly.buckets || {})) {
      const current = hourlyState.buckets[key];
      if (current && typeof current === "object") current.totals = cloneTotals(bucket.totals);
      else hourlyState.buckets[key] = bucket;
      touchedBuckets.add(key);
    }
    fileState = rebuiltFiles;
    messages = rebuiltMessages;
    recordsProcessed = rebuiltRecords;
    eventsAggregated = rebuiltEvents;
    stateChanged = true;
  } else {
    for (let index = 0; index < filePlans.length; index++) {
      const { filePath, previousFile, unchanged, canResume } = filePlans[index];
      if (!unchanged) {
        try {
          const parsed = await readLmstudioFileRecords(filePath, {
            ...readOptions,
            startOffset: canResume ? previousFile.resumeOffset : 0,
            resumeIdentity: canResume ? previousFile.resumeIdentity : undefined,
          });
          for (const event of parsed.records) {
            if (reconcilePassiveUsageEvent({
              event,
              source: LMSTUDIO_SOURCE,
              messages,
              hourlyState,
              touchedBuckets,
            })) eventsAggregated += 1;
          }
          fileState[filePath] = { ...parsed.stat, updatedAt: new Date().toISOString() };
          recordsProcessed += 1;
          stateChanged = true;
        } catch (error) {
          if (process.env.TOKEN_PULSE_DEBUG) {
            process.stderr.write(`[lmstudio] skipped ${filePath}: ${error?.message || error}\n`);
          }
        }
      }
      if (cb) {
        cb({
          index: index + 1,
          total: files.length,
          recordsProcessed,
          eventsAggregated,
          bucketsQueued: touchedBuckets.size,
        });
      }
    }
  }

  for (const filePath of Object.keys(fileState)) {
    if (!presentFiles.has(filePath)) {
      delete fileState[filePath];
      stateChanged = true;
    }
  }
  if (
    !stateChanged
    && touchedBuckets.size === 0
    && Object.keys(messages).length <= effectiveMessageLimit
  ) {
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  }
  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.lmstudio = {
    files: fileState,
    messages: retainNewestPassiveMessages(messages, effectiveMessageLimit),
    updatedAt,
  };
  return { recordsProcessed, eventsAggregated, bucketsQueued };
}

module.exports = {
  resolveLmstudioHome,
  resolveLmstudioLogFiles,
  readLmstudioFileRecords,
  parseLmstudioIncremental,
};
