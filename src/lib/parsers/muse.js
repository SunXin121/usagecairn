// muse usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const crypto = require("node:crypto");
const { ensureDir } = require("../fs");
const {
  enqueueTouchedBuckets,
  normalizeHourlyState,
  getHourlyBucket,
  addTotals,
  toUtcHalfHourStart,
  bucketKey,
  toNonNegativeInt,
  expandHomePath,
} = require("./core");

// ─────────────────────────────────────────────────────────────────────────────
// Muse Code (Meta) — passive session.jsonl reader.
//
//   ~/.local/share/muse/sessions/YYYY/MM/DD/<session>/session.jsonl
//   ~/.local/share/muse/sessions/YYYY/MM/DD/<session>/subagent/<id>/session.jsonl
//
// Every record is `{id, stream, sequence, recorded_at (epoch µs), payload_type,
// payload}`. Per-model-step usage rides on a `runtime.session` record whose
// `payload.event.kind` is `goal_usage_attribution`:
//
//   quantity: {unit:"tokens", reported:true, input_tokens, output_tokens,
//              cached_tokens, reasoning_tokens, main_llm_steps}
//
// Only `usage_family:"provider"` + `reported:true` rows are model usage (tool and
// reminder rows are zero placeholders). The usage row names no model: the model
// comes from the `run.model.configured` record of the same `run_id`.
//
// Token semantics (verified against the session snapshot `cost_ledger`, where
// `input_tokens` equals the sum of these rows and `cache_read_tokens` equals
// `cached_tokens`): `input_tokens` INCLUDES cache reads, and `reasoning_tokens`
// is a subset of `output_tokens`. Subagent logs are separate streams whose usage
// is not repeated in the parent log, so each file is counted once. No prompt,
// response or path text is read or stored; only counts, timestamps and the
// model id.
function resolveMuseHome(env = process.env) {
  if (env.TOKEN_PULSE_MUSE_HOME) return expandHomePath(env.TOKEN_PULSE_MUSE_HOME, env);
  return path.join(env.HOME || os.homedir(), ".local", "share", "muse");
}

const MUSE_SCAN_MAX_DEPTH = 8;
const MUSE_READ_CHUNK_BYTES = 8 * 1024 * 1024;
const MUSE_WATERMARK_IDS_MAX = 100;

function collectMuseSessionFiles(dir, files, depth = 0) {
  if (depth > MUSE_SCAN_MAX_DEPTH) return;
  let entries;
  try { entries = fssync.readdirSync(dir, { withFileTypes: true }); } catch { return; }
  for (const entry of entries) {
    // Dot directories (.msp-view-v1) hold derived views, not the session log.
    if (entry.name.startsWith(".")) continue;
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) collectMuseSessionFiles(full, files, depth + 1);
    else if (entry.isFile() && entry.name === "session.jsonl") files.push(full);
  }
}

function resolveMuseSessionFiles(env = process.env) {
  const sessionsDir = path.join(resolveMuseHome(env), "sessions");
  const files = [];
  collectMuseSessionFiles(sessionsDir, files);
  return files;
}

// The model Muse is configured to use, for rows whose run has no model record
// in the portion of the log we read. Reads only the `model` string.
function resolveMuseDefaultModel(env = process.env) {
  const fallback = "muse";
  try {
    const configHome = env.XDG_CONFIG_HOME || path.join(env.HOME || os.homedir(), ".config");
    const settings = JSON.parse(fssync.readFileSync(path.join(configHome, "muse", "settings.json"), "utf8"));
    return typeof settings?.model === "string" && settings.model.trim() ? settings.model.trim() : fallback;
  } catch {
    return fallback;
  }
}

// A permission transaction wraps its records as JSON strings under `children`;
// unwrap them so a model or usage record is never missed inside a frame.
function* museRecords(entry) {
  if (entry && Array.isArray(entry.children) && entry.retained_frame) {
    for (const child of entry.children) {
      if (!child || typeof child.record_json !== "string") continue;
      try { yield JSON.parse(child.record_json); } catch { /* skip */ }
    }
    return;
  }
  yield entry;
}

const MUSE_HEAD_FINGERPRINT_BYTES = 256;
const MUSE_PREFIX_FINGERPRINT_MAX_BYTES = 8 * 1024 * 1024;

// Fingerprints for Muse session logs so an in-place rewrite is not mistaken
// for an append. Prefer a hash of the entire consumed prefix (capped); fall
// back to head+tail samples only when the prefix exceeds the cap.
function museRangeFingerprint(fd, offset, length) {
  const n = Math.max(0, Number(length) || 0);
  if (n <= 0) return null;
  const buf = Buffer.alloc(n);
  const read = fssync.readSync(fd, buf, 0, n, Math.max(0, Number(offset) || 0));
  if (read <= 0) return null;
  return crypto.createHash("sha256").update(buf.subarray(0, read)).digest("hex");
}

function museHeadFingerprint(fd, size) {
  return museRangeFingerprint(fd, 0, Math.min(MUSE_HEAD_FINGERPRINT_BYTES, Math.max(0, Number(size) || 0)));
}

function musePrefixFingerprint(fd, size) {
  const total = Math.max(0, Number(size) || 0);
  if (total <= 0) return null;
  if (total <= MUSE_PREFIX_FINGERPRINT_MAX_BYTES) {
    const hash = crypto.createHash("sha256");
    let offset = 0;
    const chunk = Buffer.alloc(Math.min(64 * 1024, total));
    while (offset < total) {
      const want = Math.min(chunk.length, total - offset);
      const read = fssync.readSync(fd, chunk, 0, want, offset);
      if (read <= 0) break;
      hash.update(chunk.subarray(0, read));
      offset += read;
    }
    return offset === total ? hash.digest("hex") : null;
  }
  // Large logs: sample head and tail of the consumed prefix.
  const head = museHeadFingerprint(fd, total);
  const tailLen = Math.min(MUSE_HEAD_FINGERPRINT_BYTES, total);
  const tail = museRangeFingerprint(fd, total - tailLen, tailLen);
  return head && tail ? `${head}:${tail}` : null;
}

function museResumeLooksAppendOnly(fd, prevSize, prev, currentPrefixFp, currentMtimeMs, currentSize) {
  if (prev?.forceRewind === true) return false;
  if (!(prevSize > 0)) return true;
  if (typeof prev?.prefixFp === "string" && prev.prefixFp && prev.prefixFp !== currentPrefixFp) return false;
  // Legacy cursors without prefixFp: reject when the head sample diverges.
  if (!prev?.prefixFp && typeof prev?.headFp === "string" && prev.headFp) {
    const head = museHeadFingerprint(fd, prevSize);
    if (head && head !== prev.headFp) return false;
  }
  // Equal-size rewrite above the full-prefix cap (or any equal-size rewrite that
  // preserves sampled bytes) still bumps mtime on real filesystems. Only apply
  // when the size is unchanged — growth from an append also changes mtime.
  if (
    Number(currentSize) === Number(prevSize) &&
    Number.isFinite(Number(prev?.mtimeMs)) &&
    Number.isFinite(Number(currentMtimeMs)) &&
    Number(prev.mtimeMs) !== Number(currentMtimeMs)
  ) {
    return false;
  }
  // Resume offset must be a JSONL record boundary.
  const buf = Buffer.alloc(1);
  const read = fssync.readSync(fd, buf, 0, 1, prevSize - 1);
  return read === 1 && buf[0] === 0x0a;
}

// Subagent logs live under .../sessions/<session>/subagent/... . Matching any
// ancestor named "subagent" would misclassify roots when the Muse home path
// itself contains that segment (e.g. TOKEN_PULSE_MUSE_HOME=/data/subagent/muse).
function isMuseSubagentSessionFile(filePath, env = process.env) {
  const sessionsRoot = path.join(resolveMuseHome(env), "sessions");
  const relative = path.relative(sessionsRoot, filePath);
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) {
    // Outside the configured sessions root — fall back to a local segment check
    // that still requires the subagent directory to be a parent of session.jsonl.
    const parts = filePath.split(path.sep);
    const sessionIdx = parts.lastIndexOf("session.jsonl");
    return sessionIdx > 0 && parts.slice(0, sessionIdx).includes("subagent");
  }
  return relative.split(path.sep).includes("subagent");
}

async function parseMuseIncremental({ sessionFiles, cursors, queuePath, onProgress, env } = {}) {
  await ensureDir(path.dirname(queuePath));
  const state = cursors.muse && typeof cursors.muse === "object" ? cursors.muse : {};
  const seenIds = new Set(Array.isArray(state.seenIds) ? state.seenIds : []);
  const fileOffsets =
    state.fileOffsets && typeof state.fileOffsets === "object" ? { ...state.fileOffsets } : {};
  const files = Array.isArray(sessionFiles) ? sessionFiles : resolveMuseSessionFiles(env || process.env);
  const defaultModel = resolveMuseDefaultModel(env || process.env);

  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  const cb = typeof onProgress === "function" ? onProgress : null;
  let recordsProcessed = 0;
  let eventsAggregated = 0;

  const handleRecord = (rec, fileState) => {
    if (!rec || typeof rec !== "object") return;
    const payload = rec.payload;
    if (!payload || typeof payload !== "object") return;
    if (rec.payload_type === "run.model.configured") {
      const modelRec = payload.record;
      const runId = modelRec?.run_stream?.id;
      if (typeof modelRec?.model_id === "string" && modelRec.model_id) {
        if (runId) fileState.runModels[runId] = modelRec.model_id;
        fileState.model = modelRec.model_id;
      }
      return;
    }
    if (rec.payload_type !== "runtime.session") return;
    const event = payload.event;
    if (!event || event.kind !== "goal_usage_attribution") return;
    const usage = event.record;
    const quantity = usage?.quantity;
    if (!quantity || usage.usage_family !== "provider" || quantity.reported !== true) return;
    const usageId = typeof usage.usage_id === "string" ? usage.usage_id : null;
    if (!usageId || seenIds.has(usageId)) return;
    // A rewritten log is re-read from the start, and `seenIds` keeps only the
    // newest 10k ids. Rows older than the last one counted from this file are
    // already in the queue, so skip them even if their id was evicted. Rows at
    // exactly that timestamp are skipped only when their id was counted, so a
    // new row that shares the timestamp is kept.
    const micros = Number(rec.recorded_at);
    if (fileState.skipThrough && Number.isFinite(micros)) {
      if (micros < fileState.skipThrough) return;
      if (micros === fileState.skipThrough && fileState.skipIds.has(usageId)) return;
    }

    recordsProcessed++;
    const inputTotal = toNonNegativeInt(quantity.input_tokens);
    const output = toNonNegativeInt(quantity.output_tokens);
    const cached = Math.min(toNonNegativeInt(quantity.cached_tokens), inputTotal);
    const reasoning = Math.min(toNonNegativeInt(quantity.reasoning_tokens), output);
    seenIds.add(usageId);
    if (inputTotal === 0 && output === 0) return;

    if (!Number.isFinite(micros) || micros <= 0) return;
    const bucketStart = toUtcHalfHourStart(new Date(Math.floor(micros / 1000)).toISOString());
    if (!bucketStart) return;

    const input = inputTotal - cached;
    const model = fileState.runModels[payload.run_id] || fileState.model || defaultModel;
    // One conversation per root session, charged to its first usage record.
    // Subagent logs are part of their parent's conversation.
    const conversations = fileState.counted || fileState.isSubagent ? 0 : 1;
    fileState.counted = true;
    const bucket = getHourlyBucket(hourlyState, "muse", model, bucketStart);
    addTotals(bucket.totals, {
      input_tokens: input,
      cached_input_tokens: cached,
      cache_creation_input_tokens: 0,
      output_tokens: output,
      reasoning_output_tokens: reasoning,
      total_tokens: input + cached + output,
      conversation_count: conversations,
    });
    touchedBuckets.add(bucketKey("muse", model, bucketStart));
    eventsAggregated++;
    // Advance only for rows that reached a bucket, remembering the ids that
    // share the newest timestamp.
    if (micros > fileState.watermark) {
      fileState.watermark = micros;
      fileState.watermarkIds = [usageId];
    } else if (micros === fileState.watermark && fileState.watermarkIds.length < MUSE_WATERMARK_IDS_MAX) {
      fileState.watermarkIds.push(usageId);
    }
  };

  for (let fileIdx = 0; fileIdx < files.length; fileIdx++) {
    const filePath = files[fileIdx];
    // Open first and stat the descriptor, so the size and inode describe the
    // file that is actually read (no check-then-open race).
    let fd;
    try { fd = fssync.openSync(filePath, "r"); } catch { continue; }
    let stat;
    try { stat = fssync.fstatSync(fd); } catch {
      try { fssync.closeSync(fd); } catch { /* ignore */ }
      continue;
    }

    const prev = fileOffsets[filePath] || {};
    const prevSize = Number(prev.size) || 0;
    const inodeChanged = typeof prev.ino === "number" && prev.ino !== stat.ino;
    const prefixFp = musePrefixFingerprint(fd, Math.min(prevSize, stat.size));
    const headFp = museHeadFingerprint(fd, stat.size);
    // Same-inode compaction can shrink then grow past prevSize, rewrite the
    // middle at equal size, or be observed while empty mid-rewrite. Validate
    // the consumed-prefix fingerprint (and forceRewind) before append-only.
    const appendOnly = !inodeChanged && stat.size >= prevSize &&
      museResumeLooksAppendOnly(fd, prevSize, prev, prefixFp, stat.mtimeMs, stat.size);
    const rewound = !appendOnly;
    const startOffset = rewound ? 0 : prevSize;
    if (stat.size <= startOffset) {
      const emptyObserved = stat.size <= 0 && prevSize > 0;
      fileOffsets[filePath] = {
        ...prev,
        size: Math.min(prevSize, stat.size),
        mtimeMs: stat.mtimeMs,
        ino: stat.ino,
        headFp,
        prefixFp: stat.size > 0 ? musePrefixFingerprint(fd, stat.size) : null,
        // An empty observation between truncate and rewrite must not clear
        // watermark-based replay protection on the next population.
        forceRewind: emptyObserved || prev.forceRewind === true || rewound,
      };
      try { fssync.closeSync(fd); } catch { /* ignore */ }
      continue;
    }

    const fileState = {
      // Keep known model attribution across rewinds: compaction may drop the
      // original run.model.configured rows while later usage for those runs
      // remains. Replayed configuration records still overwrite these maps.
      model: prev.model || null,
      runModels: { ...(prev.runModels || {}) },
      // A rewritten log keeps its flag: its usage ids were already counted.
      counted: prev.counted === true,
      // Latest usage timestamp (epoch µs) counted from this file; on a rewrite
      // it becomes the point below which rows are not counted again.
      watermark: Number(prev.watermark) || 0,
      watermarkIds: Array.isArray(prev.watermarkIds) ? prev.watermarkIds.slice(0, MUSE_WATERMARK_IDS_MAX) : [],
      skipThrough: rewound ? Number(prev.watermark) || 0 : 0,
      skipIds: new Set(rewound && Array.isArray(prev.watermarkIds) ? prev.watermarkIds : []),
      isSubagent: isMuseSubagentSessionFile(filePath, env || process.env),
    };

    let consumed = 0;
    // Hash the bytes we actually parse so a rewrite between read and cursor
    // save cannot invent a matching fingerprint for unread content.
    const consumedHash = crypto.createHash("sha256");
    let hashedPrefix = startOffset === 0;
    if (startOffset > 0 && startOffset <= MUSE_PREFIX_FINGERPRINT_MAX_BYTES) {
      // Include the already-trusted prefix so the saved fingerprint covers
      // the full consumed region, not only this sync's delta.
      let off = 0;
      const pre = Buffer.alloc(Math.min(64 * 1024, startOffset));
      while (off < startOffset) {
        const want = Math.min(pre.length, startOffset - off);
        const read = fssync.readSync(fd, pre, 0, want, off);
        if (read <= 0) break;
        consumedHash.update(pre.subarray(0, read));
        off += read;
      }
      hashedPrefix = off === startOffset;
    } else if (startOffset > MUSE_PREFIX_FINGERPRINT_MAX_BYTES) {
      hashedPrefix = false;
    }
    {
      let position = startOffset;
      let carry = Buffer.alloc(0);
      while (position < stat.size) {
        const length = Math.min(MUSE_READ_CHUNK_BYTES, stat.size - position);
        const chunk = Buffer.alloc(length);
        const read = fssync.readSync(fd, chunk, 0, length, position);
        if (read <= 0) break;
        position += read;
        const data = carry.length ? Buffer.concat([carry, chunk.subarray(0, read)]) : chunk.subarray(0, read);
        // Only complete lines are consumed, so a record still being written is
        // read whole on the next sync instead of being skipped.
        const lastNewline = data.lastIndexOf(0x0a);
        if (lastNewline < 0) { carry = data; continue; }
        const complete = data.subarray(0, lastNewline + 1);
        carry = Buffer.from(data.subarray(lastNewline + 1));
        if (hashedPrefix) consumedHash.update(complete);
        consumed += complete.length;
        for (const line of complete.toString("utf8").split("\n")) {
          if (!line.trim()) continue;
          let entry;
          try { entry = JSON.parse(line); } catch { continue; }
          for (const rec of museRecords(entry)) handleRecord(rec, fileState);
        }
      }
    }
    const runModelKeys = Object.keys(fileState.runModels);
    const keptRunModels = {};
    // Bound the cursor: later records reference recent runs.
    for (const key of runModelKeys.slice(-50)) keptRunModels[key] = fileState.runModels[key];
    const consumedSize = startOffset + consumed;
    // Fingerprint from the same descriptor we read — never reopen (CodeQL
    // file-system-race) and never trust a post-read rewrite.
    let savedPrefixFp = null;
    if (hashedPrefix && consumedSize <= MUSE_PREFIX_FINGERPRINT_MAX_BYTES) {
      savedPrefixFp = consumedHash.digest("hex");
    } else {
      try { savedPrefixFp = musePrefixFingerprint(fd, consumedSize); } catch { /* best-effort */ }
    }
    fileOffsets[filePath] = {
      size: consumedSize,
      mtimeMs: stat.mtimeMs,
      ino: stat.ino,
      headFp,
      prefixFp: savedPrefixFp,
      forceRewind: false,
      model: fileState.model,
      runModels: keptRunModels,
      counted: fileState.counted,
      watermark: fileState.watermark,
      watermarkIds: fileState.watermarkIds,
    };
    try { fssync.closeSync(fd); } catch { /* ignore */ }
    if (cb) {
      cb({ index: fileIdx + 1, total: files.length, recordsProcessed, eventsAggregated, bucketsQueued: touchedBuckets.size });
    }
  }

  const seenArr = Array.from(seenIds);
  const cappedSeen = seenArr.length > 10_000 ? seenArr.slice(seenArr.length - 10_000) : seenArr;
  const bucketsQueued = await enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets });
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  cursors.muse = { ...state, seenIds: cappedSeen, fileOffsets, updatedAt };
  return { recordsProcessed, eventsAggregated, bucketsQueued };
}

module.exports = {
  resolveMuseHome,
  resolveMuseSessionFiles,
  parseMuseIncremental,
};
