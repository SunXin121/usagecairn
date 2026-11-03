// Shared parsing helpers: bucket math, cursors, queue writes and file utilities used by more than one provider.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fs = require("node:fs/promises");
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const readline = require("node:readline");
const crypto = require("node:crypto");
const { ensureDir } = require("../fs");
const { physicalJsonlRecords } = require("../jsonl-lines");
const { readSqliteJsonRows } = require("../sqlite-reader");
const wsl = require("../wsl-probe");
const { resolveInstallPaths } = require("../install-resolver");
const { USD_TICKS_PER_USD } = require("../grok-usage");

const DEFAULT_SOURCE = "codex";
const DEFAULT_MODEL = "unknown";
const BUCKET_SEPARATOR = "|";
const CLAUDE_MEM_OBSERVER_PATH_SEGMENT = "--claude-mem-observer-sessions";
const CLAUDE_MEM_OBSERVER_PROJECT_REF =
  "https://local.token-pulse/claude-mem/observer-sessions";
const PROJECT_ABSENT_CONTEXT_RESCAN_MS = 24 * 60 * 60 * 1000;

async function enqueueTouchedBuckets({ queuePath, hourlyState, touchedBuckets }) {
  if (!touchedBuckets || touchedBuckets.size === 0) return 0;

  const touchedGroups = new Set();
  for (const bucketStart of touchedBuckets) {
    const parsed = parseBucketKey(bucketStart);
    const hourStart = parsed.hourStart;
    if (!hourStart) continue;
    touchedGroups.add(groupBucketKey(parsed.source, hourStart));
  }
  if (touchedGroups.size === 0) return 0;

  const groupQueued =
    hourlyState.groupQueued && typeof hourlyState.groupQueued === "object"
      ? hourlyState.groupQueued
      : {};
  let codexTouched = false;
  const legacyGroups = new Set();
  for (const groupKey of touchedGroups) {
    if (Object.prototype.hasOwnProperty.call(groupQueued, groupKey)) {
      legacyGroups.add(groupKey);
    }
    if (!codexTouched && groupKey.startsWith(`${DEFAULT_SOURCE}${BUCKET_SEPARATOR}`)) {
      codexTouched = true;
    }
  }

  const groupedBuckets = new Map();
  for (const [key, bucket] of Object.entries(hourlyState.buckets || {})) {
    if (!bucket || !bucket.totals) continue;
    const parsed = parseBucketKey(key);
    const hourStart = parsed.hourStart;
    if (!hourStart) continue;
    const groupKey = groupBucketKey(parsed.source, hourStart);
    if (!touchedGroups.has(groupKey) || legacyGroups.has(groupKey)) continue;

    const source = normalizeSourceInput(parsed.source) || DEFAULT_SOURCE;
    const model = normalizeModelInput(parsed.model) || DEFAULT_MODEL;
    let group = groupedBuckets.get(groupKey);
    if (!group) {
      group = { source, hourStart, buckets: new Map() };
      groupedBuckets.set(groupKey, group);
    }

    if (bucket.queuedKey != null && typeof bucket.queuedKey !== "string") {
      bucket.queuedKey = null;
    }
    group.buckets.set(model, bucket);
  }

  if (codexTouched) {
    const recomputeGroups = new Set();
    for (const [key, bucket] of Object.entries(hourlyState.buckets || {})) {
      if (!bucket || !bucket.totals) continue;
      const parsed = parseBucketKey(key);
      const hourStart = parsed.hourStart;
      if (!hourStart) continue;
      const source = normalizeSourceInput(parsed.source) || DEFAULT_SOURCE;
      if (source !== "every-code") continue;
      const groupKey = groupBucketKey(source, hourStart);
      if (legacyGroups.has(groupKey) || groupedBuckets.has(groupKey)) continue;
      const model = normalizeModelInput(parsed.model) || DEFAULT_MODEL;
      if (model !== DEFAULT_MODEL) continue;
      recomputeGroups.add(groupKey);
    }

    if (recomputeGroups.size > 0) {
      for (const [key, bucket] of Object.entries(hourlyState.buckets || {})) {
        if (!bucket || !bucket.totals) continue;
        const parsed = parseBucketKey(key);
        const hourStart = parsed.hourStart;
        if (!hourStart) continue;
        const source = normalizeSourceInput(parsed.source) || DEFAULT_SOURCE;
        const groupKey = groupBucketKey(source, hourStart);
        if (!recomputeGroups.has(groupKey)) continue;
        let group = groupedBuckets.get(groupKey);
        if (!group) {
          group = { source, hourStart, buckets: new Map() };
          groupedBuckets.set(groupKey, group);
        }
        if (bucket.queuedKey != null && typeof bucket.queuedKey !== "string") {
          bucket.queuedKey = null;
        }
        const model = normalizeModelInput(parsed.model) || DEFAULT_MODEL;
        group.buckets.set(model, bucket);
      }
    }
  }

  const codexDominants = collectCodexDominantModels(hourlyState);

  const toAppend = [];
  for (const group of groupedBuckets.values()) {
    const unknownBucket = group.buckets.get(DEFAULT_MODEL) || null;
    const dominantModel = pickDominantModel(group.buckets);
    let alignedModel = null;
    if (unknownBucket?.alignedModel) {
      const normalized = normalizeModelInput(unknownBucket.alignedModel);
      alignedModel = normalized && normalized !== DEFAULT_MODEL ? normalized : null;
    }
    const zeroTotals = initTotals();
    const zeroKey = totalsKey(zeroTotals);

    if (dominantModel) {
      if (alignedModel && !group.buckets.has(alignedModel)) {
        toAppend.push(
          JSON.stringify({
            source: group.source,
            model: alignedModel,
            hour_start: group.hourStart,
            input_tokens: zeroTotals.input_tokens,
            cached_input_tokens: zeroTotals.cached_input_tokens,
            cache_creation_input_tokens: zeroTotals.cache_creation_input_tokens,
            output_tokens: zeroTotals.output_tokens,
            reasoning_output_tokens: zeroTotals.reasoning_output_tokens,
            total_tokens: zeroTotals.total_tokens,
            billable_total_tokens: zeroTotals.billable_total_tokens,
            total_cost_usd: zeroTotals.total_cost_usd,
            conversation_count: zeroTotals.conversation_count,
          }),
        );
      }
      if (
        unknownBucket &&
        !alignedModel &&
        unknownBucket.queuedKey &&
        unknownBucket.queuedKey !== zeroKey
      ) {
        if (unknownBucket.retractedUnknownKey !== zeroKey) {
          toAppend.push(
            JSON.stringify({
              source: group.source,
              model: DEFAULT_MODEL,
              hour_start: group.hourStart,
              input_tokens: zeroTotals.input_tokens,
              cached_input_tokens: zeroTotals.cached_input_tokens,
              cache_creation_input_tokens: zeroTotals.cache_creation_input_tokens,
              output_tokens: zeroTotals.output_tokens,
              reasoning_output_tokens: zeroTotals.reasoning_output_tokens,
              total_tokens: zeroTotals.total_tokens,
              billable_total_tokens: zeroTotals.billable_total_tokens,
              total_cost_usd: zeroTotals.total_cost_usd,
              conversation_count: zeroTotals.conversation_count,
            }),
          );
          unknownBucket.retractedUnknownKey = zeroKey;
        }
      }
      if (unknownBucket) unknownBucket.alignedModel = null;
      for (const [model, bucket] of group.buckets.entries()) {
        if (model === DEFAULT_MODEL) continue;
        let totals = bucket.totals;
        if (model === dominantModel && unknownBucket?.totals) {
          totals = cloneTotals(bucket.totals);
          addTotals(totals, unknownBucket.totals);
        }
        const usagePrecision = bucket.usage_precision || null;
        const key = usagePrecision ? `${totalsKey(totals)}|${usagePrecision}` : totalsKey(totals);
        if (bucket.queuedKey === key) continue;
        toAppend.push(
          JSON.stringify({
            source: group.source,
            model,
            hour_start: group.hourStart,
            input_tokens: totals.input_tokens,
            cached_input_tokens: totals.cached_input_tokens,
            cache_creation_input_tokens: totals.cache_creation_input_tokens,
            output_tokens: totals.output_tokens,
            reasoning_output_tokens: totals.reasoning_output_tokens,
            total_tokens: totals.total_tokens,
            billable_total_tokens: totals.billable_total_tokens ?? totals.total_tokens,
            total_cost_usd: totals.total_cost_usd || 0,
            usage_precision: usagePrecision || undefined,
            ...prioritySubsetOf(totals),
            conversation_count: totals.conversation_count,
          }),
        );
        bucket.queuedKey = key;
      }
      continue;
    }

    if (!unknownBucket?.totals) continue;
    let outputModel = DEFAULT_MODEL;
    if (group.source === "every-code") {
      const aligned = findNearestCodexModel(group.hourStart, codexDominants);
      if (aligned) outputModel = aligned;
    }
    const nextAligned = outputModel !== DEFAULT_MODEL ? outputModel : null;
    if (alignedModel && alignedModel !== nextAligned) {
      toAppend.push(
        JSON.stringify({
          source: group.source,
          model: alignedModel,
          hour_start: group.hourStart,
          input_tokens: zeroTotals.input_tokens,
          cached_input_tokens: zeroTotals.cached_input_tokens,
          cache_creation_input_tokens: zeroTotals.cache_creation_input_tokens,
          output_tokens: zeroTotals.output_tokens,
          reasoning_output_tokens: zeroTotals.reasoning_output_tokens,
          total_tokens: zeroTotals.total_tokens,
          billable_total_tokens: zeroTotals.billable_total_tokens,
          total_cost_usd: zeroTotals.total_cost_usd,
          conversation_count: zeroTotals.conversation_count,
        }),
      );
    }
    if (
      !alignedModel &&
      nextAligned &&
      unknownBucket.queuedKey &&
      unknownBucket.queuedKey !== zeroKey
    ) {
      if (unknownBucket.retractedUnknownKey !== zeroKey) {
        toAppend.push(
          JSON.stringify({
            source: group.source,
            model: DEFAULT_MODEL,
            hour_start: group.hourStart,
            input_tokens: zeroTotals.input_tokens,
            cached_input_tokens: zeroTotals.cached_input_tokens,
            cache_creation_input_tokens: zeroTotals.cache_creation_input_tokens,
            output_tokens: zeroTotals.output_tokens,
            reasoning_output_tokens: zeroTotals.reasoning_output_tokens,
            total_tokens: zeroTotals.total_tokens,
            billable_total_tokens: zeroTotals.billable_total_tokens,
            total_cost_usd: zeroTotals.total_cost_usd,
            conversation_count: zeroTotals.conversation_count,
          }),
        );
        unknownBucket.retractedUnknownKey = zeroKey;
      }
    }
    if (unknownBucket) unknownBucket.alignedModel = nextAligned;
    const usagePrecision = unknownBucket.usage_precision || null;
    const key = usagePrecision
      ? `${totalsKey(unknownBucket.totals)}|${usagePrecision}`
      : totalsKey(unknownBucket.totals);
    const outputKey = outputModel === DEFAULT_MODEL ? key : `${key}|${outputModel}`;
    if (unknownBucket.queuedKey === outputKey) continue;
    toAppend.push(
      JSON.stringify({
        source: group.source,
        model: outputModel,
        hour_start: group.hourStart,
        input_tokens: unknownBucket.totals.input_tokens,
        cached_input_tokens: unknownBucket.totals.cached_input_tokens,
        cache_creation_input_tokens: unknownBucket.totals.cache_creation_input_tokens,
        output_tokens: unknownBucket.totals.output_tokens,
        reasoning_output_tokens: unknownBucket.totals.reasoning_output_tokens,
        total_tokens: unknownBucket.totals.total_tokens,
        billable_total_tokens: unknownBucket.totals.billable_total_tokens ?? unknownBucket.totals.total_tokens,
        total_cost_usd: unknownBucket.totals.total_cost_usd || 0,
        usage_precision: usagePrecision || undefined,
        ...prioritySubsetOf(unknownBucket.totals),
        conversation_count: unknownBucket.totals.conversation_count,
      }),
    );
    unknownBucket.queuedKey = outputKey;
  }

  if (legacyGroups.size > 0) {
    const grouped = new Map();
    for (const [key, bucket] of Object.entries(hourlyState.buckets || {})) {
      if (!bucket || !bucket.totals) continue;
      const parsed = parseBucketKey(key);
      const hourStart = parsed.hourStart;
      if (!hourStart) continue;
      const groupKey = groupBucketKey(parsed.source, hourStart);
      if (!legacyGroups.has(groupKey)) continue;

      let group = grouped.get(groupKey);
      if (!group) {
        group = {
          source: normalizeSourceInput(parsed.source) || DEFAULT_SOURCE,
          hourStart,
          models: new Set(),
          totals: initTotals(),
        };
        grouped.set(groupKey, group);
      }
      group.models.add(parsed.model || DEFAULT_MODEL);
      addTotals(group.totals, bucket.totals);
    }

    for (const group of grouped.values()) {
      const model = group.models.size === 1 ? [...group.models][0] : DEFAULT_MODEL;
      const key = totalsKey(group.totals);
      const groupKey = groupBucketKey(group.source, group.hourStart);
      if (groupQueued[groupKey] === key) continue;
      toAppend.push(
        JSON.stringify({
          source: group.source,
          model,
          hour_start: group.hourStart,
          input_tokens: group.totals.input_tokens,
          cached_input_tokens: group.totals.cached_input_tokens,
          cache_creation_input_tokens: group.totals.cache_creation_input_tokens,
          output_tokens: group.totals.output_tokens,
          reasoning_output_tokens: group.totals.reasoning_output_tokens,
          total_tokens: group.totals.total_tokens,
          billable_total_tokens: group.totals.billable_total_tokens ?? group.totals.total_tokens,
          total_cost_usd: group.totals.total_cost_usd || 0,
          ...prioritySubsetOf(group.totals),
          conversation_count: group.totals.conversation_count,
        }),
      );
      groupQueued[groupKey] = key;
    }
  }

  hourlyState.groupQueued = groupQueued;

  if (toAppend.length > 0) {
    await fs.appendFile(queuePath, toAppend.join("\n") + "\n", "utf8");
  }

  return toAppend.length;
}

async function enqueueTouchedProjectBuckets({
  projectQueuePath,
  projectState,
  projectTouchedBuckets,
}) {
  if (
    !projectQueuePath ||
    !projectState ||
    !projectTouchedBuckets ||
    projectTouchedBuckets.size === 0
  )
    return 0;

  await ensureDir(path.dirname(projectQueuePath));

  const toAppend = [];
  for (const key of projectTouchedBuckets) {
    const bucket = projectState.buckets[key];
    if (!bucket || !bucket.totals) continue;
    const totals = bucket.totals;
    const queuedKey = totalsKey(totals);
    if (bucket.queuedKey === queuedKey) continue;
    const projectRef = typeof bucket.project_ref === "string" ? bucket.project_ref : null;
    const projectKey = typeof bucket.project_key === "string" ? bucket.project_key : null;
    if (!projectRef || !projectKey) continue;

    toAppend.push(
      JSON.stringify({
        project_ref: projectRef,
        project_key: projectKey,
        source: bucket.source,
        hour_start: bucket.hour_start,
        input_tokens: totals.input_tokens,
        cached_input_tokens: totals.cached_input_tokens,
        cache_creation_input_tokens: totals.cache_creation_input_tokens,
        output_tokens: totals.output_tokens,
        reasoning_output_tokens: totals.reasoning_output_tokens,
        total_tokens: totals.total_tokens,
        billable_total_tokens: totals.billable_total_tokens ?? totals.total_tokens,
        total_cost_usd: totals.total_cost_usd || 0,
        conversation_count: totals.conversation_count,
      }),
    );
    bucket.queuedKey = queuedKey;
  }

  if (toAppend.length > 0) {
    await fs.appendFile(projectQueuePath, toAppend.join("\n") + "\n", "utf8");
  }

  return toAppend.length;
}

function pickDominantModel(buckets) {
  let dominantModel = null;
  let dominantTotal = -1;
  for (const [model, bucket] of buckets.entries()) {
    if (model === DEFAULT_MODEL) continue;
    const total = Number(bucket?.totals?.total_tokens || 0);
    if (
      dominantModel == null ||
      total > dominantTotal ||
      (total === dominantTotal && model < dominantModel)
    ) {
      dominantModel = model;
      dominantTotal = total;
    }
  }
  return dominantModel;
}

function cloneTotals(totals) {
  const cloned = initTotals();
  addTotals(cloned, totals || {});
  return cloned;
}

function collectCodexDominantModels(hourlyState) {
  const grouped = new Map();
  for (const [key, bucket] of Object.entries(hourlyState.buckets || {})) {
    if (!bucket || !bucket.totals) continue;
    const parsed = parseBucketKey(key);
    const hourStart = parsed.hourStart;
    if (!hourStart) continue;
    const source = normalizeSourceInput(parsed.source) || DEFAULT_SOURCE;
    if (source !== DEFAULT_SOURCE) continue;
    const model = normalizeModelInput(parsed.model) || DEFAULT_MODEL;
    if (model === DEFAULT_MODEL) continue;

    let models = grouped.get(hourStart);
    if (!models) {
      models = new Map();
      grouped.set(hourStart, models);
    }
    const total = Number(bucket.totals.total_tokens || 0);
    models.set(model, (models.get(model) || 0) + total);
  }

  const dominants = [];
  for (const [hourStart, models] of grouped.entries()) {
    let dominantModel = null;
    let dominantTotal = -1;
    for (const [model, total] of models.entries()) {
      if (
        dominantModel == null ||
        total > dominantTotal ||
        (total === dominantTotal && model < dominantModel)
      ) {
        dominantModel = model;
        dominantTotal = total;
      }
    }
    if (dominantModel) {
      dominants.push({ hourStart, model: dominantModel });
    }
  }

  return dominants;
}

function findNearestCodexModel(hourStart, dominants) {
  if (!hourStart || !dominants || dominants.length === 0) return null;
  const target = Date.parse(hourStart);
  if (!Number.isFinite(target)) return null;

  let best = null;
  for (const entry of dominants) {
    const candidate = Date.parse(entry.hourStart);
    if (!Number.isFinite(candidate)) continue;
    const diff = Math.abs(candidate - target);
    if (!best || diff < best.diff || (diff === best.diff && candidate < best.time)) {
      best = { diff, time: candidate, model: entry.model };
    }
  }

  return best ? best.model : null;
}

function normalizeHourlyState(raw) {
  const state = raw && typeof raw === "object" ? raw : {};
  const version = Number(state.version || 1);
  const rawBuckets = state.buckets && typeof state.buckets === "object" ? state.buckets : {};
  const buckets = {};
  const groupQueued = {};

  if (!Number.isFinite(version) || version < 2) {
    for (const [key, value] of Object.entries(rawBuckets)) {
      const parsed = parseBucketKey(key);
      const hourStart = parsed.hourStart;
      if (!hourStart) continue;
      const source = normalizeSourceInput(parsed.source) || DEFAULT_SOURCE;
      const normalizedKey = bucketKey(source, DEFAULT_MODEL, hourStart);
      buckets[normalizedKey] = value;
      if (value?.queuedKey) {
        groupQueued[groupBucketKey(source, hourStart)] = value.queuedKey;
      }
    }
    return {
      version: 3,
      buckets,
      groupQueued,
      updatedAt: typeof state.updatedAt === "string" ? state.updatedAt : null,
    };
  }

  for (const [key, value] of Object.entries(rawBuckets)) {
    const parsed = parseBucketKey(key);
    const hourStart = parsed.hourStart;
    if (!hourStart) continue;
    const normalizedKey = bucketKey(parsed.source, parsed.model, hourStart);
    buckets[normalizedKey] = value;
  }

  const existingGroupQueued =
    state.groupQueued && typeof state.groupQueued === "object" ? state.groupQueued : {};

  return {
    version: 3,
    buckets,
    groupQueued: version >= 3 ? existingGroupQueued : {},
    updatedAt: typeof state.updatedAt === "string" ? state.updatedAt : null,
  };
}

function normalizeProjectState(raw) {
  const state = raw && typeof raw === "object" ? raw : {};
  const rawBuckets = state.buckets && typeof state.buckets === "object" ? state.buckets : {};
  const buckets = {};
  const rawProjects = state.projects && typeof state.projects === "object" ? state.projects : {};
  const projects = {};

  for (const [key, value] of Object.entries(rawBuckets)) {
    if (!key) continue;
    buckets[key] = value;
  }

  for (const [key, value] of Object.entries(rawProjects)) {
    if (!key || !value || typeof value !== "object") continue;
    projects[key] = { ...value };
  }

  return {
    version: 2,
    buckets,
    projects,
    updatedAt: typeof state.updatedAt === "string" ? state.updatedAt : null,
  };
}

function normalizeQoderState(raw) {
  const state = raw && typeof raw === "object" ? raw : {};
  const messages = state.messages && typeof state.messages === "object" ? state.messages : {};
  return {
    messages,
    updatedAt: typeof state.updatedAt === "string" ? state.updatedAt : null,
  };
}

function normalizeMessageKeyPart(value) {
  if (typeof value !== "string") return "";
  return value.trim();
}

function getHourlyBucket(state, source, model, hourStart) {
  const buckets = state.buckets;
  const normalizedSource = normalizeSourceInput(source) || DEFAULT_SOURCE;
  const normalizedModel = normalizeModelInput(model) || DEFAULT_MODEL;
  const key = bucketKey(normalizedSource, normalizedModel, hourStart);
  let bucket = buckets[key];
  if (!bucket || typeof bucket !== "object") {
    bucket = { totals: initTotals(), queuedKey: null };
    buckets[key] = bucket;
    return bucket;
  }

  if (!bucket.totals || typeof bucket.totals !== "object") {
    bucket.totals = initTotals();
  }

  if (bucket.queuedKey != null && typeof bucket.queuedKey !== "string") {
    bucket.queuedKey = null;
  }

  return bucket;
}

function getProjectBucket(state, projectKey, source, hourStart, projectRef) {
  const buckets = state.buckets;
  const normalizedSource = normalizeSourceInput(source) || DEFAULT_SOURCE;
  const key = projectBucketKey(projectKey, normalizedSource, hourStart);
  let bucket = buckets[key];
  if (!bucket || typeof bucket !== "object") {
    bucket = {
      totals: initTotals(),
      queuedKey: null,
      project_key: projectKey,
      project_ref: projectRef,
      source: normalizedSource,
      hour_start: hourStart,
    };
    buckets[key] = bucket;
    return bucket;
  }

  if (!bucket.totals || typeof bucket.totals !== "object") {
    bucket.totals = initTotals();
  }

  if (bucket.queuedKey != null && typeof bucket.queuedKey !== "string") {
    bucket.queuedKey = null;
  }

  if (projectRef) bucket.project_ref = projectRef;
  if (projectKey) bucket.project_key = projectKey;
  bucket.source = normalizedSource;
  bucket.hour_start = hourStart;

  return bucket;
}

function initTotals() {
  return {
    input_tokens: 0,
    cached_input_tokens: 0,
    cache_creation_input_tokens: 0,
    output_tokens: 0,
    reasoning_output_tokens: 0,
    total_tokens: 0,
    billable_total_tokens: 0,
    total_cost_usd: 0,
    conversation_count: 0,
  };
}

// Observed-subset markers, same shape as the session sidecar's
// long_context_* columns: "how much of the columns above came from requests on
// the priority (Astra Fast) service tier". They annotate the existing columns
// and are NEVER part of any sum — total_tokens stays
// input + output + cache_creation + cache_read + reasoning_output. They are
// omitted entirely when zero so no other provider's queue rows change.
const PRIORITY_SUBSET_FIELDS = [
  ["priority_input_tokens", "input_tokens"],
  ["priority_cached_input_tokens", "cached_input_tokens"],
  ["priority_cache_creation_input_tokens", "cache_creation_input_tokens"],
  ["priority_output_tokens", "output_tokens"],
  ["priority_reasoning_output_tokens", "reasoning_output_tokens"],
];

function prioritySubsetOf(totals) {
  const subset = {};
  for (const [field] of PRIORITY_SUBSET_FIELDS) {
    const value = Number(totals?.[field]) || 0;
    if (value > 0) subset[field] = value;
  }
  return subset;
}

function addTotals(target, delta) {
  for (const [field] of PRIORITY_SUBSET_FIELDS) {
    const value = Number(delta?.[field]) || 0;
    if (value > 0) target[field] = (Number(target[field]) || 0) + value;
  }
  target.input_tokens += delta.input_tokens || 0;
  target.cached_input_tokens += delta.cached_input_tokens || 0;
  target.cache_creation_input_tokens += delta.cache_creation_input_tokens || 0;
  target.output_tokens += delta.output_tokens || 0;
  target.reasoning_output_tokens += delta.reasoning_output_tokens || 0;
  target.total_tokens += delta.total_tokens || 0;
  target.billable_total_tokens += delta.billable_total_tokens ?? delta.total_tokens ?? 0;
  target.total_cost_usd = Math.round(
    ((target.total_cost_usd || 0) + (delta.total_cost_usd || 0)) * USD_TICKS_PER_USD,
  ) / USD_TICKS_PER_USD;
  target.conversation_count += delta.conversation_count || 0;
}

function subtractTotals(target, totals) {
  for (const [field] of PRIORITY_SUBSET_FIELDS) {
    const current = Number(target[field]) || 0;
    if (current <= 0) continue;
    target[field] = Math.max(0, current - (Number(totals?.[field]) || 0));
  }
  target.input_tokens = Math.max(0, target.input_tokens - (totals.input_tokens || 0));
  target.cached_input_tokens = Math.max(
    0,
    target.cached_input_tokens - (totals.cached_input_tokens || 0),
  );
  target.cache_creation_input_tokens = Math.max(
    0,
    target.cache_creation_input_tokens - (totals.cache_creation_input_tokens || 0),
  );
  target.output_tokens = Math.max(0, target.output_tokens - (totals.output_tokens || 0));
  target.reasoning_output_tokens = Math.max(
    0,
    target.reasoning_output_tokens - (totals.reasoning_output_tokens || 0),
  );
  target.total_tokens = Math.max(0, target.total_tokens - (totals.total_tokens || 0));
  target.billable_total_tokens = Math.max(
    0,
    target.billable_total_tokens -
      (totals.billable_total_tokens ?? totals.total_tokens ?? 0),
  );
  target.total_cost_usd = Math.max(
    0,
    Math.round(
      ((target.total_cost_usd || 0) - (totals.total_cost_usd || 0)) * USD_TICKS_PER_USD,
    ) / USD_TICKS_PER_USD,
  );
  target.conversation_count = Math.max(
    0,
    target.conversation_count - (totals.conversation_count || 0),
  );
}

function totalsKey(totals) {
  const base = [
    totals.input_tokens || 0,
    totals.cached_input_tokens || 0,
    totals.cache_creation_input_tokens || 0,
    totals.output_tokens || 0,
    totals.reasoning_output_tokens || 0,
    totals.total_tokens || 0,
    totals.billable_total_tokens ?? totals.total_tokens ?? 0,
    totals.total_cost_usd || 0,
    totals.conversation_count || 0,
  ].join("|");
  // Appended only when a priority subset exists, so buckets that never see the
  // tier keep their previously persisted queuedKey and are not re-enqueued.
  const priority = PRIORITY_SUBSET_FIELDS.map(([field]) => Number(totals[field]) || 0);
  return priority.some((value) => value > 0) ? `${base}|p:${priority.join(",")}` : base;
}

function toUtcHalfHourStart(ts) {
  const dt = new Date(ts);
  if (!Number.isFinite(dt.getTime())) return null;
  const minutes = dt.getUTCMinutes();
  const halfMinute = minutes >= 30 ? 30 : 0;
  const bucketStart = new Date(
    Date.UTC(
      dt.getUTCFullYear(),
      dt.getUTCMonth(),
      dt.getUTCDate(),
      dt.getUTCHours(),
      halfMinute,
      0,
      0,
    ),
  );
  return bucketStart.toISOString();
}

function bucketKey(source, model, hourStart) {
  const safeSource = normalizeSourceInput(source) || DEFAULT_SOURCE;
  const safeModel = normalizeModelInput(model) || DEFAULT_MODEL;
  return `${safeSource}${BUCKET_SEPARATOR}${safeModel}${BUCKET_SEPARATOR}${hourStart}`;
}

function projectBucketKey(projectKey, source, hourStart) {
  const safeSource = normalizeSourceInput(source) || DEFAULT_SOURCE;
  return `${projectKey}${BUCKET_SEPARATOR}${safeSource}${BUCKET_SEPARATOR}${hourStart}`;
}

function groupBucketKey(source, hourStart) {
  const safeSource = normalizeSourceInput(source) || DEFAULT_SOURCE;
  return `${safeSource}${BUCKET_SEPARATOR}${hourStart}`;
}

function parseBucketKey(key) {
  if (typeof key !== "string")
    return { source: DEFAULT_SOURCE, model: DEFAULT_MODEL, hourStart: "" };
  const first = key.indexOf(BUCKET_SEPARATOR);
  if (first <= 0) return { source: DEFAULT_SOURCE, model: DEFAULT_MODEL, hourStart: key };
  const second = key.indexOf(BUCKET_SEPARATOR, first + 1);
  if (second <= 0) {
    return { source: key.slice(0, first), model: DEFAULT_MODEL, hourStart: key.slice(first + 1) };
  }
  return {
    source: key.slice(0, first),
    model: key.slice(first + 1, second),
    hourStart: key.slice(second + 1),
  };
}

function normalizeSourceInput(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim().toLowerCase();
  return trimmed.length > 0 ? trimmed : null;
}

function normalizeModelInput(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

async function resolveProjectMetaForPath(startDir, cache) {
  if (!startDir || typeof startDir !== "string") return null;
  if (cache && cache.has(startDir)) return cache.get(startDir);

  if (startDir.includes(CLAUDE_MEM_OBSERVER_PATH_SEGMENT)) {
    const meta = { projectRef: CLAUDE_MEM_OBSERVER_PROJECT_REF, repoRoot: startDir };
    if (cache) cache.set(startDir, meta);
    return meta;
  }

  const visited = [];
  let current = startDir;
  const root = path.parse(startDir).root;
  while (current) {
    if (cache && cache.has(current)) {
      const cached = cache.get(current);
      for (const entry of visited) cache.set(entry, cached);
      return cached;
    }
    visited.push(current);

    const configPath = await resolveGitConfigPath(current);
    if (configPath) {
      const configStat = await fs.stat(configPath).catch(() => null);
      const remoteUrl = await readGitRemoteUrl(configPath);
      const projectRef = canonicalizeProjectRef(remoteUrl);
      const meta = {
        projectRef: projectRef || null,
        repoRoot: current,
        configPath,
        configMtimeMs:
          configStat && Number.isFinite(configStat.mtimeMs) ? configStat.mtimeMs : null,
        configSize: configStat && Number.isFinite(configStat.size) ? configStat.size : null,
      };
      if (cache) {
        for (const entry of visited) cache.set(entry, meta);
      }
      return meta;
    }

    if (current === root) break;
    const parent = path.dirname(current);
    if (!parent || parent === current) break;
    current = parent;
  }

  if (cache) {
    for (const entry of visited) cache.set(entry, null);
  }
  return null;
}

function buildProjectFileContext(projectContexts, checkedAtMs = Date.now()) {
  const contexts = Array.isArray(projectContexts)
    ? projectContexts
    : projectContexts && typeof projectContexts === "object"
      ? [projectContexts]
      : [];
  const seen = new Set();
  const configs = [];
  for (const context of contexts) {
    const configPath =
      typeof context?.configPath === "string" && context.configPath ? context.configPath : null;
    if (!configPath || seen.has(configPath)) continue;
    seen.add(configPath);
    configs.push({
      configPath,
      configMtimeMs: Number.isFinite(context.configMtimeMs) ? context.configMtimeMs : null,
      configSize: Number.isFinite(context.configSize) ? context.configSize : null,
    });
  }
  if (configs.length === 0) return { absent: true, checkedAtMs };
  if (configs.length > 1) return { configs };
  const [config] = configs;
  return {
    ...config,
    configs,
  };
}

async function isProjectFileContextFresh(projectFileContext, options = {}) {
  if (!projectFileContext || typeof projectFileContext !== "object") return false;
  const nowMs = Number.isFinite(options.nowMs) ? options.nowMs : Date.now();
  const absentTtlMs = Number.isFinite(options.absentTtlMs)
    ? options.absentTtlMs
    : PROJECT_ABSENT_CONTEXT_RESCAN_MS;
  const freshnessCache =
    options.freshnessCache && typeof options.freshnessCache === "object"
      ? options.freshnessCache
      : null;
  if (projectFileContext.absent === true) {
    const checkedAtMs = Number(projectFileContext.checkedAtMs);
    return Number.isFinite(checkedAtMs) && checkedAtMs > 0 && nowMs - checkedAtMs < absentTtlMs;
  }
  if (Array.isArray(projectFileContext.configs)) {
    if (projectFileContext.configs.length === 0) return false;
    for (const config of projectFileContext.configs) {
      if (!(await isSingleProjectConfigFresh(config, freshnessCache))) return false;
    }
    return true;
  }
  return isSingleProjectConfigFresh(projectFileContext, freshnessCache);
}

async function isSingleProjectConfigFresh(projectFileContext, freshnessCache = null) {
  const configPath =
    typeof projectFileContext.configPath === "string" ? projectFileContext.configPath : null;
  if (!configPath) return false;
  let st;
  if (freshnessCache && freshnessCache.has(configPath)) {
    st = freshnessCache.get(configPath);
  } else {
    st = await fs.stat(configPath).catch(() => null);
    if (freshnessCache) freshnessCache.set(configPath, st);
  }
  if (!st || !st.isFile()) return false;
  return (
    st.mtimeMs === projectFileContext.configMtimeMs &&
    st.size === projectFileContext.configSize
  );
}

function hashRepoRoot(repoRoot) {
  return crypto.createHash("sha256").update(String(repoRoot)).digest("hex");
}

function deriveProjectKeyFromRef(projectRef) {
  if (typeof projectRef !== "string") return null;
  try {
    const parsed = new URL(projectRef);
    const segments = parsed.pathname.split("/").filter(Boolean);
    if (segments.length < 2) return null;
    // GitHub paths are always owner/repo, but GitLab supports nested groups
    // (group/subgroup/.../repo). Preserve the full path so nested-group repos
    // don't collapse to the first two segments.
    return segments.join("/");
  } catch (_e) {
    return null;
  }
}

async function defaultPublicRepoResolver({ projectRef, repoRoot }) {
  const repoRootHash = repoRoot ? hashRepoRoot(repoRoot) : null;
  const projectKey = deriveProjectKeyFromRef(projectRef);
  if (!projectKey) {
    return {
      status: "blocked",
      projectKey: null,
      projectRef: projectRef || null,
      repoRootHash,
      reason: projectRef ? "unparseable_ref" : "missing_ref",
    };
  }
  return {
    status: "public_verified",
    projectKey,
    projectRef,
    repoRootHash,
  };
}

function recordProjectMeta(projectState, meta) {
  if (!projectState || !meta || typeof meta !== "object") return;
  const repoRootHash = typeof meta.repoRootHash === "string" ? meta.repoRootHash : null;
  let projectKey = typeof meta.projectKey === "string" ? meta.projectKey : null;
  if (
    !projectKey &&
    repoRootHash &&
    projectState.projects &&
    typeof projectState.projects === "object"
  ) {
    for (const [key, entry] of Object.entries(projectState.projects)) {
      if (entry && entry.repo_root_hash === repoRootHash) {
        projectKey = key;
        break;
      }
    }
  }
  if (!projectKey) return;
  if (!projectState.projects || typeof projectState.projects !== "object") {
    projectState.projects = {};
  }
  const prev = projectState.projects[projectKey] || {};
  const status = typeof meta.status === "string" ? meta.status : null;
  const projectRef = typeof meta.projectRef === "string" ? meta.projectRef : null;
  const next = {
    ...prev,
    project_ref: projectRef || prev.project_ref || null,
    status: status || prev.status || null,
    repo_root_hash: repoRootHash || prev.repo_root_hash || null,
    updated_at: new Date().toISOString(),
  };
  if (status === "blocked" && prev.status !== "blocked") {
    next.purge_pending = true;
  } else if (status && status !== "blocked") {
    next.purge_pending = false;
  }
  projectState.projects[projectKey] = next;
}

async function resolveProjectContextForFile({
  filePath,
  projectMetaCache,
  publicRepoCache,
  publicRepoResolver,
  projectState,
}) {
  if (!filePath) return null;
  return resolveProjectContextForPath({
    startDir: path.dirname(filePath),
    projectMetaCache,
    publicRepoCache,
    publicRepoResolver,
    projectState,
  });
}

async function resolveProjectContextForPath({
  startDir,
  projectMetaCache,
  publicRepoCache,
  publicRepoResolver,
  projectState,
}) {
  if (!startDir) return null;
  const projectMeta = await resolveProjectMetaForPath(startDir, projectMetaCache);
  if (!projectMeta) return null;
  const resolver =
    typeof publicRepoResolver === "function" ? publicRepoResolver : defaultPublicRepoResolver;
  const meta = await resolver({
    projectRef: projectMeta.projectRef,
    repoRoot: projectMeta.repoRoot,
    cache: publicRepoCache,
  });
  const repoRootHash = projectMeta.repoRoot ? hashRepoRoot(projectMeta.repoRoot) : null;
  const normalized = {
    ...(meta || {}),
    projectRef: meta?.projectRef || projectMeta.projectRef,
    projectKey: meta?.projectKey || null,
    status: meta?.status || "blocked",
    repoRootHash: meta?.repoRootHash || repoRootHash,
    configPath: projectMeta.configPath || null,
    configMtimeMs: projectMeta.configMtimeMs ?? null,
    configSize: projectMeta.configSize ?? null,
  };
  recordProjectMeta(projectState, normalized);
  const configFields = {
    configPath: projectMeta.configPath || null,
    configMtimeMs: projectMeta.configMtimeMs ?? null,
    configSize: projectMeta.configSize ?? null,
  };
  if (normalized.status !== "public_verified") {
    return {
      projectRef: normalized.projectRef,
      projectKey: null,
      status: normalized.status,
      ...configFields,
    };
  }
  return {
    projectRef: normalized.projectRef,
    projectKey: normalized.projectKey,
    status: normalized.status,
    ...configFields,
  };
}

async function resolveGitConfigPath(rootDir) {
  const gitPath = path.join(rootDir, ".git");
  const st = await fs.stat(gitPath).catch(() => null);
  if (!st) return null;
  if (st.isDirectory()) {
    const configPath = path.join(gitPath, "config");
    const cfg = await fs.stat(configPath).catch(() => null);
    return cfg && cfg.isFile() ? configPath : null;
  }
  if (st.isFile()) {
    const content = await fs.readFile(gitPath, "utf8").catch(() => "");
    const match = content.match(/gitdir:\s*(.+)/i);
    if (!match) return null;
    let gitDir = match[1].trim();
    if (!gitDir) return null;
    if (!path.isAbsolute(gitDir)) {
      gitDir = path.resolve(rootDir, gitDir);
    }
    const configPath = path.join(gitDir, "config");
    const cfg = await fs.stat(configPath).catch(() => null);
    if (cfg && cfg.isFile()) return configPath;

    const commonDirRaw = await fs.readFile(path.join(gitDir, "commondir"), "utf8").catch(() => "");
    const commonDirRel = commonDirRaw.trim();
    if (!commonDirRel) return null;
    let commonDir = commonDirRel;
    if (!path.isAbsolute(commonDir)) {
      commonDir = path.resolve(gitDir, commonDir);
    }
    const commonConfigPath = path.join(commonDir, "config");
    const commonCfg = await fs.stat(commonConfigPath).catch(() => null);
    return commonCfg && commonCfg.isFile() ? commonConfigPath : null;
  }
  return null;
}

async function readGitRemoteUrl(configPath) {
  const raw = await fs.readFile(configPath, "utf8").catch(() => "");
  if (!raw.trim()) return null;

  const remotes = new Map();
  let current = null;
  for (const line of raw.split(/\r?\n/)) {
    const sectionHeader = line.match(/^\s*\[[^\]]+\]\s*$/);
    if (sectionHeader) {
      const sectionMatch = line.match(/^\s*\[remote\s+"([^"]+)"\]\s*$/i);
      current = sectionMatch ? sectionMatch[1] : null;
      continue;
    }
    if (!current) continue;
    const urlMatch = line.match(/^\s*url\s*=\s*(.+)\s*$/i);
    if (urlMatch) {
      remotes.set(current, urlMatch[1].trim());
    }
  }

  if (remotes.has("origin")) return remotes.get("origin");
  const first = remotes.values().next();
  return first.done ? null : first.value;
}

function canonicalizeProjectRef(remoteUrl) {
  if (typeof remoteUrl !== "string") return null;
  let ref = remoteUrl.trim();
  if (!ref) return null;

  if (ref.startsWith("file://")) return null;
  if (path.isAbsolute(ref) || path.win32.isAbsolute(ref)) return null;

  const gitAtMatch = ref.match(/^git@([^:]+):(.+)$/i);
  if (gitAtMatch) {
    ref = `https://${gitAtMatch[1]}/${gitAtMatch[2]}`;
  } else if (ref.startsWith("ssh://")) {
    try {
      const parsed = new URL(ref);
      ref = `https://${parsed.hostname}${parsed.pathname}`;
    } catch (_e) {
      return null;
    }
  } else if (ref.startsWith("git://")) {
    ref = `https://${ref.slice("git://".length)}`;
  } else if (ref.startsWith("http://")) {
    ref = `https://${ref.slice("http://".length)}`;
  } else if (!ref.startsWith("https://")) {
    return null;
  }

  try {
    const parsed = new URL(ref);
    if (!parsed.hostname) return null;
    ref = `https://${parsed.hostname}${parsed.pathname}`;
  } catch (_e) {
    return null;
  }

  ref = ref.replace(/\.git$/i, "");
  ref = ref.replace(/\/+$/, "");
  return ref || null;
}

function sameGeminiTotals(a, b) {
  if (!a || !b) return false;
  return (
    a.input_tokens === b.input_tokens &&
    a.cached_input_tokens === b.cached_input_tokens &&
    a.cache_creation_input_tokens === b.cache_creation_input_tokens &&
    a.output_tokens === b.output_tokens &&
    a.reasoning_output_tokens === b.reasoning_output_tokens &&
    a.total_tokens === b.total_tokens
  );
}

function diffGeminiTotals(current, previous) {
  if (!current || typeof current !== "object") return null;
  if (!previous || typeof previous !== "object") return current;
  if (sameGeminiTotals(current, previous)) return null;

  const totalReset = (current.total_tokens || 0) < (previous.total_tokens || 0);
  if (totalReset) return current;

  const delta = {
    input_tokens: Math.max(0, (current.input_tokens || 0) - (previous.input_tokens || 0)),
    cached_input_tokens: Math.max(
      0,
      (current.cached_input_tokens || 0) - (previous.cached_input_tokens || 0),
    ),
    cache_creation_input_tokens: Math.max(
      0,
      (current.cache_creation_input_tokens || 0) - (previous.cache_creation_input_tokens || 0),
    ),
    output_tokens: Math.max(0, (current.output_tokens || 0) - (previous.output_tokens || 0)),
    reasoning_output_tokens: Math.max(
      0,
      (current.reasoning_output_tokens || 0) - (previous.reasoning_output_tokens || 0),
    ),
    total_tokens: Math.max(0, (current.total_tokens || 0) - (previous.total_tokens || 0)),
  };

  return isAllZeroUsage(delta) ? null : delta;
}

function isAllZeroUsage(u) {
  if (!u || typeof u !== "object") return true;
  for (const k of [
    "input_tokens",
    "cached_input_tokens",
    "cache_creation_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
  ]) {
    if (Number(u[k] || 0) !== 0) return false;
  }
  return true;
}

function toNonNegativeInt(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.floor(n);
}

function coerceEpochMs(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return 0;
  if (n < 1e12) return Math.floor(n * 1000);
  return Math.floor(n);
}

async function safeReadDir(dir) {
  try {
    return await fs.readdir(dir, { withFileTypes: true });
  } catch (_e) {
    return [];
  }
}

function parseIsoTimestampMs(value) {
  if (typeof value !== "string" || !value.trim()) return 0;
  const ms = Date.parse(value.trim());
  return Number.isFinite(ms) && ms > 0 ? ms : 0;
}

function pickWin32ProviderPath({ env = process.env, nativeValue, wslProviderDir, wslValue, deps = {} }) {
  const paths = resolveInstallPaths({ nativeValue, wslDir: wslProviderDir, wslValue }, env, deps);
  return paths.native || paths.wsl;
}

function sqliteStringLiteral(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

// UNC paths (\\wsl$\Ubuntu\..., \\wsl.localhost\..., \\server\share\...) make
// sqlite3.exe fail with "database is locked (5)" on Windows because the Plan 9
// / SMB bridge can't grant the locks SQLite asks for — even after `wsl
// --shutdown`. Detect those paths so we can snapshot the DB locally first.
function isUncPath(p) {
  return wsl.isUncPath(p);
}

function snapshotSqliteDb(dbPath) {
  return wsl.snapshotSqliteDb(dbPath);
}

// ── Dual-install cursor ownership probes ────────────────────────────────────
// When a flat (pre-namespaced) cursor migrates to per-install namespaces
// (multiInstallParse "both" mode), the migration must know which install the
// flat cursor was tracking: only then may the OTHER install's namespace start
// empty so its full history backfills. Guessing wrong wipes dedup state
// (snapshots / sessionTotals / threadTotals) for an already-counted install,
// and every re-read session lands in the hourly buckets a second time.
// These probes answer "does this install's DB contain the flat cursor's own
// ids?" — sampled from the cursor's per-session dedup maps. Any failure or
// missing evidence returns false so the caller falls back to seeding every
// namespace (bounded backfill loss, never a double count).

function sqliteDbContainsIds(dbPath, table, ids, sqliteOptions = {}) {
  if (!dbPath || !Array.isArray(ids) || ids.length === 0) return false;
  if (!fssync.existsSync(dbPath)) return false;
  const inList = ids.map(sqliteStringLiteral).join(",");
  const sql = `SELECT id FROM ${table} WHERE id IN (${inList}) LIMIT 1`;

  let snapshot = null;
  let effectiveDbPath = dbPath;
  if (isUncPath(dbPath)) {
    try {
      snapshot = snapshotSqliteDb(dbPath);
      effectiveDbPath = snapshot.path;
    } catch (_e) { }
  }
  try {
    const rows = readSqliteJsonRows(effectiveDbPath, sql, {
      label: "InstallProbe",
      maxBuffer: 1024 * 1024,
      timeout: 10_000,
      ...sqliteOptions,
    });
    return Array.isArray(rows) && rows.length > 0;
  } catch (_e) {
    return false;
  } finally {
    if (snapshot) snapshot.cleanup();
  }
}

// Sample the most recently inserted keys — most likely to still exist in the
// DB (providers may delete old sessions, which would blind the probe).
function sampleRecentKeys(obj, limit = 16) {
  if (!obj || typeof obj !== "object") return [];
  const keys = Object.keys(obj).filter((k) => typeof k === "string" && k.length > 0);
  return keys.slice(-limit);
}

// ─────────────────────────────────────────────────────────────────────────────
// oh-my-pi (omp) — passive JSONL reader (~/.omp/agent/sessions/**/*.jsonl)
//
// oh-my-pi writes one append-only JSONL per session:
//   ~/.omp/agent/sessions/--<cwd-encoded>--/<timestamp>_<sessionId>.jsonl
// Task subagents spawned by that session get their own JSONL files nested in
// a sibling directory named after the session file (see
// resolveOmpSubagentFiles); their usage counts toward the same "omp" totals.
//
// Per-line record types: the first line is type:"session" (header).
// Only type:"message" lines with message.role=="assistant" carry token usage.
// The shape (verbatim from oh-my-pi docs/session.md):
//
//   {
//     "type": "message",
//     "id": "a1b2c3d4",          ← 8-char dedup key
//     "parentId": "...",
//     "timestamp": "2026-02-16T10:21:00.000Z",
//     "message": {
//       "role": "assistant",
//       "provider": "anthropic",
//       "model": "claude-sonnet-4-5",
//       "usage": {
//         "input": 100, "output": 20, "cacheRead": 0, "cacheWrite": 0,
//         "totalTokens": 120, "reasoningTokens": 0
//       },
//       "timestamp": 1760000000000   ← ms epoch, preferred for bucketing
//     }
//   }
//
// oh-my-pi is a router — dispatches to upstream providers (Anthropic, OpenAI,
// etc.) and records the upstream model name per message. There is no global
// default model setting; model is always per-message (fallback: "omp-unknown").
// ─────────────────────────────────────────────────────────────────────────────

function resolveOmpHome(env = process.env) {
  const home = env.HOME || os.homedir();
  // Honor UsageCairn override first, then oh-my-pi upstream env vars.
  if (env.OMP_HOME) return env.OMP_HOME;
  if (env.PI_CONFIG_DIR) return path.join(home, env.PI_CONFIG_DIR);
  if (process.platform === "win32") {
    return pickWin32ProviderPath({
      env,
      nativeValue: path.join(home, ".omp"),
      wslProviderDir: ".omp",
    });
  }
  return path.join(home, ".omp");
}

// PI_CODING_AGENT_DIR is documented by both pi-coding-agent and oh-my-pi as
// their agent directory override. When set, attribute it to whichever tool the
// user actually has installed: ~/.pi present → "pi", otherwise "omp" (the
// historical default in this codebase, preserved for back-compat).
//
// Users with both tools installed can disambiguate explicitly with
// TOKEN_PULSE_PI_AGENT_DIR / TOKEN_PULSE_OMP_AGENT_DIR, which take
// precedence in their respective resolvers.
function decidePiCodingAgentDirOwner(env = process.env) {
  const home = env.HOME || os.homedir();
  // Require an actual directory — a stray file (lockfile, junk) at ~/.pi
  // shouldn't reroute an existing oh-my-pi user's PI_CODING_AGENT_DIR override.
  try {
    if (fssync.statSync(path.join(home, ".pi")).isDirectory()) return "pi";
  } catch {
    // ENOENT or EACCES — treat as "no pi install signal".
  }
  return "omp";
}

function expandHomePath(dir, env = process.env) {
  if (typeof dir !== "string" || !dir) return dir;
  if (dir !== "~" && !dir.startsWith("~/")) return dir;
  const home = env.HOME || os.homedir();
  return dir === "~" ? home : path.join(home, dir.slice(2));
}

function resolveOmpAgentDir(env = process.env) {
  if (env.TOKEN_PULSE_OMP_AGENT_DIR) {
    return expandHomePath(env.TOKEN_PULSE_OMP_AGENT_DIR, env);
  }
  if (env.PI_CODING_AGENT_DIR && decidePiCodingAgentDirOwner(env) === "omp") {
    return expandHomePath(env.PI_CODING_AGENT_DIR, env);
  }
  const ompHome = resolveOmpHome(env);
  return ompHome ? path.join(ompHome, "agent") : null;
}

const PI_STYLE_HEADER_SCAN_MAX_BYTES = 65536;

async function readPiStyleSessionCwd(filePath) {
  let stream;
  try {
    stream = fssync.createReadStream(filePath, {
      encoding: "utf8",
      start: 0,
      end: PI_STYLE_HEADER_SCAN_MAX_BYTES,
    });
  } catch {
    return null;
  }
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  try {
    for await (const line of rl) {
      if (!line || !line.includes('"session"')) continue;
      let entry;
      try { entry = JSON.parse(line); } catch { continue; }
      if (entry?.type !== "session") continue;
      return typeof entry.cwd === "string" && entry.cwd.trim() ? entry.cwd.trim() : null;
    }
  } finally {
    rl.close();
    stream.close?.();
  }
  return null;
}

async function resolveOmpFileCwd(filePath) {
  return readPiStyleSessionCwd(filePath);
}

// ─────────────────────────────────────────────────────────────────────────────
// Kilo Code VS Code extension — passive reader for VS Code-family
// globalStorage/kilocode.kilo-code/tasks/<uuid>/ui_messages.json files.
//
// Each task folder contains a ui_messages.json (JSON array, not JSONL). Token
// usage records are messages where `say == "api_req_started"`; the `text`
// field is a JSON-stringified payload:
//
//   {
//     "apiProtocol":    "openai" | "anthropic" | ...,
//     "tokensIn":       28673,    // request input (already excludes cache)
//     "tokensOut":      31,       // completion
//     "cacheWrites":    0,
//     "cacheReads":     5120,
//     "cost":           0,
//     "usageMissing":   false,
//     "inferenceProvider": "Moonshot AI" | "minimax" | ...,
//   }
//
// We scan every supported VS Code-family install (Cursor, Code, CodeBuddy,
// Windsurf, …) under both Library/Application Support (macOS) and Linux/Win
// equivalents. Files are small (median ~30KB) and rewritten on each turn — we
// can't byte-tail them, so we read the whole file on every sync and dedupe by
// (taskId, ts). Per-file mtime caching skips unchanged files.
// ─────────────────────────────────────────────────────────────────────────────

function resolveKilocodeRoots(env = process.env) {
  if (typeof env.TOKEN_PULSE_KILOCODE_ROOTS === "string" && env.TOKEN_PULSE_KILOCODE_ROOTS.trim()) {
    // Use the host's PATH delimiter so a Windows drive colon stays intact.
    return env.TOKEN_PULSE_KILOCODE_ROOTS.split(path.delimiter)
      .map((r) => r.trim())
      .filter(Boolean);
  }
  const home = env.HOME || os.homedir();
  const candidates = [];
  if (process.platform === "darwin") {
    const base = path.join(home, "Library", "Application Support");
    candidates.push(
      path.join(base, "Code"),
      path.join(base, "Code - Insiders"),
      path.join(base, "Cursor"),
      path.join(base, "CodeBuddy"),
      path.join(base, "Windsurf"),
      path.join(base, "VSCodium"),
      path.join(base, "Trae"),
      path.join(base, "Trae CN"),
    );
  } else if (process.platform === "win32") {
    const appData = env.APPDATA || path.join(home, "AppData", "Roaming");
    const nativeRoots = [
      path.join(appData, "Code"),
      path.join(appData, "Code - Insiders"),
      path.join(appData, "Cursor"),
      path.join(appData, "CodeBuddy"),
      path.join(appData, "Windsurf"),
      path.join(appData, "VSCodium"),
    ];
    const wslRoots = [];
    if (wsl.shouldProbeWsl(env)) {
      for (const ide of ["Code", "Code - Insiders", "Cursor", "CodeBuddy", "Windsurf", "VSCodium"]) {
        const wslDir = wsl.discoverWslHome(`.config/${ide}`, { env });
        if (wslDir) wslRoots.push(wslDir);
      }
    }
    const nativeCandidates = wsl.shouldProbeNative(env) ? nativeRoots : [];
    const mode = wsl.getWslMode(env);
    if (mode === "native-first" || mode === "native-only") {
      candidates.push(...nativeCandidates, ...wslRoots);
    } else {
      candidates.push(...wslRoots, ...nativeCandidates);
    }
  } else {
    const xdg = env.XDG_CONFIG_HOME || path.join(home, ".config");
    candidates.push(
      path.join(xdg, "Code"),
      path.join(xdg, "Code - Insiders"),
      path.join(xdg, "Cursor"),
      path.join(xdg, "CodeBuddy"),
      path.join(xdg, "Windsurf"),
      path.join(xdg, "VSCodium"),
    );
  }
  return candidates;
}

function normalizeLocalStudioTokens(usage) {
  if (!usage || typeof usage !== "object") return null;
  const prompt = toNonNegativeInt(
    usage.prompt_tokens ?? usage.promptTokens ?? usage.input_tokens ?? usage.inputTokens,
  );
  const completion = toNonNegativeInt(
    usage.completion_tokens ?? usage.completionTokens ?? usage.output_tokens ?? usage.outputTokens,
  );
  const total = Math.max(
    toNonNegativeInt(usage.total_tokens ?? usage.totalTokens),
    prompt + completion,
  );
  if (total <= 0) return null;
  const promptDetails = usage.prompt_tokens_details
    || usage.input_tokens_details
    || usage.inputTokensDetails
    || {};
  const outputDetails = usage.completion_tokens_details
    || usage.output_tokens_details
    || usage.completionTokensDetails
    || usage.outputTokensDetails
    || {};
  const cacheRead = Math.min(
    prompt,
    Math.max(
      toNonNegativeInt(promptDetails.cached_tokens ?? promptDetails.cache_read_tokens),
      toNonNegativeInt(usage.cached_tokens),
    ),
  );
  const cacheWrite = Math.min(
    Math.max(0, prompt - cacheRead),
    Math.max(
      toNonNegativeInt(
        promptDetails.cache_creation_input_tokens ?? promptDetails.cache_write_tokens,
      ),
      toNonNegativeInt(usage.cache_creation_input_tokens),
    ),
  );
  const reasoning = Math.min(
    completion,
    Math.max(
      toNonNegativeInt(outputDetails.reasoning_tokens),
      toNonNegativeInt(usage.reasoning_tokens),
    ),
  );
  return {
    input_tokens: Math.max(0, total - completion - cacheRead - cacheWrite),
    cached_input_tokens: cacheRead,
    cache_creation_input_tokens: cacheWrite,
    output_tokens: Math.max(0, completion - reasoning),
    reasoning_output_tokens: reasoning,
    total_tokens: total,
    billable_total_tokens: total,
    total_cost_usd: 0,
    conversation_count: 1,
  };
}

function reconcilePassiveUsageEvent({ event, source, messages, hourlyState, touchedBuckets }) {
  const previous = messages[event.key];
  const unchanged = previous
    && previous.model === event.model
    && previous.bucketStart === event.bucketStart
    && totalsKey(previous.totals) === totalsKey(event.totals);
  if (unchanged) return false;

  if (previous?.model && previous?.bucketStart && previous?.totals) {
    const oldBucket = getHourlyBucket(hourlyState, source, previous.model, previous.bucketStart);
    subtractTotals(oldBucket.totals, previous.totals);
    touchedBuckets.add(bucketKey(source, previous.model, previous.bucketStart));
  }
  const bucket = getHourlyBucket(hourlyState, source, event.model, event.bucketStart);
  addTotals(bucket.totals, event.totals);
  touchedBuckets.add(bucketKey(source, event.model, event.bucketStart));
  messages[event.key] = {
    model: event.model,
    bucketStart: event.bucketStart,
    totals: event.totals,
    updatedAt: new Date().toISOString(),
  };
  return true;
}

function devinSqliteFingerprint(dbPath) {
  const fingerprint = sqliteSidecarFingerprint(dbPath);
  // -shm is reader bookkeeping: opening a WAL database can bump it without any
  // content change, so it must not force a rescan (same convention as Unsloth).
  delete fingerprint["-shm"];
  return fingerprint;
}

// Stage only the bucket objects this parser can mutate. The shared
// normalizers copy the bucket maps but alias each bucket/totals object, and
// the enqueue helpers stamp queuedKey before appendFile runs — so a failed
// append used to leave the caller's published state polluted. Only entries
// the calling parser owns get private copies; every other provider's buckets
// stay shared read-only references.
function stageProviderBuckets(buckets, isOwnedBucket) {
  const staged = {};
  for (const [key, bucket] of Object.entries(buckets || {})) {
    staged[key] =
      bucket && typeof bucket === "object" && isOwnedBucket(key, bucket)
        ? {
            ...bucket,
            totals:
              bucket.totals && typeof bucket.totals === "object"
                ? { ...bucket.totals }
                : bucket.totals,
          }
        : bucket;
  }
  return staged;
}

// Tool results can be hundreds of MB, exceeding V8's ~2^29-character string
// limit without carrying usage. Skip them without decoding; assistant usage
// records are orders of magnitude smaller.
const OMP_LIKE_MAX_RECORD_BYTES = 64 * 1024 * 1024;

async function scanOmpLikeTranscript(filePath, startOffset, maxRecordBytes, onEntry) {
  let stream;
  try {
    stream = fssync.createReadStream(filePath, { start: startOffset });
  } catch {
    return startOffset;
  }
  let scannedEndOffset = startOffset;
  let committedEndOffset = startOffset;
  for await (const record of physicalJsonlRecords(stream, {
    invalidUtf8: "record",
    maxRecordBytes,
  })) {
    scannedEndOffset += record.physicalBytes;
    if (record.terminated) {
      committedEndOffset = scannedEndOffset;
      if (record.oversized || !record.utf8Valid || !record.line || !record.line.trim()) continue;
      let entry;
      try { entry = JSON.parse(record.line); } catch { continue; }
      onEntry(entry);
      continue;
    }
    // A final incomplete record may still be in the process of being written.
    if (record.oversized || !record.utf8Valid || !record.line || !record.line.trim()) break;
    let entry;
    try { entry = JSON.parse(record.line); } catch { break; }
    onEntry(entry);
    committedEndOffset = scannedEndOffset;
    break;
  }
  return committedEndOffset;
}

// Maps one parsed JSONL line to { id, message, timestamp } or null. pi-family
// files wrap each record as { type: "message", id, timestamp, message }.
function readPiMessageEntry(entry) {
  if (!entry || entry.type !== "message") return null;
  return {
    id: typeof entry.id === "string" && entry.id ? entry.id : null,
    message: entry.message,
    timestamp: entry.timestamp,
  };
}

async function parsePiLikeIncremental({
  sessionFiles,
  cursors,
  queuePath,
  projectQueuePath,
  publicRepoResolver,
  onProgress,
  env,
  defaultModel,
  stateKey,
  resolveSessionFiles,
  resolveDefaultModel,
  sourceForProvider,
  readEntry = readPiMessageEntry,
} = {}) {
  await ensureDir(path.dirname(queuePath));
  const projectEnabled = typeof projectQueuePath === "string" && projectQueuePath.length > 0;
  const providerState = cursors[stateKey] && typeof cursors[stateKey] === "object"
    ? cursors[stateKey]
    : {};
  const seenIds = new Set(Array.isArray(providerState.seenIds) ? providerState.seenIds : []);
  const fileOffsets =
    providerState.fileOffsets && typeof providerState.fileOffsets === "object"
      ? { ...providerState.fileOffsets }
      : {};

  const projectSeenIds = new Set(Array.isArray(providerState.projectSeenIds) ? providerState.projectSeenIds : []);
  const projectFileOffsets = { ...(providerState.projectFileOffsets || {}) };

  const files = Array.isArray(sessionFiles)
    ? sessionFiles
    : resolveSessionFiles(env || process.env);
  const fallbackModel = defaultModel || resolveDefaultModel();

  if (files.length === 0) {
    cursors[stateKey] = {
      ...providerState,
      seenIds: Array.from(seenIds),
      fileOffsets,
      updatedAt: new Date().toISOString(),
    };
    return { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0, projectBucketsQueued: 0 };
  }

  const hourlyState = normalizeHourlyState(cursors?.hourly);
  const touchedBuckets = new Set();
  const projectState = projectEnabled ? normalizeProjectState(cursors?.projectHourly) : null;
  const projectTouchedBuckets = projectEnabled ? new Set() : null;
  const projectMetaCache = projectEnabled ? new Map() : null;
  const publicRepoCache = projectEnabled ? new Map() : null;
  // Both queues must publish before cursor progress is acknowledged. Normalized
  // bucket maps still alias their values, so stage this provider's buckets to
  // keep a failed project append from mutating the caller's aggregate state.
  const family = sourceForProvider(null);
  const ownsSource = (source) => source === family || source.startsWith(`${family}-`);
  hourlyState.groupQueued = { ...(hourlyState.groupQueued || {}) };
  for (const [key, bucket] of Object.entries(hourlyState.buckets)) {
    if (ownsSource(parseBucketKey(key).source || "")) {
      hourlyState.buckets[key] = { ...bucket, totals: { ...bucket.totals } };
    }
  }
  if (projectState) {
    for (const [key, bucket] of Object.entries(projectState.buckets)) {
      if (ownsSource(bucket.source || "")) {
        projectState.buckets[key] = { ...bucket, totals: { ...bucket.totals } };
      }
    }
  }
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
    let streamedBytes = 0;
    let lastCompleteOffset = startOffset;
    try {
      stream = fssync.createReadStream(filePath, {
        encoding: "utf8",
        start: startOffset,
      });
    } catch { continue; }
    // readline emits an unterminated final fragment as if it were a line. If
    // the producer is still writing that JSON object, parsing fails; advancing
    // to stat.size here would then lose the completed record forever. Track the
    // last physical newline and commit only through that byte boundary.
    stream.on("data", (chunk) => {
      let searchFrom = 0;
      let newlineIndex;
      while ((newlineIndex = chunk.indexOf("\n", searchFrom)) !== -1) {
        lastCompleteOffset = startOffset
          + streamedBytes
          + Buffer.byteLength(chunk.slice(0, newlineIndex + 1), "utf8");
        searchFrom = newlineIndex + 1;
      }
      streamedBytes += Buffer.byteLength(chunk, "utf8");
    });
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

    for await (const line of rl) {
      if (!line || !line.trim()) continue;
      let entry;
      try { entry = JSON.parse(line); } catch { continue; }

      const record = readEntry(entry);
      if (!record) continue;

      const msg = record.message;
      if (!msg || msg.role !== "assistant") continue;

      const usage = msg.usage;
      if (!usage || typeof usage !== "object") continue;

      const entryId = record.id;
      if (!entryId) continue;
      if (seenIds.has(entryId)) continue;

      recordsProcessed++;

      const input = toNonNegativeInt(usage.input);
      const output = toNonNegativeInt(usage.output);
      const cacheRead = toNonNegativeInt(usage.cacheRead);
      const cacheWrite = toNonNegativeInt(usage.cacheWrite);
      const reasoningTokens = toNonNegativeInt(usage.reasoningTokens);

      if (
        input === 0 &&
        output === 0 &&
        cacheRead === 0 &&
        cacheWrite === 0 &&
        reasoningTokens === 0
      ) {
        seenIds.add(entryId);
        continue;
      }

      let tsMs = null;
      if (Number.isFinite(Number(msg.timestamp)) && Number(msg.timestamp) > 0) {
        tsMs = Number(msg.timestamp);
      } else if (typeof record.timestamp === "string" && record.timestamp) {
        const parsed = Date.parse(record.timestamp);
        if (Number.isFinite(parsed) && parsed > 0) tsMs = parsed;
      }
      if (tsMs == null) {
        seenIds.add(entryId);
        continue;
      }

      const tsIso = new Date(tsMs).toISOString();
      const bucketStart = toUtcHalfHourStart(tsIso);
      if (!bucketStart) continue;

      const totalTokens =
        Number.isFinite(Number(usage.totalTokens)) && Number(usage.totalTokens) > 0
          ? toNonNegativeInt(usage.totalTokens)
          : input + output + cacheRead + cacheWrite + reasoningTokens;

      const model = normalizeModelInput(msg.model) || fallbackModel;
      const source = sourceForProvider(msg.provider);

      const delta = {
        input_tokens: input,
        cached_input_tokens: cacheRead,
        cache_creation_input_tokens: cacheWrite,
        output_tokens: output,
        reasoning_output_tokens: reasoningTokens,
        total_tokens: totalTokens,
        conversation_count: 1,
      };

      const bucket = getHourlyBucket(hourlyState, source, model, bucketStart);
      addTotals(bucket.totals, delta);
      touchedBuckets.add(bucketKey(source, model, bucketStart));
      seenIds.add(entryId);
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
      size: Math.min(lastCompleteOffset, postStat.size),
      mtimeMs: postStat.mtimeMs,
      ino: postStat.ino,
    };
  }

  // Project attribution has an independent cursor so upgrading an existing
  // installation can backfill already-consumed Pi sessions without adding
  // those messages to the total-usage buckets a second time. Current Pi
  // session headers persist the real cwd; unlike the encoded session folder,
  // it is lossless even when path components contain dashes.
  if (projectEnabled) {
    for (const filePath of files) {
      let stat;
      try { stat = fssync.statSync(filePath); } catch { continue; }

      const prevEntry = projectFileOffsets[filePath] || {};
      const prevSize = Number(prevEntry.size) || 0;
      const prevIno = prevEntry.ino;
      const inodeChanged = typeof prevIno === "number" && prevIno !== stat.ino;
      const startOffset = stat.size < prevSize || inodeChanged ? 0 : prevSize;
      if (stat.size <= startOffset) continue;

      const cwd = await resolveOmpFileCwd(filePath);
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

      let lastCompleteOffset = startOffset;
      if (projectKey && projectRef) {
        let stream;
        try {
          stream = fssync.createReadStream(filePath, {
            encoding: "utf8",
            start: startOffset,
          });
        } catch {
          continue;
        }
        let streamedBytes = 0;
        stream.on("data", (chunk) => {
          const newlineIndex = chunk.lastIndexOf("\n");
          if (newlineIndex !== -1) {
            lastCompleteOffset = startOffset + streamedBytes
              + Buffer.byteLength(chunk.slice(0, newlineIndex + 1), "utf8");
          }
          streamedBytes += Buffer.byteLength(chunk, "utf8");
        });
        const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
        for await (const line of rl) {
          if (!line || !line.trim()) continue;
          let entry;
          try { entry = JSON.parse(line); } catch { continue; }
          const record = readEntry(entry);
          const msg = record ? record.message : null;
          const usage = msg?.role === "assistant" ? msg.usage : null;
          if (!usage || typeof usage !== "object") continue;

          const entryId = record.id;
          if (!entryId || projectSeenIds.has(entryId)) continue;

          const input = toNonNegativeInt(usage.input);
          const output = toNonNegativeInt(usage.output);
          const cacheRead = toNonNegativeInt(usage.cacheRead);
          const cacheWrite = toNonNegativeInt(usage.cacheWrite);
          const reasoningTokens = toNonNegativeInt(usage.reasoningTokens);
          if (
            input === 0 &&
            output === 0 &&
            cacheRead === 0 &&
            cacheWrite === 0 &&
            reasoningTokens === 0
          ) {
            projectSeenIds.add(entryId);
            continue;
          }

          let tsMs = null;
          if (Number.isFinite(Number(msg.timestamp)) && Number(msg.timestamp) > 0) {
            tsMs = Number(msg.timestamp);
          } else if (typeof record.timestamp === "string" && record.timestamp) {
            const parsed = Date.parse(record.timestamp);
            if (Number.isFinite(parsed) && parsed > 0) tsMs = parsed;
          }
          const bucketStart = tsMs == null
            ? null
            : toUtcHalfHourStart(new Date(tsMs).toISOString());
          if (!bucketStart) {
            projectSeenIds.add(entryId);
            continue;
          }

          const totalTokens =
            Number.isFinite(Number(usage.totalTokens)) && Number(usage.totalTokens) > 0
              ? toNonNegativeInt(usage.totalTokens)
              : input + output + cacheRead + cacheWrite + reasoningTokens;
          const delta = {
            input_tokens: input,
            cached_input_tokens: cacheRead,
            cache_creation_input_tokens: cacheWrite,
            output_tokens: output,
            reasoning_output_tokens: reasoningTokens,
            total_tokens: totalTokens,
            conversation_count: 1,
          };
          const projectBucket = getProjectBucket(
            projectState,
            projectKey,
            sourceForProvider(msg.provider),
            bucketStart,
            projectRef,
          );
          addTotals(projectBucket.totals, delta);
          projectTouchedBuckets.add(projectBucketKey(projectKey, sourceForProvider(msg.provider), bucketStart));
          projectSeenIds.add(entryId);
        }
      }

      let postStat = stat;
      try { postStat = fssync.statSync(filePath); } catch {}
      projectFileOffsets[filePath] = {
        size: Math.min(lastCompleteOffset, postStat.size),
        mtimeMs: postStat.mtimeMs,
        ino: postStat.ino,
      };
    }
  }

  const seenArr = Array.from(seenIds);
  const cappedSeen =
    seenArr.length > 10_000 ? seenArr.slice(seenArr.length - 10_000) : seenArr;

  const bucketsQueued = await enqueueTouchedBuckets({
    queuePath,
    hourlyState,
    touchedBuckets,
  });
  const projectBucketsQueued = projectEnabled
    ? await enqueueTouchedProjectBuckets({ projectQueuePath, projectState, projectTouchedBuckets })
    : 0;
  const updatedAt = new Date().toISOString();
  hourlyState.updatedAt = updatedAt;
  cursors.hourly = hourlyState;
  if (projectState) {
    projectState.updatedAt = updatedAt;
    cursors.projectHourly = projectState;
  }
  cursors[stateKey] = {
    ...providerState,
    seenIds: cappedSeen,
    fileOffsets,
    ...(projectEnabled ? {
      projectSeenIds: Array.from(projectSeenIds).slice(-10_000),
      projectFileOffsets,
    } : {}),
    updatedAt,
  };

  return { recordsProcessed, eventsAggregated, bucketsQueued, projectBucketsQueued };
}

function sqliteSidecarFingerprint(dbPath) {
  const out = {};
  for (const suffix of ["", "-wal", "-shm", "-journal"]) {
    const filePath = `${dbPath}${suffix}`;
    try {
      const stat = fssync.statSync(filePath);
      out[suffix || "db"] = {
        size: stat.size,
        mtimeMs: stat.mtimeMs,
        dev: stat.dev,
        ino: stat.ino,
      };
    } catch (e) {
      if (!e || e.code !== "ENOENT") throw e;
    }
  }
  return out;
}

function sameSqliteFingerprint(a, b) {
  return JSON.stringify(a || {}) === JSON.stringify(b || {});
}

function mergeGrokUsagePrecision(current, next) {
  if (!current) return next;
  if (!next || current === next) return current;
  return "mixed";
}

function clearSourceHourlyBuckets(hourlyState, source) {
  if (!hourlyState || typeof hourlyState !== "object") return;
  const prefix = `${source}${BUCKET_SEPARATOR}`;
  const buckets = hourlyState.buckets && typeof hourlyState.buckets === "object" ? hourlyState.buckets : null;
  if (buckets) {
    for (const key of Object.keys(buckets)) {
      if (key.startsWith(prefix)) delete buckets[key];
    }
  }
  const groupQueued =
    hourlyState.groupQueued && typeof hourlyState.groupQueued === "object"
      ? hourlyState.groupQueued
      : null;
  if (groupQueued) {
    for (const key of Object.keys(groupQueued)) {
      if (key.startsWith(prefix)) delete groupQueued[key];
    }
  }
}

async function retractStaleSourceQueueRows(queuePath, source, keepKeys) {
  if (!queuePath) return 0;
  let raw = "";
  try {
    raw = fssync.readFileSync(queuePath, "utf8");
  } catch (error) {
    if (error?.code === "ENOENT") return 0;
    throw error;
  }

  const latestRows = new Map();
  for (const line of raw.split("\n")) {
    if (!line.trim()) continue;
    let row;
    try {
      row = JSON.parse(line);
    } catch {
      continue;
    }
    if ((row?.source || "") !== source) continue;
    const model = normalizeModelInput(row.model) || DEFAULT_MODEL;
    const hourStart = typeof row.hour_start === "string" ? row.hour_start : null;
    if (!hourStart) continue;
    latestRows.set(bucketKey(source, model, hourStart), { model, hour_start: hourStart, row });
  }

  const zero = initTotals();
  const lines = [];
  for (const [key, entry] of latestRows.entries()) {
    if (keepKeys.has(key)) continue;
    if (totalsKey(entry.row) === totalsKey(zero)) continue;
    lines.push(
      JSON.stringify({
        source,
        model: entry.model,
        hour_start: entry.hour_start,
        ...zero,
      }),
    );
  }
  if (lines.length === 0) return 0;
  await fs.appendFile(queuePath, `${lines.join("\n")}\n`, "utf8");
  return lines.length;
}
const DSH_SESSION_TEXT_MAX_BYTES = 128 * 1024 * 1024;

// Inspect concatenated Zstandard frames without decompressing them. Harness
// writes a content size into every independent append frame; summing those
// declarations lets us reject a decompression bomb before the native decoder
// allocates its plaintext buffer.
function inspectDshZstdFrames(data, maxOutputBytes = DSH_SESSION_TEXT_MAX_BYTES) {
  const input = Buffer.isBuffer(data) ? data : Buffer.from(data || []);
  const limit = Number.isSafeInteger(maxOutputBytes) && maxOutputBytes >= 0
    ? maxOutputBytes
    : DSH_SESSION_TEXT_MAX_BYTES;
  let offset = 0;
  let totalContentBytes = 0;
  let allContentSizesDeclared = true;
  let frames = 0;
  const frameRanges = [];

  const need = (count, label) => {
    if (offset + count > input.length) throw new Error(`Invalid DeepSeek Harness zstd ${label}`);
  };
  const readUnsignedLE = (count) => {
    need(count, "frame header");
    let value = 0n;
    for (let i = 0; i < count; i++) value |= BigInt(input[offset + i]) << BigInt(i * 8);
    offset += count;
    return value;
  };

  while (offset < input.length) {
    const frameStart = offset;
    need(4, "magic");
    const magic = input.readUInt32LE(offset);
    offset += 4;

    if (magic >= 0x184d2a50 && magic <= 0x184d2a5f) {
      need(4, "skippable frame size");
      const skipBytes = input.readUInt32LE(offset);
      offset += 4;
      need(skipBytes, "skippable frame payload");
      offset += skipBytes;
      continue;
    }
    if (magic !== 0xfd2fb528) throw new Error("Invalid DeepSeek Harness zstd frame magic");

    need(1, "descriptor");
    const descriptor = input[offset++];
    if ((descriptor & 0x08) !== 0) throw new Error("Invalid DeepSeek Harness zstd reserved frame bit");
    const contentSizeFlag = descriptor >>> 6;
    const singleSegment = (descriptor & 0x20) !== 0;
    const hasChecksum = (descriptor & 0x04) !== 0;
    const dictionaryIdBytes = [0, 1, 2, 4][descriptor & 0x03];
    let windowSize = null;
    if (!singleSegment) {
      need(1, "window descriptor");
      const windowDescriptor = input[offset++];
      const windowBase = 2 ** (10 + (windowDescriptor >>> 3));
      windowSize = windowBase + (windowBase / 8) * (windowDescriptor & 0x07);
    }
    need(dictionaryIdBytes, "dictionary id");
    offset += dictionaryIdBytes;

    const contentSizeBytes = contentSizeFlag === 0
      ? (singleSegment ? 1 : 0)
      : [0, 2, 4, 8][contentSizeFlag];
    let declaredContentBytes = null;
    if (contentSizeBytes > 0) {
      declaredContentBytes = readUnsignedLE(contentSizeBytes);
      if (contentSizeBytes === 2) declaredContentBytes += 256n;
      if (declaredContentBytes > BigInt(limit)) {
        throw new Error(`DeepSeek Harness decompressed session log exceeds ${limit} bytes`);
      }
      if (singleSegment) windowSize = Number(declaredContentBytes);
    } else {
      allContentSizesDeclared = false;
    }

    let lastBlock = false;
    let frameUpperBound = 0;
    while (!lastBlock) {
      need(3, "block header");
      const blockHeader = input[offset] | (input[offset + 1] << 8) | (input[offset + 2] << 16);
      offset += 3;
      lastBlock = (blockHeader & 1) !== 0;
      const blockType = (blockHeader >>> 1) & 0x03;
      const blockSize = blockHeader >>> 3;
      if (blockType === 3) throw new Error("Invalid DeepSeek Harness zstd reserved block type");
      const payloadBytes = blockType === 1 ? 1 : blockSize;
      need(payloadBytes, "block payload");
      offset += payloadBytes;
      // A raw block emits blockSize bytes; an RLE block expands to blockSize
      // repeated bytes; a compressed block cannot emit more than the frame's
      // window or Zstandard's 128 KiB maximum block size. This gives frames
      // without FCS a safe allocation bound before native decompression.
      frameUpperBound += blockType === 2
        ? Math.min(Number(windowSize) || 128 * 1024, 128 * 1024)
        : blockSize;
      if (declaredContentBytes == null && frameUpperBound > limit) {
        throw new Error(`DeepSeek Harness decompressed session log exceeds ${limit} bytes`);
      }
    }
    if (hasChecksum) {
      need(4, "checksum");
      offset += 4;
    }
    const boundedFrameBytes = declaredContentBytes == null
      ? frameUpperBound
      : Number(declaredContentBytes);
    totalContentBytes += boundedFrameBytes;
    if (allContentSizesDeclared && totalContentBytes > limit) {
      throw new Error(`DeepSeek Harness decompressed session log exceeds ${limit} bytes`);
    }
    frameRanges.push({ start: frameStart, end: offset, maxContentBytes: boundedFrameBytes });
    frames += 1;
  }

  if (frames === 0) throw new Error("DeepSeek Harness zstd log contains no data frames");
  return {
    frames,
    totalContentBytes: allContentSizesDeclared ? totalContentBytes : null,
    maxTotalContentBytes: totalContentBytes,
    frameRanges,
  };
}

// Decode one already-isolated frame. Prefer Node's built-in zstd (22.15+):
// the desktop bundles install dependencies with --ignore-scripts, so
// @mongodb-js/zstd ships without its native binding there and the require()
// would throw — silently skipping every compressed session (issue #465). The
// MongoDB binding stays as the fallback for Node 20 CLI installs, where npm
// runs install scripts normally.
async function decodeDshZstdFrame(frameBytes) {
  const zlib = require("node:zlib");
  if (typeof zlib.zstdDecompressSync === "function") {
    return zlib.zstdDecompressSync(frameBytes);
  }
  return Buffer.from(await require("@mongodb-js/zstd").decompress(frameBytes));
}

// Decode one independent Harness append frame at a time; per-frame decoding
// lets us enforce the aggregate plaintext limit when the frame omits its
// content size.
async function decodeDshZstd(
  data,
  { maxOutputBytes = DSH_SESSION_TEXT_MAX_BYTES, inspected = null } = {},
) {
  const input = Buffer.isBuffer(data) ? data : Buffer.from(data || []);
  const frameInfo = inspected || inspectDshZstdFrames(input, maxOutputBytes);
  const parts = [];
  let total = 0;
  for (const frame of frameInfo.frameRanges) {
    const decoded = await decodeDshZstdFrame(input.subarray(frame.start, frame.end));
    total += decoded.length;
    if (total > maxOutputBytes) {
      throw new Error(`DeepSeek Harness decompressed session log exceeds ${maxOutputBytes} bytes`);
    }
    parts.push(decoded);
  }
  return Buffer.concat(parts, total);
}

// Return one top-level JSON object property's raw value slice without parsing
// sibling values. In particular this lets the Harness reader skip message
// content byte-for-byte and materialize only routing metadata plus token
// counters. It is intentionally small and JSON-syntax aware (strings, escapes,
// nested arrays/objects), not a regex over potentially nested content.
function findDshJsonProperty(raw, wantedKey) {
  const text = String(raw || "");
  const skipWhitespace = (index) => {
    while (index < text.length && /\s/.test(text[index])) index += 1;
    return index;
  };
  const stringEnd = (start) => {
    if (text[start] !== '"') return -1;
    for (let index = start + 1; index < text.length; index++) {
      if (text[index] === "\\") {
        index += 1;
      } else if (text[index] === '"') {
        return index + 1;
      }
    }
    return -1;
  };
  const valueEnd = (start) => {
    const first = text[start];
    if (first === '"') return stringEnd(start);
    if (first === "{" || first === "[") {
      const stack = [first === "{" ? "}" : "]"];
      let inString = false;
      for (let index = start + 1; index < text.length; index++) {
        const char = text[index];
        if (inString) {
          if (char === "\\") index += 1;
          else if (char === '"') inString = false;
          continue;
        }
        if (char === '"') {
          inString = true;
        } else if (char === "{") {
          stack.push("}");
        } else if (char === "[") {
          stack.push("]");
        } else if (char === stack.at(-1)) {
          stack.pop();
          if (stack.length === 0) return index + 1;
        }
      }
      return -1;
    }
    let index = start;
    while (index < text.length && !/[\s,}\]]/.test(text[index])) index += 1;
    return index > start ? index : -1;
  };

  let index = skipWhitespace(0);
  if (text[index] !== "{") return null;
  index += 1;
  while (index < text.length) {
    index = skipWhitespace(index);
    if (text[index] === "}") return null;
    const keyStart = index;
    const keyEnd = stringEnd(keyStart);
    if (keyEnd < 0) return null;
    let key;
    try {
      key = JSON.parse(text.slice(keyStart, keyEnd));
    } catch {
      return null;
    }
    index = skipWhitespace(keyEnd);
    if (text[index] !== ":") return null;
    index = skipWhitespace(index + 1);
    const start = index;
    const end = valueEnd(start);
    if (end < 0) return null;
    if (key === wantedKey) return text.slice(start, end);
    index = skipWhitespace(end);
    if (text[index] === ",") {
      index += 1;
      continue;
    }
    if (text[index] === "}") return null;
    return null;
  }
  return null;
}

function parseDshJsonString(raw) {
  if (typeof raw !== "string" || raw[0] !== '"') return null;
  try {
    const value = JSON.parse(raw);
    return typeof value === "string" ? value : null;
  } catch {
    return null;
  }
}

module.exports = {
  DEFAULT_SOURCE,
  DEFAULT_MODEL,
  BUCKET_SEPARATOR,
  CLAUDE_MEM_OBSERVER_PROJECT_REF,
  enqueueTouchedBuckets,
  enqueueTouchedProjectBuckets,
  cloneTotals,
  normalizeHourlyState,
  normalizeProjectState,
  normalizeQoderState,
  normalizeMessageKeyPart,
  getHourlyBucket,
  getProjectBucket,
  initTotals,
  PRIORITY_SUBSET_FIELDS,
  addTotals,
  subtractTotals,
  totalsKey,
  toUtcHalfHourStart,
  bucketKey,
  projectBucketKey,
  groupBucketKey,
  parseBucketKey,
  normalizeSourceInput,
  normalizeModelInput,
  buildProjectFileContext,
  isProjectFileContextFresh,
  deriveProjectKeyFromRef,
  resolveProjectContextForFile,
  resolveProjectContextForPath,
  canonicalizeProjectRef,
  sameGeminiTotals,
  diffGeminiTotals,
  isAllZeroUsage,
  toNonNegativeInt,
  coerceEpochMs,
  safeReadDir,
  parseIsoTimestampMs,
  pickWin32ProviderPath,
  sqliteStringLiteral,
  isUncPath,
  snapshotSqliteDb,
  sqliteDbContainsIds,
  sampleRecentKeys,
  resolveOmpHome,
  decidePiCodingAgentDirOwner,
  expandHomePath,
  resolveOmpAgentDir,
  readPiStyleSessionCwd,
  resolveOmpFileCwd,
  resolveKilocodeRoots,
  normalizeLocalStudioTokens,
  reconcilePassiveUsageEvent,
  devinSqliteFingerprint,
  stageProviderBuckets,
  OMP_LIKE_MAX_RECORD_BYTES,
  scanOmpLikeTranscript,
  parsePiLikeIncremental,
  sqliteSidecarFingerprint,
  sameSqliteFingerprint,
  mergeGrokUsagePrecision,
  clearSourceHourlyBuckets,
  retractStaleSourceQueueRows,
  DSH_SESSION_TEXT_MAX_BYTES,
  inspectDshZstdFrames,
  decodeDshZstd,
  findDshJsonProperty,
  parseDshJsonString,
};
