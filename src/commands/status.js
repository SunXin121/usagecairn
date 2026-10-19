"use strict";

const os = require("node:os");
const path = require("node:path");
const fs = require("node:fs/promises");
const fssync = require("node:fs");
const pkg = require("../../package.json");
const { readJson } = require("../lib/fs");
const { resolveTrackerPaths } = require("../lib/pulse-paths");
const { resolveUploadConfig } = require("../lib/upload-config");
const { uploadedOffset } = require("./upload");
const { formatBytes } = require("../lib/progress");
const {
  resolveAnythingllmDbPath,
  resolveUnslothDbPath,
  resolveLmstudioHome,
  resolveLmstudioLogFiles,
} = require("../lib/rollout");

function parseArgs(argv) {
  const out = { json: false, light: false };
  for (const arg of argv) {
    if (arg === "--json") out.json = true;
    else if (arg === "--light") out.light = true;
    else throw new Error(`Unknown option: ${arg}`);
  }
  return out;
}

async function fileSize(file) {
  try {
    const stat = await fs.stat(file);
    return stat.isFile() ? stat.size : 0;
  } catch {
    return 0;
  }
}

async function fileMtimeIso(file) {
  try {
    return (await fs.stat(file)).mtime.toISOString();
  } catch {
    return null;
  }
}

/**
 * Collection-side status: what the local machine has parsed, queued and where
 * the queue is uploaded. Provider quota/billing surfaces are intentionally not
 * part of this build.
 */
async function collectProviders(env = process.env) {
  const anythingllmDb = resolveAnythingllmDbPath(env);
  const unslothDb = resolveUnslothDbPath(env);
  const lmstudioLogs = await resolveLmstudioLogFiles(env);
  return {
    anythingllm: { installed: fssync.existsSync(anythingllmDb), detail: anythingllmDb },
    unsloth: { installed: fssync.existsSync(unslothDb), detail: unslothDb },
    lmstudio: {
      installed: lmstudioLogs.length > 0,
      files: lmstudioLogs.length,
      detail: path.join(resolveLmstudioHome(env), "server-logs"),
    },
  };
}
async function collectStatus({ home = os.homedir(), env = process.env } = {}) {
  const { trackerDir } = await resolveTrackerPaths({ home });
  const queuePath = path.join(trackerDir, "queue.jsonl");
  const config = (await readJson(path.join(trackerDir, "config.json"))) || {};
  const uploadOffset = await uploadedOffset(queuePath, env);
  const cursors = (await readJson(path.join(trackerDir, "cursors.json"))) || {};
  const upload = resolveUploadConfig({ queuePath, env });

  const queueBytes = await fileSize(queuePath);
  return {
    version: pkg.version,
    generated_at: new Date().toISOString(),
    tracker_dir: trackerDir,
    installed_at: config.installedAt || null,
    upload: {
      configured: Boolean(upload.apiUrl && upload.accessKey),
      api_url: upload.apiUrl || null,
      source: upload.source || null,
      invalid: Boolean(upload.invalid),
    },
    queue: {
      pending_bytes: Math.max(0, queueBytes - uploadOffset),
      size_bytes: queueBytes,
      offset: uploadOffset,
    },
    last_parse: cursors.updatedAt || null,
    last_notify: await fileMtimeIso(path.join(trackerDir, "notify.signal")),
    scan_roots: config.scanRoots || null,
    providers: await collectProviders(env),
  };
}

function renderText(status) {
  const upload = status.upload.configured
    ? `${status.upload.api_url} (${status.upload.source})`
    : status.upload.invalid
      ? "invalid credentials (run: token-pulse connect)"
      : "not configured (run: token-pulse connect)";
  const detected = Object.entries(status.providers || {})
    .filter(([, value]) => value.installed)
    .map(([name]) => name);
  return [
    `TokenPulse v${status.version}`,
    "Status:",
    `- Upload: ${upload}`,
    `- Queue: ${formatBytes(status.queue.pending_bytes)} pending / ${formatBytes(status.queue.size_bytes)} total`,
    `- Last parse: ${status.last_parse || "never"}`,
    `- Last notify: ${status.last_notify || "never"}`,
    `- Local sources: ${detected.length > 0 ? detected.join(", ") : "none detected"}`,
    `- Tracker dir: ${status.tracker_dir}`,
    "",
  ].join("\n");
}

async function cmdStatus(argv = [], { home = os.homedir(), env = process.env, stdout = process.stdout } = {}) {
  const opts = parseArgs(argv);
  const status = await collectStatus({ home, env });
  stdout.write(opts.json || opts.light ? `${JSON.stringify(status, null, 2)}\n` : renderText(status));
  return status;
}

module.exports = { cmdStatus, collectStatus, parseArgs };
