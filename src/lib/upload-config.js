"use strict";
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const FILE = "upload.json";

function defaultQueuePath() {
  return path.join(os.homedir(), ".tokentracker", "tracker", "queue.jsonl");
}

function configPathFor(queuePath) {
  return path.join(path.dirname(queuePath), FILE);
}

function isValidApiUrl(value) {
  try {
    const url = new URL(String(value || ""));
    if (url.username || url.password || url.search || url.hash || url.pathname !== "/") return false;
    return url.protocol === "https:" ||
      (url.protocol === "http:" && ["localhost", "127.0.0.1"].includes(url.hostname));
  } catch {
    return false;
  }
}

function isValidAccessKey(value) {
  return typeof value === "string" && /^tt_[a-f0-9]{64}$/.test(value);
}

/**
 * Resolve upload credentials for background hook runs.
 * Env wins (per-invocation override); the persisted file keeps hooks working
 * without shell profile changes. Only the local machine holds the raw key.
 */
function resolveUploadConfig({ queuePath, env = process.env } = {}) {
  const fileConfig = readUploadConfig(queuePath);
  const apiUrl = env.TOKENTRACKER_API_URL || fileConfig.apiUrl || null;
  const accessKey = env.TOKENTRACKER_ACCESS_KEY || fileConfig.accessKey || null;
  if (!apiUrl || !accessKey) return { apiUrl: null, accessKey: null, source: null };
  if (!isValidApiUrl(apiUrl) || !isValidAccessKey(accessKey)) {
    return { apiUrl: null, accessKey: null, source: null, invalid: true };
  }
  return { apiUrl: new URL(apiUrl).origin, accessKey, source: env.TOKENTRACKER_API_URL ? "env" : "file" };
}

function readUploadConfig(queuePath) {
  try {
    const parsed = JSON.parse(fs.readFileSync(configPathFor(queuePath), "utf8"));
    return {
      apiUrl: typeof parsed?.apiUrl === "string" ? parsed.apiUrl : null,
      accessKey: typeof parsed?.accessKey === "string" ? parsed.accessKey : null,
    };
  } catch {
    return { apiUrl: null, accessKey: null };
  }
}

function writeUploadConfig(queuePath, { apiUrl, accessKey }) {
  if (!isValidApiUrl(apiUrl)) throw new Error("API URL must be an HTTPS origin (or local HTTP)");
  if (!isValidAccessKey(accessKey)) throw new Error("Access key must look like tt_<64 hex characters>");
  const target = configPathFor(queuePath);
  fs.mkdirSync(path.dirname(target), { recursive: true });
  const temp = `${target}.${process.pid}.tmp`;
  fs.writeFileSync(temp, JSON.stringify({ apiUrl: new URL(apiUrl).origin, accessKey }, null, 2), { mode: 0o600 });
  fs.renameSync(temp, target);
  return target;
}

module.exports = { resolveUploadConfig, readUploadConfig, writeUploadConfig, isValidApiUrl, isValidAccessKey, configPathFor };
module.exports.defaultQueuePath = defaultQueuePath;
