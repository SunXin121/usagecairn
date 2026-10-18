"use strict";
const fs = require("node:fs");
const { defaultQueuePath, writeUploadConfig, readUploadConfig, configPathFor } = require("../lib/upload-config");

function parseArgs(argv) {
  const out = { apiUrl: null, key: null, show: false, clear: false };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--api-url") out.apiUrl = argv[++i] || null;
    else if (arg === "--key") out.key = argv[++i] || null;
    else if (arg === "--show") out.show = true;
    else if (arg === "--clear") out.clear = true;
  }
  return out;
}

async function cmdConnect(argv, { queuePath = defaultQueuePath(), log = console.log } = {}) {
  const opts = parseArgs(argv);
  if (opts.clear) {
    fs.rmSync(configPathFor(queuePath), { force: true });
    log("Removed stored upload credentials.");
    return { cleared: true };
  }
  if (opts.show || (!opts.apiUrl && !opts.key)) {
    const stored = readUploadConfig(queuePath);
    if (!stored.apiUrl) {
      log("No upload credentials stored. Run: tracker connect --api-url <url> --key <key>");
      return { stored: false };
    }
    log(`API: ${stored.apiUrl}\nAccess key: ${stored.accessKey ? `tt_…${stored.accessKey.slice(-4)}` : "(missing)"}\nStored in ${configPathFor(queuePath)}`);
    return { stored: true, apiUrl: stored.apiUrl };
  }
  if (!opts.apiUrl || !opts.key) throw new Error("Both --api-url and --key are required");
  const target = writeUploadConfig(queuePath, { apiUrl: opts.apiUrl, accessKey: opts.key.trim() });
  log(`Saved upload credentials to ${target} (mode 600). Background sync now uploads automatically.`);
  return { saved: true, target };
}

module.exports = { cmdConnect, parseArgs };
