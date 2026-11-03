// minimax-code usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {
  pickWin32ProviderPath,
  expandHomePath,
  parsePiLikeIncremental,
} = require("./core");

// ─────────────────────────────────────────────────────────────────────────────
// MiniMax Code (MiniMax's desktop coding agent) — passive JSONL reader
// (~/.minimax/v2/sessions/YYYY/MM/DD/<HH-MM-SS-mmm>-session_<id>/messages.jsonl)
//
// Records carry pi-shaped usage (input excludes cacheRead) and a ms-epoch
// message.timestamp, but have no type:"message" wrapper and dedupe on the
// top-level message_id. MiniMax Code routes to many upstream models and records
// the model per message. Its usage.cost block is always 0, so it is ignored in
// favor of normal pricing. Migrated legacy sessions replay as model
// "historical-transcript" with all-zero usage and are dropped by the engine's
// zero-token guard. There is no cwd header, so no project attribution.
// ─────────────────────────────────────────────────────────────────────────────

const MINIMAX_CODE_SOURCE = "minimax-code";
const MINIMAX_CODE_SESSION_FILE = "messages.jsonl";
// sessions / YYYY / MM / DD / <session dir> / messages.jsonl
const MINIMAX_CODE_MAX_SESSION_DEPTH = 5;

function resolveMinimaxCodeHome(env = process.env) {
  if (env.TOKEN_PULSE_MINIMAX_HOME) return expandHomePath(env.TOKEN_PULSE_MINIMAX_HOME, env);
  const home = env.HOME || os.homedir();
  if (process.platform === "win32") {
    return pickWin32ProviderPath({
      env,
      nativeValue: path.join(home, ".minimax"),
      wslProviderDir: ".minimax",
    });
  }
  return path.join(home, ".minimax");
}

function resolveMinimaxCodeSessionsDir(env = process.env) {
  const minimaxHome = resolveMinimaxCodeHome(env);
  return minimaxHome ? path.join(minimaxHome, "v2", "sessions") : null;
}

function resolveMinimaxCodeSessionFiles(env = process.env) {
  const sessionsDir = resolveMinimaxCodeSessionsDir(env);
  if (!sessionsDir) return [];
  const files = [];
  const walk = (dir, depth) => {
    let entries;
    try { entries = fssync.readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        if (depth < MINIMAX_CODE_MAX_SESSION_DEPTH) walk(fullPath, depth + 1);
      } else if (entry.isFile() && entry.name === MINIMAX_CODE_SESSION_FILE && depth > 1) {
        files.push(fullPath);
      }
    }
  };
  walk(sessionsDir, 1);
  files.sort((a, b) => a.localeCompare(b));
  return files;
}

function readMinimaxCodeEntry(entry) {
  if (!entry || typeof entry !== "object" || !entry.message) return null;
  return {
    id: typeof entry.message_id === "string" && entry.message_id ? entry.message_id : null,
    message: entry.message,
    timestamp: null,
  };
}

async function parseMinimaxCodeIncremental(options = {}) {
  return parsePiLikeIncremental({
    ...options,
    // Session files have no cwd header, so project attribution is unsupported.
    projectQueuePath: undefined,
    stateKey: "minimaxCode",
    resolveSessionFiles: resolveMinimaxCodeSessionFiles,
    resolveDefaultModel: () => `${MINIMAX_CODE_SOURCE}-unknown`,
    sourceForProvider: () => MINIMAX_CODE_SOURCE,
    readEntry: readMinimaxCodeEntry,
  });
}

module.exports = {
  resolveMinimaxCodeHome,
  resolveMinimaxCodeSessionsDir,
  resolveMinimaxCodeSessionFiles,
  parseMinimaxCodeIncremental,
};
