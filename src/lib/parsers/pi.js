// pi usage parser and install discovery.
// Split from rollout.js; see src/lib/parsers/README.md for the layout.
const fssync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {
  toNonNegativeInt,
  pickWin32ProviderPath,
  decidePiCodingAgentDirOwner,
  expandHomePath,
  resolveOmpAgentDir,
  parsePiLikeIncremental,
} = require("./core");

// ─────────────────────────────────────────────────────────────────────────────
// pi (@mariozechner/pi-coding-agent) — passive JSONL reader
// (~/.pi/agent/sessions/**/*.jsonl)
//
// Same on-disk session format as oh-my-pi (omp): one JSONL file per session,
// first line type:"session" header, then a tree of message/model_change/etc.
// records. Token usage lives on type:"message" entries with role:"assistant"
// under message.usage.
//
// PI_CODING_AGENT_DIR is shared with omp (both upstream tools document it).
// resolvePiAgentDir / resolveOmpAgentDir use decidePiCodingAgentDirOwner to
// route the override to exactly one provider so the same sessions dir is
// never scanned twice.
// ─────────────────────────────────────────────────────────────────────────────

function resolvePiHome(env = process.env) {
  const home = env.HOME || os.homedir();
  if (process.platform === "win32") {
    return pickWin32ProviderPath({
      env,
      nativeValue: path.join(home, ".pi"),
      wslProviderDir: ".pi",
    });
  }
  return path.join(home, ".pi");
}

function resolvePiAgentDir(env = process.env) {
  if (env.TOKEN_PULSE_PI_AGENT_DIR) {
    return expandHomePath(env.TOKEN_PULSE_PI_AGENT_DIR, env);
  }
  if (env.PI_CODING_AGENT_DIR && decidePiCodingAgentDirOwner(env) === "pi") {
    return expandHomePath(env.PI_CODING_AGENT_DIR, env);
  }
  const piHome = resolvePiHome(env);
  return piHome ? path.join(piHome, "agent") : null;
}

// Defense in depth for invariant 2 (no double-count). Two explicit overrides
// pointing at the same path (e.g. TOKEN_PULSE_OMP_AGENT_DIR === TOKEN_PULSE_PI_AGENT_DIR,
// or TOKEN_PULSE_OMP_AGENT_DIR === PI_CODING_AGENT_DIR with ~/.pi present) bypass
// the install-signal disambiguator and would otherwise have both providers scan
// the same sessions directory under different `source` tags.
function piAgentDirCollidesWithOmp(env = process.env) {
  const piAgentDir = resolvePiAgentDir(env);
  const ompAgentDir = resolveOmpAgentDir(env);
  if (!piAgentDir || !ompAgentDir) return false;
  return path.resolve(piAgentDir) === path.resolve(ompAgentDir);
}

function resolvePiSessionFiles(env = process.env) {
  const agentDir = resolvePiAgentDir(env);
  if (!agentDir) return [];
  const sessionsDir = path.join(agentDir, "sessions");
  if (!fssync.existsSync(sessionsDir)) return [];
  const files = [];
  const walk = (dir) => {
    let entries;
    try { entries = fssync.readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const entry of entries) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(full);
      } else if (entry.isFile() && entry.name.endsWith(".jsonl")) {
        files.push(full);
      }
    }
  };
  try {
    for (const cwdDir of fssync.readdirSync(sessionsDir)) {
      const cwdPath = path.join(sessionsDir, cwdDir);
      let stat;
      try { stat = fssync.statSync(cwdPath); } catch { continue; }
      if (!stat.isDirectory()) continue;
      walk(cwdPath);
    }
  } catch {
    // ignore — return what we have
  }
  // Multica launches pi headless with a dedicated flat session dir:
  //   ~/.multica/pi-sessions/<session-id>.jsonl
  // Scan it too so orchestrated runs are counted alongside TUI sessions.
  try {
    const seen = new Set(files);
    const multicaSessionsDir = path.join(
      env.HOME || os.homedir(),
      ".multica",
      "pi-sessions",
    );
    for (const entry of fssync.readdirSync(multicaSessionsDir, { withFileTypes: true })) {
      if (!entry.isFile() || !entry.name.endsWith(".jsonl")) continue;
      const full = path.join(multicaSessionsDir, entry.name);
      if (!seen.has(full)) {
        seen.add(full);
        files.push(full);
      }
    }
  } catch {
    // ignore — no Multica install
  }
  files.sort((a, b) => a.localeCompare(b));
  return files;
}

function resolvePiDefaultModel() {
  // pi has no global default model; model is per-message.
  return "pi-unknown";
}

// Prime Agent (PrimeIntellect-ai/prime-agent) persists the same metadata-only
// assistant usage envelope as pi, but uses a flat sessions directory:
//   ~/.prime/agent/sessions/<session-id>.jsonl
// Keep its path and cursor namespace independent from pi so installations of
// both tools can never suppress or double-count each other.
function resolvePrimeAgentHome(env = process.env) {
  if (env.TOKEN_PULSE_PRIME_AGENT_HOME) {
    return expandHomePath(env.TOKEN_PULSE_PRIME_AGENT_HOME, env);
  }
  const home = env.HOME || os.homedir();
  if (process.platform === "win32") {
    return pickWin32ProviderPath({
      env,
      nativeValue: path.join(home, ".prime"),
      wslProviderDir: ".prime",
    });
  }
  return path.join(home, ".prime");
}

function resolvePrimeAgentDir(env = process.env) {
  if (env.TOKEN_PULSE_PRIME_AGENT_DIR) {
    return expandHomePath(env.TOKEN_PULSE_PRIME_AGENT_DIR, env);
  }
  const primeHome = resolvePrimeAgentHome(env);
  return primeHome ? path.join(primeHome, "agent") : null;
}

function resolvePrimeAgentSessionFiles(env = process.env) {
  const agentDir = resolvePrimeAgentDir(env);
  if (!agentDir) return [];
  const sessionsDir = path.join(agentDir, "sessions");
  if (!fssync.existsSync(sessionsDir)) return [];
  const files = [];
  const walk = (dir) => {
    let entries;
    try { entries = fssync.readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const entry of entries) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (entry.isFile() && entry.name.endsWith(".jsonl")) files.push(full);
    }
  };
  walk(sessionsDir);
  return files.sort((a, b) => a.localeCompare(b));
}

function resolvePrimeAgentDefaultModel() {
  return "prime-agent-unknown";
}

// Pi is a router: the same session can send turns to Anthropic, GitHub
// Copilot, or another backend. Keep provider names in the queue source so
// those turns cannot collapse into one bucket (or inherit the wrong pricing).
// Missing providers are deliberately kept on the historical `pi` source for
// compatibility with older session formats and already-synced data.
function piSourceForProvider(provider) {
  if (typeof provider !== "string" || !provider.trim()) return "pi";
  const slug = provider
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64);
  return slug ? `pi-${slug}` : "pi";
}

function primeAgentSourceForProvider(provider) {
  if (typeof provider !== "string" || !provider.trim()) return "prime-agent";
  const slug = provider
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64);
  return slug ? `prime-agent-${slug}` : "prime-agent";
}

async function parsePiIncremental(options = {}) {
  return parsePiLikeIncremental({
    ...options,
    stateKey: "pi",
    resolveSessionFiles: resolvePiSessionFiles,
    resolveDefaultModel: resolvePiDefaultModel,
    sourceForProvider: piSourceForProvider,
  });
}

// PI-Desktop stores flat session JSONL under ~/.pi-desktop/sessions. Its
// assistant usage is metadata (not pi CLI's message.usage); never read blocks.
function resolvePiDesktopSessionFiles(env = process.env) {
  const home = env.TOKEN_PULSE_PI_DESKTOP_HOME
    ? expandHomePath(env.TOKEN_PULSE_PI_DESKTOP_HOME, env)
    : path.join(env.HOME || os.homedir(), ".pi-desktop");
  if (!home) return [];
  const dir = path.join(home, "sessions");
  try {
    return fssync.readdirSync(dir, { withFileTypes: true })
      .filter((entry) => entry.isFile() && entry.name.endsWith(".jsonl") && !entry.name.endsWith(".revisions.jsonl"))
      .map((entry) => path.join(dir, entry.name))
      .sort((a, b) => a.localeCompare(b));
  } catch {
    return [];
  }
}

function readPiDesktopUsageEntry(entry) {
  if (entry?.type !== "message" || entry.role !== "assistant") return null;
  const meta = entry.meta;
  const usage = meta?.usage;
  if (!usage || typeof usage !== "object" || meta.status === "error") return null;
  // Output already contains reasoning; supply a fallback that does not add it twice.
  const totalTokens = Number(usage.totalTokens) > 0
    ? usage.totalTokens
    : toNonNegativeInt(usage.inputTokens) + toNonNegativeInt(usage.outputTokens)
      + toNonNegativeInt(usage.cacheReadTokens) + toNonNegativeInt(usage.cacheWriteTokens);
  return {
    id: entry.id,
    timestamp: entry.createdAt,
    message: {
      role: "assistant",
      model: usage.modelId || meta.modelId,
      usage: {
        input: usage.inputTokens,
        output: usage.outputTokens,
        cacheRead: usage.cacheReadTokens,
        cacheWrite: usage.cacheWriteTokens,
        reasoningTokens: usage.reasoningTokens,
        totalTokens,
      },
    },
  };
}

async function parsePiDesktopIncremental(options = {}) {
  return parsePiLikeIncremental({
    ...options,
    stateKey: "piDesktop",
    resolveSessionFiles: resolvePiDesktopSessionFiles,
    resolveDefaultModel: () => "pi-desktop-unknown",
    sourceForProvider: () => "pi-desktop",
    readEntry: readPiDesktopUsageEntry,
    // PI-Desktop's session header has no cwd; do not attribute it to a project.
    projectQueuePath: undefined,
  });
}

async function parsePrimeAgentIncremental(options = {}) {
  return parsePiLikeIncremental({
    ...options,
    stateKey: "primeAgent",
    resolveSessionFiles: resolvePrimeAgentSessionFiles,
    resolveDefaultModel: resolvePrimeAgentDefaultModel,
    sourceForProvider: primeAgentSourceForProvider,
  });
}

module.exports = {
  resolvePiHome,
  resolvePiAgentDir,
  piAgentDirCollidesWithOmp,
  resolvePiSessionFiles,
  resolvePiDefaultModel,
  resolvePrimeAgentHome,
  resolvePrimeAgentDir,
  resolvePrimeAgentSessionFiles,
  resolvePrimeAgentDefaultModel,
  parsePiIncremental,
  resolvePiDesktopSessionFiles,
  parsePiDesktopIncremental,
  parsePrimeAgentIncremental,
};
