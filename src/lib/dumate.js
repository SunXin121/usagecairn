const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { expandHome } = require("./scan-roots");
const { readOpencodeDbMessages, parseOpencodeDbIncremental, normalizeOpencodeTokens } = require("./rollout");

function resolveDumateDbPaths({ home = os.homedir(), env = process.env, platform = process.platform } = {}) {
  const override = expandHome(env.TOKEN_PULSE_DUMATE_HOME, home);
  const appdata = typeof env.APPDATA === "string" && env.APPDATA.trim()
    ? env.APPDATA.trim() : path.join(home, "AppData", "Roaming");
  if (!override && platform !== "win32") return [];
  const root = override || path.join(appdata, "qianfan-desktop-app", "qianfan_desk_xdg");
  // The override accepts the data/opencode directory or an install's XDG root.
  const candidates = [path.join(root, "opencode.db"), path.join(root, "data", "opencode", "opencode.db")];
  try {
    for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
      if (entry.isDirectory()) candidates.push(path.join(root, entry.name, "data", "opencode", "opencode.db"));
    }
  } catch (_e) { }
  const seen = new Set();
  return candidates.sort().filter((candidate) => {
    try {
      if (!fs.statSync(candidate).isFile()) return false;
      const realPath = fs.realpathSync(candidate);
      if (seen.has(realPath)) return false;
      seen.add(realPath);
      return true;
    } catch (_e) { return false; }
  });
}

async function parseDumateIncremental({ dbPaths, ...options }) {
  // Reinstalls can copy the same sessions into a new install-id directory.
  // Share a message cursor across installations and choose the newest copy
  // before parsing, so stale copies cannot roll back a completed turn.
  const messages = new Map();
  const includeZeroMessage = (message) => {
    const provider = message.data?.providerID || message.data?.model?.providerID;
    return String(provider || "").toLowerCase() === "qianfanpersonalquota";
  };
  for (const dbPath of dbPaths) {
    for (const message of readOpencodeDbMessages(dbPath, {}, { propagateReadFailure: true, includeZeroMessage })) {
      // DuMate 1.0.83 bundles OpenCode's import command without an import
      // marker. Its own runtime uses QianfanPersonalQuota; other providers
      // can be imported history already counted under Claude or OpenCode.
      const provider = message.data?.providerID || message.data?.model?.providerID;
      if (String(provider || "").toLowerCase() !== "qianfanpersonalquota") continue;
      const id = message.data?.id || message.id;
      const session = message.data?.sessionID || message.sessionID || "";
      if (!id) continue;
      const key = `${session}|${id}`;
      if (Number(message.timeUpdated || 0) < Number(options.cursors?.dumate?.messages?.[key]?.sourceUpdatedAt || 0)) continue;
      const previous = messages.get(key);
      if (!previous || Number(message.timeUpdated || 0) > Number(previous.message.timeUpdated || 0)) {
        messages.set(key, { message, dbPath });
      }
    }
  }
  const byDatabase = new Map();
  for (const [key, { message, dbPath }] of messages) {
    // DuMate's OpenAI-compatible adapter retains reasoning inside output.
    // The shared parser expects separate columns, so subtract that subset once.
    const usage = normalizeOpencodeTokens(message.data?.tokens);
    // Zero copies participate in latest-copy selection so a stale install
    // cannot resurrect usage. Only a previously counted message needs a
    // queued retraction; a new empty message contributes nothing.
    const previous = options.cursors?.dumate?.messages?.[key];
    if (usage?.total_tokens === 0 && (!previous?.lastTotals || previous.dedupedForkCopy === true)) continue;
    if (!byDatabase.has(dbPath)) byDatabase.set(dbPath, []);
    const normalized = usage ? {
      ...message,
      data: { ...message.data, tokens: { ...message.data.tokens,
        output: Math.max(0, usage.output_tokens - usage.reasoning_output_tokens),
      } },
    } : message;
    byDatabase.get(dbPath).push(normalized);
  }
  const result = { recordsProcessed: 0, eventsAggregated: 0, bucketsQueued: 0 };
  for (const [dbPath, dbMessages] of byDatabase) {
    const parsed = await parseOpencodeDbIncremental({
      ...options, dbPath, dbMessages, source: "dumate", cursorKey: "dumate",
    });
    for (const message of dbMessages) {
      const key = `${message.data?.sessionID || message.sessionID || ""}|${message.data?.id || message.id}`;
      const cursor = options.cursors?.dumate?.messages?.[key];
      if (cursor) cursor.sourceUpdatedAt = Number(message.timeUpdated || 0);
    }
    result.recordsProcessed += parsed.messagesProcessed || 0;
    result.eventsAggregated += parsed.eventsAggregated || 0;
    result.bucketsQueued += parsed.bucketsQueued || 0;
  }
  return result;
}

module.exports = { resolveDumateDbPaths, parseDumateIncremental };
