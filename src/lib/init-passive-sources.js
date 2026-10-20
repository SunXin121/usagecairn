"use strict";

// Passive integrations need no hook to install. Show them as soon as their
// application data directory exists, even if no conversation has been saved.
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { resolveZcodeNativeDbPath } = require("./install-resolver");
const { resolveTraeCnHome, isTraeCnUsageEnabled } = require("./trae-cn-config");
const {
  resolveQoderDbPath, resolveQoderProjectsDir, resolveQoderCnProjectsDir,
  resolveClaudeScienceDbPath, resolveKiroBasePath, resolveKiroCliDbPath,
  resolveHermesPath, resolveCopilotOtelPaths, resolveCopilotSessionStorePaths,
  resolveCopilotAppDbPaths, resolveGooseDbPath, resolveZedDbPath,
  resolveDroidSessionsDirs, resolveKilocodeRoots, resolveOmoHome,
  omoAgentDirCollidesWithOmp,
  resolveDshHomes, resolveCommandCodeHomes,
} = require("./rollout");

function detectPassiveSources({ home = os.homedir(), env = process.env } = {}) {
  const found = [];
  const isDir = (p) => {
    try { return Boolean(p && fs.statSync(p).isDirectory()); }
    catch { return false; }
  };
  const anyDir = (paths) => paths.some(isDir);
  const add = (label, installed, detail = "Passive reader (no hook needed; usage appears after a session)") => {
    if (installed) found.push({ label, status: "detected", detail });
  };
  const dataHome = env.XDG_DATA_HOME || path.join(home, ".local", "share");
  add("DeepSeek Harness", anyDir(resolveDshHomes(env)));
  add("Command Code", anyDir(resolveCommandCodeHomes(env)));
  const qoderHome = (name) => env[`${name}_HOME`] || (env[`${name}_DB_PATH`]
    ? path.dirname(path.resolve(env[`${name}_DB_PATH`]))
    : path.resolve(resolveQoderDbPath({ home, env,
      appDir: name === "QODER_CN" ? "QoderCN" : "Qoder", envPrefix: name }), "../../../.."));
  const qoderProjects = (name) => name === "QODER_CN"
    ? resolveQoderCnProjectsDir({ home, env }) : resolveQoderProjectsDir({ home, env });
  add("Qoder", anyDir([qoderHome("QODER"), env.QODER_PROJECTS_DIR
    ? qoderProjects("QODER") : path.dirname(qoderProjects("QODER"))]));
  add("Qoder CN", anyDir([qoderHome("QODER_CN"), env.QODER_CN_PROJECTS_DIR
    ? qoderProjects("QODER_CN") : path.dirname(qoderProjects("QODER_CN"))]));
  add("Claude Science", isDir(env.CLAUDE_SCIENCE_HOME || path.dirname(resolveClaudeScienceDbPath({ home, env }))));
  add("ZCode", isDir(env.ZCODE_HOME || path.resolve(resolveZcodeNativeDbPath({ home, env }), "../../..")));
  add("Kiro", anyDir([resolveKiroBasePath(env), env.KIRO_HOME || path.join(home, ".kiro"),
    path.dirname(resolveKiroCliDbPath(env))]));
  add("Hermes Agent", isDir(resolveHermesPath(env)));
  add("GitHub Copilot", anyDir([
    env.COPILOT_HOME || path.join(home, ".copilot"),
    ...resolveCopilotOtelPaths(env).map(path.dirname),
    ...resolveCopilotSessionStorePaths(env).map(path.dirname),
    ...resolveCopilotAppDbPaths(env).map(path.dirname),
  ]));
  const gooseDb = resolveGooseDbPath(env);
  add("Goose", anyDir([env.GOOSE_PATH_ROOT, gooseDb && (env.TOKEN_PULSE_GOOSE_DB
    ? path.dirname(gooseDb) : path.dirname(path.dirname(gooseDb))),
    path.join(dataHome, "Block", "goose")]));
  add("Droid", anyDir([env.FACTORY_DIR || path.join(home, ".factory"),
    ...resolveDroidSessionsDirs(env)]));
  const zedDb = resolveZedDbPath(env);
  add("Zed Agent", isDir(zedDb && (env.TOKEN_PULSE_ZED_DB
    ? path.dirname(zedDb) : path.dirname(path.dirname(zedDb)))));
  add("Roo Code", resolveKilocodeRoots(env).some((root) => isDir(path.join(root,
    "User", "globalStorage", "rooveterinaryinc.roo-cline"))));
  if (!omoAgentDirCollidesWithOmp(env)) add("OmO", isDir(resolveOmoHome(env)));

  const geminiHome = env.GEMINI_HOME || path.join(home, ".gemini");
  add("Antigravity", anyDir(["antigravity", "antigravity-ide", "antigravity-cli"]
    .map((name) => path.join(geminiHome, name))));
  const traeCnHome = resolveTraeCnHome({ home, env });
  add("TRAE Work CN", isDir(traeCnHome), isTraeCnUsageEnabled(env)
    ? "API reader enabled (requires valid login)" : "Detected; API usage requires TOKEN_PULSE_TRAE_CN_USAGE=1");
  return found;
}

module.exports = { detectPassiveSources };
