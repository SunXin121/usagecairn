"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { detectPassiveSources } = require("../src/lib/init-passive-sources");

async function touch(file) {
  await fs.mkdir(path.dirname(file), { recursive: true });
  await fs.writeFile(file, "");
}

test("init discovers passive sources from the same local artifacts as sync", async () => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "usagecairn-init-passive-"));
  try {
    const env = {
      HOME: home,
      XDG_CONFIG_HOME: path.join(home, ".config"),
      XDG_DATA_HOME: path.join(home, ".local", "share"),
      QODER_CN_DB_PATH: path.join(home, "qoder-cn.db"),
      CLAUDE_SCIENCE_DB_PATH: path.join(home, "science.db"),
      KIRO_HOME: path.join(home, ".kiro"),
      TOKEN_PULSE_HERMES_HOME: path.join(home, ".hermes"),
      COPILOT_HOME: path.join(home, ".copilot"),
      TOKEN_PULSE_KILOCODE_ROOTS: path.join(home, "Code"),
      TOKEN_PULSE_OMO_HOME: path.join(home, ".omo"),
      GEMINI_HOME: path.join(home, ".gemini"),
      TOKEN_PULSE_GOOSE_DB: path.join(home, "goose.db"),
      DROID_SESSIONS_DIR: path.join(home, "droid"),
      TOKEN_PULSE_ZED_DB: path.join(home, "zed.db"),
      TOKEN_PULSE_TRAE_CN_HOME: path.join(home, "trae-cn"),
      TOKEN_PULSE_TRAE_CN_USAGE: "1",
    };
    await Promise.all([
      touch(env.QODER_CN_DB_PATH), touch(env.CLAUDE_SCIENCE_DB_PATH),
      touch(path.join(home, ".zcode", "cli", "db", "db.sqlite")),
      touch(path.join(env.KIRO_HOME, "sessions", "cli", "01234567-89ab-cdef-0123-456789abcdef.json")),
      touch(path.join(env.TOKEN_PULSE_HERMES_HOME, "profiles", "work", "state.db")),
      touch(path.join(home, ".qoder", "projects", "demo", "session.jsonl")),
      touch(path.join(home, "Code", "User", "globalStorage", "rooveterinaryinc.roo-cline", "tasks", "1", "ui_messages.json")),
      touch(path.join(env.TOKEN_PULSE_OMO_HOME, "agent", "sessions", "--project--", "a.jsonl")),
      touch(path.join(env.GEMINI_HOME, "antigravity", "brain", "a", ".system_generated", "logs", "transcript.jsonl")),
      touch(path.join(env.COPILOT_HOME, "session-store.db")),
      touch(env.TOKEN_PULSE_GOOSE_DB), touch(path.join(env.DROID_SESSIONS_DIR, "a.settings.json")),
      touch(env.TOKEN_PULSE_ZED_DB),
      touch(path.join(env.TOKEN_PULSE_TRAE_CN_HOME, "User", "globalStorage", "storage.json")),
    ]);
    const found = await detectPassiveSources({ home, env });
    for (const label of ["Qoder", "Qoder CN", "Claude Science", "ZCode", "Kiro", "Hermes Agent",
      "GitHub Copilot", "Goose", "Droid", "Zed Agent", "Roo Code", "OmO", "Antigravity", "TRAE Work CN"]) {
      assert.ok(found.some((item) => item.label === label && item.status === "detected"), label);
    }
    assert.ok(!found.some((item) => item.label === "Cline"), "an unrelated extension is not inferred");
    const withoutOptIn = await detectPassiveSources({ home, env: { ...env, TOKEN_PULSE_TRAE_CN_USAGE: "0" } });
    assert.match(withoutOptIn.find((item) => item.label === "TRAE Work CN").detail, /requires TOKEN_PULSE_TRAE_CN_USAGE=1/);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("init detects newly installed passive sources before any conversation", async () => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "usagecairn-init-empty-"));
  try {
    const env = {
      HOME: home,
      DSH_HOME: path.join(home, ".dsh"),
      TOKEN_PULSE_COMMANDCODE_HOME: path.join(home, ".commandcode"),
      TOKEN_PULSE_HERMES_HOME: path.join(home, ".hermes"),
      TOKEN_PULSE_OMO_HOME: path.join(home, ".omo"),
      QODER_HOME: path.join(home, "Qoder"),
      QODER_CN_HOME: path.join(home, "QoderCN"),
      TOKEN_PULSE_TRAE_CN_HOME: path.join(home, "TRAE SOLO CN"),
    };
    for (const dir of [env.DSH_HOME, env.TOKEN_PULSE_COMMANDCODE_HOME,
      env.TOKEN_PULSE_HERMES_HOME, env.TOKEN_PULSE_OMO_HOME, env.QODER_HOME,
      env.QODER_CN_HOME, env.TOKEN_PULSE_TRAE_CN_HOME]) {
      await fs.mkdir(dir, { recursive: true });
    }
    const found = detectPassiveSources({ home, env });
    for (const label of ["DeepSeek Harness", "Command Code", "Hermes Agent", "OmO", "Qoder", "Qoder CN", "TRAE Work CN"]) {
      assert.ok(found.some((item) => item.label === label && item.status === "detected"), label);
    }
    assert.ok(!found.some((item) => item.label === "Antigravity"));
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});
