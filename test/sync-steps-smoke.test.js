"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const root = path.resolve(__dirname, "..");

// Runs every provider step of a full `sync` in a child process whose
// environment holds only an isolated home, so no developer data is scanned.
test("a full sync runs every provider step and queues usage from an isolated home", async () => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "usagecairn-sync-smoke-"));
  try {
    const project = path.join(home, ".claude", "projects", "demo");
    await fs.mkdir(project, { recursive: true });
    await fs.writeFile(path.join(project, "session.jsonl"), JSON.stringify({
      timestamp: "2026-10-18T12:05:00.000Z", requestId: "r1",
      message: { id: "m1", model: "claude-sonnet-4", usage: { input_tokens: 100, output_tokens: 20 } },
    }) + "\n");
    const env = { HOME: home, USERPROFILE: home, PATH: process.env.PATH,
      APPDATA: path.join(home, "AppData", "Roaming"), LOCALAPPDATA: path.join(home, "AppData", "Local") };
    if (process.env.SystemRoot) env.SystemRoot = process.env.SystemRoot;
    const script = "require('./src/commands/sync').cmdSync([]).then(() => {}, (e) => { console.error(e); process.exit(1); })";
    const run = spawnSync(process.execPath, ["-e", script], { cwd: root, env, encoding: "utf8", timeout: 60_000 });
    assert.equal(run.status, 0, run.stderr);
    assert.doesNotMatch(run.stderr, /failed|Error/i);
    assert.match(run.stdout, /Sync finished/);
    const queue = (await fs.readFile(path.join(home, ".token-pulse", "tracker", "queue.jsonl"), "utf8")).trim().split("\n").map(JSON.parse);
    assert.deepEqual(queue.map((row) => [row.source, row.model, row.total_tokens]), [["claude", "claude-sonnet-4", 120]]);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});
