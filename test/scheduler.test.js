"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { test } = require("node:test");
const { installScheduler, removeScheduler, schedulerSpec, DEFAULT_INTERVAL_MINUTES } = require("../src/lib/scheduler");

async function fixture(fn) {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "uc-scheduler-"));
  const trackerBinPath = path.join(home, ".token-pulse", "tracker", "app", "bin", "usagecairn.js");
  await fs.mkdir(path.dirname(trackerBinPath), { recursive: true });
  await fs.writeFile(trackerBinPath, "#!/usr/bin/env node\n");
  try { await fn({ home, trackerBinPath }); }
  finally { await fs.rm(home, { recursive: true, force: true }); }
}

function fakeExec(calls) {
  return async (cmd, args) => {
    calls.push([cmd, args]);
    if (cmd === "schtasks" && args.includes("/Query")) return { ok: false, stdout: "", message: "not found" };
    return { ok: true, stdout: "", message: "" };
  };
}

test("Linux timer installs idempotently, calls the installed runtime, and is removed", async () => fixture(async ({ home, trackerBinPath }) => {
  const calls = [];
  const deps = { exec: fakeExec(calls) };
  for (let i = 0; i < 2; i++) {
    const result = await installScheduler({ home, trackerBinPath, platform: "linux", deps });
    assert.deepEqual(result, { installed: true, intervalMinutes: DEFAULT_INTERVAL_MINUTES });
  }
  const spec = schedulerSpec({ home, platform: "linux", trackerBinPath });
  const service = await fs.readFile(spec.files[0].path, "utf8");
  assert.ok(service.includes(`"${process.execPath.replace(/\\/g, "\\\\")}" "${trackerBinPath.replace(/\\/g, "\\\\")}" "sync" "--auto" "--background" "--all-local-sources"`));
  assert.match(await fs.readFile(spec.files[1].path, "utf8"), /OnUnitActiveSec=5min/);
  assert.ok(calls.some(([cmd, args]) => cmd === "systemctl" && args.includes("enable") && args.includes("--now")));
  assert.equal((await removeScheduler({ home, platform: "linux", deps })).removed, true);
  for (const file of spec.files) await assert.rejects(fs.access(file.path), { code: "ENOENT" });
  assert.equal((await removeScheduler({ home, platform: "linux", deps })).removed, false);
}));

test("activation failure is not presented as installed; unmanaged units are untouched", async () => fixture(async ({ home, trackerBinPath }) => {
  const deps = { exec: async () => ({ ok: false, message: "No user bus" }) };
  const result = await installScheduler({ home, trackerBinPath, platform: "linux", deps });
  assert.equal(result.installed, false);
  assert.equal(result.reason, "No user bus");
  const spec = schedulerSpec({ home, platform: "linux" });
  await fs.writeFile(spec.files[0].path, "unmanaged", "utf8");
  const calls = [];
  const owned = await installScheduler({ home, trackerBinPath, platform: "linux", deps: { exec: fakeExec(calls) } });
  assert.equal(owned.reason, "unmanaged-file");
  assert.equal((await removeScheduler({ home, platform: "linux", deps: { exec: fakeExec(calls) } })).reason, "unmanaged-file");
  assert.equal(await fs.readFile(spec.files[0].path, "utf8"), "unmanaged");
  assert.equal(calls.length, 0);
}));

test("macOS LaunchAgent runs every five minutes and can be unloaded", async () => fixture(async ({ home, trackerBinPath }) => {
  const calls = [];
  const deps = { exec: fakeExec(calls) };
  assert.equal((await installScheduler({ home, trackerBinPath, platform: "darwin", deps })).installed, true);
  const plist = schedulerSpec({ home, platform: "darwin" }).files[0].path;
  const text = await fs.readFile(plist, "utf8");
  assert.match(text, /<integer>300<\/integer>/);
  assert.match(text, /--all-local-sources/);
  assert.ok(calls.some(([cmd, args]) => cmd === "launchctl" && args.includes("load")));
  assert.equal((await removeScheduler({ home, platform: "darwin", deps })).removed, true);
  await assert.rejects(fs.access(plist), { code: "ENOENT" });
}));

test("Windows task command has real paths, and unrelated tasks are preserved", async () => fixture(async ({ home, trackerBinPath }) => {
  const calls = [];
  const deps = { exec: fakeExec(calls) };
  assert.equal((await installScheduler({ home, trackerBinPath, platform: "win32", deps })).installed, true);
  const spec = schedulerSpec({ home, trackerBinPath, platform: "win32" });
  const create = calls.find(([cmd, args]) => cmd === "schtasks" && args.includes("/Create"))[1];
  assert.equal(create[create.indexOf("/TR") + 1], `wscript.exe //B //Nologo "${spec.scriptPath}"`);
  assert.equal(create[create.indexOf("/MO") + 1], "5");
  const script = await fs.readFile(spec.scriptPath, "utf8");
  assert.match(script, /UsageCairn background sync scheduler/);
  assert.match(script, /WScript\.Shell"\)\.Run\("""/);
  assert.match(script, /, 0, True\)/);
  assert.ok(script.includes(trackerBinPath));
  const unmanaged = { exec: async () => ({ ok: true, stdout: "<Command>other.exe</Command>" }) };
  assert.equal((await installScheduler({ home, trackerBinPath, platform: "win32", deps: unmanaged })).reason, "unmanaged-task");
  assert.equal((await removeScheduler({ home, platform: "win32", deps: unmanaged })).reason, "unmanaged-task");
  let deleted = false;
  const owned = { exec: async (cmd, args) => {
    if (args.includes("/Delete")) { deleted = true; return { ok: true }; }
    return { ok: true, stdout: "<Command>usagecairn.js --all-local-sources</Command>" };
  } };
  assert.equal((await removeScheduler({ home, platform: "win32", deps: owned })).removed, true);
  assert.equal(deleted, true);
  await assert.rejects(fs.access(spec.scriptPath), { code: "ENOENT" });
}));

test("Windows reinstallation migrates the direct-node task and preserves foreign scripts", async () => fixture(async ({ home, trackerBinPath }) => {
  const spec = schedulerSpec({ home, trackerBinPath, platform: "win32" });
  const calls = [];
  const legacy = { exec: async (cmd, args) => {
    calls.push([cmd, args]);
    if (args.includes("/Query")) return { ok: true, stdout: "<Command>node.exe</Command><Arguments>usagecairn.js sync --all-local-sources</Arguments>" };
    return { ok: true };
  } };
  assert.equal((await installScheduler({ home, trackerBinPath, platform: "win32", deps: legacy })).installed, true);
  assert.ok(calls.some(([, args]) => args.includes("/Create") && args.includes("/F")));
  await fs.writeFile(spec.scriptPath, "foreign script");
  assert.equal((await installScheduler({ home, trackerBinPath, platform: "win32", deps: legacy })).reason, "unmanaged-file");
  assert.equal((await removeScheduler({ home, platform: "win32", deps: legacy })).reason, "unmanaged-file");
  assert.equal(await fs.readFile(spec.scriptPath, "utf8"), "foreign script");
}));

test("unsupported platforms and missing runtimes are skipped", async () => fixture(async ({ home, trackerBinPath }) => {
  assert.equal((await installScheduler({ home, trackerBinPath, platform: "sunos" })).reason, "unsupported-platform");
  assert.equal((await installScheduler({ home, trackerBinPath: path.join(home, "missing"), platform: "linux" })).reason, "local-runtime-missing");
}));
