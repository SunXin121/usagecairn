"use strict";

// A user-level timer supplements event-driven hooks for passive local sources.
// Only units/plists carrying our marker (or our Windows task command) are
// overwritten or removed. A scheduler failure must not break init/uninstall.
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { execFile } = require("node:child_process");

const DEFAULT_INTERVAL_MINUTES = 5;
const MARKER = "UsageCairn background sync scheduler";
const ARGS = ["sync", "--auto", "--background", "--all-local-sources"];
const TASK_NAME = "UsageCairnSync";

function run(command, args) {
  return new Promise((resolve) => {
    execFile(command, args, { timeout: 30_000 }, (error, stdout, stderr) => {
      resolve({ ok: !error, message: String(stderr || error?.message || "").trim(), stdout: String(stdout || "") });
    });
  });
}

function xmlEscape(value) {
  return String(value).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&apos;");
}

function systemdQuote(value) {
  return `"${String(value).replace(/%/g, "%%").replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

function schedulerSpec({ home = os.homedir(), platform = process.platform, intervalMinutes = DEFAULT_INTERVAL_MINUTES,
  nodePath = process.execPath, trackerBinPath = path.join(home, ".token-pulse", "tracker", "app", "bin", "usagecairn.js") } = {}) {
  if (!Number.isInteger(intervalMinutes) || intervalMinutes < 1) throw new Error("Invalid sync interval");
  if (platform === "linux") {
    const base = path.join(home, ".config", "systemd", "user");
    return {
      platform, intervalMinutes,
      files: [
        { path: path.join(base, "usagecairn-sync.service"), content: [
          "[Unit]", `Description=${MARKER}`, "", "[Service]", "Type=oneshot",
          `ExecStart=${[nodePath, trackerBinPath, ...ARGS].map(systemdQuote).join(" ")}`, "",
        ].join("\n") },
        { path: path.join(base, "usagecairn-sync.timer"), content: [
          "[Unit]", `Description=${MARKER} timer`, "", "[Timer]", "OnBootSec=2min",
          `OnUnitActiveSec=${intervalMinutes}min`, "Persistent=true", "", "[Install]", "WantedBy=timers.target", "",
        ].join("\n") },
      ],
    };
  }
  if (platform === "darwin") {
    return {
      platform, intervalMinutes,
      files: [{ path: path.join(home, "Library", "LaunchAgents", "com.usagecairn.sync.plist"), content: [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">',
        '<plist version="1.0"><dict>',
        `  <key>Label</key><string>com.usagecairn.sync</string>`,
        `  <key>Comment</key><string>${MARKER}</string>`,
        "  <key>ProgramArguments</key><array>",
        ...[nodePath, trackerBinPath, ...ARGS].map((arg) => `    <string>${xmlEscape(arg)}</string>`),
        "  </array>",
        `  <key>StartInterval</key><integer>${intervalMinutes * 60}</integer>`,
        "  <key>RunAtLoad</key><false/>",
        "</dict></plist>", "",
      ].join("\n") }],
    };
  }
  if (platform === "win32") {
    // Keep the launcher outside app/, which init replaces when updating the runtime.
    const scriptPath = path.join(path.dirname(trackerBinPath), "..", "..", "usagecairn-sync.vbs");
    const command = [nodePath, trackerBinPath].map((arg) => `"${arg}"`).join(" ") + ` ${ARGS.join(" ")}`;
    return {
      platform, intervalMinutes, scriptPath,
      files: [{ path: scriptPath, content: [
        `' ${MARKER}`,
        "Dim exitCode",
        `exitCode = CreateObject("WScript.Shell").Run("${command.replace(/"/g, '""')}", 0, True)`,
        "WScript.Quit exitCode", "",
      ].join("\r\n") }],
      taskCommand: `wscript.exe //B //Nologo "${scriptPath}"`,
    };
  }
  return null;
}

async function readOwnedFiles(spec) {
  let count = 0;
  for (const file of spec.files) {
    let content;
    try { content = await fs.readFile(file.path, "utf8"); }
    catch (error) { if (error.code === "ENOENT") continue; throw error; }
    if (!content.includes(MARKER)) return { conflict: file.path, count };
    count++;
  }
  return { count };
}

function isOwnedWindowsTask(xml, spec) {
  // Accept the old direct-node task so a later init can migrate it.
  if (xml.includes("usagecairn.js") && xml.includes("--all-local-sources")) return true;
  const decoded = xml.replace(/&quot;/g, '"').replace(/&apos;/g, "'")
    .replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&");
  return decoded.toLowerCase().includes("wscript.exe") &&
    decoded.toLowerCase().includes(spec.scriptPath.toLowerCase());
}

async function installScheduler({ home = os.homedir(), platform = process.platform, trackerBinPath,
  intervalMinutes = DEFAULT_INTERVAL_MINUTES, deps = {} } = {}) {
  const exec = deps.exec || run;
  const spec = schedulerSpec({ home, platform, trackerBinPath, intervalMinutes });
  if (!spec) return { installed: false, reason: "unsupported-platform" };
  try {
    if (!trackerBinPath || !(await fs.stat(trackerBinPath)).isFile()) {
      return { installed: false, reason: "local-runtime-missing" };
    }
    if (platform === "win32") {
      const existing = await exec("schtasks", ["/Query", "/TN", TASK_NAME, "/XML"]);
      if (existing.ok && !isOwnedWindowsTask(existing.stdout, spec)) {
        return { installed: false, reason: "unmanaged-task" };
      }
      const owned = await readOwnedFiles(spec);
      if (owned.conflict) return { installed: false, reason: "unmanaged-file", path: owned.conflict };
      await fs.mkdir(path.dirname(spec.scriptPath), { recursive: true });
      await fs.writeFile(spec.scriptPath, spec.files[0].content, { encoding: "utf8", mode: 0o600 });
      // Do not use /F when the task is absent (or the query failed): an
      // unexpected existing task must never be silently overwritten.
      const createArgs = ["/Create", "/TN", TASK_NAME, "/TR", spec.taskCommand,
        "/SC", "MINUTE", "/MO", String(intervalMinutes)];
      if (existing.ok) createArgs.push("/F");
      const result = await exec("schtasks", createArgs);
      return result.ok ? { installed: true, intervalMinutes } : { installed: false, reason: result.message || "activation-failed" };
    }
    const owned = await readOwnedFiles(spec);
    if (owned.conflict) return { installed: false, reason: "unmanaged-file", path: owned.conflict };
    for (const file of spec.files) {
      await fs.mkdir(path.dirname(file.path), { recursive: true });
      await fs.writeFile(file.path, file.content, { encoding: "utf8", mode: 0o600 });
    }
    if (platform === "linux") {
      const reload = await exec("systemctl", ["--user", "daemon-reload"]);
      if (!reload.ok) return { installed: false, reason: reload.message || "daemon-reload failed" };
      const enabled = await exec("systemctl", ["--user", "enable", "--now", "usagecairn-sync.timer"]);
      if (!enabled.ok) return { installed: false, reason: enabled.message || "enable failed" };
    } else {
      if (owned.count) await exec("launchctl", ["unload", spec.files[0].path]);
      const loaded = await exec("launchctl", ["load", "-w", spec.files[0].path]);
      if (!loaded.ok) return { installed: false, reason: loaded.message || "launchctl load failed" };
    }
    return { installed: true, intervalMinutes };
  } catch (error) {
    return { installed: false, reason: error.code === "ENOENT" ? "local-runtime-missing" : error.message };
  }
}

async function removeScheduler({ home = os.homedir(), platform = process.platform, deps = {} } = {}) {
  const exec = deps.exec || run;
  const spec = schedulerSpec({ home, platform });
  if (!spec) return { removed: false, reason: "unsupported-platform" };
  try {
    if (platform === "win32") {
      const existing = await exec("schtasks", ["/Query", "/TN", TASK_NAME, "/XML"]);
      if (!existing.ok) return { removed: false };
      if (!isOwnedWindowsTask(existing.stdout, spec)) {
        return { removed: false, reason: "unmanaged-task" };
      }
      const owned = await readOwnedFiles(spec);
      if (owned.conflict) return { removed: false, reason: "unmanaged-file" };
      const result = await exec("schtasks", ["/Delete", "/TN", TASK_NAME, "/F"]);
      if (result.ok) for (const file of spec.files) await fs.rm(file.path, { force: true });
      return { removed: result.ok, reason: result.ok ? undefined : result.message };
    }
    const owned = await readOwnedFiles(spec);
    if (owned.conflict) return { removed: false, reason: "unmanaged-file" };
    if (!owned.count) return { removed: false };
    if (platform === "linux") {
      const result = await exec("systemctl", ["--user", "disable", "--now", "usagecairn-sync.timer"]);
      if (!result.ok) return { removed: false, reason: result.message || "disable failed" };
    } else {
      const result = await exec("launchctl", ["unload", "-w", spec.files[0].path]);
      if (!result.ok) return { removed: false, reason: result.message || "unload failed" };
    }
    for (const file of spec.files) await fs.rm(file.path, { force: true });
    if (platform === "linux") await exec("systemctl", ["--user", "daemon-reload"]);
    return { removed: true };
  } catch (error) {
    return { removed: false, reason: error.message };
  }
}

module.exports = { DEFAULT_INTERVAL_MINUTES, MARKER, schedulerSpec, installScheduler, removeScheduler };
