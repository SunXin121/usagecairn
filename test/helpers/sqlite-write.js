"use strict";

// Build SQLite fixtures in-process via node:sqlite (Node 22.5+, CI runs Node 24)
// instead of spawning the sqlite3 CLI once per statement. The per-statement
// spawn dominated the wall time of the sqlite-backed parser tests (the copilot
// session-store suite alone spent ~40s in spawnSync). Fixture creation is the
// only thing that changes — the code under test still reads through
// src/lib/sqlite-reader (CLI-first), so parser fidelity is preserved.
//
// node:sqlite emits a one-time ExperimentalWarning on first require; harmless in
// tests (it goes to stderr and does not fail the run). On Node 20, which has no
// node:sqlite, fixtures fall back to the sqlite3 CLI.
const { execFileSync } = require("node:child_process");
let DatabaseSync = null;
try {
  ({ DatabaseSync } = require("node:sqlite"));
} catch (_) {}

// Runs one or more semicolon-separated SQL statements against dbPath, creating
// the database file if it does not exist. Drop-in replacement for a helper that
// shelled out to `sqlite3 <dbPath> <sql>`.
function runSql(dbPath, sql) {
  if (!DatabaseSync) {
    execFileSync("sqlite3", [dbPath, sql], { stdio: ["ignore", "ignore", "pipe"] });
    return;
  }
  const db = new DatabaseSync(dbPath);
  try {
    db.exec(sql);
  } finally {
    db.close();
  }
}

// A `cp`-shaped stand-in for test files whose only child_process use is running
// `sqlite3 <db> <sql>` write statements. Lets those files keep their existing
// `cp.execFileSync("sqlite3", [dbPath, sql])` call sites while routing the work
// through in-process node:sqlite. Throws on any non-sqlite3 command so an
// unexpected spawn is loud rather than silently swallowed.
const sqliteOnlyCp = {
  execFileSync(bin, args) {
    if (bin === "sqlite3") {
      runSql(args[0], args[1]);
      return "";
    }
    throw new Error(`sqlite-write shim: unexpected command "${bin}"`);
  },
};

module.exports = { runSql, sqliteOnlyCp };
