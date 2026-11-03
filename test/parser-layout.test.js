"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const dir = path.join(__dirname, "../src/lib/parsers");

test("provider parsers depend only on core, and core on no provider", () => {
  const modules = fs.readdirSync(dir).filter((file) => file.endsWith(".js"));
  assert.ok(modules.includes("core.js"));
  for (const file of modules) {
    const source = fs.readFileSync(path.join(dir, file), "utf8");
    const siblings = [...source.matchAll(/require\(["']\.\/([^"']+)["']\)/g)].map((match) => match[1]);
    const expected = file === "core.js" ? [] : siblings.length ? ["core"] : [];
    assert.deepEqual([...new Set(siblings)], expected, `${file} requires ${siblings.join(", ")}`);
  }
});

test("rollout.js stays a re-export facade", () => {
  const source = fs.readFileSync(path.join(__dirname, "../src/lib/rollout.js"), "utf8");
  assert.doesNotMatch(source, /^\s*(async\s+)?function\s/m);
  const api = require("../src/lib/rollout");
  assert.equal(typeof api.parseRolloutIncremental, "function");
  assert.equal(typeof api.parseClaudeIncremental, "function");
});
