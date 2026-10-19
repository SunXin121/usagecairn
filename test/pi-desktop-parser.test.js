"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { test } = require("node:test");
const { resolvePiDesktopSessionFiles, parsePiDesktopIncremental } = require("../src/lib/rollout");

function usageLine(id, createdAt, usage = {}) {
  return JSON.stringify({
    type: "message", id, role: "assistant", createdAt,
    blocks: [{ type: "text", text: "private response must not enter queue" }],
    meta: {
      modelId: "gpt-test", providerId: "local-provider",
      usage: { inputTokens: 10, outputTokens: 5, cacheReadTokens: 20,
        cacheWriteTokens: 3, reasoningTokens: 2, totalTokens: 38, ...usage },
    },
  });
}

async function rows(file) {
  return (await fs.readFile(file, "utf8")).trim().split("\n").map(JSON.parse);
}

test("PI-Desktop discovers only flat session logs (not revisions), including when pi CLI is absent", async () => {
  const home = await fs.mkdtemp(path.join(os.tmpdir(), "tt-pi-desktop-"));
  try {
    const dir = path.join(home, ".pi-desktop", "sessions");
    await fs.mkdir(path.join(dir, "nested"), { recursive: true });
    await Promise.all([
      fs.writeFile(path.join(dir, "a.jsonl"), ""),
      fs.writeFile(path.join(dir, "a.revisions.jsonl"), ""),
      fs.writeFile(path.join(dir, "nested", "b.jsonl"), ""),
    ]);
    assert.deepEqual(resolvePiDesktopSessionFiles({ HOME: home }), [path.join(dir, "a.jsonl")]);
    assert.deepEqual(resolvePiDesktopSessionFiles({ HOME: path.join(home, "absent") }), []);
  } finally {
    await fs.rm(home, { recursive: true, force: true });
  }
});

test("PI-Desktop collects metadata only, deduplicates runs and appends incremental usage", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "tt-pi-desktop-"));
  try {
    const file = path.join(dir, "session.jsonl");
    const queuePath = path.join(dir, "queue.jsonl");
    const cursors = {};
    await fs.writeFile(file, [
      JSON.stringify({ type: "session", sessionId: "session" }),
      JSON.stringify({ type: "message", id: "user", role: "user", meta: { usage: { inputTokens: 999 } } }),
      usageLine("first", "2026-10-18T10:05:00.000Z"),
      JSON.stringify({ type: "message", id: "error", role: "assistant", meta: { status: "error" } }),
    ].join("\n") + "\n");
    const options = { sessionFiles: [file], cursors, queuePath };
    const first = await parsePiDesktopIncremental(options);
    assert.equal(first.eventsAggregated, 1);
    assert.equal(cursors.piDesktop.seenIds.length, 1);
    assert.equal(cursors.pi, undefined);
    assert.deepEqual((await rows(queuePath)).map(({ source, model, input_tokens,
      cached_input_tokens, cache_creation_input_tokens, output_tokens,
      reasoning_output_tokens, total_tokens, conversation_count, hour_start }) => ({
      source, model, input_tokens, cached_input_tokens, cache_creation_input_tokens,
      output_tokens, reasoning_output_tokens, total_tokens, conversation_count, hour_start,
    })), [{
      source: "pi-desktop", model: "gpt-test", input_tokens: 10,
      cached_input_tokens: 20, cache_creation_input_tokens: 3,
      output_tokens: 5, reasoning_output_tokens: 2, total_tokens: 38,
      conversation_count: 1, hour_start: "2026-10-18T10:00:00.000Z",
    }]);
    assert.doesNotMatch(await fs.readFile(queuePath, "utf8"), /private response/);
    assert.equal((await parsePiDesktopIncremental(options)).eventsAggregated, 0);
    await fs.appendFile(file, usageLine("second", "2026-10-18T10:15:00.000Z", {
      inputTokens: 2, outputTokens: 1, cacheReadTokens: 0,
      cacheWriteTokens: 0, reasoningTokens: 0, totalTokens: 3,
    }) + "\n");
    const next = await parsePiDesktopIncremental(options);
    assert.equal(next.eventsAggregated, 1);
    assert.equal((await rows(queuePath)).at(-1).total_tokens, 41);
    assert.equal((await rows(queuePath)).at(-1).conversation_count, 2);
    await fs.appendFile(file, usageLine("fallback", "2026-10-18T10:20:00.000Z", {
      inputTokens: 2, outputTokens: 3, cacheReadTokens: 4,
      cacheWriteTokens: 0, reasoningTokens: 2, totalTokens: undefined,
    }) + "\n");
    assert.equal((await parsePiDesktopIncremental(options)).eventsAggregated, 1);
    assert.equal((await rows(queuePath)).at(-1).total_tokens, 50); // +9, not +11
  } finally {
    await fs.rm(dir, { recursive: true, force: true });
  }
});
