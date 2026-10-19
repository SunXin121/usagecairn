"use strict";

const fs = require("node:fs");
const { physicalJsonlRecords } = require("./jsonl-lines");

// A legacy line-only cursor is scanned once to establish a byte offset. Later
// runs read only appended bytes, even when SQLite is the active source.
async function readKiroJsonlTokens(filePath, previous = {}, { countOnly = false } = {}) {
  const stat = await fs.promises.stat(filePath);
  const lastLine = Number.isSafeInteger(previous.lastLine) && previous.lastLine >= 0 ? previous.lastLine : 0;
  const oldOffset = Number.isSafeInteger(previous.byteOffset) && previous.byteOffset >= 0
    ? previous.byteOffset : null;
  const replaced = oldOffset !== null &&
    (oldOffset > stat.size || (previous.dev != null && previous.dev !== stat.dev) ||
      (previous.ino != null && previous.ino !== stat.ino) ||
      (oldOffset === stat.size && previous.mtimeMs != null && previous.mtimeMs !== stat.mtimeMs));
  const start = oldOffset !== null && !replaced ? oldOffset : 0;
  const rows = [];
  let lineCount = start > 0 ? lastLine : 0;
  let byteOffset = start;
  const timestamp = stat.mtime.toISOString().replace("T", " ").replace("Z", "").slice(0, 19);
  if (start < stat.size) {
    const input = fs.createReadStream(filePath, { start, end: stat.size - 1 });
    for await (const record of physicalJsonlRecords(input, { invalidUtf8: "record" })) {
      byteOffset += record.physicalBytes;
      if (!record.line?.trim()) continue;
      lineCount++;
      if (countOnly || replaced || lineCount <= lastLine) continue;
      try {
        const obj = JSON.parse(record.line);
        rows.push({
          id: lineCount,
          model: obj.model || "agent",
          provider: obj.provider || "kiro",
          tokens_prompt: obj.promptTokens || 0,
          tokens_generated: obj.generatedTokens || 0,
          timestamp,
        });
      } catch (_e) { /* skip malformed lines, but advance past them */ }
    }
  }
  const reset = replaced || (start === 0 && lastLine > lineCount);
  return {
    rows: reset ? [] : rows,
    lineCount,
    reset,
    cursor: { lastLine: lineCount, byteOffset, dev: stat.dev, ino: stat.ino, mtimeMs: stat.mtimeMs },
  };
}

module.exports = { readKiroJsonlTokens };
