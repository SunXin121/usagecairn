# Usage parsers

Each supported agent has one module here that discovers its local install and
turns its logs/databases into hourly usage buckets appended to `queue.jsonl`.

- `core.js` holds everything used by more than one provider: bucket keys and
  totals math, cursor bookkeeping, queue writes, project attribution, shared
  JSONL/SQLite helpers, and the generic engines that several agents reuse
  (for example `parsePiLikeIncremental` for pi-shaped session logs).
- `<provider>.js` holds code used only by that provider. Related products that
  share a format live together: `opencode.js` also covers MiMo and Z Code,
  `codebuddy.js` covers WorkBuddy, `omp.js` covers OmO, `pi.js` covers Pi
  Desktop and Prime Agent, `kiro.js` covers Kiro CLI, `copilot.js` covers the
  CLI, session store and app database, and `trae.js` covers TRAE Work CN.
- `../rollout.js` re-exports every public function, so callers (`sync.js`,
  `status.js`, tests) keep a single import.

Dependency rule: a provider module may require `core.js` and `src/lib/*`, never
another provider module, and `core.js` never requires a provider. When code
starts being shared by a second provider, move it into `core.js`.
