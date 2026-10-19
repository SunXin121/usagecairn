const FIELDS = ['input_tokens', 'output_tokens', 'cached_input_tokens', 'cache_creation_input_tokens', 'reasoning_output_tokens', 'total_tokens', 'conversation_count'];
const SESSION_FIELDS = FIELDS.slice(0, -1);

function count(value, field) {
  if (!Number.isSafeInteger(value ?? 0) || (value ?? 0) < 0) throw new Error(`Invalid ${field}`);
  return value ?? 0;
}

function instant(value, field) {
  if (typeof value !== 'string' || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z$/.test(value) ||
      !Number.isFinite(Date.parse(value)) || new Date(value).toISOString().slice(0, 10) !== value.slice(0, 10)) throw new Error(`Invalid ${field}`);
  return new Date(value).toISOString();
}

function parseBucket(row) {
  if (!row || typeof row !== 'object' || !/^[a-z0-9][a-z0-9_-]{0,63}$/i.test(row.source || '') ||
      typeof row.model !== 'string' || !row.model || row.model.length > 160) throw new Error('Invalid usage bucket');
  const cost = row.total_cost_usd ?? 0;
  if (typeof cost !== 'number' || !Number.isFinite(cost) || cost < 0 || cost > 1_000_000_000) throw new Error('Invalid total_cost_usd');
  return [row.source.toLowerCase(), row.model, instant(row.hour_start, 'hour_start'), ...FIELDS.map((field) => count(row[field], field)), cost];
}

function parseSession(row) {
  if (!row || row.source !== 'trae-cn' || typeof row.session_id !== 'string' || !row.session_id || row.session_id.length > 200 ||
      typeof row.model !== 'string' || !row.model || row.model.length > 160) throw new Error('Invalid session state');
  return [row.source, row.session_id, row.model, instant(row.bucket_start, 'bucket_start'),
    instant(row.snapshot_verified_at, 'snapshot_verified_at'), ...SESSION_FIELDS.map((field) => count(row[field], field))];
}

module.exports = { FIELDS, SESSION_FIELDS, count, instant, parseBucket, parseSession };
