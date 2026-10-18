const { createHash, timingSafeEqual } = require('node:crypto');
const { computeRowCost } = require('../src/lib/pricing');
const FIELDS = ['input_tokens', 'output_tokens', 'cached_input_tokens', 'cache_creation_input_tokens', 'reasoning_output_tokens', 'total_tokens', 'conversation_count'];
const SESSION_FIELDS = FIELDS.slice(0, -1);

function count(value, field) {
  if (!Number.isSafeInteger(value ?? 0) || (value ?? 0) < 0) throw new Error(`Invalid ${field}`);
  return value ?? 0;
}
function instant(value, field) {
  if (typeof value !== 'string' || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z$/.test(value) || !Number.isFinite(Date.parse(value))) throw new Error(`Invalid ${field}`);
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
function send(res, status, value) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.end(JSON.stringify(value));
}
// Single-user deployment: the one expected key comes from the environment, not
// from a database accounts table. Any non-empty key is accepted; compare
// SHA-256 digests with timingSafeEqual.
function expectedKeyDigest() {
  const key = process.env.ACCESS_KEY;
  if (typeof key !== 'string' || key.trim() === '') return null;
  return createHash('sha256').update(key).digest();
}
function authorized(req) {
  const digest = expectedKeyDigest();
  if (!digest) return false;
  const token = /^Bearer (.+)$/.exec(req.headers.authorization || '')?.[1];
  if (!token) return false;
  return timingSafeEqual(createHash('sha256').update(token).digest(), digest);
}
function dateRange(url) {
  const from = url.searchParams.get('from') || new Date(Date.now() - 30 * 86400000).toISOString().slice(0, 10);
  const to = url.searchParams.get('to') || new Date().toISOString().slice(0, 10);
  const valid = (day) => /^\d{4}-\d\d-\d\d$/.test(day) && Number.isFinite(Date.parse(`${day}T00:00:00Z`)) &&
    new Date(`${day}T00:00:00Z`).toISOString().slice(0, 10) === day;
  if (!valid(from) || !valid(to) || from > to || Date.parse(to) - Date.parse(from) > 3660 * 86400000) throw new Error('Invalid date range (maximum 10 years)');
  return { from, to };
}
function summarize(rows) {
  const daily = new Map();
  const models = new Map();
  const totals = Object.fromEntries([...FIELDS, 'cost_usd'].map((key) => [key, 0]));
  // Cursor is account-level data copied to every local device. Pick the largest
  // whole snapshot, never sum copies from distinct devices.
  const cursor = new Map();
  const counted = [];
  for (const row of rows) {
    if (row.source !== 'cursor') { counted.push(row); continue; }
    const key = `${row.source}\0${row.model}\0${new Date(row.hour_start).toISOString()}`;
    if (!cursor.has(key) || Number(row.total_tokens) > Number(cursor.get(key).total_tokens)) cursor.set(key, row);
  }
  counted.push(...cursor.values());
  for (const row of counted) {
    const day = new Date(row.hour_start).toISOString().slice(0, 10);
    const key = `${row.source}\0${row.model}`;
    const cost = computeRowCost(row);
    const record = daily.get(day) || { day, ...Object.fromEntries([...FIELDS, 'cost_usd'].map((field) => [field, 0])) };
    const model = models.get(key) || { source: row.source, model: row.model, total_tokens: 0, cost_usd: 0 };
    for (const field of FIELDS) {
      const value = Number(row[field] || 0);
      record[field] += value;
      totals[field] += value;
    }
    record.cost_usd += cost;
    totals.cost_usd += cost;
    model.total_tokens += Number(row.total_tokens);
    model.cost_usd += cost;
    daily.set(day, record);
    models.set(key, model);
  }
  return { summary: totals, daily: [...daily.values()].sort((a, b) => a.day.localeCompare(b.day)), models: [...models.values()].sort((a, b) => b.total_tokens - a.total_tokens) };
}
function createHandler(pool) {
  return async function handler(req, res) {
    try {
      if (!expectedKeyDigest()) {
        console.error('[api] ACCESS_KEY is missing or empty');
        return send(res, 503, { error: 'Server misconfigured' });
      }
      if (!authorized(req)) return send(res, 401, { error: 'Access key required' });
      const route = new URL(req.url, 'http://localhost').pathname;
      if (route === '/api/ingest' && req.method === 'POST') {
        if (Number(req.headers['content-length']) > 1024 * 1024) throw new Error('Payload too large');
        let body = req.body;
        if (typeof body === 'string') body = JSON.parse(body);
        if (!body && req[Symbol.asyncIterator]) {
          let text = '';
          for await (const chunk of req) {
            text += chunk;
            if (text.length > 1024 * 1024) throw new Error('Payload too large');
          }
          body = JSON.parse(text);
        }
        const states = body?.account_session_states;
        if (!body || !/^[a-zA-Z0-9_-]{8,100}$/.test(body.device_id || '') ||
            !Array.isArray(body.hourly) || (states != null && !Array.isArray(states)) ||
            body.hourly.length + (states?.length || 0) > 200) throw new Error('Invalid upload');
        const buckets = body.hourly.map(parseBucket);
        const sessionRows = (states || []).map(parseSession);
        // New collectors send a (file mtime, byte offset) stamp so a delayed
        // replay cannot overwrite newer rows. Clients that omit both keep the
        // legacy last-writer-wins behaviour instead of freezing on epoch.
        const guarded = body.snapshot_at != null || body.snapshot_revision != null;
        const snapshotAt = body.snapshot_at == null ? '1970-01-01T00:00:00.000Z' : instant(body.snapshot_at, 'snapshot_at');
        const revision = count(body.snapshot_revision, 'snapshot_revision');
        const client = await pool.connect();
        try {
          await client.query('BEGIN');
          for (const [source, model, time, ...values] of buckets) {
            const columns = ['device_id', 'source', 'model', 'hour_start', ...FIELDS, 'total_cost_usd', 'snapshot_at', 'snapshot_revision'];
            await client.query(`INSERT INTO usage_buckets (${columns.join(', ')})
              VALUES (${columns.map((_, i) => `$${i + 1}`).join(', ')})
              ON CONFLICT (device_id, source, model, hour_start) DO UPDATE SET
              ${[...FIELDS, 'total_cost_usd', 'snapshot_at', 'snapshot_revision'].map((field) => `${field} = EXCLUDED.${field}`).join(', ')}${guarded
                ? '\n              WHERE (EXCLUDED.snapshot_at, EXCLUDED.snapshot_revision) > (usage_buckets.snapshot_at, usage_buckets.snapshot_revision)'
                : ''}`,
            [body.device_id, source, model, time, ...values, snapshotAt, revision]);
          }
          for (const [source, sessionId, model, bucketStart, verifiedAt, ...values] of sessionRows) {
            const columns = ['source', 'session_id', 'model', 'bucket_start', 'snapshot_verified_at', ...SESSION_FIELDS];
            await client.query(`INSERT INTO account_session_states (${columns.join(', ')})
              VALUES (${columns.map((_, i) => `$${i + 1}`).join(', ')})
              ON CONFLICT (source, session_id) DO UPDATE SET
              ${['model', 'bucket_start', 'snapshot_verified_at', ...SESSION_FIELDS].map((field) => `${field} = EXCLUDED.${field}`).join(', ')}
              WHERE EXCLUDED.snapshot_verified_at > account_session_states.snapshot_verified_at`,
            [source, sessionId, model, bucketStart, verifiedAt, ...values]);
          }
          await client.query('COMMIT');
        } catch (error) { await client.query('ROLLBACK'); throw error; }
        finally { client.release(); }
        return send(res, 200, { inserted: buckets.length + sessionRows.length, skipped: 0 });
      }
      if (route === '/api/usage' && req.method === 'GET') {
        const { from, to } = dateRange(new URL(req.url, 'http://localhost'));
        const start = `${from}T00:00:00Z`;
        const end = new Date(Date.parse(`${to}T00:00:00Z`) + 86400000).toISOString();
        const result = await pool.query(`SELECT source, model, hour_start, ${FIELDS.join(', ')}, total_cost_usd FROM usage_buckets
          WHERE source <> 'trae-cn' AND hour_start >= $1::timestamptz AND hour_start < $2::timestamptz
          UNION ALL
          SELECT source, model, bucket_start AS hour_start, ${SESSION_FIELDS.join(', ')}, 1::bigint AS conversation_count, 0::double precision AS total_cost_usd
          FROM account_session_states WHERE bucket_start >= $1::timestamptz AND bucket_start < $2::timestamptz`, [start, end]);
        return send(res, 200, { from, to, ...summarize(result.rows) });
      }
      return send(res, 404, { error: 'Not found' });
    } catch (error) {
      if (/^(Invalid|Payload too large)/.test(error.message) || error instanceof SyntaxError) return send(res, 400, { error: error.message });
      console.error('[api] request failed:', error.message);
      return send(res, 500, { error: 'Server error' });
    }
  };
}
module.exports = { createHandler, summarize, parseBucket };
