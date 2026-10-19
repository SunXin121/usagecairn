const { FIELDS, SESSION_FIELDS, count, instant, parseBucket, parseSession } = require('./validation.cjs');

const BUCKET_COLUMNS = ['device_id', 'source', 'model', 'hour_start', ...FIELDS, 'total_cost_usd', 'snapshot_at', 'snapshot_revision'];
const SESSION_COLUMNS = ['source', 'session_id', 'model', 'bucket_start', 'snapshot_verified_at', ...SESSION_FIELDS];
const SESSION_UPSERT = `INSERT INTO account_session_states (${SESSION_COLUMNS.join(', ')})
              VALUES (${SESSION_COLUMNS.map((_, i) => `$${i + 1}`).join(', ')})
              ON CONFLICT (source, session_id) DO UPDATE SET
              ${['model', 'bucket_start', 'snapshot_verified_at', ...SESSION_FIELDS].map((field) => `${field} = EXCLUDED.${field}`).join(', ')}
              WHERE EXCLUDED.snapshot_verified_at > account_session_states.snapshot_verified_at`;

function bucketUpsert(guarded) {
  return `INSERT INTO usage_buckets (${BUCKET_COLUMNS.join(', ')})
              VALUES (${BUCKET_COLUMNS.map((_, i) => `$${i + 1}`).join(', ')})
              ON CONFLICT (device_id, source, model, hour_start) DO UPDATE SET
              ${[...FIELDS, 'total_cost_usd', 'snapshot_at', 'snapshot_revision'].map((field) => `${field} = EXCLUDED.${field}`).join(', ')}${guarded
                ? '\n              WHERE (EXCLUDED.snapshot_at, EXCLUDED.snapshot_revision) > (usage_buckets.snapshot_at, usage_buckets.snapshot_revision)'
                : "\n              WHERE usage_buckets.snapshot_at = '-infinity'::timestamptz OR (usage_buckets.snapshot_at = '1970-01-01T00:00:00Z'::timestamptz AND usage_buckets.snapshot_revision = 0)"}`;
}

const MAX_BODY_BYTES = 1024 * 1024;

async function readBody(req) {
  if (Number(req.headers['content-length']) > MAX_BODY_BYTES) throw new Error('Payload too large');
  let body = req.body;
  if (body != null) {
    const raw = typeof body === 'string' || Buffer.isBuffer(body) ? body : JSON.stringify(body);
    if (Buffer.byteLength(raw) > MAX_BODY_BYTES) throw new Error('Payload too large');
    if (typeof body === 'string' || Buffer.isBuffer(body)) body = JSON.parse(raw.toString());
  }
  if (!body && req[Symbol.asyncIterator]) {
    const chunks = [];
    let bytes = 0;
    for await (const chunk of req) {
      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      bytes += buffer.length;
      if (bytes > MAX_BODY_BYTES) throw new Error('Payload too large');
      chunks.push(buffer);
    }
    body = JSON.parse(Buffer.concat(chunks).toString('utf8'));
  }
  return body;
}

async function ingest(pool, req) {
  const body = await readBody(req);
  const states = body?.account_session_states;
  if (!body || !/^[a-zA-Z0-9_-]{8,100}$/.test(body.device_id || '') ||
      !Array.isArray(body.hourly) || (states != null && !Array.isArray(states)) ||
      body.hourly.length + (states?.length || 0) > 200) throw new Error('Invalid upload');
  const buckets = body.hourly.map(parseBucket);
  const sessionRows = (states || []).map(parseSession);
  // Legacy uploads keep last-writer-wins among legacy rows, but must not
  // replace a row carrying a newer collector's snapshot stamp.
  const guarded = body.snapshot_at != null || body.snapshot_revision != null;
  const snapshotAt = body.snapshot_at == null ? (guarded ? '1970-01-01T00:00:00.000Z' : '-infinity') : instant(body.snapshot_at, 'snapshot_at');
  const revision = count(body.snapshot_revision, 'snapshot_revision');
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const [source, model, time, ...values] of buckets) {
      await client.query(bucketUpsert(guarded),
        [body.device_id, source, model, time, ...values, snapshotAt, revision]);
    }
    for (const [source, sessionId, model, bucketStart, verifiedAt, ...values] of sessionRows) {
      await client.query(SESSION_UPSERT,
        [source, sessionId, model, bucketStart, verifiedAt, ...values]);
    }
    await client.query('COMMIT');
  } catch (error) { await client.query('ROLLBACK'); throw error; }
  finally { client.release(); }
  return { inserted: buckets.length + sessionRows.length, skipped: 0 };
}

module.exports = { ingest };
