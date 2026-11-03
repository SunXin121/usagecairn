const { FIELDS, SESSION_FIELDS, PRIORITY_FIELDS, BadRequestError, count, instant, parseBucket, parseSession } = require('./validation.cjs');

const MAX_BODY_BYTES = 1024 * 1024;
const MAX_RECORDS = 200;

// Column layout of parseBucket()/parseSession() tuples, with the Postgres array
// type each column is sent as. One INSERT … SELECT FROM unnest(…) writes a whole
// request in a single round trip instead of one statement per row.
const BUCKET_TUPLE = [['source', 'text'], ['model', 'text'], ['hour_start', 'timestamptz'],
  ...FIELDS.map((field) => [field, 'bigint']), ['total_cost_usd', 'float8'], ...PRIORITY_FIELDS.map((field) => [field, 'bigint'])];
const SESSION_TUPLE = [['source', 'text'], ['session_id', 'text'], ['model', 'text'], ['bucket_start', 'timestamptz'],
  ['snapshot_verified_at', 'timestamptz'], ...SESSION_FIELDS.map((field) => [field, 'bigint'])];
const BUCKET_UPDATED = [...FIELDS, 'total_cost_usd', ...PRIORITY_FIELDS, 'snapshot_at', 'snapshot_revision'];

function unnest(tuple, firstParam) {
  return `unnest(${tuple.map(([, type], i) => `$${firstParam + i}::${type}[]`).join(', ')}) AS u(${tuple.map(([name]) => name).join(', ')})`;
}

function bucketUpsert(guarded) {
  const columns = BUCKET_TUPLE.map(([name]) => name);
  return `INSERT INTO usage_buckets (device_id, ${columns.join(', ')}, snapshot_at, snapshot_revision)
              SELECT $1, ${columns.map((name) => `u.${name}`).join(', ')}, $2::timestamptz, $3::bigint
              FROM ${unnest(BUCKET_TUPLE, 4)}
              ON CONFLICT (device_id, source, model, hour_start) DO UPDATE SET
              ${BUCKET_UPDATED.map((field) => `${field} = EXCLUDED.${field}`).join(', ')}${guarded
                ? '\n              WHERE (EXCLUDED.snapshot_at, EXCLUDED.snapshot_revision) > (usage_buckets.snapshot_at, usage_buckets.snapshot_revision)'
                : "\n              WHERE usage_buckets.snapshot_at = '-infinity'::timestamptz OR (usage_buckets.snapshot_at = '1970-01-01T00:00:00Z'::timestamptz AND usage_buckets.snapshot_revision = 0)"}
              RETURNING 1`;
}

const SESSION_UPSERT = `INSERT INTO account_session_states (${SESSION_TUPLE.map(([name]) => name).join(', ')})
              SELECT ${SESSION_TUPLE.map(([name]) => `u.${name}`).join(', ')} FROM ${unnest(SESSION_TUPLE, 1)}
              ON CONFLICT (source, session_id) DO UPDATE SET
              ${['model', 'bucket_start', 'snapshot_verified_at', ...SESSION_FIELDS].map((field) => `${field} = EXCLUDED.${field}`).join(', ')}
              WHERE EXCLUDED.snapshot_verified_at > account_session_states.snapshot_verified_at
              RETURNING 1`;

// A single INSERT cannot touch the same conflict key twice, so collapse
// duplicates first, keeping the row a statement-per-row loop would have left:
// stamped uploads never replace an equal stamp (first wins), legacy uploads
// are last-writer-wins, and session states keep the strictly newest fetch.
function dedupeBuckets(buckets, guarded) {
  const byKey = new Map();
  for (const tuple of buckets) {
    const key = JSON.stringify(tuple.slice(0, 3));
    if (guarded && byKey.has(key)) continue;
    byKey.set(key, tuple);
  }
  return [...byKey.values()];
}

function dedupeSessions(sessions) {
  const byKey = new Map();
  for (const tuple of sessions) {
    const key = JSON.stringify(tuple.slice(0, 2));
    const previous = byKey.get(key);
    if (!previous || tuple[4] > previous[4]) byKey.set(key, tuple);
  }
  return [...byKey.values()];
}

const columnsOf = (tuples, width) => Array.from({ length: width }, (_, i) => tuples.map((tuple) => tuple[i]));
const rowCount = (result) => result?.rows?.length ?? 0;

async function readBody(req) {
  if (Number(req.headers['content-length']) > MAX_BODY_BYTES) throw new BadRequestError('Payload too large');
  let body = req.body;
  if (body != null) {
    const raw = typeof body === 'string' || Buffer.isBuffer(body) ? body : JSON.stringify(body);
    if (Buffer.byteLength(raw) > MAX_BODY_BYTES) throw new BadRequestError('Payload too large');
    if (typeof body === 'string' || Buffer.isBuffer(body)) body = JSON.parse(raw.toString());
  }
  if (!body && req[Symbol.asyncIterator]) {
    const chunks = [];
    let bytes = 0;
    for await (const chunk of req) {
      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      bytes += buffer.length;
      if (bytes > MAX_BODY_BYTES) throw new BadRequestError('Payload too large');
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
      body.hourly.length + (states?.length || 0) > MAX_RECORDS) throw new BadRequestError('Invalid upload');
  // Legacy uploads keep last-writer-wins among legacy rows, but must not
  // replace a row carrying a newer collector's snapshot stamp.
  const guarded = body.snapshot_at != null || body.snapshot_revision != null;
  const buckets = dedupeBuckets(body.hourly.map(parseBucket), guarded);
  const sessionRows = dedupeSessions((states || []).map(parseSession));
  const snapshotAt = body.snapshot_at == null ? (guarded ? '1970-01-01T00:00:00.000Z' : '-infinity') : instant(body.snapshot_at, 'snapshot_at');
  const revision = count(body.snapshot_revision, 'snapshot_revision');
  const received = body.hourly.length + (states?.length || 0);
  // max_records tells collectors this server writes a request in one
  // statement, so they can grow from the legacy 20-row batches.
  if (!buckets.length && !sessionRows.length) return { inserted: 0, skipped: received, max_records: MAX_RECORDS };
  let written = 0;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    if (buckets.length) {
      written += rowCount(await client.query(bucketUpsert(guarded),
        [body.device_id, snapshotAt, revision, ...columnsOf(buckets, BUCKET_TUPLE.length)]));
    }
    if (sessionRows.length) {
      written += rowCount(await client.query(SESSION_UPSERT, columnsOf(sessionRows, SESSION_TUPLE.length)));
    }
    await client.query('COMMIT');
  } catch (error) {
    // Report the original failure, not a secondary rollback error.
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally { client.release(); }
  // `inserted` counts rows actually written; stale snapshots and in-request
  // duplicates rejected by the guards are `skipped`.
  return { inserted: written, skipped: received - written, max_records: MAX_RECORDS };
}

module.exports = { ingest, MAX_RECORDS };
