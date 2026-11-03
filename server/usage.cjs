const { computeRowCost, TIME_PRICED_MODEL_SUBSTRINGS } = require('./pricing');
const { FIELDS, SESSION_FIELDS, PRIORITY_FIELDS, BadRequestError } = require('./validation.cjs');

// Rows are pre-aggregated in SQL (one per source/model/day, hourly only for
// time-of-use priced models), so the budget bounds aggregated rows rather than
// raw device buckets. Fetch one extra row to detect overflow before
// summarizing: a truncated set would silently under-report totals.
const MAX_USAGE_ROWS = 100_000;
const PRIORITY_BASES = ['input_tokens', 'cached_input_tokens', 'cache_creation_input_tokens', 'output_tokens', 'reasoning_output_tokens'];
const LIKE_PATTERNS = TIME_PRICED_MODEL_SUBSTRINGS.map((name) => `%${name.replace(/[\\%_]/g, '\\$&')}%`);

function validTimeZone(tz) {
  if (!/^[A-Za-z0-9_+\-/]{1,64}$/.test(tz)) return false;
  try {
    new Intl.DateTimeFormat('en-US', { timeZone: tz });
    return true;
  } catch { return false; }
}

// Calendar day "today" in the requested zone, so defaults match the viewer.
function zonedDay(ms, tz) {
  return new Intl.DateTimeFormat('en-CA', { timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit' }).format(ms);
}

function queryRange(url) {
  const tz = url.searchParams.get('tz') || 'UTC';
  if (!validTimeZone(tz)) throw new BadRequestError('Invalid time zone');
  const from = url.searchParams.get('from') || zonedDay(Date.now() - 30 * 86400000, tz);
  const to = url.searchParams.get('to') || zonedDay(Date.now(), tz);
  const valid = (day) => /^\d{4}-\d\d-\d\d$/.test(day) && Number.isFinite(Date.parse(`${day}T00:00:00Z`)) &&
    new Date(`${day}T00:00:00Z`).toISOString().slice(0, 10) === day;
  if (!valid(from) || !valid(to) || from > to || Date.parse(to) - Date.parse(from) > 3660 * 86400000) throw new BadRequestError('Invalid date range (maximum 10 years)');
  return { from, to, tz };
}

const sum = (field) => `SUM(${field})::bigint AS ${field}`;
const BUCKET_COLUMNS = [...FIELDS, 'total_cost_usd', ...PRIORITY_FIELDS].join(', ');

// 1. Cursor is account-level data copied to every local device: keep the
//    largest whole snapshot per (model, bucket), never a sum of copies.
// 2. Everything else sums across devices. Cost is linear in tokens for a fixed
//    (source, model) — except time-of-use models, which keep their bucket time,
//    and sources with an authoritative reported cost, where reported and
//    unreported buckets price differently and stay apart.
// 3. Days are calendar days in the viewer's time zone.
const USAGE_SQL = `WITH bounds AS (
    SELECT ($1::date::timestamp AT TIME ZONE $3::text) AS lo, (($2::date + 1)::timestamp AT TIME ZONE $3::text) AS hi
  ), raw AS (
    SELECT b.* FROM usage_buckets b, bounds
    WHERE b.source <> 'trae-cn' AND b.hour_start >= bounds.lo AND b.hour_start < bounds.hi
  ), deduped AS (
    SELECT source, model, hour_start, ${BUCKET_COLUMNS} FROM raw WHERE source <> 'cursor'
    UNION ALL
    (SELECT DISTINCT ON (model, hour_start) source, model, hour_start, ${BUCKET_COLUMNS}
      FROM raw WHERE source = 'cursor' ORDER BY model, hour_start, total_tokens DESC)
    UNION ALL
    SELECT source, model, bucket_start, ${SESSION_FIELDS.join(', ')}, 1::bigint, 0::double precision, ${PRIORITY_FIELDS.map(() => '0::bigint').join(', ')}
    FROM account_session_states, bounds WHERE bucket_start >= bounds.lo AND bucket_start < bounds.hi
  )
  SELECT source, model,
    to_char(hour_start AT TIME ZONE $3::text, 'YYYY-MM-DD') AS day,
    CASE WHEN lower(model) LIKE ANY($4::text[]) THEN hour_start END AS hour_start,
    (total_cost_usd > 0) AS reported,
    ${FIELDS.map(sum).join(', ')},
    SUM(total_cost_usd)::double precision AS total_cost_usd,
    ${PRIORITY_FIELDS.map((field, i) => `SUM(LEAST(${field}, ${PRIORITY_BASES[i]}))::bigint AS ${field}`).join(', ')}
  FROM deduped
  GROUP BY 1, 2, 3, 4, 5
  LIMIT $5`;

const NUMERIC = [...FIELDS, ...PRIORITY_FIELDS, 'total_cost_usd'];
function normalizeRow(row) {
  const out = { ...row };
  for (const field of NUMERIC) out[field] = Number(row[field] || 0);
  if (row.hour_start instanceof Date) out.hour_start = row.hour_start.toISOString();
  return out;
}

// Rows carry a pre-computed calendar `day`; callers passing raw buckets without
// one fall back to the UTC day of hour_start.
function summarize(rows) {
  const daily = new Map();
  const models = new Map();
  const totals = Object.fromEntries([...FIELDS, 'cost_usd'].map((key) => [key, 0]));
  for (const row of rows) {
    const day = row.day || new Date(row.hour_start).toISOString().slice(0, 10);
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
    model.total_tokens += Number(row.total_tokens || 0);
    model.cost_usd += cost;
    daily.set(day, record);
    models.set(key, model);
  }
  return { summary: totals, daily: [...daily.values()].sort((a, b) => a.day.localeCompare(b.day)), models: [...models.values()].sort((a, b) => b.total_tokens - a.total_tokens) };
}

async function usage(pool, url) {
  const { from, to, tz } = queryRange(url);
  let result;
  try {
    result = await pool.query(USAGE_SQL, [from, to, tz, LIKE_PATTERNS, MAX_USAGE_ROWS + 1]);
  } catch (error) {
    // IANA names Node accepts but this PostgreSQL build does not know.
    if (error?.code === '22023' && /time zone/i.test(error.message)) throw new BadRequestError('Invalid time zone');
    throw error;
  }
  if (result.rows.length > MAX_USAGE_ROWS) throw new BadRequestError(`Invalid usage range: more than ${MAX_USAGE_ROWS} rows; choose a shorter date range`);
  return { from, to, tz, ...summarize(result.rows.map(normalizeRow)) };
}

module.exports = { usage, summarize, MAX_USAGE_ROWS };
