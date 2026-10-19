const { computeRowCost } = require('./pricing');
const { FIELDS, SESSION_FIELDS } = require('./validation.cjs');

// Fetch one extra row to detect overflow before summarizing: pricing and
// cursor de-duplication both need the complete set of rows.
const MAX_USAGE_ROWS = 50_000;

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

async function usage(pool, url) {
  const { from, to } = dateRange(url);
  const start = `${from}T00:00:00Z`;
  const end = new Date(Date.parse(`${to}T00:00:00Z`) + 86400000).toISOString();
  const result = await pool.query(`SELECT * FROM (
          SELECT source, model, hour_start, ${FIELDS.join(', ')}, total_cost_usd FROM usage_buckets
          WHERE source <> 'trae-cn' AND hour_start >= $1::timestamptz AND hour_start < $2::timestamptz
          UNION ALL
          SELECT source, model, bucket_start AS hour_start, ${SESSION_FIELDS.join(', ')}, 1::bigint AS conversation_count, 0::double precision AS total_cost_usd
          FROM account_session_states WHERE bucket_start >= $1::timestamptz AND bucket_start < $2::timestamptz
          ) AS usage_rows LIMIT $3`, [start, end, MAX_USAGE_ROWS + 1]);
  if (result.rows.length > MAX_USAGE_ROWS) throw new Error(`Invalid usage range: more than ${MAX_USAGE_ROWS} rows; choose a shorter date range`);
  return { from, to, ...summarize(result.rows) };
}

module.exports = { usage, summarize };
