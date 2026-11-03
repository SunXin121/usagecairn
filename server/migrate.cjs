const fs = require('node:fs');
const path = require('node:path');

// Must match the version that schema.sql records in usagecairn_schema.
const SCHEMA_VERSION = 2;
const SCHEMA_PATH = path.join(__dirname, 'schema.sql');
// Arbitrary constant shared by every instance so concurrent cold starts apply
// the schema one at a time.
const MIGRATION_LOCK_KEY = 0x75636e;

// schema.sql has no functions or quoted semicolons, so statements split cleanly
// on a terminating semicolon. Single statements also keep it compatible with
// drivers that reject multi-statement queries.
function schemaStatements(sql = fs.readFileSync(SCHEMA_PATH, 'utf8')) {
  return sql.split('\n').filter((line) => !line.trimStart().startsWith('--')).join('\n')
    .split(/;\s*(?:\n|$)/).map((statement) => statement.trim()).filter(Boolean);
}

async function currentVersion(client) {
  const table = await client.query("SELECT to_regclass('usagecairn_schema') IS NOT NULL AS present");
  if (!table.rows[0]?.present) return 0;
  const result = await client.query('SELECT version FROM usagecairn_schema WHERE id = 1');
  return Number(result.rows[0]?.version) || 0;
}

async function migrate(pool) {
  const client = await pool.connect();
  try {
    if (await currentVersion(client) >= SCHEMA_VERSION) return { migrated: false };
    await client.query('BEGIN');
    try {
      await client.query('SELECT pg_advisory_xact_lock($1)', [MIGRATION_LOCK_KEY]);
      if (await currentVersion(client) < SCHEMA_VERSION) {
        for (const statement of schemaStatements()) await client.query(statement);
      }
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    }
    return { migrated: true };
  } finally { client.release(); }
}

// One attempt per warm instance; a failure is retried on the next request.
const pending = new WeakMap();
function ensureSchema(pool) {
  let promise = pending.get(pool);
  if (!promise) {
    promise = migrate(pool).catch((error) => {
      pending.delete(pool);
      throw error;
    });
    pending.set(pool, promise);
  }
  return promise;
}

module.exports = { SCHEMA_VERSION, ensureSchema, migrate, schemaStatements };
