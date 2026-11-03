const { createHash, timingSafeEqual } = require('node:crypto');
const { ingest } = require('./ingest.cjs');
const { usage, summarize } = require('./usage.cjs');
const { parseBucket, BadRequestError } = require('./validation.cjs');
const { ensureSchema } = require('./migrate.cjs');

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

// `migrate` applies server/schema.sql once per warm instance before touching
// tables (the deployed API enables it; tests that inject a fake pool do not).
function createHandler(pool, { migrate = false } = {}) {
  return async function handler(req, res) {
    try {
      if (!expectedKeyDigest()) {
        console.error('[api] ACCESS_KEY is missing or empty');
        return send(res, 503, { error: 'Server misconfigured' });
      }
      if (!authorized(req)) return send(res, 401, { error: 'Access key required' });
      const url = new URL(req.url, 'http://localhost');
      const route = url.pathname === '/api/ingest' && req.method === 'POST' ? ingest
        : url.pathname === '/api/usage' && req.method === 'GET' ? usage : null;
      if (!route) return send(res, 404, { error: 'Not found' });
      if (migrate) {
        try { await ensureSchema(pool); } catch (error) {
          console.error('[api] schema migration failed; apply server/schema.sql manually:', error.message);
          return send(res, 500, { error: 'Server error' });
        }
      }
      return send(res, 200, await route(pool, route === ingest ? req : url));
    } catch (error) {
      if (error instanceof BadRequestError || error instanceof SyntaxError) return send(res, 400, { error: error.message });
      console.error('[api] request failed:', error.message);
      return send(res, 500, { error: 'Server error' });
    }
  };
}
module.exports = { createHandler, summarize, parseBucket };
