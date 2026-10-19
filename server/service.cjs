const { createHash, timingSafeEqual } = require('node:crypto');
const { ingest } = require('./ingest.cjs');
const { usage, summarize } = require('./usage.cjs');
const { parseBucket } = require('./validation.cjs');

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

function createHandler(pool) {
  return async function handler(req, res) {
    try {
      if (!expectedKeyDigest()) {
        console.error('[api] ACCESS_KEY is missing or empty');
        return send(res, 503, { error: 'Server misconfigured' });
      }
      if (!authorized(req)) return send(res, 401, { error: 'Access key required' });
      const url = new URL(req.url, 'http://localhost');
      if (url.pathname === '/api/ingest' && req.method === 'POST') {
        return send(res, 200, await ingest(pool, req));
      }
      if (url.pathname === '/api/usage' && req.method === 'GET') {
        return send(res, 200, await usage(pool, url));
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
