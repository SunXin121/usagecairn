const { Pool } = require('pg');
const { createHandler } = require('../server/service.cjs');
const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 2 });
module.exports = createHandler(pool);
