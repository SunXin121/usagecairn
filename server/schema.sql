-- UsageCairn schema for a Vercel-integrated PostgreSQL database (Neon or equivalent).
-- Single-user deployment: authentication is the ACCESS_KEY
-- environment variable, so there is no accounts table.
--
-- Every statement is idempotent. The API applies this file automatically on
-- its first request when usagecairn_schema.version is older than the version
-- recorded at the bottom; running it by hand (psql -f) is equivalent and is
-- only needed when the database role cannot run DDL.
CREATE TABLE IF NOT EXISTS usage_buckets (
  device_id text NOT NULL,
  source text NOT NULL,
  model text NOT NULL,
  hour_start timestamptz NOT NULL,
  input_tokens bigint NOT NULL DEFAULT 0,
  output_tokens bigint NOT NULL DEFAULT 0,
  cached_input_tokens bigint NOT NULL DEFAULT 0,
  cache_creation_input_tokens bigint NOT NULL DEFAULT 0,
  reasoning_output_tokens bigint NOT NULL DEFAULT 0,
  total_tokens bigint NOT NULL DEFAULT 0,
  conversation_count bigint NOT NULL DEFAULT 0,
  total_cost_usd double precision NOT NULL DEFAULT 0,
  snapshot_at timestamptz NOT NULL DEFAULT '-infinity',
  snapshot_revision bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (device_id, source, model, hour_start)
);
CREATE INDEX IF NOT EXISTS usage_buckets_time ON usage_buckets (hour_start DESC);
CREATE TABLE IF NOT EXISTS account_session_states (
  source text NOT NULL,
  session_id text NOT NULL,
  model text NOT NULL,
  bucket_start timestamptz NOT NULL,
  snapshot_verified_at timestamptz NOT NULL,
  input_tokens bigint NOT NULL DEFAULT 0,
  output_tokens bigint NOT NULL DEFAULT 0,
  cached_input_tokens bigint NOT NULL DEFAULT 0,
  cache_creation_input_tokens bigint NOT NULL DEFAULT 0,
  reasoning_output_tokens bigint NOT NULL DEFAULT 0,
  total_tokens bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (source, session_id)
);
CREATE INDEX IF NOT EXISTS account_session_states_time ON account_session_states (bucket_start DESC);

-- Version 2: observed priority-tier subsets of the base token columns, used to
-- price Fast/priority requests. Annotations only, never part of total_tokens.
ALTER TABLE usage_buckets ADD COLUMN IF NOT EXISTS priority_input_tokens bigint NOT NULL DEFAULT 0;
ALTER TABLE usage_buckets ADD COLUMN IF NOT EXISTS priority_cached_input_tokens bigint NOT NULL DEFAULT 0;
ALTER TABLE usage_buckets ADD COLUMN IF NOT EXISTS priority_cache_creation_input_tokens bigint NOT NULL DEFAULT 0;
ALTER TABLE usage_buckets ADD COLUMN IF NOT EXISTS priority_output_tokens bigint NOT NULL DEFAULT 0;
ALTER TABLE usage_buckets ADD COLUMN IF NOT EXISTS priority_reasoning_output_tokens bigint NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS usagecairn_schema (
  id smallint PRIMARY KEY CHECK (id = 1),
  version integer NOT NULL
);
INSERT INTO usagecairn_schema (id, version) VALUES (1, 2)
  ON CONFLICT (id) DO UPDATE SET version = GREATEST(usagecairn_schema.version, EXCLUDED.version);
