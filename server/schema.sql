-- Apply once to a Vercel-integrated PostgreSQL database (Neon or equivalent).
-- Single-user deployment: authentication is the TOKENTRACKER_ACCESS_KEY
-- environment variable, so there is no accounts table.
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
