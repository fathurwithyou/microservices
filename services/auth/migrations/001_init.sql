-- Auth Service owns this schema. Tokens are stored only as hashes.
CREATE TABLE IF NOT EXISTS auth_clients (
  client_id TEXT PRIMARY KEY,
  secret_hash BYTEA NOT NULL,
  scopes JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS access_tokens (
  token_hash BYTEA PRIMARY KEY,
  client_id TEXT NOT NULL REFERENCES auth_clients(client_id),
  scopes JSONB NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS access_tokens_expiry_idx ON access_tokens (expires_at);

CREATE TABLE IF NOT EXISTS refresh_tokens (
  token_hash BYTEA PRIMARY KEY,
  client_id TEXT NOT NULL REFERENCES auth_clients(client_id),
  scopes JSONB NOT NULL,
  expires_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS refresh_tokens_client_idx ON refresh_tokens (client_id);
