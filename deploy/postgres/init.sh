#!/bin/sh
set -eu

export PGPASSWORD="$POSTGRES_PASSWORD"
psql --username "$POSTGRES_USER" --dbname postgres -v ON_ERROR_STOP=1 \
  -v aggregator_password="$AGGREGATOR_DB_PASSWORD" \
  -v auth_password="$AUTH_DB_PASSWORD" <<'SQL'
CREATE USER aggregator WITH PASSWORD :'aggregator_password';
CREATE USER auth WITH PASSWORD :'auth_password';
CREATE DATABASE aggregator_db OWNER aggregator;
CREATE DATABASE auth_db OWNER auth;
SQL
