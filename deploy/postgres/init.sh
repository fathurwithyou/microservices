#!/bin/sh
set -eu

export PGPASSWORD="$POSTGRES_PASSWORD"
psql --username "$POSTGRES_USER" --dbname postgres -v ON_ERROR_STOP=1 \
  -v auth_password="$AUTH_DB_PASSWORD" <<'SQL'
CREATE USER auth WITH PASSWORD :'auth_password';
CREATE DATABASE auth_db OWNER auth;
SQL
