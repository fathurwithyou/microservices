#!/bin/sh
set -eu

export PGPASSWORD="$POSTGRES_PASSWORD"
psql --username "$POSTGRES_USER" --dbname auth_db -v ON_ERROR_STOP=1 -f /migrations-auth/001_init.sql
