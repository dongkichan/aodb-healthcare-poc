#!/usr/bin/env bash
# Writes .env with fresh random values for this machine. Refuses to overwrite an existing .env,
# because the PostgreSQL volume keeps the passwords it was first created with.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env ]; then
  echo ".env already exists; delete it and run 'docker compose down -v' first to start over." >&2
  exit 1
fi
rand() { LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c "$1"; }
{
  echo "POSTGRES_PASSWORD=$(rand 24)"  # generated per machine; pragma: allowlist secret
  echo "APP_DB_PASSWORD=$(rand 24)"  # generated per machine; pragma: allowlist secret
  echo "NEO4J_PASSWORD=$(rand 24)"  # generated per machine; pragma: allowlist secret
  echo "JWT_SECRET=$(rand 48)"
} > .env
chmod 600 .env
echo "Wrote .env with random local values."
