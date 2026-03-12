#!/usr/bin/env bash
set -euo pipefail

# List the databases you want to ensure exist
DBS=(
  "mindsdb"
  "kb"
  "langfuse"
)

POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-postgres}"

for db in "${DBS[@]}"; do
  echo "Ensuring database '${db}' exists..."
  PGPASSWORD=$POSTGRES_PASSWORD psql -v ON_ERROR_STOP=1 \
       -U "${POSTGRES_USER}" \
       -h "${POSTGRES_HOST}" \
       -d "${POSTGRES_DB}" <<SQL
SELECT 'CREATE DATABASE "${db}"'
WHERE NOT EXISTS (
  SELECT FROM pg_database WHERE datname = '${db}'
) \gexec
SQL
done

echo "Done."