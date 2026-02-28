#!/usr/bin/env bash
set -euo pipefail

DB_HOST="${POSTGRES_HOST:-localhost}"
DB_PORT="${POSTGRES_PORT:-5432}"
DB_NAME="${POSTGRES_DB:-autoscholar}"
DB_USER="${POSTGRES_USER:-autoscholar}"
DB_PASS="${POSTGRES_PASSWORD:-autoscholar}"

MIGRATIONS_DIR="$(dirname "$0")/../internal/db/migrations"

for f in "$MIGRATIONS_DIR"/*.up.sql; do
  echo "Applying $(basename "$f")..."
  PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$f"
done

echo "Migrations complete."
