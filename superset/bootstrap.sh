#!/bin/bash
set -euo pipefail

# Ensure reporting database is reachable before initializing Superset
REPORTING_HOST="${REPORTING_DB_HOST:-postgres-reporting}"
REPORTING_PORT="${REPORTING_DB_PORT:-5432}"
REPORTING_URI="${REPORTING_DB_URI:-postgresql://user:password@${REPORTING_HOST}:${REPORTING_PORT}/reporting_db}"

echo "[Superset bootstrap] Waiting for reporting DB at ${REPORTING_HOST}:${REPORTING_PORT}..."
for i in {1..30}; do
  if bash -c "</dev/tcp/${REPORTING_HOST}/${REPORTING_PORT}" >/dev/null 2>&1; then
    echo "[Superset bootstrap] Reporting DB is reachable."
    break
  fi
  if [[ $i -eq 30 ]]; then
    echo "[Superset bootstrap] Reporting DB is not reachable after waiting. Exiting."
    exit 1
  fi
  sleep 2
done

# Initialize Superset metadata and admin user
superset db upgrade
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@moovtek.local \
  --password admin123
superset init

# Import the reporting database connection if the config is present
if [[ -f /app/superset_home/database_config.yaml ]]; then
  echo "[Superset bootstrap] Importing database connections from database_config.yaml"
  superset import-databases -p /app/superset_home/database_config.yaml --overwrite
else
  echo "[Superset bootstrap] database_config.yaml not found; skipping database import"
fi

echo "[Superset bootstrap] Starting Superset server..."
exec superset run -h 0.0.0.0 -p 8088 --with-threads --reload
