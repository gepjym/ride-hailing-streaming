#!/bin/bash
set -euo pipefail

SUPERSET_HOME_DIR="${SUPERSET_HOME:-/app/superset_home}"
REPORTING_HOST="${REPORTING_DB_HOST:-postgres-reporting}"
REPORTING_PORT="${REPORTING_DB_PORT:-5432}"
REPORTING_URI="${REPORTING_DB_URI:-postgresql://user:password@${REPORTING_HOST}:${REPORTING_PORT}/reporting_db}"
DATABASE_CONFIG_PATH="${DATABASE_CONFIG_PATH:-${SUPERSET_HOME_DIR}/database_config.yaml}"

SUPERSET_PORT="${SUPERSET_PORT:-8088}"
SUPERSET_WORKERS="${SUPERSET_WORKERS:-4}"
SUPERSET_TIMEOUT="${SUPERSET_TIMEOUT:-120}"


echo "[Superset bootstrap] Ensuring Superset home exists at ${SUPERSET_HOME_DIR}"
mkdir -p "${SUPERSET_HOME_DIR}"
export SUPERSET_HOME="${SUPERSET_HOME_DIR}"

echo "[Superset bootstrap] Waiting for reporting DB at ${REPORTING_HOST}:${REPORTING_PORT}..."
CHECK_DB_CMD=(pg_isready -h "${REPORTING_HOST}" -p "${REPORTING_PORT}")
if ! command -v pg_isready >/dev/null 2>&1; then
  CHECK_DB_CMD=(bash -c "</dev/tcp/${REPORTING_HOST}/${REPORTING_PORT}")
fi

for i in {1..60}; do
  if "${CHECK_DB_CMD[@]}" >/dev/null 2>&1; then
    echo "[Superset bootstrap] Reporting DB is reachable."
    break
  fi

  if [[ $i -eq 60 ]]; then
    echo "[Superset bootstrap] Reporting DB is not reachable after waiting. Exiting."
    exit 1
  fi

  sleep 2
done

echo "[Superset bootstrap] Upgrading metadata DB and creating admin user"
superset db upgrade
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@moovtek.local \
  --password admin123 || true
superset init

echo "[Superset bootstrap] Registering reporting database connection"
if [[ -f "${DATABASE_CONFIG_PATH}" ]]; then
  echo "[Superset bootstrap] Importing database connections from ${DATABASE_CONFIG_PATH}"

  superset import-databases -p "${DATABASE_CONFIG_PATH}" --overwrite || echo "[Superset bootstrap] Database import failed (continuing so UI can still start)"


  superset import-databases -p "${DATABASE_CONFIG_PATH}" --overwrite || echo "[Superset bootstrap] Database import failed (continuing so UI can still start)"


  superset import-databases -p "${DATABASE_CONFIG_PATH}" --overwrite || echo "[Superset bootstrap] Database import failed (continuing so UI can still start)"

  superset import-databases -p "${DATABASE_CONFIG_PATH}" --overwrite || true



else
  echo "[Superset bootstrap] ${DATABASE_CONFIG_PATH} not found; skipping database import"
fi

superset set_database_uri \
  --database_name "Reporting DB" \

  --uri "${REPORTING_URI}" || echo "[Superset bootstrap] Database URI update failed (continuing so UI can still start)"

echo "[Superset bootstrap] Starting Superset server with gunicorn on port ${SUPERSET_PORT} (workers=${SUPERSET_WORKERS}, timeout=${SUPERSET_TIMEOUT})..."
export FLASK_ENV=production
export SUPERSET_ENV=production
export FLASK_APP="superset.app:create_app()"

exec gunicorn \
  --bind "0.0.0.0:${SUPERSET_PORT}" \
  --workers "${SUPERSET_WORKERS}" \
  --worker-class gevent \
  --timeout "${SUPERSET_TIMEOUT}" \
  --access-logfile "-" \
  --error-logfile "-" \
  "superset.app:create_app()"



  --uri "${REPORTING_URI}" || echo "[Superset bootstrap] Database URI update failed (continuing so UI can still start)"

echo "[Superset bootstrap] Starting Superset server..."
exec superset run -h 0.0.0.0 -p 8088 --with-threads --reload=false

  --uri "${REPORTING_URI}" || true

echo "[Superset bootstrap] Starting Superset server..."
exec gunicorn -w 4 -k gevent --timeout 120 -b 0.0.0.0:8088 "superset.app:create_app()"



