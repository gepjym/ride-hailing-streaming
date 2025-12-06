#!/usr/bin/env bash
set -euo pipefail

# ==== 1. Đọc biến môi trường ====
SUPERSET_HOME_DIR="${SUPERSET_HOME:-/app/superset_home}"
REPORTING_HOST="${REPORTING_DB_HOST:-postgres-reporting}"
REPORTING_PORT="${REPORTING_DB_PORT:-5432}"

# File YAML chứa cấu hình DB external (Reporting DB)
DATABASE_CONFIG_PATH="${DATABASE_CONFIG_PATH:-${SUPERSET_HOME_DIR}/database_config.yaml}"

# URI cho Reporting DB (nơi chứa mart, views…)
REPORTING_URI="${REPORTING_DB_URI:-postgresql://user:password@${REPORTING_HOST}:${REPORTING_PORT}/reporting_db}"

# Superset metadata DB (nơi lưu charts/dashboards/users)
# Ở đây cho dùng luôn biến SUPERSET_DATABASE_URI nếu đã set trong docker-compose,
# còn không thì fallback về SQLite local cho Superset.
SUPERSET_METADATA_URI="${SUPERSET_DATABASE_URI:-sqlite:////app/superset_home/superset.db}"

SUPERSET_PORT="${SUPERSET_PORT:-8088}"
SUPERSET_WORKERS="${SUPERSET_WORKERS:-4}"
SUPERSET_TIMEOUT="${SUPERSET_TIMEOUT:-120}"

ADMIN_USERNAME="${SUPERSET_ADMIN_USERNAME:-admin}"
ADMIN_FIRSTNAME="${SUPERSET_ADMIN_FIRSTNAME:-Admin}"
ADMIN_LASTNAME="${SUPERSET_ADMIN_LASTNAME:-User}"
ADMIN_EMAIL="${SUPERSET_ADMIN_EMAIL:-admin@moovtek.local}"
ADMIN_PASSWORD="${SUPERSET_ADMIN_PASSWORD:-admin123}"

echo "[Superset bootstrap] Ensuring Superset home exists at ${SUPERSET_HOME_DIR}"
mkdir -p "${SUPERSET_HOME_DIR}"
export SUPERSET_HOME="${SUPERSET_HOME_DIR}"

# ==== 2. Chờ Reporting DB sẵn sàng (dùng cho kết nối datasource) ====
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

# ==== 3. Upgrade metadata DB + tạo admin user (non-interactive) ====
echo "[Superset bootstrap] Upgrading Superset metadata DB (${SUPERSET_METADATA_URI})"
export SUPERSET_DATABASE_URI="${SUPERSET_METADATA_URI}"
superset db upgrade

echo "[Superset bootstrap] Creating admin user (if not exists)"
superset fab create-admin \
  --username "${ADMIN_USERNAME}" \
  --firstname "${ADMIN_FIRSTNAME}" \
  --lastname "${ADMIN_LASTNAME}" \
  --email "${ADMIN_EMAIL}" \
  --password "${ADMIN_PASSWORD}" \
  --skip-if-exists

echo "[Superset bootstrap] Running superset init"
superset init

# ==== 4. Import datasource từ YAML + set Reporting DB connection ====
echo "[Superset bootstrap] Registering reporting database connection"
if [[ -f "${DATABASE_CONFIG_PATH}" ]]; then
  echo "[Superset bootstrap] Importing database connections from ${DATABASE_CONFIG_PATH}"
  superset import-databases -p "${DATABASE_CONFIG_PATH}" --overwrite \
    || echo "[Superset bootstrap] Database import failed (continuing so UI can still start)"
else
  echo "[Superset bootstrap] ${DATABASE_CONFIG_PATH} not found; skipping database import"
fi

# Đảm bảo có database name 'Reporting DB' trỏ tới REPORTING_URI
superset set_database_uri \
  --database_name "Reporting DB" \
  --uri "${REPORTING_URI}" \
  || echo "[Superset bootstrap] Database URI update failed (continuing so UI can still start)"

# ==== 5. Start Superset webserver ====
echo "[Superset bootstrap] Starting Superset webserver on port ${SUPERSET_PORT}"

export FLASK_ENV=production
export SUPERSET_ENV=production
export FLASK_APP="superset.app:create_app()"

# Đơn giản: dùng superset run (sẵn có trong image)
exec superset run -h 0.0.0.0 -p "${SUPERSET_PORT}" --with-threads
