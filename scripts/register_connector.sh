#!/usr/bin/env bash
set -euo pipefail
echo "[*] Registering Debezium Postgres connector..."

curl -s -X DELETE http://localhost:8083/connectors/pg-source-connector >/dev/null || true

http_code=$(curl -s -o /tmp/resp.json -w "%{http_code}" \
  -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @connectors/pg-source-connector.json)

cat /tmp/resp.json; echo
if [[ "$http_code" != "200" && "$http_code" != "201" ]]; then
  echo "[ERROR] Registration failed with HTTP $http_code"
  exit 1
fi
echo "[OK] Connector registered."
