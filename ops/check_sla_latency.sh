#!/bin/bash
set -e

echo "=== SLA LATENCY CHECK (TARGET < 5s) ==="

# Query vw_data_freshness from reporting_db
RESULT=$(docker exec postgres-reporting psql -U user -d reporting_db -t -A -F ',' << 'SQL'
SELECT source_name,
       ingestion_lag_seconds,
       processing_lag_seconds
FROM mart.vw_data_freshness;
SQL
)

THRESHOLD=5
VIOLATION=0

echo "source_name,ingestion_lag_seconds,processing_lag_seconds,status"
while IFS=',' read -r source lag_ing lag_proc; do
  [ -z "$source" ] && continue
  STATUS="OK"
  if [ "$lag_ing" != "" ] && (( $(printf "%.0f" "$lag_ing") > THRESHOLD )); then
    STATUS="LAGGING"
    VIOLATION=1
  fi
  echo "$source,$lag_ing,$lag_proc,$STATUS"
done <<< "$RESULT"

if [ $VIOLATION -eq 0 ]; then
  echo "=> SLA: ✅ All sources within latency threshold (${THRESHOLD}s)"
  exit 0
else
  echo "=> SLA: ❌ Some sources exceed latency threshold (${THRESHOLD}s)"
  exit 1
fi
