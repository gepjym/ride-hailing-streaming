#!/bin/bash
set -e

echo "=== HEALTH CHECK: RIDE-HAILING STREAMING SYSTEM ==="

# 1. Check Flink Job
echo "[1] Checking Flink job status..."
FLINK_JOBS_JSON=$(curl -s http://localhost:8081/jobs)
JOB_STATUS=$(echo "$FLINK_JOBS_JSON" | jq -r '.jobs[0].status' 2>/dev/null || echo "UNKNOWN")
echo "    Flink job status: $JOB_STATUS"

# 2. Check Kafka container
echo "[2] Checking Kafka container..."
KAFKA_STATUS=$(docker inspect -f '{{.State.Running}}' kafka 2>/dev/null || echo "false")
echo "    Kafka running: $KAFKA_STATUS"

# 3. Check Postgres (source + reporting)
echo "[3] Checking Postgres databases..."
docker exec postgres-source psql -U user -d ride_hailing_db -c "SELECT 1;" >/dev/null 2>&1 && \
  echo "    Source DB: OK" || echo "    Source DB: FAIL"

docker exec postgres-reporting psql -U user -d reporting_db -c "SELECT 1;" >/dev/null 2>&1 && \
  echo "    Reporting DB: OK" || echo "    Reporting DB: FAIL"

# 4. Check Elasticsearch
echo "[4] Checking Elasticsearch..."
curl -s -o /dev/null -w "%{http_code}" http://localhost:9200 >/tmp/es_status
ES_CODE=$(cat /tmp/es_status)
echo "    Elasticsearch HTTP: $ES_CODE"

# 5. Check Kibana
echo "[5] Checking Kibana..."
curl -s -o /dev/null -w "%{http_code}" http://localhost:5601 >/tmp/kibana_status
KIBANA_CODE=$(cat /tmp/kibana_status)
echo "    Kibana HTTP: $KIBANA_CODE"

echo "=== HEALTH CHECK DONE ==="
