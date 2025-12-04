#!/bin/bash
set -e

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUT_DIR="ops/resource_logs"
mkdir -p "$OUT_DIR"

echo "=== CAPTURE RESOURCE USAGE @ $TIMESTAMP ==="

docker stats \
  flink-jobmanager \
  flink-taskmanager \
  kafka \
  postgres-source \
  postgres-reporting \
  --no-stream > "${OUT_DIR}/docker_stats_${TIMESTAMP}.log"

echo "Saved to ${OUT_DIR}/docker_stats_${TIMESTAMP}.log"
