#!/bin/bash
set -euo pipefail

echo "=== RIDE-HAILING STREAMING: PREFLIGHT CHECK ==="

check_cmd() {
  local cmd=$1
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "❌ Missing required command: $cmd"
    exit 1
  fi
  echo "✅ Found $cmd"
}

echo "[1] Checking required CLIs..."
check_cmd docker
check_cmd docker-compose

echo "[2] Checking Docker daemon access..."
if ! docker info >/tmp/docker-preflight.log 2>&1; then
  echo "❌ Docker daemon unreachable. See /tmp/docker-preflight.log for details."
  exit 1
fi
echo "✅ Docker daemon reachable"

echo "[3] Checking default ports availability..."
PORTS=(8081 8083 8088 5601 9200 9092 5432 5433 3000)
PORT_CONFLICT=0
for p in "${PORTS[@]}"; do
  if lsof -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "❌ Port $p already in use"
    PORT_CONFLICT=1
  else
    echo "✅ Port $p free"
  fi
done

if [[ $PORT_CONFLICT -eq 1 ]]; then
  echo "Please free the ports above before running docker-compose."
  exit 1
fi

echo "[4] Checking available disk space (need >5GB)..."
# Use POSIX-compatible df output (1K blocks) for Linux/macOS portability
FREE_KB=$(df -Pk / | awk 'NR==2 {print $4}')
FREE_GB=$(( FREE_KB / 1024 / 1024 ))
FREE_GB=$(df -BG --output=avail / | tail -n1 | tr -dc '0-9')

if [[ ${FREE_GB:-0} -lt 5 ]]; then
  echo "❌ Not enough disk space: ${FREE_GB}GB available"
  exit 1
fi
echo "✅ Disk space OK (${FREE_GB}GB free)"

echo "[5] Quick config sanity checks..."
if [[ ! -f docker-compose.yaml ]]; then
  echo "❌ docker-compose.yaml not found in current directory"
  exit 1
fi
echo "✅ docker-compose.yaml present"

echo "=== PREFLIGHT PASSED: You can now run ./scripts/run_end_to_end.sh ==="
