#!/usr/bin/env bash
# One-command launcher: preflight + full stack + generator + health/SLA + optional tests
set -euo pipefail

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$ROOT"

# Defaults that can be overridden by env vars
RUN_GENERATOR="${RUN_GENERATOR:-true}"
GENERATOR_SECONDS="${GENERATOR_SECONDS:-600}"
GENERATOR_MODE="${GENERATOR_MODE:-stream}"
GENERATOR_VOLUME="${GENERATOR_VOLUME:-small}"
GENERATOR_EXTRA_ARGS="${GENERATOR_EXTRA_ARGS:-}"

RUN_CORRECTNESS_TESTS="${RUN_CORRECTNESS_TESTS:-false}"
RUN_FAULT_TESTS="${RUN_FAULT_TESTS:-false}"
RUN_PERF_TESTS="${RUN_PERF_TESTS:-false}"

# Propagate generator DB overrides if provided
export GENERATOR_PGHOST="${GENERATOR_PGHOST:-localhost}"
export GENERATOR_PGPORT="${GENERATOR_PGPORT:-5432}"
export GENERATOR_PGDB="${GENERATOR_PGDB:-ride_hailing_db}"
export GENERATOR_PGUSER="${GENERATOR_PGUSER:-user}"
export GENERATOR_PGPASSWORD="${GENERATOR_PGPASSWORD:-password}"

export RUN_GENERATOR GENERATOR_SECONDS GENERATOR_MODE GENERATOR_VOLUME GENERATOR_EXTRA_ARGS

banner(){ echo -e "\n================== $* ==================\n"; }

banner "0) Preflight checks (Docker, ports, files)"
bash ops/preflight_check.sh

echo "\nPreflight OK — launching full pipeline."

banner "1) Redeploy stack + generator"
bash scripts/redeploy_all.sh

echo "Superset: http://localhost:8088 | Kibana: http://localhost:5601 | ES: http://localhost:9200"

echo "Waiting a few seconds for services to finish bootstrapping..."
sleep 5

banner "2) Health + SLA freshness checks"
bash ops/check_health.sh || true
bash ops/check_sla_latency.sh || true

if [[ "$RUN_CORRECTNESS_TESTS" == "true" ]]; then
  banner "3) Correctness tests"
  python3 tests/correctness_test.py || true
fi

if [[ "$RUN_FAULT_TESTS" == "true" ]]; then
  banner "4) Fault-tolerance tests"
  bash tests/fault_tolerance_test.sh || true
fi

if [[ "$RUN_PERF_TESTS" == "true" ]]; then
  banner "5) Performance tests"
  python3 tests/performance_test.py || true
fi

banner "Done"
echo "Stack is up and feeding data. Access dashboards in Superset/Kibana or run ops scripts for more checks."
