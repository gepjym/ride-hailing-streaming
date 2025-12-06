#!/usr/bin/env bash
# Orchestrate full bring-up: infra + Flink + generator + health/SLA checks
set -euo pipefail

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$ROOT"

RUN_GENERATOR="${RUN_GENERATOR:-true}"
GENERATOR_SECONDS="${GENERATOR_SECONDS:-600}"
GENERATOR_MODE="${GENERATOR_MODE:-stream}"
GENERATOR_VOLUME="${GENERATOR_VOLUME:-small}"
GENERATOR_EXTRA_ARGS="${GENERATOR_EXTRA_ARGS:-}" # optional, e.g. "--stream-max-inflight 6000"

RUN_CORRECTNESS_TESTS="${RUN_CORRECTNESS_TESTS:-false}"
RUN_FAULT_TESTS="${RUN_FAULT_TESTS:-false}"
RUN_PERF_TESTS="${RUN_PERF_TESTS:-false}"

# Defaults for data generator DB
export GENERATOR_PGHOST="${GENERATOR_PGHOST:-localhost}"
export GENERATOR_PGPORT="${GENERATOR_PGPORT:-5432}"
export GENERATOR_PGDB="${GENERATOR_PGDB:-ride_hailing_db}"
export GENERATOR_PGUSER="${GENERATOR_PGUSER:-user}"
export GENERATOR_PGPASSWORD="${GENERATOR_PGPASSWORD:-password}"

export RUN_GENERATOR GENERATOR_SECONDS GENERATOR_MODE GENERATOR_VOLUME GENERATOR_EXTRA_ARGS

banner(){ echo -e "\n================== $* ==================\n"; }

banner "1) Redeploy full stack + optional generator"
bash scripts/redeploy_all.sh

banner "2) Quick health + SLA checks"
bash ops/check_health.sh || true
bash ops/check_sla_latency.sh || true

echo "(Superset will be available at http://localhost:8088 once the container finishes bootstrapping.)"
echo "Kibana available at http://localhost:5601; Elasticsearch at http://localhost:9200"

if [[ "$RUN_CORRECTNESS_TESTS" == "true" ]]; then
  banner "3) Running correctness tests"
  python3 tests/correctness_test.py || true
fi

if [[ "$RUN_FAULT_TESTS" == "true" ]]; then
  banner "4) Running fault-tolerance tests"
  bash tests/fault_tolerance_test.sh || true
fi

if [[ "$RUN_PERF_TESTS" == "true" ]]; then
  banner "5) Running performance tests"
  python3 tests/performance_test.py || true
fi

banner "Done"
echo "Stack is up. Open Superset/Kibana, or run ops scripts for deeper inspection."
