#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ "${RUN_STREAMING_STACK:-}" == "" ]]; then
  echo "[test runner] Ensure the streaming stack is running (docker-compose up) before running tests."
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "[test runner] python3 is required to run tests." >&2
  exit 1
fi

TEST_TARGETS=(
  "tests/correctness_test.py"
  "tests/performance_test.py"
)

for test_script in "${TEST_TARGETS[@]}"; do
  echo "\n[test runner] Running ${test_script}"
  python3 "${test_script}" || {
    echo "[test runner] ${test_script} failed" >&2
    exit 1
  }
  echo "[test runner] ${test_script} completed"
done

if [[ -x tests/fault_tolerance_test.sh ]]; then
  echo "\n[test runner] Running tests/fault_tolerance_test.sh"
  tests/fault_tolerance_test.sh
else
  echo "[test runner] Skipping fault_tolerance_test.sh (not executable)"
fi

echo "\n[test runner] All tests completed"
