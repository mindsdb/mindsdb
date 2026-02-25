#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

HANDLERS_TO_INSTALL='
postgres
mysql
salesforce
snowflake
timescaledb
mssql
oracle
slack
redshift
bigquery
clickhouse
web
databricks
github
ms_teams
statsforecast
chromadb
confluence
'

HANDLERS_TO_VERIFY='
mysql
salesforce
postgres
snowflake
timescaledb
mssql
oracle
slack
file
redshift
bigquery
confluence
'

INSTALL_HANDLERS=()
while IFS= read -r handler; do
  handler=${handler//$'\r'/}
  [[ -z "${handler}" || "${handler}" =~ ^[[:space:]]*# ]] && continue
  INSTALL_HANDLERS+=("${handler}")
done <<< "${HANDLERS_TO_INSTALL}"

HANDLER_EXTRAS=()
for handler in "${INSTALL_HANDLERS[@]}"; do
  HANDLER_EXTRAS+=(".[${handler}]")
done

uv pip install ".[agents,kb]" \
  -r requirements/requirements-test.txt \
  "${HANDLER_EXTRAS[@]}"

uv pip install --force-reinstall onnxruntime==1.20.1

# Ensure parser tests are present (required for render tests when --runslow)
if [[ ! -d parser_tests ]]; then
  git clone --branch v$(uv pip show mindsdb_sql_parser | grep Version | cut -d ' ' -f 2) \
    https://github.com/mindsdb/mindsdb_sql_parser.git parser_tests
fi

# Run the exact test target used in CI
make unit_tests_slow

# Generate the extra artifacts produced in CI
HANDLERS_TO_INSTALL="${HANDLERS_TO_INSTALL}" \
HANDLERS_TO_VERIFY="${HANDLERS_TO_VERIFY}" \
COVERAGE_FAIL_UNDER="80" \
COVERAGE_FILE=.coverage.unit \
  uv run tests/scripts/check_handler_coverage.py > pytest-coverage-handlers.txt
COVERAGE_FILE=.coverage.unit uv run coverage html -d reports/htmlcov

# Collect artifacts in a single directory
ARTIFACT_DIR="tests_artifacts"
mkdir -p "${ARTIFACT_DIR}"

for artifact in pytest.xml coverage.xml .coverage.unit pytest-coverage.txt pytest-coverage-handlers.txt; do
  if [[ -f "${artifact}" ]]; then
    mv "${artifact}" "${ARTIFACT_DIR}/"
  fi
done

if [[ -d reports/htmlcov ]]; then
  rm -rf "${ARTIFACT_DIR}/htmlcov"
  mv reports/htmlcov "${ARTIFACT_DIR}/htmlcov"
fi
