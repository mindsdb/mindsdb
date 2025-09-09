# --- Refactored Pytest Arguments ---
# Base arguments for all test runs
PYTEST_BASE_ARGS = -v -rs --disable-warnings
# Arguments for parallel execution (used by most test suites)
PYTEST_PARALLEL_ARGS = -n auto --dist loadfile
# Arguments specific to DSI tests
DSI_PYTEST_ARGS = --run-dsi-tests
DSI_REPORT_ARGS = --json-report --json-report-file=reports/report.json
# Arguments for debug runs
PYTEST_ARGS_DEBUG = --runslow -vs -rs

install_mindsdb:
	pip install -e .
	pip install -r requirements/requirements-dev.txt
	pre-commit install

install_handler:
	@if [[ -n "$(HANDLER_NAME)" ]]; then\
		pip install -e .[$(HANDLER_NAME)];\
	else\
		echo 'Please set $$HANDLER_NAME to the handler to install.';\
	fi

precommit:
	pre-commit install
	pre-commit run --files $$(git diff --cached --name-only)

format:
	pre-commit run --hook-stage manual

run_mindsdb:
	python -m mindsdb

check:
	python tests/scripts/check_requirements.py
	python tests/scripts/check_print_statements.py
	pre-commit install
	pre-commit run --files $$(git diff --cached --name-only)

build_docker:
	docker buildx build -t mdb --load -f docker/mindsdb.Dockerfile .

run_docker: build_docker
	docker run -it -p 47334:47334 mdb

integration_tests:
	pytest $(PYTEST_BASE_ARGS) $(PYTEST_PARALLEL_ARGS) tests/integration/ -k "not test_auth"
	pytest $(PYTEST_BASE_ARGS) $(PYTEST_PARALLEL_ARGS) tests/integration/ -k test_auth # Run this test separately because it alters the auth requirements, which breaks other tests

integration_tests_slow:
	pytest --runslow $(PYTEST_BASE_ARGS) $(PYTEST_PARALLEL_ARGS) tests/integration/ -k "not test_auth"
	pytest --runslow $(PYTEST_BASE_ARGS) $(PYTEST_PARALLEL_ARGS) tests/integration/ -k test_auth

integration_tests_debug:
	pytest $(PYTEST_ARGS_DEBUG) tests/integration/

datasource_integration_tests:
	@echo "--- Running Datasource Integration (DSI) Tests ---"
	# This target runs the tests sequentially to avoid xdist race conditions during dynamic test generation.
	pytest $(PYTEST_BASE_ARGS) $(DSI_PYTEST_ARGS) tests/integration/handlers/

datasource_integration_tests_report:
	@echo "--- Running DSI Tests and Generating JSON Report ---"
	mkdir -p reports
	pytest $(PYTEST_BASE_ARGS) $(DSI_PYTEST_ARGS) $(DSI_REPORT_ARGS) tests/integration/handlers/

datasource_integration_tests_debug:
	@echo "--- Running Datasource Integration (DSI) Tests (Debug) ---"
	mkdir -p reports
	pytest $(PYTEST_ARGS_DEBUG) $(DSI_PYTEST_ARGS) $(DSI_REPORT_ARGS) tests/integration/handlers/

unit_tests:
	# We have to run executor tests separately because they do weird things that break everything else
	env PYTHONPATH=./ pytest $(PYTEST_BASE_ARGS) $(PYTEST_PARALLEL_ARGS) tests/unit/executor/
	pytest $(PYTEST_BASE_ARGS) $(PYTEST_PARALLEL_ARGS) --ignore=tests/unit/executor tests/unit/

unit_tests_slow:
	env PYTHONPATH=./ pytest --runslow $(PYTEST_BASE_ARGS) $(PYTEST_PARALLEL_ARGS) tests/unit/executor/ # We have to run executor tests separately because they do weird things that break everything else
	pytest --runslow $(PYTEST_BASE_ARGS) $(PYTEST_PARALLEL_ARGS) --ignore=tests/unit/executor tests/unit/

unit_tests_debug:
	env PYTHONPATH=./ pytest $(PYTEST_ARGS_DEBUG) tests/unit/executor/
	pytest $(PYTEST_ARGS_DEBUG) --ignore=tests/unit/executor tests/unit/

.PHONY: install_mindsdb install_handler precommit format run_mindsdb check build_docker run_docker integration_tests integration_tests_slow integration_tests_debug datasource_integration_tests datasource_integration_tests_report datasource_integration_tests_debug unit_tests unit_tests_slow unit_tests_debug