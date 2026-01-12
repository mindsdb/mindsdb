PYTEST_ARGS = -v -rs --disable-warnings -n auto --dist loadfile
PYTEST_ARGS_DEBUG = --runslow -vs -rs

DSI_PYTEST_ARGS = --run-dsi-tests
DSI_REPORT_ARGS = --json-report --json-report-file=reports/report.json

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
	pytest $(PYTEST_ARGS) tests/integration/ -k "not test_auth"
	pytest $(PYTEST_ARGS) tests/integration/ -k test_auth  # Run this test separately because it alters the auth requirements, which breaks other tests

integration_tests_slow:
	pytest --runslow $(PYTEST_ARGS) tests/integration/ -k "not test_auth"
	pytest --runslow $(PYTEST_ARGS) tests/integration/ -k test_auth

integration_tests_debug:
	pytest $(PYTEST_ARGS_DEBUG) tests/integration/

datasource_integration_tests:
	@echo "--- Running Datasource Integration (DSI) Tests ---"
	# Ensure the reports directory exists before running tests
	mkdir -p reports
	# Added DSI_REPORT_ARGS to generate JSON report
	pytest $(PYTEST_ARGS) $(DSI_PYTEST_ARGS) $(DSI_REPORT_ARGS) tests/integration/handlers/

datasource_integration_tests_debug:
	@echo "--- Running Datasource Integration (DSI) Tests (Debug) ---"
	mkdir -p reports
	pytest $(PYTEST_ARGS_DEBUG) $(DSI_PYTEST_ARGS) $(DSI_REPORT_ARGS) tests/integration/handlers/

unit_tests:
	@set -o pipefail; \
	mkdir -p reports; \
	COVERAGE_FILE=.coverage.unit PYTHONPATH=./ pytest $(PYTEST_ARGS) \
		--junitxml=pytest.xml \
		--cov=mindsdb \
		--cov-report=term \
		--cov-report=xml:coverage.xml \
		--cov-branch \
		tests/unit | tee pytest-coverage.txt

unit_tests_slow:
	@set -o pipefail; \
	mkdir -p reports; \
	COVERAGE_FILE=.coverage.unit PYTHONPATH=./ pytest --runslow $(PYTEST_ARGS) \
		--junitxml=pytest.xml \
		--cov=mindsdb \
		--cov-report=term \
		--cov-report=xml:coverage.xml \
		--cov-branch \
		tests/unit | tee pytest-coverage.txt

unit_tests_debug:
	@set -o pipefail; \
	mkdir -p reports; \
	COVERAGE_FILE=.coverage.unit PYTHONPATH=./ pytest $(PYTEST_ARGS_DEBUG) \
		--junitxml=pytest.xml \
		--cov=mindsdb \
		--cov-report=term \
		--cov-report=xml:coverage.xml \
		--cov-branch \
		tests/unit | tee pytest-coverage.txt

.PHONY: install_mindsdb install_handler precommit format run_mindsdb check build_docker run_docker integration_tests integration_tests_slow integration_tests_debug datasource_integration_tests datasource_integration_tests_debug unit_tests unit_tests_slow unit_tests_debug
