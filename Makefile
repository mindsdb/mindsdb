PYTEST_ARGS = -v -rs --disable-warnings -n auto --dist loadfile
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
	pytest $(PYTEST_ARGS) tests/integration/ -k "not test_auth"
	pytest $(PYTEST_ARGS) tests/integration/ -k test_auth  # Run this test separately because it alters the auth requirements, which breaks other tests

integration_tests_slow:
	pytest --runslow $(PYTEST_ARGS) tests/integration/ -k "not test_auth"
	pytest --runslow $(PYTEST_ARGS) tests/integration/ -k test_auth

integration_tests_debug:
	pytest $(PYTEST_ARGS_DEBUG) tests/integration/

unit_tests:
	# We have to run executor tests separately because they do weird things that break everything else
	env PYTHONPATH=./ pytest $(PYTEST_ARGS) tests/unit/executor/  
	pytest $(PYTEST_ARGS) --ignore=tests/unit/executor tests/unit/

unit_tests_slow:
	env PYTHONPATH=./ pytest --runslow $(PYTEST_ARGS) tests/unit/executor/  # We have to run executor tests separately because they do weird things that break everything else
	pytest --runslow $(PYTEST_ARGS) --ignore=tests/unit/executor tests/unit/

unit_tests_debug:
	env PYTHONPATH=./ pytest $(PYTEST_ARGS_DEBUG) tests/unit/executor/  
	pytest $(PYTEST_ARGS_DEBUG) --ignore=tests/unit/executor tests/unit/

.PHONY: install_mindsdb install_handler precommit format run_mindsdb check build_docker run_docker integration_tests integration_tests_slow integration_tests_debug unit_tests unit_tests_slow unit_tests_debug
