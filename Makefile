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

build_docker:
	docker buildx build -t mdb --load -f docker/mindsdb.Dockerfile .

run_docker: build_docker
	docker run -it -p 47334:47334 mdb

integration_tests:
	# Run tests in parallel and distribute a whole file to each worker
	pytest -n 8 -rs -v --dist loadfile tests/integration/ -k "not test_auth"
	# Run this test separately because it alters the auth requirements, which breaks other tests
	pytest -v tests/integration/ -k test_auth

integration_tests_debug:
	pytest -vxs tests/integration/ -k "not test_auth"
	pytest -vxs tests/integration/ -k test_auth

unit_tests:
	env PYTHONPATH=./ pytest tests/unit/executor/  # We have to run executor tests separately because they do weird things that break everything else
	pytest --ignore=tests/unit/executor tests/unit/


.PHONY: install_mindsdb install_handler precommit format run_mindsdb check build_docker run_docker integration_tests integration_tests_debug unit_tests
