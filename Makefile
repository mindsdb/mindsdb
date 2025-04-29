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

run_mindsdb:
	python -m mindsdb

check:
	python tests/scripts/check_requirements.py
	python tests/scripts/check_version.py
	python tests/scripts/check_print_statements.py

build_docker:
	docker buildx build -t mdb --load -f docker/mindsdb.Dockerfile .

run_docker: build_docker
	docker run -it -p 47334:47334 mdb

integration_tests:
	pytest -n 8 --dist loadfile -vsx tests/integration/tutorials/ tests/integration/flows/

integration_tests_debug:
	pytest -vsx tests/integration/tutorials/ tests/integration/flows/

.PHONY: install_mindsdb install_handler precommit run_mindsdb check build_docker run_docker integration_tests integration_tests_debug
