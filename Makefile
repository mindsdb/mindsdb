install_mindsdb:
	pip install -e .
	pip install -r requirements/requirements-dev.txt
	pre-commit install

install_handler:
	pip install -r mindsdb/integrations/handlers/$(HANDLER_NAME)_handler/requirements.txt

precommit:
	pre-commit install
	pre-commit run --files $$(git diff --cached --name-only)

run_mindsdb:
	python -m mindsdb

check:
	python tests/scripts/check_requirements.py
	python tests/scripts/check_code.py

.PHONY: install_mindsdb precommit install_handler run_mindsdb check
