setup_dev:
	pip install -r requirements_dev.txt
	pre-commit install

precommit: setup_dev
	pre-commit run --files $$(shell git diff --cached --name-only)

.PHONY: setup_dev  precommit
