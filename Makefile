install_precommit:
	pip install pre-commit
	pre-commit install

precommit: install_precommit
	pre-commit run --files $(shell git diff --cached --name-only)


.PHONY: precommit
