install_precommit:
	pip install pre-commit
	pre-commit install

precommit: install_precommit
	pre-commit run --all-files


.PHONY: precommit
