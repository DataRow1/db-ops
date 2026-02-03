.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo "Targets:"
	@echo "  setup       Create venv and install dev deps"
	@echo "  lint        Run ruff checks"
	@echo "  format      Run ruff formatter"
	@echo "  test        Run pytest"
	@echo "  smoke       Run CLI smoke test"
	@echo "  check       Run lint + tests + smoke"
	@echo "  precommit   Install commit-msg hook"

.PHONY: setup
setup:
	uv venv
	. .venv/bin/activate && uv pip install -e .
	. .venv/bin/activate && uv pip install ruff pytest pre-commit

.PHONY: lint
lint:
	. .venv/bin/activate && ruff check .

.PHONY: format
format:
	. .venv/bin/activate && ruff format .

.PHONY: test
test:
	. .venv/bin/activate && pytest

.PHONY: smoke
smoke:
	. .venv/bin/activate && python -m dbops --help

.PHONY: check
check: lint test smoke

.PHONY: precommit
precommit:
	. .venv/bin/activate && pre-commit install --hook-type commit-msg
