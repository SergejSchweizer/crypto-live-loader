PYTHON ?= .venv/bin/python

.PHONY: setup test lint typecheck typecheck-strict check

setup:
	python3 -m venv .venv
	$(PYTHON) -m pip install -U pip
	$(PYTHON) -m pip install -e ".[dev]"

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .

typecheck:
	$(PYTHON) -m mypy .

typecheck-strict:
	$(PYTHON) -m pyright

check: lint typecheck typecheck-strict test
