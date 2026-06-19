PYTHON ?= .venv/bin/python
PYTEST_ARGS ?=
PYTEST_WORKERS ?= 2

.PHONY: setup test test-fast test-parallel test-coverage lint typecheck typecheck-strict check

setup:
	python3 -m venv .venv
	$(PYTHON) -m pip install -U pip
	$(PYTHON) -m pip install -e ".[dev]"

test:
	$(PYTHON) -m pytest $(PYTEST_ARGS)

test-fast:
	$(PYTHON) -m pytest -x --ff $(PYTEST_ARGS)

test-parallel:
	$(PYTHON) -m pytest -n $(PYTEST_WORKERS) --dist loadfile $(PYTEST_ARGS)

test-coverage:
	$(PYTHON) -m pytest --cov --cov-report=term-missing $(PYTEST_ARGS)

lint:
	$(PYTHON) -m ruff check .

typecheck:
	$(PYTHON) -m mypy .

typecheck-strict:
	$(PYTHON) -m pyright

check: lint typecheck typecheck-strict test-coverage
