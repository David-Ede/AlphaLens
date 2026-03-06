PYTHON ?= python
PIP ?= $(PYTHON) -m pip

.PHONY: setup lint typecheck test test-live demo-seed run refresh build-gold export quality-report ci

setup:
	$(PYTHON) -m venv .venv || true
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"
	pre-commit install

lint:
	ruff check src tests

typecheck:
	mypy src/fg

test:
	pytest

test-live:
	pytest -m live --allow-hosts=sec.report,data.sec.gov,query1.finance.yahoo.com,financialmodelingprep.com

demo-seed:
	fg demo-seed

run:
	fg run-dashboard

refresh:
	fg refresh-ticker --ticker $(TICKER)

build-gold:
	fg build-gold --ticker $(TICKER)

export:
	fg export --ticker $(TICKER) --format $(FORMAT)

quality-report:
	fg quality-report --ticker $(TICKER)

ci: lint typecheck test
