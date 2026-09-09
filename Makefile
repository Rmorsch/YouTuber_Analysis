.PHONY: install dev dbt-deps dbt-run dbt-test test

install:
	pip install -e ".[dev]"

dbt-deps:
	cd dbt && dbt deps

dev:
	dagster dev -m youtuber_analysis.definitions

dbt-run:
	cd dbt && dbt build

dbt-test:
	cd dbt && dbt test

test:
	pytest tests/
