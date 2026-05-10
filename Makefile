PYTHON ?= python3.12
VENV ?= .venv
DAGSTER_HOME ?= $(CURDIR)/.dagster_home

PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
DAGSTER := $(CURDIR)/$(VENV)/bin/dagster

.PHONY: help check-python check-venv check-no-dev venv dagster-home install seed-partitions dev stop-dev test prepare build export article revalidate reset clean

help:
	@echo "Targets:"
	@echo "  make install          Create .venv and install project with dev deps"
	@echo "  make dev              Start Dagster UI at http://127.0.0.1:3000"
	@echo "  make stop-dev         Stop the local Dagster webserver on port 3000"
	@echo "  make seed-partitions  Register article partitions from data/chunks"
	@echo "  make test             Run unit tests"
	@echo "  make prepare          Fetch markdown, chunk articles, and seed partitions"
	@echo "  make build            Run prepare, generate/validate all articles, and export"
	@echo "  make export           Export validated files to JSONL"
	@echo "  make article ARTICLE=94  Regenerate one article partition"
	@echo "  make revalidate       Rebuild validated files from existing synthetic JSON"
	@echo "  make reset            Remove synthetic, validated, and dataset outputs"
	@echo "  make clean            Remove local Dagster state and Python caches"

check-python:
	@$(PYTHON) -c 'import sys; v=sys.version_info; ok=(3, 10) <= v[:2] < (3, 14); raise SystemExit(0 if ok else f"Python 3.10-3.13 required; got {sys.version.split()[0]} from $(PYTHON)")'

check-venv:
	@test -x "$(VENV)/bin/python" || (echo "Missing $(VENV). Run: make install" && exit 1)
	@$(VENV)/bin/python -c 'import sys; v=sys.version_info; ok=(3, 10) <= v[:2] < (3, 14); raise SystemExit(0 if ok else f"$(VENV) uses unsupported Python {sys.version.split()[0]}. Remove $(VENV) and rerun: make install")'

check-no-dev:
	@pids="$$( (lsof -nP -tiTCP:3000 -sTCP:LISTEN 2>/dev/null; lsof -t +D "$(DAGSTER_HOME)" 2>/dev/null) | sort -u )"; \
	if [ -n "$$pids" ]; then \
		echo "Dagster already appears to be running for this repo: $$pids"; \
		echo "Run: make stop-dev"; \
		exit 1; \
	fi

venv: check-python
	@if [ -x "$(VENV)/bin/python" ]; then \
		$(VENV)/bin/python -c 'import sys; v=sys.version_info; ok=(3, 10) <= v[:2] < (3, 14); raise SystemExit(0 if ok else f"$(VENV) uses unsupported Python {sys.version.split()[0]}. Remove $(VENV) and rerun: make install")'; \
	else \
		$(PYTHON) -m venv $(VENV); \
	fi
	$(PIP) install --upgrade pip

dagster-home:
	mkdir -p "$(DAGSTER_HOME)"
	cp dagster.yaml "$(DAGSTER_HOME)/dagster.yaml"

install: venv
	$(PIP) install -e ".[dev]"

seed-partitions: check-venv dagster-home
	DAGSTER_HOME="$(DAGSTER_HOME)" $(VENV)/bin/python -m dagster_pipeline.register_partitions

dev: check-venv check-no-dev dagster-home seed-partitions
	cd "$(DAGSTER_HOME)" && DAGSTER_HOME="$(DAGSTER_HOME)" $(DAGSTER) dev -w "$(CURDIR)/workspace.yaml"

stop-dev:
	@pids="$$( (lsof -nP -tiTCP:3000 -sTCP:LISTEN 2>/dev/null; lsof -t +D "$(DAGSTER_HOME)" 2>/dev/null) | sort -u )"; \
	if [ -n "$$pids" ]; then \
		echo "Stopping local Dagster processes: $$pids"; \
		kill $$pids; \
	else \
		echo "No local Dagster processes found for port 3000 or $(DAGSTER_HOME)"; \
	fi

test: check-venv
	$(PYTEST) dagster_pipeline_tests

prepare: check-venv dagster-home
	cd "$(DAGSTER_HOME)" && DAGSTER_HOME="$(DAGSTER_HOME)" $(DAGSTER) job execute -j prepare_chunks -m dagster_pipeline
	$(MAKE) seed-partitions

build: check-venv dagster-home seed-partitions
	cd "$(DAGSTER_HOME)" && DAGSTER_HOME="$(DAGSTER_HOME)" $(DAGSTER) job execute -j run_full_pipeline -m dagster_pipeline

export: check-venv dagster-home
	cd "$(DAGSTER_HOME)" && DAGSTER_HOME="$(DAGSTER_HOME)" $(DAGSTER) job execute -j export_dataset -m dagster_pipeline

article: check-venv dagster-home seed-partitions
	@test -n "$(ARTICLE)" || (echo "Usage: make article ARTICLE=94" && exit 1)
	cd "$(DAGSTER_HOME)" && DAGSTER_HOME="$(DAGSTER_HOME)" $(DAGSTER) asset materialize \
		--select 'synthetic_entries+' \
		--partition "$(ARTICLE)" \
		-m dagster_pipeline

revalidate: check-venv
	$(VENV)/bin/python -m dagster_pipeline.revalidate_existing

reset:
	mkdir -p data/synthetic data/validated data/datasets
	find data/synthetic data/validated data/datasets -type f -delete

clean:
	rm -rf .dagster_home .pytest_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
