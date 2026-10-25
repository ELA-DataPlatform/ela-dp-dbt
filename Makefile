.DEFAULT_GOAL := help

IMAGE ?= ela-dp-dbt:local
TARGET ?= dev
SELECT ?=
ARGS ?=
PORT ?= 8080
PLATFORM ?=

export DBT_DOCKER_IMAGE = $(IMAGE)
export DBT_TARGET = $(TARGET)
export DBT_SELECT = $(SELECT)
export DBT_DOCS_PORT = $(PORT)
export DBT_DOCKER_PLATFORM = $(PLATFORM)

.PHONY: help setup lint-sql fix-sql lint-yaml hooks parse-local check \
	docker-build docker-pull version deps debug ls parse compile run build test \
	freshness docs docs-serve dbt

help:
	@echo "setup       Install locked dependencies, dbt packages and Git hooks"
	@echo "lint-sql    Check maintained SQL with the same command as CI/hooks"
	@echo "fix-sql     Apply SQLFluff formatting explicitly"
	@echo "lint-yaml   Check repository YAML, excluding generated dependencies"
	@echo "hooks       Run all pre-commit checks on tracked files"
	@echo "parse-local Parse offline using uv.lock (same as the CI parse job)"
	@echo "check       Run SQL lint, YAML lint and offline parse via uv"
	@echo ""
	@echo "dbt in Docker (IMAGE=$(IMAGE), TARGET=$(TARGET)):"
	@echo "docker-build  Build the local image from this repository's Dockerfile"
	@echo "docker-pull   Pull IMAGE from its registry"
	@echo "version/deps/debug/ls/parse/compile/run/build/test/freshness"
	@echo "docs          Generate the catalog in target/"
	@echo "docs-serve    Serve it at http://localhost:$(PORT)"
	@echo "dbt           Run a custom command, e.g. make dbt ARGS='ls --select tag:health'"
	@echo ""
	@echo "Options: SELECT=tag:activities, ARGS='--full-refresh', IMAGE=..., PLATFORM=..."

setup:
	uv sync --locked
	uv run --locked dbt deps --profiles-dir .
	uv run --locked pre-commit install

lint-sql:
	./scripts/sqlfluff.sh lint

fix-sql:
	./scripts/sqlfluff.sh fix

lint-yaml:
	uv run --locked yamllint -c .yamllint.yml .

hooks:
	uv run --locked pre-commit run --all-files

parse-local:
	uv run --locked dbt parse --profiles-dir . --target dev

check: lint-sql lint-yaml parse-local

docker-build:
	docker build $(if $(PLATFORM),--platform "$(PLATFORM)") --tag "$(IMAGE)" .

docker-pull:
	docker pull $(if $(PLATFORM),--platform "$(PLATFORM)") "$(IMAGE)"

version:
	./scripts/dbt-docker.sh --version

deps:
	./scripts/dbt-docker.sh deps $(ARGS)

debug ls parse compile run build test:
	./scripts/dbt-docker.sh $@ $(ARGS)

freshness:
	./scripts/dbt-docker.sh source freshness $(ARGS)

docs:
	./scripts/dbt-docker.sh docs generate $(ARGS)

docs-serve:
	./scripts/dbt-docker.sh docs serve --host 0.0.0.0 --port 8080 --no-browser $(ARGS)

dbt:
	./scripts/dbt-docker.sh $(ARGS)
