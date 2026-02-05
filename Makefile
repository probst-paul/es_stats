SHELL := /bin/bash

BREW_PG_FORMULA ?= postgresql@16
DB_NAME ?= es_stats
DB_HOST ?= localhost
DB_PORT ?= 5432
DB_USER ?= $(shell whoami)
ES_STATS_DATABASE_URL ?= postgresql://$(DB_USER)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)

.PHONY: dev-test db-up db-init test

# One command for local Homebrew Postgres + schema init + full test run.
dev-test: db-up db-init test

db-up:
	@brew services start $(BREW_PG_FORMULA)
	@psql postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$(DB_NAME)'" | grep -q 1 || createdb $(DB_NAME)

db-init:
	@ES_STATS_DATABASE_URL="$(ES_STATS_DATABASE_URL)" python -m es_stats.cli.main init-db

test:
	@ES_STATS_DATABASE_URL="$(ES_STATS_DATABASE_URL)" pytest -q
