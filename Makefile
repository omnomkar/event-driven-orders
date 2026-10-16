SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c

# Build and test each module through its own go.mod, exactly as CI and Docker do.
export GOWORK := off

COMPOSE := docker compose -f deploy/compose/docker-compose.yml
COMPOSE_ENV := deploy/compose/.env

# Every directory containing a go.mod, excluding vendored or hidden paths.
MODULES := $(shell find . -name go.mod -not -path './.*' -exec dirname {} \; | sort)

.PHONY: up down proto lint test test-integration ready

## up: build images and start the local stack, waiting until every container is healthy
up: $(COMPOSE_ENV)
	$(COMPOSE) up --build --wait

## down: stop the local stack and remove its containers and volumes
down:
	$(COMPOSE) down --volumes

# Local dev passwords: create .env from the committed example on first use.
$(COMPOSE_ENV):
	cp deploy/compose/.env.example $@

## proto: lint, format, and regenerate Go code from proto/ (stale generated files are removed first)
proto:
	cd proto && buf lint && buf format -w && rm -rf gen/go/eventorders && buf generate
	cd proto/gen/go && go mod tidy

## lint: run golangci-lint in every Go module
lint:
	@for m in $(MODULES); do echo "==> lint $$m"; (cd $$m && golangci-lint run ./...); done

## test: run go test with the race detector in every Go module
test:
	@for m in $(MODULES); do echo "==> test $$m"; (cd $$m && go test -race ./...); done

## test-integration: run the integration tests (needs Docker; starts real Postgres and Kafka containers)
test-integration:
	@for m in $(MODULES); do echo "==> integration $$m"; (cd $$m && go test -tags integration -race -count=1 ./...); done

## ready: check /readyz on every service and verify database role isolation
ready:
	bash deploy/compose/check-ready.sh
