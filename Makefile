SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c

COMPOSE := docker compose -f deploy/compose/docker-compose.yml

# Every directory containing a go.mod, excluding vendored or hidden paths.
MODULES := $(shell find . -name go.mod -not -path './.*' -exec dirname {} \; | sort)

.PHONY: up down proto lint test ready

## up: build images and start the local stack, waiting until every container is healthy
up:
	$(COMPOSE) up --build --wait

## down: stop the local stack and remove its containers
down:
	$(COMPOSE) down

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

## ready: check /readyz on every service and verify database role isolation
ready:
	bash deploy/compose/check-ready.sh
