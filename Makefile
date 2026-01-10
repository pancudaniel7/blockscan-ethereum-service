.PHONY: build test run integration-test unit-test docker-up docker-down lint lint-install

# Config selection: prefer GO_ENV if provided, default to "local".
CONFIG_NAME ?= $(GO_ENV)
CONFIG_NAME ?= local

COMPOSE_FILE := deployments/docker-compose.yml

# All packages except the top-level integration tests under ./test
UNIT_PKGS := $(shell go list ./... | grep -v '/test')

# Linter settings
LINTER ?= golangci-lint
GOLANGCI_LINT_VERSION ?= v1.60.3

vet:
	go vet -v ./...

build:
	go build -v ./...

test:
	CONFIG_NAME=test go test -v ./...

lint:
	@command -v $(LINTER) >/dev/null 2>&1 || { \
		echo "$(LINTER) not found. Install it via: make lint-install"; \
		exit 1; \
	}
	$(LINTER) run ./...

lint-install:
	GO111MODULE=on go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

unit-test:
	CONFIG_NAME=test go test -v $(UNIT_PKGS)

integration-test:
	CONFIG_NAME=test go test -v ./test -count=1

run:
	CONFIG_NAME=$(CONFIG_NAME) go run -v ./cmd

docker-dev-up:
	docker compose --verbose -f $(COMPOSE_FILE) up -d redis redis-provisioner kafka kafka-provisioner kafka-ui redis-commander

docker-down:
	docker compose --verbose -f $(COMPOSE_FILE) down

docker-up:
	docker compose --verbose -f $(COMPOSE_FILE) build blockscan-replica1 blockscan-replica2
	docker compose --verbose -f $(COMPOSE_FILE) up -d --build --force-recreate grafana blockscan-replica1 blockscan-replica2
	docker compose --verbose -f $(COMPOSE_FILE) up -d \
		redis redis-provisioner kafka kafka-provisioner kafka-ui redis-commander \
		prometheus loki promtail telegraf docker-proxy
