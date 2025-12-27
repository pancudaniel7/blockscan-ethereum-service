.PHONY: build test run integration-test unit-test docker-up docker-down

# Config selection: prefer GO_ENV if provided, default to "local".
CONFIG_NAME ?= $(GO_ENV)
CONFIG_NAME ?= local

COMPOSE_FILE := deployments/docker-compose.yml

# All packages except the top-level integration tests under ./test
UNIT_PKGS := $(shell go list ./... | grep -v '/test')

vet:
	go vet -v ./...

build:
	go build -v ./...

test:
	go fmt ./...
	CONFIG_NAME=test go test -v ./...

unit-test:
	CONFIG_NAME=test go test -v $(UNIT_PKGS)

integration-test:
	CONFIG_NAME=test go test -v ./test -count=1

run:
	CONFIG_NAME=$(CONFIG_NAME) go run -v ./cmd

docker-up:
	docker compose --verbose -f $(COMPOSE_FILE) up -d

docker-down:
	docker compose --verbose -f $(COMPOSE_FILE) down
