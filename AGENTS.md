# Repository Guidelines

## Project Structure & Module Organization
- `cmd/main.go` wires the runtime and hosts the CLI entrypoint.
- `internal/pkg` contains reusable packages such as `applogger` and `apperror`; introduce new modules under domain-centric subfolders to keep boundaries clear.
- `configs/*.yml` stores environment presets consumed via Viper; extend with additional files instead of inlining constants.
- `deployments/` holds operational assets (`docker-compose.yml`, Redis function loader) used for local provisioning.
- `docs/` provides architecture references, and `test/` houses integration helpers targeting Redis Lua functions.
- `test/` houses unit and integration tests.

## Build, Test, and Development Commands
- `go fmt ./... && go test ./...` keeps code formatted and verifies unit and integration suites.
- `go build ./...` ensures all packages compile with Go 1.25.2.
- `go run ./cmd` executes the service entrypoint; pass `GO_ENV=local` to pick up `configs/local.yml`.
- `docker compose -f deployments/docker-compose.yml up redis redis-provisioner` starts Redis and loads the `add_block` Lua library for end-to-end testing.

## Coding Style & Naming Conventions
- Rely on `gofmt` (tabs for indentation) and `goimports` to manage formatting and imports; CI expects canonical Go style.
- Package names stay lowercase with no underscores; exported identifiers use PascalCase, private identifiers camelCase.
- Group configuration constants under typed structs and prefer `context.Context` plumbing for cancellable operations.
- Log through `applogger.Logger` to keep slog configuration centralized instead of instantiating raw loggers.
- Comments: Do not add comments inside function bodies. Only GoDoc-style comments (above packages, types, functions, methods, or exported vars) are allowed; avoid inline/block comments within functions.
- Don't format the code using go fmt or any other format tool or command.

## Testing Guidelines
- Primary tests live under `test/` and use `stretchr/testify`; name test functions `Test<FeatureScenario>`.
- Integration tests expect a reachable Redis with the `add_block` function loaded; start the provided Docker compose stack or point `REDIS_HOST`/`REDIS_PORT` to an existing node.
- Run `go test ./... -count=1` when iterating on Lua or Redis stateful tests to avoid cached results; collect logs for flaky behaviour.

## Commit & Pull Request Guidelines
- Follow the observed conventional commits style (`feat(scope): description`); keep the first line ≤72 chars and reference issues with `#ID` when applicable.
- Each PR should explain the change impact, list verification steps (commands and configs), and include screenshots or log excerpts if behaviour changes.
- Ensure tests and linters pass locally before requesting review; note any follow-up work or config migrations in the PR description.
