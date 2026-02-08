# Contributing

Thanks for your interest in improving Blockscan! Contributions of all kinds are welcome — bug reports, feature requests, docs, and code.

## Getting Started
- Fork the repository and create a topic branch from `main`.
- Use Go 1.25.2.
- For local services (Redis, Kafka, Grafana, etc.), use Docker Compose:
  ```sh
  make infra-up
  make infra-ps
  ```
  Stop when finished:
  ```sh
  make infra-down
  ```

## Development Workflow
- Run the service locally with your config:
  ```sh
  CONFIG_NAME=local go run ./cmd
  ```
- Unit tests:
  ```sh
  make unit-test
  ```
- Integration tests (Redis/Kafka/Ganache):
  ```sh
  make integration-test
  ```
  Tip: use `-count=1` to avoid cached results for stateful tests.

## Code Style
- Go style: follow standard Go conventions (idiomatic Go, tabs for indentation). Use `gofmt`/`goimports` in your editor.
- Package names are lowercase; exported identifiers use PascalCase; unexported identifiers use camelCase.
- Prefer `context.Context` plumbing for cancellable operations.
- Log through the centralized `applogger.Logger` instead of instantiating raw loggers.
- Comments: use GoDoc-style comments above packages, types, functions, methods, or exported vars; avoid inline/block comments inside functions.

## Commits and PRs
- Use Conventional Commits (e.g., `feat(scanner): add finalized mode`, `fix(store): handle XADD error`). Keep the subject ≤ 72 chars.
- Reference issues with `#ID` when applicable.
- In PRs, describe change impact, verification steps (commands/configs), and include screenshots/log excerpts if behavior changes.
- Ensure tests pass locally before requesting review.

## Issues
- For bugs, include reproduction steps, expected vs. actual behavior, logs, and environment details.
- For features, describe the use case, proposed solution, and alternatives considered.

## Security
Please do not open public issues for security concerns. See `SECURITY.md` to report a vulnerability privately.

