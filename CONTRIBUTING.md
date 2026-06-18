# Contributing

Thanks for taking an interest in TaskForge.
The project is still early, so contributions are most useful when they tighten the runtime contract, public API, tests, docs, or operational behavior without widening the scope too quickly.

## Development Setup

Prerequisites:

- Go 1.25+
- Docker with Compose support
- Redis for integration tests

Useful commands:

```bash
make test
make lint
make race-test
TASKFORGE_RUN_INTEGRATION=1 make integration-test
TASKFORGE_RUN_BENCHMARKS=1 make bench
```

## Contribution Guidelines

- Keep the at-least-once execution contract explicit.
- Assume handlers must be idempotent.
- Prefer durable broker state over process-local bookkeeping.
- Keep public APIs small, documented, and covered by tests.
- Add or update tests alongside observable behavior changes.
- Keep unrelated refactors out of focused changes.

## Pull Requests

PRs should include:

- A short description of the behavior change.
- Validation performed, such as `make test` and `make lint`.
- Notes about reliability, lease, retry, scheduling, or compatibility impacts.
- Logs or screenshots only when API or operational output changed.

## Commit Style

Use concise imperative subjects, for example:

```text
Add public Redis worker API
```
