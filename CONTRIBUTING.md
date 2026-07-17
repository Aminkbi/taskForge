# Contributing

TaskForge is early-stage. Keep contributions scoped to one observable behavior.

## Development Setup

Prerequisites: Go 1.26.5+, Docker Compose, and Redis for integration tests.
Start with the [architecture map](docs/development/agent-context.md). Common checks:

```bash
make test
make lint
make race-test
make integration-test # requires Redis on localhost:6379
```

## Pull Requests

Keep public APIs small and tested, and avoid unrelated refactors. Include a
short behavior summary, validation performed, and delivery, lease, retry,
scheduling, or compatibility implications. Use a concise, imperative commit
subject.
