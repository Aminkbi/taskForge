# Repository Guidelines

## Project Structure & Module Organization
TaskForge is a Go module with three entrypoints under `cmd/`: `worker`, `scheduler`, and `api`. Core runtime code lives in `internal/`, with key packages including `internal/runtime` for worker execution, `internal/broker` and `internal/brokerredis` for queue contracts and Redis transport, `internal/scheduler` for delayed and retry flows, and `internal/config` for environment-driven settings. Shared public surface area belongs in `pkg/taskforge/`. Integration coverage sits in `test/integration/`, operational assets in `deploy/docker/`, and design notes in `docs/roadmap/`.

## Build, Test, and Development Commands
Use the `Makefile` or scripts as the default interface:

- `make run-worker`, `make run-scheduler`, `make run-api`: run individual services locally.
- `make test` or `./scripts/test.sh`: run the unit test suite with `go test ./...`.
- `make lint` or `./scripts/lint.sh`: run `go vet`, check `gofmt`, and run `staticcheck` when installed.
- `make fmt`: format the repo with `gofmt -w .`.
- `make compose-up` / `make compose-down`: start or stop the local Docker stack.

## Coding Style & Naming Conventions
Follow standard Go formatting and import ordering; `gofmt` is required. Use tabs for indentation, keep package names short and lowercase, and prefer descriptive exported identifiers such as `RetryPolicy` or `TaskMessage`. Name files by responsibility (`redis.go`, `worker_test.go`), keep command wiring in `internal/app/*`, and use environment variables with the `TASKFORGE_` prefix.

## Testing Guidelines
Write table-driven tests where practical and prefer `t.Parallel()` for independent unit tests. Keep unit tests next to the package they cover in `*_test.go` files. Integration tests live in `test/integration/` and are opt-in: run them with `TASKFORGE_RUN_INTEGRATION=1 go test ./test/integration/...` and a local Redis on `localhost:6379`.

## Commit & Pull Request Guidelines
Recent commits use concise, imperative subjects such as `Implement execution contract and broker state model`. Keep commit titles short, present tense, and scoped to one change. PRs should explain behavior changes, list validation performed (`make test`, `make lint`), link relevant issues, and include logs or screenshots only when API or operational output changed.

## Configuration & Ops Notes
Configuration is environment-based through `internal/config`; prefer adding new settings there instead of hardcoding values. Preserve the explicit at-least-once execution model documented in `README.md`, and call out any reliability or lease-semantics changes in PR descriptions.

## Future Work Planning
After a big work is done and there are improvements that are differed to later time during planning and execution, create a proper file
under docs/roadmap somewhere (DONT TOUCH PREVIOUS ROADMAP FILES ONLY APPEND)
Do not commit the roadmap docs unless the user explicitly asks for them to be included.


## Codex Token Discipline
- Prefer targeted discovery before opening files: `fd`, `rg --files`, `rg`, and `ctags` indexes when useful.
- Prefer `ast-grep` for syntax-aware code searches before broad text searches.
- Avoid dumping whole files, generated files, dependency directories, or large command output into context.
- Open only the smallest relevant file sections and summarize bulky output.
- Keep tool use narrow: use precise globs, patterns, and bounded result counts.

## Codex Implementation Discipline
- Do not rewrite unrelated code while completing a focused task.
- Reuse existing package patterns before introducing new abstractions.
- Add tests before or alongside fixes when the behavior is observable.
- Run the narrowest relevant test first, then broader validation if the change is cross-cutting.
- Report commands run and any tests not run in the final response.
- Do not silently ignore failing tests, lint errors, generated-file drift, or
