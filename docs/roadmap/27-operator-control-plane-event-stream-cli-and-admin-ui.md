# Phase 27: Operator Control Plane, Event Stream, CLI, and Admin UI

## Commit goal

Build a coherent operator experience across HTTP APIs, event streams, CLI workflows, and a small admin UI.

## Why this phase exists

TaskForge has health endpoints, metrics, task state, scheduler leadership visibility, and runbooks. That is enough for maintainers, but production users need a higher-level control plane: inspect workers, watch task events, replay safely, drain pools, understand admission decisions, and debug workflows without reading Redis keys.

Celery's events, inspect/control commands, and Flower ecosystem set user expectations here. TaskForge should provide a tighter, safer version that respects leases, fences, and replay risk.

## Changes

### Define an event stream

Emit structured events for:

- task published, leased, started, succeeded, failed, retried, dead-lettered, canceled, and replayed
- worker online, heartbeat, draining, stopped, and lease-loss events
- scheduler leadership changes and stale-write rejections
- admission, dependency budget, and adaptive concurrency decisions
- workflow node and graph transitions

Events should include stable IDs and enough context to reconstruct recent cluster state.

### Add operator APIs

Expose supported endpoints for:

- task lookup and filtered search
- workflow lookup and graph state
- worker and pool inventory
- queue depth, lag, fairness, admission, and dependency health
- DLQ inspect, annotate, replay, and discard
- drain, pause, resume, and quiesce operations with audit reasons

Unsafe operations should require explicit confirmation metadata even in API form.

### Build a CLI around real workflows

Add a `taskforge` CLI for:

- `taskforge queues`
- `taskforge workers`
- `taskforge tasks get`
- `taskforge workflows get`
- `taskforge dlq list/replay/discard`
- `taskforge pools drain/pause/resume`
- `taskforge events tail`

The CLI should be script-friendly and return structured output with `--json`.

### Add a restrained admin UI

Provide a small UI focused on operations:

- queues and lag
- workers and lifecycle state
- task and workflow search
- DLQ triage
- admission and dependency health
- scheduler leadership

The UI should not become the only way to operate the system.

## Tests

- API test: operator endpoints return stable schemas and enforce unsafe-action confirmation
- Integration test: event stream can reconstruct task and worker state over a bounded window
- CLI test: commands support human and JSON output
- UI smoke test: main views render against fixture data
- Audit test: replay, discard, pause, and drain operations record actor and reason

## Acceptance criteria

- Operators can inspect and control TaskForge without Redis spelunking
- Event, API, CLI, and UI surfaces use one coherent state model
- Dangerous operations are audited and guarded by explicit semantics
