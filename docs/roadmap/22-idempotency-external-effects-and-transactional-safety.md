# Phase 22: Idempotency, External Effects, and Transactional Side-Effect Safety

## Commit goal

Turn idempotency from guidance into an implementable framework for handlers that perform external side effects.

## Why this phase exists

TaskForge correctly states that handlers must be idempotent because execution is at-least-once. That is honest, but it leaves too much burden on every application team. A system meant to surpass Celery should provide a strong side-effect safety kit: idempotency keys, effect logs, dedupe stores, transactional outbox integration, and replay diagnostics.

This phase does not promise impossible exactly-once execution against arbitrary external systems. It narrows the problem into explicit effect protocols operators and developers can reason about.

## Changes

### Define an effect record model

Add durable records for external effects:

- task ID, delivery ID, and idempotency key
- effect name and target dependency
- prepared, committed, failed, compensated, or unknown state
- request fingerprint and optional response metadata
- first seen, last attempted, and completed timestamps

Effect records should be queryable alongside task state and workflow node state.

### Provide handler-side idempotency helpers

Expose a small public package for:

- reserving an idempotency key before an external call
- recording a successful external commit
- replaying a cached result when the same key is seen again
- classifying ambiguous failures where the external system may have committed

The helpers should be useful without forcing every handler into one framework.

### Support transactional outbox and inbox patterns

Document and implement reference integrations for:

- database transaction writes plus task publish through an outbox
- consuming external events through an inbox dedupe table
- replay-safe task publication using `PublishOptions.DeduplicationKey`

This is the path that lets TaskForge be used in high-integrity business systems without pretending Redis alone can coordinate every side effect.

### Add compensation hooks for workflows

For workflow nodes with external effects, allow optional compensation task metadata. Compensation should be explicit, best-effort, and observable. It should not be automatically run unless the workflow policy says so.

### Expose unsafe replay warnings

The API and CLI should warn when replaying or retrying tasks with:

- no idempotency key
- unknown effect state
- nondeterministic output feeding downstream work
- external side effects without compensation metadata

## Tests

- Unit test: idempotency helper returns cached success for repeated keys
- Integration test: worker crash after external-effect prepare is surfaced as ambiguous instead of silently retried as clean
- Integration test: outbox publish deduplicates repeated dispatcher attempts
- API test: replay warnings include effect-state and idempotency metadata
- Workflow test: compensation hooks are scheduled only under declared policy

## Acceptance criteria

- Developers have a supported path for replay-safe external effects
- Operators can inspect ambiguous side-effect state before replaying work
- Transactional outbox and inbox patterns are documented and covered by examples
- TaskForge keeps its at-least-once contract while reducing application-level footguns
