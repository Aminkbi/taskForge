# Phase 01: Execution Contract and Broker State Model

## Commit goal

Make the execution contract explicit and reshape the internal broker model so later Redis and runtime work has a stable target.

This commit is documentation plus internal type and test refactoring. It should not yet depend on Redis Streams.

## Why this phase exists

The current broker model only tracks:

- task ID
- queue
- consumer ID
- lease token
- lease expiry

That is not enough to reason about duplicate delivery, late ack, reclaim history, or operator-visible recovery state.

## Changes

### Define the execution contract

Document, in code comments and docs, that TaskForge guarantees:

- `at-least-once` delivery
- duplicates are possible
- handlers must be idempotent
- successful completion means handler success followed by durable ack
- exactly-once is out of scope

### Extend internal execution metadata

Introduce internal concepts that distinguish:

- `task_id`: logical task identity
- `delivery_id`: a specific delivery attempt or lease ownership instance
- `delivery_count`: total number of deliveries observed
- `first_enqueued_at`
- `leased_at`
- `lease_expires_at`
- `lease_owner`
- `last_error`

This should be modeled in broker-facing types, even if some fields are not fully populated until later phases.

### Clarify state transitions

Write the allowed transitions explicitly:

- `queued -> leased`
- `leased -> running`
- `running -> succeeded`
- `running -> retry_scheduled`
- `running -> dead_lettered`
- `leased -> queued` after lease expiry and reclaim eligibility

Late ack after a lost lease must be defined now. Recommended rule: the broker rejects or ignores it deterministically, but never lets it incorrectly finalize a newer delivery owner.

### Tighten runtime assumptions

Refactor [internal/runtime/worker.go](../../internal/runtime/worker.go) and related runtime helpers so the worker can reason about:

- delivery identity
- lease ownership
- lease expiry
- retry classification

This phase should avoid behavior changes beyond type and state-shape preparation.

## Tests

- Unit tests for valid and invalid state transitions
- Unit tests for late-ack decisions
- Unit tests for expired-lease decisions
- Unit tests for idempotency metadata defaults

## Acceptance criteria

- The codebase has a documented execution contract
- Internal broker/runtime types can represent durable recovery state
- Existing binaries still compile and existing tests still pass
- No Redis Streams logic is introduced yet
