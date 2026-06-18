# Phase 17: Queryable Task State and Result Retention

## Commit goal

Turn task state and result storage from a sketch into an explicit product surface with durable lookup, retention policy, and operator-oriented task inspection.

## Why this phase exists

The repository already contains a result-store abstraction, but it is still only a sketch. That leaves a gap between the runtime's delivery semantics and the operator or caller experience after a task has run.

Large projects usually need more than fire-and-forget execution. They need to inspect task progress, query final state, understand failure history, and reason about retention and replay.

TaskForge should decide what task-state visibility is part of the product instead of leaving it implicit or fragmented across logs, metrics, and broker internals.

## Changes

### Define a durable task-state model

Specify what becomes queryable durable state, including:

- logical task ID
- current terminal or non-terminal state
- last error
- timestamps such as created, started, completed, and last-updated
- optional result payload or metadata

Keep delivery-attempt detail separate from the higher-level task view where that distinction matters.

### Define retention and cleanup policy

Make retention explicit for:

- successful task state
- failed task state
- dead-letter references
- large or optional result payloads

Operators should know what is retained, for how long, and what cleanup path removes it.

### Add operator and API lookup use cases

Support query flows such as:

- fetch task state by task ID
- inspect latest error and retry history summary
- trace dead-lettered tasks to their stored task record
- determine whether a client-visible task has finished successfully

This phase is about observability and operability, not exactly-once semantics.

### Clarify replay and mutation boundaries

Document what is and is not allowed:

- querying state is stable and supported
- replay is an explicit operator action
- task-state mutation is controlled by runtime transitions rather than arbitrary external writes

That keeps the surface coherent as the system grows.

## Tests

- Integration test: task state transitions are persisted and queryable across success, retry, and dead-letter paths
- Integration test: retention rules expire or preserve task records as configured
- API test: task lookup returns stable task-level state independent of delivery duplication
- Integration test: dead-letter inspection can locate the corresponding durable task record

## Acceptance criteria

- TaskForge has a documented durable task-state surface instead of a placeholder store abstraction
- Operators and callers can inspect task outcomes without scraping logs or broker internals
- Retention and replay boundaries are explicit and testable
