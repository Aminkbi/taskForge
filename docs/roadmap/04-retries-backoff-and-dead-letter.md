# Phase 04: Retries, Backoff, and Dead-Letter Envelope

## Commit goal

Upgrade retries and dead-letter handling from a simple branch in the worker loop into a well-defined execution policy with operator-visible metadata.

## Why this phase exists

The current worker runtime retries by publishing another task and DLQs by queue-prefix convention. That is too shallow for production debugging and policy control.

## Changes

### Add failure classification

Introduce a runtime-visible failure model that distinguishes:

- transient retryable failure
- permanent failure
- lease-lost or timeout failure
- decode or validation failure

The worker should decide retry vs DLQ from this classification, not from a single generic error path.

### Improve retry policy

Upgrade [internal/tasks/retry_policy.go](../../internal/tasks/retry_policy.go) to support:

- exponential backoff
- jitter
- max deliveries
- max task age
- optional per-task overrides

Retry metadata should be stored in message headers or structured execution metadata, not reconstructed from logs.

### Replace the DLQ shortcut with a dead-letter envelope

Create a first-class dead-letter record that includes:

- original task payload
- failure class
- last error
- delivery count
- first enqueue time
- last failure time
- worker identity
- trace ID if available

### Prepare admin operations

Define service-level interfaces for:

- inspect dead-letter entries
- replay one entry
- replay a batch
- discard with audit trail

HTTP APIs can come later, but the internal shape should be designed in this phase.

## Tests

- Unit tests for retry policy jitter and cap behavior
- Integration test: retryable error schedules another attempt
- Integration test: permanent error goes directly to DLQ
- Integration test: max-delivery exhaustion moves task to DLQ

## Acceptance criteria

- Retry behavior is policy-driven and not hidden in ad hoc branches
- Dead-letter data is useful for operators, not just for requeueing
- Runtime paths remain explicit and testable
