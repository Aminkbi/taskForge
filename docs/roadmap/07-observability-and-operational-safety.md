# Phase 07: Observability and Operational Safety

## Commit goal

Make production operation practical by adding broker-aware metrics, trace propagation, structured recovery logs, and readiness behavior that reflects the real state of the system.

## Why this phase exists

The current observability baseline is good for a scaffold, but it does not yet answer the questions operators will ask during incidents:

- Are tasks stuck pending?
- Are leases being reclaimed?
- Are retries spiking?
- Is the scheduler leader healthy?
- Which worker owned the last failed delivery?

## Changes

### Expand Prometheus metrics

Add metrics for:

- queue depth
- pending lease count
- reclaim count
- lease extension failures
- retry schedule count
- dead-letter size
- scheduler lag
- active consumers

Prefer low-cardinality labels:

- queue
- task name
- result class

Avoid task ID labels.

### Add tracing around queue lifecycle

Instrument publish, reserve, execute, ack, retry, reclaim, and DLQ transitions with OpenTelemetry spans.

Propagate trace context through task headers so a delivery can be tied back to the originating request or upstream job.

### Improve structured logging

Standardize log fields:

- task ID
- delivery ID
- queue
- worker identity
- delivery count
- lease expiry
- trace ID when present

### Tighten readiness and safety checks

Readiness should eventually reflect:

- Redis connectivity
- required scheduler leadership state
- internal recovery loop health

Liveness should remain shallow and safe.

## Tests

- Unit tests for metrics registration and label safety
- Integration test: trace context survives publish to execute
- Integration test: reclaim and DLQ paths emit expected counters
- Smoke test: readiness changes on broker unavailability where intended

## Acceptance criteria

- Operators can diagnose stuck leases, retry storms, and DLQ growth from metrics and logs
- Trace context is preserved across asynchronous execution
- Readiness semantics are intentional rather than placeholder-only
