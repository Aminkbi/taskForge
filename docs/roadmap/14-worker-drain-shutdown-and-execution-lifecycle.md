# Phase 14: Worker Drain, Shutdown, and Execution Lifecycle

## Commit goal

Make worker shutdown behavior explicit by defining drain states, in-flight execution handling, and lease semantics during controlled process stop.

## Why this phase exists

The current worker runtime is intentionally simple: when the parent context is canceled, reserve and dispatch loops stop and in-flight handlers observe cancellation through context propagation.

That is acceptable for an early at-least-once system, but it leaves too much operational behavior implicit. Large projects need a documented answer to a basic question: what exactly happens when an operator drains a worker, rolls a deployment, or shuts down a node under load?

TaskForge should make shutdown behavior explicit instead of leaving it as a side effect of local goroutine cancellation.

## Changes

### Define worker lifecycle states

Introduce explicit lifecycle states such as:

- `accepting`
- `draining`
- `stopped`

The runtime should make it clear when a worker is still reserving work, when it has stopped taking new deliveries, and when all owned execution has either completed or been abandoned.

### Stop reservation before stopping execution

Define controlled drain behavior as:

- stop reserving new deliveries first
- keep renewing owned leases while drain is active
- allow in-flight handlers to complete within a bounded shutdown window
- cancel and abandon remaining execution only after the drain deadline expires

This keeps at-least-once semantics intact while reducing unnecessary duplicate execution during normal deploys and restarts.

### Make shutdown outcomes operator-visible

Surface lifecycle outcomes such as:

- drained successfully
- forced shutdown after timeout
- abandoned running deliveries
- lease-renewal loss during drain

Operators should be able to distinguish a healthy drain from a crash-like stop.

### Clarify handler expectations

Document the execution contract clearly:

- handlers must remain idempotent
- handlers should respect `ctx.Done()`
- lease loss means local execution ownership is no longer authoritative
- a cancellation-insensitive handler may still produce duplicate side effects

This phase is about making that risk explicit and operationally visible.

## Tests

- Integration test: worker enters drain mode and stops reserving new deliveries immediately
- Integration test: in-flight work completes successfully when it finishes within the shutdown timeout
- Integration test: long-running work is canceled and abandoned predictably after the drain deadline
- Health and metric test: lifecycle state and shutdown outcome are visible through admin endpoints and metrics

## Acceptance criteria

- Worker drain semantics are documented rather than implied by context cancellation
- Controlled shutdown reduces avoidable duplicate work during deploys and restarts
- Operators can distinguish graceful drain from forced termination
