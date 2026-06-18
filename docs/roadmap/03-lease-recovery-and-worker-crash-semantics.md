# Phase 03: Lease Recovery and Worker Crash Semantics

## Commit goal

Finish the core durability story for active work: reclaim orphaned deliveries, enforce lease ownership, and make worker crash behavior explicit and observable.

## Why this phase exists

Moving to Redis Streams is necessary but not sufficient. Production behavior depends on how pending messages are reclaimed and how workers renew ownership while they are still healthy.

## Changes

### Add reclaim loop

Implement a reclaim component that:

- inspects pending deliveries
- identifies entries idle longer than the lease TTL
- reclaims them with `XAUTOCLAIM`
- increments delivery count
- emits logs and metrics for each reclaim decision

This can live in the worker process or in a dedicated recovery loop, but the ownership model must be consistent either way.

### Implement durable lease extension

Replace the current in-process extension assumption with durable heartbeat or lease-renew semantics against Redis.

The lease model should define:

- reserve time
- expiry time
- renewal interval
- maximum tolerated idle interval before reclaim

### Enforce ownership rules

Define and implement:

- ack by current delivery owner succeeds
- ack by stale delivery owner is rejected or ignored
- nack by stale owner is rejected or ignored
- reclaimed deliveries are eligible for a new execution attempt

### Improve worker identity and logs

Emit worker identity, delivery ID, and lease expiry consistently in logs so operators can reconstruct what happened during crashes and reclaims.

## Tests

- Integration test: worker crash before ack leads to reclaim
- Integration test: reclaimed task is delivered to another consumer
- Integration test: stale ack after reclaim does not finalize the task incorrectly
- Integration test: active lease extension prevents reclaim for a long-running task

## Acceptance criteria

- Worker crash recovery is durable and test-covered
- Lease ownership no longer depends on local process memory
- The system can explain what happens after worker death without ambiguity
