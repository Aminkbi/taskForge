# Runnable Examples

TaskForge now ships three runnable examples that compile against the current internal broker/runtime wiring. Each example assumes local Redis and avoids any external network dependency.

## Idempotent email dispatch

Command:

```bash
make run-example-email
```

What it does:

- publishes the same logical email task twice
- uses an in-memory idempotency store keyed by `IdempotencyKey`
- runs one worker that acknowledges both deliveries but only performs one actual send

Operational point:

- duplicate deliveries still happen at the broker layer
- the handler prevents duplicate side effects by claiming the idempotency key before send and only marking it complete after a successful send

Policy choices:

- idempotency strategy: explicit `IdempotencyKey`
- retry policy: none beyond a single delivery in the example
- timeout policy: default worker lease only
- duplicate behavior: duplicate deliveries are acknowledged, but only the first send is executed

## Long-running media processing with lease renewal

Command:

```bash
make run-example-media
```

What it does:

- publishes one media task with a short lease TTL
- runs a handler longer than one lease-renew interval
- attempts a competing reserve while the task is still executing

Operational point:

- the competing reserve should return `broker.ErrNoTask` because the active worker extends the lease before reclaim becomes valid
- this demonstrates that long-running tasks can remain owned by one worker when lease renewal is functioning

Policy choices:

- idempotency strategy: asset-level task identity
- retry policy: none in the example
- timeout policy: handler duration intentionally exceeds lease TTL, relying on runtime renewal
- duplicate behavior: if lease renewal failed in production, another worker could reclaim and re-run the asset; this example shows the healthy path

## Retryable external API task with DLQ and replay

Command:

```bash
make run-example-external-api
```

What it does:

- runs a local fake upstream dependency that starts unavailable
- publishes one task with a short retry backoff and `max_deliveries=2`
- lets the task fail into DLQ
- flips the fake dependency healthy
- replays the DLQ entry and lets the worker succeed

Operational point:

- retry scheduling still preserves at-least-once semantics
- once retries are exhausted, operators can inspect the DLQ entry and replay it after the dependency recovers

Policy choices:

- idempotency strategy: resource-level task identity
- retry policy: two deliveries, fixed `50ms` retry delay
- timeout policy: default worker lease only
- duplicate behavior: replay is a fresh publish and may re-run side effects if the handler is not written idempotently

## Notes

- The examples choose dedicated Redis DBs when `TASKFORGE_REDIS_DB` is not explicitly set:
  - email: `12`
  - media: `13`
  - external API: `11`
- The demo binary still defaults to Redis DB `15`, and the benchmark harness defaults to DB `14`.
- These examples reflect the current internal wiring, not a stable public API.
