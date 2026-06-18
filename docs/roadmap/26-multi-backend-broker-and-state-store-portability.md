# Phase 26: Multi-Backend Broker and State-Store Portability

## Commit goal

Make Redis the first production backend, not the only possible backend, by hardening broker and state-store contracts with conformance tests.

## Why this phase exists

Celery's longevity comes partly from broker and result-backend flexibility. TaskForge should support portability without inheriting ambiguous semantics from every backend. The contract should remain TaskForge's contract: at-least-once delivery, delivery-owner acks, durable delayed release, observable retries, and explicit state retention.

The current `broker.Broker` interface is intentionally small, while Redis-specific code owns many advanced behaviors. This phase decides which behaviors become core backend requirements and which stay optional capabilities.

## Changes

### Define backend capability interfaces

Split backend support into explicit capabilities:

- active queue delivery
- delayed and retry scheduling
- durable task state
- result payload retention
- deduplication receipts
- fairness queues
- dependency budgets
- leadership and fencing
- workflow graph state

Backends should declare capabilities so unsupported features fail clearly.

### Build a broker conformance suite

Create a reusable suite that validates:

- publish, reserve, ack, nack, and lease extension
- stale ack rejection
- redelivery after lease expiry
- deduplication behavior
- delayed release correctness
- retry and DLQ transitions
- task-state transitions and retention

Redis should pass this suite first. Future backends should not be accepted without it.

### Evaluate candidate backends deliberately

Document tradeoffs for:

- Redis Streams for low-latency operational simplicity
- Postgres for transactional state and outbox alignment
- NATS JetStream for streaming and distributed delivery
- Kafka-compatible logs for high-throughput event retention

Do not add a backend just to increase the count. Each backend must carry its semantics honestly.

### Separate data encoding from Redis internals

Stabilize message, delivery, task-state, workflow-state, and effect-record encodings so multiple backends and SDKs can share them.

## Tests

- Conformance test: Redis passes the core broker behavior suite
- Conformance test: Redis state store passes retention and transition invariants
- Unit test: unsupported backend capability returns a typed error
- Integration test: dedupe and stale-ack behavior match the documented backend contract
- Documentation test: backend capability matrix matches implemented features

## Acceptance criteria

- Backend semantics are defined by TaskForge, not by accidental Redis behavior
- Redis remains production-grade while the architecture permits additional backends
- New backends have a clear acceptance bar through conformance tests
