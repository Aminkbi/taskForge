# Phase 10: Tenant-Aware Fairness and Quotas

## Commit goal

Add tenant-aware scheduling policy so shared queues can enforce fair access, reserved capacity, and quotas instead of relying only on queue isolation and static task caps.

## Why this phase exists

Phase 06 added queue isolation and per-task concurrency limits. That protects critical queues, but it does not answer a harder production question: when multiple tenants or traffic classes share a queue, who gets capacity under bursty or adversarial load?

Many job systems leave this to queue sharding, manual routing, or paid add-ons. TaskForge should make the policy explicit instead of forcing operators to approximate fairness with queue sprawl.

## Changes

### Introduce a stable fairness key

Extend publish and runtime policy so work can be classified by a low-cardinality key such as:

- `tenant_key`
- `traffic_class`

This key is for scheduling and admission policy, not for high-cardinality tracing or metrics.

### Add weighted fair sharing

Support policy that can express:

- weighted shares across tenants or classes
- reserved capacity for protected traffic
- hard and soft quotas
- bounded burst allowance

The scheduler and worker runtime should make fair-progress decisions from durable broker-visible state rather than process-local guesswork.

### Define starvation and burst behavior

Document the intended fairness contract clearly:

- one noisy tenant should not starve another indefinitely
- unused reserved capacity may be borrowed temporarily
- tenants that exceed quota should be slowed predictably rather than causing opaque contention

### Add operator visibility

Expose fairness-specific observability such as:

- queued and reserved work by fairness key
- quota rejections or deferrals
- share utilization
- starvation indicators

Prefer low-cardinality labels and keep tenant identifiers optional where operators only need class-level fairness.

## Tests

- Integration test: one tenant cannot indefinitely starve another on a shared queue
- Integration test: reserved capacity remains available for protected traffic during a burst
- Integration test: tenants above quota are throttled while compliant tenants continue to make progress
- Metric test: fairness counters and gauges remain low-cardinality and stable

## Acceptance criteria

- Shared queues can enforce a documented fairness policy without requiring one queue per tenant
- Fairness behavior under burst and contention is explicit and measurable
- Operators can reason about quota pressure and starvation from metrics and logs
