# Logical Routing and Sharding Operations

This page owns placement operations. The routing-policy schema and examples
belong to the [configuration reference](../reference/configuration.md).

TaskForge chooses placement before a new task enters Redis. Retries, delayed
releases, recurrence, DLQ flows, and requeues retain the task's existing queue.
Logical shard metadata identifies placement for operators; current Redis keys
remain queue-scoped.

This is application-level placement, not Redis Cluster support. TaskForge
supports only a direct connection to standalone Redis; see the [Redis operating
model](redis.md).

## Boundaries

- Local: worker concurrency, prefetch, drain, and pool task-type limits.
- Queue/control plane: depth, reservations, delayed/retry indexes, admission,
  fairness, adaptive signals, and dependency budgets.
- Global: routing-policy distribution, tenant/traffic placement, and deliberate
  cross-shard rebalance.

Fairness protects tenants that share a queue; it is not global across queues or
shards.

## Operating procedure

1. Identify whether pressure is in a queue, logical shard, or Redis control
   plane. Scale a hot queue with worker replicas or safe pool concurrency first.
2. Isolate or spread a hot tenant or traffic class with a routing rule for new
   work. Watch the old queue's ready, delayed, retry, and DLQ backlog drain.
3. Treat shard-local overload locally. Admission, fairness, adaptive
   concurrency, and retries use destination-queue state; capacity is not
   borrowed from another shard without an explicit routing change.
4. When a tenant spans shards, compare per-shard metrics before changing policy.

Move tenants deliberately: in-flight work and scheduled retries stay at their
original placement unless an operator republishes them under a new policy.
