# Cluster Routing and Sharding Operating Model

TaskForge routes work before it enters Redis. Workers still reserve from queues, and schedulers still operate on queue-scoped delayed, retry, and recurring state. A routing policy makes the placement decision explicit instead of relying on every publisher to remember queue naming conventions.

## Routing Policy

Set `TASKFORGE_ROUTING_POLICY_JSON` on publishers and services that can publish new work.

```json
{
  "default_queue": "default",
  "default_shard": "shard-a",
  "rules": [
    {
      "name": "critical-eu",
      "match": {
        "task_names": ["billing.charge"],
        "fairness_keys": ["tenant-vip"],
        "traffic_classes": ["critical"],
        "headers": {"region": "eu"}
      },
      "destination": {
        "queue": "critical",
        "shard": "shard-eu"
      }
    },
    {
      "name": "bulk-tenant-spread",
      "match": {"traffic_classes": ["bulk"]},
      "destination": {
        "queue": "bulk",
        "shards": ["bulk-a", "bulk-b", "bulk-c"],
        "shard_by": "fairness_key"
      }
    }
  ]
}
```

Rules are evaluated in order. A rule can match `task_names`, source `queues`, `fairness_keys`, `traffic_classes`, and exact header values. Traffic class is read from the `taskforge_traffic_class` header.

Destinations can set a queue, one fixed logical shard, or a list of logical shards. When `shards` is set, TaskForge chooses a stable shard with FNV-1a hashing over `shard_by`. Supported `shard_by` values are `fairness_key`, `task_id`, `task_name`, `queue`, and `header:<name>`. The default is `fairness_key`, with fallback to task ID and task name.

Routing currently affects only `new` publishes. Retries, delayed due releases, recurring dispatches, dead-letter publishes, dead-letter replays, and broker requeues preserve the queue already attached to the task. This keeps control-plane retries and recovery local to the placement that originally owned the work.

## Control Boundaries

Local controls:

- worker pool concurrency and prefetch
- worker drain and lease extension behavior
- pool-local task-type limits

Queue or Redis-cluster controls:

- active stream depth and reserved counts
- delayed and retry indexes
- admission control
- fairness policy
- adaptive concurrency signals
- dependency budgets backed by the same Redis control plane
- scheduler leadership and fenced control-plane writes

Global controls:

- routing policy distribution
- tenant or traffic-class placement
- cross-shard rebalance decisions
- multi-Redis dispatch, when a later phase adds it

The `taskforge_shard` header is logical metadata in this phase. It identifies intended placement for operators and future routing layers, but Redis keys remain queue-scoped in the current runtime.

## Operating Guidance

Add capacity by first deciding whether the bottleneck is a queue, a logical shard, or an entire Redis control plane. For a hot queue within one shard, add worker replicas for that queue or increase that pool's concurrency within downstream limits. For a hot tenant or traffic class, add a routing rule that isolates it to a dedicated queue or spreads it over a larger shard set.

Move tenants deliberately. Update the routing policy for new work, then watch old placement drain through queue depth, delayed backlog, retry backlog, and DLQ metrics. In-flight work, scheduled retries, and DLQ replays stay on their original queue unless an operator explicitly republishes them under a new policy.

Handle shard-local overload locally first. Admission, fairness, adaptive concurrency, and retries are evaluated against the destination queue and Redis-visible state. Do not expect an overloaded shard to borrow capacity from another shard unless routing policy or an external operator has intentionally moved new work there.

Reason about fairness inside the placement boundary. A fairness policy protects tenants sharing one queue. It does not provide global fairness across queues or logical shards. If a tenant is split across shards, compare per-shard metrics before deciding whether skew is a routing problem, a worker capacity problem, or a downstream dependency problem.
