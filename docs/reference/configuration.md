# Configuration Reference

TaskForge configuration is environment-based.
Use `TASKFORGE_`-prefixed variables for service settings and JSON variables for structured queue, routing, budget, and schedule policies.

## Common Settings

```env
TASKFORGE_LOG_LEVEL=info
TASKFORGE_HTTP_ADDR=:8080
TASKFORGE_METRICS_ADDR=:8080
TASKFORGE_REDIS_ADDR=localhost:6379
TASKFORGE_REDIS_PASSWORD=
TASKFORGE_REDIS_DB=0
TASKFORGE_POLL_INTERVAL=1s
TASKFORGE_SHUTDOWN_TIMEOUT=10s
TASKFORGE_SCHEDULER_LOCK_TTL=15s
TASKFORGE_SCHEDULER_RENEW_INTERVAL=5s
TASKFORGE_TASK_SUCCESS_RETENTION=24h
TASKFORGE_TASK_FAILURE_RETENTION=168h
TASKFORGE_TASK_PAYLOAD_RETENTION=24h
TASKFORGE_OTEL_ENABLED=false
TASKFORGE_SERVICE_NAME=taskforge
```

`TASKFORGE_METRICS_ADDR` is part of the config surface, but `/metrics` is currently served on the main HTTP listener.
`TASKFORGE_SHUTDOWN_TIMEOUT` is also the worker drain grace window.

## Worker Pools

Workers are configured through `TASKFORGE_WORKER_POOLS_JSON`.
Each pool can set queue placement, concurrency, prefetch, lease TTL, retry defaults, fairness rules, admission control, adaptive concurrency, and task-type limits.

```env
TASKFORGE_WORKER_POOLS_JSON=[
  {
    "name":"critical",
    "queue":"critical",
    "concurrency":2,
    "prefetch":2,
    "lease_ttl":"20s",
    "fairness":{
      "default":{"hard_quota":8},
      "rules":[
        {"name":"vip","keys":["tenant-vip"],"hard_quota":2,"reserved_concurrency":1}
      ]
    },
    "admission":{
      "mode":"defer",
      "max_pending":2000,
      "max_pending_per_fairness_key":250,
      "max_oldest_ready_age":"15s",
      "max_retry_backlog":500,
      "max_dead_letter_size":1000,
      "defer_interval":"5s"
    },
    "adaptive":{
      "enabled":true,
      "min_concurrency":1,
      "max_concurrency":6,
      "control_period":"5s",
      "cooldown":"15s",
      "scale_up_step":1,
      "scale_down_step":1,
      "latency_threshold":"500ms",
      "error_rate_threshold":0.2,
      "backlog_threshold":10,
      "healthy_windows_required":2
    },
    "task_limits":[
      {"task_name":"reports.generate","max_concurrency":1}
    ]
  },
  {
    "name":"bulk",
    "queue":"bulk",
    "concurrency":6,
    "prefetch":12,
    "lease_ttl":"45s",
    "retry":{
      "max_deliveries":5,
      "initial_backoff":"1s",
      "max_backoff":"30s",
      "multiplier":2
    }
  }
]
```

## Dependency Budgets

Dependency budgets are Redis-backed token pools held for the full task execution.
Task budget attachment is static by task name.

```env
TASKFORGE_DEPENDENCY_BUDGETS_JSON=[
  {"name":"external-api","capacity":4}
]
TASKFORGE_TASK_BUDGETS_JSON=[
  {"task_name":"reports.generate","budget":"external-api","tokens":1}
]
TASKFORGE_TASK_TYPE_LIMITS_JSON=[
  {"task_name":"tenant.sync","max_concurrency":2}
]
```

## Routing

Routing is configured globally through `TASKFORGE_ROUTING_POLICY_JSON`.
Rules match new publishes by task name, source queue, fairness key, traffic class, and headers, then assign a destination queue and optional logical shard.
Retries, due releases, recurring dispatches, DLQ flows, and broker requeues preserve existing queue placement.

```env
TASKFORGE_ROUTING_POLICY_JSON={
  "default_queue":"default",
  "default_shard":"shard-a",
  "rules":[
    {
      "name":"critical-tenant",
      "match":{"fairness_keys":["tenant-vip"],"traffic_classes":["critical"]},
      "destination":{"queue":"critical","shard":"shard-a"}
    },
    {
      "name":"bulk-spread",
      "match":{"traffic_classes":["bulk"]},
      "destination":{"queue":"bulk","shards":["bulk-a","bulk-b"],"shard_by":"fairness_key"}
    }
  ]
}
```

## Recurring Schedules

Recurring schedules are configured through `TASKFORGE_SCHEDULES_JSON`.
The current implementation supports interval schedules and the `coalesce` misfire policy.

```env
TASKFORGE_SCHEDULES_JSON=[
  {
    "id":"nightly-report",
    "interval":"15m",
    "queue":"default",
    "task_name":"reports.generate",
    "payload":{"kind":"nightly"},
    "headers":{"x-source":"scheduler"},
    "enabled":true,
    "misfire_policy":"coalesce",
    "start_at":"2026-04-14T10:00:00Z"
  }
]
```

## Retention

Task state is retained in Redis under task-level records.
Publish records `queued`; reserve records `leased`; execution records `running`; successful ack records `succeeded`; retry scheduling records `retry_scheduled`; dead-letter ack records `dead_lettered`.

Retention settings:

- `TASKFORGE_TASK_SUCCESS_RETENTION`: terminal successful records.
- `TASKFORGE_TASK_FAILURE_RETENTION`: failed, retry-scheduled, and dead-lettered records.
- `TASKFORGE_TASK_PAYLOAD_RETENTION`: optional retained result payload bytes.

A retention value of `0` disables expiration for that category.
