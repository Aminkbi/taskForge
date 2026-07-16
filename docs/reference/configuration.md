# Configuration Reference

TaskForge has one product configuration model: `taskforge.Config`. Embedded Go
applications pass that model through `redis.OptionsFromConfig` and
`worker.OptionsFromConfig`; the scheduler and API sidecars decode their
`TASKFORGE_` environment variables into the same model and call the same
validation. Invalid cross-policy combinations fail during startup.

Connection addresses, credentials, logging, HTTP, tracing, and routing remain
deployment settings. Redis key layouts and controller step/cooldown internals
are intentionally not part of the supported control surface.

## Go API

```go
cfg := taskforge.Config{
  LeaseTTL: 30 * time.Second,
  WorkerPools: []taskforge.WorkerPoolConfig{{
    Name: "reports", Queue: "reports", Concurrency: 2, Prefetch: 4,
    TaskTimeout: 20 * time.Second,
    Retry: taskforge.RetryPolicy{
      MaxDeliveries: 4, InitialBackoff: time.Second,
      MaxBackoff: 30 * time.Second, Multiplier: 2, Jitter: 0.1,
    },
    Fairness: &taskforge.FairnessConfig{
      Rules: []taskforge.FairnessRule{
        {Name: "vip", Keys: []string{"tenant-vip"}, ReservedConcurrency: 1, HardQuota: 1},
      },
    },
    Admission: taskforge.AdmissionPolicy{
      Mode: taskforge.AdmissionDefer,
      MaxPending: 2_000, MaxPendingPerFairnessKey: 250,
      MaxOldestReadyAge: 15 * time.Second, MaxRetryBacklog: 500,
    },
    Adaptive: taskforge.AdaptiveConcurrencyConfig{
      Enabled: true, MinConcurrency: 1, MaxConcurrency: 4,
      LatencyThreshold: 500 * time.Millisecond, ErrorRateThreshold: 0.2,
    },
  }},
  DependencyBudgets: []taskforge.DependencyBudget{{Name: "external-api", Capacity: 4}},
  TaskBudgets: []taskforge.TaskBudget{{TaskName: "reports.generate", Budget: "external-api"}},
}

broker, err := taskforgeredis.NewFromConfig(cfg, taskforgeredis.Options{Addr: "localhost:6379"})
if err != nil { return err }
runtime, err := worker.NewFromConfig(cfg, "reports", worker.Options{Broker: broker, Handler: registry})
if err != nil { return err }
```

`taskforge.DefaultConfig()` returns a default pool with concurrency and prefetch
4, a 30-second lease and task timeout, three deliveries with exponential
backoff, bounded state retention, and safe scheduler lease intervals.
`Config.Normalize()` applies those defaults to an owned copy. A nil pool slice
selects the default pool; an explicitly empty slice means no embedded worker.

## Environment

Common deployment and global control settings:

```env
TASKFORGE_LOG_LEVEL=info
TASKFORGE_HTTP_ADDR=:8080
TASKFORGE_REDIS_ADDR=localhost:6379
TASKFORGE_REDIS_PASSWORD=
TASKFORGE_REDIS_DB=0
TASKFORGE_SERVICE_NAME=taskforge
TASKFORGE_OTEL_ENABLED=false
TASKFORGE_SHUTDOWN_TIMEOUT=10s
TASKFORGE_LEASE_TTL=30s
TASKFORGE_POLL_INTERVAL=1s
TASKFORGE_SCHEDULER_LOCK_TTL=15s
TASKFORGE_SCHEDULER_RENEW_INTERVAL=5s
TASKFORGE_TASK_SUCCESS_RETENTION=24h
TASKFORGE_TASK_FAILURE_RETENTION=168h
TASKFORGE_TASK_PAYLOAD_RETENTION=24h
```

`/metrics` is served on `TASKFORGE_HTTP_ADDR`.
`TASKFORGE_SHUTDOWN_TIMEOUT` is also the worker drain grace window.

Worker policies use `TASKFORGE_WORKER_POOLS_JSON`:

```json
[
  {
    "name": "reports",
    "queue": "reports",
    "concurrency": 2,
    "prefetch": 4,
    "task_timeout": "20s",
    "retry": {
      "max_deliveries": 4,
      "initial_backoff": "1s",
      "max_backoff": "30s",
      "multiplier": 2,
      "jitter": 0.1,
      "max_task_age": "10m"
    },
    "fairness": {
      "default": {"hard_quota": 2},
      "rules": [
        {"name": "vip", "keys": ["tenant-vip"], "weight": 2, "reserved_concurrency": 1, "hard_quota": 1}
      ]
    },
    "admission": {
      "mode": "defer",
      "max_pending": 2000,
      "max_pending_per_fairness_key": 250,
      "max_oldest_ready_age": "15s",
      "max_retry_backlog": 500,
      "defer_interval": "5s"
    },
    "adaptive": {
      "enabled": true,
      "min_concurrency": 1,
      "max_concurrency": 4,
      "control_period": "5s",
      "latency_threshold": "500ms",
      "error_rate_threshold": 0.2,
      "backlog_threshold": 10
    },
    "task_limits": [
      {"task_name": "reports.generate", "max_concurrency": 1}
    ]
  }
]
```

The lease duration is global because the Redis broker owns the reserve lease;
all worker pools therefore extend the same initial lease. A pool's task timeout
is the default handler deadline and can be overridden by `taskforge.WithTimeout`
on an individual publish. Prefetch must cover the configured concurrency and
the adaptive maximum. Reserved fairness concurrency must fit within the pool.

Dependency budgets are Redis-backed token pools held for the full handler run:

```env
TASKFORGE_DEPENDENCY_BUDGETS_JSON=[{"name":"external-api","capacity":4}]
TASKFORGE_TASK_BUDGETS_JSON=[{"task_name":"reports.generate","budget":"external-api","tokens":1}]
TASKFORGE_TASK_TYPE_LIMITS_JSON=[{"task_name":"tenant.sync","max_concurrency":2}]
```

A task mapping must reference a declared budget and cannot request more tokens
than that budget's capacity.

## Scheduling

`TASKFORGE_SCHEDULES_JSON` configures interval schedules. `coalesce` is the
only supported misfire policy. Schedules are disabled unless `enabled` is true,
matching the zero value of the Go `Schedule.Enabled` field.

```json
[
  {
    "id": "nightly-report",
    "interval": "15m",
    "queue": "reports",
    "fairness_key": "tenant-vip",
    "task_name": "reports.generate",
    "payload": {"kind": "nightly"},
    "headers": {"x-source": "scheduler"},
    "enabled": true,
    "misfire_policy": "coalesce",
    "start_at": "2026-04-14T10:00:00Z"
  }
]
```

## Routing

Routing remains a deployment policy configured by
`TASKFORGE_ROUTING_POLICY_JSON`. It applies only to new publishes; retries, due
release, recurrence, DLQ flows, and broker requeues preserve placement.

```env
TASKFORGE_ROUTING_POLICY_JSON={
  "default_queue":"default",
  "default_shard":"shard-a",
  "rules":[
    {
      "name":"critical-tenant",
      "match":{"fairness_keys":["tenant-vip"],"traffic_classes":["critical"]},
      "destination":{"queue":"critical","shard":"shard-a"}
    }
  ]
}
```

## Retention

Successful state, failed/retry/dead-letter state, and result payload bytes have
separate retention durations. In Go, set `Config.Retention` to a
`taskforge.RetentionPolicy`; in the environment use the three retention
variables above. A duration of zero explicitly retains that category without
expiration. A nil Go retention policy selects bounded defaults.

## Dynamic and restart-required settings

TaskForge does not hot-reload configuration.

| Setting or state | Lifecycle |
| --- | --- |
| Worker pools, fairness rules, admission thresholds/mode, dependency capacities/mappings, adaptive bounds/targets, retry defaults, pool task timeouts, retention, scheduler intervals/schedules, routing | Restart required |
| Adaptive effective concurrency | Dynamic within the restart-configured bounds |
| Admission signal/state and dependency tokens in use | Dynamic runtime state; policy/capacity is restart required |
| Schedule next-run state | Dynamic runtime state; definitions are restart required |
| Per-task timeout and max deliveries supplied with task options | Dynamic per publish; they do not mutate pool defaults |

Environment JSON rejects unknown fields so misspelled or retired settings fail
at startup instead of being silently ignored.

For the T05 configuration migration, replace per-pool `lease_ttl` with the
global `TASKFORGE_LEASE_TTL` and rename fairness `default_rule` to `default`.
Fairness soft quotas/burst, admission DLQ thresholds, and adaptive cooldown,
step, and healthy-window fields are controller internals and are no longer
accepted by the supported environment schema.
