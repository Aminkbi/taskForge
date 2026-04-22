# Operator Runbooks

These runbooks are intentionally short and tied to the current observability surface. They assume the Redis broker implementation and the metrics/readiness behavior added in Phase 07.

## Stuck pending entries

Symptoms:

- `taskforge_queue_reserved` remains non-zero for one queue
- `taskforge_queue_depth` does not fall
- workers may report reclaims or lease extension failures

Check first:

- worker `/readyz`
- worker logs for `delivery_id`, `worker_identity`, and lease errors
- `taskforge_tasks_reclaimed_total`

Response:

1. Confirm whether the original worker is still healthy and connected to Redis.
2. If the worker is dead or partitioned, wait for reclaim before forcing a replay.
3. If reclaim does not occur, inspect Redis pending entries and consumer ownership directly.
4. Only intervene manually after confirming the task handler is idempotent or the side effect state is known.

## Reclaim storms

Symptoms:

- `taskforge_tasks_reclaimed_total` increases rapidly
- repeated lease extension failures in worker logs
- task throughput drops while duplicate work rises

Check first:

- worker CPU saturation
- Redis latency/connectivity
- task execution time versus lease TTL

Response:

1. Compare handler duration to configured lease TTL.
2. Increase lease TTL for legitimately long tasks or reduce handler time if possible.
3. Investigate Redis connectivity or worker pauses causing missed lease extension ticks.
4. Watch for downstream duplicate side effects while the storm is active.

## Growing DLQ

Symptoms:

- `taskforge_dead_letter_queue_size` grows steadily
- failure logs repeat for the same task names or queues

Check first:

- DLQ entries via the admin service or Redis inspection
- failure class in each envelope
- retry headers and max-delivery settings on the original tasks

Response:

1. Separate transient upstream failures from permanent payload or validation failures.
2. Fix the underlying dependency or payload issue before replaying.
3. Replay only tasks whose side effects are safe to rerun.
4. Discard entries only with an explicit operator reason.

## High scheduler lag

Symptoms:

- `taskforge_scheduler_queue_lag_seconds` rises
- delayed tasks are published on time but not becoming reservable quickly
- scheduler readiness stays healthy but leadership may be standby on one node and leader on another

Check first:

- scheduler `/readyz` leadership status
- scheduler `/v1/admin/leadership` local and Redis epoch view
- Redis latency
- scheduler logs around leadership and delayed release

Response:

1. Verify a healthy leader exists.
2. If leadership is flapping, stabilize Redis and scheduler connectivity first.
3. Reduce scheduler poll interval only if Redis can tolerate the additional load.
4. Check whether delayed workload volume exceeds what one leader can release per tick.
5. For recurring-heavy deployments, inspect the recurring due-time sorted set and confirm the lag is coming from schedules due now, not from a large number of inactive or far-future schedules.

## Queue starvation

Symptoms:

- one queue keeps growing while another queue drains normally
- critical work sits in depth while bulk work dominates workers

Check first:

- `taskforge_queue_depth` by queue
- worker pool mapping and queue isolation config
- task-type concurrency limits

Response:

1. Confirm the starving queue actually has a dedicated worker pool if it is meant to be isolated.
2. Add or scale workers for that queue rather than only increasing a noisy shared pool.
3. Inspect task-type limits that may be throttling one hot task name.
4. Use the benchmark harness before and after changes to verify the new configuration behaves better under load.
