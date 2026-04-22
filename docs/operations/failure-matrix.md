# Failure Matrix

This matrix documents how TaskForge behaves under common failure modes. The guarantees below describe the current implementation, not an idealized queue.

## Worker crash mid-task

- Guarantee: the active delivery remains pending until its lease expires, then another worker may reclaim it.
- Duplicate risk: the task may execute again from the start after reclaim.
- Look at first: `taskforge_queue_reserved`, `taskforge_tasks_reclaimed_total`, worker logs with `delivery_id`, readiness on `/readyz`.
- Operator action: confirm the crashed worker is gone, watch reclaim counters, and verify the replacement worker is healthy before manual intervention.

## Worker crash after side effect but before ack

- Guarantee: TaskForge does not provide exactly-once completion; the task can be delivered again after lease expiry.
- Duplicate risk: the external side effect may already have happened once, and replayed delivery can repeat it.
- Look at first: handler-side idempotency records, `taskforge_tasks_reclaimed_total`, logs by `task_id` and `trace_id`.
- Operator action: verify the side effect in the downstream system before replaying or discarding. Prefer idempotent handlers over manual dedupe.

## Redis restart during active deliveries

- Guarantee: in-flight reserve and ack calls can fail; after Redis recovers, workers reconnect and resume polling.
- Duplicate risk: previously leased work may reappear depending on which operations reached Redis durably before restart.
- Look at first: `/readyz`, Redis connectivity errors in logs, queue depth and reserved gauges after recovery.
- Operator action: restore Redis first, then inspect queue depth, reclaim counts, and DLQ growth before restarting workers aggressively.

## Network partition between worker and Redis

- Guarantee: the partitioned worker loses the ability to extend leases or ack deliveries.
- Duplicate risk: another healthy worker can reclaim the same task when the original lease expires.
- Look at first: lease extension failure logs, reclaim counters, worker readiness, and Redis ping failures.
- Operator action: isolate or stop the partitioned worker if it might continue side effects without broker coordination.

## Scheduler leader loss

- Guarantee: leadership is Redis-backed and another scheduler can acquire after TTL expiry; stale leaders lose fenced write authority after failover.
- Duplicate risk: delayed release may pause briefly; recurring dispatch should resume under the new leader, but work scheduled around leader loss can bunch up.
- Look at first: scheduler `/readyz`, `/v1/admin/leadership`, `taskforge_scheduler_queue_lag_seconds`, leadership logs, stale-write rejection counters.
- Operator action: confirm one healthy standby becomes leader, confirm stale-write rejections are not climbing unexpectedly, then monitor scheduler lag until it returns to normal.

## Dead-letter replay failure

- Guarantee: replay republishes the original task only if the broker publish succeeds; otherwise the DLQ entry remains available.
- Duplicate risk: if publish succeeds and downstream work partially completes before another failure, replay can still produce duplicate side effects.
- Look at first: DLQ entry contents, replay audit stream, worker failure logs, dead-letter size metric.
- Operator action: inspect the original failure class and downstream state before retrying replay. If the task is unsafe to rerun, discard with a documented reason.
