# HTTP and Operations API Reference

Every TaskForge process exposes:

- `/healthz`
- `/readyz`
- `/metrics`

The scheduler process also exposes:

- `/v1/admin/leadership`

The API process also exposes:

- `/`
- `/dashboard/`
- `/v1/admin/ping`
- `/v1/admin/admission`
- `/v1/admin/adaptive`
- `/v1/admin/workers`
- `/v1/tasks/{task_id}`

`/dashboard/` serves an embedded operator dashboard with no external assets or build step.
It includes a config builder for `TASKFORGE_*` settings and a read-only live operations view backed by the `/v1/admin` endpoints.

## Admin Endpoints

`/v1/admin/admission` reports each queue's configured mode, current admission state, dominant rejection or defer reason, latest signal snapshot, and `defer_interval`.

`/v1/admin/adaptive` reports each worker pool's effective concurrency, configured bounds, latest adjustment reason, sampled adaptive signals, and cluster-wide dependency budget usage.

`/v1/admin/workers` reports each worker instance's lifecycle state, current pending and running ownership, drain timestamps, shutdown outcome, abandoned-delivery count, and drain-time lease losses.

`/v1/admin/leadership` reports the scheduler's local leadership state, current fenced epoch, live Redis leadership record, stale-write rejections, and control-plane failure counters.

`/v1/tasks/{task_id}` returns the durable task record for a logical task ID, including state, last error, timestamps, delivery count, last delivery ID, lease owner, and retained result payload when one exists.
Lookup is read-only; replay remains an explicit operator action rather than an arbitrary task-state mutation.

## Metrics

Worker and API metrics include queue-aware counters and gauges such as:

- `taskforge_queue_depth`
- `taskforge_queue_reserved`
- `taskforge_queue_consumers`
- `taskforge_admission_decisions_total`
- `taskforge_admission_state`
- `taskforge_admission_signal`
- `taskforge_worker_effective_concurrency`
- `taskforge_worker_concurrency_adjustments_total`
- `taskforge_worker_lifecycle_state`
- `taskforge_worker_shutdown_outcomes_total`
- `taskforge_worker_abandoned_deliveries_total`
- `taskforge_worker_drain_lease_losses_total`
- `taskforge_dependency_budget_capacity`
- `taskforge_dependency_budget_in_use`
- `taskforge_dependency_budget_blocked_total`
- `taskforge_dependency_budget_lease_renew_failures_total`
- per-queue success, failure, retry, reclaim, and active-task metrics

Scheduler metrics include:

- `taskforge_scheduler_leader`
- `taskforge_scheduler_leadership_epoch`
- `taskforge_scheduler_leadership_last_renewed_at_seconds`
- `taskforge_scheduler_stale_write_rejections_total`
- `taskforge_scheduler_control_plane_failures_total`
