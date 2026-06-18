# Phase 05: Delayed and Recurring Jobs

## Commit goal

Make delayed and recurring scheduling production-oriented by tightening release semantics, scheduler ownership, and misfire behavior.

## Why this phase exists

The current scheduler only moves ETA-delayed jobs from a sorted set back to a queue. That is enough for a scaffold, but not enough for recurring jobs or safe scheduler operation in a multi-instance deployment.

## Changes

### Strengthen delayed-job release

Keep delayed jobs in Redis sorted sets for now, but make the release path explicit about:

- release ordering
- duplicate suppression where possible
- relationship between scheduled time and actual release time
- metadata preservation across release

### Add recurring schedules

Introduce an internal schedule model with:

- schedule ID
- cron or interval specification
- target queue and task template
- enabled flag
- next run time
- misfire policy

The first recurring implementation should be intentionally narrow and well documented.

### Enforce scheduler singleton behavior

Add leader election or a fencing-token lock so only one scheduler instance actively owns recurring dispatch at a time.

Recommended default:

- Redis lock with renewal
- fence token recorded in scheduler logs
- clear shutdown and takeover behavior

### Define misfire policy

At minimum, choose and implement one of:

- `skip`
- `coalesce`
- `fire_immediately`

The recommended default for the first recurring release is `coalesce`.

## Tests

- Integration test: delayed tasks release in ETA order
- Integration test: only one scheduler leader dispatches recurring jobs
- Integration test: scheduler failover does not duplicate a recurring run beyond the documented semantics
- Unit tests for schedule next-run calculation and misfire policy

## Acceptance criteria

- Delayed jobs are still simple, but less ambiguous
- Recurring jobs exist with explicit scheduler ownership rules
- Operators can reason about duplicate risk and scheduler failover
