# Execution amendment and audit trail

## Latest-state rerun (2026-09-23)

The original run compared `4446ab3` with `b2947f3`. This rerun keeps the
original baseline, shared benchmark harness, factor matrix, iteration count,
arm order, and analysis method, but replaces the treatment with committed
revision `5d1d882`. That revision includes the Redis optimization and later
worker, metrics, and recurring-scheduler changes. The result measures the
cumulative latest state against the pre-optimization baseline; it does not
isolate the effect of any one later change. The original analysis plan below
remains a historical registration, and this treatment change is disclosed as
an amendment.

A provisional run at `2bc45a8` failed the concurrent recurring-dispatch
integration gate before measurements began. It revealed that two reconciliation
passes could restore an old schedule state after a dispatch. Revision `5d1d882`
adds optimistic locking around reconciliation and repeats the concurrent
integration scenario. The failed provisional logs remain outside the checked-in
evidence and do not enter this rerun's estimates. The previous final evidence
is replaced only after the new correctness and benchmark gates complete.

The original analysis plan is retained unchanged. These implementation details
were resolved on 2026-09-22 after diagnostic runs, before the final paired run.
The final analysis is therefore an amended execution of the registered plan,
not an untouched preregistration. Diagnostic logs were excluded from the final evidence directory; no diagnostic observations enter the final estimates.

## Identical harness, unchanged product

The baseline commit does not contain `cleanup_benchmark_test.go`. Both source
exports receive the treatment's benchmark files, byte for byte. The only other
benchmark-file change between commits turns unavailable Redis from a skip into
a failure. Product source remains exactly at each recorded commit. The source
archive digest and each overlaid harness digest are recorded.

The primary run covers `BenchmarkPublishStateCosts`, `BenchmarkSnapshotCosts`,
and `BenchmarkPublishThroughput`: 17 factor cells, 30 iterations and ten
repetitions each. Whole-arm order is baseline then treatment, pairing by
repetition index as in the original protocol. This does not control temporal
drift; the two arms are not independent-host replications.

The registered setup/key family lacked benchmark definitions. A common harness
now measures uncached group checks, cached group checks, and three key builders
per operation. These are explicitly supplementary measurements, defined after
the diagnostic runs. They cannot be described as preregistered confirmation.
The first-use group case clears only the broker cache between checks; Redis
already has the stream/group, so it measures an uncached existing-group check,
not provisioning a new Redis stream.

## Analysis details

For each metric/cell, take the median of the ten within-index percentage changes.
Bootstrap those paired changes 10,000 times with seed 20260922, using Python's
standard-library PRNG and linear-interpolated percentile endpoints. Bonferroni
adjustment covers all primary metric/cell tests within a family: 27 publish,
72 snapshot, and nine supplementary setup/key comparisons. The intervals are
nominal small-sample bootstrap intervals, not strong coverage guarantees.
Zero-to-zero is a zero change; zero-to-positive has undefined relative change
and fails the regression guard. Also apply the repository's 15% guard to the
ratio of arm medians, including bytes/op, and require both definitions to pass.
The supplementary setup/key results are guarded separately.

Go `ns/op` is the average of 30 operations within a repetition. Redis command
hooks count client-issued commands, not commands executed inside Lua. Network
counters are server-wide.

## Correctness and isolation

A dedicated Redis process with persistence disabled is used exclusively for
this run. Neither the existing development Redis nor its data is modified.
Tests run before final measurements. Both revisions' Redis, worker, and
integration suites are recorded; integration-test source in the temporary
export only has its hard-coded loopback endpoint redirected to the dedicated
process. Connection skips fail the evidence gate. No product code is patched.

Panic containment, corrupt retry-state dead-lettering, and the DLQ size repair
already exist in the baseline commit. They are correctness gates, not effects
of the optimization treatment.

## Diagnostic attempts

An initial sandboxed baseline invocation had no measurements because local
sockets were denied; it was a setup failure, not an accepted run. Its log was overwritten before the final evidence directory was created. The
original full-suite baseline and treatment runs completed, but had different
benchmark sets; those diagnostic outputs were excluded. The final evidence
contains only the shared-harness paired run, its supplementary setup/key run,
and the correctness outputs.
