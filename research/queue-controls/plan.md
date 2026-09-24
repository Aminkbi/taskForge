# Queue control comparison: corrected open-loop protocol

Status: exploratory protocol fixed before the retained run. The earlier
closed-loop results remain in `research/third-wave/control-comparison/`.
This follow-up was motivated by those results and by diagnostic runs on
2026-09-24. It is not independent confirmation or a preregistered study.
This prose file was edited after the retained run and no longer matches the
plan hash in `data/metadata.json`; the original copy is unavailable. The
recorded runner and executable profile hashes still match the retained files.

## Question

Under what conditions does each optional control improve its intended outcome,
and what does it cost under light load? Use the current TaskForge product in
all arms, immutable arrivals independent of publish completion, and one control
at a time. Full-control interactions need a separate experiment.

The open-loop adapter previously multiplied entitlement weights by 1000. The
product interprets each integer weight as consecutive reservation tickets.
Reduce the adapter's weights to their smallest integer ratio so equal tenants
use 1:1, not 1000:1000. This is a harness correction; product code is unchanged.

## Fixed matrix

All cells start with eight workers and prefetch 16; adaptive arms may range
from two to sixteen. A static-capacity reference uses four workers and the
same prefetch. No control settings are tuned using retained outcomes.

| Profile | Arms | Primary outcome |
| --- | --- | --- |
| fairness-pressure | FIFO, fairness only | protected tenant on-time completions / offered tasks |
| admission-pressure | FIFO, deferred admission only | sampled ready backlog, with deferred and total backlog reported |
| dependency-pressure | FIFO, budget only, static capacity | downstream over-capacity and failure rates; on-time completion fraction |
| adaptive-pressure | FIFO, adaptive only, static capacity | on-time completion fraction; concurrency trajectory and downstream failures |
| stable-light | FIFO, fairness only, admission only, budget only, adaptive only | overhead and on-time completion fraction |
| fragile-light | FIFO, budget only, adaptive only, static capacity | overhead and on-time completion fraction |

Each profile has a two-second warmup, six-second measurement arrival window,
and two-second recovery arrival window, followed by at most 15 seconds of
drain. Two new input seeds (20260925 and 20260926) give 38 cells. System order
is deterministically shuffled by seed. This is descriptive evidence on one
host, not a significance test. The same immutable trace is used for all arms
of a profile/seed. All cells run serially in a dedicated persistence-disabled
Redis 7.4 container on loopback, flushing its database 14 between arms.

Stable handlers take 20 ms. Pressure offers 600 tasks/s against eight workers'
ideal 400/s capacity, with a 9:1 tenant mix and equal entitlement for fairness.
Admission defers at 32 pending tasks; this tests containment of ready work,
not extra processing capacity or elimination of total stored work.

The fragile dependency has capacity four, 10 ms base latency plus 10 ms
application service. Above capacity, base latency increases quadratically
with slope 3 (capped at 100 ms), and failure probability increases by 0.4 per
unit excess load ratio, reaching at least 0.9 at twice capacity. Pressure
arrivals are 260/s, light arrivals 50/s. Retry backoff is 20 ms, at most three
attempts. The capacity is a declared model assumption, not calibrated from a
production service. Static concurrency four is included to avoid attributing
the benefit of any sensible concurrency limit uniquely to a budget or adaptation.
The adaptive and dependency profiles deliberately share the same workload;
only the tested policy differs. The SLO is 500 ms from the scheduled arrival.

## Measurement and integrity

Primary success fractions use **all offered measurement-window tasks** as the
denominator. Rejected, failed, unresolved, and late tasks cannot disappear from
the comparison. Report these counts separately. Completion p99 is conditional
on successful completion, includes publish/queue/retry wait, and excludes the
broker acknowledgment after handler return. It must be read beside the
success fraction. Report unique completions, retries, all-attempt downstream
failures/overlap, sampled ready/deferred/total backlog, controller range,
Redis CPU delta, and p99 dispatch delay. Backlog maxima are sampled at 100 ms;
admission is a policy threshold, not an assumed strict atomic cap.

A dispatch p99 above 10 ms, any enqueue error, a missing cell, or an unknown
terminal outcome invalidates an unqualified headline. Retain those results
and label their limitations; do not silently rerun them. Count unresolved work
at the bounded observation horizon instead of labeling it successful. Zero
with zero successes, conditional latency is unavailable, not zero.

Record the source parent commit, all measured Go/module file hashes, exact
changed/new source overlays, profile/plan/runner hashes, generated trace hashes,
binary hashes, host/toolchain facts, Redis image and configuration, exact cell
arguments, failures, and raw compressed results. Build once from a source
snapshot. Preserve diagnostics separately and exclude them from the estimates.
Do not overwrite an existing run. Report negative and inconclusive results.
