# TaskForge wave 3 research package

Wave 3 studies the cost of the optimized control plane in committed revision
`b2947f3`. It is deliberately a two-layer package:

* the **baseline layer** reuses the frozen wave 2 paired study as the external
  workload and overload context; and
* the **treatment layer** measures the committed optimization with fixed-iteration,
  paired Redis microbenchmarks and invariant checks.

The package is written for two audiences from one evidence source. The paper
(`paper/paper.md`) states the methods, estimands, and limits in research form;
the blog (`blog/taskforge-wave3.md`) explains the same result in engineering
language. Neither document invents a result when the treatment benchmark has
not been run.

## What is new in wave 3

The treatment is the committed control-plane optimization, not a re-tuning of
the worker. It changes the following observable surfaces:

| Surface | Mechanism under test | Primary measurement |
| --- | --- | --- |
| Publish | one Redis script records the ready entry and built-in queued state | Redis round trips and commands per publish |
| Queue metrics | one pipeline reads stream length, pending count, and optional consumers | p95 snapshot latency as tenant count grows |
| Setup | consumer-group existence is cached after the first successful check | first-use versus steady-state reserve cost |
| Key construction | concatenation replaces formatted strings on hot paths | allocations and ns/op |
| Delivery safety | unprocessable delivery handling is bounded and dead-letter size is signalled | invariant/property test outcomes |

The source-level map and exact file set are in `evidence/optimization-map.md`.
The benchmark protocol is frozen in `analysis-plan.md` before treatment
measurements are accepted.

## Reproduction

From the repository root:

```bash
make third-wave-check
```

This verifies the evidence manifest, checks the repository diff for whitespace
errors, runs the research module tests, and validates that the protocol names
the exact committed treatment surfaces. Redis-backed treatment runs are opt-in:

```bash
TASKFORGE_RUN_BENCHMARKS=1 \
  GOFLAGS='-benchtime=30x -count=10' \
  make bench > /tmp/taskforge-wave3-treatment.txt
```

Run that command once at `b2947f3` and once at its clean parent `4446ab3`, on
the same host and Redis configuration. Compare the two logs with:

```bash
make benchmark-regression \
  BENCHMARK_ARGS='/tmp/taskforge-wave3-baseline.txt /tmp/taskforge-wave3-treatment.txt'
```

The comparison is a regression gate, not the wave 3 analysis. For publication,
retain both logs, the commit/tree identifiers, Redis `INFO` output, CPU model,
Go version, and the exact command line. The analysis plan specifies the paired
bootstrap and multiplicity rule for those observations.

## Evidence status

The checked-in wave 2 corpus contains 96 paired measured cells plus eight
explicitly unsupported recovery cells. It supplies the workload context and is
not relabelled as a wave 3 treatment result. At the time this package was
authored, the Redis benchmark service was unavailable in the execution
environment, so no treatment number is claimed. The paper and blog state that
boundary explicitly.

The first publishable wave 3 result therefore requires both benchmark logs and
the corresponding correctness test output. The report template in
`paper/paper.md` has a fixed results table for filling those artifacts without
changing the estimands after seeing the data.
