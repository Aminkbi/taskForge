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
language. The completed run and derived analysis are under `data/final/` and
`results/`.

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
errors, runs the research module tests, and validates the completed analysis.
To reproduce the paired run with a dedicated Redis container, supply its name
and mapped address:

```bash
python3 scripts/third-wave-run.py \
  --redis-container <dedicated-redis-container> \
  --redis-addr <mapped-redis-address> \
  --output research/third-wave/data/rerun
```

The runner exports both revisions, overlays the identical benchmark harness,
runs correctness gates before measurements, records Redis and host metadata,
and refuses to accept skipped or failed benchmark output. Analyze retained
logs with:

```bash
python3 scripts/third-wave-analysis.py \
  --baseline research/third-wave/data/rerun/baseline.txt \
  --treatment research/third-wave/data/rerun/treatment.txt \
  --setup-baseline research/third-wave/data/rerun/baseline-setup.txt \
  --setup-treatment research/third-wave/data/rerun/treatment-setup.txt \
  --output research/third-wave/results/rerun
```

The analysis retains raw per-sample values and applies the paired bootstrap and
family adjustment in the analysis plan. The public article reports the scope
boundary instead of treating these host-local observations as universal speed
claims.

## Evidence status

The checked-in wave 2 corpus contains 96 paired measured cells plus eight
explicitly unsupported recovery cells. It supplies workload context and is not
relabelled as a control-plane treatment result. The completed control-plane run
contains 27 publish comparisons, 72 snapshot comparisons, and nine setup/key
comparisons, each with 30 iterations and ten repetitions. The raw logs,
metadata, regression gate, and correctness output are under `data/final/`; the
derived table is `results/analysis.md`. The result is host-local and bounded by
the documented Redis topology and toolchain.
