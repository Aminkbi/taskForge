# Paired multi-environment study artifact

This directory is the registered follow-up to the original ablation artifact.
It uses immutable external arrival traces and within-block system contrasts.
The two measured classes share one physical workstation: native execution with
direct loopback Redis, and four-Go-processor execution through a loopback proxy
with 500 microseconds injected in each direction. The latter is an emulated
network path, not a remote-host result.

## Reproduction

The normal artifact check does not rerun wall-clock experiments:

```bash
make second-wave-check
```

It verifies every frozen code/profile/trace byte, rebuilds and byte-compares
the measured replay binary, validates the complete cell ledger and raw-result
digests, regenerates machine-readable analysis, Markdown, SVG, and paper text
in a temporary directory, and byte-compares the derived outputs.

To create the deterministic release archive:

```bash
make second-wave-package
```

The archive contains the locked source inputs, immutable traces, raw data,
ledger, generated results, figure, paper, and a SHA-256 manifest. It performs
no upload.

The experiment lifecycle is deliberately split so inputs predate outcomes:

```bash
make second-wave-freeze # one-time plan/code/profile/trace lock
make second-wave-run    # one-time registered replay; refuses replacement
make second-wave-analysis
```

`second-wave-run` retains every scheduled cell. Runner failures and capability
exclusions are recorded in `data/dataset.json` and generated reports rather
than silently rerun, selected, or encoded as zero.

## Baseline tuning and semantic boundary

Asynq 0.25.1 uses its documented server concurrency, queue-specific task IDs,
fixed trace retry delays, 10ms task polling, 10ms delayed-task polling, one
second completed-task retention, and queue `default`. Concurrency 16 is the
matched tuned setting used in the preceding common-contract frontier pass.
Every raw result repeats those settings in `capabilities.tuning`.

Asynq participates only in the successful Redis-backed common-delivery family.
Its archive is not called equivalent to TaskForge's DLQ, and it has no matched
tenant-entitlement, admission, adaptive-concurrency, dependency-budget, or
process-crash contract. River is omitted because its PostgreSQL persistence
would change both storage and delivery contract in this Redis study.
