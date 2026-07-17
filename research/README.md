# TaskForge Research Artifact

This directory is the reproducible research artifact for the study
"Overload Controls for Multi-Tenant Background Task Execution: A
Pre-Registered Ablation Study of TaskForge"
([`paper/paper.md`](paper/paper.md)). It is prepared for artifact
evaluation and Zenodo archiving; nothing here has been submitted or uploaded
anywhere.

## Contents

| Path | Role |
| --- | --- |
| [`analysis-plan.md`](analysis-plan.md) | Pre-registered hypotheses, design, metrics, statistics, pilot amendments, and the disclosed full-grid replacement |
| [`data/raw/`](data/raw/) | Committed raw evidence: 504 runs (6 workloads x 7 variants x 12 seeds), gzipped JSON with per-task samples and full environment metadata |
| [`data/run-log.txt`](data/run-log.txt) | Per-cell execution log of the registered grid (all 504 ok) |
| [`results/`](results/) | Derived statistical report (`analysis.md`, `analysis.json`) — generated, never hand-edited |
| [`figures/`](figures/) | Derived SVG figures — generated, never hand-edited |
| [`paper/paper.md`](paper/paper.md) | The paper draft; every number traces to `results/analysis.md` |

Citation metadata lives at the repository root (`CITATION.cff`,
`.zenodo.json`).

## Kick-the-tires (about 10 minutes)

Prerequisites: Go 1.26.5+, Docker with Compose (or a local Redis 7 on
`localhost:6379`), `redis-cli`. All commands run from the repository root.

1. Regenerate the entire statistical report and every figure from the
   committed raw data:

   ```bash
   make research-analysis
   git status --short research/   # no changes: outputs are byte-reproducible
   ```

   This is the core artifact claim: every table and chart is derived from
   checked-in raw evidence by one documented command, deterministically
   (seeded bootstrap, fixed iteration order).

   `make research-check` performs the same regeneration in a temporary
   directory, byte-compares every result and figure, and validates the
   privacy-safe 504-cell run log without modifying committed outputs.

2. Run a small live slice of the experiment against Redis:

   ```bash
   docker compose up -d redis
   go run ./cmd/experiment -manifest noisy-neighbor -variant taskforge-full \
     -scale 8 -output /tmp/taskforge-artifact-check
   ```

   The printed JSON path contains raw per-task samples plus the summary
   block; compare its shape with any file in `research/data/raw/`.

## Full reproduction (about 40 minutes on a 12-CPU host)

```bash
make research-experiments   # re-runs the registered grid into research/data/raw
make research-analysis      # re-derives results/ and figures/
```

`research-experiments` overwrites `research/data/raw/` with runs from your
machine; use a scratch output directory
(`scripts/research-experiments.sh /tmp/rerun`) to keep the registered
evidence intact. Absolute numbers will differ on your hardware; the
qualitative contrasts in the paper (admission benefits, fairness protection,
budget capping, control overhead) are the reproduction target. Runs use the
dedicated Redis DB 14 and flush it between cells; set
`TASKFORGE_EXPERIMENT_REDIS_DB` if DB 14 is not free.

## Provenance and privacy

Every raw file records seed, manifest, variant, build SHA, Go version, OS,
architecture, CPU count, and the Redis configuration string. The hostname is
replaced with the neutral label `research-host` before results are committed.
The registered replacement grid was produced from clean source commit
`ac19e98ce8190bf962798e35f45052f0a76c4f91` with raw schema
`taskforge-experiment/v2`, on the environment described in the plan. The
analysis command rejects missing, duplicate, mixed-revision, mixed-environment,
or non-neutral-host cells before generating publication output.

## Packaging for Zenodo

```bash
make research-package
```

builds a deterministic `dist/taskforge-research-artifact.tar.gz` containing this directory,
the citation metadata, and a manifest of SHA-256 checksums. It does not
upload anything; archiving to Zenodo and adding the minted DOI to the citation
metadata are separate, human-approved steps.
