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
| [`data/dataset.json`](data/dataset.json) | Per-cell provenance ledger: source/tree, binary and result digests, dependency locks, exact arguments, Redis configuration, sanitized environment, and measured/not-measured status |
| [`data/raw/`](data/raw/) | Committed raw evidence: exactly 504 gzipped cell results (6 workloads x 7 variants x 12 seeds) |
| [`data/run-log.txt`](data/run-log.txt) | Privacy-safe per-cell status log; unsupported Asynq crash cells are `not_measured` |
| [`results/`](results/) | Derived statistical report (`analysis.md`, `analysis.json`) — generated, never hand-edited |
| [`figures/`](figures/) | Derived SVG figures — generated, never hand-edited |
| [`paper/paper.template.md`](paper/paper.template.md) | Narrative paper source with strict generated-evidence tokens |
| [`paper/paper.md`](paper/paper.md) | Generated paper; its complete numeric result table comes from `analysis.json` |

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
   directory, byte-compares every result, figure, and the paper, validates all
   raw result digests and the complete provenance ledger, and checks the
   privacy-safe 504-cell run log without modifying committed outputs.

2. Run an explicitly non-publishable pilot grid against Redis:

   ```bash
   docker compose up -d redis
   make research-experiments RESEARCH_ARGS='-pilot -seeds 20260717 \
     -output /tmp/taskforge-artifact-pilot'
   ```

   Pilot mode is the only mode that permits a dirty checkout or reduced grid.
   Its `dataset.json` says `publishable: false`, and publication analysis
   refuses it.

## Full reproduction (about 40 minutes on a 12-CPU host)

```bash
make research-experiments   # atomically replaces research/data after all cells pass
make research-analysis      # re-derives results/ and figures/
make artifact-integrity     # rebuilds the recorded binary and byte-compares outputs
```

Publication mode refuses any tracked or untracked source change, builds one
measured binary from `HEAD`, writes every cell into a separate staging
directory, validates the complete dataset, and replaces `research/data` only
after all cells pass. Use `RESEARCH_ARGS='-pilot ... -output /tmp/rerun'` for
exploration. Absolute numbers will differ on other hardware. Runs use the
dedicated Redis DB 14 and flush it between cells.

## Provenance and privacy

Every ledger record repeats the exact source commit and tree, measured binary
digest, `go.mod` and `go.sum` digests, build and runner arguments, Redis
configuration, sanitized OS/architecture/Go/CPU facts, result schema and
digest, and cell status. Raw results retain the neutral hostname
`research-host`; generic environment maps, home paths, user names, email-like
identifiers, credentials, and remote Redis addresses are not accepted.

`make artifact-integrity` uses the ledger to extract and rebuild the exact
measured source and binary. Dataset validation rejects dirty-pilot evidence,
missing or duplicate cells, failures, mixed revision/schema/binary/lock or
environment records, unexpected raw files, changed result bytes, and privacy
leaks before generating publication output.

## Packaging for Zenodo

```bash
make research-package
```

builds a deterministic `dist/taskforge-research-artifact.tar.gz` containing this directory,
the citation metadata, and a manifest of SHA-256 checksums. It does not
upload anything; archiving to Zenodo and adding the minted DOI to the citation
metadata are separate, human-approved steps.
