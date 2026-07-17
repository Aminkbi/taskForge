# Reliability Contract

TaskForge provides at-least-once background execution over a direct,
standalone Redis primary. A task may execute more than once. Applications must
ensure handler side effects are idempotent and must treat a successful handler return
as incomplete until TaskForge durably accepts that delivery owner's
acknowledgement.

The machine-readable index for this contract is the
[certification manifest](../../certification/manifest.json). `make
certification-check` verifies that each certified claim names an executable
check, each assumption is explicit, every check is a documented Make target,
and every committed evidence path exists.

## Guaranteed semantics and executable evidence

| Contract | Executable evidence |
| --- | --- |
| Task ID identifies logical work. Queue/fairness stream plus stream-local delivery ID identifies a broker entry; the consumer owner fences one lease. A stale or expired owner cannot ack, retry, dead-letter, or extend newer work. | `make test`, `make race-test`, `make integration-test`, `make simulation-test`, `make model-check` |
| Retry preserves task identity and placement, is bounded by delivery policy, and is deduplicated by failed delivery. `retry_scheduled` completes a delivery but not the logical task. | `make test`, `make integration-test`, `make simulation-test`, `make model-check` |
| DLQ publication must succeed before the source delivery is acknowledged. Failure leaves the source recoverable. | `make test`, `make integration-test`, `make simulation-test` |
| Scheduler writes require the current leadership fence. A stale leader cannot release delayed, retry, or recurring work. | `make test`, `make integration-test`, `make simulation-test`, `make model-check` |
| New publishes are routed once. Retry, due release, recurrence, DLQ, and recovery preserve established placement. | `make test`, `make integration-test` |
| Public configuration and `TASKFORGE_` environment decoding normalize through `taskforge.Config`; sidecars do not maintain a second runtime control model. | `make test`, `make certification-check` |
| Sensitive HTTP routes are absent without authentication, task payload/results are redacted, and server resource limits have safe defaults. | `make test` |
| TaskForge rejects Redis Cluster, Sentinel, and replica endpoints during validated startup. TLS configuration is explicit. | `make test`, `make integration-test` |
| Release dry-runs build the supported binaries and images, verify checksums, SBOM/provenance metadata, non-root image execution, labels, health checks, and reproducible binary metadata without publishing. | `make release-smoke`, `make release-validate`, `make vuln-check` |
| The public demo executes publish-to-handler behavior, and comparative experiment smoke runs retain raw samples separately from derived reports. Neither is an SLA or superiority claim. | `make test-demo`, `make experiment-smoke`, `make bench-smoke` |

Simulation and model checking are bounded evidence, not proofs over every
deployment or unbounded execution. Their modeled state, bounds, mutations, and
code mapping are documented in [deterministic simulation](../development/deterministic-simulation.md)
and [protocol models](../development/protocol-models.md).

## Assumptions and operator obligations

- **Handler idempotency:** TaskForge cannot guarantee external side effects exactly
  once. Handlers must use an application-owned idempotency or transaction
  boundary.
- **Redis durability:** Accepted work is only as durable as the operator's Redis
  persistence, replication, backup, capacity, and recovery configuration.
- **Deployment boundary:** Loopback defaults are safe for a single host.
  Non-loopback HTTP exposure requires an authentication token and a trusted TLS
  reverse proxy. Redis credentials and private CA material remain operator-owned.
- **Timing and liveness:** Lease, leadership, retry, and fairness liveness assume
  bounded pauses, a sufficiently accurate local clock, and eventual access to a
  writable Redis primary. Safety rejects stale ownership even when these
  liveness assumptions fail.
- **Evidence bounds:** Race detection, seeded simulation, bounded state-space
  exploration, smoke workloads, and vulnerability databases reduce known risk;
  they do not establish absence of every race, protocol defect, workload
  pathology, or vulnerability.

## Certification commands

The complete gate and its prerequisites are listed in the
[manifest](../../certification/manifest.json). The primary commands are:

```bash
make test
make lint
make coverage
make race-test
make integration-test
make test-demo
make simulation-test
make model-check
make bench-smoke
make experiment-smoke
make vuln-check
make release-smoke
make release-validate
make docs-check
make certification-check
```

`make certification-report` is the command interface for a versioned JSON and
Markdown release attachment (pass flags through `CERTIFICATION_ARGS`). It can
execute selected manifest checks with `-run` or consume CI-produced results
with `-input`. Every unreported check is explicitly
`skipped`; if any required check is skipped the report status is `incomplete`,
not `passed`. Supply `SOURCE_DATE_EPOCH` (or use the command's commit-time
default) with the commit to reproduce the same report bytes for identical
inputs. `make release-validate` emits an attachable artifact-only report in
`dist/`; it is deliberately incomplete because it does not rerun the broader
reliability suite.

Consumed-result files use `taskforge-certification-results/v1` as defined by
[`certification/results.schema.json`](../../certification/results.schema.json).
They contain check IDs and `passed`, `failed`, or `skipped` states, and may
include machine-specific benchmark deltas. Results from different commits or
environments must not be combined.

Redis-backed commands require a writable Redis primary on
`localhost:6379`. `make experiment-smoke` may start the repository's Redis
Compose service and uses dedicated database 14. `make release-validate`
requires a working Docker daemon with Buildx. Generated experiment and release
outputs are local evidence for the exact tree and environment that produced
them; they are intentionally not committed as certification results.
