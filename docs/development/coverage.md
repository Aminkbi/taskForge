# Coverage checks

Run `make coverage` to produce a local coverage report and enforce the
critical-package floors. Profiles default to `/tmp/taskforge-coverage`; set
`TASKFORGE_COVERAGE_DIR` to retain them elsewhere.

The report tests every source file in the listed package. It does not exclude
generated-looking, transport, or error-handling files to inflate a result.
The floors were set from the T09 measured unit baseline after adding direct
protocol tests: 45% overall, 72% core public API, 18% Redis, 28% scheduler,
and 72% worker. They are deliberately modest for the broad Redis and scheduler
packages, whose critical behavior is also exercised by Redis integration tests;
future changes must raise a floor when coverage grows materially.

Redis integration tests remain a required separate check:

```sh
make integration-test
```

They verify Redis ownership, fencing, and persistence semantics that unit
coverage cannot faithfully simulate.

The bounded [deterministic simulator](deterministic-simulation.md) is also a
separate CI check. It exercises repeatable fault interleavings without Redis:

```sh
make simulation-test
```

The named fuzz seeds run with the normal Go test suite. To explore additional
inputs locally, run one target at a time; any failure is saved by Go in that
package's `testdata/fuzz` corpus and is replayed by `go test` thereafter.

```sh
go test ./ -run=^$ -fuzz=FuzzConfigNormalizeScheduleValidation -fuzztime=30s
go test ./redis -run=^$ -fuzz=FuzzDecodeDelayedEntry -fuzztime=30s
go test ./internal/scheduler -run=^$ -fuzz=FuzzParseLeadershipFence -fuzztime=30s
```
