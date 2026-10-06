# Coverage and fuzzing

`make coverage` tests all production files and enforces statement coverage
floors: 50% overall, 72% core public API, 18% Redis, 28% scheduler, and 72% worker.
Profiles default to `/tmp/taskforge-coverage`; override with
`TASKFORGE_COVERAGE_DIR`. Raise floors when coverage grows materially.

Coverage complements `make integration-test` for Redis semantics and
`make simulation-test` for deterministic protocol faults.

Named fuzz seeds run in `make test`. Use `make fuzz-smoke` for short mutation
runs or explore one target directly:

```bash
go test ./ -run=^$ -fuzz=FuzzConfigNormalizeScheduleValidation -fuzztime=30s
go test ./redis -run=^$ -fuzz=FuzzDecodeDelayedEntry -fuzztime=30s
go test ./internal/scheduler -run=^$ -fuzz=FuzzParseLeadershipFence -fuzztime=30s
```

Go saves failures in the package's `testdata/fuzz` corpus and replays them in
normal tests. Keep a minimized reproducer for a fixed bug.
