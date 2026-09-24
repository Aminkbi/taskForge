SHELL := /bin/bash

GO ?= go
export GOCACHE ?= /tmp/taskforge-gocache

.PHONY: run-scheduler run-api run-demo test-demo test simulation-test simulation-replay model-check integration-test coverage race-test fuzz-smoke security-check benchmark-regression certification-report bench bench-smoke research-test experiment-smoke experiment-trace experiment-neutral experiment-neutral-smoke frontier-check research-experiments research-analysis research-check artifact-integrity research-package second-wave-freeze second-wave-run second-wave-analysis second-wave-check second-wave-package third-wave-check third-wave-run third-wave-analysis third-wave-controls-check queue-controls-run queue-controls-check lint fmt docs-check certification-check release-smoke release-validate vuln-check compose-up compose-down compose-reset

run-scheduler:
	$(GO) run ./cmd/scheduler

run-api:
	$(GO) run ./cmd/api

run-demo:
	$(GO) run ./examples/overload

test-demo:
	TASKFORGE_RUN_INTEGRATION=1 $(GO) test -count=1 ./test/integration/... -run '^TestOverloadDemoExecutableContract$$'

test:
	$(SHELL) ./scripts/test.sh

simulation-test:
	$(GO) test -count=1 ./internal/sim

simulation-replay:
	@test -n "$(TASKFORGE_SIM_SEED)" || { echo "TASKFORGE_SIM_SEED is required"; exit 2; }
	$(GO) test -count=1 -run '^TestReplaySeed$$' -v ./internal/sim

model-check:
	$(GO) test -count=1 ./internal/modelcheck
	$(GO) run ./internal/modelcheck/cmd/modelcheck -model all -max-depth 32 -max-states 100000

integration-test:
	TASKFORGE_RUN_INTEGRATION=1 $(GO) test ./test/integration/...

coverage:
	$(SHELL) ./scripts/coverage.sh

race-test:
	$(SHELL) ./scripts/race.sh

fuzz-smoke:
	$(SHELL) ./scripts/fuzz-smoke.sh

security-check:
	$(SHELL) ./scripts/security-check.sh

benchmark-regression:
	$(SHELL) ./scripts/benchmark-regression.sh $(BENCHMARK_ARGS)

certification-report:
	$(GO) run ./cmd/certify $(CERTIFICATION_ARGS)

bench:
	$(SHELL) ./scripts/bench.sh

bench-smoke:
	$(GO) test -run '^$$' -bench . -benchtime=1x ./...

experiment-smoke:
	$(SHELL) ./scripts/experiment-smoke.sh

experiment-trace:
	@test -n "$(PROFILE)" || { echo "PROFILE is required"; exit 2; }
	@test -n "$(TRACE)" || { echo "TRACE is required"; exit 2; }
	$(GO) -C research run ./cmd/experiment-trace -profile "$(abspath $(PROFILE))" -output "$(abspath $(TRACE))" $(TRACE_ARGS)

experiment-neutral:
	@test -n "$(TRACE)" || { echo "TRACE is required"; exit 2; }
	$(GO) -C research run ./cmd/experiment-neutral -trace "$(abspath $(TRACE))" $(NEUTRAL_ARGS)

experiment-neutral-smoke:
	$(SHELL) ./scripts/experiment-neutral-smoke.sh

research-test:
	$(GO) -C research test ./...

frontier-check:
	@test -n "$(FRONTIER_RESULTS)" || { echo "FRONTIER_RESULTS is required"; exit 2; }
	$(GO) -C research run ./cmd/experiment-frontier-check -input "$(abspath $(FRONTIER_RESULTS))" -max-throughput-loss 0.15

research-experiments:
	$(SHELL) ./scripts/research-experiments.sh $(RESEARCH_ARGS)

research-analysis:
	$(GO) -C research run ./cmd/experiment-analysis

research-check:
	$(SHELL) ./scripts/research-check.sh

artifact-integrity:
	$(SHELL) ./scripts/artifact-integrity.sh

research-package:
	$(SHELL) ./scripts/package-artifact.sh

second-wave-freeze:
	$(SHELL) ./scripts/second-wave-freeze.sh

second-wave-run:
	$(SHELL) ./scripts/second-wave-run.sh

second-wave-analysis:
	$(GO) -C research run ./cmd/experiment-study-analysis

second-wave-check:
	$(SHELL) ./scripts/second-wave-check.sh

second-wave-package:
	$(SHELL) ./scripts/second-wave-package.sh

third-wave-check:
	$(SHELL) ./scripts/third-wave-check.sh

third-wave-controls-check:
	$(SHELL) ./scripts/third-wave-controls-check.sh

third-wave-run:
	python3 scripts/third-wave-run.py $(THIRD_WAVE_ARGS)

third-wave-analysis:
	python3 scripts/third-wave-analysis.py $(THIRD_WAVE_ARGS)

lint:
	$(SHELL) ./scripts/lint.sh

fmt:
	$(GO)fmt -w .

docs-check: certification-check
	$(SHELL) ./scripts/docs-check.sh

certification-check:
	$(GO) test ./certification

release-smoke:
	$(SHELL) ./scripts/release-smoke.sh

release-validate:
	$(SHELL) ./scripts/release-validate.sh

vuln-check:
	$(SHELL) ./scripts/vuln-check.sh

compose-up:
	docker compose up --build -d

compose-down:
	docker compose down

compose-reset:
	docker compose down -v

queue-controls-run:
	@test -n "$(QUEUE_CONTROLS_OUTPUT)" || { echo "QUEUE_CONTROLS_OUTPUT is required"; exit 2; }
	python3 scripts/queue-controls-run.py --output "$(abspath $(QUEUE_CONTROLS_OUTPUT))"

queue-controls-check:
	@test -d research/queue-controls/data
	python3 scripts/queue-controls-analysis.py --data research/queue-controls/data --output /tmp/taskforge-queue-controls-analysis
	cmp research/queue-controls/data/analysis/analysis.json /tmp/taskforge-queue-controls-analysis/analysis.json
	cmp research/queue-controls/data/analysis/analysis.md /tmp/taskforge-queue-controls-analysis/analysis.md
