SHELL := /bin/bash

GO ?= go
export GOCACHE ?= /tmp/taskforge-gocache

.PHONY: run-scheduler run-api run-demo test-demo test simulation-test simulation-replay model-check integration-test coverage race-test fuzz-smoke security-check benchmark-regression certification-report bench bench-smoke experiment-smoke experiment-trace experiment-neutral experiment-neutral-smoke research-experiments research-analysis research-check artifact-integrity research-package lint fmt docs-check certification-check release-smoke release-validate vuln-check compose-up compose-down compose-reset

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
	$(SHELL) ./scripts/benchmark-regression.sh

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
	$(GO) run ./cmd/experiment-trace -profile "$(PROFILE)" -output "$(TRACE)" $(TRACE_ARGS)

experiment-neutral:
	@test -n "$(TRACE)" || { echo "TRACE is required"; exit 2; }
	$(GO) run ./cmd/experiment-neutral -trace "$(TRACE)" $(NEUTRAL_ARGS)

experiment-neutral-smoke:
	$(SHELL) ./scripts/experiment-neutral-smoke.sh

research-experiments:
	$(SHELL) ./scripts/research-experiments.sh $(RESEARCH_ARGS)

research-analysis:
	$(GO) run ./cmd/experiment-analysis

research-check:
	$(SHELL) ./scripts/research-check.sh

artifact-integrity:
	$(SHELL) ./scripts/artifact-integrity.sh

research-package:
	$(SHELL) ./scripts/package-artifact.sh

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
