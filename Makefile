SHELL := /bin/bash

GO ?= go
export GOCACHE ?= /tmp/taskforge-gocache

.PHONY: run-scheduler run-api run-demo test-demo test simulation-test simulation-replay model-check integration-test coverage race-test fuzz-smoke security-check benchmark-regression certification-report bench bench-smoke lint fmt docs-check certification-check release-smoke release-validate vuln-check compose-up compose-down compose-reset

run-scheduler:
	$(GO) run ./cmd/scheduler

run-api:
	$(GO) run ./cmd/api

run-demo:
	$(GO) run ./examples/overload

test-demo:
	@test -n "$(TASKFORGE_INTEGRATION_REDIS_ADDR)" || { echo "TASKFORGE_INTEGRATION_REDIS_ADDR is required"; exit 2; }
	@test -n "$(TASKFORGE_INTEGRATION_REDIS_DB)" || { echo "TASKFORGE_INTEGRATION_REDIS_DB is required"; exit 2; }
	@test "$(TASKFORGE_INTEGRATION_REDIS_DB)" != "0" || { echo "TASKFORGE_INTEGRATION_REDIS_DB must be non-zero"; exit 2; }
	TASKFORGE_RUN_INTEGRATION=1 TASKFORGE_INTEGRATION_REDIS_ADDR="$(TASKFORGE_INTEGRATION_REDIS_ADDR)" TASKFORGE_INTEGRATION_REDIS_DB="$(TASKFORGE_INTEGRATION_REDIS_DB)" $(GO) test -count=1 ./test/integration/... -run '^TestOverloadDemoExecutableContract$$'

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
	@test -n "$(TASKFORGE_INTEGRATION_REDIS_ADDR)" || { echo "TASKFORGE_INTEGRATION_REDIS_ADDR is required"; exit 2; }
	@test -n "$(TASKFORGE_INTEGRATION_REDIS_DB)" || { echo "TASKFORGE_INTEGRATION_REDIS_DB is required"; exit 2; }
	@test "$(TASKFORGE_INTEGRATION_REDIS_DB)" != "0" || { echo "TASKFORGE_INTEGRATION_REDIS_DB must be non-zero"; exit 2; }
	TASKFORGE_RUN_INTEGRATION=1 TASKFORGE_INTEGRATION_REDIS_ADDR="$(TASKFORGE_INTEGRATION_REDIS_ADDR)" TASKFORGE_INTEGRATION_REDIS_DB="$(TASKFORGE_INTEGRATION_REDIS_DB)" $(GO) test ./test/integration/...

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
	$(GO) test -p 1 -run '^$$' -bench . -benchtime=1x ./...

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
