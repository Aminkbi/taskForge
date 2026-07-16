SHELL := /bin/bash

GO ?= go
export GOCACHE ?= /tmp/taskforge-gocache

.PHONY: run-scheduler run-api run-demo test-demo test integration-test race-test bench bench-smoke lint fmt release-smoke compose-up compose-down

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

integration-test:
	TASKFORGE_RUN_INTEGRATION=1 $(GO) test ./test/integration/...

race-test:
	$(SHELL) ./scripts/race.sh

bench:
	$(SHELL) ./scripts/bench.sh

bench-smoke:
	$(GO) test -run '^$$' -bench . -benchtime=1x ./...

lint:
	$(SHELL) ./scripts/lint.sh

fmt:
	$(GO)fmt -w .

release-smoke:
	$(SHELL) ./scripts/release-smoke.sh

compose-up:
	docker compose up --build -d

compose-down:
	docker compose down -v
