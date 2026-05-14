SHELL := /bin/bash

GO ?= go
export GOCACHE ?= /tmp/taskforge-gocache

.PHONY: run-worker run-scheduler run-api run-demo run-example-email run-example-media run-example-external-api test integration-test race-test bench bench-smoke lint fmt release-smoke compose-up compose-down

run-worker:
	$(GO) run ./cmd/worker

run-scheduler:
	$(GO) run ./cmd/scheduler

run-api:
	$(GO) run ./cmd/api

run-demo:
	$(GO) run ./cmd/demo

run-example-email:
	$(GO) run ./cmd/example-email

run-example-media:
	$(GO) run ./cmd/example-media

run-example-external-api:
	$(GO) run ./cmd/example-external-api

test:
	$(SHELL) ./scripts/test.sh

integration-test:
	TASKFORGE_RUN_INTEGRATION=1 $(GO) test ./test/integration/...

race-test:
	$(GO) test -race ./...

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
