SHELL := /bin/bash

GO ?= go
GOPROXY ?=

.PHONY: run-worker run-scheduler run-api run-demo run-example-email run-example-media run-example-external-api test bench lint fmt compose-up compose-down

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
	$(GO) test ./...

bench:
	./scripts/bench.sh

lint:
	$(GO) vet ./...
	@test -z "$$($(GO)fmt -l .)" || (echo "gofmt reported unformatted files"; $(GO)fmt -l .; exit 1)
	@command -v staticcheck >/dev/null 2>&1 && staticcheck ./... || true

fmt:
	$(GO)fmt -w .

compose-up:
	docker compose up --build -d

compose-down:
	docker compose down -v
