SHELL := /bin/bash

GO ?= go
GOPROXY ?=

.PHONY: run-worker run-scheduler run-api run-demo test lint fmt compose-up compose-down

run-worker:
	$(GO) run ./cmd/worker

run-scheduler:
	$(GO) run ./cmd/scheduler

run-api:
	$(GO) run ./cmd/api

run-demo:
	$(GO) run ./cmd/demo

test:
	$(GO) test ./...

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
