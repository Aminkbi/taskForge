#!/usr/bin/env bash
set -euo pipefail

export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"

# The race detector is module-scoped, so both modules are exercised.
go test -race ./...
go -C research test -race ./...
