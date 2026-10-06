#!/usr/bin/env bash
set -euo pipefail

export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"

# Static checks complement the reachable dependency scan in vuln-check.
go vet ./...
