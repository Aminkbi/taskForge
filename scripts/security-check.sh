#!/usr/bin/env bash
set -euo pipefail

export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"

# This deterministic local check complements (but does not replace) the
# reachable dependency scan in vuln-check. Module-scoped vet does not cross
# module boundaries, so the nested research module is checked explicitly.
go vet ./...
go -C research vet ./...
