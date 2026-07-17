#!/usr/bin/env bash
set -euo pipefail

export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"

# This deterministic local check complements (but does not replace) the
# reachable dependency scan in vuln-check.
go vet ./...
