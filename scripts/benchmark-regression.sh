#!/usr/bin/env bash
set -euo pipefail

test -s certification/benchmark-baseline.json
go test -run '^$' -bench . -benchtime=1x ./...
