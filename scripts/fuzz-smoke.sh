#!/usr/bin/env bash
set -euo pipefail

export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"

go test ./ -run '^$' -fuzz '^FuzzConfigNormalizeScheduleValidation$' -fuzztime=1s
go test ./redis -run '^$' -fuzz '^FuzzDecodeDelayedEntry$' -fuzztime=1s
go test ./internal/scheduler -run '^$' -fuzz '^FuzzParseLeadershipFence$' -fuzztime=1s
