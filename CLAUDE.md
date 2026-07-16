# Claude Routing

Read [the architecture map](docs/development/agent-context.md), then open only
the owning package and its nearby tests. Use the Makefile or `scripts/` for
validation; use a focused `go test ./package -run TestName` during iteration.

Preserve at-least-once delivery semantics and call out reliability changes.
New environment settings belong in `internal/config` and use `TASKFORGE_`.
Roadmaps `01` through `30` are immutable; append new deferred work and do not
commit it unless requested.
