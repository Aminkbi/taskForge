# Agent Routing

Start with [the architecture map](docs/development/agent-context.md) for package
ownership, invariants, and the narrowest validation command. Use the Makefile
or `scripts/` as the command interface.

Keep focused changes focused; add observable tests with behavior changes and
run the narrowest relevant check. Go code must be `gofmt`-formatted. Put new
environment settings in `internal/config` with the `TASKFORGE_` prefix.

Roadmaps `01` through `30` are immutable historical records. For deferred
follow-up work, append a new roadmap file; do not commit it unless explicitly
requested. See [CONTRIBUTING.md](CONTRIBUTING.md) for PR expectations.
