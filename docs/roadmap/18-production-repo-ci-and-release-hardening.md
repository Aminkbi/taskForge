# Phase 18: Production Repo, CI, and Release Hardening

## Commit goal

Raise the repository itself to production standards by making build, test, release, and operational documentation behavior reproducible and enforceable.

## Why this phase exists

A production queue is not only a runtime. It is also a release process, a CI policy, an operational contract, and a contributor experience.

The current repository already has a useful baseline, but large projects need stronger guarantees around toolchain pinning, CI coverage shape, release artifacts, and operational documentation. Without that, repo maturity lags behind runtime maturity.

TaskForge should treat repository hardening as product work rather than cleanup.

## Changes

### Split CI by purpose

Organize CI into distinct tracks such as:

- fast unit validation
- Redis-backed integration coverage
- race detection for concurrency-sensitive packages
- benchmark smoke or regression checks

This makes failures easier to interpret and prevents critical concurrency validation from being skipped accidentally.

### Make toolchain and cache behavior reproducible

Pin or document the versions and behavior of:

- Go toolchain
- static analysis tools
- formatting expectations
- local and CI cache paths

The goal is consistent behavior in CI, local development, and sandboxed environments.

### Harden release and artifact policy

Define how TaskForge produces release artifacts such as:

- versioned container images
- tagged source releases
- changelog or release notes expectations
- dependency and supply-chain verification steps

Production consumers should be able to reason about provenance and repeatability.

### Close repo-to-runtime contract gaps

Use this phase to fix mismatches between documented config and actual runtime behavior, including any config surface that is advertised but not truly implemented.

The repository should not overstate what the binaries do.

## Tests

- CI test: unit, integration, and race jobs run as separate required checks
- Tooling test: pinned format and lint behavior is reproducible in CI and local scripts
- Release smoke test: built binaries and containers start successfully from the release pipeline output
- Documentation test: operational commands and env var surfaces match the implemented binaries

## Acceptance criteria

- CI coverage is intentionally shaped for correctness, concurrency safety, and operational confidence
- Tooling and cache behavior are reproducible across local and CI environments
- Release and documentation expectations are explicit enough for real production consumers
