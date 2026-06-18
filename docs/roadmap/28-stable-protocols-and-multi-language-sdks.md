# Phase 28: Stable Protocols and Multi-Language SDKs

## Commit goal

Define stable wire protocols and SDK contracts so TaskForge can be used from multiple languages without coupling every client to Go internals.

## Why this phase exists

A queue system competes on ecosystem as much as runtime semantics. Celery is successful because Python developers can define, route, inspect, and compose tasks naturally. TaskForge can keep its Go core while supporting teams that publish from Python, TypeScript, Java, Rust, or other services.

This phase should happen after task, workflow, effect, and event models are stable enough to avoid churning every client.

## Changes

### Define versioned schemas

Create versioned schemas for:

- task publish request and result
- delivery metadata
- retry policy
- workflow graph definition
- effect record
- task and workflow state views
- event stream payloads
- operator API errors

Schemas should support additive evolution and clear deprecation policy.

### Add a public HTTP/gRPC boundary

Decide which APIs are public and stable:

- publish task
- publish workflow
- query task or workflow
- stream events
- inspect queue and worker state
- operator actions where appropriate

Internal Redis formats should remain private.

### Build SDKs in priority order

Start with:

- Go SDK over the existing package surface plus public API client
- Python SDK for Celery migration and data teams
- TypeScript SDK for web and platform services

Later SDKs should be generated where possible from the stable schemas.

### Provide migration adapters

Add helpers for:

- mapping Celery task names, queues, routes, retries, and ETA to TaskForge publish requests
- importing simple chain/group workflow shapes
- documenting semantic differences such as acknowledgement timing and idempotency requirements

Adapters should make migration easier without promising transparent drop-in compatibility.

## Tests

- Contract test: schemas are backward compatible across minor versions
- SDK integration test: Go, Python, and TypeScript clients can publish and query tasks
- Event contract test: SDK event decoders handle unknown additive fields
- Migration test: representative Celery-style task options map to explicit TaskForge requests
- Documentation test: public API examples compile or run

## Acceptance criteria

- Public protocols are versioned and separate from internal storage formats
- Multiple languages can publish, query, and observe TaskForge workloads
- Celery migration guidance is practical and honest about semantic differences
