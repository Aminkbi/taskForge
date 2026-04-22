// Package taskforge contains package-level documentation for the TaskForge module.
//
// TaskForge uses an at-least-once execution contract. A logical task may be
// delivered more than once, so handlers must be idempotent and the runtime
// distinguishes logical task identity from individual broker delivery attempts.
// Handlers are also expected to honor context cancellation because worker drain
// may force-abandon running deliveries after the configured shutdown timeout.
package taskforge
