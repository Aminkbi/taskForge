package broker

import (
	"context"
	"time"
)

type PublishSource string

const (
	PublishSourceNew        PublishSource = "new"
	PublishSourceRetry      PublishSource = "retry"
	PublishSourceDueRelease PublishSource = "due_release"
	PublishSourceRecurring  PublishSource = "recurring"
	PublishSourceDLQReplay  PublishSource = "dlq_replay"
	PublishSourceDeadLetter PublishSource = "dead_letter"
)

type PublishOptions struct {
	Source           PublishSource
	DeduplicationKey string
}

func (o PublishOptions) Normalize() PublishOptions {
	if o.Source == "" {
		o.Source = PublishSourceNew
	}
	return o
}

type AdmissionDecision string

const (
	AdmissionDecisionAccepted AdmissionDecision = "accepted"
	AdmissionDecisionDeferred AdmissionDecision = "deferred"
	AdmissionDecisionRejected AdmissionDecision = "rejected"
)

type PublishResult struct {
	Decision      AdmissionDecision
	Reason        string
	Queue         string
	DeferredUntil *time.Time
	Deduplicated  bool
}

// Broker exposes TaskForge's internal at-least-once delivery contract.
// A logical task may be delivered more than once, and brokers must reject
// stale acknowledgements so an older delivery owner cannot finalize newer work.
type Broker interface {
	Publish(ctx context.Context, msg TaskMessage, opts PublishOptions) (PublishResult, error)
	Reserve(ctx context.Context, queue, consumerID string) (Delivery, error)
	Ack(ctx context.Context, delivery Delivery) error
	Nack(ctx context.Context, delivery Delivery, requeue bool) error
	ExtendLease(ctx context.Context, delivery Delivery, ttl time.Duration) error
}
