package taskforge

import (
	"context"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
)

type PublishSource string

const (
	PublishSourceNew        PublishSource = "new"
	PublishSourceRequeue    PublishSource = "requeue"
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
	Shard         string
	RoutingRule   string
	DeferredUntil *time.Time
	Deduplicated  bool
}

type Publisher interface {
	Publish(ctx context.Context, task Task, options PublishOptions) (PublishResult, error)
}

func (o PublishOptions) toBrokerOptions() broker.PublishOptions {
	return broker.PublishOptions{
		Source:           broker.PublishSource(o.Source),
		DeduplicationKey: o.DeduplicationKey,
	}
}

func publishResultFromBroker(result broker.PublishResult) PublishResult {
	return PublishResult{
		Decision:      AdmissionDecision(result.Decision),
		Reason:        result.Reason,
		Queue:         result.Queue,
		Shard:         result.Shard,
		RoutingRule:   result.RoutingRule,
		DeferredUntil: cloneTimePtr(result.DeferredUntil),
		Deduplicated:  result.Deduplicated,
	}
}
