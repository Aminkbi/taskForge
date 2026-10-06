package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"uuid"

	"go.opentelemetry.io/otel/attribute"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/observability"
)

func (b *Broker) Publish(ctx context.Context, msg taskforge.Task, opts taskforge.PublishOptions) (taskforge.PublishResult, error) {
	if err := b.checkConfig(); err != nil {
		return taskforge.PublishResult{}, err
	}
	if msg.ID == "" {
		return taskforge.PublishResult{}, fmt.Errorf("publish task: missing id")
	}
	msg = msg.Clone()

	now := time.Now().UTC()
	msg = normalizePublishedMessage(msg, now)
	opts = opts.Normalize()
	msg, placement := b.routePublishedMessage(msg, opts)
	ctx, span := observability.StartQueueSpan(
		ctx,
		"taskforge.redis",
		"taskforge.publish",
		msg,
		attribute.Bool("taskforge.delayed", msg.ETA != nil && msg.ETA.After(now)),
	)
	defer span.End()
	msg.Headers = observability.InjectTraceContext(ctx, msg.Headers)

	result, queuedStateRecorded, err := b.publishMessage(ctx, msg, opts, now)
	if err != nil {
		observability.MarkSpanError(span, err)
		return taskforge.PublishResult{}, err
	}
	if placement.Shard != "" {
		result.Shard = placement.Shard
	}
	if placement.Rule != "" {
		result.RoutingRule = placement.Rule
	}
	if b.stateMode != StateModeDeliveryOnly && b.stateStore != nil && !queuedStateRecorded && result.Decision != taskforge.AdmissionDecisionRejected && !result.Deduplicated {
		if err := b.stateStore.RecordQueued(ctx, msg); err != nil {
			observability.MarkSpanError(span, err)
			b.logger.Warn("record queued task state failed", "task_id", msg.ID, "error", err)
		}
	}
	return result, nil
}

func (b *Broker) routePublishedMessage(msg taskforge.Task, opts taskforge.PublishOptions) (taskforge.Task, RoutingPlacement) {
	placement := RoutingPlacement{Queue: taskforge.EffectiveQueue(msg)}
	if b.routingPolicy == nil || opts.Source != taskforge.PublishSourceNew {
		return msg, placement
	}
	return b.routingPolicy.Apply(msg)
}

func (b *Broker) publishMessage(ctx context.Context, msg taskforge.Task, opts taskforge.PublishOptions, now time.Time) (taskforge.PublishResult, bool, error) {
	opts = opts.Normalize()
	queue := taskforge.EffectiveQueue(msg)
	result := taskforge.PublishResult{
		Decision: taskforge.AdmissionDecisionAccepted,
		Queue:    queue,
	}

	if opts.DeduplicationKey != "" && opts.Source != taskforge.PublishSourceDeadLetter && b.admissionPolicy(queue).Mode != AdmissionModeDisabled {
		exists, err := b.publishReceiptExists(ctx, opts.DeduplicationKey)
		if err != nil {
			return taskforge.PublishResult{}, false, err
		}
		if exists {
			result.Deduplicated = true
			return result, false, nil
		}
	}

	eval, err := b.evaluateAdmission(ctx, msg, opts, now)
	if err != nil {
		return taskforge.PublishResult{}, false, err
	}
	b.observeAdmissionDecision(queue, opts.Source, eval)

	result.Decision = eval.decision
	result.Reason = eval.reason

	switch eval.decision {
	case taskforge.AdmissionDecisionRejected:
		return result, false, &taskforge.AdmissionError{Queue: queue, Reason: eval.reason}
	case taskforge.AdmissionDecisionDeferred:
		if eval.deferUntil == nil {
			return taskforge.PublishResult{}, false, fmt.Errorf("publish task: deferred admission missing defer deadline")
		}
		msg = b.annotateDeferredMessage(msg, opts.Source, eval.reason, *eval.deferUntil, now)
		result.DeferredUntil = eval.deferUntil
	}

	if msg.ETA != nil && msg.ETA.After(now) {
		published, err := b.publishDelayed(ctx, msg, opts.DeduplicationKey, now)
		if err != nil {
			return taskforge.PublishResult{}, false, err
		}
		result.Deduplicated = !published
		if published {
			b.metrics.IncPublished(queue)
		}
		_, builtIn := b.stateStore.(*stateStore)
		return result, published && builtIn && b.stateMode != StateModeDeliveryOnly, nil
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return taskforge.PublishResult{}, false, fmt.Errorf("publish task: marshal message: %w", err)
	}

	published, stateRecorded, err := b.publishReadyTask(ctx, msg, payload, opts.DeduplicationKey, now)
	if err != nil {
		return taskforge.PublishResult{}, false, err
	}
	result.Deduplicated = !published
	if published {
		b.metrics.IncPublished(queue)
	}
	return result, stateRecorded, nil
}

func (b *Broker) publishDelayed(ctx context.Context, msg taskforge.Task, deduplicationKey string, now time.Time) (bool, error) {
	queue := taskforge.EffectiveQueue(msg)
	entryID := uuid.New().String()
	entryPayload, err := json.Marshal(delayedEntry{
		EntryID:      entryID,
		ScheduledFor: msg.ETA.UTC(),
		Message:      msg,
	})
	if err != nil {
		return false, fmt.Errorf("publish task: marshal delayed entry: %w", err)
	}

	taskKey := ""
	stateArgs := []any{0}
	if store, ok := b.stateStore.(*stateStore); ok && b.stateMode != StateModeDeliveryOnly {
		record, err := store.queuedRecord(msg, now)
		if err != nil {
			return false, err
		}
		taskKey = store.taskKey(record.taskID)
		stateArgs = []any{len(record.fields)}
		for field, value := range record.fields {
			stateArgs = append(stateArgs, field, value)
		}
	}

	if deduplicationKey == "" {
		args := []any{
			msg.ETA.UTC().UnixMilli(),
			string(entryPayload),
			queue,
			entryID,
			retryIndexFlag(msg),
		}
		args = append(args, stateArgs...)
		if err := publishDelayedScript.Run(
			ctx,
			b.client,
			[]string{b.delayedQueueKey(queue), b.delayedQueueIndexKey(), b.delayedRetryIndexKey(queue), taskKey},
			args...,
		).Err(); err != nil {
			return false, err
		}
		return true, nil
	}
	args := []any{
		msg.ETA.UTC().UnixMilli(),
		string(entryPayload),
		b.publishReceiptTTL().Milliseconds(),
		queue,
		entryID,
		retryIndexFlag(msg),
	}
	args = append(args, stateArgs...)
	published, err := publishDelayedWithReceiptScript.Run(
		ctx,
		b.client,
		[]string{
			b.delayedQueueKey(queue),
			b.publishReceiptKey(deduplicationKey),
			b.delayedQueueIndexKey(),
			b.delayedRetryIndexKey(queue),
			taskKey,
		},
		args...,
	).Int64()
	if err != nil {
		return false, err
	}
	return published == 1, nil
}
