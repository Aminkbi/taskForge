package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/logging"
	"github.com/aminkbi/taskforge/internal/observability"
)

func (b *Broker) Ack(ctx context.Context, delivery taskforge.Delivery) error {
	if err := b.checkConfig(); err != nil {
		return err
	}
	ctx, span := observability.StartQueueSpan(
		ctx,
		"taskforge.redis",
		"taskforge.ack",
		delivery.Message,
		deliverySpanAttributes(delivery)...,
	)
	defer span.End()

	pending, ttl, err := b.finalizeDelivery(ctx, delivery, nil)
	if err != nil {
		b.logDeliveryRejection("ack rejected", delivery, pending, ttl, err)
		observability.MarkSpanError(span, err)
		return err
	}

	return nil
}

func (b *Broker) Nack(ctx context.Context, delivery taskforge.Delivery, requeue bool) error {
	if err := b.checkConfig(); err != nil {
		return err
	}
	ctx, span := observability.StartQueueSpan(
		ctx,
		"taskforge.redis",
		"taskforge.nack",
		delivery.Message,
		append(deliverySpanAttributes(delivery), attribute.Bool("taskforge.requeue", requeue))...,
	)
	defer span.End()

	if requeue {
		pending, ttl, err := b.validatePendingDelivery(ctx, delivery)
		if err != nil {
			b.logDeliveryRejection("nack rejected", delivery, pending, ttl, err)
			observability.MarkSpanError(span, err)
			return err
		}
		requeued := delivery.Message
		requeued.ETA = nil
		if _, err := b.Publish(ctx, requeued, taskforge.PublishOptions{
			Source:           taskforge.PublishSourceRequeue,
			DeduplicationKey: fmt.Sprintf("requeue:%s", delivery.OwnershipKey()),
		}); err != nil {
			observability.MarkSpanError(span, err)
			return err
		}
	}

	pending, ttl, err := b.finalizeDelivery(ctx, delivery, nil)
	if err != nil {
		b.logDeliveryRejection("nack rejected", delivery, pending, ttl, err)
		observability.MarkSpanError(span, err)
		return err
	}
	return nil
}

func (b *Broker) ExtendLease(ctx context.Context, delivery taskforge.Delivery, _ time.Duration) error {
	if err := b.checkConfig(); err != nil {
		return err
	}
	ctx, span := observability.StartQueueSpan(
		ctx,
		"taskforge.redis",
		"taskforge.extend_lease",
		delivery.Message,
		deliverySpanAttributes(delivery)...,
	)
	defer span.End()

	authoritativeTTL := b.effectiveLeaseTTL(delivery.Message)
	errs, batchErr := b.ExtendLeases(ctx, []taskforge.Delivery{delivery})
	err := batchErr
	if len(errs) > 0 && errs[0] != nil {
		err = errs[0]
	}
	if err != nil {
		b.logLeaseExtensionRejection(ctx, delivery, authoritativeTTL, err)
		observability.MarkSpanError(span, err)
		return err
	}

	logging.WithDelivery(b.logger, delivery).Debug(
		"extended task lease",
		"lease_expiry", time.Now().UTC().Add(authoritativeTTL),
	)

	return nil
}

func (b *Broker) finalizeDelivery(ctx context.Context, delivery taskforge.Delivery, record *stateRecord) (redis.XPendingExt, time.Duration, error) {
	ttl := b.effectiveLeaseTTL(delivery.Message)
	if delivery.Execution.DeliveryID == "" {
		return redis.XPendingExt{}, ttl, taskforge.ErrUnknownDelivery
	}

	queue := taskforge.EffectiveQueue(delivery.Message)
	streamKey := b.queueStreamKey(queue, delivery.Message.FairnessKey)
	keys := []string{streamKey, b.leaseDeadlineKey(streamKey)}
	args := []any{
		b.groupName(queue),
		delivery.Execution.DeliveryID,
		delivery.Execution.LeaseOwner,
		ttl.Milliseconds(),
	}
	if record != nil {
		keys = append(keys, b.stateStore.(*stateStore).taskKey(record.taskID))
		stateTTL := b.stateStore.(*stateStore).recordTTL(record.state)
		stateTTLMillis := int64(-1)
		if stateTTL > 0 {
			stateTTLMillis = stateTTL.Milliseconds()
		}
		args = append(args, stateTTLMillis, len(record.fields))
		for field, value := range record.fields {
			args = append(args, field, value)
		}
	}
	result, err := finalizeDeliveryScript.Run(
		ctx,
		b.client,
		keys,
		args...,
	).Result()
	if err != nil {
		return redis.XPendingExt{}, ttl, fmt.Errorf("finalize task: %w", err)
	}
	values, ok := result.([]any)
	if !ok || len(values) != 3 {
		return redis.XPendingExt{}, ttl, fmt.Errorf("finalize task: unexpected result %T", result)
	}
	code, ok := int64Value(values[0])
	if !ok {
		return redis.XPendingExt{}, ttl, fmt.Errorf("finalize task: unexpected status %T", values[0])
	}
	owner, _ := values[1].(string)
	idleMillis, _ := int64Value(values[2])
	pending := redis.XPendingExt{
		ID:       delivery.Execution.DeliveryID,
		Consumer: owner,
		Idle:     time.Duration(idleMillis) * time.Millisecond,
	}
	switch code {
	case 1:
		return pending, ttl, nil
	case 2:
		return pending, ttl, taskforge.ErrStaleDelivery
	case 3:
		return pending, ttl, taskforge.ErrDeliveryExpired
	default:
		return pending, ttl, taskforge.ErrUnknownDelivery
	}
}

func (b *Broker) validatePendingDelivery(ctx context.Context, delivery taskforge.Delivery) (redis.XPendingExt, time.Duration, error) {
	if delivery.Execution.DeliveryID == "" {
		return redis.XPendingExt{}, 0, taskforge.ErrUnknownDelivery
	}

	queue := taskforge.EffectiveQueue(delivery.Message)
	pendingEntries, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: b.queueStreamKey(queue, delivery.Message.FairnessKey),
		Group:  b.groupName(queue),
		Start:  delivery.Execution.DeliveryID,
		End:    delivery.Execution.DeliveryID,
		Count:  1,
	}).Result()
	if err != nil {
		return redis.XPendingExt{}, 0, fmt.Errorf("inspect pending delivery: %w", err)
	}
	if len(pendingEntries) == 0 || pendingEntries[0].ID != delivery.Execution.DeliveryID {
		return redis.XPendingExt{}, 0, taskforge.ErrUnknownDelivery
	}

	pending := pendingEntries[0]
	ttl := b.effectiveLeaseTTL(delivery.Message)
	if delivery.Execution.LeaseOwner != "" && pending.Consumer != delivery.Execution.LeaseOwner {
		return pending, ttl, taskforge.ErrStaleDelivery
	}
	if ttl > 0 && pending.Idle >= ttl {
		return pending, ttl, taskforge.ErrDeliveryExpired
	}

	return pending, ttl, nil
}

func (b *Broker) logLeaseExtensionRejection(ctx context.Context, delivery taskforge.Delivery, ttl time.Duration, err error) {
	pending := redis.XPendingExt{}
	if errors.Is(err, taskforge.ErrStaleDelivery) || errors.Is(err, taskforge.ErrDeliveryExpired) {
		if entries, inspectErr := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: b.queueStreamKey(taskforge.EffectiveQueue(delivery.Message), delivery.Message.FairnessKey),
			Group:  b.groupName(taskforge.EffectiveQueue(delivery.Message)),
			Start:  delivery.Execution.DeliveryID,
			End:    delivery.Execution.DeliveryID,
			Count:  1,
		}).Result(); inspectErr == nil && len(entries) == 1 {
			pending = entries[0]
		}
	}
	b.logDeliveryRejection("lease extension rejected", delivery, pending, ttl, err)
}

func (b *Broker) logDeliveryRejection(message string, delivery taskforge.Delivery, pending redis.XPendingExt, ttl time.Duration, err error) {
	if b.logger == nil {
		return
	}

	var expiresAt any
	if ttl > 0 && pending.Idle > 0 {
		expiresAt = time.Now().UTC().Add(ttl - pending.Idle)
	}

	logging.WithDelivery(b.logger, delivery).Warn(
		message,
		"current_owner", pending.Consumer,
		"lease_expiry", expiresAt,
		"error", err,
	)
}
