package brokerredis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/fairness"
	"github.com/aminkbi/taskforge/internal/logging"
	"github.com/aminkbi/taskforge/internal/observability"
	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
	"github.com/aminkbi/taskforge/internal/tasks"
)

const (
	defaultPrefix            = "taskforge"
	defaultReserveTimeout    = time.Second
	defaultPublishReceiptTTL = 7 * 24 * time.Hour
	streamPayloadField       = "message"
	reclaimScanCount         = 20
)

var (
	publishReadyWithReceiptScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("XADD", KEYS[1], "*", ARGV[1], ARGV[2])
redis.call("PSETEX", KEYS[2], ARGV[3], "1")
return 1
`)
	publishFairReadyWithReceiptScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[4]) == 1 then
  return 0
end
redis.call("SADD", KEYS[1], ARGV[1])
redis.call("XADD", KEYS[2], "*", ARGV[2], ARGV[3])
redis.call("LPUSH", KEYS[3], ARGV[4])
redis.call("PSETEX", KEYS[4], ARGV[5], "1")
return 1
`)
	publishDelayedWithReceiptScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[2])
redis.call("PSETEX", KEYS[2], ARGV[3], "1")
return 1
`)
)

type RedisBroker struct {
	client            *redis.Client
	logger            *slog.Logger
	metrics           *observability.Metrics
	leaseTTL          time.Duration
	reserveTTL        time.Duration
	prefix            string
	hostname          string
	instanceID        string
	fairnessPolicies  map[string]*fairness.Policy
	admissionPolicies map[string]AdmissionPolicy
	admissionStateMu  sync.RWMutex
	admissionStates   map[string]observability.AdmissionStatusSnapshot
	budgetStore       *BudgetStore
	adaptiveStore     *AdaptiveStateStore
}

func New(client *redis.Client, logger *slog.Logger, leaseTTL time.Duration, metrics *observability.Metrics) *RedisBroker {
	return NewWithOptions(client, logger, leaseTTL, metrics, Options{})
}

type Options struct {
	ReserveTimeout    time.Duration
	FairnessPolicies  map[string]*fairness.Policy
	AdmissionPolicies map[string]AdmissionPolicy
	DependencyBudgets map[string]int
}

func NewWithOptions(client *redis.Client, logger *slog.Logger, leaseTTL time.Duration, metrics *observability.Metrics, options Options) *RedisBroker {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	reserveTimeout := options.ReserveTimeout
	if reserveTimeout <= 0 {
		reserveTimeout = defaultReserveTimeout
	}

	return &RedisBroker{
		client:            client,
		logger:            logger,
		metrics:           metrics,
		leaseTTL:          leaseTTL,
		reserveTTL:        reserveTimeout,
		prefix:            defaultPrefix,
		hostname:          hostname,
		instanceID:        fmt.Sprintf("%d", os.Getpid()),
		fairnessPolicies:  cloneFairnessPolicies(options.FairnessPolicies),
		admissionPolicies: cloneAdmissionPolicies(options.AdmissionPolicies),
		admissionStates:   make(map[string]observability.AdmissionStatusSnapshot),
		budgetStore:       NewBudgetStore(client, metrics, defaultPrefix, options.DependencyBudgets),
		adaptiveStore:     NewAdaptiveStateStore(client, defaultPrefix),
	}
}

func (b *RedisBroker) BudgetManager() *BudgetStore {
	return b.budgetStore
}

func (b *RedisBroker) AdaptiveStateStore() *AdaptiveStateStore {
	return b.adaptiveStore
}

func (b *RedisBroker) DependencyBudgetUsageSnapshots(ctx context.Context) ([]observability.DependencyBudgetUsageSnapshot, error) {
	if b.budgetStore == nil {
		return nil, nil
	}
	return b.budgetStore.DependencyBudgetUsageSnapshots(ctx)
}

func (b *RedisBroker) AdaptiveStatusSnapshot(ctx context.Context, pool string) (observability.AdaptivePoolSnapshot, error) {
	if b.adaptiveStore == nil {
		return observability.AdaptivePoolSnapshot{Pool: pool}, nil
	}
	return b.adaptiveStore.AdaptiveStatusSnapshot(ctx, pool)
}

func (b *RedisBroker) Ping(ctx context.Context) error {
	return b.client.Ping(ctx).Err()
}

func (b *RedisBroker) Publish(ctx context.Context, msg broker.TaskMessage, opts broker.PublishOptions) (broker.PublishResult, error) {
	if msg.ID == "" {
		return broker.PublishResult{}, fmt.Errorf("publish task: missing id")
	}

	now := time.Now().UTC()
	msg = normalizePublishedMessage(msg, now)
	opts = opts.Normalize()
	ctx, span := observability.StartQueueSpan(
		ctx,
		"taskforge.brokerredis",
		"taskforge.publish",
		msg,
		attribute.Bool("taskforge.delayed", msg.ETA != nil && msg.ETA.After(now)),
	)
	defer span.End()
	msg.Headers = observability.InjectTraceContext(ctx, msg.Headers)

	result, err := b.publishMessage(ctx, msg, opts, now)
	if err != nil {
		observability.MarkSpanError(span, err)
		return broker.PublishResult{}, err
	}
	return result, nil
}

func (b *RedisBroker) Reserve(ctx context.Context, queue, consumerID string) (broker.Delivery, error) {
	started := time.Now()
	defer func(queue string) {
		b.metrics.ObserveReserveLatency(normalizeQueue(queue), time.Since(started).Seconds())
	}(queue)

	queue = normalizeQueue(queue)
	if b.fairnessPolicy(queue) != nil {
		return b.reserveFair(ctx, queue, consumerID)
	}
	streamKey := b.streamKey(queue)
	groupName := b.groupName(queue)
	consumerName := b.consumerName(consumerID)

	if err := b.ensureGroup(ctx, streamKey, groupName); err != nil {
		return broker.Delivery{}, err
	}

	if reclaimed, ok, err := b.reclaimExpiredDelivery(ctx, queue, streamKey, groupName, consumerName); err != nil {
		return broker.Delivery{}, err
	} else if ok {
		return reclaimed, nil
	}

	streams, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamKey, ">"},
		Count:    1,
		Block:    b.reserveTTL,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return broker.Delivery{}, broker.ErrNoTask
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return broker.Delivery{}, err
		}
		return broker.Delivery{}, fmt.Errorf("reserve task: %w", err)
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return broker.Delivery{}, broker.ErrNoTask
	}

	entry := streams[0].Messages[0]
	msg, err := decodeTaskMessage(entry)
	if err != nil {
		return broker.Delivery{}, fmt.Errorf("reserve task: %w", err)
	}
	if msg.Queue == "" {
		msg.Queue = queue
	}

	spanCtx := observability.ExtractTraceContext(ctx, msg.Headers)
	ttl := b.effectiveLeaseTTL(msg)
	now := time.Now().UTC()
	delivery := newDelivery(msg, queue, consumerName, entry.ID, now, ttl, deliveryCount(msg, 0))
	_, span := observability.StartQueueSpan(
		spanCtx,
		"taskforge.brokerredis",
		"taskforge.reserve",
		msg,
		deliverySpanAttributes(delivery)...,
	)
	defer span.End()

	logging.WithDelivery(b.logger, delivery).Info("reserved task delivery")

	return delivery, nil
}

func (b *RedisBroker) Ack(ctx context.Context, delivery broker.Delivery) error {
	ctx, span := observability.StartQueueSpan(
		ctx,
		"taskforge.brokerredis",
		"taskforge.ack",
		delivery.Message,
		deliverySpanAttributes(delivery)...,
	)
	defer span.End()

	pending, ttl, err := b.validatePendingDelivery(ctx, delivery)
	if err != nil {
		b.logDeliveryRejection("ack rejected", delivery, pending, ttl, err)
		observability.MarkSpanError(span, err)
		return err
	}

	queue := tasks.EffectiveQueue(delivery.Message)
	streamKey := b.queueStreamKey(queue, delivery.Message.FairnessKey)
	acked, err := b.client.XAck(ctx, streamKey, b.groupName(queue), delivery.Execution.DeliveryID).Result()
	if err != nil {
		observability.MarkSpanError(span, err)
		return fmt.Errorf("ack task: %w", err)
	}
	if acked == 0 {
		observability.MarkSpanError(span, broker.ErrUnknownDelivery)
		return broker.ErrUnknownDelivery
	}
	if err := b.deleteFinalizedEntry(ctx, streamKey, delivery.Execution.DeliveryID); err != nil {
		observability.MarkSpanError(span, err)
		return err
	}
	return nil
}

func (b *RedisBroker) Nack(ctx context.Context, delivery broker.Delivery, requeue bool) error {
	ctx, span := observability.StartQueueSpan(
		ctx,
		"taskforge.brokerredis",
		"taskforge.nack",
		delivery.Message,
		append(deliverySpanAttributes(delivery), attribute.Bool("taskforge.requeue", requeue))...,
	)
	defer span.End()

	pending, ttl, err := b.validatePendingDelivery(ctx, delivery)
	if err != nil {
		b.logDeliveryRejection("nack rejected", delivery, pending, ttl, err)
		observability.MarkSpanError(span, err)
		return err
	}

	if requeue {
		requeued := delivery.Message
		requeued.ETA = nil
		if _, err := b.Publish(ctx, requeued, broker.PublishOptions{
			Source:           broker.PublishSourceNew,
			DeduplicationKey: fmt.Sprintf("requeue:%s", delivery.Execution.DeliveryID),
		}); err != nil {
			observability.MarkSpanError(span, err)
			return err
		}
	}

	queue := tasks.EffectiveQueue(delivery.Message)
	streamKey := b.queueStreamKey(queue, delivery.Message.FairnessKey)
	acked, err := b.client.XAck(ctx, streamKey, b.groupName(queue), delivery.Execution.DeliveryID).Result()
	if err != nil {
		observability.MarkSpanError(span, err)
		return fmt.Errorf("nack task: %w", err)
	}
	if acked == 0 {
		observability.MarkSpanError(span, broker.ErrUnknownDelivery)
		return broker.ErrUnknownDelivery
	}
	if err := b.deleteFinalizedEntry(ctx, streamKey, delivery.Execution.DeliveryID); err != nil {
		observability.MarkSpanError(span, err)
		return err
	}
	return nil
}

func (b *RedisBroker) ExtendLease(ctx context.Context, delivery broker.Delivery, ttl time.Duration) error {
	ctx, span := observability.StartQueueSpan(
		ctx,
		"taskforge.brokerredis",
		"taskforge.extend_lease",
		delivery.Message,
		deliverySpanAttributes(delivery)...,
	)
	defer span.End()

	pending, effectiveTTL, err := b.validatePendingDelivery(ctx, delivery)
	if err != nil {
		b.incrementLeaseExtensionFailure(tasks.EffectiveQueue(delivery.Message))
		b.logDeliveryRejection("lease extension rejected", delivery, pending, effectiveTTL, err)
		observability.MarkSpanError(span, err)
		return err
	}

	queue := tasks.EffectiveQueue(delivery.Message)
	resetIDs, err := b.client.XClaimJustID(ctx, &redis.XClaimArgs{
		Stream:   b.queueStreamKey(queue, delivery.Message.FairnessKey),
		Group:    b.groupName(queue),
		Consumer: delivery.Execution.LeaseOwner,
		MinIdle:  0,
		Messages: []string{delivery.Execution.DeliveryID},
	}).Result()
	if err != nil {
		b.incrementLeaseExtensionFailure(queue)
		observability.MarkSpanError(span, err)
		return fmt.Errorf("extend lease: %w", err)
	}
	if len(resetIDs) == 0 {
		b.incrementLeaseExtensionFailure(queue)
		observability.MarkSpanError(span, broker.ErrUnknownDelivery)
		return broker.ErrUnknownDelivery
	}

	logging.WithDelivery(b.logger, delivery).Debug(
		"extended task lease",
		"lease_expiry", time.Now().UTC().Add(ttl),
	)

	return nil
}

func (b *RedisBroker) MoveDue(ctx context.Context, now time.Time, limit int64) (int, error) {
	if limit <= 0 {
		limit = 100
	}

	values, err := b.client.ZRangeByScore(ctx, b.delayedKey(), &redis.ZRangeBy{
		Min:    "-inf",
		Max:    fmt.Sprintf("%d", now.UTC().UnixMilli()),
		Offset: 0,
		Count:  limit,
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("move due tasks: query delayed set: %w", err)
	}

	moved := 0
	for _, raw := range values {
		entry, err := decodeDelayedEntry(raw)
		if err != nil {
			return moved, fmt.Errorf("move due tasks: decode delayed entry: %w", err)
		}

		msg := entry.Message
		if msg.Headers == nil {
			msg.Headers = map[string]string{}
		}
		scheduledFor := entry.ScheduledFor.UTC()
		msg.Headers[schedulerpkg.HeaderScheduledFor] = scheduledFor.Format(time.RFC3339Nano)
		msg.Headers[schedulerpkg.HeaderReleasedAt] = now.UTC().Format(time.RFC3339Nano)
		msg.Headers[schedulerpkg.HeaderReleaseLagMS] = strconv.FormatInt(now.UTC().Sub(scheduledFor).Milliseconds(), 10)
		msg.ETA = nil
		if _, err := b.publishMessage(ctx, msg, broker.PublishOptions{
			Source:           broker.PublishSourceDueRelease,
			DeduplicationKey: fmt.Sprintf("delayed:%s", entry.EntryID),
		}, now); err != nil {
			return moved, fmt.Errorf("move due tasks: release delayed message: %w", err)
		}

		removed, err := b.client.ZRem(ctx, b.delayedKey(), raw).Result()
		if err != nil {
			return moved, fmt.Errorf("move due tasks: remove delayed entry: %w", err)
		}
		if removed == 1 {
			moved++
		}
	}

	return moved, nil
}

func (b *RedisBroker) reclaimExpiredDelivery(ctx context.Context, queue, streamKey, groupName, consumerName string) (broker.Delivery, bool, error) {
	pendingEntries, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupName,
		Start:  "-",
		End:    "+",
		Count:  reclaimScanCount,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return broker.Delivery{}, false, nil
		}
		return broker.Delivery{}, false, fmt.Errorf("reclaim task: inspect pending deliveries: %w", err)
	}

	for _, pending := range pendingEntries {
		entry, msg, err := b.pendingTask(ctx, streamKey, pending.ID)
		if err != nil {
			if errors.Is(err, broker.ErrUnknownDelivery) {
				continue
			}
			return broker.Delivery{}, false, fmt.Errorf("reclaim task: %w", err)
		}

		ttl := b.effectiveLeaseTTL(msg)
		if ttl <= 0 || pending.Idle < ttl {
			continue
		}

		claimed, err := b.client.XClaim(ctx, &redis.XClaimArgs{
			Stream:   streamKey,
			Group:    groupName,
			Consumer: consumerName,
			MinIdle:  ttl,
			Messages: []string{pending.ID},
		}).Result()
		if err != nil {
			return broker.Delivery{}, false, fmt.Errorf("reclaim task: claim expired delivery: %w", err)
		}
		if len(claimed) == 0 {
			continue
		}

		now := time.Now().UTC()
		delivery := newDelivery(msg, queue, consumerName, entry.ID, now, ttl, deliveryCount(msg, pending.RetryCount+1))
		b.metrics.IncReclaimed(queue)
		reclaimCtx := observability.ExtractTraceContext(ctx, msg.Headers)
		_, span := observability.StartQueueSpan(
			reclaimCtx,
			"taskforge.brokerredis",
			"taskforge.reclaim",
			msg,
			append(
				deliverySpanAttributes(delivery),
				attribute.String("taskforge.previous_owner", pending.Consumer),
			)...,
		)
		span.End()

		logging.WithDelivery(b.logger, delivery).Info(
			"reclaimed expired delivery",
			"previous_owner", pending.Consumer,
			"idle", pending.Idle,
		)

		return delivery, true, nil
	}

	return broker.Delivery{}, false, nil
}

func (b *RedisBroker) pendingTask(ctx context.Context, streamKey, deliveryID string) (redis.XMessage, broker.TaskMessage, error) {
	messages, err := b.client.XRangeN(ctx, streamKey, deliveryID, deliveryID, 1).Result()
	if err != nil {
		return redis.XMessage{}, broker.TaskMessage{}, fmt.Errorf("load pending delivery %s: %w", deliveryID, err)
	}
	if len(messages) == 0 {
		return redis.XMessage{}, broker.TaskMessage{}, broker.ErrUnknownDelivery
	}

	msg, err := decodeTaskMessage(messages[0])
	if err != nil {
		return redis.XMessage{}, broker.TaskMessage{}, fmt.Errorf("decode pending delivery %s: %w", deliveryID, err)
	}
	return messages[0], msg, nil
}

func (b *RedisBroker) publishReady(ctx context.Context, queue string, payload []byte) error {
	if _, err := b.client.XAdd(ctx, &redis.XAddArgs{
		Stream: b.streamKey(queue),
		Values: map[string]interface{}{
			streamPayloadField: string(payload),
		},
	}).Result(); err != nil {
		return fmt.Errorf("publish task: add stream entry: %w", err)
	}
	return nil
}

func (b *RedisBroker) publishReadyWithReceipt(ctx context.Context, queue string, payload []byte, receiptKey string) (bool, error) {
	published, err := publishReadyWithReceiptScript.Run(
		ctx,
		b.client,
		[]string{b.streamKey(queue), receiptKey},
		streamPayloadField,
		string(payload),
		b.publishReceiptTTL().Milliseconds(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("publish task: add stream entry: %w", err)
	}
	return published == 1, nil
}

func (b *RedisBroker) publishMessage(ctx context.Context, msg broker.TaskMessage, opts broker.PublishOptions, now time.Time) (broker.PublishResult, error) {
	opts = opts.Normalize()
	queue := tasks.EffectiveQueue(msg)
	result := broker.PublishResult{
		Decision: broker.AdmissionDecisionAccepted,
		Queue:    queue,
	}

	if opts.DeduplicationKey != "" {
		exists, err := b.publishReceiptExists(ctx, opts.DeduplicationKey)
		if err != nil {
			return broker.PublishResult{}, err
		}
		if exists {
			result.Deduplicated = true
			return result, nil
		}
	}

	eval, err := b.evaluateAdmission(ctx, msg, opts, now)
	if err != nil {
		return broker.PublishResult{}, err
	}
	b.observeAdmissionDecision(queue, opts.Source, eval)

	result.Decision = eval.decision
	result.Reason = eval.reason

	switch eval.decision {
	case broker.AdmissionDecisionRejected:
		return result, &broker.AdmissionError{Queue: queue, Reason: eval.reason}
	case broker.AdmissionDecisionDeferred:
		if eval.deferUntil == nil {
			return broker.PublishResult{}, fmt.Errorf("publish task: deferred admission missing defer deadline")
		}
		msg = b.annotateDeferredMessage(msg, opts.Source, eval.reason, *eval.deferUntil, now)
		result.DeferredUntil = eval.deferUntil
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return broker.PublishResult{}, fmt.Errorf("publish task: marshal message: %w", err)
	}

	if msg.ETA != nil && msg.ETA.After(now) {
		published, err := b.publishDelayed(ctx, msg, opts.DeduplicationKey)
		if err != nil {
			return broker.PublishResult{}, err
		}
		result.Deduplicated = !published
		if published {
			b.metrics.IncPublished(queue)
		}
		return result, nil
	}

	if b.fairnessPolicy(queue) != nil {
		published, err := b.publishFairReady(ctx, msg, payload, opts.DeduplicationKey, now)
		if err != nil {
			return broker.PublishResult{}, err
		}
		result.Deduplicated = !published
		if published {
			b.metrics.IncPublished(queue)
		}
		return result, nil
	}
	published, err := b.publishReadyWithDedup(ctx, queue, payload, opts.DeduplicationKey)
	if err != nil {
		return broker.PublishResult{}, err
	}
	result.Deduplicated = !published
	if published {
		b.metrics.IncPublished(queue)
	}
	return result, nil
}

func (b *RedisBroker) publishReadyWithDedup(ctx context.Context, queue string, payload []byte, deduplicationKey string) (bool, error) {
	if deduplicationKey == "" {
		return true, b.publishReady(ctx, queue, payload)
	}
	return b.publishReadyWithReceipt(ctx, queue, payload, b.publishReceiptKey(deduplicationKey))
}

func (b *RedisBroker) publishDelayed(ctx context.Context, msg broker.TaskMessage, deduplicationKey string) (bool, error) {
	entryPayload, err := json.Marshal(delayedEntry{
		EntryID:      uuid.NewString(),
		ScheduledFor: msg.ETA.UTC(),
		Message:      msg,
	})
	if err != nil {
		return false, fmt.Errorf("publish task: marshal delayed entry: %w", err)
	}
	if deduplicationKey == "" {
		if err := b.client.ZAdd(ctx, b.delayedKey(), redis.Z{
			Score:  float64(msg.ETA.UnixMilli()),
			Member: entryPayload,
		}).Err(); err != nil {
			return false, err
		}
		return true, nil
	}
	published, err := publishDelayedWithReceiptScript.Run(
		ctx,
		b.client,
		[]string{b.delayedKey(), b.publishReceiptKey(deduplicationKey)},
		msg.ETA.UTC().UnixMilli(),
		string(entryPayload),
		b.publishReceiptTTL().Milliseconds(),
	).Int64()
	if err != nil {
		return false, err
	}
	return published == 1, nil
}

func (b *RedisBroker) ensureGroup(ctx context.Context, streamKey, groupName string) error {
	err := b.client.XGroupCreateMkStream(ctx, streamKey, groupName, "0").Err()
	if err == nil {
		return nil
	}
	if strings.HasPrefix(err.Error(), "BUSYGROUP ") {
		return nil
	}
	return fmt.Errorf("ensure consumer group: %w", err)
}

func (b *RedisBroker) validatePendingDelivery(ctx context.Context, delivery broker.Delivery) (redis.XPendingExt, time.Duration, error) {
	if delivery.Execution.DeliveryID == "" {
		return redis.XPendingExt{}, 0, broker.ErrUnknownDelivery
	}

	queue := tasks.EffectiveQueue(delivery.Message)
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
		return redis.XPendingExt{}, 0, broker.ErrUnknownDelivery
	}

	pending := pendingEntries[0]
	ttl := b.effectiveLeaseTTL(delivery.Message)
	if delivery.Execution.LeaseOwner != "" && pending.Consumer != delivery.Execution.LeaseOwner {
		return pending, ttl, broker.ErrStaleDelivery
	}
	if ttl > 0 && pending.Idle >= ttl {
		return pending, ttl, broker.ErrDeliveryExpired
	}

	return pending, ttl, nil
}

func (b *RedisBroker) logDeliveryRejection(message string, delivery broker.Delivery, pending redis.XPendingExt, ttl time.Duration, err error) {
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

func (b *RedisBroker) QueueMetricsSnapshot(ctx context.Context, queue string) (observability.QueueMetricsSnapshot, error) {
	queue = normalizeQueue(queue)
	if b.fairnessPolicy(queue) != nil {
		return b.fairQueueMetricsSnapshot(ctx, queue)
	}
	streamKey := b.streamKey(queue)
	groupName := b.groupName(queue)

	length := int64(0)
	streamInfo, err := b.client.XInfoStream(ctx, streamKey).Result()
	if err != nil {
		if !isMissingStream(err) {
			return observability.QueueMetricsSnapshot{}, fmt.Errorf("queue metrics: stream %q: %w", queue, err)
		}
	} else {
		length = int64(streamInfo.Length)
	}

	pendingCount := int64(0)
	pending, err := b.client.XPending(ctx, streamKey, groupName).Result()
	if err != nil {
		if !isMissingGroup(err) && !isMissingStream(err) {
			return observability.QueueMetricsSnapshot{}, fmt.Errorf("queue metrics: pending %q: %w", queue, err)
		}
	} else {
		pendingCount = pending.Count
	}

	consumerCount := 0
	consumers, err := b.client.XInfoConsumers(ctx, streamKey, groupName).Result()
	if err != nil {
		if !isMissingGroup(err) && !isMissingStream(err) {
			return observability.QueueMetricsSnapshot{}, fmt.Errorf("queue metrics: consumers %q: %w", queue, err)
		}
	} else {
		consumerCount = len(consumers)
	}

	ready := length - pendingCount
	if ready < 0 {
		ready = 0
	}

	return observability.QueueMetricsSnapshot{
		Depth:     float64(ready),
		Reserved:  float64(pendingCount),
		Consumers: float64(consumerCount),
	}, nil
}

func (b *RedisBroker) DeadLetterQueueSize(ctx context.Context, queue string) (float64, error) {
	length, err := b.client.XLen(ctx, dlqStreamKey(queue)).Result()
	if err != nil {
		if isMissingStream(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("dead-letter queue metrics %q: %w", queue, err)
	}
	return float64(length), nil
}

func (b *RedisBroker) SchedulerLag(ctx context.Context, now time.Time, queue string) (float64, error) {
	values, err := b.client.ZRange(ctx, b.delayedKey(), 0, -1).Result()
	if err != nil {
		if isMissingStream(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("scheduler lag metrics %q: %w", queue, err)
	}

	for _, raw := range values {
		entry, err := decodeDelayedEntry(raw)
		if err != nil {
			return 0, fmt.Errorf("scheduler lag metrics %q: decode delayed entry: %w", queue, err)
		}
		if tasks.EffectiveQueue(entry.Message) != normalizeQueue(queue) {
			continue
		}
		lag := now.UTC().Sub(entry.ScheduledFor.UTC())
		if lag < 0 {
			return 0, nil
		}
		return lag.Seconds(), nil
	}

	return 0, nil
}

func (b *RedisBroker) incrementLeaseExtensionFailure(queue string) {
	b.metrics.IncLeaseExtensionFailure(queue)
}

func (b *RedisBroker) deleteFinalizedEntry(ctx context.Context, streamKey, deliveryID string) error {
	deleted, err := b.client.XDel(ctx, streamKey, deliveryID).Result()
	if err != nil {
		return fmt.Errorf("delete finalized task entry: %w", err)
	}
	if deleted == 0 {
		return broker.ErrUnknownDelivery
	}
	return nil
}

func isMissingGroup(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "NOGROUP")
}

func isMissingStream(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key")
}

func (b *RedisBroker) effectiveLeaseTTL(msg broker.TaskMessage) time.Duration {
	if msg.VisibilityTimeout > 0 {
		return msg.VisibilityTimeout
	}
	return b.leaseTTL
}

func (b *RedisBroker) streamKey(queue string) string {
	return fmt.Sprintf("%s:stream:%s", b.prefix, normalizeQueue(queue))
}

func (b *RedisBroker) groupName(queue string) string {
	return fmt.Sprintf("%s:%s", b.prefix, normalizeQueue(queue))
}

func (b *RedisBroker) consumerName(consumerID string) string {
	base := consumerID
	if base == "" {
		base = "worker"
	}
	return fmt.Sprintf("%s:%s:%s", base, b.hostname, b.instanceID)
}

func (b *RedisBroker) delayedKey() string {
	return fmt.Sprintf("%s:delayed", b.prefix)
}

func (b *RedisBroker) publishReceiptKey(deduplicationKey string) string {
	return fmt.Sprintf("%s:publish:receipt:%x", b.prefix, sha256Sum(deduplicationKey))
}

func (b *RedisBroker) publishReceiptTTL() time.Duration {
	return defaultPublishReceiptTTL
}

func (b *RedisBroker) publishReceiptExists(ctx context.Context, deduplicationKey string) (bool, error) {
	exists, err := b.client.Exists(ctx, b.publishReceiptKey(deduplicationKey)).Result()
	if err != nil {
		return false, fmt.Errorf("publish task: inspect receipt: %w", err)
	}
	return exists > 0, nil
}

func normalizeQueue(queue string) string {
	if queue == "" {
		return "default"
	}
	return queue
}

func normalizePublishedMessage(msg broker.TaskMessage, now time.Time) broker.TaskMessage {
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	if msg.Queue == "" {
		msg.Queue = "default"
	}
	msg.FairnessKey = strings.TrimSpace(msg.FairnessKey)
	return msg
}

func decodeTaskMessage(entry redis.XMessage) (broker.TaskMessage, error) {
	raw, ok := entry.Values[streamPayloadField]
	if !ok {
		return broker.TaskMessage{}, fmt.Errorf("missing %q field", streamPayloadField)
	}

	payload, err := messagePayload(raw)
	if err != nil {
		return broker.TaskMessage{}, err
	}

	var msg broker.TaskMessage
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		return broker.TaskMessage{}, fmt.Errorf("unmarshal message: %w", err)
	}
	return msg, nil
}

func messagePayload(raw interface{}) (string, error) {
	switch value := raw.(type) {
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	default:
		return "", fmt.Errorf("unexpected stream payload type %T", raw)
	}
}

type delayedEntry struct {
	EntryID      string             `json:"entry_id"`
	ScheduledFor time.Time          `json:"scheduled_for"`
	Message      broker.TaskMessage `json:"message"`
}

func decodeDelayedEntry(raw string) (delayedEntry, error) {
	var entry delayedEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		return delayedEntry{}, err
	}
	if entry.EntryID == "" {
		return delayedEntry{}, fmt.Errorf("missing delayed entry id")
	}
	return entry, nil
}

func deliveryCount(msg broker.TaskMessage, fallback int64) int {
	count := msg.Attempt + 1
	if fallback > int64(count) {
		count = int(fallback)
	}
	if count < 1 {
		return 1
	}
	return count
}

func newDelivery(msg broker.TaskMessage, queue, consumerID, deliveryID string, now time.Time, ttl time.Duration, count int) broker.Delivery {
	firstEnqueuedAt := msg.CreatedAt
	if firstEnqueuedAt.IsZero() {
		firstEnqueuedAt = now
	}

	return broker.Delivery{
		Message: msg,
		Execution: broker.ExecutionMetadata{
			TaskID:          msg.ID,
			DeliveryID:      deliveryID,
			DeliveryCount:   count,
			FirstEnqueuedAt: firstEnqueuedAt,
			LeasedAt:        now,
			LeaseExpiresAt:  now.Add(ttl),
			LeaseOwner:      consumerID,
			LastError:       messageLastError(msg),
			State:           string(tasks.StateLeased),
		},
	}
}

func messageLastError(msg broker.TaskMessage) string {
	if msg.Headers == nil {
		return ""
	}
	return msg.Headers["last_error"]
}

func deliverySpanAttributes(delivery broker.Delivery) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("taskforge.delivery_id", delivery.Execution.DeliveryID),
		attribute.String("taskforge.worker_identity", delivery.Execution.LeaseOwner),
		attribute.Int("taskforge.delivery_count", delivery.Execution.DeliveryCount),
	}
}

func dlqStreamKey(queue string) string {
	return fmt.Sprintf("%s:stream:dlq.%s", defaultPrefix, normalizeQueue(queue))
}
