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
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/observability"
	schedulerpkg "github.com/aminkbi/taskforge/internal/scheduler"
	"github.com/aminkbi/taskforge/internal/tasks"
)

const (
	defaultPrefix         = "taskforge"
	defaultReserveTimeout = time.Second
	streamPayloadField    = "message"
	reclaimScanCount      = 20
)

type RedisBroker struct {
	client     *redis.Client
	logger     *slog.Logger
	metrics    *observability.Metrics
	leaseTTL   time.Duration
	reserveTTL time.Duration
	prefix     string
	hostname   string
	instanceID string
}

func New(client *redis.Client, logger *slog.Logger, leaseTTL time.Duration, metrics *observability.Metrics) *RedisBroker {
	return NewWithOptions(client, logger, leaseTTL, metrics, Options{})
}

type Options struct {
	ReserveTimeout time.Duration
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
		client:     client,
		logger:     logger,
		metrics:    metrics,
		leaseTTL:   leaseTTL,
		reserveTTL: reserveTimeout,
		prefix:     defaultPrefix,
		hostname:   hostname,
		instanceID: fmt.Sprintf("%d", os.Getpid()),
	}
}

func (b *RedisBroker) Ping(ctx context.Context) error {
	return b.client.Ping(ctx).Err()
}

func (b *RedisBroker) Publish(ctx context.Context, msg broker.TaskMessage) error {
	if msg.ID == "" {
		return fmt.Errorf("publish task: missing id")
	}

	now := time.Now().UTC()
	msg = normalizePublishedMessage(msg, now)

	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("publish task: marshal message: %w", err)
	}

	if msg.ETA != nil && msg.ETA.After(now) {
		entryPayload, err := json.Marshal(delayedEntry{
			EntryID:      uuid.NewString(),
			ScheduledFor: msg.ETA.UTC(),
			Message:      msg,
		})
		if err != nil {
			return fmt.Errorf("publish task: marshal delayed entry: %w", err)
		}
		if err := b.client.ZAdd(ctx, b.delayedKey(), redis.Z{
			Score:  float64(msg.ETA.UnixMilli()),
			Member: entryPayload,
		}).Err(); err != nil {
			return err
		}
		b.metrics.IncPublished(tasks.EffectiveQueue(msg))
		return nil
	}

	if err := b.publishReady(ctx, tasks.EffectiveQueue(msg), payload); err != nil {
		return err
	}
	b.metrics.IncPublished(tasks.EffectiveQueue(msg))
	return nil
}

func (b *RedisBroker) Reserve(ctx context.Context, queue, consumerID string) (broker.Delivery, error) {
	queue = normalizeQueue(queue)
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

	ttl := b.effectiveLeaseTTL(msg)
	now := time.Now().UTC()
	delivery := newDelivery(msg, queue, consumerName, entry.ID, now, ttl, deliveryCount(msg, 0))

	b.logger.Info(
		"reserved task delivery",
		"task_id", delivery.Execution.TaskID,
		"delivery_id", delivery.Execution.DeliveryID,
		"lease_owner", delivery.Execution.LeaseOwner,
		"lease_expires_at", delivery.Execution.LeaseExpiresAt,
	)

	return delivery, nil
}

func (b *RedisBroker) Ack(ctx context.Context, delivery broker.Delivery) error {
	pending, ttl, err := b.validatePendingDelivery(ctx, delivery)
	if err != nil {
		b.logDeliveryRejection("ack rejected", delivery, pending, ttl, err)
		return err
	}

	queue := tasks.EffectiveQueue(delivery.Message)
	acked, err := b.client.XAck(ctx, b.streamKey(queue), b.groupName(queue), delivery.Execution.DeliveryID).Result()
	if err != nil {
		return fmt.Errorf("ack task: %w", err)
	}
	if acked == 0 {
		return broker.ErrUnknownDelivery
	}
	if err := b.deleteFinalizedEntry(ctx, queue, delivery.Execution.DeliveryID); err != nil {
		return err
	}
	return nil
}

func (b *RedisBroker) Nack(ctx context.Context, delivery broker.Delivery, requeue bool) error {
	pending, ttl, err := b.validatePendingDelivery(ctx, delivery)
	if err != nil {
		b.logDeliveryRejection("nack rejected", delivery, pending, ttl, err)
		return err
	}

	if requeue {
		requeued := delivery.Message
		requeued.ETA = nil
		if err := b.Publish(ctx, requeued); err != nil {
			return err
		}
	}

	queue := tasks.EffectiveQueue(delivery.Message)
	acked, err := b.client.XAck(ctx, b.streamKey(queue), b.groupName(queue), delivery.Execution.DeliveryID).Result()
	if err != nil {
		return fmt.Errorf("nack task: %w", err)
	}
	if acked == 0 {
		return broker.ErrUnknownDelivery
	}
	if err := b.deleteFinalizedEntry(ctx, queue, delivery.Execution.DeliveryID); err != nil {
		return err
	}
	return nil
}

func (b *RedisBroker) ExtendLease(ctx context.Context, delivery broker.Delivery, ttl time.Duration) error {
	pending, effectiveTTL, err := b.validatePendingDelivery(ctx, delivery)
	if err != nil {
		b.incrementLeaseExtensionFailure(tasks.EffectiveQueue(delivery.Message))
		b.logDeliveryRejection("lease extension rejected", delivery, pending, effectiveTTL, err)
		return err
	}

	queue := tasks.EffectiveQueue(delivery.Message)
	resetIDs, err := b.client.XClaimJustID(ctx, &redis.XClaimArgs{
		Stream:   b.streamKey(queue),
		Group:    b.groupName(queue),
		Consumer: delivery.Execution.LeaseOwner,
		MinIdle:  0,
		Messages: []string{delivery.Execution.DeliveryID},
	}).Result()
	if err != nil {
		b.incrementLeaseExtensionFailure(queue)
		return fmt.Errorf("extend lease: %w", err)
	}
	if len(resetIDs) == 0 {
		b.incrementLeaseExtensionFailure(queue)
		return broker.ErrUnknownDelivery
	}

	b.logger.Debug(
		"extended task lease",
		"task_id", delivery.Execution.TaskID,
		"delivery_id", delivery.Execution.DeliveryID,
		"lease_owner", delivery.Execution.LeaseOwner,
		"lease_expires_at", time.Now().UTC().Add(ttl),
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
		payload, err := json.Marshal(msg)
		if err != nil {
			return moved, fmt.Errorf("move due tasks: marshal ready message: %w", err)
		}

		pipe := b.client.TxPipeline()
		pipe.ZRem(ctx, b.delayedKey(), raw)
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: b.streamKey(tasks.EffectiveQueue(msg)),
			Values: map[string]interface{}{
				streamPayloadField: string(payload),
			},
		})

		if _, err := pipe.Exec(ctx); err != nil {
			return moved, fmt.Errorf("move due tasks: release delayed message: %w", err)
		}
		moved++
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

		b.logger.Info(
			"reclaimed expired delivery",
			"task_id", delivery.Execution.TaskID,
			"delivery_id", delivery.Execution.DeliveryID,
			"previous_owner", pending.Consumer,
			"lease_owner", delivery.Execution.LeaseOwner,
			"idle", pending.Idle,
			"lease_expires_at", delivery.Execution.LeaseExpiresAt,
			"delivery_count", delivery.Execution.DeliveryCount,
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
		Stream: b.streamKey(queue),
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

	b.logger.Warn(
		message,
		"task_id", delivery.Execution.TaskID,
		"delivery_id", delivery.Execution.DeliveryID,
		"lease_owner", delivery.Execution.LeaseOwner,
		"current_owner", pending.Consumer,
		"lease_expires_at", expiresAt,
		"error", err,
	)
}

func (b *RedisBroker) QueueMetricsSnapshot(ctx context.Context, queue string) (observability.QueueMetricsSnapshot, error) {
	queue = normalizeQueue(queue)
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

func (b *RedisBroker) incrementLeaseExtensionFailure(queue string) {
	b.metrics.IncLeaseExtensionFailure(queue)
}

func (b *RedisBroker) deleteFinalizedEntry(ctx context.Context, queue, deliveryID string) error {
	deleted, err := b.client.XDel(ctx, b.streamKey(queue), deliveryID).Result()
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
