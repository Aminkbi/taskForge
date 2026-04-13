package brokerredis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/tasks"
)

const (
	defaultPrefix         = "taskforge"
	defaultReserveTimeout = time.Second
	streamPayloadField    = "message"
)

type RedisBroker struct {
	client     *redis.Client
	logger     *slog.Logger
	leaseTTL   time.Duration
	prefix     string
	hostname   string
	instanceID string
}

func New(client *redis.Client, logger *slog.Logger, leaseTTL time.Duration) *RedisBroker {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}

	return &RedisBroker{
		client:     client,
		logger:     logger,
		leaseTTL:   leaseTTL,
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
		return b.client.ZAdd(ctx, b.delayedKey(), redis.Z{
			Score:  float64(msg.ETA.UnixMilli()),
			Member: payload,
		}).Err()
	}

	return b.publishReady(ctx, tasks.EffectiveQueue(msg), payload)
}

func (b *RedisBroker) Reserve(ctx context.Context, queue, consumerID string) (broker.Delivery, error) {
	queue = normalizeQueue(queue)
	streamKey := b.streamKey(queue)
	groupName := b.groupName(queue)
	consumerName := b.consumerName(consumerID)

	if err := b.ensureGroup(ctx, streamKey, groupName); err != nil {
		return broker.Delivery{}, err
	}

	streams, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamKey, ">"},
		Count:    1,
		Block:    defaultReserveTimeout,
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

	ttl := msg.VisibilityTimeout
	if ttl <= 0 {
		ttl = b.leaseTTL
	}

	return newDelivery(msg, queue, consumerName, entry.ID, time.Now().UTC(), ttl), nil
}

func (b *RedisBroker) Ack(ctx context.Context, delivery broker.Delivery) error {
	if err := b.validatePendingDelivery(ctx, delivery); err != nil {
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
	return nil
}

func (b *RedisBroker) Nack(ctx context.Context, delivery broker.Delivery, requeue bool) error {
	if err := b.validatePendingDelivery(ctx, delivery); err != nil {
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
	return nil
}

func (b *RedisBroker) ExtendLease(context.Context, broker.Delivery, time.Duration) error {
	// Streams reserve/ack ownership is durable in this phase, but reclaim and
	// lease-renew semantics are deferred to the next roadmap step.
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
		var msg broker.TaskMessage
		if err := json.Unmarshal([]byte(raw), &msg); err != nil {
			return moved, fmt.Errorf("move due tasks: unmarshal delayed message: %w", err)
		}

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

func (b *RedisBroker) validatePendingDelivery(ctx context.Context, delivery broker.Delivery) error {
	if delivery.Execution.DeliveryID == "" {
		return broker.ErrUnknownDelivery
	}

	queue := tasks.EffectiveQueue(delivery.Message)
	pending, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: b.streamKey(queue),
		Group:  b.groupName(queue),
		Start:  delivery.Execution.DeliveryID,
		End:    delivery.Execution.DeliveryID,
		Count:  1,
	}).Result()
	if err != nil {
		return fmt.Errorf("inspect pending delivery: %w", err)
	}
	if len(pending) == 0 || pending[0].ID != delivery.Execution.DeliveryID {
		return broker.ErrUnknownDelivery
	}
	if delivery.Execution.LeaseOwner != "" && pending[0].Consumer != delivery.Execution.LeaseOwner {
		return broker.ErrStaleDelivery
	}

	return nil
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

func newDelivery(msg broker.TaskMessage, queue, consumerID, deliveryID string, now time.Time, ttl time.Duration) broker.Delivery {
	firstEnqueuedAt := msg.CreatedAt
	if firstEnqueuedAt.IsZero() {
		firstEnqueuedAt = now
	}

	deliveryCount := msg.Attempt + 1
	if deliveryCount < 1 {
		deliveryCount = 1
	}

	return broker.Delivery{
		Message: msg,
		Execution: broker.ExecutionMetadata{
			TaskID:          msg.ID,
			DeliveryID:      deliveryID,
			DeliveryCount:   deliveryCount,
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
