package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/logging"
	"github.com/aminkbi/taskforge/internal/observability"
)

func (b *Broker) Reserve(ctx context.Context, queue, consumerID string) (taskforge.Delivery, error) {
	if err := b.checkConfig(); err != nil {
		return taskforge.Delivery{}, err
	}
	started := time.Now()
	defer func(queue string) {
		b.metrics.ObserveReserveLatency(normalizeQueue(queue), time.Since(started).Seconds())
	}(queue)

	queue = normalizeQueue(queue)
	if b.fairnessPolicy(queue) != nil {
		return b.reserveFair(ctx, queue, consumerID)
	}
	deliveries, err := b.reserveFIFO(ctx, queue, consumerID, 1)
	if err != nil {
		return taskforge.Delivery{}, err
	}
	return deliveries[0], nil
}

// ReserveBatch reserves up to max deliveries with one blocking stream read.
// Fairness queues deliberately retain single-candidate selection because a
// batch from one tenant stream would bypass weighted tier selection.
func (b *Broker) ReserveBatch(ctx context.Context, queue, consumerID string, max int) ([]taskforge.Delivery, error) {
	if err := b.checkConfig(); err != nil {
		return nil, err
	}
	started := time.Now()
	defer func(queue string) {
		b.metrics.ObserveReserveLatency(normalizeQueue(queue), time.Since(started).Seconds())
	}(queue)

	queue = normalizeQueue(queue)
	if max < 1 {
		max = 1
	}
	if b.fairnessPolicy(queue) != nil {
		delivery, err := b.reserveFair(ctx, queue, consumerID)
		if err != nil {
			return nil, err
		}
		return []taskforge.Delivery{delivery}, nil
	}
	return b.reserveFIFO(ctx, queue, consumerID, max)
}

func (b *Broker) reserveFIFO(ctx context.Context, queue, consumerID string, max int) ([]taskforge.Delivery, error) {
	streamKey := b.streamKey(queue)
	groupName := b.groupName(queue)
	consumerName := b.consumerName(consumerID)

	if err := b.ensureGroup(ctx, streamKey, groupName); err != nil {
		return nil, err
	}

	if b.shouldCheckReclaim(queue) {
		if reclaimed, ok, err := b.reclaimExpiredDelivery(ctx, queue, streamKey, groupName, consumerName); err != nil {
			return nil, err
		} else if ok {
			return []taskforge.Delivery{reclaimed}, nil
		}
	}

	args := &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamKey, ">"},
		Count:    int64(max),
		Block:    b.reserveTTL,
	}
	streams, err := b.readGroup(ctx, streamKey, groupName, args)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, taskforge.ErrNoTask
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("reserve task: %w", err)
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return nil, taskforge.ErrNoTask
	}

	now := time.Now().UTC()
	deliveries := make([]taskforge.Delivery, 0, len(streams[0].Messages))
	for _, entry := range streams[0].Messages {
		msg, err := decodeTask(entry)
		if err != nil {
			return nil, fmt.Errorf("reserve task: %w", err)
		}
		if msg.Queue == "" {
			msg.Queue = queue
		}

		spanCtx := observability.ExtractTraceContext(ctx, msg.Headers)
		ttl := b.effectiveLeaseTTL(msg)
		b.noteLeaseTTL(queue, ttl)
		delivery := newDelivery(msg, queue, consumerName, entry.ID, now, ttl, deliveryCount(msg, 0))
		_, span := observability.StartQueueSpan(
			spanCtx,
			"taskforge.redis",
			"taskforge.reserve",
			msg,
			deliverySpanAttributes(delivery)...,
		)
		span.End()

		logDeliveryReservation(ctx, b.logger, delivery)
		deliveries = append(deliveries, delivery)
	}
	b.recordLeaseDeadlines(ctx, streamKey, deliveries)

	return deliveries, nil
}

func (b *Broker) shouldCheckReclaim(queue string) bool {
	now := time.Now()
	b.reclaimMu.Lock()
	defer b.reclaimMu.Unlock()
	if now.Before(b.reclaimNext[queue]) {
		return false
	}
	interval := b.reclaimIntervals[queue]
	if interval <= 0 {
		interval = min(b.leaseTTL/4, 100*time.Millisecond)
	}
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	b.reclaimNext[queue] = now.Add(interval)
	return true
}

func (b *Broker) noteLeaseTTL(queue string, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	interval := min(ttl/4, 100*time.Millisecond)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	b.reclaimMu.Lock()
	if current := b.reclaimIntervals[queue]; current <= 0 || interval < current {
		b.reclaimIntervals[queue] = interval
	}
	b.reclaimMu.Unlock()
}

func (b *Broker) recordLeaseDeadlines(ctx context.Context, streamKey string, deliveries []taskforge.Delivery) {
	if len(deliveries) == 0 {
		return
	}
	now, err := b.redisNow(ctx)
	if err != nil {
		b.logger.Debug("record lease deadline index time failed", "stream", streamKey, "error", err)
		return
	}
	pipe := b.client.Pipeline()
	key := b.leaseDeadlineKey(streamKey)
	for _, delivery := range deliveries {
		deadline := now.Add(b.effectiveLeaseTTL(delivery.Message))
		pipe.ZAdd(ctx, key, redis.Z{Score: float64(deadline.UnixMilli()), Member: delivery.Execution.DeliveryID})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		b.logger.Debug("record lease deadline index failed", "stream", streamKey, "error", err)
	}
}

func logDeliveryReservation(ctx context.Context, logger *slog.Logger, delivery taskforge.Delivery) {
	if logger == nil || !logger.Enabled(ctx, slog.LevelDebug) {
		return
	}
	logging.WithDelivery(logger, delivery).Debug("reserved task delivery")
}

func (b *Broker) leaseDeadlineKey(streamKey string) string {
	return streamKey + ":lease-deadlines"
}

func (b *Broker) leaseIndexCoversPending(ctx context.Context, streamKey, groupName string) (bool, error) {
	pending, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupName,
		Start:  "-",
		End:    "+",
		Count:  maxReclaimIndexAuditEntries,
	}).Result()
	if err != nil {
		if isMissingGroup(err) || isMissingStream(err) {
			return true, nil
		}
		return false, fmt.Errorf("inspect pending lease index coverage: %w", err)
	}
	if len(pending) == 0 {
		return true, nil
	}
	if int64(len(pending)) >= maxReclaimIndexAuditEntries {
		return false, nil
	}

	members := make([]string, len(pending))
	for index, entry := range pending {
		members[index] = entry.ID
	}
	scores, err := b.client.ZMScore(ctx, b.leaseDeadlineKey(streamKey), members...).Result()
	if err != nil {
		if isMissingStream(err) || isIndexTypeError(err) {
			return false, nil
		}
		return false, fmt.Errorf("inspect lease index members: %w", err)
	}
	if len(scores) != len(pending) {
		return false, nil
	}
	for _, score := range scores {
		// ZMScore decodes missing members as zero; lease deadlines are positive
		// Unix-millisecond timestamps.
		if score <= 0 {
			return false, nil
		}
	}
	return true, nil
}

func (b *Broker) ensureGroup(ctx context.Context, streamKey, groupName string) error {
	cacheKey := streamKey + "\x00" + groupName
	b.consumerGroupsMu.RLock()
	_, ok := b.consumerGroups[cacheKey]
	b.consumerGroupsMu.RUnlock()
	if ok {
		return nil
	}

	b.consumerGroupsMu.Lock()
	defer b.consumerGroupsMu.Unlock()
	if _, ok := b.consumerGroups[cacheKey]; ok {
		return nil
	}

	err := b.client.XGroupCreateMkStream(ctx, streamKey, groupName, "0").Err()
	if err == nil || strings.HasPrefix(err.Error(), "BUSYGROUP ") {
		b.consumerGroups[cacheKey] = struct{}{}
		return nil
	}
	return fmt.Errorf("ensure consumer group: %w", err)
}

func (b *Broker) forgetGroup(streamKey, groupName string) {
	b.consumerGroupsMu.Lock()
	delete(b.consumerGroups, streamKey+"\x00"+groupName)
	b.consumerGroupsMu.Unlock()
	b.reclaimMu.Lock()
	delete(b.reclaimCursors, streamKey)
	delete(b.reclaimScanning, streamKey)
	delete(b.reclaimScanSteps, streamKey)
	delete(b.reclaimAuditNext, streamKey)
	b.reclaimMu.Unlock()
}

func (b *Broker) readGroup(ctx context.Context, streamKey, groupName string, args *redis.XReadGroupArgs) ([]redis.XStream, error) {
	streams, err := b.client.XReadGroup(ctx, args).Result()
	if !isMissingGroup(err) {
		return streams, err
	}

	// Redis may be reset independently of a long-lived broker. Invalidate the
	// positive cache and recreate the group before retrying once.
	b.forgetGroup(streamKey, groupName)
	if ensureErr := b.ensureGroup(ctx, streamKey, groupName); ensureErr != nil {
		return nil, ensureErr
	}
	return b.client.XReadGroup(ctx, args).Result()
}
