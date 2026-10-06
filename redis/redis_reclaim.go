package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/logging"
	"github.com/aminkbi/taskforge/internal/observability"
)

func (b *Broker) reclaimExpiredDelivery(ctx context.Context, queue, streamKey, groupName, consumerName string) (taskforge.Delivery, bool, error) {
	if reclaimed, ok, err := b.reclaimIndexedDelivery(ctx, queue, streamKey, groupName, consumerName); err != nil {
		return taskforge.Delivery{}, false, err
	} else if ok {
		return reclaimed, true, nil
	}

	covered, err := b.leaseIndexCoversPending(ctx, streamKey, groupName)
	if err != nil {
		return taskforge.Delivery{}, false, err
	}
	if covered && !b.shouldAuditReclaim(streamKey, time.Now()) {
		return taskforge.Delivery{}, false, nil
	}
	exists, err := b.client.Exists(ctx, b.leaseDeadlineKey(streamKey)).Result()
	if err != nil {
		return taskforge.Delivery{}, false, fmt.Errorf("inspect lease index presence: %w", err)
	}
	if exists == 0 {
		b.resetReclaimScan(streamKey)
	}
	return b.reclaimByScan(ctx, queue, streamKey, groupName, consumerName)
}

func (b *Broker) reclaimIndexedDelivery(ctx context.Context, queue, streamKey, groupName, consumerName string) (taskforge.Delivery, bool, error) {
	now, err := b.redisNow(ctx)
	if err != nil {
		return taskforge.Delivery{}, false, err
	}
	ids, err := b.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     b.leaseDeadlineKey(streamKey),
		Start:   "-inf",
		Stop:    strconv.FormatInt(now.UnixMilli(), 10),
		ByScore: true,
		Offset:  0,
		Count:   b.reclaimBatchSize,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || isIndexTypeError(err) {
			return taskforge.Delivery{}, false, nil
		}
		return taskforge.Delivery{}, false, fmt.Errorf("reclaim task: inspect lease deadlines: %w", err)
	}
	for _, id := range ids {
		delivery, claimed, err := b.reclaimPendingID(ctx, queue, streamKey, groupName, consumerName, id)
		if err != nil {
			if !errors.Is(err, taskforge.ErrUnknownDelivery) {
				return taskforge.Delivery{}, false, err
			}
			_ = b.client.ZRem(ctx, b.leaseDeadlineKey(streamKey), id).Err()
			continue
		}
		if claimed {
			return delivery, true, nil
		}
	}
	return taskforge.Delivery{}, false, nil
}

func (b *Broker) reclaimPendingID(ctx context.Context, queue, streamKey, groupName, consumerName, id string) (taskforge.Delivery, bool, error) {
	pendingEntries, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupName,
		Start:  id,
		End:    id,
		Count:  1,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || isMissingGroup(err) || isMissingStream(err) {
			return taskforge.Delivery{}, false, taskforge.ErrUnknownDelivery
		}
		return taskforge.Delivery{}, false, fmt.Errorf("inspect pending delivery %s: %w", id, err)
	}
	if len(pendingEntries) == 0 || pendingEntries[0].ID != id {
		return taskforge.Delivery{}, false, taskforge.ErrUnknownDelivery
	}
	messages, err := b.client.XRangeN(ctx, streamKey, id, id, 1).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || isMissingGroup(err) || isMissingStream(err) {
			return taskforge.Delivery{}, false, taskforge.ErrUnknownDelivery
		}
		return taskforge.Delivery{}, false, fmt.Errorf("load pending delivery %s: %w", id, err)
	}
	if len(messages) == 0 {
		return taskforge.Delivery{}, false, taskforge.ErrUnknownDelivery
	}
	msg, err := decodeTask(messages[0])
	if err != nil {
		return taskforge.Delivery{}, false, fmt.Errorf("decode pending delivery %s: %w", id, err)
	}
	return b.claimPendingDelivery(ctx, queue, streamKey, groupName, consumerName, pendingEntries[0], messages[0], msg)
}

func (b *Broker) reclaimByScan(ctx context.Context, queue, streamKey, groupName, consumerName string) (taskforge.Delivery, bool, error) {
	cursor, ok := b.beginReclaimScan(streamKey)
	if !ok {
		return taskforge.Delivery{}, false, nil
	}
	complete := false
	defer func() {
		b.endReclaimScan(streamKey, cursor, complete)
	}()

	start := cursor
	if cursor != "-" {
		start = nextStreamID(cursor)
	}
	pendingEntries, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupName,
		Start:  start,
		End:    "+",
		Count:  b.reclaimBatchSize,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || isMissingGroup(err) || isMissingStream(err) {
			complete = true
			return taskforge.Delivery{}, false, nil
		}
		complete = true
		return taskforge.Delivery{}, false, fmt.Errorf("reclaim task: inspect pending deliveries: %w", err)
	}
	if len(pendingEntries) == 0 {
		complete = true
		return taskforge.Delivery{}, false, nil
	}
	if int64(len(pendingEntries)) < b.reclaimBatchSize {
		complete = true
	}
	cursor = pendingEntries[len(pendingEntries)-1].ID
	for _, pending := range pendingEntries {
		messages, err := b.client.XRangeN(ctx, streamKey, pending.ID, pending.ID, 1).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || isMissingGroup(err) || isMissingStream(err) {
				_ = b.client.ZRem(ctx, b.leaseDeadlineKey(streamKey), pending.ID).Err()
				continue
			}
			complete = true
			return taskforge.Delivery{}, false, fmt.Errorf("reclaim task: load pending delivery %s: %w", pending.ID, err)
		}
		if len(messages) == 0 {
			_ = b.client.ZRem(ctx, b.leaseDeadlineKey(streamKey), pending.ID).Err()
			continue
		}
		msg, err := decodeTask(messages[0])
		if err != nil {
			complete = true
			return taskforge.Delivery{}, false, fmt.Errorf("reclaim task: decode pending delivery %s: %w", pending.ID, err)
		}
		delivery, claimed, err := b.claimPendingDelivery(ctx, queue, streamKey, groupName, consumerName, pending, messages[0], msg)
		if err != nil {
			complete = true
			return taskforge.Delivery{}, false, err
		}
		if claimed {
			cursor = pending.ID
			return delivery, true, nil
		}
	}
	return taskforge.Delivery{}, false, nil
}

func (b *Broker) reclaimAuditInterval() time.Duration {
	interval := b.leaseTTL / 4
	if interval < time.Second {
		interval = time.Second
	}
	if interval > 5*time.Second {
		interval = 5 * time.Second
	}
	return interval
}

func (b *Broker) shouldAuditReclaim(streamKey string, now time.Time) bool {
	b.reclaimMu.Lock()
	defer b.reclaimMu.Unlock()
	next, scheduled := b.reclaimAuditNext[streamKey]
	if !scheduled {
		b.reclaimAuditNext[streamKey] = now.Add(b.reclaimAuditInterval())
		return false
	}
	if now.Before(next) {
		return false
	}
	b.reclaimAuditNext[streamKey] = now.Add(b.reclaimAuditInterval())
	return true
}

func (b *Broker) resetReclaimScan(streamKey string) {
	b.reclaimMu.Lock()
	delete(b.reclaimCursors, streamKey)
	delete(b.reclaimScanSteps, streamKey)
	b.reclaimAuditNext[streamKey] = time.Time{}
	b.reclaimMu.Unlock()
}

func (b *Broker) beginReclaimScan(streamKey string) (string, bool) {
	b.reclaimMu.Lock()
	defer b.reclaimMu.Unlock()
	if b.reclaimScanning[streamKey] {
		return "", false
	}
	if b.reclaimScanSteps[streamKey] >= maxReclaimScanSteps {
		delete(b.reclaimCursors, streamKey)
		b.reclaimScanSteps[streamKey] = 0
	}
	b.reclaimScanning[streamKey] = true
	b.reclaimScanSteps[streamKey]++
	cursor := b.reclaimCursors[streamKey]
	if cursor == "" {
		return "-", true
	}
	return cursor, true
}

func (b *Broker) endReclaimScan(streamKey, cursor string, complete bool) {
	b.reclaimMu.Lock()
	delete(b.reclaimScanning, streamKey)
	if complete {
		delete(b.reclaimCursors, streamKey)
		delete(b.reclaimScanSteps, streamKey)
	} else if cursor != "" && cursor != "-" {
		b.reclaimCursors[streamKey] = cursor
	}
	b.reclaimMu.Unlock()
}

func (b *Broker) claimPendingDelivery(ctx context.Context, queue, streamKey, groupName, consumerName string, pending redis.XPendingExt, entry redis.XMessage, msg taskforge.Task) (taskforge.Delivery, bool, error) {
	ttl := b.effectiveLeaseTTL(msg)
	if ttl <= 0 {
		return taskforge.Delivery{}, false, nil
	}
	now, err := b.redisNow(ctx)
	if err != nil {
		return taskforge.Delivery{}, false, err
	}
	deadline := now.Add(ttl - pending.Idle)
	b.updateLeaseDeadline(ctx, streamKey, pending.ID, deadline)
	if pending.Idle < ttl {
		return taskforge.Delivery{}, false, nil
	}

	claimed, err := b.client.XClaimJustID(ctx, &redis.XClaimArgs{
		Stream:   streamKey,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  ttl,
		Messages: []string{pending.ID},
	}).Result()
	if err != nil {
		return taskforge.Delivery{}, false, fmt.Errorf("reclaim task: claim expired delivery %s: %w", pending.ID, err)
	}
	if len(claimed) == 0 {
		_ = b.client.ZRem(ctx, b.leaseDeadlineKey(streamKey), pending.ID).Err()
		return taskforge.Delivery{}, false, nil
	}

	b.updateLeaseDeadline(ctx, streamKey, pending.ID, now.Add(ttl))
	delivery := newDelivery(msg, queue, consumerName, entry.ID, now, ttl, deliveryCount(msg, pending.RetryCount+1))
	b.metrics.IncReclaimed(queue)
	reclaimCtx := observability.ExtractTraceContext(ctx, msg.Headers)
	_, span := observability.StartQueueSpan(
		reclaimCtx,
		"taskforge.redis",
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

func (b *Broker) updateLeaseDeadline(ctx context.Context, streamKey, deliveryID string, deadline time.Time) {
	if err := b.client.ZAdd(ctx, b.leaseDeadlineKey(streamKey), redis.Z{
		Score:  float64(deadline.UnixMilli()),
		Member: deliveryID,
	}).Err(); err != nil {
		b.logger.Debug("update lease deadline index failed", "stream", streamKey, "delivery_id", deliveryID, "error", err)
	}
}
