package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge"
)

type queueDepth struct {
	length       int64
	pendingCount int64
	consumers    int
}

// loadQueueDepth reads a non-fair queue's depth state in one pipelined round
// trip; consumers are only enumerated when requested.
func (b *Broker) loadQueueDepth(ctx context.Context, queue string, includeConsumers bool) (queueDepth, error) {
	streamKey := b.streamKey(queue)
	groupName := b.groupName(queue)

	pipe := b.client.Pipeline()
	lengthCmd := pipe.XLen(ctx, streamKey)
	pendingCmd := pipe.XPending(ctx, streamKey, groupName)
	consumersCmd := (*redis.XInfoConsumersCmd)(nil)
	if includeConsumers {
		consumersCmd = pipe.XInfoConsumers(ctx, streamKey, groupName)
	}
	_, _ = pipe.Exec(ctx)

	depth := queueDepth{}
	length, err := lengthCmd.Result()
	if err != nil {
		if !isMissingStream(err) {
			return queueDepth{}, fmt.Errorf("queue metrics: stream %q: %w", queue, err)
		}
	} else {
		depth.length = length
	}

	pending, err := pendingCmd.Result()
	if err != nil {
		if !isMissingGroup(err) && !isMissingStream(err) {
			return queueDepth{}, fmt.Errorf("queue metrics: pending %q: %w", queue, err)
		}
	} else {
		depth.pendingCount = pending.Count
	}

	if includeConsumers {
		consumers, err := consumersCmd.Result()
		if err != nil {
			if !isMissingGroup(err) && !isMissingStream(err) {
				return queueDepth{}, fmt.Errorf("queue metrics: consumers %q: %w", queue, err)
			}
		} else {
			depth.consumers = len(consumers)
		}
	}
	return depth, nil
}

func (b *Broker) QueueMetricsSnapshot(ctx context.Context, queue string) (taskforge.QueueMetricsSnapshot, error) {
	queue = normalizeQueue(queue)
	if b.fairnessPolicy(queue) != nil {
		return b.fairQueueMetricsSnapshot(ctx, queue)
	}

	depth, err := b.loadQueueDepth(ctx, queue, true)
	if err != nil {
		return taskforge.QueueMetricsSnapshot{}, err
	}

	ready := depth.length - depth.pendingCount
	if ready < 0 {
		ready = 0
	}

	return taskforge.QueueMetricsSnapshot{
		Depth:     float64(ready),
		Reserved:  float64(depth.pendingCount),
		Consumers: float64(depth.consumers),
	}, nil
}

func (b *Broker) DeadLetterQueueSize(ctx context.Context, queue string) (float64, error) {
	length, err := b.deadLetterQueueSizeInt(ctx, queue)
	if err != nil {
		return 0, err
	}
	return float64(length), nil
}

func (b *Broker) SchedulerLag(ctx context.Context, now time.Time, queue string) (float64, error) {
	queue = normalizeQueue(queue)
	values, err := b.client.ZRange(ctx, b.delayedQueueKey(queue), 0, 0).Result()
	if err != nil {
		if isMissingStream(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("scheduler lag metrics %q: %w", queue, err)
	}

	if len(values) == 0 {
		return 0, nil
	}
	entry, err := decodeDelayedEntry(values[0])
	if err != nil {
		return 0, fmt.Errorf("scheduler lag metrics %q: decode delayed entry: %w", queue, err)
	}
	lag := now.UTC().Sub(entry.ScheduledFor.UTC())
	if lag < 0 {
		return 0, nil
	}
	return lag.Seconds(), nil
}

func (b *Broker) incrementLeaseExtensionFailure(queue string) {
	b.metrics.IncLeaseExtensionFailure(queue)
}

func isIndexTypeError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "WRONGTYPE")
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

func (b *Broker) oldestReadyAge(ctx context.Context, streamKey, groupName string, now time.Time) time.Duration {
	firstReady, ok, err := b.oldestReadyMessage(ctx, streamKey, groupName)
	if err != nil || !ok {
		return 0
	}

	msg, err := decodeTask(firstReady)
	if err != nil || msg.CreatedAt.IsZero() {
		return 0
	}
	age := now.UTC().Sub(msg.CreatedAt.UTC())
	if age < 0 {
		return 0
	}
	return age
}

func (b *Broker) oldestReadyMessage(ctx context.Context, streamKey, groupName string) (redis.XMessage, bool, error) {
	if groupName == "" {
		return b.firstStreamMessage(ctx, streamKey)
	}

	pending, err := b.client.XPending(ctx, streamKey, groupName).Result()
	switch {
	case err == nil && pending.Count == 0:
		return b.firstStreamMessage(ctx, streamKey)
	case err == nil:
	case isMissingGroup(err):
		return b.firstStreamMessage(ctx, streamKey)
	case isMissingStream(err):
		return redis.XMessage{}, false, nil
	default:
		return redis.XMessage{}, false, fmt.Errorf("oldest ready message: inspect pending: %w", err)
	}

	streamCursor := "-"
	pendingCursor := "-"
	var streamBatch []redis.XMessage
	var pendingBatch []redis.XPendingExt
	streamIndex := 0
	pendingIndex := 0

	for {
		if streamIndex >= len(streamBatch) {
			streamBatch, err = b.loadStreamBatch(ctx, streamKey, streamCursor)
			if err != nil {
				return redis.XMessage{}, false, err
			}
			streamIndex = 0
			if len(streamBatch) == 0 {
				return redis.XMessage{}, false, nil
			}
			streamCursor = streamBatch[len(streamBatch)-1].ID
		}

		if pendingIndex >= len(pendingBatch) {
			pendingBatch, err = b.loadPendingBatch(ctx, streamKey, groupName, pendingCursor)
			if err != nil {
				return redis.XMessage{}, false, err
			}
			pendingIndex = 0
			if len(pendingBatch) > 0 {
				pendingCursor = pendingBatch[len(pendingBatch)-1].ID
			}
		}

		streamEntry := streamBatch[streamIndex]
		if pendingIndex >= len(pendingBatch) {
			return streamEntry, true, nil
		}

		switch compareStreamIDs(streamEntry.ID, pendingBatch[pendingIndex].ID) {
		case -1:
			return streamEntry, true, nil
		case 0:
			streamIndex++
			pendingIndex++
		default:
			pendingIndex++
		}
	}
}

func (b *Broker) firstStreamMessage(ctx context.Context, streamKey string) (redis.XMessage, bool, error) {
	messages, err := b.client.XRangeN(ctx, streamKey, "-", "+", 1).Result()
	if err != nil {
		if isMissingStream(err) {
			return redis.XMessage{}, false, nil
		}
		return redis.XMessage{}, false, fmt.Errorf("oldest ready message: load first stream entry: %w", err)
	}
	if len(messages) == 0 {
		return redis.XMessage{}, false, nil
	}
	return messages[0], true, nil
}

func (b *Broker) loadStreamBatch(ctx context.Context, streamKey, cursor string) ([]redis.XMessage, error) {
	messages, err := b.client.XRangeN(ctx, streamKey, cursor, "+", oldestReadyScanCount).Result()
	if err != nil {
		if isMissingStream(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("oldest ready message: load stream batch: %w", err)
	}
	if cursor != "-" && len(messages) > 0 && messages[0].ID == cursor {
		messages = messages[1:]
	}
	return messages, nil
}

func (b *Broker) loadPendingBatch(ctx context.Context, streamKey, groupName, cursor string) ([]redis.XPendingExt, error) {
	start := cursor
	if cursor != "-" {
		start = nextStreamID(cursor)
	}
	pendingEntries, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupName,
		Start:  start,
		End:    "+",
		Count:  oldestReadyScanCount,
	}).Result()
	if err != nil {
		if isMissingGroup(err) || isMissingStream(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("oldest ready message: load pending batch: %w", err)
	}
	return pendingEntries, nil
}

func compareStreamIDs(left, right string) int {
	leftMillis, leftSeq, leftOK := parseStreamID(left)
	rightMillis, rightSeq, rightOK := parseStreamID(right)
	if !leftOK || !rightOK {
		return compareStrings(left, right)
	}
	switch {
	case leftMillis < rightMillis:
		return -1
	case leftMillis > rightMillis:
		return 1
	case leftSeq < rightSeq:
		return -1
	case leftSeq > rightSeq:
		return 1
	default:
		return 0
	}
}

func parseStreamID(value string) (int64, int64, bool) {
	parts := strings.SplitN(value, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	millis, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return millis, seq, true
}

func nextStreamID(value string) string {
	millis, seq, ok := parseStreamID(value)
	if !ok {
		return value
	}
	return fmt.Sprintf("%d-%d", millis, seq+1)
}

func (b *Broker) redisNow(ctx context.Context) (time.Time, error) {
	now, err := b.client.Time(ctx).Result()
	if err != nil {
		return time.Time{}, fmt.Errorf("redis time: %w", err)
	}
	return now.UTC(), nil
}
