package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
	"uuid"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge"
)

func (b *Broker) MoveDue(ctx context.Context, fence taskforge.LeadershipFence, now time.Time, limit int64) (int, error) {
	if limit <= 0 {
		limit = 100
	}

	queues, err := b.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     b.delayedQueueIndexKey(),
		Start:   "-inf",
		Stop:    fmt.Sprintf("%d", now.UTC().UnixMilli()),
		ByScore: true,
		Offset:  0,
		Count:   limit,
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("move due tasks: query delayed queue index: %w", err)
	}

	moved := 0
	for _, queue := range queues {
		remaining := limit - int64(moved)
		if remaining <= 0 {
			break
		}
		values, err := b.client.ZRangeArgs(ctx, redis.ZRangeArgs{
			Key:     b.delayedQueueKey(queue),
			Start:   "-inf",
			Stop:    fmt.Sprintf("%d", now.UTC().UnixMilli()),
			ByScore: true,
			Offset:  0,
			Count:   remaining,
		}).Result()
		if err != nil {
			return moved, fmt.Errorf("move due tasks: query delayed queue %q: %w", queue, err)
		}
		if len(values) == 0 {
			if err := b.refreshDelayedQueueIndex(ctx, queue); err != nil {
				return moved, err
			}
			continue
		}
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
			msg.Headers[taskforge.HeaderScheduledFor] = scheduledFor.Format(time.RFC3339Nano)
			msg.Headers[taskforge.HeaderReleasedAt] = now.UTC().Format(time.RFC3339Nano)
			msg.Headers[taskforge.HeaderReleaseLagMS] = strconv.FormatInt(now.UTC().Sub(scheduledFor).Milliseconds(), 10)
			msg.ETA = nil
			if err := b.releaseDueEntry(ctx, fence, raw, entry, msg, now); err != nil {
				return moved, fmt.Errorf("move due tasks: release delayed message: %w", err)
			}
			moved++
		}
	}

	return moved, nil
}

func (b *Broker) releaseDueEntry(
	ctx context.Context,
	fence taskforge.LeadershipFence,
	raw string,
	entry delayedEntry,
	msg taskforge.Task,
	now time.Time,
) error {
	opts := taskforge.PublishOptions{
		Source:           taskforge.PublishSourceDueRelease,
		DeduplicationKey: fmt.Sprintf("delayed:%s", entry.EntryID),
	}
	eval, err := b.evaluateAdmission(ctx, msg, opts, now)
	if err != nil {
		return err
	}
	queue := taskforge.EffectiveQueue(msg)
	b.observeAdmissionDecision(queue, opts.Source, eval)
	if eval.decision == taskforge.AdmissionDecisionDeferred {
		if eval.deferUntil == nil {
			return fmt.Errorf("publish task: deferred admission missing defer deadline")
		}
		msg = b.annotateDeferredMessage(msg, opts.Source, eval.reason, *eval.deferUntil, now)
		return b.releaseDueIntoDelayed(ctx, fence, raw, msg, opts.DeduplicationKey)
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("publish task: marshal message: %w", err)
	}
	if b.fairnessPolicy(queue) != nil {
		return b.releaseDueIntoFairReady(ctx, fence, raw, entry.EntryID, queue, msg.FairnessKey, payload, opts.DeduplicationKey, now)
	}
	return b.releaseDueIntoReady(ctx, fence, raw, entry.EntryID, queue, payload, opts.DeduplicationKey)
}

func (b *Broker) releaseDueIntoReady(ctx context.Context, fence taskforge.LeadershipFence, raw, entryID, queue string, payload []byte, deduplicationKey string) error {
	values, err := fencedReleaseReadyScript.Run(
		ctx,
		b.client,
		[]string{
			b.schedulerLeadershipKey(),
			b.delayedQueueKey(queue),
			b.streamKey(queue),
			b.publishReceiptKey(deduplicationKey),
			b.delayedQueueIndexKey(),
			b.delayedRetryIndexKey(queue),
		},
		fence.Token,
		raw,
		streamPayloadField,
		string(payload),
		b.publishReceiptTTL().Milliseconds(),
		queue,
		entryID,
	).Result()
	if err != nil {
		return err
	}
	published, _, err := parseReleaseScriptResult(values, "move_due")
	if err != nil {
		return err
	}
	if published {
		b.metrics.IncPublished(queue)
	}
	return nil
}

func (b *Broker) releaseDueIntoFairReady(ctx context.Context, fence taskforge.LeadershipFence, raw, entryID, queue, fairnessKey string, payload []byte, deduplicationKey string, now time.Time) error {
	values, err := fencedReleaseFairReadyScript.Run(
		ctx,
		b.client,
		[]string{
			b.schedulerLeadershipKey(),
			b.delayedQueueKey(queue),
			b.fairnessKeysSetKey(queue),
			b.fairnessStreamKey(queue, NormalizeFairnessKey(fairnessKey)),
			b.fairnessNotifyKey(queue),
			b.publishReceiptKey(deduplicationKey),
			b.delayedQueueIndexKey(),
			b.delayedRetryIndexKey(queue),
		},
		fence.Token,
		raw,
		NormalizeFairnessKey(fairnessKey),
		streamPayloadField,
		string(payload),
		now.UTC().Format(time.RFC3339Nano),
		b.publishReceiptTTL().Milliseconds(),
		queue,
		entryID,
	).Result()
	if err != nil {
		return err
	}
	published, _, err := parseReleaseScriptResult(values, "move_due")
	if err != nil {
		return err
	}
	if published {
		b.metrics.IncPublished(queue)
	}
	return nil
}

func (b *Broker) releaseDueIntoDelayed(ctx context.Context, fence taskforge.LeadershipFence, raw string, msg taskforge.Task, deduplicationKey string) error {
	oldEntry, err := decodeDelayedEntry(raw)
	if err != nil {
		return fmt.Errorf("publish task: decode delayed entry: %w", err)
	}
	newEntryID := uuid.New().String()
	entryPayload, err := json.Marshal(delayedEntry{
		EntryID:      newEntryID,
		ScheduledFor: msg.ETA.UTC(),
		Message:      msg,
	})
	if err != nil {
		return fmt.Errorf("publish task: marshal delayed entry: %w", err)
	}
	values, err := fencedReleaseDelayedScript.Run(
		ctx,
		b.client,
		[]string{
			b.schedulerLeadershipKey(),
			b.delayedQueueKey(taskforge.EffectiveQueue(oldEntry.Message)),
			b.delayedQueueKey(taskforge.EffectiveQueue(msg)),
			b.publishReceiptKey(deduplicationKey),
			b.delayedQueueIndexKey(),
			b.delayedRetryIndexKey(taskforge.EffectiveQueue(oldEntry.Message)),
			b.delayedRetryIndexKey(taskforge.EffectiveQueue(msg)),
		},
		fence.Token,
		raw,
		msg.ETA.UTC().UnixMilli(),
		string(entryPayload),
		b.publishReceiptTTL().Milliseconds(),
		taskforge.EffectiveQueue(oldEntry.Message),
		taskforge.EffectiveQueue(msg),
		oldEntry.EntryID,
		newEntryID,
		retryIndexFlag(msg),
	).Result()
	if err != nil {
		return err
	}
	published, _, err := parseReleaseScriptResult(values, "move_due")
	if err != nil {
		return err
	}
	if published {
		b.metrics.IncPublished(taskforge.EffectiveQueue(msg))
	}
	return nil
}

func parseReleaseScriptResult(values interface{}, operation string) (bool, bool, error) {
	result, ok := values.([]interface{})
	if !ok || len(result) != 2 {
		return false, false, fmt.Errorf("unexpected release script response %T", values)
	}
	existed, err := redisScriptInt(result[0])
	if err != nil {
		return false, false, err
	}
	if existed == -1 {
		return false, false, taskforge.NewStaleLeadershipError(operation)
	}
	removed, err := redisScriptInt(result[1])
	if err != nil {
		return false, false, err
	}
	return existed == 0, removed == 1, nil
}
