package brokerredis

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/fairness"
	"github.com/aminkbi/taskforge/internal/logging"
	"github.com/aminkbi/taskforge/internal/observability"
)

const (
	fairnessTierReserved = "reserved"
	fairnessTierShared   = "shared"
	fairnessTierBorrow   = "borrow"
)

type fairnessKeySnapshot struct {
	Key            string
	Rule           fairness.ResolvedRule
	Ready          int64
	Reserved       int64
	OldestReadyAge float64
}

func cloneFairnessPolicies(policies map[string]*fairness.Policy) map[string]*fairness.Policy {
	if len(policies) == 0 {
		return nil
	}

	cloned := make(map[string]*fairness.Policy, len(policies))
	for queue, policy := range policies {
		if policy == nil {
			continue
		}
		cloned[normalizeQueue(queue)] = policy
	}
	if len(cloned) == 0 {
		return nil
	}
	return cloned
}

func (b *RedisBroker) fairnessPolicy(queue string) *fairness.Policy {
	if len(b.fairnessPolicies) == 0 {
		return nil
	}
	return b.fairnessPolicies[normalizeQueue(queue)]
}

func (b *RedisBroker) queueStreamKey(queue, fairnessKey string) string {
	if b.fairnessPolicy(queue) == nil {
		return b.streamKey(queue)
	}
	return b.fairnessStreamKey(queue, fairness.NormalizeKey(fairnessKey))
}

func (b *RedisBroker) fairnessStreamKey(queue, fairnessKey string) string {
	return fmt.Sprintf("%s:stream:%s:fair:%x", b.prefix, normalizeQueue(queue), sha256Sum(fairnessKey))
}

func (b *RedisBroker) fairnessKeysSetKey(queue string) string {
	return fmt.Sprintf("%s:fairness:%s:keys", b.prefix, normalizeQueue(queue))
}

func (b *RedisBroker) fairnessNotifyKey(queue string) string {
	return fmt.Sprintf("%s:fairness:%s:ready", b.prefix, normalizeQueue(queue))
}

func (b *RedisBroker) fairnessCursorKey(queue, tier string) string {
	return fmt.Sprintf("%s:fairness:%s:cursor:%s", b.prefix, normalizeQueue(queue), tier)
}

func (b *RedisBroker) publishFairReady(ctx context.Context, msg broker.TaskMessage, payload []byte, deduplicationKey string, now time.Time) (bool, error) {
	queue := normalizeQueue(msg.Queue)
	fairnessKey := fairness.NormalizeKey(msg.FairnessKey)
	streamKey := b.fairnessStreamKey(queue, fairnessKey)

	if deduplicationKey == "" {
		pipe := b.client.TxPipeline()
		pipe.SAdd(ctx, b.fairnessKeysSetKey(queue), fairnessKey)
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]interface{}{
				streamPayloadField: string(payload),
			},
		})
		pipe.LPush(ctx, b.fairnessNotifyKey(queue), now.UTC().Format(time.RFC3339Nano))
		pipe.LTrim(ctx, b.fairnessNotifyKey(queue), 0, 0)
		if _, err := pipe.Exec(ctx); err != nil {
			return false, fmt.Errorf("publish task: fairness queue entry: %w", err)
		}
		return true, nil
	}

	published, err := publishFairReadyWithReceiptScript.Run(
		ctx,
		b.client,
		[]string{
			b.fairnessKeysSetKey(queue),
			streamKey,
			b.fairnessNotifyKey(queue),
			b.publishReceiptKey(deduplicationKey),
		},
		fairnessKey,
		streamPayloadField,
		string(payload),
		now.UTC().Format(time.RFC3339Nano),
		b.publishReceiptTTL().Milliseconds(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("publish task: fairness queue entry: %w", err)
	}
	return published == 1, nil
}

func (b *RedisBroker) reserveFair(ctx context.Context, queue, consumerID string) (broker.Delivery, error) {
	queue = normalizeQueue(queue)
	consumerName := b.consumerName(consumerID)

	if reclaimed, ok, err := b.reclaimFairExpiredDelivery(ctx, queue, consumerName); err != nil {
		return broker.Delivery{}, err
	} else if ok {
		return reclaimed, nil
	}

	for {
		now := time.Now().UTC()
		snapshots, err := b.loadFairnessSnapshots(ctx, queue, now)
		if err != nil {
			return broker.Delivery{}, err
		}

		candidate, tier, ok, err := b.selectFairnessCandidate(ctx, queue, snapshots)
		if err != nil {
			return broker.Delivery{}, err
		}
		if ok {
			delivery, reserved, err := b.reserveFairCandidate(ctx, queue, consumerName, candidate)
			if err != nil {
				return broker.Delivery{}, err
			}
			if reserved {
				b.metrics.IncFairnessReservation(queue, candidate.Rule.Bucket)
				return delivery, nil
			}
			_ = tier
			continue
		}

		values, err := b.client.BLPop(ctx, b.reserveTTL, b.fairnessNotifyKey(queue)).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return broker.Delivery{}, broker.ErrNoTask
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return broker.Delivery{}, err
			}
			return broker.Delivery{}, fmt.Errorf("reserve task: fairness wait: %w", err)
		}
		if len(values) == 0 {
			return broker.Delivery{}, broker.ErrNoTask
		}
	}
}

func (b *RedisBroker) selectFairnessCandidate(ctx context.Context, queue string, snapshots []fairnessKeySnapshot) (fairnessKeySnapshot, string, bool, error) {
	reservedCandidates := make([]fairnessKeySnapshot, 0, len(snapshots))
	sharedCandidates := make([]fairnessKeySnapshot, 0, len(snapshots))
	borrowCandidates := make([]fairnessKeySnapshot, 0, len(snapshots))

	for _, snapshot := range snapshots {
		if snapshot.Ready == 0 {
			continue
		}

		switch tier, reason := classifyFairnessTier(snapshot.Rule, snapshot.Reserved); {
		case tier == fairnessTierReserved:
			reservedCandidates = append(reservedCandidates, snapshot)
		case tier == fairnessTierShared:
			sharedCandidates = append(sharedCandidates, snapshot)
		case tier == fairnessTierBorrow:
			borrowCandidates = append(borrowCandidates, snapshot)
		case reason != "":
			b.metrics.IncFairnessQuotaDeferral(queue, snapshot.Rule.Bucket, reason)
		}
	}

	if len(reservedCandidates) > 0 {
		candidate, err := b.chooseWeightedFairnessCandidate(ctx, queue, fairnessTierReserved, reservedCandidates)
		return candidate, fairnessTierReserved, err == nil, err
	}
	if len(sharedCandidates) > 0 {
		candidate, err := b.chooseWeightedFairnessCandidate(ctx, queue, fairnessTierShared, sharedCandidates)
		return candidate, fairnessTierShared, err == nil, err
	}
	if len(borrowCandidates) > 0 {
		candidate, err := b.chooseWeightedFairnessCandidate(ctx, queue, fairnessTierBorrow, borrowCandidates)
		return candidate, fairnessTierBorrow, err == nil, err
	}
	return fairnessKeySnapshot{}, "", false, nil
}

func (b *RedisBroker) chooseWeightedFairnessCandidate(ctx context.Context, queue, tier string, candidates []fairnessKeySnapshot) (fairnessKeySnapshot, error) {
	slices.SortFunc(candidates, func(a, c fairnessKeySnapshot) int {
		return compareStrings(a.Key, c.Key)
	})

	totalWeight := 0
	for _, candidate := range candidates {
		weight := candidate.Rule.Weight
		if weight < 1 {
			weight = 1
		}
		totalWeight += weight
	}
	if totalWeight == 0 {
		return fairnessKeySnapshot{}, fmt.Errorf("choose fairness candidate: no candidate weight")
	}

	ticket, err := b.client.Incr(ctx, b.fairnessCursorKey(queue, tier)).Result()
	if err != nil {
		return fairnessKeySnapshot{}, fmt.Errorf("choose fairness candidate: %w", err)
	}
	index := int((ticket - 1) % int64(totalWeight))
	running := 0
	for _, candidate := range candidates {
		weight := candidate.Rule.Weight
		if weight < 1 {
			weight = 1
		}
		running += weight
		if index < running {
			return candidate, nil
		}
	}
	return candidates[0], nil
}

func (b *RedisBroker) reserveFairCandidate(ctx context.Context, queue, consumerName string, candidate fairnessKeySnapshot) (broker.Delivery, bool, error) {
	streamKey := b.fairnessStreamKey(queue, candidate.Key)
	groupName := b.groupName(queue)
	if err := b.ensureGroup(ctx, streamKey, groupName); err != nil {
		return broker.Delivery{}, false, err
	}

	streams, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamKey, ">"},
		Count:    1,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return broker.Delivery{}, false, nil
		}
		return broker.Delivery{}, false, fmt.Errorf("reserve task: fairness read: %w", err)
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return broker.Delivery{}, false, nil
	}

	entry := streams[0].Messages[0]
	msg, err := decodeTaskMessage(entry)
	if err != nil {
		return broker.Delivery{}, false, fmt.Errorf("reserve task: fairness decode: %w", err)
	}
	if msg.Queue == "" {
		msg.Queue = queue
	}
	ttl := b.effectiveLeaseTTL(msg)
	now := time.Now().UTC()
	delivery := newDelivery(msg, queue, consumerName, entry.ID, now, ttl, deliveryCount(msg, 0))
	spanCtx := observability.ExtractTraceContext(ctx, msg.Headers)
	_, span := observability.StartQueueSpan(
		spanCtx,
		"taskforge.brokerredis",
		"taskforge.reserve",
		msg,
		deliverySpanAttributes(delivery)...,
	)
	defer span.End()
	logging.WithDelivery(b.logger, delivery).Info("reserved task delivery")
	return delivery, true, nil
}

func (b *RedisBroker) reclaimFairExpiredDelivery(ctx context.Context, queue, consumerName string) (broker.Delivery, bool, error) {
	keys, err := b.activeFairnessKeys(ctx, queue)
	if err != nil {
		return broker.Delivery{}, false, err
	}

	for _, fairnessKey := range keys {
		streamKey := b.fairnessStreamKey(queue, fairnessKey)
		exists, err := b.client.Exists(ctx, streamKey).Result()
		if err != nil {
			return broker.Delivery{}, false, fmt.Errorf("reclaim task: fairness inspect stream: %w", err)
		}
		if exists == 0 {
			_ = b.client.SRem(ctx, b.fairnessKeysSetKey(queue), fairnessKey).Err()
			continue
		}
		if err := b.ensureGroup(ctx, streamKey, b.groupName(queue)); err != nil {
			return broker.Delivery{}, false, err
		}
		delivery, ok, err := b.reclaimExpiredDelivery(ctx, queue, streamKey, b.groupName(queue), consumerName)
		if err != nil {
			return broker.Delivery{}, false, err
		}
		if ok {
			return delivery, true, nil
		}
	}

	return broker.Delivery{}, false, nil
}

func (b *RedisBroker) loadFairnessSnapshots(ctx context.Context, queue string, now time.Time) ([]fairnessKeySnapshot, error) {
	policy := b.fairnessPolicy(queue)
	if policy == nil {
		return nil, nil
	}

	keys, err := b.activeFairnessKeys(ctx, queue)
	if err != nil {
		return nil, err
	}

	snapshots := make([]fairnessKeySnapshot, 0, len(keys))
	for _, fairnessKey := range keys {
		snapshot, ok, err := b.loadFairnessKeySnapshot(ctx, queue, fairnessKey, now)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		snapshot.Rule = policy.Resolve(fairnessKey)
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

func (b *RedisBroker) loadFairnessKeySnapshot(ctx context.Context, queue, fairnessKey string, now time.Time) (fairnessKeySnapshot, bool, error) {
	streamKey := b.fairnessStreamKey(queue, fairnessKey)
	groupName := b.groupName(queue)

	length := int64(0)
	streamInfo, err := b.client.XInfoStream(ctx, streamKey).Result()
	if err != nil {
		if !isMissingStream(err) {
			return fairnessKeySnapshot{}, false, fmt.Errorf("fairness metrics: stream %q: %w", fairnessKey, err)
		}
		_ = b.client.SRem(ctx, b.fairnessKeysSetKey(queue), fairnessKey).Err()
		return fairnessKeySnapshot{}, false, nil
	}
	length = streamInfo.Length

	pendingCount := int64(0)
	pending, err := b.client.XPending(ctx, streamKey, groupName).Result()
	if err != nil {
		if !isMissingGroup(err) {
			return fairnessKeySnapshot{}, false, fmt.Errorf("fairness metrics: pending %q: %w", fairnessKey, err)
		}
	} else {
		pendingCount = pending.Count
	}

	ready := length - pendingCount
	if ready < 0 {
		ready = 0
	}
	if ready == 0 && pendingCount == 0 {
		_ = b.client.SRem(ctx, b.fairnessKeysSetKey(queue), fairnessKey).Err()
	}

	return fairnessKeySnapshot{
		Key:            fairnessKey,
		Ready:          ready,
		Reserved:       pendingCount,
		OldestReadyAge: b.oldestFairnessReadyAge(ctx, streamKey, groupName, now),
	}, true, nil
}

func (b *RedisBroker) oldestFairnessReadyAge(ctx context.Context, streamKey, groupName string, now time.Time) float64 {
	age := b.oldestReadyAge(ctx, streamKey, groupName, now)
	return age.Seconds()
}

func (b *RedisBroker) activeFairnessKeys(ctx context.Context, queue string) ([]string, error) {
	keys, err := b.client.SMembers(ctx, b.fairnessKeysSetKey(queue)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("fairness keys %q: %w", queue, err)
	}
	slices.Sort(keys)
	return keys, nil
}

func (b *RedisBroker) FairnessMetricsSnapshot(ctx context.Context, queue string, now time.Time) ([]observability.FairnessMetricsSnapshot, error) {
	policy := b.fairnessPolicy(queue)
	if policy == nil {
		return nil, nil
	}

	buckets := make(map[string]observability.FairnessMetricsSnapshot)
	for _, bucket := range policy.Buckets() {
		rule, _ := policy.BucketRule(bucket)
		buckets[bucket] = observability.FairnessMetricsSnapshot{
			Bucket: bucket,
			Weight: float64(rule.Weight),
		}
	}

	snapshots, err := b.loadFairnessSnapshots(ctx, queue, now)
	if err != nil {
		return nil, err
	}
	for _, snapshot := range snapshots {
		current := buckets[snapshot.Rule.Bucket]
		current.Bucket = snapshot.Rule.Bucket
		current.Weight = float64(snapshot.Rule.Weight)
		current.Depth += float64(snapshot.Ready)
		current.Reserved += float64(snapshot.Reserved)
		if snapshot.OldestReadyAge > current.OldestReadyAge {
			current.OldestReadyAge = snapshot.OldestReadyAge
		}
		buckets[snapshot.Rule.Bucket] = current
	}

	result := make([]observability.FairnessMetricsSnapshot, 0, len(buckets))
	for _, bucket := range policy.Buckets() {
		result = append(result, buckets[bucket])
	}
	return result, nil
}

func (b *RedisBroker) fairQueueMetricsSnapshot(ctx context.Context, queue string) (observability.QueueMetricsSnapshot, error) {
	keys, err := b.activeFairnessKeys(ctx, queue)
	if err != nil {
		return observability.QueueMetricsSnapshot{}, err
	}

	depth := 0.0
	reserved := 0.0
	consumers := map[string]struct{}{}
	for _, fairnessKey := range keys {
		snapshot, ok, err := b.loadFairnessKeySnapshot(ctx, queue, fairnessKey, time.Now().UTC())
		if err != nil {
			return observability.QueueMetricsSnapshot{}, err
		}
		if !ok {
			continue
		}
		depth += float64(snapshot.Ready)
		reserved += float64(snapshot.Reserved)

		streamKey := b.fairnessStreamKey(queue, fairnessKey)
		entries, err := b.client.XInfoConsumers(ctx, streamKey, b.groupName(queue)).Result()
		if err != nil {
			if isMissingGroup(err) || isMissingStream(err) {
				continue
			}
			return observability.QueueMetricsSnapshot{}, fmt.Errorf("queue metrics: consumers %q/%q: %w", queue, fairnessKey, err)
		}
		for _, entry := range entries {
			consumers[entry.Name] = struct{}{}
		}
	}

	return observability.QueueMetricsSnapshot{
		Depth:     depth,
		Reserved:  reserved,
		Consumers: float64(len(consumers)),
	}, nil
}

func classifyFairnessTier(rule fairness.ResolvedRule, reserved int64) (string, string) {
	if rule.ReservedConcurrency > 0 && reserved < int64(rule.ReservedConcurrency) {
		return fairnessTierReserved, ""
	}
	if rule.HardQuota > 0 && reserved >= int64(rule.HardQuota) {
		return "", "hard_quota"
	}
	if rule.SoftQuota <= 0 {
		return fairnessTierShared, ""
	}
	if reserved < int64(rule.SoftQuota) {
		return fairnessTierShared, ""
	}
	borrowLimit := rule.SoftQuota + rule.Burst
	if borrowLimit > 0 && reserved < int64(borrowLimit) {
		return fairnessTierBorrow, ""
	}
	return "", "soft_quota"
}

func compareStrings(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func sha256Sum(value string) [32]byte {
	return sha256.Sum256([]byte(value))
}
