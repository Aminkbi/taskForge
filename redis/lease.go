package redis

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aminkbi/taskforge"
)

const (
	maxRedisLeaseBatchSize    = 128
	maxRedisLeaseStreamWorker = 8
)

const (
	leaseRenewalUnknown int64 = iota
	leaseRenewalSucceeded
	leaseRenewalStale
	leaseRenewalExpired
)

type redisLeaseBatch struct {
	stream string
	group  string
	items  []int
}

func (b *Broker) ExtendLeases(ctx context.Context, deliveries []taskforge.Delivery) ([]error, error) {
	if err := b.checkConfig(); err != nil {
		return nil, err
	}
	if len(deliveries) == 0 {
		return nil, nil
	}
	if b == nil || b.client == nil {
		return nil, fmt.Errorf("extend leases: nil Redis broker")
	}

	errs := make([]error, len(deliveries))
	var batchErr error
	for start := 0; start < len(deliveries); start += maxRedisLeaseBatchSize {
		end := min(start+maxRedisLeaseBatchSize, len(deliveries))
		chunkErrs, err := b.extendLeaseChunk(ctx, deliveries[start:end])
		copy(errs[start:end], chunkErrs)
		if err != nil && batchErr == nil {
			batchErr = err
		}
	}
	for index, err := range errs {
		if err != nil {
			b.incrementLeaseExtensionFailure(taskforge.EffectiveQueue(deliveries[index].Message))
		}
	}
	return errs, batchErr
}

func (b *Broker) extendLeaseChunk(ctx context.Context, deliveries []taskforge.Delivery) ([]error, error) {
	errs := make([]error, len(deliveries))
	batches := make(map[string]*redisLeaseBatch)
	for index, delivery := range deliveries {
		if delivery.Execution.DeliveryID == "" || delivery.Execution.LeaseOwner == "" {
			errs[index] = taskforge.ErrUnknownDelivery
			continue
		}
		queue := taskforge.EffectiveQueue(delivery.Message)
		stream := b.queueStreamKey(queue, delivery.Message.FairnessKey)
		group := b.groupName(queue)
		key := stream + "\x00" + group
		batch := batches[key]
		if batch == nil {
			batch = &redisLeaseBatch{stream: stream, group: group}
			batches[key] = batch
		}
		batch.items = append(batch.items, index)
	}
	if len(batches) == 0 {
		return errs, ctx.Err()
	}

	keys := make([]string, 0, len(batches))
	for key := range batches {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	workers := min(maxRedisLeaseStreamWorker, len(keys))
	var next atomic.Int64
	var firstErr error
	var errMu sync.Mutex
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for {
				index := int(next.Add(1)) - 1
				if index >= len(keys) {
					return
				}
				batch := batches[keys[index]]
				if err := ctx.Err(); err != nil {
					for _, item := range batch.items {
						errs[item] = err
					}
					continue
				}
				if err := b.runRedisLeaseBatch(ctx, batch, deliveries, errs); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
				}
			}
		})
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return errs, err
	}
	return errs, firstErr
}

func (b *Broker) runRedisLeaseBatch(ctx context.Context, batch *redisLeaseBatch, deliveries []taskforge.Delivery, errs []error) error {
	args := make([]any, 0, 1+len(batch.items)*3)
	args = append(args, batch.group)
	for _, index := range batch.items {
		delivery := deliveries[index]
		ttl := b.effectiveLeaseTTL(delivery.Message)
		args = append(args, delivery.Execution.DeliveryID, delivery.Execution.LeaseOwner, ttl.Milliseconds())
	}
	result, err := fencedRenewLeasesScript.Run(ctx, b.client, []string{batch.stream, b.leaseDeadlineKey(batch.stream)}, args...).Result()
	if err != nil {
		for _, index := range batch.items {
			errs[index] = fmt.Errorf("extend lease stream %q delivery %q: %w", batch.stream, deliveries[index].Execution.DeliveryID, err)
		}
		return fmt.Errorf("extend lease stream %q: %w", batch.stream, err)
	}
	statuses, ok := result.([]any)
	if !ok || len(statuses) != len(batch.items) {
		for _, index := range batch.items {
			errs[index] = fmt.Errorf("extend lease stream %q: unexpected response %T", batch.stream, result)
		}
		return fmt.Errorf("extend lease stream %q: unexpected response %T", batch.stream, result)
	}
	for itemIndex, status := range statuses {
		index := batch.items[itemIndex]
		code, ok := int64Value(status)
		if !ok {
			errs[index] = fmt.Errorf("extend lease stream %q delivery %q: unexpected status %T", batch.stream, deliveries[index].Execution.DeliveryID, status)
			continue
		}
		switch code {
		case leaseRenewalUnknown:
			errs[index] = taskforge.ErrUnknownDelivery
		case leaseRenewalSucceeded:
		case leaseRenewalStale:
			errs[index] = taskforge.ErrStaleDelivery
		case leaseRenewalExpired:
			errs[index] = taskforge.ErrDeliveryExpired
		default:
			errs[index] = taskforge.ErrUnknownDelivery
		}
	}
	return nil
}

func normalizeLeaseTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return ttl
	}
	ttl = ttl.Truncate(time.Millisecond)
	if ttl < time.Millisecond {
		return time.Millisecond
	}
	return ttl
}
