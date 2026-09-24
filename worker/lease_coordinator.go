package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/logging"
)

const (
	maxLeaseRenewalBatchSize           = 128
	maxLeaseRenewalFallbackConcurrency = 8
	leaseRenewalTimeout                = 2 * time.Second
	leaseRenewalTimeoutDivisor         = 3
)

var (
	errLeaseCoordinatorStopped = errors.New("lease coordinator stopped")
	errLeaseAlreadyExpired     = errors.New("delivery lease already expired")
)

type leaseBatcher interface {
	ExtendLeases(context.Context, []taskforge.Delivery) ([]error, error)
}

type brokerLeaseEntry struct {
	delivery  taskforge.Delivery
	ttl       time.Duration
	next      time.Time
	expiresAt time.Time
}

type leaseCandidate struct {
	handle *leaseHandle
	entry  brokerLeaseEntry
}

type expiredLease struct {
	handle   *leaseHandle
	delivery taskforge.Delivery
}

type leaseRenewal struct {
	handle    *leaseHandle
	delivery  taskforge.Delivery
	ttl       time.Duration
	expiresAt time.Time
}

type leaseBatch struct {
	renewals []leaseRenewal
	expired  []expiredLease
}

type leaseCoordinator struct {
	ctx       context.Context
	logger    *slog.Logger
	broker    taskforge.Broker
	mu        sync.Mutex
	entries   map[*leaseHandle]brokerLeaseEntry
	scheduled time.Time
	wake      chan struct{}
	stopped   bool
	done      chan struct{}
}

func newLeaseCoordinator(ctx context.Context, logger *slog.Logger, broker taskforge.Broker) *leaseCoordinator {
	return &leaseCoordinator{
		ctx:     ctx,
		logger:  logger,
		broker:  broker,
		entries: make(map[*leaseHandle]brokerLeaseEntry),
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
}

func (c *leaseCoordinator) register(delivery taskforge.Delivery, ttl time.Duration) (*leaseHandle, error) {
	if ttl <= 0 {
		return nil, nil
	}
	now := time.Now()
	expiresAt := delivery.Execution.LeaseExpiresAt
	if expiresAt.IsZero() {
		expiresAt = now.Add(ttl)
	}
	if !expiresAt.After(now) {
		return nil, errLeaseAlreadyExpired
	}

	handle := &leaseHandle{doneCh: make(chan struct{}), lostCh: make(chan struct{})}
	handle.setExpiresAt(expiresAt)
	handle.cancel = func() { c.remove(handle) }
	next := now.Add(leaseRenewalDelay(ttl, expiresAt.Sub(now)))

	c.mu.Lock()
	if c.stopped || c.ctx.Err() != nil {
		c.mu.Unlock()
		return nil, errLeaseCoordinatorStopped
	}
	c.entries[handle] = brokerLeaseEntry{
		delivery:  delivery,
		ttl:       ttl,
		next:      next,
		expiresAt: expiresAt,
	}
	wake := c.scheduled.IsZero() || next.Before(c.scheduled)
	c.mu.Unlock()
	if wake {
		notify(c.wake)
	}
	return handle, nil
}

func leaseRenewalDelay(ttl, remaining time.Duration) time.Duration {
	if remaining <= 0 {
		return 0
	}
	if remaining < 2*time.Millisecond {
		return 0
	}
	return min(max(ttl/2, time.Millisecond), max(remaining/2, time.Millisecond))
}

func (c *leaseCoordinator) remove(handle *leaseHandle) {
	if handle == nil {
		return
	}
	c.mu.Lock()
	delete(c.entries, handle)
	stopped := c.stopped
	c.mu.Unlock()
	handle.closeDone()
	if !stopped {
		notify(c.wake)
	}
}

func (c *leaseCoordinator) run() {
	defer func() {
		c.stopAll()
		close(c.done)
	}()

	for {
		if c.ctx.Err() != nil {
			return
		}
		batch, earliest := c.nextBatch(time.Now())
		if len(batch.expired) > 0 || len(batch.renewals) > 0 {
			c.renew(batch)
			continue
		}

		var timer *time.Timer
		var timerCh <-chan time.Time
		if !earliest.IsZero() {
			delay := time.Until(earliest)
			if delay < 0 {
				delay = 0
			}
			timer = time.NewTimer(delay)
			timerCh = timer.C
		}
		select {
		case <-c.ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case <-c.wake:
		case <-timerCh:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

func (c *leaseCoordinator) nextBatch(now time.Time) (leaseBatch, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	candidates := make([]leaseCandidate, 0, len(c.entries))
	for handle, entry := range c.entries {
		window := min(entry.ttl/10, 10*time.Millisecond)
		if entry.next.After(now.Add(window)) || !entry.expiresAt.After(now) {
			continue
		}
		candidates = append(candidates, leaseCandidate{handle: handle, entry: entry})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].entry.next.Equal(candidates[j].entry.next) {
			return candidates[i].entry.delivery.OwnershipKey() < candidates[j].entry.delivery.OwnershipKey()
		}
		return candidates[i].entry.next.Before(candidates[j].entry.next)
	})
	if len(candidates) > maxLeaseRenewalBatchSize {
		candidates = candidates[:maxLeaseRenewalBatchSize]
	}

	batch := leaseBatch{
		renewals: make([]leaseRenewal, 0, len(candidates)),
		expired:  make([]expiredLease, 0),
	}
	for _, candidate := range candidates {
		entry := candidate.entry
		entry.next = now.Add(leaseRenewalDelay(entry.ttl, entry.expiresAt.Sub(now)))
		c.entries[candidate.handle] = entry
		batch.renewals = append(batch.renewals, leaseRenewal{
			handle:    candidate.handle,
			delivery:  entry.delivery,
			ttl:       entry.ttl,
			expiresAt: entry.expiresAt,
		})
	}
	for handle, entry := range c.entries {
		if len(batch.expired) >= maxLeaseRenewalBatchSize {
			break
		}
		window := min(entry.ttl/10, 10*time.Millisecond)
		if !entry.next.After(now.Add(window)) && !entry.expiresAt.After(now) {
			batch.expired = append(batch.expired, expiredLease{handle: handle, delivery: entry.delivery})
		}
	}

	c.scheduled = time.Time{}
	for _, entry := range c.entries {
		if c.scheduled.IsZero() || entry.next.Before(c.scheduled) {
			c.scheduled = entry.next
		}
	}
	return batch, c.scheduled
}

func (c *leaseCoordinator) renew(batch leaseBatch) {
	for _, item := range batch.expired {
		if c.ctx.Err() == nil {
			c.lose(item.handle, item.delivery, taskforge.ErrDeliveryExpired)
		}
	}
	if len(batch.renewals) == 0 {
		return
	}

	active := make([]leaseRenewal, 0, len(batch.renewals))
	for _, item := range batch.renewals {
		if !item.expiresAt.After(time.Now()) {
			c.lose(item.handle, item.delivery, taskforge.ErrDeliveryExpired)
			continue
		}
		active = append(active, item)
	}
	if len(active) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(c.ctx, leaseBatchTimeout(active))
	defer cancel()
	errs, batchErr := c.renewWithFallback(ctx, active)
	if c.ctx.Err() != nil {
		return
	}
	if len(errs) != len(active) {
		if batchErr == nil {
			batchErr = fmt.Errorf("lease batch returned %d results for %d renewals", len(errs), len(active))
		}
		errs = make([]error, len(active))
		for index := range errs {
			errs[index] = batchErr
		}
	}
	for index, item := range active {
		if errs[index] == nil {
			c.refresh(item.handle, item.ttl)
			continue
		}
		c.lose(item.handle, item.delivery, errs[index])
	}
}

func (c *leaseCoordinator) renewWithFallback(ctx context.Context, renewals []leaseRenewal) ([]error, error) {
	if batcher, ok := c.broker.(leaseBatcher); ok {
		deliveries := make([]taskforge.Delivery, len(renewals))
		for index, renewal := range renewals {
			deliveries[index] = renewal.delivery
		}
		errs, err := batcher.ExtendLeases(ctx, deliveries)
		if err == nil && len(errs) == len(renewals) {
			return errs, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err == nil {
			err = fmt.Errorf("lease batch returned %d results for %d renewals", len(errs), len(renewals))
		}
		if fallbackErrs := c.renewScalar(ctx, renewals); len(fallbackErrs) == len(renewals) {
			return fallbackErrs, err
		}
		return nil, err
	}
	return c.renewScalar(ctx, renewals), nil
}

func (c *leaseCoordinator) renewScalar(ctx context.Context, renewals []leaseRenewal) []error {
	errs := make([]error, len(renewals))
	if len(renewals) == 0 {
		return errs
	}
	workers := min(maxLeaseRenewalFallbackConcurrency, len(renewals))
	var next atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for {
				index := int(next.Add(1)) - 1
				if index >= len(renewals) {
					return
				}
				if err := ctx.Err(); err != nil {
					errs[index] = err
					continue
				}
				renewal := renewals[index]
				errs[index] = c.broker.ExtendLease(ctx, renewal.delivery, renewal.ttl)
			}
		})
	}
	wg.Wait()
	return errs
}

func (c *leaseCoordinator) refresh(handle *leaseHandle, ttl time.Duration) {
	now := time.Now()
	c.mu.Lock()
	entry, active := c.entries[handle]
	if active {
		entry.expiresAt = now.Add(ttl)
		entry.next = now.Add(leaseRenewalDelay(ttl, ttl))
		entry.delivery.Execution.LeasedAt = now
		entry.delivery.Execution.LeaseExpiresAt = entry.expiresAt
		handle.setExpiresAt(entry.expiresAt)
		c.entries[handle] = entry
	}
	c.mu.Unlock()
}

func (c *leaseCoordinator) lose(handle *leaseHandle, delivery taskforge.Delivery, err error) {
	if err == nil {
		err = errors.New("lease renewal failed")
	}
	c.mu.Lock()
	_, active := c.entries[handle]
	if active {
		delete(c.entries, handle)
	}
	c.mu.Unlock()
	if !active {
		return
	}
	handle.setLost(err)
	logging.WithDelivery(c.logger, delivery).Debug("broker lease extension failed", "error", err)
	handle.closeDone()
}

func (c *leaseCoordinator) stopAll() {
	c.mu.Lock()
	c.stopped = true
	handles := make([]*leaseHandle, 0, len(c.entries))
	for handle := range c.entries {
		handles = append(handles, handle)
	}
	c.entries = make(map[*leaseHandle]brokerLeaseEntry)
	c.scheduled = time.Time{}
	c.mu.Unlock()
	for _, handle := range handles {
		handle.closeDone()
	}
}

func leaseBatchTimeout(renewals []leaseRenewal) time.Duration {
	timeout := time.Millisecond
	for _, renewal := range renewals {
		itemTimeout := renewal.ttl - renewal.ttl/leaseRenewalTimeoutDivisor
		remaining := time.Until(renewal.expiresAt)
		if remaining < itemTimeout {
			itemTimeout = remaining
		}
		if itemTimeout > timeout {
			timeout = itemTimeout
		}
	}
	if timeout > leaseRenewalTimeout {
		timeout = leaseRenewalTimeout
	}
	if timeout < time.Millisecond {
		timeout = time.Millisecond
	}
	return timeout
}
