package runtime

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/logging"
)

type leaseHandle struct {
	cancel   context.CancelFunc
	doneCh   chan struct{}
	lostCh   chan struct{}
	closeMu  sync.Mutex
	closeSet bool
	errMu    sync.RWMutex
	err      error
}

func startLeaseExtender(ctx context.Context, logger *slog.Logger, b broker.Broker, delivery broker.Delivery, ttl time.Duration) *leaseHandle {
	if ttl <= 0 {
		return nil
	}
	return startRenewalLoop(
		ctx,
		logger,
		delivery,
		ttl/2,
		"broker lease extension failed",
		func(renewCtx context.Context) error {
			if err := b.ExtendLease(renewCtx, delivery, ttl); err != nil {
				return err
			}
			delivery.Execution.LeaseExpiresAt = time.Now().UTC().Add(ttl)
			return nil
		},
	)
}

func startBudgetExtender(ctx context.Context, logger *slog.Logger, manager BudgetManager, delivery broker.Delivery, leaseKey string, budget TaskBudget, ttl time.Duration) *leaseHandle {
	if ttl <= 0 || manager == nil {
		return nil
	}
	return startRenewalLoop(
		ctx,
		logger,
		delivery,
		ttl/2,
		"budget lease renewal failed",
		func(renewCtx context.Context) error {
			return manager.RenewLease(renewCtx, budget.Budget, leaseKey, ttl)
		},
	)
}

func startRenewalLoop(ctx context.Context, logger *slog.Logger, delivery broker.Delivery, every time.Duration, failureMessage string, renew func(context.Context) error) *leaseHandle {
	if every <= 0 {
		every = time.Second
	}

	childCtx, cancel := context.WithCancel(ctx)
	handle := &leaseHandle{
		cancel: cancel,
		doneCh: make(chan struct{}),
		lostCh: make(chan struct{}),
	}

	go func() {
		defer handle.closeDone()

		ticker := time.NewTicker(every)
		defer ticker.Stop()

		for {
			select {
			case <-childCtx.Done():
				return
			case <-ticker.C:
				if err := renew(childCtx); err != nil {
					handle.setLost(err)
					logging.WithDelivery(logger, delivery).Debug(
						failureMessage,
						"error", err,
					)
					return
				}
			}
		}
	}()

	return handle
}

func (h *leaseHandle) Stop() {
	if h == nil {
		return
	}
	h.cancel()
}

func (h *leaseHandle) Done() <-chan struct{} {
	if h == nil {
		return nil
	}
	return h.doneCh
}

func (h *leaseHandle) Lost() <-chan struct{} {
	if h == nil {
		return nil
	}
	return h.lostCh
}

func (h *leaseHandle) IsLost() bool {
	if h == nil {
		return false
	}
	select {
	case <-h.lostCh:
		return true
	default:
		return false
	}
}

func (h *leaseHandle) Err() error {
	if h == nil {
		return nil
	}
	h.errMu.RLock()
	defer h.errMu.RUnlock()
	return h.err
}

func (h *leaseHandle) setLost(err error) {
	if h == nil {
		return
	}
	h.errMu.Lock()
	if h.err == nil {
		h.err = err
	}
	h.errMu.Unlock()

	h.closeMu.Lock()
	defer h.closeMu.Unlock()
	select {
	case <-h.lostCh:
	default:
		close(h.lostCh)
	}
}

func (h *leaseHandle) closeDone() {
	if h == nil {
		return
	}
	h.closeMu.Lock()
	defer h.closeMu.Unlock()
	if h.closeSet {
		return
	}
	h.closeSet = true
	close(h.doneCh)
}
