package runtime

import (
	"context"
	"log/slog"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
)

func startLeaseExtender(ctx context.Context, logger *slog.Logger, b broker.Broker, lease broker.Lease, ttl time.Duration) context.CancelFunc {
	if ttl <= 0 {
		return func() {}
	}

	renewEvery := ttl / 2
	if renewEvery <= 0 {
		renewEvery = time.Second
	}

	childCtx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(renewEvery)
		defer ticker.Stop()

		for {
			select {
			case <-childCtx.Done():
				return
			case <-ticker.C:
				if err := b.ExtendLease(childCtx, lease, ttl); err != nil {
					logger.Debug("lease extension failed", "lease_token", lease.Token, "error", err)
					return
				}
			}
		}
	}()

	return cancel
}
