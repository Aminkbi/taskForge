package runtime

import (
	"context"
	"log/slog"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
)

func startLeaseExtender(ctx context.Context, logger *slog.Logger, b broker.Broker, delivery broker.Delivery, ttl time.Duration) context.CancelFunc {
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
				if err := b.ExtendLease(childCtx, delivery, ttl); err != nil {
					logger.Debug(
						"lease extension failed",
						"task_id", delivery.Execution.TaskID,
						"delivery_id", delivery.Execution.DeliveryID,
						"lease_owner", delivery.Execution.LeaseOwner,
						"lease_expires_at", delivery.Execution.LeaseExpiresAt,
						"error", err,
					)
					return
				}
				delivery.Execution.LeaseExpiresAt = time.Now().UTC().Add(ttl)
			}
		}
	}()

	return cancel
}
