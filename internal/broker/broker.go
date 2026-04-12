package broker

import (
	"context"
	"time"
)

type Broker interface {
	Publish(ctx context.Context, msg TaskMessage) error
	Reserve(ctx context.Context, queue, consumerID string) (Lease, TaskMessage, error)
	Ack(ctx context.Context, lease Lease) error
	Nack(ctx context.Context, lease Lease, requeue bool) error
	ExtendLease(ctx context.Context, lease Lease, ttl time.Duration) error
}
