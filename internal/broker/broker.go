package broker

import (
	"context"
	"time"
)

// Broker exposes TaskForge's internal at-least-once delivery contract.
// A logical task may be delivered more than once, and brokers must reject
// stale acknowledgements so an older delivery owner cannot finalize newer work.
type Broker interface {
	Publish(ctx context.Context, msg TaskMessage) error
	Reserve(ctx context.Context, queue, consumerID string) (Delivery, error)
	Ack(ctx context.Context, delivery Delivery) error
	Nack(ctx context.Context, delivery Delivery, requeue bool) error
	ExtendLease(ctx context.Context, delivery Delivery, ttl time.Duration) error
}
