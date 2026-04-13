package broker

import "errors"

var (
	ErrNoTask          = errors.New("broker: no task available")
	ErrDeliveryExpired = errors.New("broker: delivery expired")
	ErrUnknownDelivery = errors.New("broker: unknown delivery")
	ErrStaleDelivery   = errors.New("broker: stale delivery")

	// Backward-compatible aliases while the internal lease-oriented naming is
	// phased out in favor of delivery ownership terminology.
	ErrLeaseExpired = ErrDeliveryExpired
	ErrUnknownLease = ErrUnknownDelivery
)
