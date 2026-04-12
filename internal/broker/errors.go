package broker

import "errors"

var (
	ErrNoTask       = errors.New("broker: no task available")
	ErrLeaseExpired = errors.New("broker: lease expired")
	ErrUnknownLease = errors.New("broker: unknown lease")
)
