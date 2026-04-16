package broker

import "errors"

var (
	ErrNoTask            = errors.New("broker: no task available")
	ErrDeliveryExpired   = errors.New("broker: delivery expired")
	ErrUnknownDelivery   = errors.New("broker: unknown delivery")
	ErrStaleDelivery     = errors.New("broker: stale delivery")
	ErrAdmissionRejected = errors.New("broker: admission rejected")

	// Backward-compatible aliases while the internal lease-oriented naming is
	// phased out in favor of delivery ownership terminology.
	ErrLeaseExpired = ErrDeliveryExpired
	ErrUnknownLease = ErrUnknownDelivery
)

type AdmissionError struct {
	Queue  string
	Reason string
}

func (e *AdmissionError) Error() string {
	if e == nil {
		return ErrAdmissionRejected.Error()
	}
	if e.Queue == "" && e.Reason == "" {
		return ErrAdmissionRejected.Error()
	}
	if e.Queue == "" {
		return ErrAdmissionRejected.Error() + ": " + e.Reason
	}
	if e.Reason == "" {
		return ErrAdmissionRejected.Error() + " for queue " + e.Queue
	}
	return ErrAdmissionRejected.Error() + " for queue " + e.Queue + ": " + e.Reason
}

func (e *AdmissionError) Unwrap() error {
	return ErrAdmissionRejected
}
