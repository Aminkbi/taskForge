package taskforge

import (
	"errors"

	"github.com/aminkbi/taskforge/internal/broker"
	runtimepkg "github.com/aminkbi/taskforge/internal/runtime"
	"github.com/aminkbi/taskforge/internal/store"
)

var (
	ErrNoTask            = broker.ErrNoTask
	ErrDeliveryExpired   = broker.ErrDeliveryExpired
	ErrUnknownDelivery   = broker.ErrUnknownDelivery
	ErrStaleDelivery     = broker.ErrStaleDelivery
	ErrAdmissionRejected = broker.ErrAdmissionRejected
	ErrTaskNotFound      = store.ErrTaskNotFound
	ErrUnknownTask       = errors.New("taskforge: unknown task")
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

type UnknownTaskError struct {
	Name string
}

func (e *UnknownTaskError) Error() string {
	if e == nil || e.Name == "" {
		return ErrUnknownTask.Error()
	}
	return ErrUnknownTask.Error() + ": " + e.Name
}

func (e *UnknownTaskError) Unwrap() error {
	return ErrUnknownTask
}

func Retryable(err error) error {
	return runtimepkg.Retryable(err)
}

func Permanent(err error) error {
	return runtimepkg.Permanent(err)
}

func Validation(err error) error {
	return runtimepkg.Validation(err)
}

func Decode(err error) error {
	return runtimepkg.Decode(err)
}

func LeaseLost(err error) error {
	return runtimepkg.LeaseLost(err)
}

func Timeout(err error) error {
	return runtimepkg.Timeout(err)
}

func admissionErrorFromBroker(err error) error {
	var admissionErr *broker.AdmissionError
	if !errors.As(err, &admissionErr) {
		return err
	}
	return &AdmissionError{
		Queue:  admissionErr.Queue,
		Reason: admissionErr.Reason,
	}
}
