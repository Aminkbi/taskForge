package taskforge

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrNoTask            = errors.New("taskforge: no task available")
	ErrDeliveryExpired   = errors.New("taskforge: delivery expired")
	ErrUnknownDelivery   = errors.New("taskforge: unknown delivery")
	ErrStaleDelivery     = errors.New("taskforge: stale delivery")
	ErrAdmissionRejected = errors.New("taskforge: admission rejected")
	ErrTaskNotFound      = errors.New("taskforge: task not found")
	ErrUnknownTask       = errors.New("taskforge: unknown task")
	ErrLeadershipLost    = errors.New("taskforge: scheduler leadership lost")
)

type StaleLeadershipError struct{ Operation string }

func (e *StaleLeadershipError) Error() string {
	if e == nil || e.Operation == "" {
		return ErrLeadershipLost.Error()
	}
	return fmt.Sprintf("%s during %s", ErrLeadershipLost, e.Operation)
}

func (e *StaleLeadershipError) Unwrap() error { return ErrLeadershipLost }

func NewStaleLeadershipError(operation string) error {
	return &StaleLeadershipError{Operation: operation}
}

type AdmissionError struct {
	Queue  string
	Reason string
}

func (e *AdmissionError) Error() string {
	if e == nil || e.Queue == "" && e.Reason == "" {
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

func (e *AdmissionError) Unwrap() error { return ErrAdmissionRejected }

type UnknownTaskError struct{ Name string }

func (e *UnknownTaskError) Error() string {
	if e == nil || e.Name == "" {
		return ErrUnknownTask.Error()
	}
	return ErrUnknownTask.Error() + ": " + e.Name
}

func (e *UnknownTaskError) Unwrap() error { return ErrUnknownTask }

type classifiedError struct {
	class FailureClass
	err   error
}

func (e *classifiedError) Error() string { return e.err.Error() }
func (e *classifiedError) Unwrap() error { return e.err }

func classifyAs(class FailureClass, err error) error {
	if err == nil {
		err = fmt.Errorf("%s", class)
	}
	return &classifiedError{class: class, err: err}
}

func Retryable(err error) error  { return classifyAs(FailureClassTransientRetryable, err) }
func Permanent(err error) error  { return classifyAs(FailureClassPermanent, err) }
func Validation(err error) error { return classifyAs(FailureClassDecodeValidation, err) }
func Decode(err error) error     { return classifyAs(FailureClassDecodeValidation, err) }
func LeaseLost(err error) error  { return classifyAs(FailureClassLeaseLost, err) }
func Timeout(err error) error    { return classifyAs(FailureClassTimeout, err) }

func ClassifyFailure(ctx context.Context, err error) FailureClass {
	if err == nil {
		return ""
	}
	var typed *classifiedError
	if errors.As(err, &typed) {
		return typed.class
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return FailureClassTimeout
	}
	if errors.Is(err, ErrDeliveryExpired) || errors.Is(err, ErrStaleDelivery) {
		return FailureClassLeaseLost
	}
	return FailureClassTransientRetryable
}
