package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/dlq"
)

type classifiedError struct {
	class dlq.FailureClass
	err   error
}

func (e *classifiedError) Error() string {
	return e.err.Error()
}

func (e *classifiedError) Unwrap() error {
	return e.err
}

func Retryable(err error) error {
	return classifyAs(dlq.FailureClassTransientRetryable, err)
}

func Permanent(err error) error {
	return classifyAs(dlq.FailureClassPermanent, err)
}

func Validation(err error) error {
	return classifyAs(dlq.FailureClassDecodeValidation, err)
}

func Decode(err error) error {
	return classifyAs(dlq.FailureClassDecodeValidation, err)
}

func LeaseLost(err error) error {
	return classifyAs(dlq.FailureClassLeaseLost, err)
}

func Timeout(err error) error {
	return classifyAs(dlq.FailureClassTimeout, err)
}

func classifyAs(class dlq.FailureClass, err error) error {
	if err == nil {
		err = fmt.Errorf("%s", class)
	}
	return &classifiedError{class: class, err: err}
}

func classifyFailure(execCtx context.Context, err error) dlq.FailureClass {
	if err == nil {
		return ""
	}

	var typed *classifiedError
	if errors.As(err, &typed) {
		return typed.class
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(execCtx.Err(), context.DeadlineExceeded) {
		return dlq.FailureClassTimeout
	}
	if errors.Is(err, broker.ErrDeliveryExpired) || errors.Is(err, broker.ErrStaleDelivery) {
		return dlq.FailureClassLeaseLost
	}

	return dlq.FailureClassTransientRetryable
}
