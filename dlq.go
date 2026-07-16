package taskforge

import (
	"context"
	"strings"
	"time"
)

type FailureClass string

const (
	FailureClassTransientRetryable FailureClass = "transient_retryable"
	FailureClassPermanent          FailureClass = "permanent"
	FailureClassLeaseLost          FailureClass = "lease_lost"
	FailureClassTimeout            FailureClass = "timeout"
	FailureClassDecodeValidation   FailureClass = "decode_or_validation"
	FailureClassOverloaded         FailureClass = "overloaded"
)

type DeadLetterEnvelope struct {
	OriginalTask     Task         `json:"original_task"`
	FailureClass     FailureClass `json:"failure_class"`
	LastError        string       `json:"last_error"`
	DeliveryCount    int          `json:"delivery_count"`
	FirstEnqueuedAt  time.Time    `json:"first_enqueued_at"`
	LastFailureAt    time.Time    `json:"last_failure_at"`
	WorkerIdentity   string       `json:"worker_identity"`
	DeliveryID       string       `json:"delivery_id"`
	TraceID          string       `json:"trace_id,omitempty"`
	OriginalQueue    string       `json:"original_queue"`
	OriginalTaskName string       `json:"original_task_name"`
}

type DeadLetterEntry struct {
	ID       string             `json:"id"`
	Queue    string             `json:"queue"`
	Envelope DeadLetterEnvelope `json:"envelope"`
}

type DeadLetterPublisher interface {
	PublishDeadLetter(ctx context.Context, envelope DeadLetterEnvelope) error
}

func NewDeadLetterEnvelope(delivery Delivery, class FailureClass, lastError string, failedAt time.Time) DeadLetterEnvelope {
	return DeadLetterEnvelope{
		OriginalTask:     delivery.Message.Clone(),
		FailureClass:     class,
		LastError:        lastError,
		DeliveryCount:    delivery.Execution.DeliveryCount,
		FirstEnqueuedAt:  delivery.Execution.FirstEnqueuedAt,
		LastFailureAt:    failedAt.UTC(),
		WorkerIdentity:   delivery.Execution.LeaseOwner,
		DeliveryID:       delivery.Execution.DeliveryID,
		TraceID:          traceIDFromHeaders(delivery.Message.Headers),
		OriginalQueue:    delivery.Message.Queue,
		OriginalTaskName: delivery.Message.Name,
	}
}

func traceIDFromHeaders(headers map[string]string) string {
	if traceID := strings.TrimSpace(headers["trace_id"]); traceID != "" {
		return traceID
	}
	parts := strings.Split(strings.TrimSpace(headers["traceparent"]), "-")
	if len(parts) < 2 {
		return ""
	}
	return parts[1]
}
