package taskforge

import (
	"context"
	"time"

	"github.com/aminkbi/taskforge/internal/dlq"
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

func (b *RedisBroker) ListDeadLetters(ctx context.Context, queue string, limit int64) ([]DeadLetterEntry, error) {
	entries, err := b.deadLetter.List(ctx, queue, limit)
	if err != nil {
		return nil, err
	}
	result := make([]DeadLetterEntry, 0, len(entries))
	for _, entry := range entries {
		result = append(result, deadLetterEntryFromInternal(entry))
	}
	return result, nil
}

func (b *RedisBroker) ReplayDeadLetter(ctx context.Context, queue, entryID string) error {
	return b.deadLetter.Replay(ctx, queue, entryID)
}

func (b *RedisBroker) ReplayDeadLetters(ctx context.Context, queue string, limit int64) (int, error) {
	return b.deadLetter.ReplayBatch(ctx, queue, limit)
}

func (b *RedisBroker) DiscardDeadLetter(ctx context.Context, queue, entryID, reason string) error {
	return b.deadLetter.Discard(ctx, queue, entryID, reason)
}

func deadLetterEntryFromInternal(entry dlq.Entry) DeadLetterEntry {
	return DeadLetterEntry{
		ID:       entry.ID,
		Queue:    entry.Queue,
		Envelope: deadLetterEnvelopeFromInternal(entry.Envelope),
	}
}

func deadLetterEnvelopeFromInternal(envelope dlq.Envelope) DeadLetterEnvelope {
	return DeadLetterEnvelope{
		OriginalTask:     taskFromBrokerMessage(envelope.OriginalTask),
		FailureClass:     FailureClass(envelope.FailureClass),
		LastError:        envelope.LastError,
		DeliveryCount:    envelope.DeliveryCount,
		FirstEnqueuedAt:  envelope.FirstEnqueuedAt,
		LastFailureAt:    envelope.LastFailureAt,
		WorkerIdentity:   envelope.WorkerIdentity,
		DeliveryID:       envelope.DeliveryID,
		TraceID:          envelope.TraceID,
		OriginalQueue:    envelope.OriginalQueue,
		OriginalTaskName: envelope.OriginalTaskName,
	}
}
