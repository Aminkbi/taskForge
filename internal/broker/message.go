package broker

import "time"

const (
	HeaderAdmissionDecision      = "taskforge_admission_decision"
	HeaderAdmissionReason        = "taskforge_admission_reason"
	HeaderAdmissionSource        = "taskforge_admission_source"
	HeaderAdmissionDeferredUntil = "taskforge_admission_deferred_until"
	HeaderAdmissionEvaluatedAt   = "taskforge_admission_evaluated_at"
)

// TaskMessage carries the logical task payload. The ID identifies the logical
// task, not a single broker delivery attempt, so handlers must be idempotent.
type TaskMessage struct {
	ID                string            `json:"id"`
	Name              string            `json:"name"`
	Queue             string            `json:"queue"`
	FairnessKey       string            `json:"fairness_key,omitempty"`
	Payload           []byte            `json:"payload"`
	Headers           map[string]string `json:"headers,omitempty"`
	Attempt           int               `json:"attempt"`
	MaxAttempts       int               `json:"max_attempts"`
	VisibilityTimeout time.Duration     `json:"visibility_timeout"`
	ETA               *time.Time        `json:"eta,omitempty"`
	Timeout           *time.Duration    `json:"timeout,omitempty"`
	IdempotencyKey    string            `json:"idempotency_key,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`
}

// Delivery represents one leased execution attempt for a logical task.
// Delivery ownership, not just task identity, controls ack/nack/extend.
type Delivery struct {
	Message   TaskMessage       `json:"message"`
	Execution ExecutionMetadata `json:"execution"`
}

// ExecutionMetadata captures delivery identity and operator-visible state for
// one broker delivery attempt.
type ExecutionMetadata struct {
	TaskID          string    `json:"task_id"`
	DeliveryID      string    `json:"delivery_id"`
	DeliveryCount   int       `json:"delivery_count"`
	FirstEnqueuedAt time.Time `json:"first_enqueued_at"`
	LeasedAt        time.Time `json:"leased_at,omitempty"`
	LeaseExpiresAt  time.Time `json:"lease_expires_at,omitempty"`
	LeaseOwner      string    `json:"lease_owner,omitempty"`
	LastError       string    `json:"last_error,omitempty"`
	State           string    `json:"state"`
}

func (d Delivery) WithState(state string) Delivery {
	d.Execution.State = state
	return d
}

func (d Delivery) WithLastError(lastError string) Delivery {
	d.Execution.LastError = lastError
	return d
}
