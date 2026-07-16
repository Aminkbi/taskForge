package taskforge

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type State string

const (
	StateQueued         State = "queued"
	StateLeased         State = "leased"
	StateRunning        State = "running"
	StateSucceeded      State = "succeeded"
	StateRetryScheduled State = "retry_scheduled"
	StateDeadLettered   State = "dead_lettered"
)

type TaskRecord struct {
	TaskID         string    `json:"task_id"`
	Name           string    `json:"name,omitempty"`
	Queue          string    `json:"queue,omitempty"`
	State          State     `json:"state"`
	LastError      string    `json:"last_error,omitempty"`
	CreatedAt      time.Time `json:"created_at,omitempty"`
	StartedAt      time.Time `json:"started_at,omitempty"`
	CompletedAt    time.Time `json:"completed_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
	DeliveryCount  int       `json:"delivery_count,omitempty"`
	LastDeliveryID string    `json:"last_delivery_id,omitempty"`
	LastLeaseOwner string    `json:"last_lease_owner,omitempty"`
	ResultPayload  []byte    `json:"result_payload,omitempty"`
}

type RetentionPolicy struct {
	SucceededState time.Duration
	FailedState    time.Duration
	ResultPayload  time.Duration
}

type StateStore interface {
	RecordQueued(ctx context.Context, task Task) error
	RecordDelivery(ctx context.Context, delivery Delivery, state State, resultPayload []byte) error
	Get(ctx context.Context, taskID string) (TaskRecord, error)
}

var ErrInvalidStateTransition = errors.New("taskforge: invalid state transition")

var allowedTransitions = map[State]map[State]struct{}{
	StateQueued: {StateLeased: {}},
	StateLeased: {StateQueued: {}, StateRunning: {}},
	StateRunning: {
		StateSucceeded: {}, StateRetryScheduled: {}, StateDeadLettered: {},
	},
}

func CanTransition(from, to State) bool {
	_, ok := allowedTransitions[from][to]
	return ok
}

func ValidateTransition(from, to State) error {
	if CanTransition(from, to) {
		return nil
	}
	return fmt.Errorf("%w: %s -> %s", ErrInvalidStateTransition, from, to)
}

// CompletesDelivery reports whether state ends the current delivery. A retry
// remains non-final for the logical task even though it completes its delivery.
func CompletesDelivery(state State) bool {
	return state == StateSucceeded || state == StateRetryScheduled || state == StateDeadLettered
}

func CompletesTask(state State) bool {
	return state == StateSucceeded || state == StateDeadLettered
}
