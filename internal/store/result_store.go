package store

import (
	"context"
	"errors"
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
	"github.com/aminkbi/taskforge/internal/tasks"
)

var ErrTaskNotFound = errors.New("store: task not found")

type RetentionPolicy struct {
	SucceededState time.Duration
	FailedState    time.Duration
	ResultPayload  time.Duration
}

type TaskRecord struct {
	TaskID         string      `json:"task_id"`
	Name           string      `json:"name,omitempty"`
	Queue          string      `json:"queue,omitempty"`
	State          tasks.State `json:"state"`
	LastError      string      `json:"last_error,omitempty"`
	CreatedAt      time.Time   `json:"created_at,omitempty"`
	StartedAt      time.Time   `json:"started_at,omitempty"`
	CompletedAt    time.Time   `json:"completed_at,omitempty"`
	UpdatedAt      time.Time   `json:"updated_at"`
	DeliveryCount  int         `json:"delivery_count,omitempty"`
	LastDeliveryID string      `json:"last_delivery_id,omitempty"`
	LastLeaseOwner string      `json:"last_lease_owner,omitempty"`
	ResultPayload  []byte      `json:"result_payload,omitempty"`
}

type StateStore interface {
	RecordQueued(ctx context.Context, msg broker.TaskMessage) error
	RecordDelivery(ctx context.Context, delivery broker.Delivery, state tasks.State, resultPayload []byte) error
	Get(ctx context.Context, taskID string) (TaskRecord, error)
}

type ResultStore interface {
	Save(ctx context.Context, taskID string, state tasks.State, payload []byte) error
}
