package taskforge

import (
	"time"

	"github.com/aminkbi/taskforge/internal/store"
	"github.com/aminkbi/taskforge/internal/tasks"
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

func CanTransition(from, to State) bool {
	return tasks.CanTransition(tasks.State(from), tasks.State(to))
}

func IsTerminal(state State) bool {
	return tasks.IsTerminal(tasks.State(state))
}

func taskRecordFromStore(record store.TaskRecord) TaskRecord {
	return TaskRecord{
		TaskID:         record.TaskID,
		Name:           record.Name,
		Queue:          record.Queue,
		State:          State(record.State),
		LastError:      record.LastError,
		CreatedAt:      record.CreatedAt,
		StartedAt:      record.StartedAt,
		CompletedAt:    record.CompletedAt,
		UpdatedAt:      record.UpdatedAt,
		DeliveryCount:  record.DeliveryCount,
		LastDeliveryID: record.LastDeliveryID,
		LastLeaseOwner: record.LastLeaseOwner,
		ResultPayload:  append([]byte(nil), record.ResultPayload...),
	}
}
