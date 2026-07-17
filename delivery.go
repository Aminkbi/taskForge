package taskforge

import (
	"strconv"
	"strings"
	"time"
)

type Delivery struct {
	Message   Task              `json:"message"`
	Execution ExecutionMetadata `json:"execution"`
}

type ExecutionMetadata struct {
	TaskID          string    `json:"task_id"`
	DeliveryID      string    `json:"delivery_id"`
	DeliveryCount   int       `json:"delivery_count"`
	FirstEnqueuedAt time.Time `json:"first_enqueued_at"`
	LeasedAt        time.Time `json:"leased_at,omitempty"`
	LeaseExpiresAt  time.Time `json:"lease_expires_at,omitempty"`
	LeaseOwner      string    `json:"lease_owner,omitempty"`
	LastError       string    `json:"last_error,omitempty"`
	State           State     `json:"state"`
}

func (d Delivery) WithLastError(lastError string) Delivery {
	d.Execution.LastError = lastError
	return d
}

// OwnershipKey identifies one broker entry across queues and fairness streams.
// Redis stream IDs are unique only within a stream, so delivery-derived
// deduplication and lease keys must not use DeliveryID alone. The lease owner is
// deliberately excluded: a reclaimed owner must deduplicate work already
// published by the previous owner for the same broker entry.
func (d Delivery) OwnershipKey() string {
	taskID := d.Message.ID
	if taskID == "" {
		taskID = d.Execution.TaskID
	}
	parts := [...]string{
		EffectiveQueue(d.Message),
		d.Message.FairnessKey,
		taskID,
		d.Execution.DeliveryID,
	}
	var key strings.Builder
	for _, part := range parts {
		key.WriteString(strconv.Itoa(len(part)))
		key.WriteByte(':')
		key.WriteString(part)
	}
	return key.String()
}
