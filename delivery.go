package taskforge

import "time"

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
