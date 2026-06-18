package taskforge

import (
	"time"

	"github.com/aminkbi/taskforge/internal/broker"
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
	State           string    `json:"state"`
}

func (d Delivery) toBrokerDelivery() broker.Delivery {
	return broker.Delivery{
		Message: d.Message.toBrokerMessage(),
		Execution: broker.ExecutionMetadata{
			TaskID:          d.Execution.TaskID,
			DeliveryID:      d.Execution.DeliveryID,
			DeliveryCount:   d.Execution.DeliveryCount,
			FirstEnqueuedAt: d.Execution.FirstEnqueuedAt,
			LeasedAt:        d.Execution.LeasedAt,
			LeaseExpiresAt:  d.Execution.LeaseExpiresAt,
			LeaseOwner:      d.Execution.LeaseOwner,
			LastError:       d.Execution.LastError,
			State:           d.Execution.State,
		},
	}
}

func deliveryFromBroker(delivery broker.Delivery) Delivery {
	return Delivery{
		Message: taskFromBrokerMessage(delivery.Message),
		Execution: ExecutionMetadata{
			TaskID:          delivery.Execution.TaskID,
			DeliveryID:      delivery.Execution.DeliveryID,
			DeliveryCount:   delivery.Execution.DeliveryCount,
			FirstEnqueuedAt: delivery.Execution.FirstEnqueuedAt,
			LeasedAt:        delivery.Execution.LeasedAt,
			LeaseExpiresAt:  delivery.Execution.LeaseExpiresAt,
			LeaseOwner:      delivery.Execution.LeaseOwner,
			LastError:       delivery.Execution.LastError,
			State:           delivery.Execution.State,
		},
	}
}
