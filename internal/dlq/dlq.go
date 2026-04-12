package dlq

import (
	"context"

	"github.com/aminkbi/taskforge/internal/broker"
)

type Publisher interface {
	PublishDeadLetter(ctx context.Context, msg broker.TaskMessage, reason string) error
}

type Dispatcher struct {
	Broker broker.Broker
}

func (d Dispatcher) PublishDeadLetter(ctx context.Context, msg broker.TaskMessage, reason string) error {
	if msg.Headers == nil {
		msg.Headers = make(map[string]string, 1)
	}
	msg.Headers["dead_letter_reason"] = reason
	msg.Queue = "dlq." + msg.Queue
	msg.ETA = nil
	return d.Broker.Publish(ctx, msg)
}
