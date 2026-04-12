package runtime

import (
	"context"

	"github.com/aminkbi/taskforge/internal/broker"
)

type Handler interface {
	HandleTask(ctx context.Context, msg broker.TaskMessage) error
}

type HandlerFunc func(context.Context, broker.TaskMessage) error

func (f HandlerFunc) HandleTask(ctx context.Context, msg broker.TaskMessage) error {
	return f(ctx, msg)
}
