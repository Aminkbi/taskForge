package taskforge

import (
	"context"
	"fmt"
	"sync"

	"github.com/aminkbi/taskforge/internal/broker"
)

type Handler interface {
	HandleTask(ctx context.Context, task Task) error
}

type HandlerFunc func(context.Context, Task) error

func (f HandlerFunc) HandleTask(ctx context.Context, task Task) error {
	return f(ctx, task)
}

type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

func (r *Registry) Register(name string, handler Handler) error {
	if name == "" {
		return fmt.Errorf("register task: missing name")
	}
	if handler == nil {
		return fmt.Errorf("register task %q: missing handler", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.handlers == nil {
		r.handlers = make(map[string]Handler)
	}
	r.handlers[name] = handler
	return nil
}

func (r *Registry) RegisterFunc(name string, handler func(context.Context, Task) error) error {
	if handler == nil {
		return fmt.Errorf("register task %q: missing handler", name)
	}
	return r.Register(name, HandlerFunc(handler))
}

func (r *Registry) HandleTask(ctx context.Context, task Task) error {
	r.mu.RLock()
	handler := r.handlers[task.Name]
	r.mu.RUnlock()
	if handler == nil {
		return Validation(&UnknownTaskError{Name: task.Name})
	}
	return handler.HandleTask(ctx, task)
}

type runtimeHandler struct {
	handler Handler
}

func (h runtimeHandler) HandleTask(ctx context.Context, msg broker.TaskMessage) error {
	return h.handler.HandleTask(ctx, taskFromBrokerMessage(msg))
}
