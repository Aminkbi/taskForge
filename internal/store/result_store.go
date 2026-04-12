package store

import (
	"context"

	"github.com/aminkbi/taskforge/internal/tasks"
)

type ResultStore interface {
	Save(ctx context.Context, taskID string, state tasks.State, payload []byte) error
}
