package shutdown

import (
	"context"
	"os/signal"
	"syscall"
)

func NotifyContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
}
