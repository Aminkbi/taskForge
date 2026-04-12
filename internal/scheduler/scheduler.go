package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/aminkbi/taskforge/internal/clock"
)

type DueMover interface {
	MoveDue(ctx context.Context, now time.Time, limit int64) (int, error)
}

type Scheduler struct {
	mover    DueMover
	clock    clock.Clock
	logger   *slog.Logger
	interval time.Duration
}

func New(mover DueMover, clk clock.Clock, logger *slog.Logger, interval time.Duration) *Scheduler {
	return &Scheduler{
		mover:    mover,
		clock:    clk,
		logger:   logger,
		interval: interval,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.logger.Info("scheduler loop started", "interval", s.interval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			moved, err := s.mover.MoveDue(ctx, s.clock.Now(), 100)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				s.logger.Error("scheduler move due tasks failed", "error", err)
				continue
			}
			if moved > 0 {
				s.logger.Info("scheduler released delayed tasks", "count", moved)
			}
		}
	}
}
