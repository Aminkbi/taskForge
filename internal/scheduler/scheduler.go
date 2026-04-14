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

type RecurringDispatcher interface {
	SyncDue(ctx context.Context, now time.Time) (int, error)
}

type LeaderElector interface {
	Ensure(ctx context.Context) (bool, error)
	Release(ctx context.Context) error
}

type Scheduler struct {
	mover         DueMover
	recurring     RecurringDispatcher
	elector       LeaderElector
	clock         clock.Clock
	logger        *slog.Logger
	interval      time.Duration
	renewInterval time.Duration
}

func New(
	mover DueMover,
	recurring RecurringDispatcher,
	elector LeaderElector,
	clk clock.Clock,
	logger *slog.Logger,
	interval time.Duration,
	renewInterval time.Duration,
) *Scheduler {
	return &Scheduler{
		mover:         mover,
		recurring:     recurring,
		elector:       elector,
		clock:         clk,
		logger:        logger,
		interval:      interval,
		renewInterval: renewInterval,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	workTicker := time.NewTicker(s.interval)
	defer workTicker.Stop()

	renewTicker := time.NewTicker(s.renewInterval)
	defer renewTicker.Stop()

	s.logger.Info(
		"scheduler loop started",
		"interval", s.interval,
		"renew_interval", s.renewInterval,
	)
	for {
		select {
		case <-ctx.Done():
			releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := s.elector.Release(releaseCtx); err != nil && !errors.Is(err, context.Canceled) {
				s.logger.Warn("scheduler leadership release failed", "error", err)
			}
			return nil
		case <-renewTicker.C:
			if _, err := s.elector.Ensure(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				s.logger.Error("scheduler leadership renewal failed", "error", err)
			}
		case <-workTicker.C:
			leader, err := s.elector.Ensure(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				s.logger.Error("scheduler leadership check failed", "error", err)
				continue
			}
			if !leader {
				continue
			}

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

			if s.recurring == nil {
				continue
			}

			dispatched, err := s.recurring.SyncDue(ctx, s.clock.Now())
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				s.logger.Error("scheduler recurring dispatch failed", "error", err)
				continue
			}
			if dispatched > 0 {
				s.logger.Info("scheduler dispatched recurring tasks", "count", dispatched)
			}
		}
	}
}
