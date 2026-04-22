package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/aminkbi/taskforge/internal/clock"
	"github.com/aminkbi/taskforge/internal/healthcheck"
	"github.com/aminkbi/taskforge/internal/observability"
)

type DueMover interface {
	MoveDue(ctx context.Context, fence LeadershipFence, now time.Time, limit int64) (int, error)
}

type RecurringDispatcher interface {
	SyncDue(ctx context.Context, fence LeadershipFence, now time.Time) (int, error)
}

type LeaderElector interface {
	Ensure(ctx context.Context) (LeadershipSnapshot, error)
	Release(ctx context.Context) error
	Demote(reason string)
}

type SafetySnapshot struct {
	StaleWriteRejections map[string]int64 `json:"stale_write_rejections"`
	ControlPlaneFailures map[string]int64 `json:"control_plane_failures"`
}

type Scheduler struct {
	mover         DueMover
	recurring     RecurringDispatcher
	elector       LeaderElector
	clock         clock.Clock
	logger        *slog.Logger
	metrics       *observability.Metrics
	interval      time.Duration
	renewInterval time.Duration
	LoopHealth    *healthcheck.Reporter

	safetyMu             sync.RWMutex
	staleWriteRejections map[string]int64
	controlPlaneFailures map[string]int64
}

func New(
	mover DueMover,
	recurring RecurringDispatcher,
	elector LeaderElector,
	clk clock.Clock,
	logger *slog.Logger,
	metrics *observability.Metrics,
	interval time.Duration,
	renewInterval time.Duration,
) *Scheduler {
	return &Scheduler{
		mover:                mover,
		recurring:            recurring,
		elector:              elector,
		clock:                clk,
		logger:               logger,
		metrics:              metrics,
		interval:             interval,
		renewInterval:        renewInterval,
		staleWriteRejections: make(map[string]int64),
		controlPlaneFailures: make(map[string]int64),
	}
}

func (s *Scheduler) SafetySnapshot() SafetySnapshot {
	s.safetyMu.RLock()
	defer s.safetyMu.RUnlock()

	return SafetySnapshot{
		StaleWriteRejections: cloneCounts(s.staleWriteRejections),
		ControlPlaneFailures: cloneCounts(s.controlPlaneFailures),
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
	if s.LoopHealth != nil {
		s.LoopHealth.MarkReady("scheduler loop healthy")
	}
	for {
		select {
		case <-ctx.Done():
			if s.LoopHealth != nil {
				s.LoopHealth.MarkNotReady("scheduler shutting down")
			}
			releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := s.elector.Release(releaseCtx); err != nil && !errors.Is(err, context.Canceled) {
				s.logger.Warn("scheduler leadership release failed", "error", err)
			}
			return nil
		case <-renewTicker.C:
			snapshot, err := s.elector.Ensure(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				s.recordFailure("leadership_renew", "error")
				if s.LoopHealth != nil {
					s.LoopHealth.MarkFailed(err.Error())
				}
				s.logger.Error("scheduler leadership renewal failed", "error", err)
				continue
			}
			if !snapshot.Leader && s.LoopHealth != nil {
				s.LoopHealth.MarkReady("scheduler standby healthy")
			}
		case <-workTicker.C:
			snapshot, err := s.elector.Ensure(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				s.recordFailure("leadership_check", "error")
				if s.LoopHealth != nil {
					s.LoopHealth.MarkFailed(err.Error())
				}
				s.logger.Error("scheduler leadership check failed", "error", err)
				continue
			}
			if !snapshot.Leader || !snapshot.Fence.Valid() {
				if s.LoopHealth != nil {
					s.LoopHealth.MarkReady("scheduler standby healthy")
				}
				continue
			}

			moved, err := s.mover.MoveDue(ctx, snapshot.Fence, s.clock.Now(), 100)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				if s.handleControlPlaneError(err) {
					continue
				}
				s.recordFailure("move_due", "error")
				if s.LoopHealth != nil {
					s.LoopHealth.MarkFailed(err.Error())
				}
				s.logger.Error("scheduler move due tasks failed", "error", err)
				continue
			}
			if moved > 0 {
				s.logger.Info("scheduler released delayed tasks", "count", moved, "epoch", snapshot.Epoch)
			}

			if s.recurring == nil {
				if s.LoopHealth != nil {
					s.LoopHealth.MarkReady("scheduler leader healthy")
				}
				continue
			}

			dispatched, err := s.recurring.SyncDue(ctx, snapshot.Fence, s.clock.Now())
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				if s.handleControlPlaneError(err) {
					continue
				}
				s.recordFailure("sync_recurring", "error")
				if s.LoopHealth != nil {
					s.LoopHealth.MarkFailed(err.Error())
				}
				s.logger.Error("scheduler recurring dispatch failed", "error", err)
				continue
			}
			if dispatched > 0 {
				s.logger.Info("scheduler dispatched recurring tasks", "count", dispatched, "epoch", snapshot.Epoch)
			}
			if s.LoopHealth != nil {
				s.LoopHealth.MarkReady("scheduler leader healthy")
			}
		}
	}
}

func (s *Scheduler) handleControlPlaneError(err error) bool {
	var staleErr *StaleLeadershipError
	if !errors.As(err, &staleErr) {
		return false
	}

	operation := "unknown"
	if staleErr.Operation != "" {
		operation = staleErr.Operation
	}
	s.recordStale(operation)
	s.elector.Demote("stale_write_rejected")
	if s.LoopHealth != nil {
		s.LoopHealth.MarkReady("scheduler standby healthy")
	}
	if s.logger != nil {
		s.logger.Warn("scheduler stale leadership rejected control-plane write", "operation", operation, "error", err)
	}
	return true
}

func (s *Scheduler) recordStale(operation string) {
	s.safetyMu.Lock()
	s.staleWriteRejections[operation]++
	s.safetyMu.Unlock()
	if s.metrics != nil {
		s.metrics.IncSchedulerStaleWriteRejection(operation)
	}
}

func (s *Scheduler) recordFailure(operation, reason string) {
	key := operation + ":" + reason
	s.safetyMu.Lock()
	s.controlPlaneFailures[key]++
	s.safetyMu.Unlock()
	if s.metrics != nil {
		s.metrics.IncSchedulerControlPlaneFailure(operation, reason)
	}
}

func cloneCounts(src map[string]int64) map[string]int64 {
	dst := make(map[string]int64, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}
