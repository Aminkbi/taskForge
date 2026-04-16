package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/aminkbi/taskforge/internal/broker"
)

type MisfirePolicy string

const (
	MisfirePolicyCoalesce MisfirePolicy = "coalesce"
)

const (
	HeaderScheduledFor          = "taskforge_scheduled_for"
	HeaderReleasedAt            = "taskforge_released_at"
	HeaderReleaseLagMS          = "taskforge_release_lag_ms"
	HeaderScheduleID            = "taskforge_schedule_id"
	HeaderScheduleNominalAt     = "taskforge_schedule_nominal_at"
	HeaderScheduleDispatchedAt  = "taskforge_schedule_dispatched_at"
	HeaderScheduleMisfirePolicy = "taskforge_schedule_misfire_policy"
	HeaderScheduleMissedRuns    = "taskforge_schedule_missed_runs"
)

type ScheduleDefinition struct {
	ID            string
	Interval      time.Duration
	Queue         string
	FairnessKey   string
	TaskName      string
	Payload       json.RawMessage
	Headers       map[string]string
	Enabled       bool
	MisfirePolicy MisfirePolicy
	StartAt       *time.Time
}

type ScheduleState struct {
	NextRunAt        time.Time     `json:"next_run_at"`
	LastDispatchedAt time.Time     `json:"last_dispatched_at,omitempty"`
	DefinitionHash   string        `json:"definition_hash"`
	MisfirePolicy    MisfirePolicy `json:"misfire_policy"`
}

type ScheduleStateStore interface {
	ReconcileConfigured(ctx context.Context, schedules []ScheduleDefinition, now time.Time) error
	DueScheduleIDs(ctx context.Context, now time.Time, limit int64) ([]string, error)
	LoadStates(ctx context.Context, scheduleIDs []string) (map[string]ScheduleState, error)
	SaveIndexed(ctx context.Context, scheduleID string, state ScheduleState) error
	RemoveSchedule(ctx context.Context, scheduleID string) error
	RemoveFromDueIndex(ctx context.Context, scheduleID string) error
}

type RecurringService struct {
	publisher    broker.Broker
	store        ScheduleStateStore
	schedules    []ScheduleDefinition
	scheduleByID map[string]ScheduleDefinition
	logger       *slog.Logger
	idFunc       func() string
	reconciled   bool
}

const recurringDueBatchLimit int64 = 100

func NewRecurringService(
	publisher broker.Broker,
	store ScheduleStateStore,
	schedules []ScheduleDefinition,
	logger *slog.Logger,
) *RecurringService {
	scheduleByID := make(map[string]ScheduleDefinition, len(schedules))
	for _, schedule := range schedules {
		scheduleByID[schedule.ID] = schedule
	}

	return &RecurringService{
		publisher:    publisher,
		store:        store,
		schedules:    slices.Clone(schedules),
		scheduleByID: scheduleByID,
		logger:       logger,
		idFunc: func() string {
			return uuid.NewString()
		},
	}
}

func (s *RecurringService) SyncDue(ctx context.Context, now time.Time) (int, error) {
	if !s.reconciled {
		if err := s.store.ReconcileConfigured(ctx, s.schedules, now); err != nil {
			return 0, fmt.Errorf("reconcile recurring schedules: %w", err)
		}
		s.reconciled = true
	}

	dispatched := 0
	for {
		dueIDs, err := s.store.DueScheduleIDs(ctx, now, recurringDueBatchLimit)
		if err != nil {
			return dispatched, fmt.Errorf("query due recurring schedules: %w", err)
		}
		if len(dueIDs) == 0 {
			return dispatched, nil
		}

		states, err := s.store.LoadStates(ctx, dueIDs)
		if err != nil {
			return dispatched, fmt.Errorf("load due recurring schedule states: %w", err)
		}

		for _, scheduleID := range dueIDs {
			schedule, configured := s.scheduleByID[scheduleID]
			if !configured {
				if err := s.store.RemoveSchedule(ctx, scheduleID); err != nil {
					return dispatched, fmt.Errorf("cleanup unknown recurring schedule %s: %w", scheduleID, err)
				}
				continue
			}

			if !schedule.Enabled {
				if err := s.store.RemoveFromDueIndex(ctx, scheduleID); err != nil {
					return dispatched, fmt.Errorf("remove disabled recurring schedule %s from due index: %w", scheduleID, err)
				}
				continue
			}

			definitionHash := hashScheduleDefinition(schedule)
			state, exists := states[scheduleID]
			if !exists || state.DefinitionHash != definitionHash || state.NextRunAt.IsZero() {
				state = initialScheduleState(schedule, now, definitionHash)
				if err := s.store.SaveIndexed(ctx, scheduleID, state); err != nil {
					return dispatched, fmt.Errorf("save initial schedule %s state: %w", scheduleID, err)
				}
			}

			if state.NextRunAt.After(now) {
				continue
			}

			nominalAt, nextRunAt, missedRuns := coalesceNextRun(state.NextRunAt, schedule.Interval, now)
			task := broker.TaskMessage{
				ID:          s.idFunc(),
				Name:        schedule.TaskName,
				Queue:       schedule.Queue,
				FairnessKey: schedule.FairnessKey,
				Payload:     slices.Clone(schedule.Payload),
				Headers:     cloneHeaders(schedule.Headers),
				CreatedAt:   now.UTC(),
			}
			task.Headers[HeaderScheduleID] = schedule.ID
			task.Headers[HeaderScheduleNominalAt] = nominalAt.UTC().Format(time.RFC3339Nano)
			task.Headers[HeaderScheduleDispatchedAt] = now.UTC().Format(time.RFC3339Nano)
			task.Headers[HeaderScheduleMisfirePolicy] = string(schedule.MisfirePolicy)
			task.Headers[HeaderScheduleMissedRuns] = fmt.Sprintf("%d", missedRuns)

			if _, err := s.publisher.Publish(ctx, task, broker.PublishOptions{Source: broker.PublishSourceRecurring}); err != nil {
				return dispatched, fmt.Errorf("publish recurring task for %s: %w", schedule.ID, err)
			}

			state.NextRunAt = nextRunAt.UTC()
			state.LastDispatchedAt = now.UTC()
			state.DefinitionHash = definitionHash
			state.MisfirePolicy = schedule.MisfirePolicy
			if err := s.store.SaveIndexed(ctx, schedule.ID, state); err != nil {
				return dispatched, fmt.Errorf("save dispatched schedule %s state: %w", schedule.ID, err)
			}

			dispatched++
			if s.logger != nil {
				s.logger.Info(
					"dispatched recurring task",
					"schedule_id", schedule.ID,
					"nominal_at", nominalAt.UTC(),
					"dispatched_at", now.UTC(),
					"next_run_at", state.NextRunAt,
					"missed_runs", missedRuns,
				)
			}
		}
	}
}

func initialScheduleState(schedule ScheduleDefinition, now time.Time, definitionHash string) ScheduleState {
	nextRunAt := now.UTC().Add(schedule.Interval)
	if schedule.StartAt != nil {
		nextRunAt = schedule.StartAt.UTC()
	}
	return ScheduleState{
		NextRunAt:      nextRunAt.UTC(),
		DefinitionHash: definitionHash,
		MisfirePolicy:  schedule.MisfirePolicy,
	}
}

func coalesceNextRun(nextRunAt time.Time, interval time.Duration, now time.Time) (time.Time, time.Time, int) {
	nominalAt := nextRunAt.UTC()
	missedRuns := 0
	for !nextRunAt.After(now.UTC()) {
		nextRunAt = nextRunAt.Add(interval)
		if !nextRunAt.After(now.UTC()) {
			missedRuns++
		}
	}
	return nominalAt, nextRunAt.UTC(), missedRuns
}

func cloneHeaders(headers map[string]string) map[string]string {
	cloned := make(map[string]string, len(headers)+4)
	for key, value := range headers {
		cloned[key] = value
	}
	return cloned
}

func hashScheduleDefinition(schedule ScheduleDefinition) string {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(schedule.ID))
	_, _ = hasher.Write([]byte{'\n'})
	_, _ = hasher.Write([]byte(schedule.Interval.String()))
	_, _ = hasher.Write([]byte{'\n'})
	_, _ = hasher.Write([]byte(schedule.Queue))
	_, _ = hasher.Write([]byte{'\n'})
	_, _ = hasher.Write([]byte(schedule.FairnessKey))
	_, _ = hasher.Write([]byte{'\n'})
	_, _ = hasher.Write([]byte(schedule.TaskName))
	_, _ = hasher.Write([]byte{'\n'})
	_, _ = hasher.Write([]byte(schedule.MisfirePolicy))
	_, _ = hasher.Write([]byte{'\n'})
	if schedule.StartAt != nil {
		_, _ = hasher.Write([]byte(schedule.StartAt.UTC().Format(time.RFC3339Nano)))
	}
	_, _ = hasher.Write([]byte{'\n'})
	_, _ = hasher.Write(schedule.Payload)

	keys := make([]string, 0, len(schedule.Headers))
	for key := range schedule.Headers {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		_, _ = hasher.Write([]byte{'\n'})
		_, _ = hasher.Write([]byte(key))
		_, _ = hasher.Write([]byte{'='})
		_, _ = hasher.Write([]byte(schedule.Headers[key]))
	}

	return hex.EncodeToString(hasher.Sum(nil))
}
