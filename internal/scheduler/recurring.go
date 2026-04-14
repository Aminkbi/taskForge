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
	TaskName      string
	Payload       json.RawMessage
	Headers       map[string]string
	Enabled       bool
	MisfirePolicy MisfirePolicy
	StartAt       *time.Time
}

type ScheduleState struct {
	NextRunAt        time.Time `json:"next_run_at"`
	LastDispatchedAt time.Time `json:"last_dispatched_at,omitempty"`
	DefinitionHash   string    `json:"definition_hash"`
}

type ScheduleStateStore interface {
	Load(ctx context.Context, scheduleID string) (ScheduleState, bool, error)
	Save(ctx context.Context, scheduleID string, state ScheduleState) error
}

type RecurringService struct {
	publisher broker.Broker
	store     ScheduleStateStore
	schedules []ScheduleDefinition
	logger    *slog.Logger
	idFunc    func() string
}

func NewRecurringService(
	publisher broker.Broker,
	store ScheduleStateStore,
	schedules []ScheduleDefinition,
	logger *slog.Logger,
) *RecurringService {
	return &RecurringService{
		publisher: publisher,
		store:     store,
		schedules: slices.Clone(schedules),
		logger:    logger,
		idFunc: func() string {
			return uuid.NewString()
		},
	}
}

func (s *RecurringService) SyncDue(ctx context.Context, now time.Time) (int, error) {
	dispatched := 0
	for _, schedule := range s.schedules {
		if !schedule.Enabled {
			continue
		}

		state, exists, err := s.store.Load(ctx, schedule.ID)
		if err != nil {
			return dispatched, fmt.Errorf("load schedule %s state: %w", schedule.ID, err)
		}

		definitionHash := hashScheduleDefinition(schedule)
		if !exists || state.DefinitionHash != definitionHash || state.NextRunAt.IsZero() {
			state = initialScheduleState(schedule, now, definitionHash)
			if err := s.store.Save(ctx, schedule.ID, state); err != nil {
				return dispatched, fmt.Errorf("save initial schedule %s state: %w", schedule.ID, err)
			}
		}

		if state.NextRunAt.After(now) {
			continue
		}

		nominalAt, nextRunAt, missedRuns := coalesceNextRun(state.NextRunAt, schedule.Interval, now)
		task := broker.TaskMessage{
			ID:        s.idFunc(),
			Name:      schedule.TaskName,
			Queue:     schedule.Queue,
			Payload:   slices.Clone(schedule.Payload),
			Headers:   cloneHeaders(schedule.Headers),
			CreatedAt: now.UTC(),
		}
		task.Headers[HeaderScheduleID] = schedule.ID
		task.Headers[HeaderScheduleNominalAt] = nominalAt.UTC().Format(time.RFC3339Nano)
		task.Headers[HeaderScheduleDispatchedAt] = now.UTC().Format(time.RFC3339Nano)
		task.Headers[HeaderScheduleMisfirePolicy] = string(schedule.MisfirePolicy)
		task.Headers[HeaderScheduleMissedRuns] = fmt.Sprintf("%d", missedRuns)

		if err := s.publisher.Publish(ctx, task); err != nil {
			return dispatched, fmt.Errorf("publish recurring task for %s: %w", schedule.ID, err)
		}

		state.NextRunAt = nextRunAt.UTC()
		state.LastDispatchedAt = now.UTC()
		state.DefinitionHash = definitionHash
		if err := s.store.Save(ctx, schedule.ID, state); err != nil {
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

	return dispatched, nil
}

func initialScheduleState(schedule ScheduleDefinition, now time.Time, definitionHash string) ScheduleState {
	nextRunAt := now.UTC().Add(schedule.Interval)
	if schedule.StartAt != nil {
		nextRunAt = schedule.StartAt.UTC()
	}
	return ScheduleState{
		NextRunAt:      nextRunAt,
		DefinitionHash: definitionHash,
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
