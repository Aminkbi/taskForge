package scheduler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aminkbi/taskforge/internal/clock"
)

func TestSchedulerStaleMoveDueDemotesAndSkipsRecurringMutation(t *testing.T) {
	t.Parallel()

	elector := &stubLeaderElector{
		snapshot: LeadershipSnapshot{
			Leader: true,
			Owner:  "scheduler-a",
			Epoch:  1,
			Fence: LeadershipFence{
				Owner: "scheduler-a",
				Epoch: 1,
				Token: "scheduler-a|1",
			},
		},
	}
	mover := &staleDueMover{err: NewStaleLeadershipError("move_due")}
	recurring := &countingRecurringDispatcher{}
	scheduler := New(
		mover,
		recurring,
		elector,
		clock.RealClock{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		time.Millisecond,
		time.Hour,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- scheduler.Run(ctx)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot := scheduler.SafetySnapshot()
		if snapshot.StaleWriteRejections["move_due"] == 1 {
			cancel()
			select {
			case err := <-errCh:
				if err != nil {
					t.Fatalf("Scheduler.Run() error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("scheduler did not stop before timeout")
			}
			if elector.demoteReason != "stale_write_rejected" {
				t.Fatalf("demote reason = %q, want stale_write_rejected", elector.demoteReason)
			}
			if recurring.calls != 0 {
				t.Fatalf("recurring calls = %d, want 0 after stale move due", recurring.calls)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	t.Fatal("scheduler did not record stale move_due rejection before timeout")
}

type stubLeaderElector struct {
	snapshot     LeadershipSnapshot
	demoteReason string
}

func (e *stubLeaderElector) Ensure(context.Context) (LeadershipSnapshot, error) {
	if e.demoteReason != "" {
		return LeadershipSnapshot{}, nil
	}
	return e.snapshot, nil
}

func (e *stubLeaderElector) Release(context.Context) error {
	return nil
}

func (e *stubLeaderElector) Demote(reason string) {
	e.demoteReason = reason
}

type staleDueMover struct {
	err error
}

func (m *staleDueMover) MoveDue(context.Context, LeadershipFence, time.Time, int64) (int, error) {
	if m.err == nil {
		return 0, errors.New("unexpected MoveDue call")
	}
	return 0, m.err
}

type countingRecurringDispatcher struct {
	calls int
}

func (d *countingRecurringDispatcher) SyncDue(context.Context, LeadershipFence, time.Time) (int, error) {
	d.calls++
	return 0, nil
}
