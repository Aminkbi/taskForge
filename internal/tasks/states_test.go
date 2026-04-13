package tasks

import (
	"errors"
	"testing"
)

func TestValidateTransition(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		from    State
		to      State
		wantErr bool
	}{
		{name: "queued to leased", from: StateQueued, to: StateLeased},
		{name: "leased to running", from: StateLeased, to: StateRunning},
		{name: "leased back to queued", from: StateLeased, to: StateQueued},
		{name: "running to succeeded", from: StateRunning, to: StateSucceeded},
		{name: "running to retry scheduled", from: StateRunning, to: StateRetryScheduled},
		{name: "running to dead lettered", from: StateRunning, to: StateDeadLettered},
		{name: "queued to running rejected", from: StateQueued, to: StateRunning, wantErr: true},
		{name: "running to queued rejected", from: StateRunning, to: StateQueued, wantErr: true},
		{name: "succeeded to leased rejected", from: StateSucceeded, to: StateLeased, wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTransition(tc.from, tc.to)
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidStateTransition) {
					t.Fatalf("ValidateTransition(%q, %q) error = %v, want %v", tc.from, tc.to, err, ErrInvalidStateTransition)
				}
				return
			}
			if err != nil {
				t.Fatalf("ValidateTransition(%q, %q) error = %v", tc.from, tc.to, err)
			}
		})
	}
}

func TestIsTerminal(t *testing.T) {
	t.Parallel()

	if IsTerminal(StateQueued) {
		t.Fatalf("IsTerminal(%q) = true, want false", StateQueued)
	}
	if !IsTerminal(StateSucceeded) {
		t.Fatalf("IsTerminal(%q) = false, want true", StateSucceeded)
	}
	if !IsTerminal(StateRetryScheduled) {
		t.Fatalf("IsTerminal(%q) = false, want true", StateRetryScheduled)
	}
	if !IsTerminal(StateDeadLettered) {
		t.Fatalf("IsTerminal(%q) = false, want true", StateDeadLettered)
	}
}
