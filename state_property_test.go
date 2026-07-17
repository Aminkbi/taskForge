package taskforge

import (
	"testing"
	"testing/quick"
)

func TestAcceptedTransitionsNeverLeaveATerminalState(t *testing.T) {
	if err := quick.Check(func(steps []uint8) bool {
		state := StateQueued
		for _, step := range steps {
			candidate := []State{
				StateQueued,
				StateLeased,
				StateRunning,
				StateSucceeded,
				StateRetryScheduled,
				StateDeadLettered,
			}[int(step)%6]
			if ValidateTransition(state, candidate) == nil {
				if CompletesTask(state) {
					return false
				}
				state = candidate
			}
		}
		return true
	}, nil); err != nil {
		t.Fatalf("terminal state monotonicity property failed: %v", err)
	}
}

func FuzzValidateTransition(f *testing.F) {
	f.Add(string(StateQueued), string(StateLeased))
	f.Add(string(StateRunning), string(StateSucceeded))
	f.Add(string(StateSucceeded), string(StateQueued))

	f.Fuzz(func(t *testing.T, from, to string) {
		fromState, toState := State(from), State(to)
		err := ValidateTransition(fromState, toState)
		if CompletesTask(fromState) && err == nil {
			t.Fatalf("terminal state %q accepted transition to %q", fromState, toState)
		}
	})
}
