package tasks

import (
	"errors"
	"fmt"
)

type State string

const (
	StateQueued         State = "queued"
	StateLeased         State = "leased"
	StateRunning        State = "running"
	StateSucceeded      State = "succeeded"
	StateRetryScheduled State = "retry_scheduled"
	StateDeadLettered   State = "dead_lettered"
)

var ErrInvalidStateTransition = errors.New("tasks: invalid state transition")

var allowedTransitions = map[State]map[State]struct{}{
	StateQueued: {
		StateLeased: {},
	},
	StateLeased: {
		StateQueued:  {},
		StateRunning: {},
	},
	StateRunning: {
		StateSucceeded:      {},
		StateRetryScheduled: {},
		StateDeadLettered:   {},
	},
}

func CanTransition(from, to State) bool {
	transitions, ok := allowedTransitions[from]
	if !ok {
		return false
	}
	_, ok = transitions[to]
	return ok
}

func ValidateTransition(from, to State) error {
	if CanTransition(from, to) {
		return nil
	}
	return fmt.Errorf("%w: %s -> %s", ErrInvalidStateTransition, from, to)
}

func IsTerminal(state State) bool {
	return state == StateSucceeded || state == StateRetryScheduled || state == StateDeadLettered
}
