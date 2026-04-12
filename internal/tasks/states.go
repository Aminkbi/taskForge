package tasks

type State string

const (
	StateQueued         State = "queued"
	StateLeased         State = "leased"
	StateRunning        State = "running"
	StateSucceeded      State = "succeeded"
	StateFailed         State = "failed"
	StateRetryScheduled State = "retry_scheduled"
	StateDeadLettered   State = "dead_lettered"
)

func IsTerminal(state State) bool {
	return state == StateSucceeded || state == StateFailed || state == StateDeadLettered
}
