package runtime

type TaskTypeLimiter struct {
	limits map[string]chan struct{}
}

func NewTaskTypeLimiter(limits map[string]int) *TaskTypeLimiter {
	if len(limits) == 0 {
		return nil
	}

	sem := make(map[string]chan struct{}, len(limits))
	for taskName, limit := range limits {
		if limit < 1 {
			continue
		}
		sem[taskName] = make(chan struct{}, limit)
	}
	if len(sem) == 0 {
		return nil
	}
	return &TaskTypeLimiter{limits: sem}
}

func tryAcquireTaskSlot(limiter *TaskTypeLimiter, taskName string) (func(), bool) {
	if limiter == nil {
		return func() {}, true
	}

	sem, ok := limiter.limits[taskName]
	if !ok {
		return func() {}, true
	}

	select {
	case sem <- struct{}{}:
		return func() {
			select {
			case <-sem:
			default:
			}
		}, true
	default:
		return nil, false
	}
}
