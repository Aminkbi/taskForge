package healthcheck

import (
	"sync"
	"time"
)

type Snapshot struct {
	Ready     bool
	Status    string
	Detail    string
	UpdatedAt time.Time
}

type Reporter struct {
	mu      sync.RWMutex
	ready   bool
	status  string
	detail  string
	updated time.Time
}

func NewReporter(status, detail string) *Reporter {
	r := &Reporter{}
	r.Set(false, status, detail)
	return r
}

func (r *Reporter) Set(ready bool, status, detail string) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.ready = ready
	r.status = status
	r.detail = detail
	r.updated = time.Now().UTC()
}

func (r *Reporter) MarkReady(detail string) {
	r.Set(true, "ready", detail)
}

func (r *Reporter) MarkNotReady(detail string) {
	r.Set(false, "not_ready", detail)
}

func (r *Reporter) MarkFailed(detail string) {
	r.Set(false, "failed", detail)
}

func (r *Reporter) Snapshot() Snapshot {
	if r == nil {
		return Snapshot{}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	return Snapshot{
		Ready:     r.ready,
		Status:    r.status,
		Detail:    r.detail,
		UpdatedAt: r.updated,
	}
}
