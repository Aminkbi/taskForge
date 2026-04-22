package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/aminkbi/taskforge/internal/observability"
)

type Manager struct {
	Workers         []*Worker
	ShutdownTimeout time.Duration
}

func (m *Manager) Run(ctx context.Context) error {
	if len(m.Workers) == 0 {
		<-ctx.Done()
		return nil
	}

	errCh := make(chan error, len(m.Workers))
	drainWorkers := make(chan struct{})
	forceWorkers := make(chan struct{})
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	var drainOnce sync.Once
	var forceOnce sync.Once
	var wg sync.WaitGroup
	for _, worker := range m.Workers {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := worker.run(runCtx, drainWorkers, forceWorkers, m.ShutdownTimeout); err != nil {
				errCh <- err
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		drainOnce.Do(func() {
			close(drainWorkers)
		})
		if m.ShutdownTimeout <= 0 {
			forceOnce.Do(func() {
				close(forceWorkers)
			})
			<-done
			return nil
		}
		select {
		case <-done:
			return nil
		case <-time.After(m.ShutdownTimeout):
			forceOnce.Do(func() {
				close(forceWorkers)
			})
			<-done
		}
		return nil
	case err := <-errCh:
		forceOnce.Do(func() {
			close(forceWorkers)
		})
		<-done
		return err
	}
}

func (m *Manager) WorkerLifecycleSnapshots(context.Context) ([]observability.WorkerLifecycleSnapshot, error) {
	snapshots := make([]observability.WorkerLifecycleSnapshot, 0, len(m.Workers))
	for _, worker := range m.Workers {
		if worker == nil {
			continue
		}
		snapshot, ok := worker.LifecycleSnapshot()
		if !ok {
			continue
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}
