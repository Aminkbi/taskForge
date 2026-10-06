package worker

import (
	"context"
	"github.com/aminkbi/taskforge"
	"sync"
	"time"
)

type Manager struct {
	Workers         []*Worker
	ShutdownTimeout time.Duration
}

type managerSupervisor struct {
	drainWorkers chan struct{}
	forceWorkers chan struct{}
	drainOnce    sync.Once
	forceOnce    sync.Once
}

func newManagerSupervisor() *managerSupervisor {
	return &managerSupervisor{
		drainWorkers: make(chan struct{}),
		forceWorkers: make(chan struct{}),
	}
}

func (s *managerSupervisor) drain() {
	s.drainOnce.Do(func() { close(s.drainWorkers) })
}

func (s *managerSupervisor) force() {
	s.forceOnce.Do(func() { close(s.forceWorkers) })
}

func (m *Manager) Run(ctx context.Context) error {
	if len(m.Workers) == 0 {
		<-ctx.Done()
		return nil
	}

	errCh := make(chan error, len(m.Workers))
	supervisor := newManagerSupervisor()
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	var wg sync.WaitGroup
	for _, worker := range m.Workers {
		wg.Go(func() {
			if err := worker.run(runCtx, supervisor.drainWorkers, supervisor.forceWorkers, m.ShutdownTimeout); err != nil {
				errCh <- err
			}
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		supervisor.drain()
		if m.ShutdownTimeout <= 0 {
			supervisor.force()
			<-done
			return nil
		}
		select {
		case <-done:
			return nil
		case <-time.After(m.ShutdownTimeout):
			supervisor.force()
			<-done
		}
		return nil
	case err := <-errCh:
		supervisor.force()
		<-done
		return err
	}
}

func (m *Manager) WorkerLifecycleSnapshots(context.Context) ([]taskforge.WorkerLifecycleSnapshot, error) {
	snapshots := make([]taskforge.WorkerLifecycleSnapshot, 0, len(m.Workers))
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
