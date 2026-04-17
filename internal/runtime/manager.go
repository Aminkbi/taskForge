package runtime

import (
	"context"
	"sync"
)

type Manager struct {
	Workers []*Worker
}

func (m *Manager) Run(ctx context.Context) error {
	if len(m.Workers) == 0 {
		<-ctx.Done()
		return nil
	}

	errCh := make(chan error, len(m.Workers))
	stopWorkers := make(chan struct{})
	var stopOnce sync.Once
	var wg sync.WaitGroup
	for _, worker := range m.Workers {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := worker.run(ctx, stopWorkers); err != nil {
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
		<-done
		return nil
	case err := <-errCh:
		stopOnce.Do(func() {
			close(stopWorkers)
		})
		<-done
		return err
	}
}
