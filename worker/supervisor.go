package worker

import (
	"context"
	"sync"
	"time"
)

type workerSupervisorEvent uint8

const (
	workerContextCanceled workerSupervisorEvent = iota
	workerForceReturned
	workerLoopsDone
	workerError
)

// workerSupervisor owns the goroutine and context lifecycle for one Worker.
// Keeping the shutdown choreography here makes the ordering of cancellation,
// loop waits, lease cleanup, and forced shutdown explicit in Worker.run.
type workerSupervisor struct {
	worker          *Worker
	ctx             context.Context
	drain           <-chan struct{}
	force           <-chan struct{}
	shutdownTimeout time.Duration
	state           *workerState

	reserveCtx    context.Context
	cancelReserve context.CancelFunc
	execCtx       context.Context
	cancelExec    context.CancelFunc
	leaseCtx      context.Context
	cancelLeases  context.CancelFunc
	leases        *leaseCoordinator

	leaseWG      sync.WaitGroup
	loops        sync.WaitGroup
	executions   sync.WaitGroup
	errCh        chan error
	forcedReturn chan struct{}
	done         chan struct{}
	stopRefresh  chan struct{}
	reserveWake  chan struct{}
	dispatchWake chan struct{}
}

func newWorkerSupervisor(
	ctx context.Context,
	worker *Worker,
	state *workerState,
	drain <-chan struct{},
	force <-chan struct{},
	shutdownTimeout time.Duration,
) *workerSupervisor {
	reserveCtx, cancelReserve := context.WithCancel(ctx)
	execCtx, cancelExec := context.WithCancel(ctx)
	leaseCtx, cancelLeases := context.WithCancel(ctx)
	return &workerSupervisor{
		worker:          worker,
		ctx:             ctx,
		drain:           drain,
		force:           force,
		shutdownTimeout: shutdownTimeout,
		state:           state,
		reserveCtx:      reserveCtx,
		cancelReserve:   cancelReserve,
		execCtx:         execCtx,
		cancelExec:      cancelExec,
		leaseCtx:        leaseCtx,
		cancelLeases:    cancelLeases,
		leases:          newLeaseCoordinator(leaseCtx, worker.Logger, worker.Broker),
		errCh:           make(chan error, 1),
		forcedReturn:    make(chan struct{}, 1),
		stopRefresh:     make(chan struct{}),
		reserveWake:     make(chan struct{}, 1),
		dispatchWake:    make(chan struct{}, 1),
	}
}

func (s *workerSupervisor) start() {
	s.leaseWG.Go(s.leases.run)
	go s.worker.lifecycleRefreshLoop(s.stopRefresh, s.state)

	s.loops.Go(func() {
		if err := s.worker.reserveLoop(s.reserveCtx, s.leaseCtx, s.leases, s.state, s.reserveWake, s.dispatchWake); err != nil {
			s.report(err)
		}
	})
	s.loops.Go(func() {
		if err := s.worker.dispatchLoop(s.execCtx, s.state, s.reserveWake, s.dispatchWake, s.errCh, &s.executions); err != nil {
			s.report(err)
		}
	})
	if s.worker.Adaptive.Enabled {
		s.loops.Go(func() {
			if err := s.worker.adaptiveLoop(s.reserveCtx, s.state, s.reserveWake, s.dispatchWake); err != nil {
				s.report(err)
			}
		})
	}

	if s.drain != nil {
		go s.watchDrain()
	}
	if s.force != nil {
		go s.watchForce()
	}

	s.done = make(chan struct{})
	go func() {
		s.loops.Wait()
		if !s.worker.isStopped(s.state) {
			s.executions.Wait()
		}
		close(s.done)
	}()
}

func (s *workerSupervisor) report(err error) {
	if err == nil {
		return
	}
	select {
	case s.errCh <- err:
	default:
	}
}

func (s *workerSupervisor) watchDrain() {
	select {
	case <-s.drain:
		if s.worker.beginDrain(s.ctx, s.state, s.shutdownTimeout) {
			s.stopReservation()
			notify(s.dispatchWake)
		}
	case <-s.execCtx.Done():
	}
}

func (s *workerSupervisor) watchForce() {
	select {
	case <-s.force:
		s.worker.forceStop(s.ctx, s.state, s.cancelReserve, s.cancelExec, s.cancelLeases)
		select {
		case s.forcedReturn <- struct{}{}:
		default:
		}
		notify(s.reserveWake)
		notify(s.dispatchWake)
	case <-s.execCtx.Done():
	}
}

func (s *workerSupervisor) await() (workerSupervisorEvent, error) {
	select {
	case <-s.ctx.Done():
		return workerContextCanceled, nil
	case <-s.forcedReturn:
		return workerForceReturned, nil
	case <-s.done:
		return workerLoopsDone, nil
	case err := <-s.errCh:
		return workerError, err
	}
}

func (s *workerSupervisor) stopReservation() {
	s.cancelReserve()
	notify(s.reserveWake)
}

func (s *workerSupervisor) stopAll() {
	s.cancelReserve()
	s.cancelExec()
	s.cancelLeases()
	notify(s.reserveWake)
	notify(s.dispatchWake)
}

func (s *workerSupervisor) waitDone() {
	<-s.done
}

func (s *workerSupervisor) waitLoops() {
	s.loops.Wait()
}

func (s *workerSupervisor) stopLeases() {
	s.cancelLeases()
	s.leaseWG.Wait()
}

func (s *workerSupervisor) close() {
	close(s.stopRefresh)
	s.cancelExec()
	s.cancelLeases()
	s.leaseWG.Wait()
	s.cancelReserve()
}
