package workerpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var ErrStopped = errors.New("workerpool: stopped")

type Job func(context.Context) error

type Pool struct {
	workers   int
	jobs      chan Job
	stopCh    chan struct{}
	start     sync.Once
	stop      sync.Once
	closeJobs sync.Once
	submitMu  sync.Mutex
	workerWG  sync.WaitGroup
	submitWG  sync.WaitGroup
	stopped   atomic.Bool
	inflight  atomic.Int64
}

func New(workers int, queueSize int) *Pool {
	if workers <= 0 {
		workers = 1
	}
	if queueSize <= 0 {
		queueSize = workers
	}
	return &Pool{
		workers: workers,
		jobs:    make(chan Job, queueSize),
		stopCh:  make(chan struct{}),
	}
}

func (p *Pool) Start(ctx context.Context) {
	p.start.Do(func() {
		for i := 0; i < p.workers; i++ {
			p.workerWG.Add(1)
			go func() {
				defer p.workerWG.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case job, ok := <-p.jobs:
						if !ok {
							return
						}
						p.inflight.Add(1)
						_ = job(ctx)
						p.inflight.Add(-1)
					}
				}
			}()
		}
	})
}

func (p *Pool) Submit(ctx context.Context, job Job) error {
	if job == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.submitMu.Lock()
	if p.stopped.Load() {
		p.submitMu.Unlock()
		return ErrStopped
	}

	p.submitWG.Add(1)
	p.submitMu.Unlock()
	defer p.submitWG.Done()

	if p.stopped.Load() {
		return ErrStopped
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.stopCh:
		return ErrStopped
	case p.jobs <- job:
		return nil
	}
}

func (p *Pool) Stop() {
	_ = p.Shutdown(context.Background())
}

func (p *Pool) Shutdown(ctx context.Context) error {
	p.stop.Do(func() {
		p.submitMu.Lock()
		p.stopped.Store(true)
		close(p.stopCh)
		p.submitMu.Unlock()
	})

	submitted := make(chan struct{})
	go func() {
		p.submitWG.Wait()
		close(submitted)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-submitted:
	}

	p.closeJobs.Do(func() {
		close(p.jobs)
	})

	done := make(chan struct{})
	go func() {
		p.workerWG.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (p *Pool) Inflight() int64 {
	return p.inflight.Load()
}

func (p *Pool) QueueDepth() int {
	return len(p.jobs)
}

func (p *Pool) QueueCapacity() int {
	return cap(p.jobs)
}
