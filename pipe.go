package vfs

import (
	"context"
	"errors"
	"sync"

	"github.com/hanzoai/vfs/pkg/backend"
)

// uploader writes objects to the store with a bounded number in flight. put returns
// once the write is under way, waiting for a free slot first, so a producer is held
// to the speed of the store; wait returns when every write made so far has finished,
// with the first error. After an error no new write starts: a generation that failed
// is started again whole, and every object it wrote is named by its content or by a
// random id, so repeating a write is harmless.
type uploader struct {
	ctx context.Context
	be  backend.Backend
	sem chan struct{}
	wg  sync.WaitGroup

	mu  sync.Mutex
	err error
}

func newUploader(ctx context.Context, be backend.Backend, streams int) *uploader {
	return &uploader{ctx: ctx, be: be, sem: make(chan struct{}, max(streams, 1))}
}

// put starts writing data at key and calls done, if it is not nil, when the store
// has it. The slice is not copied and must not change until the write is done.
func (u *uploader) put(key string, data []byte, done func()) {
	select {
	case u.sem <- struct{}{}:
	case <-u.ctx.Done():
		u.fail(u.ctx.Err())
		return
	}
	if u.failed() != nil {
		<-u.sem
		return
	}
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		defer func() { <-u.sem }()
		if err := u.be.Put(u.ctx, key, data); err != nil {
			u.fail(err)
			return
		}
		if done != nil {
			done()
		}
	}()
}

func (u *uploader) fail(err error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.err == nil {
		u.err = err
	}
}

func (u *uploader) failed() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.err
}

// wait returns when every write started so far has finished.
func (u *uploader) wait() error {
	u.wg.Wait()
	return u.failed()
}

// workers bounds the CPU work of a Volume's pipeline (cutting, hashing and sealing
// chunks) to one count shared by every generation and every file, so that a
// machine that is also serving callers is not taken whole by its own publisher.
type workers struct{ sem chan struct{} }

func newWorkers(n int) *workers { return &workers{sem: make(chan struct{}, max(n, 1))} }

// group is a set of tasks of one job: each runs on a worker, and wait returns the first
// error once all have finished.
type group struct {
	w    *workers
	ctx  context.Context
	wg   sync.WaitGroup
	mu   sync.Mutex
	err  error
	quit context.CancelFunc
}

func (w *workers) group(ctx context.Context) *group {
	ctx, quit := context.WithCancel(ctx)
	return &group{w: w, ctx: ctx, quit: quit}
}

// do runs fn on a worker once one is free. The caller holds no worker while it waits
// for one, so a task that itself starts tasks cannot starve the workers it needs.
func (g *group) do(fn func(context.Context) error) {
	select {
	case g.w.sem <- struct{}{}:
	case <-g.ctx.Done():
		g.fail(g.ctx.Err())
		return
	}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		defer func() { <-g.w.sem }()
		if g.failed() != nil {
			return
		}
		if err := fn(g.ctx); err != nil {
			g.fail(err)
		}
	}()
}

func (g *group) fail(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err == nil {
		g.err = err
		g.quit()
	}
}

func (g *group) failed() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.err
}

func (g *group) wait() error {
	g.wg.Wait()
	g.quit()
	return g.failed()
}

// flights makes concurrent requests for the same key share one call: the first runs
// fn, the others wait for its result. A caller whose own context is alive does not
// take the first caller's cancellation for an answer; it asks again.
type flights[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*flight[V]
}

type flight[V any] struct {
	done chan struct{}
	v    V
	err  error
}

func (f *flights[K, V]) do(ctx context.Context, k K, fn func() (V, error)) (V, error) {
	for {
		f.mu.Lock()
		if f.m == nil {
			f.m = map[K]*flight[V]{}
		}
		if c, ok := f.m[k]; ok {
			f.mu.Unlock()
			select {
			case <-c.done:
			case <-ctx.Done():
				var zero V
				return zero, ctx.Err()
			}
			if c.err != nil && (errors.Is(c.err, context.Canceled) || errors.Is(c.err, context.DeadlineExceeded)) && ctx.Err() == nil {
				continue
			}
			return c.v, c.err
		}
		c := &flight[V]{done: make(chan struct{})}
		f.m[k] = c
		f.mu.Unlock()
		c.v, c.err = fn()
		f.mu.Lock()
		delete(f.m, k)
		f.mu.Unlock()
		close(c.done)
		return c.v, c.err
	}
}
