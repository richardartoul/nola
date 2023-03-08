package futures

import (
	"context"
	"fmt"
	"sync"
)

type future[T any] struct {
	sync.Mutex
	sync.WaitGroup

	result   T
	err      error
	resolved bool
}

func New[T any]() Future[T] {
	f := &future[T]{}
	f.Add(1)
	return f
}

func (f *future[T]) Go(fn func() (T, error)) {
	f.ResolveOrReject(fn())
}

func (f *future[T]) Resolve(result T) {
	f.Lock()
	defer f.Unlock()

	if f.resolved {
		panic("future resolved multiple times")
	}

	f.resolved = true
	f.result = result
	f.WaitGroup.Done()
}

func (f *future[T]) Reject(err error) {
	f.Lock()
	defer f.Unlock()

	if f.resolved {
		panic("future resolved multiple times")
	}

	f.resolved = true
	f.err = err
	f.WaitGroup.Done()
}

func (f *future[T]) ResolveOrReject(result T, err error) {
	f.Lock()
	defer f.Unlock()

	if f.resolved {
		panic("future resolved multiple times")
	}

	f.resolved = true
	f.result = result
	f.err = err
	f.WaitGroup.Done()
}

func (f *future[T]) Wait() (result T, err error) {
	f.WaitGroup.Wait()
	return f.result, f.err
}

func WaitAllSlice[T any](futures []Future[T]) ([]T, error) {
	results := make([]T, 0, len(futures))
	for i, fut := range futures {
		result, err := fut.Wait()
		if err != nil {
			return nil, fmt.Errorf(
				"WaitAllSlice: future at index: %d resolved with error: %w",
				i, err)
		}
		results = append(results, result)
	}

	return results, nil
}

func WaitAllSliceCtx[T any](ctx context.Context, futures []Future[T]) ([]T, error) {
	waitCh := make(chan resultsOrErr[T])
	go func() {
		results, err := WaitAllSlice(futures)
		waitCh <- resultsOrErr[T]{results, err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-waitCh:
		return r.results, r.err
	}
}

type resultsOrErr[T any] struct {
	results []T
	err     error
}
