package registry

import (
	"context"
	"errors"
)

var ErrWorkerUnimplemented = errors.New("not implemented, tried to use transaction from worker")

type NoOpTransaction struct{}

func (tr NoOpTransaction) Put(ctx context.Context, key []byte, value []byte) error {
	return ErrWorkerUnimplemented
}

func (tr NoOpTransaction) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	return nil, false, ErrWorkerUnimplemented
}

func (tr NoOpTransaction) Commit(ctx context.Context) error {
	return ErrWorkerUnimplemented
}

func (tr NoOpTransaction) Cancel(ctx context.Context) error {
	return ErrWorkerUnimplemented
}
