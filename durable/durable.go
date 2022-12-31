package durable

import (
	"context"
	"io"
)

type Module interface {
	Instantiate(
		ctx context.Context,
		id string,
	) (Object, error)
	Close(ctx context.Context) error
}

type Object interface {
	Invoke(ctx context.Context, operation string, payload []byte) ([]byte, error)
	Close(ctx context.Context) error
	Snapshot(ctx context.Context, w io.Writer) error
	SnapshotIncremental(
		ctx context.Context,
		prev []byte,
		w io.Writer,
	) error
	Hydrate(ctx context.Context, r io.Reader, readerSize int) error
}

type Logger func(msg string)

type OperationLogger func(operation string, payload []byte)
