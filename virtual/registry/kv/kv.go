package kv

import "context"

// Store is a generic interface for a transactional, sorted KV store. It is used to
// abstract over various KV implementation so we can implement the registry
// generically in kv_registry.go and still use all the logic/code for multiple
// KV backends.
type Store interface {
	BeginTransaction(ctx context.Context) (Transaction, error)
	Transact(func(Transaction) (any, error)) (any, error)
	Close(ctx context.Context) error
	UnsafeWipeAll() error
}

type Transaction interface {
	Put(ctx context.Context, key []byte, value []byte) error
	Get(ctx context.Context, key []byte) ([]byte, bool, error)
	IterPrefix(ctx context.Context, prefix []byte, fn func(k, v []byte) error) error
	// Monotonically increase number that should increase at a rate of ~ 1 million
	// per second.
	GetVersionStamp() (int64, error)
	Commit(ctx context.Context) error
	Cancel(ctx context.Context) error
}
