package registry

import "context"

// kv is a generic interface for a transactional, sorted KV. It is used to
// abstract over various KV implementation so we can implement the registry
// generically in kv_registry.go and still use all the logic/code for multiple
// KV backends.
type kv interface {
	beginTransaction(ctx context.Context) (transaction, error)
	transact(func(transaction) (any, error)) (any, error)
	close(ctx context.Context) error
	unsafeWipeAll() error
}

type transaction interface {
	put(ctx context.Context, key []byte, value []byte) error
	get(ctx context.Context, key []byte) ([]byte, bool, error)
	iterPrefix(ctx context.Context, prefix []byte, fn func(k, v []byte) error) error
	// Monotonically increase number that should increase at a rate of ~ 1 million
	// per second.
	getVersionStamp() (int64, error)
	commit(ctx context.Context) error
	cancel(ctx context.Context) error
}
