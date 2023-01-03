package registry

import "context"

// kv is a generic interface for a transactional, sorted KV. It is used to
// abstract over various KV implementation so we can implement the registry
// generically in kv_registry.go and still use all the logic/code for multiple
// KV backends.
type kv interface {
	transact(func(transaction) (any, error)) (any, error)
	close(ctx context.Context) error
	unsafeWipeAll() error
}

type transaction interface {
	put([]byte, []byte)
	get([]byte) ([]byte, bool, error)
	iterPrefix(prefix []byte, fn func(k, v []byte) error) error
}
