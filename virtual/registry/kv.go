package registry

type kv interface {
	transact(func(transaction) (any, error)) (any, error)
}

type transaction interface {
	put([]byte, []byte)
	get([]byte) ([]byte, bool, error)
	iterPrefix(prefix []byte, fn func(k, v []byte) error) error
}
