package registry

import "sync"

type kv interface {
	transact(func(transaction) (any, error)) (any, error)
}

type transaction interface {
	put([]byte, []byte)
	get([]byte) ([]byte, bool, error)
}

type localKV struct {
	sync.Mutex
	m map[string][]byte
}

func newLocalKV() kv {
	return &localKV{
		m: make(map[string][]byte),
	}
}

// TODO: This doesn't actually implement rollbacks. It's fine for now, but we should
//
//	make it rollback on failure so it matches FDB.
func (l *localKV) transact(fn func(transaction) (any, error)) (any, error) {
	l.Lock()
	defer l.Unlock()
	return fn(l)
}

func (l *localKV) put(k, v []byte) {
	l.m[string(k)] = v
}

func (l *localKV) get(k []byte) ([]byte, bool, error) {
	v, ok := l.m[string(k)]
	if !ok {
		return nil, false, nil
	}
	return v, true, nil
}
