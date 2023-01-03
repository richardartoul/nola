package registry

import (
	"bytes"
	"sync"

	"github.com/google/btree"
)

// localKV is an implementation of kv backed by local memory.
type localKV struct {
	sync.Mutex
	b *btree.BTreeG[btreeKV]
}

func newLocalKV() kv {
	return &localKV{
		// m: make(map[string][]byte),
		b: btree.NewG(16, func(a, b btreeKV) bool {
			return bytes.Compare(a.k, b.k) < 0
		}),
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
	// Copy v in case the caller reuses it or mutates it.
	l.b.ReplaceOrInsert(btreeKV{k, append([]byte(nil), v...)})
}

func (l *localKV) get(k []byte) ([]byte, bool, error) {
	v, ok := l.b.Get(btreeKV{k, nil})
	if !ok {
		return nil, false, nil
	}
	return v.v, true, nil
}

func (l *localKV) iterPrefix(prefix []byte, fn func(k, v []byte) error) error {
	var globalErr error
	l.b.AscendGreaterOrEqual(btreeKV{prefix, nil}, func(currKV btreeKV) bool {
		if bytes.HasPrefix(currKV.k, prefix) {
			if err := fn(currKV.k, currKV.v); err != nil {
				globalErr = err
				return false
			}
			return true
		}
		return false
	})
	return globalErr
}

type btreeKV struct {
	k []byte
	v []byte
}
