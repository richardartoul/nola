package registry

import (
	"bytes"
	"sync"

	"github.com/google/btree"
)

type kv interface {
	transact(func(transaction) (any, error)) (any, error)
}

type transaction interface {
	put([]byte, []byte)
	get([]byte) ([]byte, bool, error)
}

type localKV struct {
	sync.Mutex
	b *btree.BTreeG[btreeKV]
	// m map[string][]byte
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
	l.b.ReplaceOrInsert(btreeKV{k, v})
}

func (l *localKV) get(k []byte) ([]byte, bool, error) {
	v, ok := l.b.Get(btreeKV{k, nil})
	if !ok {
		return nil, false, nil
	}
	return v.v, true, nil
}

func (l *localKV) iterPrefix(prefix []byte, fn func(k, v []byte)) error {
	l.b.AscendGreaterOrEqual(btreeKV{prefix, nil}, func(currKV btreeKV) bool {
		if bytes.HasPrefix(currKV.k, prefix) {
			fn(currKV.k, currKV.v)
			return true
		}
		return false
	})
	return nil
}

type btreeKV struct {
	k []byte
	v []byte
}
