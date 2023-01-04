package registry

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/google/btree"
)

// localKV is an implementation of kv backed by local memory.
type localKV struct {
	sync.Mutex
	t      time.Time
	b      *btree.BTreeG[btreeKV]
	closed bool
}

func newLocalKV() kv {
	return &localKV{
		t: time.Now(),
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

func (l *localKV) unsafeWipeAll() error {
	l.Lock()
	defer l.Unlock()

	l.b.Clear(false)
	return nil
}

func (l *localKV) close(ctx context.Context) error {
	l.Lock()
	defer l.Unlock()

	l.closed = true
	return nil
}

// "transaction" method so no lock because we're already locked.
func (l *localKV) put(k, v []byte) {
	if l.closed {
		panic("KV already closed")
	}

	// Copy v in case the caller reuses it or mutates it.
	l.b.ReplaceOrInsert(btreeKV{k, append([]byte(nil), v...)})
}

// "transaction" method so no lock because we're already locked.
func (l *localKV) get(k []byte) ([]byte, bool, error) {
	if l.closed {
		panic("KV already closed")
	}

	v, ok := l.b.Get(btreeKV{k, nil})
	if !ok {
		return nil, false, nil
	}
	return v.v, true, nil
}

// "transaction" method so no lock because we're already locked.
func (l *localKV) iterPrefix(prefix []byte, fn func(k, v []byte) error) error {
	if l.closed {
		panic("KV already closed")
	}

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

// "transaction" method so no lock because we're already locked.
func (l *localKV) getVersionStamp() (int64, error) {
	// Return microseconds since l.t since that will automatically increase at
	// a rate of ~ 1 million/s just like FDB's versionstamp.
	return time.Since(l.t).Microseconds(), nil
}

type btreeKV struct {
	k []byte
	v []byte
}
