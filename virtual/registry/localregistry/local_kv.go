package localregistry

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/richardartoul/nola/virtual/registry/kv"

	"github.com/google/btree"
)

// localKV is an implementation of kv backed by local memory.
type localKV struct {
	sync.Mutex
	t time.Time
	b *btree.BTreeG[btreeKV]
	// clone of b for current transaction, if any.
	trClone *btree.BTreeG[btreeKV]
	closed  bool
}

func newLocalKV() kv.Store {
	return &localKV{
		t: time.Now(),
		b: btree.NewG(16, func(a, b btreeKV) bool {
			return bytes.Compare(a.k, b.k) < 0
		}),
	}
}

func (l *localKV) BeginTransaction(ctx context.Context) (kv.Transaction, error) {
	l.Lock()
	l.trClone = l.b.Clone()
	return l, nil
}

func (l *localKV) Commit(ctx context.Context) error {
	defer l.Unlock()
	if l.trClone == nil {
		panic("trClone should not be nil")
	}

	l.trClone = nil
	return nil
}

func (l *localKV) Cancel(ctx context.Context) error {
	defer l.Unlock()
	if l.trClone == nil {
		panic("trClone should not be nil")
	}

	l.b = l.trClone
	l.trClone = nil
	return nil
}

func (l *localKV) Transact(fn func(kv.Transaction) (any, error)) (any, error) {
	l.Lock()
	defer l.Unlock()

	clone := l.b.Clone()
	result, err := fn(l)
	if err != nil {
		l.b = clone
	}
	return result, err
}

func (l *localKV) UnsafeWipeAll() error {
	l.Lock()
	defer l.Unlock()

	l.b.Clear(false)
	return nil
}

func (l *localKV) Close(ctx context.Context) error {
	l.Lock()
	defer l.Unlock()

	l.closed = true
	return nil
}

// "transaction" method so no lock because we're already locked.
func (l *localKV) Put(
	ctx context.Context,
	k, v []byte,
) error {
	if l.closed {
		return errors.New("KV already closed")
	}

	// Copy v in case the caller reuses it or mutates it.
	l.b.ReplaceOrInsert(btreeKV{k, append([]byte(nil), v...)})
	return nil
}

// "transaction" method so no lock because we're already locked.
func (l *localKV) Get(
	ctx context.Context,
	k []byte,
) ([]byte, bool, error) {
	if l.closed {
		return nil, false, errors.New("KV already closed")
	}

	v, ok := l.b.Get(btreeKV{k, nil})
	if !ok {
		return nil, false, nil
	}
	return v.v, true, nil
}

// "transaction" method so no lock because we're already locked.
func (l *localKV) IterPrefix(
	ctx context.Context,
	prefix []byte, fn func(k, v []byte) error,
) error {
	if l.closed {
		return errors.New("KV already closed")
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
func (l *localKV) GetVersionStamp() (int64, error) {
	// Return microseconds since l.t since that will automatically increase at
	// a rate of ~ 1 million/s just like FDB's versionstamp.
	return time.Since(l.t).Microseconds(), nil
}

type btreeKV struct {
	k []byte
	v []byte
}
