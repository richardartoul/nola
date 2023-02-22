package fdbregistry

import (
	"context"
	"fmt"

	"github.com/richardartoul/nola/virtual/registry/kv"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// fdbKV is an implementation of kv backed by FoundationDB.
type fdbKV struct {
	db fdb.Database
}

func newFDBKV(clusterFile string) (kv.Store, error) {
	fdb.MustAPIVersion(710)
	db, err := fdb.OpenDatabase(clusterFile)
	if err != nil {
		return nil, fmt.Errorf("error opening FDB database: %w", err)
	}
	return &fdbKV{db: db}, nil
}

func (f *fdbKV) BeginTransaction(ctx context.Context) (kv.Transaction, error) {
	tr, err := f.db.CreateTransaction()
	if err != nil {
		return nil, fmt.Errorf("fdbKV: beginTransaction: error creating transaction: %w", err)
	}
	return &fdbTransaction{tr: tr}, nil
}

func (f *fdbKV) Transact(fn func(tr kv.Transaction) (any, error)) (any, error) {
	return f.db.Transact(func(tr fdb.Transaction) (any, error) {
		return fn(&fdbTransaction{tr})
	})
}

func (f *fdbKV) Close(ctx context.Context) error {
	// TODO: Why does f.db.Close() not exist?
	// https://pkg.go.dev/github.com/apple/foundationdb/bindings/go/src/fdb#Database.Close
	return nil
}

func (f *fdbKV) UnsafeWipeAll() error {
	_, err := f.db.Transact(func(tr fdb.Transaction) (any, error) {
		tr.ClearRange(fdb.KeyRange{Begin: fdb.Key{0x00}, End: fdb.Key{0xFF}})
		return nil, nil
	})
	return err
}

type fdbTransaction struct {
	tr fdb.Transaction
}

func (tr *fdbTransaction) Put(
	ctx context.Context,
	k, v []byte,
) error {
	tr.tr.Set(fdb.Key(k), v)
	return nil
}

func (tr *fdbTransaction) Get(
	ctx context.Context,
	k []byte,
) ([]byte, bool, error) {
	v, err := tr.tr.Get(fdb.Key(k)).Get()
	if err != nil {
		return nil, false, err
	}

	if v == nil {
		return nil, false, nil
	}

	return v, true, nil
}

func (tr *fdbTransaction) IterPrefix(
	ctx context.Context,
	prefix []byte,
	fn func(k, v []byte) error,
) error {
	prefixRange, err := fdb.PrefixRange(prefix)
	if err != nil {
		return err
	}
	iter := tr.tr.GetRange(prefixRange, fdb.RangeOptions{}).Iterator()
	for iter.Advance() {
		kv, err := iter.Get()
		if err != nil {
			return err
		}
		if err := fn(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	return nil
}

func (tr *fdbTransaction) GetVersionStamp() (int64, error) {
	readV, err := tr.tr.GetReadVersion().Get()
	if err != nil {
		return -1, err
	}
	return readV, nil
}

func (tr *fdbTransaction) Commit(ctx context.Context) error {
	return tr.tr.Commit().Get()
}

func (tr *fdbTransaction) Cancel(ctx context.Context) error {
	tr.tr.Cancel()
	return nil
}
