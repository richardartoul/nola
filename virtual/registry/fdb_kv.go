package registry

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// fdbKV is an implementation of kv backed by FoundationDB.
type fdbKV struct {
	db fdb.Database
}

func newFDBKV(clusterFile string) (kv, error) {
	fdb.MustAPIVersion(710)
	db, err := fdb.OpenDatabase(clusterFile)
	if err != nil {
		return nil, fmt.Errorf("error opening FDB database: %w", err)
	}
	return &fdbKV{db: db}, nil
}

func (f *fdbKV) transact(fn func(tr transaction) (any, error)) (any, error) {
	return f.db.Transact(func(tr fdb.Transaction) (any, error) {
		return fn(&fdbTransaction{tr})
	})
}

type fdbTransaction struct {
	tr fdb.Transaction
}

func (tr *fdbTransaction) put(k, v []byte) {
	tr.tr.Set(fdb.Key(k), v)
}

func (tr *fdbTransaction) get(k []byte) ([]byte, bool, error) {
	v, err := tr.tr.Get(fdb.Key(k)).Get()
	if err != nil {
		return nil, false, err
	}

	if v == nil {
		return nil, false, nil
	}

	return v, true, nil
}

func (tr *fdbTransaction) iterPrefix(prefix []byte, fn func(k, v []byte) error) error {
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
