package registry

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

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

// func

// // NewFoundationDBRegistry creates a new FoundationDB-backed registry.
// func NewFoundationDBRegistry() (Registry, error) {
// 	fdb.MustAPIVersion(710)
// 	db, err := fdb.OpenDefault()
// 	if err != nil {
// 		return nil, fmt.Errorf("error creating FDB registry: %w", err)
// 	}

// 	tr, err := db.CreateTransaction()
// 	if err != nil {
// 		return nil, fmt.Errorf("error creating initial FDB transaction: %w", err)
// 	}

// 	modules, err := directory.CreateOrOpen(tr, []string{"modules"}, nil)
// 	if err != nil {
// 		return nil, fmt.Errorf("error creating modules directory: %w", err)
// 	}

// 	return &fdbRegistry{
// 		db:      db,
// 		modules: modules,
// 	}, nil
// }

// func (f *fdbRegistry) RegisterModule(
// 	ctx context.Context,
// 	namespace,
// 	moduleID string,
// 	moduleBytes []byte,
// 	opts ModuleOptions,
// ) (RegisterModuleResult, error) {
// 	key := f.modules.Pack(namespace, moduleID)
// 	rIface, err := f.db.Transact(func(tr fdb.Transaction) (any, error) {
// 		tr.Get()
// 		result, err := tr.Get(key).Get()
// 		if err != nil {
// 			return nil, err
// 		}

// 		if result == nil {
// 			return RegisterModuleResult{}, fmt.Errorf(
// 				"error creating module: %s in namespace: %s, already exists",
// 				moduleID, namespace)
// 		}
// 		tr.Set(key, mod)
// 	})
// 	if err != nil {
// 		return RegisterModuleResult{}, err
// 	}
// 	return rIface.(RegisterModuleResult), nil
// }

// // GetModule gets the bytes and options associated with the provided module.
// func (f *fdbRegistry) GetModule(
// 	ctx context.Context,
// 	namespace,
// 	moduleID string,
// ) ([]byte, ModuleOptions, error) {
// 	panic("not implemented")
// }

// func (f *fdbRegistry) CreateActor(
// 	ctx context.Context,
// 	namespace,
// 	actorID,
// 	moduleID string,
// 	opts ActorOptions,
// ) (CreateActorResult, error) {
// 	panic("not implemented")
// }

// func (f *fdbRegistry) IncGeneration(
// 	ctx context.Context,
// 	namespace,
// 	actorID string,
// ) error {
// 	panic("not implemented")
// }

// func (f *fdbRegistry) EnsureActivation(
// 	ctx context.Context,
// 	namespace,
// 	actorID string,
// ) ([]types.ActorReference, error) {
// 	panic("not implemented")
// }

// func (f *fdbRegistry) ActorKVPut(
// 	ctx context.Context,
// 	namespace string,
// 	actorID string,
// 	key []byte,
// 	value []byte,
// ) error {
// 	panic("not implemented")
// }

// func (f *fdbRegistry) ActorKVGet(
// 	ctx context.Context,
// 	namespace string,
// 	actorID string,
// 	key []byte,
// ) ([]byte, bool, error) {
// 	panic("not implemented")
// }

// func (f *fdbRegistry) Heartbeat(
// 	ctx context.Context,
// 	serverID string,
// 	heartbeatState HeartbeatState,
// ) error {
// 	panic("not implemented")
// }
