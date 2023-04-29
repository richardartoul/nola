package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/richardartoul/nola/virtual/registry/kv"
	"github.com/richardartoul/nola/virtual/registry/tuple"
	"github.com/richardartoul/nola/virtual/types"

	"golang.org/x/sync/singleflight"
)

const (
	// HeartbeatTTL is the maximum amount of time between server heartbeats before
	// the registry will consider a server as dead.
	//
	// TODO: Should be configurable.
	HeartbeatTTL = 5 * time.Second

	// RebalanceMemoryThreshold is the minimum delta between the memory usage of the
	// minimum and maximum servers before the registry will begin making balancing
	// decisions based on memory usage.
	// TODO: Replace magic constant with config option.
	RebalanceMemoryThreshold = 1 << 31
)

var (
	errActorDoesNotExist = errors.New("actor does not exist")

	// Make sure kvRegistry implements ModuleStore as well.
	_ ModuleStore = &validator{}
)

// IsActorDoesNotExistErr returns a boolean indicating whether the error is an
// instance of (or wraps) errActorDoesNotExist.
func IsActorDoesNotExistErr(err error) bool {
	return errors.Is(err, errActorDoesNotExist)
}

type kvRegistry struct {
	versionStampBatcher singleflight.Group

	// State.
	kv kv.Store
}

// NewKVRegistry creates a new KV-backed registry.
func NewKVRegistry(kv kv.Store) Registry {
	return NewValidatedRegistry(&kvRegistry{
		kv: kv,
	})
}

// TODO: Add compression?
func (k *kvRegistry) RegisterModule(
	ctx context.Context,
	namespace,
	moduleID string,
	moduleBytes []byte,
	opts ModuleOptions,
) (RegisterModuleResult, error) {
	r, err := k.kv.Transact(func(tr kv.Transaction) (any, error) {
		_, ok, err := tr.Get(ctx, getModulePartKey(namespace, moduleID, 0))
		if err != nil {
			return nil, err
		}
		if ok {
			return RegisterModuleResult{}, fmt.Errorf(
				"error creating module: %s in namespace: %s, already exists",
				moduleID, namespace)
		}

		rm := registeredModule{
			Bytes: moduleBytes,
			Opts:  opts,
		}
		marshaled, err := json.Marshal(&rm)
		if err != nil {
			return nil, err
		}

		for i := 0; len(marshaled) > 0; i++ {
			// Maximum value size in FoundationDB is 100_000, so split anything larger
			// over multiple KV pairs.
			numBytes := 99_999
			if len(marshaled) < numBytes {
				numBytes = len(marshaled)
			}
			toWrite := marshaled[:numBytes]
			tr.Put(ctx, getModulePartKey(namespace, moduleID, i), toWrite)
			marshaled = marshaled[numBytes:]
		}
		return RegisterModuleResult{}, err
	})
	if err != nil {
		return RegisterModuleResult{}, fmt.Errorf("RegisterModule: error: %w", err)
	}

	return r.(RegisterModuleResult), nil
}

// GetModule gets the bytes and options associated with the provided module.
func (k *kvRegistry) GetModule(
	ctx context.Context,
	namespace,
	moduleID string,
) ([]byte, ModuleOptions, error) {
	key := getModulePrefix(namespace, moduleID)
	r, err := k.kv.Transact(func(tr kv.Transaction) (any, error) {
		var (
			moduleBytes []byte
			i           = 0
		)
		err := tr.IterPrefix(ctx, key, func(k, v []byte) error {
			moduleBytes = append(moduleBytes, v...)
			i++
			return nil
		})
		if err != nil {
			return ModuleOptions{}, err
		}
		if i == 0 {
			return ModuleOptions{}, fmt.Errorf(
				"error getting module: %s, does not exist in namespace: %s",
				moduleID, namespace)
		}

		rm := registeredModule{}
		if err := json.Unmarshal(moduleBytes, &rm); err != nil {
			return ModuleOptions{}, fmt.Errorf("error unmarshaling stored module: %w", err)
		}
		return rm, nil
	})
	if err != nil {
		return nil, ModuleOptions{}, fmt.Errorf("GetModule: error: %w", err)
	}

	result := r.(registeredModule)
	return result.Bytes, result.Opts, nil
}

func (k *kvRegistry) createActor(
	ctx context.Context,
	tr kv.Transaction,
	namespace,
	actorID,
	moduleID string,
	opts types.ActorOptions,
) (CreateActorResult, error) {
	actorKey := getActorKey(namespace, actorID, moduleID)
	_, ok, err := k.getActorBytes(ctx, tr, actorKey)
	if err != nil {
		return CreateActorResult{}, err
	}
	if ok {
		return CreateActorResult{}, fmt.Errorf(
			"error creating actor with ID: %s, already exists in namespace: %s",
			actorID, namespace)
	}

	ra := registeredActor{
		Opts:       opts,
		ModuleID:   moduleID,
		Generation: 1,
	}
	marshaled, err := json.Marshal(&ra)
	if err != nil {
		return CreateActorResult{}, err
	}

	tr.Put(ctx, actorKey, marshaled)
	return CreateActorResult{}, nil
}

func (k *kvRegistry) IncGeneration(
	ctx context.Context,
	namespace,
	actorID string,
	moduleID string,
) error {
	actorKey := getActorKey(namespace, actorID, moduleID)
	_, err := k.kv.Transact(func(tr kv.Transaction) (any, error) {
		ra, ok, err := k.getActor(ctx, tr, actorKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return RegisterModuleResult{}, fmt.Errorf(
				"error incrementing generation for actor with ID: %s, actor does not exist in namespace: %s",
				actorID, namespace)
		}

		ra.Generation++

		marshaled, err := json.Marshal(&ra)
		if err != nil {
			return nil, fmt.Errorf("error marshaling registered actor: %w", err)
		}

		tr.Put(ctx, actorKey, marshaled)

		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("IncGeneration: error: %w", err)
	}

	return nil
}

func (k *kvRegistry) EnsureActivation(
	ctx context.Context,
	namespace,
	actorID string,
	moduleID string,
) ([]types.ActorReference, error) {
	actorKey := getActorKey(namespace, actorID, moduleID)
	references, err := k.kv.Transact(func(tr kv.Transaction) (any, error) {
		ra, ok, err := k.getActor(ctx, tr, actorKey)
		if err == nil && !ok {
			_, err := k.createActor(ctx, tr, namespace, actorID, moduleID, types.ActorOptions{})
			if err != nil {
				return nil, fmt.Errorf("EnsureActivation: error creating actor: %w", err)
			}
			ra, ok, err = k.getActor(ctx, tr, actorKey)
			if err != nil {
				return nil, fmt.Errorf("EnsureActivation: error getting actor: %w", err)
			}
			if !ok {
				return nil, fmt.Errorf(
					"[invariant violated] error ensuring activation of actor with ID: %s, does not exist in namespace: %s, err: %w",
					actorID, namespace, errActorDoesNotExist)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("EnsureActivation: error getting actor: %w", err)
		}
		if !ok {
			// Make sure we use %w to wrap the errActorDoesNotExist so the caller can use
			// errors.Is() on it.
			return nil, fmt.Errorf(
				"[invariant violated] error ensuring activation of actor with ID: %s, does not exist in namespace: %s, err: %w",
				actorID, namespace, errActorDoesNotExist)
		}

		serverKey := getServerKey(ra.Activation.ServerID)
		v, ok, err := tr.Get(ctx, serverKey)
		if err != nil {
			return nil, err
		}

		var (
			server       serverState
			serverExists bool
		)
		if ok {
			if err := json.Unmarshal(v, &server); err != nil {
				return nil, fmt.Errorf("error unmarsaling server state with ID: %s", actorID)
			}
			serverExists = true
		}

		vs, err := tr.GetVersionStamp()
		if err != nil {
			return nil, fmt.Errorf("error getting versionstamp: %w", err)
		}

		var (
			currActivation, activationExists = ra.Activation, ra.Activation.ServerID != ""
			timeSinceLastHeartbeat           = versionSince(vs, server.LastHeartbeatedAt)
			serverID                         string
			serverAddress                    string
			serverVersion                    int64
		)
		if activationExists && serverExists && timeSinceLastHeartbeat < HeartbeatTTL {
			// We have an existing activation and the server is still alive, so just use that.

			// It is acceptable to look up the ServerVersion from the server discovery key directly,
			// as long as the activation is still active, it guarantees that the server's version
			// has not changed since the activation was first created.
			serverVersion = server.ServerVersion
			serverID = currActivation.ServerID
			serverAddress = server.HeartbeatState.Address
		} else {
			// We need to create a new activation.
			liveServers, err := getLiveServers(ctx, vs, tr)
			if err != nil {
				return nil, err
			}
			if len(liveServers) == 0 {
				return nil, fmt.Errorf("0 live servers available for new activation")
			}

			selected := pickServerForActivation(liveServers)
			serverID = selected.ServerID
			serverAddress = selected.HeartbeatState.Address
			serverVersion = selected.ServerVersion
			currActivation = newActivation(serverID, serverVersion)

			ra.Activation = currActivation
			marshaled, err := json.Marshal(&ra)
			if err != nil {
				return nil, fmt.Errorf("error marshaling activation: %w", err)
			}

			tr.Put(ctx, actorKey, marshaled)
		}

		ref, err := types.NewActorReference(serverID, serverVersion, serverAddress, namespace, ra.ModuleID, actorID, ra.Generation)
		if err != nil {
			return nil, fmt.Errorf("error creating new actor reference: %w", err)
		}

		return []types.ActorReference{ref}, nil

	})
	if err != nil {
		return nil, fmt.Errorf("EnsureActivation: error: %w", err)
	}

	return references.([]types.ActorReference), nil
}

func (k *kvRegistry) GetVersionStamp(
	ctx context.Context,
) (int64, error) {
	// GetVersionStamp() is in the critical path of the entire system. It is
	// called extremely frequently. Caching it directly is unsafe and could lead
	// to correctness issues. Instead, we use a singleflight.Group to debounce/batch
	// calls to the underlying storage. This has the same effect as an extremely
	// short TTL cache, but with none of the correctness issues. In effect, the
	// system calls getVersionStamp() in a *single-threaded* loop as fast as it
	// can and each GetVersionStamp() call "gloms on" to the current outstanding
	// call (or initiates the next one if none is ongoing).
	//
	// We pass "" as the key because every call is the same.
	v, err, _ := k.versionStampBatcher.Do("", func() (any, error) {
		return k.kv.Transact(func(tr kv.Transaction) (any, error) {
			return tr.GetVersionStamp()
		})
	})
	if err != nil {
		return -1, fmt.Errorf("GetVersionStamp: error: %w", err)
	}

	return v.(int64), nil
}

func (k *kvRegistry) BeginTransaction(
	ctx context.Context,
	namespace string,
	actorID string,
	moduleID string,
	serverID string,
	serverVersion int64,
) (_ ActorKVTransaction, err error) {
	var kvTr kv.Transaction
	kvTr, err = k.kv.BeginTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("kvRegistry: beginTransaction: error beginning transaction: %w", err)
	}
	defer func() {
		if err != nil {
			kvTr.Cancel(ctx)
		}
	}()

	actorKey := getActorKey(namespace, actorID, moduleID)
	ra, ok, err := k.getActor(ctx, kvTr, actorKey)
	if err != nil {
		return nil, fmt.Errorf("kvRegistry: beginTransaction: error getting actor key: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("kvRegistry: beginTransaction: cannot perform KV Get for actor: %s(%s) that does not exist", actorID, moduleID)
	}

	// We treat the tuple of <ServerID, ServerVersion> as a fencing token for all
	// actor KV storage operations. This guarantees that actor KV storage behaves in
	// a linearizable manner because an actor can only ever be activated on one server
	// in the registry at a given time, therefore linearizability is maintained as long
	// as we check that the server performing the KV storage transaction is also the
	// server responsible for the actor's current activation.
	if ra.Activation.ServerID != serverID {
		return nil, fmt.Errorf(
			"kvRegistry: beginTransaction: cannot begin transaction because server IDs do not match: %s != %s",
			ra.Activation.ServerID, serverID)
	}
	if ra.Activation.ServerVersion != serverVersion {
		return nil, fmt.Errorf(
			"kvRegistry: beginTransaction: cannot begin transaction because server versions do not match: %d != %d",
			ra.Activation.ServerVersion, serverVersion)
	}

	tr := newKVTransaction(ctx, namespace, actorID, moduleID, kvTr)
	return tr, nil
}

func (k *kvRegistry) Heartbeat(
	ctx context.Context,
	serverID string,
	heartbeatState HeartbeatState,
) (HeartbeatResult, error) {
	key := getServerKey(serverID)
	var serverVersion int64
	versionStamp, err := k.kv.Transact(func(tr kv.Transaction) (any, error) {
		// First do all the logic to update the server's heartbeat state.
		v, ok, err := tr.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("error getting server state: %w", err)
		}

		var state serverState
		if !ok {
			serverVersion = 1
			state = serverState{
				ServerID:      serverID,
				ServerVersion: serverVersion,
			}
			vs, err := tr.GetVersionStamp()
			if err != nil {
				return nil, fmt.Errorf("error getting versionstamp: %w", err)
			}
			state.LastHeartbeatedAt = vs
		} else {
			if err := json.Unmarshal(v, &state); err != nil {
				return nil, fmt.Errorf("error unmarshaling server state: %w", err)
			}
		}

		vs, err := tr.GetVersionStamp()
		if err != nil {
			return nil, fmt.Errorf("error getting versionstamp: %w", err)
		}
		timeSinceLastHeartbeat := versionSince(vs, state.LastHeartbeatedAt)
		if timeSinceLastHeartbeat >= HeartbeatTTL {
			state.ServerVersion++
		}

		serverVersion = state.ServerVersion
		state.LastHeartbeatedAt = vs
		state.HeartbeatState = heartbeatState

		marshaled, err := json.Marshal(&state)
		if err != nil {
			return nil, fmt.Errorf("error marshaling server state: %w", err)
		}

		tr.Put(ctx, key, marshaled)

		// Next, check if we should ask the server to shed some load to force
		// rebalancing.
		//
		// TODO: Unit test this logic in this package.
		liveServers, err := getLiveServers(ctx, vs, tr)
		if err != nil {
			return nil, fmt.Errorf("error getting live servers during heartbeat for load balancing: %w", err)
		}

		result := HeartbeatResult{
			VersionStamp: vs,
			// VersionStamp corresponds to ~ 1 million increments per second.
			HeartbeatTTL:  int64(HeartbeatTTL.Microseconds()),
			ServerVersion: serverVersion,
		}

		min, max := minMaxMemUsage(liveServers)
		delta := max.HeartbeatState.UsedMemory - min.HeartbeatState.UsedMemory
		if delta > RebalanceMemoryThreshold && max.ServerID == serverID {
			// If the server currently heartbeating is also the server with the highest memory usage
			// and its memory usage delta vs. the server with the lowest memory usage is above the
			// threshold, ask it to shed some actors to balance memory usage.
			result.MemoryBytesToShed = int64(delta)
		}

		return vs, nil
	})
	if err != nil {
		return HeartbeatResult{}, fmt.Errorf("Heartbeat: error: %w", err)
	}
	return HeartbeatResult{
		VersionStamp: versionStamp.(int64),
		// VersionStamp corresponds to ~ 1 million increments per second.
		HeartbeatTTL:  int64(HeartbeatTTL.Microseconds()),
		ServerVersion: serverVersion,
	}, nil
}

func (k *kvRegistry) Close(ctx context.Context) error {
	return k.kv.Close(ctx)
}

func (k *kvRegistry) UnsafeWipeAll() error {
	return k.kv.UnsafeWipeAll()
}

func (k *kvRegistry) getActorBytes(
	ctx context.Context,
	tr kv.Transaction,
	actorKey []byte,
) ([]byte, bool, error) {
	actorBytes, ok, err := tr.Get(ctx, actorKey)
	if err != nil {
		return nil, false, fmt.Errorf(
			"error getting actor bytes for key: %s", string(actorBytes))
	}
	if !ok {
		return nil, false, nil
	}
	return actorBytes, true, nil
}

func (k *kvRegistry) getActor(
	ctx context.Context,
	tr kv.Transaction,
	actorKey []byte,
) (registeredActor, bool, error) {
	actorBytes, ok, err := k.getActorBytes(ctx, tr, actorKey)
	if err != nil {
		return registeredActor{}, false, err
	}
	if !ok {
		return registeredActor{}, false, nil
	}

	var ra registeredActor
	if err := json.Unmarshal(actorBytes, &ra); err != nil {
		return registeredActor{}, false, fmt.Errorf("error unmarshaling registered actor: %w", err)
	}

	return ra, true, nil
}

func getModulePrefix(namespace, moduleID string) []byte {
	return tuple.Tuple{namespace, "modules", moduleID}.Pack()
}

func getModulePartKey(namespace, moduleID string, part int) []byte {
	return tuple.Tuple{namespace, "modules", moduleID, part}.Pack()
}

func getActorKey(namespace, actorID, moduleID string) []byte {
	return tuple.Tuple{namespace, "actors", moduleID, actorID, "state"}.Pack()
}

func getActoKVKey(namespace, actorID string, moduleID string, key []byte) []byte {
	return tuple.Tuple{namespace, "actors", moduleID, actorID, "kv", key}.Pack()
}

func getServerKey(serverID string) []byte {
	return tuple.Tuple{"servers", serverID}.Pack()
}

func getServersPrefix() []byte {
	return tuple.Tuple{"servers"}.Pack()
}

type registeredActor struct {
	Opts       types.ActorOptions
	ModuleID   string
	Generation uint64
	Activation activation
}

type registeredModule struct {
	Bytes []byte
	Opts  ModuleOptions
}

type serverState struct {
	ServerID          string
	LastHeartbeatedAt int64
	HeartbeatState    HeartbeatState
	ServerVersion     int64
}

type activation struct {
	ServerID      string
	ServerVersion int64
}

func newActivation(serverID string, serverVersion int64) activation {
	return activation{
		ServerID:      serverID,
		ServerVersion: serverVersion,
	}
}

func versionSince(curr, prev int64) time.Duration {
	since := curr - prev
	if since < 0 {
		panic(fmt.Sprintf(
			"prev: %d, curr: %d, versionstamp did not increase monotonically",
			prev, curr))
	}
	return time.Duration(since) * time.Microsecond
}

type kvTransaction struct {
	namespace string
	actorID   string
	moduleID  string
	tr        kv.Transaction
}

func newKVTransaction(
	ctx context.Context,
	namespace string,
	actorID string,
	moduleID string,
	tr kv.Transaction,
) *kvTransaction {
	return &kvTransaction{
		namespace: namespace,
		actorID:   actorID,
		moduleID:  moduleID,
		tr:        tr,
	}
}

func (tr *kvTransaction) Get(
	ctx context.Context,
	key []byte,
) ([]byte, bool, error) {
	actorKVKey := getActoKVKey(tr.namespace, tr.actorID, tr.moduleID, key)
	return tr.tr.Get(ctx, actorKVKey)
}

func (tr *kvTransaction) Put(
	ctx context.Context,
	key []byte,
	value []byte,
) error {
	actorKVKey := getActoKVKey(tr.namespace, tr.actorID, tr.moduleID, key)
	return tr.tr.Put(ctx, actorKVKey, value)
}

func (tr *kvTransaction) Commit(ctx context.Context) error {
	return tr.tr.Commit(ctx)
}

func (tr *kvTransaction) Cancel(ctx context.Context) error {
	return tr.tr.Cancel(ctx)
}

func getLiveServers(
	ctx context.Context,
	versionStamp int64,
	tr kv.Transaction,
) ([]serverState, error) {
	liveServers := []serverState{}
	err := tr.IterPrefix(ctx, getServersPrefix(), func(k, v []byte) error {
		var currServer serverState
		if err := json.Unmarshal(v, &currServer); err != nil {
			return fmt.Errorf("error unmarshaling server state: %w", err)
		}

		if versionSince(versionStamp, currServer.LastHeartbeatedAt) < HeartbeatTTL {
			liveServers = append(liveServers, currServer)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return liveServers, nil
}

func pickServerForActivation(available []serverState) serverState {
	if len(available) == 0 {
		panic("[invariant violated] pickServerForActivation should not be called with empty slice")
	}

	minMemUsage, maxMemUsage := minMaxMemUsage(available)
	if maxMemUsage.HeartbeatState.UsedMemory-minMemUsage.HeartbeatState.UsedMemory > RebalanceMemoryThreshold {
		// If there is a large enough delta in memory usage between the minimum and maximum
		// memory usage of servers in the cluster, assign new actors to the server with the
		// lowest memory usage.
		return minMemUsage
	}

	// Otherwise just try and keep an even balance in terms of the # of actors on each server.
	sort.Slice(available, func(i, j int) bool {
		return available[i].HeartbeatState.NumActivatedActors < available[j].HeartbeatState.NumActivatedActors
	})

	return available[0]
}

func minMaxMemUsage(available []serverState) (serverState, serverState) {
	if len(available) == 0 {
		panic("[invariant violated] pickServerForActivation should not be called with empty slice")
	}

	var (
		minMemUsage serverState
		maxMemUsage serverState
	)
	for _, s := range available {
		if s.HeartbeatState.UsedMemory < minMemUsage.HeartbeatState.UsedMemory {
			minMemUsage = s
		}
		if s.HeartbeatState.UsedMemory > maxMemUsage.HeartbeatState.UsedMemory {
			maxMemUsage = s
		}
	}

	return minMemUsage, maxMemUsage
}
