package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/richardartoul/nola/virtual/types"
)

const (
	// MaxHeartbeatDelay is the maximum amount of time between server heartbeats before
	// the registry will consider a server as dead.
	//
	// TODO: Should be configurable.
	MaxHeartbeatDelay = 5 * time.Second
)

type kvRegistry struct {
	// State.
	kv kv
}

func newKVRegistry(kv kv) Registry {
	return &kvRegistry{
		kv: kv,
	}
}

// TODO: Add compression?
func (k *kvRegistry) RegisterModule(
	ctx context.Context,
	namespace,
	moduleID string,
	moduleBytes []byte,
	opts ModuleOptions,
) (RegisterModuleResult, error) {
	r, err := k.kv.transact(func(tr transaction) (any, error) {
		_, ok, err := tr.get(k.getModulePartKey(namespace, moduleID, 0))
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
			// Maximum valeu size in FoundationDB is 100_000, so split anything larger
			// over multiple KV pairs.
			numBytes := 99_999
			if len(marshaled) < numBytes {
				numBytes = len(marshaled)
			}
			toWrite := marshaled[:numBytes]
			tr.put(k.getModulePartKey(namespace, moduleID, i), toWrite)
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
	key := k.getModulePrefix(namespace, moduleID)
	r, err := k.kv.transact(func(tr transaction) (any, error) {
		var (
			moduleBytes []byte
			i           = 0
		)
		err := tr.iterPrefix(key, func(k, v []byte) error {
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

func (k *kvRegistry) CreateActor(
	ctx context.Context,
	namespace,
	actorID,
	moduleID string,
	opts ActorOptions,
) (CreateActorResult, error) {
	var (
		actorKey  = k.getActorKey(namespace, actorID)
		moduleKey = k.getModulePartKey(namespace, moduleID, 0)
	)
	r, err := k.kv.transact(func(tr transaction) (any, error) {
		_, ok, err := tr.get(actorKey)
		if err != nil {
			return nil, err
		}
		if ok {
			return RegisterModuleResult{}, fmt.Errorf(
				"error creating actor with ID: %s, already exists in namespace: %s",
				actorID, namespace)
		}

		_, ok, err = tr.get(moduleKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return RegisterModuleResult{}, fmt.Errorf(
				"error creating actor, module: %s does not exist in namespace: %s",
				moduleID, namespace)
		}

		ra := registeredActor{
			Opts:     opts,
			ModuleID: moduleID,
		}
		marshaled, err := json.Marshal(&ra)
		if err != nil {
			return nil, err
		}

		tr.put(actorKey, marshaled)
		return CreateActorResult{}, err
	})
	if err != nil {
		return CreateActorResult{}, fmt.Errorf("CreateActor: error: %w", err)
	}

	return r.(CreateActorResult), nil
}

func (k *kvRegistry) IncGeneration(
	ctx context.Context,
	namespace,
	actorID string,
) error {
	actorKey := k.getActorKey(namespace, actorID)
	_, err := k.kv.transact(func(tr transaction) (any, error) {
		actorBytes, ok, err := tr.get(actorKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return RegisterModuleResult{}, fmt.Errorf(
				"error incrementing generation for actor with ID: %s, actor does not exist in namespace: %s",
				actorID, namespace)
		}

		var ra registeredActor
		if err := json.Unmarshal(actorBytes, &ra); err != nil {
			return nil, fmt.Errorf("error unmarshaling registered actor: %w", err)
		}

		ra.Generation++

		marshaled, err := json.Marshal(&ra)
		if err != nil {
			return nil, fmt.Errorf("error marshaling registered actor: %w", err)
		}

		tr.put(actorKey, marshaled)

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
) ([]types.ActorReference, error) {
	actorKey := k.getActorKey(namespace, actorID)
	references, err := k.kv.transact(func(tr transaction) (any, error) {
		v, ok, err := tr.get(actorKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf(
				"error ensuring activation of actor with ID: %s, does not exist in namespace: %s",
				actorID, namespace)
		}

		var ra registeredActor
		if err := json.Unmarshal(v, &ra); err != nil {
			return nil, fmt.Errorf("error unmarsaling registered actor with ID: %s", actorID)
		}

		serverKey := k.getServerKey(ra.Activation.ServerID)
		v, ok, err = tr.get(serverKey)
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

		var (
			currActivation, activationExists = ra.Activation, ra.Activation.ServerID != ""
			timeSinceLastHeartbeat           = time.Since(server.LastHeartbeatedAt)
			serverID                         string
			serverAddress                    string
		)
		if activationExists && serverExists && timeSinceLastHeartbeat < MaxHeartbeatDelay {
			// We have an existing activation and the server is still alive, so just use that.
			serverID = currActivation.ServerID
			serverAddress = server.HeartbeatState.Address
		} else {
			// We need to create a new activation.
			liveServers := []serverState{}
			err = tr.iterPrefix(k.getServersPrefix(), func(k, v []byte) error {
				var currServer serverState
				if err := json.Unmarshal(v, &currServer); err != nil {
					return fmt.Errorf("error unmarshaling server state: %w", err)
				}

				if time.Since(currServer.LastHeartbeatedAt) < MaxHeartbeatDelay {
					liveServers = append(liveServers, currServer)
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			if len(liveServers) == 0 {
				return nil, fmt.Errorf("0 live servers available for new activation")
			}

			// Pick the server with the lowest current number of activated actors to try and load-balance.
			// TODO: This is obviously insufficient and we should take other factors into account like
			//       memory / CPU usage.
			// TODO: We should also have some hard limits and just reject new activations at some point.
			sort.Slice(liveServers, func(i, j int) bool {
				return liveServers[i].HeartbeatState.NumActivatedActors < liveServers[j].HeartbeatState.NumActivatedActors
			})

			serverID = liveServers[0].ServerID
			serverAddress = liveServers[0].HeartbeatState.Address
			currActivation = activation{ServerID: serverID}

			ra.Activation = currActivation
			marshaled, err := json.Marshal(&ra)
			if err != nil {
				return nil, fmt.Errorf("error marshaling activation: %w", err)
			}

			tr.put(actorKey, marshaled)
		}

		return []types.ActorReference{
			types.NewLocalReference(serverID, serverAddress, namespace, actorID, ra.ModuleID, ra.Generation),
		}, nil

	})
	if err != nil {
		return nil, fmt.Errorf("EnsureActivation: error: %w", err)
	}

	return references.([]types.ActorReference), nil
}

func (k *kvRegistry) ActorKVPut(
	ctx context.Context,
	namespace string,
	actorID string,
	key []byte,
	value []byte,
) error {
	var (
		actorKey = k.getActorKey(namespace, actorID)
		kvKey    = k.getActoKVKey(namespace, actorID, key)
	)
	_, err := k.kv.transact(func(tr transaction) (any, error) {
		// TODO: This is an expensive check to run each time, consider removing this if it becomes
		//       a bottleneck.
		_, ok, err := tr.get(actorKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("cannot perform KV Put for actor: %s that does not exist", actorID)
		}

		tr.put(kvKey, value)
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("ActorKVPUT: error: %w", err)
	}

	return nil
}

func (k *kvRegistry) ActorKVGet(
	ctx context.Context,
	namespace string,
	actorID string,
	key []byte,
) ([]byte, bool, error) {
	var (
		actorKey = k.getActorKey(namespace, actorID)
		kvKey    = k.getActoKVKey(namespace, actorID, key)
	)
	result, err := k.kv.transact(func(tr transaction) (any, error) {
		// TODO: This is an expensive check to run each time, consider removing this if it becomes
		//       a bottleneck.
		_, ok, err := tr.get(actorKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("cannot perform KV Get for actor: %s that does not exist", actorID)
		}

		v, ok, err := tr.get(kvKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}

		return v, nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("ActorKVGet: error: %w", err)
	}
	if result == nil {
		return nil, false, nil
	}
	return result.([]byte), true, nil
}

func (k *kvRegistry) Heartbeat(
	ctx context.Context,
	serverID string,
	heartbeatState HeartbeatState,
) (HeartbeatResult, error) {
	key := k.getServerKey(serverID)
	versionStamp, err := k.kv.transact(func(tr transaction) (any, error) {
		v, ok, err := tr.get(key)
		if err != nil {
			return nil, err
		}

		var state serverState
		if !ok {
			state = serverState{
				ServerID: serverID,
			}
		} else {
			if err := json.Unmarshal(v, &state); err != nil {
				return nil, err
			}
		}

		state.LastHeartbeatedAt = time.Now()
		state.HeartbeatState = heartbeatState

		marshaled, err := json.Marshal(&state)
		if err != nil {
			return nil, err
		}

		tr.put(key, marshaled)

		return tr.getVersionStamp()
	})
	if err != nil {
		return HeartbeatResult{}, fmt.Errorf("Heartbeat: error: %w", err)
	}
	return HeartbeatResult{VersionStamp: versionStamp.(int64)}, nil
}

func (k *kvRegistry) Close(ctx context.Context) error {
	return k.kv.close(ctx)
}

func (k *kvRegistry) UnsafeWipeAll() error {
	return k.kv.unsafeWipeAll()
}

func (k *kvRegistry) getModulePrefix(namespace, moduleID string) []byte {
	return tuple.Tuple{namespace, "modules", moduleID}.Pack()
}

func (k *kvRegistry) getModulePartKey(namespace, moduleID string, part int) []byte {
	return tuple.Tuple{namespace, "modules", moduleID, part}.Pack()
}

func (k *kvRegistry) getActorKey(namespace, actorID string) []byte {
	return tuple.Tuple{namespace, "actors", actorID, "state"}.Pack()
}

func (k *kvRegistry) getActoKVKey(namespace, actorID string, key []byte) []byte {
	return tuple.Tuple{namespace, "actors", actorID, "kv", key}.Pack()
}

func (k *kvRegistry) getServerKey(serverID string) []byte {
	return tuple.Tuple{"servers", serverID}.Pack()
}

func (k *kvRegistry) getServersPrefix() []byte {
	return tuple.Tuple{"servers"}.Pack()
}

type registeredActor struct {
	Opts       ActorOptions
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
	LastHeartbeatedAt time.Time
	HeartbeatState    HeartbeatState
}

type activation struct {
	ServerID string
}
