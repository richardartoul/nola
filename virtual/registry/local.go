package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
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

type local struct {
	sync.Mutex

	// State.
	m  map[string][]byte
	kv kv

	// Contains all known registered modules. Key is module ID.
	// modules map[types.NamespacedID]registeredModule
	// Contains all actors ever created. Key is actor ID.
	// actors map[types.NamespacedID]registeredActor
	// Contain the last known activation for every actor ever activated.
	// Key is actor ID.
	// activations map[types.NamespacedID]activation
	// Contains all servers that have ever heartbeated.
	// servers map[string]serverState
}

// NewLocal creates a new local (in-memory) registry. It is primarily used for
// tests and simple benchmarking.
func NewLocal() Registry {
	return newValidatedRegistry(&local{
		m:  make(map[string][]byte),
		kv: newLocalKV(),
		// modules:     make(map[types.NamespacedID]registeredModule),
		// actors:      make(map[types.NamespacedID]registeredActor),
		// activations: make(map[types.NamespacedID]activation),
		// servers: make(map[string]serverState),
	})
}

func (l *local) RegisterModule(
	ctx context.Context,
	namespace,
	moduleID string,
	moduleBytes []byte,
	opts ModuleOptions,
) (RegisterModuleResult, error) {
	key := l.getModuleKey(namespace, moduleID)
	r, err := l.kv.transact(func(tr transaction) (any, error) {
		_, ok, err := tr.get(key)
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

		tr.put(key, marshaled)
		return RegisterModuleResult{}, err
	})
	if err != nil {
		return RegisterModuleResult{}, err
	}

	return r.(RegisterModuleResult), nil
}

// GetModule gets the bytes and options associated with the provided module.
func (l *local) GetModule(
	ctx context.Context,
	namespace,
	moduleID string,
) ([]byte, ModuleOptions, error) {
	key := l.getModuleKey(namespace, moduleID)
	r, err := l.kv.transact(func(tr transaction) (any, error) {
		v, ok, err := tr.get(key)
		if err != nil {
			return ModuleOptions{}, err
		}
		if !ok {
			return ModuleOptions{}, fmt.Errorf(
				"error getting module: %s, does not exist in namespace: %s",
				moduleID, namespace)
		}

		rm := registeredModule{}
		if err := json.Unmarshal(v, &rm); err != nil {
			return ModuleOptions{}, fmt.Errorf("error unmarshaling stored module: %w", err)
		}
		return rm, nil
	})
	if err != nil {
		return nil, ModuleOptions{}, err
	}

	result := r.(registeredModule)
	return result.Bytes, result.Opts, nil
}

func (l *local) CreateActor(
	ctx context.Context,
	namespace,
	actorID,
	moduleID string,
	opts ActorOptions,
) (CreateActorResult, error) {
	var (
		actorKey  = l.getActorKey(namespace, actorID)
		moduleKey = l.getModuleKey(namespace, moduleID)
	)
	r, err := l.kv.transact(func(tr transaction) (any, error) {
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
		return CreateActorResult{}, err
	}

	return r.(CreateActorResult), nil
}

// func (l *local) actorExists(namespace, actorID string, tr transaction) (bool, error) {
// 	actorKey := tuple.Tuple{namespace, "actors", actorID}.Pack()
// 	_, ok, err := tr.get(actorKey)
// 	if err != nil {
// 		return false, err
// 	}

// 	return ok,nil
// }

// TODO: Make sure this function is tested.
func (l *local) IncGeneration(
	ctx context.Context,
	namespace,
	actorID string,
) error {
	actorKey := l.getActorKey(namespace, actorID)
	_, err := l.kv.transact(func(tr transaction) (any, error) {
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
		return err
	}

	return nil
}

func (l *local) EnsureActivation(
	ctx context.Context,
	namespace,
	actorID string,
) ([]types.ActorReference, error) {
	actorKey := l.getActorKey(namespace, actorID)
	references, err := l.kv.transact(func(tr transaction) (any, error) {
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

		serverKey := l.getServerKey(ra.Activation.ServerID)
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
			err = tr.iterPrefix(l.getServersPrefix(), func(k, v []byte) error {
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
		return nil, err
	}

	return references.([]types.ActorReference), nil
}

func (l *local) ActorKVPut(
	ctx context.Context,
	namespace string,
	actorID string,
	key []byte,
	value []byte,
) error {
	var (
		actorKey  = l.getActorKey(namespace, actorID)
		packedKey = tuple.Tuple{namespace, "kv", "actors", actorID}.Pack()
	)
	_, err := l.kv.transact(func(tr transaction) (any, error) {
		// TODO: This is an expensive check to run each time, consider removing this if it becomes
		//       a bottleneck.
		_, ok, err := tr.get(actorKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("cannot perform KV Put for actor: %s that does not exist", actorID)
		}

		tr.put(packedKey, value)
		return nil, nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (l *local) ActorKVGet(
	ctx context.Context,
	namespace string,
	actorID string,
	key []byte,
) ([]byte, bool, error) {
	var (
		actorKey  = l.getActorKey(namespace, actorID)
		packedKey = tuple.Tuple{namespace, "kv", "actors", actorID}.Pack()
	)
	result, err := l.kv.transact(func(tr transaction) (any, error) {
		// TODO: This is an expensive check to run each time, consider removing this if it becomes
		//       a bottleneck.
		_, ok, err := tr.get(actorKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("cannot perform KV Put for actor: %s that does not exist", actorID)
		}

		v, ok, err := tr.get(packedKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}

		return v, nil
	})
	if err != nil {
		return nil, false, err
	}
	if result == nil {
		return nil, false, nil
	}
	return result.([]byte), true, nil
}

func (l *local) Heartbeat(
	ctx context.Context,
	serverID string,
	heartbeatState HeartbeatState,
) error {
	key := l.getServerKey(serverID)
	_, err := l.kv.transact(func(tr transaction) (any, error) {
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

		return nil, nil
	})
	return err
}

func (l *local) getModuleKey(namespace, moduleID string) []byte {
	return tuple.Tuple{namespace, "modules", moduleID}.Pack()
}

func (l *local) getActorKey(namespace, actorID string) []byte {
	return tuple.Tuple{namespace, "actors", actorID}.Pack()
}

func (l *local) getServerKey(serverID string) []byte {
	return tuple.Tuple{"servers", serverID}.Pack()
}

func (l *local) getServersPrefix() []byte {
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
