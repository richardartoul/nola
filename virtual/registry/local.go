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
	modules map[types.NamespacedID]registeredModule
	// Contains all actors ever created. Key is actor ID.
	actors map[types.NamespacedID]registeredActor
	// Contain the last known activation for every actor ever activated.
	// Key is actor ID.
	activations map[types.NamespacedID]activation
	// Contains all servers that have ever heartbeated.
	servers map[string]serverState
}

// NewLocal creates a new local (in-memory) registry. It is primarily used for
// tests and simple benchmarking.
func NewLocal() Registry {
	return newValidatedRegistry(&local{
		m:           make(map[string][]byte),
		kv:          newLocalKV(),
		modules:     make(map[types.NamespacedID]registeredModule),
		actors:      make(map[types.NamespacedID]registeredActor),
		activations: make(map[types.NamespacedID]activation),
		servers:     make(map[string]serverState),
	})
}

func (l *local) RegisterModule(
	ctx context.Context,
	namespace,
	moduleID string,
	moduleBytes []byte,
	opts ModuleOptions,
) (RegisterModuleResult, error) {
	key := tuple.Tuple{namespace, "modules", moduleID}.Pack()
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
	key := tuple.Tuple{namespace, "modules", moduleID}.Pack()
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
		actorKey  = tuple.Tuple{namespace, "actors", actorID}.Pack()
		moduleKey = tuple.Tuple{namespace, "modules", moduleID}.Pack()
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

func (l *local) IncGeneration(
	ctx context.Context,
	namespace,
	actorID string,
) error {
	l.Lock()
	defer l.Unlock()

	nsActorID := types.NewNamespacedID(namespace, actorID)
	actor, ok := l.actors[nsActorID]
	if !ok {
		return fmt.Errorf(
			"error incrementing generation for actor with ID: %s, actor does not exist in namespace: %s",
			actorID, namespace)
	}

	actor.Generation++
	l.actors[nsActorID] = actor
	return nil
}

func (l *local) EnsureActivation(
	ctx context.Context,
	namespace,
	actorID string,
) ([]types.ActorReference, error) {
	l.Lock()
	defer l.Unlock()

	nsActorID := types.NewNamespacedID(namespace, actorID)
	actor, ok := l.actors[nsActorID]
	if !ok {
		return nil, fmt.Errorf(
			"error ensuring activation of actor with ID: %s, does not exist in namespace: %s",
			actorID, namespace)
	}

	var (
		currActivation, activationExists = l.activations[nsActorID]
		server, serverExists             = l.servers[currActivation.serverID]
		timeSinceLastHeartbeat           = time.Since(server.lastHeartbeatedAt)
		serverID                         string
		serverAddress                    string
	)
	if activationExists && serverExists && timeSinceLastHeartbeat < MaxHeartbeatDelay {
		// We have an existing activation and the server is still alive, so just use that.
		serverID = currActivation.serverID
		serverAddress = l.servers[currActivation.serverID].heartbeatState.Address
	} else {
		// We need to create a new activation.
		liveServers := []serverState{}
		for _, server := range l.servers {
			if time.Since(server.lastHeartbeatedAt) < MaxHeartbeatDelay {
				liveServers = append(liveServers, server)
			}
		}
		if len(liveServers) == 0 {
			return nil, fmt.Errorf("0 live servers available for new activation")
		}

		// Pick the server with the lowest current number of activated actors to try and load-balance.
		// TODO: This is obviously insufficient and we should take other factors into account like
		//       memory / CPU usage.
		// TODO: We should also have some hard limits and just reject new activations at some point.
		sort.Slice(liveServers, func(i, j int) bool {
			return liveServers[i].heartbeatState.NumActivatedActors < liveServers[j].heartbeatState.NumActivatedActors
		})

		serverID = liveServers[0].serverID
		serverAddress = liveServers[0].heartbeatState.Address
		currActivation = activation{serverID: serverID}
		l.activations[nsActorID] = currActivation
	}

	return []types.ActorReference{
		types.NewLocalReference(serverID, serverAddress, namespace, actorID, actor.ModuleID, actor.Generation),
	}, nil
}

func (l *local) ActorKVPut(
	ctx context.Context,
	namespace string,
	actorID string,
	key []byte,
	value []byte,
) error {
	l.Lock()
	defer l.Unlock()

	nsActorID := types.NewNamespacedID(namespace, actorID)
	if _, ok := l.actors[nsActorID]; !ok {
		return fmt.Errorf(
			"error performing PUT for actor: %s with key: %s, actor does not exist in namespace: %s",
			actorID, key, namespace)
	}

	// Copy the key and value in case they are reused.
	l.m[string(key)] = append([]byte(nil), value...)
	return nil
}

func (l *local) ActorKVGet(
	ctx context.Context,
	namespace string,
	actorID string,
	key []byte,
) ([]byte, bool, error) {
	l.Lock()
	defer l.Unlock()

	nsActorID := types.NewNamespacedID(namespace, actorID)
	if _, ok := l.actors[nsActorID]; !ok {
		return nil, false, fmt.Errorf(
			"error performing GET for actor: %s with key: %s, actor does not exist in namespace: %s",
			actorID, key, namespace)
	}

	v, ok := l.m[string(key)]
	return v, ok, nil
}

func (l *local) Heartbeat(
	ctx context.Context,
	serverID string,
	heartbeatState HeartbeatState,
) error {
	l.Lock()
	defer l.Unlock()

	state, ok := l.servers[serverID]
	if !ok {
		state = serverState{
			serverID: serverID,
		}
	}
	state.lastHeartbeatedAt = time.Now()
	state.heartbeatState = heartbeatState

	l.servers[serverID] = state

	return nil
}

type registeredActor struct {
	Opts       ActorOptions
	ModuleID   string
	Generation uint64
}

type registeredModule struct {
	Bytes []byte
	Opts  ModuleOptions
}

type serverState struct {
	serverID          string
	lastHeartbeatedAt time.Time
	heartbeatState    HeartbeatState
}

type activation struct {
	serverID string
}
