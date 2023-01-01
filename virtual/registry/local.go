package registry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/richardartoul/nola/virtual/types"
)

type local struct {
	sync.Mutex

	// State.
	m       map[string][]byte
	actors  map[types.NamespacedID]registeredActor
	modules map[types.NamespacedID]registeredModule
	servers map[string]serverState
}

// NewLocal creates a new local (in-memory) registry. It is primarily used for
// tests and simple benchmarking.
func NewLocal() Registry {
	return newValidatedRegistry(&local{
		m:       make(map[string][]byte),
		actors:  make(map[types.NamespacedID]registeredActor),
		modules: make(map[types.NamespacedID]registeredModule),
		servers: make(map[string]serverState),
	})
}

func (l *local) RegisterModule(
	ctx context.Context,
	namespace,
	moduleID string,
	moduleBytes []byte,
	opts ModuleOptions,
) (RegisterModuleResult, error) {
	l.Lock()
	defer l.Unlock()

	nsModID := types.NewNamespacedID(namespace, moduleID)
	if _, ok := l.modules[nsModID]; ok {
		return RegisterModuleResult{}, fmt.Errorf(
			"error creating module: %s in namespace: %s, already exists",
			moduleID, namespace)
	}

	l.modules[nsModID] = registeredModule{
		bytes: moduleBytes,
		opts:  opts,
	}

	return RegisterModuleResult{}, nil
}

// GetModule gets the bytes and options associated with the provided module.
func (l *local) GetModule(
	ctx context.Context,
	namespace,
	moduleID string,
) ([]byte, ModuleOptions, error) {
	l.Lock()
	defer l.Unlock()

	nsModID := types.NewNamespacedID(namespace, moduleID)
	module, ok := l.modules[nsModID]
	if !ok {
		return nil, ModuleOptions{}, fmt.Errorf(
			"error getting module: %s, does not exist in namespace: %s",
			moduleID, namespace)
	}

	return module.bytes, module.opts, nil
}

func (l *local) CreateActor(
	ctx context.Context,
	namespace,
	actorID,
	moduleID string,
	opts ActorOptions,
) (CreateActorResult, error) {
	l.Lock()
	defer l.Unlock()

	nsActorID := types.NewNamespacedID(namespace, actorID)
	if _, ok := l.actors[nsActorID]; ok {
		return CreateActorResult{}, fmt.Errorf(
			"error creating actor with ID: %s, already exists in namespace: %s",
			actorID, namespace)
	}

	if _, ok := l.modules[types.NewNamespacedID(namespace, moduleID)]; !ok {
		return CreateActorResult{}, fmt.Errorf(
			"error creating actor, module: %s does not exist in namespace: %s",
			moduleID, namespace)
	}

	l.actors[nsActorID] = registeredActor{
		opts:     opts,
		moduleID: moduleID,
	}

	return CreateActorResult{}, nil
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

	actor.generation++
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

	return []types.ActorReference{
		types.NewLocalReference(namespace, actorID, actor.moduleID, actor.generation),
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
		state = serverState{}
	}
	state.lastHeartbeatedAt = time.Now()
	state.heartbeatState = heartbeatState
	l.servers[serverID] = state

	return nil
}

type registeredActor struct {
	opts       ActorOptions
	moduleID   string
	generation uint64
}

type registeredModule struct {
	bytes []byte
	opts  ModuleOptions
}

type serverState struct {
	lastHeartbeatedAt time.Time
	heartbeatState    HeartbeatState
}
