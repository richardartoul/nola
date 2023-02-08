package virtual

import (
	"context"
	"fmt"
	"sync"

	"github.com/richardartoul/nola/durable/durablewazero"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"
	"github.com/wapc/wapc-go/engines/wazero"
)

type activations struct {
	sync.RWMutex

	// State.
	_modules    map[types.NamespacedID]Module
	_actors     map[types.NamespacedID]activatedActor
	serverState struct {
		sync.RWMutex
		serverID      string
		serverVersion int64
	}

	// Dependencies.
	registry      registry.Registry
	environment   Environment
	goModules     map[types.NamespacedIDNoType]Module
	customHostFns map[string]func([]byte) ([]byte, error)
}

func newActivations(
	registry registry.Registry,
	environment Environment,
	goModules map[types.NamespacedIDNoType]Module,
	customHostFns map[string]func([]byte) ([]byte, error),
) *activations {
	return &activations{
		_modules: make(map[types.NamespacedID]Module),
		_actors:  make(map[types.NamespacedID]activatedActor),

		registry:      registry,
		environment:   environment,
		goModules:     goModules,
		customHostFns: customHostFns,
	}
}

// invoke has a lot of manual locking and unlocking. While error prone, this is intentional
// as we need to avoid holding the lock in certain paths that may end up doing expensive
// or high latency operations. In addition, we need to ensure that the lock is not held while
// actor.o.Invoke() is called because it may run for a long time, but also to avoid deadlocks
// when one actor ends up invoking a function on another actor running in the same environment.
func (a *activations) invoke(
	ctx context.Context,
	reference types.ActorReferenceVirtual,
	operation string,
	payload []byte,
) ([]byte, error) {
	a.RLock()
	actor, ok := a._actors[reference.ActorID()]
	if ok && actor.reference.Generation() >= reference.Generation() {
		a.RUnlock()
		return actor.invoke(ctx, operation, payload)
	}
	a.RUnlock()

	a.Lock()
	if ok && actor.reference.Generation() >= reference.Generation() {
		a.Unlock()
		return actor.invoke(ctx, operation, payload)
	}

	if ok && actor.reference.Generation() < reference.Generation() {
		// The actor is already activated, however, the generation count has
		// increased. Therefore we need to pretend like the actor doesn't
		// already exist and reactivate it.
		if err := actor.close(ctx); err != nil {
			// TODO: This should probably be a warning, but if this happens
			//       I want to understand why.
			panic(err)
		}

		delete(a._actors, reference.ActorID())
		actor = activatedActor{}
	}

	// Actor was not already activated locally. Check if the module is already
	// cached.
	module, ok := a._modules[reference.ModuleID()]
	if ok {
		// Module is cached, instantiate the actor then we're done.
		hostCapabilities := newHostCapabilities(
			a.registry, a.environment, a.customHostFns,
			reference.Namespace(), reference.ActorID().ID, reference.ModuleID().ID, a.getServerState)
		iActor, err := module.Instantiate(ctx, reference.ActorID().ID, hostCapabilities)
		if err != nil {
			a.Unlock()
			return nil, fmt.Errorf(
				"error instantiating actor: %s from module: %s, err: %w",
				reference.ActorID(), reference.ModuleID(), err)
		}
		actor, err = newActivatedActor(ctx, iActor, reference, hostCapabilities)
		if err != nil {
			a.Unlock()
			return nil, fmt.Errorf("error activating actor: %w", err)
		}
		a._actors[reference.ActorID()] = actor
	}

	// Module is not cached. We may need to load the bytes from a remote store
	// so lets release the lock before continuing.
	a.Unlock()

	// TODO: Thundering herd problem here on module load. We should add support
	//       for automatically deduplicating this fetch. Although, it may actually
	//       be more prudent to just do that in the Registry implementation so we
	//       can implement deduplication + on-disk caching transparently in one
	//       place.
	moduleBytes, _, err := a.registry.GetModule(
		ctx, reference.Namespace(), reference.ModuleID().ID)
	if err != nil {
		return nil, fmt.Errorf(
			"error getting module bytes from registry for module: %s, err: %w",
			reference.ModuleID(), err)
	}

	// Now that we've loaded the module bytes from a (potentially remote) store, we
	// need to reacquire the lock to create the in-memory module + actor. Note that
	// since we released the lock previously, we need to redo all the checks to make
	// sure the module/actor don't already exist since a different goroutine may have
	// created them in the meantime.

	a.Lock()

	module, ok = a._modules[reference.ModuleID()]
	if !ok {
		hostFn := newHostFnRouter(
			a.registry, a.environment, a.customHostFns,
			reference.Namespace(), reference.ModuleID().ID)

		if len(moduleBytes) > 0 {
			// WASM byte codes exists for the module so we should just use that.
			// TODO: Hard-coded for now, but we should support using different runtimes with
			//       configuration since we've already abstracted away the module/object
			//       interfaces.
			wazeroMod, err := durablewazero.NewModule(ctx, wazero.Engine(), hostFn, moduleBytes)
			if err != nil {
				a.Unlock()
				return nil, fmt.Errorf(
					"error constructing module: %s from module bytes, err: %w",
					reference.ModuleID(), err)
			}

			// Wrap the wazero module so it implements Module.
			module = wazeroModule{wazeroMod}
			a._modules[reference.ModuleID()] = module
		} else {
			// No WASM code, must be a hard-coded Go module.
			goModID := types.NewNamespacedIDNoType(reference.
				ModuleID().Namespace, reference.ModuleID().ID)
			goMod, ok := a.goModules[goModID]
			if !ok {
				a.Unlock()
				return nil, fmt.Errorf(
					"error constructing module: %s, hard-coded Go module does not exist",
					reference.ModuleID())
			}
			module = goMod
			a._modules[reference.ModuleID()] = module
		}

	}

	actor, ok = a._actors[reference.ActorID()]
	if !ok {
		hostCapabilities := newHostCapabilities(
			a.registry, a.environment, a.customHostFns,
			reference.Namespace(), reference.ActorID().ID, reference.ModuleID().ID, a.getServerState)
		iActor, err := module.Instantiate(ctx, reference.ActorID().ID, hostCapabilities)
		if err != nil {
			a.Unlock()
			return nil, fmt.Errorf(
				"error instantiating actor: %s from module: %s",
				reference.ActorID(), reference.ModuleID())
		}
		actor, err = newActivatedActor(ctx, iActor, reference, hostCapabilities)
		if err != nil {
			a.Unlock()
			return nil, fmt.Errorf("error activating actor: %w", err)
		}
		a._actors[reference.ActorID()] = actor
	}

	a.Unlock()
	return actor.invoke(ctx, operation, payload)
}

func (a *activations) numActivatedActors() int {
	a.RLock()
	defer a.RUnlock()
	return len(a._actors)
}

func (a *activations) setServerState(
	serverID string,
	serverVersion int64,
) {
	a.serverState.Lock()
	defer a.serverState.Unlock()
	a.serverState.serverID = serverID
	a.serverState.serverVersion = serverVersion
}

func (a *activations) getServerState() (
	serverID string,
	serverVersion int64,
) {
	a.serverState.RLock()
	defer a.serverState.RUnlock()
	return a.serverState.serverID, a.serverState.serverVersion
}

type activatedActor struct {
	// Don't access directly from outside this structs own method implementations,
	// use methods like invoke() and close() instead.
	_a        Actor
	reference types.ActorReferenceVirtual
	host      HostCapabilities
}

func newActivatedActor(
	ctx context.Context,
	actor Actor,
	reference types.ActorReferenceVirtual,
	host HostCapabilities,
) (activatedActor, error) {
	a := activatedActor{
		_a:        actor,
		reference: reference,
		host:      host,
	}

	_, err := a.invoke(ctx, wapcutils.StartupOperationName, nil)
	if err != nil {
		a.close(ctx)
		return activatedActor{}, fmt.Errorf("newActivatedActor: error invoking startup function: %w", err)
	}

	return a, nil
}

func (a *activatedActor) invoke(
	ctx context.Context,
	operation string,
	payload []byte,
) ([]byte, error) {
	// Workers can't have KV storage because they're not global singletons like actors
	// are. They're also not registered with the Registry explicitly, so we can skip
	// this step in that case.
	if a.reference.ActorID().IDType != types.IDTypeWorker {
		result, err := a.host.Transact(ctx, func(tr registry.ActorKVTransaction) (any, error) {
			return a._a.Invoke(ctx, operation, payload, tr)
		})
		if err != nil {
			return nil, err
		}
		return result.([]byte), nil
	}

	return a._a.Invoke(ctx, operation, payload, nil)
}

func (a *activatedActor) close(ctx context.Context) error {
	return a._a.Close(ctx)
}
