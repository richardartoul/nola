package virtual

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"sync"

	"github.com/richardartoul/nola/durable/durablewazero"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"

	"github.com/wapc/wapc-go/engines/wazero"
	"golang.org/x/sync/singleflight"
)

type activations struct {
	sync.RWMutex

	// State.
	_modules           map[types.NamespacedID]Module
	_actors            map[types.NamespacedActorID]*activatedActor
	moduleFetchDeduper singleflight.Group
	activationDeduper  singleflight.Group
	serverState        struct {
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
	customHostFns map[string]func([]byte) ([]byte, error),
) *activations {
	return &activations{
		_modules: make(map[types.NamespacedID]Module),
		_actors:  make(map[types.NamespacedActorID]*activatedActor),

		registry:      registry,
		environment:   environment,
		goModules:     make(map[types.NamespacedIDNoType]Module),
		customHostFns: customHostFns,
	}
}

func (a *activations) registerGoModule(id types.NamespacedIDNoType, module Module) error {
	a.Lock()
	defer a.Unlock()
	if _, ok := a.goModules[id]; ok {
		return fmt.Errorf("error registering go module with ID: %v, already registered", id)
	}
	a.goModules[id] = module
	return nil
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
	instantiatePayload []byte,
	invokePayload []byte,
) (io.ReadCloser, error) {
	a.RLock()
	actor, ok := a._actors[reference.ActorID()]
	if ok && actor.reference.Generation() >= reference.Generation() {
		a.RUnlock()
		return actor.invoke(ctx, operation, invokePayload)
	}
	a.RUnlock()

	a.Lock()
	if ok && actor.reference.Generation() >= reference.Generation() {
		a.Unlock()
		return actor.invoke(ctx, operation, invokePayload)
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
		actor = nil
	}

	// Actor was not already activated locally. Unlock and then use singleflight to
	// run all the logic for activating the actor to:
	//   1. Avoid thundering herd problems by fetching the module repeatedly.
	//   2. Avoid lock contention so we can scope locking required for creating
	//      the actor in-memory only to operations on that specific actor instead
	//      of locking the entire *activations datastructure for the duration.
	//   3. Perform potentially expensive operations like invoking the actor's
	//      wapcutils.StartupOperationName function outside of *activations global
	//      lock.
	a.Unlock()

	dedupeBy := fmt.Sprintf("%s-%s-%s", reference.Namespace(), reference.ModuleID(), reference.ActorID())
	actorI, err, _ := a.activationDeduper.Do(dedupeBy, func() (any, error) {
		module, err := a.ensureModule(ctx, reference.ModuleID())
		if err != nil {
			return nil, fmt.Errorf(
				"error ensuring module for reference: %v, err: %w",
				reference, err)
		}

		hostCapabilities := newHostCapabilities(
			a.registry, a.environment, a.customHostFns,
			reference.Namespace(), reference.ActorID().ID, reference.ModuleID().ID, a.getServerState)

		iActor, err := module.Instantiate(ctx, reference.ActorID().ID, instantiatePayload, hostCapabilities)
		if err != nil {
			return nil, fmt.Errorf(
				"error instantiating actor: %s from module: %s, err: %w",
				reference.ActorID(), reference.ModuleID(), err)
		}
		if err := assertActorIface(iActor); err != nil {
			return nil, fmt.Errorf(
				"error instantiating actor: %s from module: %s, err: %w",
				reference.ActorID(), reference.ModuleID(), err)
		}

		actor, err = a.newActivatedActor(ctx, iActor, reference, hostCapabilities, instantiatePayload)
		if err != nil {
			return nil, fmt.Errorf("error activating actor: %w", err)
		}
		a._actors[reference.ActorID()] = actor

		return actor, nil
	})
	if err != nil {
		return nil, fmt.Errorf("error deduping actor activation: %w", err)
	}
	actor = actorI.(*activatedActor)

	return actor.invoke(ctx, operation, invokePayload)
}

func (a *activations) ensureModule(
	ctx context.Context,
	moduleID types.NamespacedID,
) (Module, error) {
	// TODO: Should do this again in the beginning of the singleflight stuff?
	a.RLock()
	module, ok := a._modules[moduleID]
	a.RUnlock()
	if ok {
		return module, nil
	}

	// Module wasn't cached already, we need to go fetch it.
	dedupeBy := fmt.Sprintf("%s-%s", moduleID.Namespace, moduleID.ID)
	moduleI, err, _ := a.moduleFetchDeduper.Do(dedupeBy, func() (any, error) {
		// TODO: Should consider not using the context from the request here since this
		// timeout ends up being shared across multiple different requests potentially.
		moduleBytes, _, err := a.registry.GetModule(ctx, moduleID.Namespace, moduleID.ID)
		if err != nil {
			return nil, fmt.Errorf(
				"error getting module bytes from registry for module: %s, err: %w",
				moduleID, err)
		}

		hostFn := newHostFnRouter(
			a.registry, a.environment, a.customHostFns, moduleID.Namespace, moduleID.ID)
		if len(moduleBytes) > 0 {
			// WASM byte codes exists for the module so we should just use that.
			// TODO: Hard-coded for now, but we should support using different runtimes with
			//       configuration since we've already abstracted away the module/object
			//       interfaces.
			wazeroMod, err := durablewazero.NewModule(ctx, wazero.Engine(), hostFn, moduleBytes)
			if err != nil {
				return nil, fmt.Errorf(
					"error constructing module: %s from module bytes, err: %w",
					moduleID, err)
			}

			// Wrap the wazero module so it implements Module.
			module = wazeroModule{wazeroMod}

			// Can set unconditionally without checking if it already exists since we're in
			// the singleflight context.
			//
			// TODO: We probably need to acquire lock and check it at the top of the singleflight
			// callback function?
			a.Lock()
			a._modules[moduleID] = module
			a.Unlock()
			return module, nil
		} else {
			// No WASM code, must be a hard-coded Go module.
			goModID := types.NewNamespacedIDNoType(moduleID.Namespace, moduleID.ID)
			goMod, ok := a.goModules[goModID]
			if !ok {
				return nil, fmt.Errorf(
					"error constructing module: %s, hard-coded Go module does not exist",
					moduleID)
			}
			module = goMod

			// Can set unconditionally without checking if it already exists since we're in
			// the singleflight context.
			//
			// TODO: We probably need to acquire lock and check it at the top of the singleflight
			// callback function?
			a.Lock()
			a._modules[moduleID] = module
			a.Unlock()
			return module, nil
		}
	})
	if err != nil {
		return nil, err
	}

	return moduleI.(Module), nil
}

func (a *activations) newActivatedActor(
	ctx context.Context,
	actor Actor,
	reference types.ActorReferenceVirtual,
	host HostCapabilities,
	instantiatePayload []byte,
) (*activatedActor, error) {
	return newActivatedActor(ctx, actor, reference, host, instantiatePayload)
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
	sync.Mutex

	// Don't access directly from outside this structs own method implementations,
	// use methods like invoke() and close() instead.
	_a        Actor
	reference types.ActorReferenceVirtual
	host      HostCapabilities
	closed    bool
	// shutdownTimer *time.Timer
}

func newActivatedActor(
	ctx context.Context,
	actor Actor,
	reference types.ActorReferenceVirtual,
	host HostCapabilities,
	instantiatePayload []byte,
) (*activatedActor, error) {
	a := &activatedActor{
		_a:        actor,
		reference: reference,
		host:      host,
	}
	// time.AfterFunc(time.Minute, func() {
	// 	if err := a.close(context.TODO()); err != nil {
	// 		log.Printf("error closing actor: %v due to inactivity: %w", reference, err)
	// 	}
	// })

	_, err := a.invoke(ctx, wapcutils.StartupOperationName, instantiatePayload)
	if err != nil {
		a.close(ctx)
		return nil, fmt.Errorf("newActivatedActor: error invoking startup function: %w", err)
	}

	return a, nil
}

func (a *activatedActor) invoke(
	ctx context.Context,
	operation string,
	payload []byte,
) (io.ReadCloser, error) {
	a.Lock()
	defer a.Unlock()
	if a.closed {
		return nil, fmt.Errorf("tried to invoke actor: %v which has already been closed", a.reference)
	}

	// Workers can't have KV storage because they're not global singletons like actors
	// are. They're also not registered with the Registry explicitly, so we can skip
	// this step in that case.
	if a.reference.ActorID().IDType != types.IDTypeWorker {
		result, err := a.host.Transact(ctx, func(tr registry.ActorKVTransaction) (any, error) {
			streamActor, ok := a._a.(ActorStream)
			if ok {
				// This actor has support for the streaming interface so we should use that
				// directly since its more efficient.
				return streamActor.InvokeStream(ctx, operation, payload, tr)
			}

			// The actor doesn't support streaming responses, we'll convert the returned []byte
			// to a stream ourselves.
			return a._a.(ActorBytes).Invoke(ctx, operation, payload, tr)
		})
		if err != nil {
			return nil, err
		}

		if result == nil {
			// Actor returned nil stream, convert it to an empty one.
			return ioutil.NopCloser(bytes.NewReader(nil)), nil
		}

		stream, ok := result.(io.ReadCloser)
		if ok {
			return stream, nil
		}
		return io.NopCloser(bytes.NewBuffer(result.([]byte))), nil
	}

	streamActor, ok := a._a.(ActorStream)
	if ok {
		// This actor has support for the streaming interface so we should use that
		// directly since its more efficient.
		return streamActor.InvokeStream(ctx, operation, payload, nil)
	}

	// The actor doesn't support streaming responses, we'll convert the returned []byte
	// to a stream ourselves.
	resp, err := a._a.(ActorBytes).Invoke(ctx, operation, payload, nil)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewBuffer(resp)), nil
}

func (a *activatedActor) close(ctx context.Context) error {
	a.Lock()
	defer a.Unlock()
	if a.closed {
		return nil
	}

	// TODO: We should let a retry policy be specific for this before the actor is finally
	// evicted, but we just evict regardless of failure for now.
	_, err := a.invoke(ctx, wapcutils.ShutdownOperationName, nil)
	if err != nil {
		log.Printf(
			"error invoking shutdown operation for actor: %v during close: %v",
			a.reference, err)
	}

	return a._a.Close(ctx)
}

func assertActorIface(actor Actor) error {
	var (
		_, implementsByteActor   = actor.(ActorBytes)
		_, implementsStreamActor = actor.(ActorStream)
	)
	if implementsByteActor || implementsStreamActor {
		return nil
	}

	return fmt.Errorf("%T does not implement ByteActor or StreamActor", actor)
}
