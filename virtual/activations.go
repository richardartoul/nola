package virtual

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"sync"

	"github.com/richardartoul/nola/durable/durablewazero"
	"github.com/richardartoul/nola/virtual/futures"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"

	"github.com/wapc/wapc-go/engines/wazero"
	"golang.org/x/sync/singleflight"
)

type activations struct {
	sync.Mutex

	// State.
	_modules           map[types.NamespacedID]Module
	_actors            map[types.NamespacedActorID]futures.Future[*activatedActor]
	moduleFetchDeduper singleflight.Group
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
		_actors:  make(map[types.NamespacedActorID]futures.Future[*activatedActor]),

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
	// First check if the actor is already activated.
	a.Lock()
	actorF, ok := a._actors[reference.ActorID()]
	if !ok {
		// Actor is not activated, so we can just activate it ourselves.
		return a.invokeNotExistWithLock(
			ctx, reference, operation, instantiatePayload, invokePayload, nil)
	}

	// Actor is activated already (or in the process of being activated). Unlock and
	// wait for the future to resolve. If the actor is already activated and ready to
	// go this will resolve immediately, otherwise it will wait for the actor to be
	// properly instantiated.
	a.Unlock()
	actor, err := actorF.Wait()
	if err != nil {
		// Something went wrong instantiating the actor, just accept the error. The
		// goroutine that originally failed to instantiate the actor will take care
		// of removing the future from the map so that subsequent invocations can try
		// to re-instantiate.
		return nil, err
	}

	// The actor is/was activated without error, but we still need to check the generation
	// count before we're allowed to invoke it.
	if actor.reference.Generation() >= reference.Generation() {
		// The activated actor's generation count is high enough, we can just invoke now.
		return actor.invoke(ctx, operation, invokePayload, false)
	}

	// The activated actor's generation count is too low. We need to reinstantiate it.
	// First, we reacquire the lock since we can't do anything outside of the critical
	// loop.
	a.Lock()

	// Next, we check if the actor is still in the map (since we released and re-acquired
	// the lock, anything could have happened in the meantime).
	actorF2, ok := a._actors[reference.ActorID()]
	if !ok {
		// Actor is no longer in the map. We can just proceed with a normal activation then.
		return a.invokeNotExistWithLock(
			ctx, reference, operation, instantiatePayload, invokePayload, nil)
	}

	// Actor is still in the map. We need to know if it has changed since we last checked.
	// Pointer comparison is fine here since if it was changed then the future pointer will
	// have changed also.
	if actorF2 == actorF {
		// If it hasn't changed then we can just pretend it does not exist.
		// invokeNotExistWithLock will ensure the old activation is properly closed as
		// well.
		return a.invokeNotExistWithLock(
			ctx, reference, operation, instantiatePayload, invokePayload, actor)
	}

	// The future has changed, the generation count should be high enough now and
	// we can just ignore the old actor (whichever Goroutine increased the generation
	// count will have closed it already)
	if actor.reference.Generation() >= reference.Generation() {
		return actor.invoke(ctx, operation, invokePayload, false)
	}

	// Something weird happened. Just return an error and let the caller retry.
	return nil, errors.New(
		"[invariant violated] actor generation count too low after reactivation, caller should retry")
}

func (a *activations) invokeNotExistWithLock(
	ctx context.Context,
	reference types.ActorReferenceVirtual,
	operation string,
	instantiatePayload []byte,
	invokePayload []byte,
	prevActor *activatedActor,
) (io.ReadCloser, error) {
	fut := futures.New[*activatedActor]()
	a._actors[reference.ActorID()] = fut
	a.Unlock()

	// GoSync since this goroutine needs to wait anyways.
	fut.GoSync(func() (actor *activatedActor, err error) {
		if prevActor != nil {
			if err := prevActor.close(ctx); err != nil {
				log.Printf(
					"error closing previous instance of actor: %v, err: %v",
					reference, err)
			}
		}

		defer func() {
			if err != nil {
				// If resolving the future results in an error, ensure that
				// the future gets cleared from the map so that subsequent
				// invocations will try to recreate the actor instead of
				// receiving the same hard-coded over and over again.
				delete(a._actors, reference.ActorID())
			}
		}()

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

		return actor, nil
	})

	actor, err := fut.Wait()
	if err != nil {
		return nil, err
	}

	return actor.invoke(ctx, operation, invokePayload, false)
}

func (a *activations) ensureModule(
	ctx context.Context,
	moduleID types.NamespacedID,
) (Module, error) {
	a.Lock()
	module, ok := a._modules[moduleID]
	a.Unlock()
	if ok {
		return module, nil
	}

	// Module wasn't cached already, we need to go fetch it.
	dedupeBy := fmt.Sprintf("%s-%s", moduleID.Namespace, moduleID.ID)
	moduleI, err, _ := a.moduleFetchDeduper.Do(dedupeBy, func() (any, error) {
		// Need to check map again once we get into singleflight context in case
		// the actor was instantiated since we released the lock above and entered
		// the singleflight context.
		a.Lock()
		module, ok := a._modules[moduleID]
		a.Unlock()
		if ok {
			return module, nil
		}

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
		}

		// Can set unconditionally without checking if it already exists since we're in
		// the singleflight context.
		a.Lock()
		a._modules[moduleID] = module
		a.Unlock()
		return module, nil
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
	a.Lock()
	defer a.Unlock()
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

	_, err := a.invoke(ctx, wapcutils.StartupOperationName, instantiatePayload, false)
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
	alreadyLocked bool,
) (io.ReadCloser, error) {
	if !alreadyLocked {
		a.Lock()
		defer a.Unlock()
	}

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
	_, err := a.invoke(ctx, wapcutils.ShutdownOperationName, nil, true)
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
