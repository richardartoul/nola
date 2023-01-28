package virtual

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/richardartoul/nola/durable/durablewazero"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"
	"github.com/wapc/wapc-go/engines/wazero"
)

type activations struct {
	sync.RWMutex

	// State.
	_modules map[types.NamespacedID]Module
	_actors  map[types.NamespacedID]activatedActor

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
	// This is required for module that are using WASM/wazero so we can propage the actor
	// ID to invocations of the host's capabilities. The reason this is required is that the
	// WACP implementation we're using defines a host router per-module instead of per-actor, so
	// we use the context.Context to "smuggle" the actor ID into each invocation. See
	// newHostFnRouter in wazer.go to see the implementation.
	ctx = context.WithValue(ctx, hostFnActorIDCtxKey{}, reference.ActorID().ID)

	a.RLock()
	actor, ok := a._actors[reference.ActorID()]
	if ok && actor.generation >= reference.Generation() {
		a.RUnlock()
		return actor.a.Invoke(ctx, operation, payload)
	}
	a.RUnlock()

	a.Lock()
	if ok && actor.generation >= reference.Generation() {
		a.Unlock()
		return actor.a.Invoke(ctx, operation, payload)
	}

	if ok && actor.generation < reference.Generation() {
		// The actor is already activated, however, the generation count has
		// increased. Therefore we need to pretend like the actor doesn't
		// already exist and reactivate it.
		if err := actor.a.Close(ctx); err != nil {
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
			reference.Namespace(), reference.ActorID().ID, reference.ModuleID().ID)
		iActor, err := module.Instantiate(ctx, reference.ActorID().ID, hostCapabilities)
		if err != nil {
			a.Unlock()
			return nil, fmt.Errorf(
				"error instantiating actor: %s from module: %s, err: %w",
				reference.ActorID(), reference.ModuleID(), err)
		}
		actor, err = newActivatedActor(ctx, iActor, reference.Generation())
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
			reference.Namespace(), reference.ActorID().ID, reference.ModuleID().ID)
		iActor, err := module.Instantiate(ctx, reference.ActorID().ID, hostCapabilities)
		if err != nil {
			a.Unlock()
			return nil, fmt.Errorf(
				"error instantiating actor: %s from module: %s",
				reference.ActorID(), reference.ModuleID())
		}
		actor, err = newActivatedActor(ctx, iActor, reference.Generation())
		if err != nil {
			a.Unlock()
			return nil, fmt.Errorf("error activating actor: %w", err)
		}
		a._actors[reference.ActorID()] = actor
	}

	a.Unlock()
	return actor.a.Invoke(ctx, operation, payload)
}

func (a *activations) numActivatedActors() int {
	a.RLock()
	defer a.RUnlock()
	return len(a._actors)
}

type hostCapabilities struct {
	reg           registry.Registry
	env           Environment
	customHostFns map[string]func([]byte) ([]byte, error)
	namespace     string
	actorID       string
	actorModuleID string
}

func newHostCapabilities(
	reg registry.Registry,
	env Environment,
	customHostFns map[string]func([]byte) ([]byte, error),
	namespace string,
	actorID string,
	actorModuleID string,
) HostCapabilities {
	return &hostCapabilities{
		reg:           reg,
		env:           env,
		customHostFns: customHostFns,
		namespace:     namespace,
		actorID:       actorID,
		actorModuleID: actorModuleID,
	}
}

func (h *hostCapabilities) Put(
	ctx context.Context,
	key []byte,
	value []byte,
) error {
	return h.reg.ActorKVPut(ctx, h.namespace, h.actorID, key, value)
}

func (h *hostCapabilities) Get(
	ctx context.Context,
	key []byte,
) ([]byte, bool, error) {
	return h.reg.ActorKVGet(ctx, h.namespace, h.actorID, key)
}

func (h *hostCapabilities) CreateActor(
	ctx context.Context,
	req wapcutils.CreateActorRequest,
) (CreateActorResult, error) {
	if req.ModuleID == "" {
		// If no module ID was specified then assume the actor is trying to "fork"
		// itself and create the new actor using the same module as the existing
		// actor.
		req.ModuleID = h.actorModuleID
	}

	_, err := h.reg.CreateActor(ctx, h.namespace, req.ActorID, req.ModuleID, registry.ActorOptions{})
	if err != nil {
		return CreateActorResult{}, err
	}
	return CreateActorResult{}, nil
}

func (h *hostCapabilities) InvokeActor(
	ctx context.Context,
	req wapcutils.InvokeActorRequest,
) ([]byte, error) {
	return h.env.InvokeActor(ctx, h.namespace, req.ActorID, req.Operation, req.Payload)
}

func (h *hostCapabilities) ScheduleInvokeActor(
	ctx context.Context,
	req wapcutils.ScheduleInvocationRequest,
) error {
	if req.Invoke.ActorID == "" {
		// Omitted if the actor wants to schedule a delayed invocation (timer) for itself.
		req.Invoke.ActorID = h.actorID
	}

	// TODO: When the actor gets GC'd (which is not currently implemented), this
	//       timer won't get GC'd with it. We should keep track of all outstanding
	//       timers with the instantiation and terminate them if the actor is
	//       killed.
	time.AfterFunc(time.Duration(req.AfterMillis)*time.Millisecond, func() {
		// Copy the payload to make sure its safe to retain across invocations.
		payloadCopy := make([]byte, len(req.Invoke.Payload))
		copy(payloadCopy, req.Invoke.Payload)
		_, err := h.env.InvokeActor(ctx, h.namespace, req.Invoke.ActorID, req.Invoke.Operation, payloadCopy)
		if err != nil {
			log.Printf(
				"error performing scheduled invocation from actor: %s to actor: %s for operation: %s, err: %v\n",
				h.actorID, req.Invoke.ActorID, req.Invoke.Operation, err)
		}
	})

	return nil
}

func (h *hostCapabilities) CustomFn(
	ctx context.Context,
	operation string,
	payload []byte,
) ([]byte, error) {
	customFn, ok := h.customHostFns[operation]
	if ok {
		res, err := customFn(payload)
		if err != nil {
			return nil, fmt.Errorf("error running custom host function: %s, err: %w", operation, err)
		}
		return res, nil
	}
	return nil, fmt.Errorf(
		"unknown host function: %s::%s::%s",
		h.namespace, operation, payload)
}
