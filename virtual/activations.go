package virtual

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/richardartoul/nola/durable"
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
	registry    registry.Registry
	environment Environment
}

func newActivations(
	registry registry.Registry,
	environment Environment,
) *activations {
	return &activations{
		_modules: make(map[types.NamespacedID]Module),
		_actors:  make(map[types.NamespacedID]activatedActor),

		registry:    registry,
		environment: environment,
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
		iActor, err := module.Instantiate(ctx, reference.ActorID().ID)
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
			a.registry, a.environment,
			reference.Namespace(), reference.ActorID().ID, reference.ModuleID().ID)
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
	}

	actor, ok = a._actors[reference.ActorID()]
	if !ok {
		iActor, err := module.Instantiate(ctx, reference.ActorID().ID)
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

// TODO: Should have some kind of ACL enforcement polic here, but for now allow any module to
//
//	run any host function.
func newHostFnRouter(
	reg registry.Registry,
	environment Environment,
	actorNamespace string,
	actorID string,
	actorModuleID string,
) func(ctx context.Context, binding, namespace, operation string, payload []byte) ([]byte, error) {
	return func(
		ctx context.Context,
		wapcBinding string,
		wapcNamespace string,
		wapcOperation string,
		wapcPayload []byte,
	) ([]byte, error) {
		switch wapcOperation {
		case wapcutils.KVPutOperationName:
			k, v, err := wapcutils.ExtractKVFromPutPayload(wapcPayload)
			if err != nil {
				return nil, fmt.Errorf("error extracting KV from PUT payload: %w", err)
			}

			if err := reg.ActorKVPut(ctx, actorNamespace, actorID, k, v); err != nil {
				return nil, fmt.Errorf("error performing PUT against registry: %w", err)
			}

			return nil, nil
		case wapcutils.KVGetOperationName:
			v, ok, err := reg.ActorKVGet(ctx, actorNamespace, actorID, wapcPayload)
			if err != nil {
				return nil, fmt.Errorf("error performing GET against registry: %w", err)
			}
			if !ok {
				return []byte{0}, nil
			} else {
				// TODO: Avoid these useless allocs.
				resp := make([]byte, 0, len(v)+1)
				resp = append(resp, 1)
				resp = append(resp, v...)
				return resp, nil
			}
		case wapcutils.CreateActorOperationName:
			var req wapcutils.CreateActorRequest
			if err := json.Unmarshal(wapcPayload, &req); err != nil {
				return nil, fmt.Errorf("error unmarshaling CreateActorRequest: %w", err)
			}

			if req.ModuleID == "" {
				// If no module ID was specified then assume the actor is trying to "fork"
				// itself and create the new actor using the same module as the existing
				// actor.
				req.ModuleID = actorModuleID
			}

			if _, err := reg.CreateActor(
				ctx, actorNamespace, req.ActorID, req.ModuleID, registry.ActorOptions{}); err != nil {
				return nil, fmt.Errorf("error creating new actor in registry: %w", err)
			}

			return nil, nil

		case wapcutils.InvokeActorOperationName:
			var req wapcutils.InvokeActorRequest
			if err := json.Unmarshal(wapcPayload, &req); err != nil {
				return nil, fmt.Errorf("error unmarshaling InvokeActorRequest: %w", err)
			}

			return environment.InvokeActor(ctx, actorNamespace, req.ActorID, req.Operation, req.Payload)

		case wapcutils.ScheduleInvocationOperationName:
			var req wapcutils.ScheduleInvocationRequest
			if err := json.Unmarshal(wapcPayload, &req); err != nil {
				return nil, fmt.Errorf(
					"error unmarshaling ScheduleInvocationRequest: %w, payload: %s",
					err, string(wapcPayload))
			}

			if req.Invoke.ActorID == "" {
				// Omitted if the actor wants to schedule a delayed invocation (timer) for itself.
				req.Invoke.ActorID = actorID
			}

			// TODO: When the actor gets GC'd (which is not currently implemented), this
			//       timer won't get GC'd with it. We should keep track of all outstanding
			//       timers with the instantiation and terminate them if the actor is
			//       killed.
			time.AfterFunc(time.Duration(req.AfterMillis)*time.Millisecond, func() {
				// Copy the payload to make sure its safe to retain across invocations.
				payloadCopy := make([]byte, len(req.Invoke.Payload))
				copy(payloadCopy, req.Invoke.Payload)
				_, err := environment.InvokeActor(ctx, actorNamespace, req.Invoke.ActorID, req.Invoke.Operation, payloadCopy)
				if err != nil {
					log.Printf(
						"error performing scheduled invocation from actor: %s to actor: %s for operation: %s, err: %v\n",
						actorID, req.Invoke.ActorID, req.Invoke.Operation, err)
				}
			})

			return nil, nil
		default:
			return nil, fmt.Errorf(
				"unknown host function: %s::%s::%s::%s",
				wapcBinding, wapcNamespace, wapcOperation, wapcPayload)
		}
	}
}

type activatedActor struct {
	a          Actor
	generation uint64
}

func newActivatedActor(
	ctx context.Context,
	actor Actor,
	generation uint64,
) (activatedActor, error) {
	_, err := actor.Invoke(ctx, wapcutils.StartupOperationName, nil)
	if err != nil {
		return activatedActor{}, fmt.Errorf("newActivatedActor: error invoking startup function: %w", err)
	}

	return activatedActor{
		a:          actor,
		generation: generation,
	}, nil
}

type wazeroModule struct {
	m durable.Module
}

func (w wazeroModule) Instantiate(
	ctx context.Context,
	id string,
) (Actor, error) {
	obj, err := w.m.Instantiate(ctx, id)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (w wazeroModule) Close(ctx context.Context) error {
	return nil
}
