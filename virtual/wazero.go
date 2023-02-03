package virtual

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/richardartoul/nola/durable"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"
)

// hostFnActorIDCtxKey is the key that is used to store/retrieve the actorID field
// from the context.
type hostFnActorIDCtxKey struct{}

// hostFnActorTxnKey is the key that is used to store/retrieve the actor's per-invocation
// lazyTransaction from the context.
type hostFnActorTxnKey struct{}

// TODO: Should have some kind of ACL enforcement polic here, but for now allow any module to
// run any host function.
func newHostFnRouter(
	reg registry.Registry,
	environment Environment,
	customHostFns map[string]func([]byte) ([]byte, error),
	actorNamespace string,
	actorModuleID string,
) func(ctx context.Context, binding, namespace, operation string, payload []byte) ([]byte, error) {
	return func(
		ctx context.Context,
		wapcBinding string,
		wapcNamespace string,
		wapcOperation string,
		wapcPayload []byte,
	) ([]byte, error) {
		actorID, err := extractActorID(ctx)
		if err != nil {
			return nil, fmt.Errorf("error extracting actorID from context: %w", err)
		}

		switch wapcOperation {
		case wapcutils.KVPutOperationName:
			k, v, err := wapcutils.ExtractKVFromPutPayload(wapcPayload)
			if err != nil {
				return nil, fmt.Errorf("error extracting KV from PUT payload: %w", err)
			}

			tr, err := extractTransaction(ctx)
			if err != nil {
				return nil, fmt.Errorf("error extracting transaction from context: %w", err)
			}

			if err := tr.Put(ctx, k, v); err != nil {
				return nil, fmt.Errorf("error performing PUT against registry: %w", err)
			}

			return nil, nil
		case wapcutils.KVGetOperationName:
			tr, err := extractTransaction(ctx)
			if err != nil {
				return nil, fmt.Errorf("error extracting transaction from context: %w", err)
			}

			v, ok, err := tr.Get(ctx, wapcPayload)
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
				ctx, actorNamespace, req.ActorID, req.ModuleID, types.ActorOptions{}); err != nil {
				return nil, fmt.Errorf("error creating new actor in registry: %w", err)
			}

			return nil, nil

		case wapcutils.InvokeActorOperationName:
			var req types.InvokeActorRequest
			if err := json.Unmarshal(wapcPayload, &req); err != nil {
				return nil, fmt.Errorf("error unmarshaling InvokeActorRequest: %w", err)
			}

			return environment.InvokeActor(ctx, actorNamespace, req.ActorID, req.Operation, req.Payload, req.CreateIfNotExist)

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
				_, err := environment.InvokeActor(
					ctx, actorNamespace, req.Invoke.ActorID, req.Invoke.Operation, payloadCopy, req.Invoke.CreateIfNotExist)
				if err != nil {
					log.Printf(
						"error performing scheduled invocation from actor: %s to actor: %s for operation: %s, err: %v\n",
						actorID, req.Invoke.ActorID, req.Invoke.Operation, err)
				}
			})

			return nil, nil
		default:
			customFn, ok := customHostFns[wapcOperation]
			if ok {
				res, err := customFn(wapcPayload)
				if err != nil {
					return nil, fmt.Errorf("error running custom host function: %s, err: %w", wapcOperation, err)
				}
				return res, nil
			}
			return nil, fmt.Errorf(
				"unknown host function: %s::%s::%s::%s",
				wapcBinding, wapcNamespace, wapcOperation, wapcPayload)
		}
	}
}

func extractActorID(ctx context.Context) (string, error) {
	actorIDIface := ctx.Value(hostFnActorIDCtxKey{})
	if actorIDIface == nil {
		return "", fmt.Errorf("wazeroHostFnRouter: could not find non-empty actor ID in context")
	}
	actorID, ok := actorIDIface.(string)
	if !ok {
		return "", fmt.Errorf("wazeroHostFnRouter: wrong type for actor ID in context: %T", actorIDIface)
	}
	if actorID == "" {
		return "", fmt.Errorf("wazeroHostFnRouter: could not find non-empty actor ID in context")
	}
	return actorID, nil
}

func extractTransaction(ctx context.Context) (registry.ActorKVTransaction, error) {
	trIface := ctx.Value(hostFnActorTxnKey{})
	if trIface == nil {
		return nil, fmt.Errorf("wazeroHostFnRouter: could not find non-empty transaction in context")
	}
	tr, ok := trIface.(registry.ActorKVTransaction)
	if !ok {
		return nil, fmt.Errorf("wazeroHostFnRouter: wrong type for actor ID in context: %T", trIface)
	}
	return tr, nil
}

type wazeroModule struct {
	m durable.Module
}

func (w wazeroModule) Instantiate(
	ctx context.Context,
	id string,
	host HostCapabilities,
) (Actor, error) {
	obj, err := w.m.Instantiate(ctx, id)
	if err != nil {
		return nil, err
	}

	return wazeroActor{obj, id}, nil
}

func (w wazeroModule) Close(ctx context.Context) error {
	return nil
}

type wazeroActor struct {
	obj durable.Object
	id  string
}

func (w wazeroActor) Invoke(
	ctx context.Context,
	operation string,
	payload []byte,
	transaction registry.ActorKVTransaction,
) ([]byte, error) {
	// This is required for modules that are using WASM/wazero so we can propagate
	// the actor ID to invocations of the host's capabilities. The reason this is
	// required is that the WAPC implementation we're using defines a host router
	// per-module instead of per-actor, so we use the context.Context to "smuggle"
	// the actor ID into each invocation. See newHostFnRouter in wazero.go to see
	// the implementation.
	ctx = context.WithValue(ctx, hostFnActorIDCtxKey{}, w.id)

	// This is required for modules that are using WASM/wazero so we can propagate a
	// per-invocation transaction to the hostFnRouter. The reason this is required is
	// that the WAPC implementation we're using defines a host router per-module
	// instead of per-actor or per-invocation, so we use the context.Context to
	// "smuggle" the actor ID into each invocation. See newHostFnRouter in wazero.go
	// to see the implementation.
	ctx = context.WithValue(ctx, hostFnActorTxnKey{}, transaction)

	return w.obj.Invoke(ctx, operation, payload)
}

func (w wazeroActor) Close(ctx context.Context) error {
	return w.obj.Close(ctx)
}
