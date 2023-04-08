package virtual

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"golang.org/x/exp/slog"

	"github.com/richardartoul/nola/durable"
	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"
)

// hostFnActorReferenceCtxKey is the key that is used to store/retrieve the actor reference
// field from the context.
type hostFnActorReferenceCtxKey struct{}

// hostFnActorTxnKey is the key that is used to store/retrieve the actor's per-invocation
// lazyTransaction from the context.
type hostFnActorTxnKey struct{}

// TODO: Should have some kind of ACL enforcement polic here, but for now allow any module to
// run any host function.
func newHostFnRouter(
	log *slog.Logger,
	reg registry.Registry,
	environment Environment,
	activations *activations,
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
		actorRef, err := extractActorRef(ctx)
		if err != nil {
			return nil, fmt.Errorf("error extracting actor reference from context: %w", err)
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

		case wapcutils.InvokeActorOperationName:
			var req types.InvokeActorRequest
			if err := json.Unmarshal(wapcPayload, &req); err != nil {
				return nil, fmt.Errorf("error unmarshaling InvokeActorRequest: %w", err)
			}

			return environment.InvokeActor(
				ctx, actorNamespace, req.ActorID, req.ModuleID,
				req.Operation, req.Payload, req.CreateIfNotExist)

		case wapcutils.ScheduleSelfTimerOperationName:
			var req wapcutils.ScheduleSelfTimer
			if err := json.Unmarshal(wapcPayload, &req); err != nil {
				return nil, fmt.Errorf(
					"error unmarshaling ScheduleSelfTimer: %w, payload: %s",
					err, string(wapcPayload))
			}
			if req.Operation == "" {
				return nil, fmt.Errorf("cant schedule self timer with empty operation name")
			}
			if req.AfterMillis <= 0 {
				return nil, fmt.Errorf("cant schedule self timer with AfterMillis <= 0")
			}

			// Copy the payload to make sure its safe to retain across invocations.
			payloadCopy := make([]byte, len(req.Payload))
			copy(payloadCopy, req.Payload)

			// TODO: When the actor gets GC'd (which is not currently implemented), this
			//       timer won't get GC'd with it. We should keep track of all outstanding
			//       timers with the instantiation and terminate them if the actor is
			//       killed.
			time.AfterFunc(time.Duration(req.AfterMillis)*time.Millisecond, func() {
				// TODO: Fix generation number.
				reader, err := activations.invoke(context.Background(), actorRef, req.Operation, nil, payloadCopy, true)
				if err == nil {
					if reader != nil {
						// This is weird, but the reader can be nil in the case where the actor the timer is
						// associated with is no longer activated in memory anymore.
						//
						// TestScheduleSelfTimersAndGC intentionally triggers this behavior to ensure that timers
						// never trigger reactivation of deactivated actors.
						defer reader.Close()
					}
				}
				if err != nil {
					log.Error("error firing timer for actor", slog.Any("actor", actorRef), slog.Any("error", err))
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

func extractActorRef(ctx context.Context) (types.ActorReferenceVirtual, error) {
	actorRefIface := ctx.Value(hostFnActorReferenceCtxKey{})
	if actorRefIface == nil {
		return nil, fmt.Errorf("wazeroHostFnRouter: could not find non-empty actor reference in context")
	}
	actorRef, ok := actorRefIface.(types.ActorReferenceVirtual)
	if !ok {
		return nil, fmt.Errorf("wazeroHostFnRouter: wrong type for actor reference in context: %T", actorRef)
	}
	return actorRef, nil
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
	reference types.ActorReferenceVirtual,
	instantiatePayload []byte,
	host HostCapabilities,
) (Actor, error) {
	obj, err := w.m.Instantiate(ctx, reference.ActorID().ID)
	if err != nil {
		return nil, err
	}

	return wazeroActor{obj, reference}, nil
}

func (w wazeroModule) Close(ctx context.Context) error {
	return nil
}

type wazeroActor struct {
	obj       durable.Object
	reference types.ActorReferenceVirtual
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
	ctx = context.WithValue(ctx, hostFnActorReferenceCtxKey{}, w.reference)

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
