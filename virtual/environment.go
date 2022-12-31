package virtual

import (
	"context"
	"fmt"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
)

type environment struct {
	// State.
	activations *activations // Internally synchronized.

	// Dependencies.
	registry registry.Registry
}

func NewEnvironment(registry registry.Registry) (Environment, error) {
	env := &environment{
		registry: registry,
	}
	activations := newActivations(registry, env)
	env.activations = activations

	return env, nil
}

func (r *environment) Invoke(
	ctx context.Context,
	namespace string,
	actorID string,
	operation string,
	payload []byte,
) ([]byte, error) {
	references, err := r.registry.EnsureActivation(ctx, namespace, actorID)
	if err != nil {
		return nil, fmt.Errorf(
			"error ensuring activation of actor: %s in registry: %w",
			actorID, err)
	}

	if len(references) == 0 {
		return nil, fmt.Errorf(
			"[invariant violated] ensureActivation() success with 0 references for actor ID: %s", actorID)
	}

	return r.invokeReferences(ctx, references, operation, payload)
}

func (r *environment) InvokeLocal(
	ctx context.Context,
	reference types.ActorReference,
	operation string,
	payload []byte,
) ([]byte, error) {
	return r.activations.invoke(ctx, reference, operation, payload)
}

func (r *environment) Close() error {
	// TODO: This should call Close on the activations field (which needs to be implemented).
	return nil
}

func (r *environment) invokeReferences(
	ctx context.Context,
	references []types.ActorReference,
	operation string,
	payload []byte,
) ([]byte, error) {
	// TODO: Load balancing or some other strategy if the number of references is > 1?
	reference := references[0]
	switch reference.Type() {
	case types.ReferenceTypeLocal:
		return r.InvokeLocal(ctx, reference, operation, payload)
	case types.ReferenceTypeRemoteHTTP:
		fallthrough
	default:
		return nil, fmt.Errorf(
			"reference for actor: %s has unhandled type: %v", reference.ActorID(), reference.Type())
	}
}
