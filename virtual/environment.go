package virtual

import (
	"context"
	"fmt"
	"sync"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
)

type environment struct {
	// State.
	activations *activations // Internally synchronized.

	// Dependencies.
	serverID string
	registry registry.Registry
}

func NewEnvironment(
	ctx context.Context,
	serverID string,
	reg registry.Registry,
) (Environment, error) {
	env := &environment{
		registry: reg,
		serverID: serverID,
	}
	activations := newActivations(reg, env)
	env.activations = activations

	// Do one heartbeat right off the bat so the environment is immediately useable.
	err := reg.Heartbeat(ctx, serverID, registry.HeartbeatState{NumActivatedActors: 0})
	if err != nil {
		return nil, fmt.Errorf("failed to perform initial heartbeat: %w", err)
	}

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
	serverID string,
	reference types.ActorReference,
	operation string,
	payload []byte,
) ([]byte, error) {
	if serverID != r.serverID {
		return nil, fmt.Errorf(
			"request for serverID: %s received by server: %s, cannot fullfil",
			serverID, r.serverID)
	}

	return r.activations.invoke(ctx, reference, operation, payload)
}

func (r *environment) Close() error {
	// TODO: This should call Close on the activations field (which needs to be implemented).
	return nil
}

// TODO: This is kind of a giant hack, but it's really only used for testing. The idea is that
// even when we're using local references, we still want to be able to create multiple
// environments in memory that can all "route" to each other. To accomplish this, everytime an
// environment is created in memory we added it to this global map. Once it is closed, we remove it.
var (
	localEnvironmentsRouter     map[string]Environment = map[string]Environment{}
	localEnvironmentsRouterLock sync.RWMutex
)

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
		localEnvironmentsRouterLock.RLock()
		localEnv, ok := localEnvironmentsRouter[reference.Address()]
		localEnvironmentsRouterLock.RUnlock()
		if !ok {
			return nil, fmt.Errorf(
				"unable to route invocation for server: %s at path: %s, does not exist in global routing map",
				reference.ServerID(), reference.Address())
		}
		return localEnv.InvokeLocal(ctx, reference.ServerID(), reference, operation, payload)
	case types.ReferenceTypeRemoteHTTP:
		fallthrough
	default:
		return nil, fmt.Errorf(
			"reference for actor: %s has unhandled type: %v", reference.ActorID(), reference.Type())
	}
}
