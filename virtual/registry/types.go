package registry

import (
	"context"

	"github.com/richardartoul/nola/virtual/types"
)

// Registry is the interface that is implemented by the virtual actor registry.
type Registry interface {
	ActorStorage
	ServiceDiscovery

	// RegisterModule registers the provided module []byte and options with the
	// provided module ID for subsequent calls to CreateActor().
	RegisterModule(
		ctx context.Context,
		namespace,
		moduleID string,
		moduleBytes []byte,
		opts ModuleOptions,
	) (RegisterModuleResult, error)

	// GetModule gets the bytes and options associated with the provided module.
	GetModule(
		ctx context.Context,
		namespace,
		moduleID string,
	) ([]byte, ModuleOptions, error)

	// CreateActor creates a new actor in the given namespace from the provided module
	// ID.
	CreateActor(
		ctx context.Context,
		namespace,
		actorID,
		moduleID string,
		opts ActorOptions,
	) (CreateActorResult, error)

	// IncGeneration increments the actor's generation count. This is useful for ensuring
	// that all actor activations are invalidated and recreated.
	IncGeneration(
		ctx context.Context,
		namespace,
		actorID string,
	) error

	// EnsureActivation checks the registry to see if the provided actor is already
	// activated, and if so it returns an ActorReference that points to its activated
	// location. Otherwise, the registry will pick a location to activate the actor at
	// and then return an ActorReference that points to the newly selected location.
	//
	// Note that when this method returns it is guaranteed that a location will have
	// been selected for the actor to be activated at, but the actor may not necessarily
	// have been activated. In general, actor activation is handled "lazily" when a
	// location (server) receives its first invocation for an actor ID that it doesn't
	// currently have activated.
	EnsureActivation(
		ctx context.Context,
		namespace,
		actorID string,
	) ([]types.ActorReference, error)

	// Close closes the registry and releases any resources associated (DB connections, etc).
	Close(ctx context.Context) error

	// UnsafeWipeAll wipes the entire registry. Only used for tests. Do not call it anywhere
	// in production code.
	UnsafeWipeAll() error
}

// ActorStorage contains the methods for interacting with per-actor durable storage.
type ActorStorage interface {
	// ActorKVPut stores value at key in the provided actor's durable KV storage.
	ActorKVPut(
		ctx context.Context,
		namespace string,
		actorID string,
		key []byte,
		value []byte,
	) error

	// ActorKVGet retrieves the value associated with key from the provided actor's
	// durable KV storage.
	ActorKVGet(
		ctx context.Context,
		namespace string,
		actorID string,
		key []byte,
	) ([]byte, bool, error)
}

// ServiceDiscovery contains the methods for interacting with the Registry's service
// discovery mechanism.
type ServiceDiscovery interface {
	// Heartbeat updates the "lastHeartbeatedAt" value for the provided server ID. Server's
	// must heartbeat regularly to be considered alive and eligible for hosting actor
	// activations.
	Heartbeat(
		ctx context.Context,
		serverID string,
		state HeartbeatState,
	) error
}

// ActorOptions contains the options for a given actor.
type ActorOptions struct {
}

// CreateActorResult is the result of a call to CreateActor().
type CreateActorResult struct{}

// ModuleOptions contains the options for a given module.
type ModuleOptions struct{}

// RegisterModuleResult is the result of a call to RegisterModule().
type RegisterModuleResult struct{}

// HeartbeatState contains information that accompanies a server's heartbeat. It contains
// various information about the current state of the server that might be useful to the
// registry. For example, the number of currently activated actors on the server is useful
// to the registry so it can load-balance future actor activations around the cluster to
// achieve uniformity.
//
// TODO: This should include things like how many CPU seconds and memory the actors are
//
//	using, etc for hotspot detection.
type HeartbeatState struct {
	// NumActivatedActors is the number of actors currently activated on the server.
	NumActivatedActors int
	// Address is the address at which the server can be reached.
	Address string
}
