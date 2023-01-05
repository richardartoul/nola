package virtual

import (
	"context"

	"github.com/richardartoul/nola/virtual/types"
)

// Environment is the interface responsible for routing invocations to the appropriate
// actor. If the actor is not currently activated in the environment, it will take
// care of activating it.
type Environment interface {
	// Invoke invokes the specified operation on the specified actorID with the
	// provided payload. If the actor is already activated somewhere in the system,
	// the invocation will be routed appropriately. Otherwise, the request will
	// activate the actor somewhere in the system and then perform the invocation.
	Invoke(
		ctx context.Context,
		namespace string,
		actorID string,
		operation string,
		payload []byte,
	) ([]byte, error)

	// InvokeLocal is the same as Invoke, however, it performs the invocation locally.
	// This method should only be called if the Registry has indicated that the specified
	// actorID should be activated in this process. If this constraint is violated then
	// inconsistencies may be introduced into the system.
	InvokeLocal(
		ctx context.Context,
		versionStamp int64,
		serverID string,
		reference types.ActorReference,
		operation string,
		payload []byte,
	) ([]byte, error)

	// Close closes the Environment and all of its associated resources.
	Close() error

	// numActivatedActors returns the number of activated actors in the environment. It is
	// primarily used for tests.
	numActivatedActors() int

	// heartbeat forces the environment to heartbeat the Registry immediately. It is primarily
	// used for tests.
	heartbeat() error
}
