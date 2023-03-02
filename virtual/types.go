package virtual

import (
	"context"
	"io"

	"github.com/richardartoul/nola/virtual/registry"
	"github.com/richardartoul/nola/virtual/types"
	"github.com/richardartoul/nola/wapcutils"
)

// Environment is the interface responsible for routing invocations to the appropriate
// actor. If the actor is not currently activated in the environment, it will take
// care of activating it.
type Environment interface {
	debug

	// InvokeActor invokes the specified operation on the specified actorID with the
	// provided payload. If the actor is already activated somewhere in the system,
	// the invocation will be routed appropriately. Otherwise, the request will
	// activate the actor somewhere in the system and then perform the invocation.
	InvokeActor(
		ctx context.Context,
		namespace string,
		actorID string,
		moduleID string,
		operation string,
		payload []byte,
		createIfNotExist types.CreateIfNotExist,
	) ([]byte, error)

	// InvokeActorStream is the same as InvokeActor, except it uses the streaming
	// interface instead of returning a []byte directly. This is useful for actors
	// that need to shuttle large volumes of data around (perhaps in an async manner).
	InvokeActorStream(
		ctx context.Context,
		namespace string,
		actorID string,
		moduleID string,
		operation string,
		payload []byte,
		createIfNotExist types.CreateIfNotExist,
	) (io.ReadCloser, error)

	// InvokeActorDirect is the same as InvokeActor, however, it performs the invocation
	// "directly".
	//
	// This method should only be called if the Registry has indicated that the specified
	// actorID should be activated in this process. If this constraint is violated then
	// inconsistencies may be introduced into the system.
	InvokeActorDirect(
		ctx context.Context,
		versionStamp int64,
		serverID string,
		serverVersion int64,
		reference types.ActorReferenceVirtual,
		operation string,
		payload []byte,
	) ([]byte, error)

	// InvokeActorDirectStream is the same as InvokeActorDirect, except it uses the streaming
	// interface instead of returning a []byte directly. This is useful for actors that need
	// to shuttle large volumes of data around (perhaps in an async manner).
	InvokeActorDirectStream(
		ctx context.Context,
		versionStamp int64,
		serverID string,
		serverVersion int64,
		reference types.ActorReferenceVirtual,
		operation string,
		payload []byte,
	) (io.ReadCloser, error)

	// InvokeWorker invokes the specified operation from the specified module. Unlike
	// actors, workers provide no guarantees about single-threaded execution or only
	// a single instance running at a time. This makes them easier to scale than
	// actors. They're especially useful for large workloads that don't require the
	// same guarantees actors provide.
	//
	// Also keep in mind that actor's can still "accumulate" in-memory state, just like
	// actors. However, there is no guarantee of linearizability like with Actors so
	// callers may see "inconsistent" memory state depending on which server/environment
	// their worker invocation is routed to.
	InvokeWorker(
		ctx context.Context,
		namespace string,
		moduleID string,
		operation string,
		payload []byte,
	) ([]byte, error)

	// InvokeWorkerStream is the same as InvokeWorker, except it uses the streaming interface
	// instead of returning a []byte directly. This is useful for actors that need to shuttle
	// large volumes of data around (perhaps in an async manner).
	InvokeWorkerStream(
		ctx context.Context,
		namespace string,
		moduleID string,
		operation string,
		payload []byte,
	) (io.ReadCloser, error)

	// Close closes the Environment and all of its associated resources.
	Close() error
}

// debug contains private methods that are only used for debugging / tests.
type debug interface {
	// numActivatedActors returns the number of activated actors in the environment. It is
	// primarily used for tests.
	numActivatedActors() int

	// heartbeat forces the environment to heartbeat the Registry immediately. It is primarily
	// used for tests.
	heartbeat() error

	// freezeHeartbeatState allows the environment to keep heartbeating the registry, but
	// prevents it from updating its internal heartbeat state. This keeps the server registered
	// in the registry, but allows us to test interaction between the client versionstamp
	// and the serverion heartbeat versionstamp.
	freezeHeartbeatState()

	// pauseHeartbeat prevents the heartbeat goroutine from sending the heartbeat
	// only used for testing purposes to simulate a server missing (sending a delayed) heartbeat.
	pauseHeartbeat()

	// resumeHeartbeat function resumes the heartbeat goroutine, used only for testing purposes.
	resumeHeartbeat()
}

// RemoteClient is the interface implemented by a client that is capable of communicating with
// remote nodes in the system.
type RemoteClient interface {
	// InvokeActorRemote is the same as Invoke, however, it performs the actor invocation on a
	// specific remote server.
	InvokeActorRemote(
		ctx context.Context,
		versionStamp int64,
		reference types.ActorReference,
		operation string,
		payload []byte,
	) (io.ReadCloser, error)
}

// Module represents a "module" / template from which new actors are constructed/instantiated.
type Module interface {
	// Instantiate instantiates a new in-memory actor from the module.
	Instantiate(
		ctx context.Context,
		id string,
		host HostCapabilities,
	) (Actor, error)
	// Close closes the modules.
	Close(ctx context.Context) error
}

// Actor represents an activated actor in memory.
type Actor interface {
	// Close closes the in-memory actor.
	Close(ctx context.Context) error
}

// ActorBytes is the version of Actor that returns responses as a []byte directly.
type ActorBytes interface {
	Actor

	// Invoke invokes the specified operation on the in-memory actor with the provided
	// payload. The transaction is invocation-specific and will automatically be
	// committed or rolled back / canceled based on whether Invoke returns an error.
	Invoke(
		ctx context.Context,
		operation string,
		payload []byte,
		transaction registry.ActorKVTransaction,
	) ([]byte, error)
}

// ActorStream is the same as ByteActor, except it can return responses as streams
// instead of []byte which is useful in scenarios where large amounts of data need
// to be shuttled around. It also allows the actor to behave in an "async" manner by
// return streams and then "filling them in" later.
type ActorStream interface {
	Actor

	InvokeStream(
		ctx context.Context,
		operation string,
		payload []byte,
		transaction registry.ActorKVTransaction,
	) (io.ReadCloser, error)
}

// HostCapabilities defines the interface of capabilities exposed by the host to the Actor.
type HostCapabilities interface {
	KV

	// InvokeActor invokes a function on the specified actor.
	InvokeActor(context.Context, types.InvokeActorRequest) ([]byte, error)

	// ScheduleInvokeActor is the same as InvokeActor, except the invocation is scheduled
	// in memory to be run later.
	ScheduleInvokeActor(context.Context, wapcutils.ScheduleInvocationRequest) error

	// CustomFn invoke a custom (user defined) host function. This will only work if the
	// custom host function was registered with the environment when it was instantiated.
	CustomFn(
		ctx context.Context,
		operation string,
		payload []byte,
	) ([]byte, error)
}

// KV is the host KV interface exposed to each actor.
type KV interface {
	// BeginTransaction begins a new transaction. This transaction is different from the
	// transaction that is provided to each call to Invoke() in that its lifecycle is not
	// managed by NOLA automatically and it is the actor's responsibility to commit or
	// cancel the transaction when it is ready.
	BeginTransaction(ctx context.Context) (registry.ActorKVTransaction, error)
	// Transact is the same as BeginTransaction, except with an easier to use interface.
	Transact(context.Context, func(tr registry.ActorKVTransaction) (any, error)) (any, error)
}

type CreateActorResult struct {
}

type InvokeActorResult struct {
}

type ScheduleInvocationResult struct {
}
