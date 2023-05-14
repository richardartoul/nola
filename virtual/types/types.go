package types

import "encoding/json"

// ReferenceType is an enum type that indicates what the underlying type of Reference is,
// see the different ReferenceType's below.
type ReferenceType string

const (
	// ReferenceTypeLocal indicates the actor is "local" to the current process. Mainly
	// used for benchmarking and tests.
	ReferenceTypeLocal ReferenceType = "local"
	// ReferenceTypeRemoteHTTP indicates the actor is remote and must be accessed via
	// an HTTP call to the associated address.
	ReferenceTypeRemoteHTTP ReferenceType = "remote-http"
)

// ActorReference abstracts over different forms of ReferenceType. It provides all the
// necessary information for communicating with an actor. Some of the fields are "logical"
type ActorReference interface {
	json.Marshaler

	ActorReferenceVirtual
	ActorReferencePhysical
}

// ActorReferenceVirtual is the subset of data in ActorReference that is "virtual" and has
// nothing to do with the physical location of the actor's activation. The virtual fields
// are all that is required for the Registry to resolve a physical reference.
type ActorReferenceVirtual interface {
	// Namespace is the namespace to which this ActorReference belongs.
	Namespace() string
	// ModuleID is the ID of the WASM module that this actor is instantiated from.
	ModuleID() NamespacedID
	// The ID of the referenced actor.
	ActorID() NamespacedActorID
	// Generation represents the generation count for the actor's activation. This value
	// may be bumped by the registry at any time to signal to the rest of the system that
	// all outstanding activations should be recreated for whatever reason.
	Generation() uint64
}

// ActorReferencePhysical is the subset of data in ActorReference that is "physical" and
// that is used to actually find and communicate with the actor's current activation.
type ActorReferencePhysical interface {
	// ServerID is the ID of the physical server that this reference targets.
	ServerID() string
	// ServerVersion is incremented every time a server's heartbeat expires and resumes,
	// guaranteeing the server's ability to identify periods of inactivity/death for correctness purposes.
	ServerVersion() int64

	// The state of the physical server that this reference targets.
	// Contains information that is sent in the heartbeat.
	ServerState() ServerState
}


type ServerState interface {
	// NumActivatedActors is the number of actors currently activated on the server.
	NumActivatedActors() int
	// UsedMemory is the amount of memory currently being used by actors on the server.
	UsedMemory() int
	// Address is the address at which the server can be reached.
	Address() string
}
