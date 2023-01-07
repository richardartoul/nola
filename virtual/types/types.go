package types

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
	ActorID() NamespacedID
	// Generation represents the generation count for the actor's activation. This value
	// may be bumped by the registry at any time to signal to the rest of the system that
	// all outstanding activations should be recreated for whatever reason.
	Generation() uint64
}

// ActorReferencePhysical is the subset of data in ActorReference that is "physical" and
// that is used to actually find and communicate with the actor's current activation.
type ActorReferencePhysical interface {
	// Type is the ReferenceType of the current ActorReference.
	Type() ReferenceType
	// ServerID is the ID of the physical server that this reference targets.
	ServerID() string
	// The address of the referenced actor.
	Address() string
}
