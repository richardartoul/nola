package types

import (
	"encoding/json"
	"errors"
	"fmt"
)

// NewActorReference creates an ActorReference.
func NewActorReference(
	serverID string,
	serverVersion int64,
	namespace string,
	moduleID string,
	actorID string,
	generation uint64,
	serverState ServerState,
) (ActorReference, error) {
	virtual, err := NewVirtualActorReference(namespace, moduleID, actorID, generation)
	if err != nil {
		return ActorReference{}, fmt.Errorf("NewActorReference: error creating new virtual reference: %w", err)
	}

	if serverID == "" {
		return ActorReference{}, errors.New("serverID cannot be empty")
	}
	if serverState.Address == "" {
		return ActorReference{}, errors.New("address cannot be empty")
	}

	return ActorReference{
		Virtual: virtual,
		Physical: ActorReferencePhysical{
			ServerID:      serverID,
			ServerVersion: serverVersion,
			ServerState:   serverState,
		},
	}, nil
}

// NewActorReferenceFromJSON is the same as NewActorReference, except it recreates
// the in-memory representation of the ActorReference that was previously marshaled
// by calling MarshalJSON on the ActorReference.
func NewActorReferenceFromJSON(data []byte) (ActorReference, error) {
	var ref ActorReference
	if err := json.Unmarshal(data, &ref); err != nil {
		return ActorReference{}, fmt.Errorf("error creating new actor reference after JSON unmarshal: %w", err)
	}

	ref.Type = ReferenceTypeLocal

	return ref, nil
}

// ActorReference abstracts over different forms of ReferenceType. It provides all the
// necessary information for communicating with an actor. Some of the fields are "logical"
type ActorReference struct {
	Type     ReferenceType          `json:"type"`
	Virtual  ActorReferenceVirtual  `json:"virtual"`
	Physical ActorReferencePhysical `json:"physical"`
}

// ActorReferenceVirtual is the subset of data in ActorReference that is "virtual" and has
// nothing to do with the physical location of the actor's activation. The virtual fields
// are all that is required for the Registry to resolve a physical reference.
type ActorReferenceVirtual struct {
	// Namespace is the namespace to which this ActorReference belongs.
	Namespace string `json:"namespace"`
	// ModuleID is the ID of the WASM module that this actor is instantiated from.
	ModuleID string `json:"module_id"`
	// The ID of the referenced actor.
	ActorID string `json:"actor_id"`
	// Generation represents the generation count for the actor's activation. This value
	// may be bumped by the registry at any time to signal to the rest of the system that
	// all outstanding activations should be recreated for whatever reason.
	Generation uint64 `json:"generation"`

	// IDType allows us to ensure that an actor and a worker with the
	// same tuple of <namespace, moduleID, "actorID"> are still
	// namespaced away from each other in any in-memory datastructures.
	IDType string `json:"id_type"`

	// Buffers for Namespaced ActorID and ModuleID
	actorIDWithNamespace  *NamespacedActorID `json:"-"`
	moduleIDWithNamespace *NamespacedID      `json:"-"`
}

func (ref *ActorReferenceVirtual) ActorIDWithNamespace() NamespacedActorID {
	if ref.actorIDWithNamespace == nil {
		actorIDWithNamespace := NewNamespacedActorID(ref.Namespace, ref.ActorID, ref.ModuleID, ref.IDType)
		ref.actorIDWithNamespace = &actorIDWithNamespace
	}
	return *ref.actorIDWithNamespace
}

func (ref *ActorReferenceVirtual) ModuleIDWithNamespace() NamespacedID {
	if ref.moduleIDWithNamespace == nil {
		moduleIDWithNamespace := NewNamespacedID(ref.Namespace, ref.ModuleID, ref.IDType)
		ref.moduleIDWithNamespace = &moduleIDWithNamespace
	}
	return *ref.moduleIDWithNamespace
}

// ActorReferencePhysical is the subset of data in ActorReference that is "physical" and
// that is used to actually find and communicate with the actor's current activation.
type ActorReferencePhysical struct {
	// ServerID is the ID of the physical server that this reference targets.
	ServerID string `json:"server_id"`
	// ServerVersion is incremented every time a server's heartbeat expires and resumes,
	// guaranteeing the server's ability to identify periods of inactivity/death for correctness purposes.
	ServerVersion int64 `json:"server_version"`

	// The state of the physical server that this reference targets.
	// Contains information that is sent in the heartbeat.
	ServerState ServerState `json:"server_state"`
}

type ServerState struct {
	// Address is the address at which the server can be reached.
	Address string `json:"address"`
}
