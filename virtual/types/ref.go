package types

import (
	"encoding/json"
	"errors"
	"fmt"
)

type actorRef struct {
	virtualRef    *virtualRef
	serverID      string
	serverVersion int64
	address       string
}

// NewActorReference creates an ActorReference.
func NewActorReference(
	serverID string,
	serverVersion int64,
	address string,
	namespace string,
	moduleID string,
	actorID string,
	generation uint64,
) (ActorReference, error) {
	virtual, err := NewVirtualActorReference(namespace, moduleID, actorID, generation)
	if err != nil {
		return nil, fmt.Errorf("NewActorReference: error creating new virtual reference: %w", err)
	}

	if serverID == "" {
		return nil, errors.New("serverID cannot be empty")
	}
	if address == "" {
		return nil, errors.New("address cannot be empty")
	}

	vr := virtual.(virtualRef)
	return &actorRef{
		virtualRef:    &vr,
		serverID:      serverID,
		serverVersion: serverVersion,
		address:       address,
	}, nil
}

// NewActorReferenceFromJSON is the same as NewActorReference, except it recreates
// the in-memory representation of the ActorReference that was previously marshaled
// by calling MarshalJSON on the ActorReference.
func NewActorReferenceFromJSON(data []byte) (ActorReference, error) {
	var serializable serializableActorRef
	if err := json.Unmarshal(data, &serializable); err != nil {
		return nil, err
	}

	ref, err := NewActorReference(
		serializable.ServerID,
		serializable.ServerVersion,
		serializable.Address,
		serializable.Namespace,
		serializable.ModuleID,
		serializable.ActorID,
		serializable.Generation)
	if err != nil {
		return nil, fmt.Errorf("error creating new actor reference after JSON unmarshal: %w", err)
	}
	ref.(*actorRef).virtualRef.idType = serializable.IDType

	return ref, nil
}

func (l actorRef) Type() ReferenceType {
	return ReferenceTypeLocal
}

func (l actorRef) ServerID() string {
	return l.serverID
}

func (l actorRef) ServerVersion() int64 {
	return l.serverVersion
}

func (l actorRef) Namespace() string {
	return l.virtualRef.Namespace()
}

func (l actorRef) ActorID() NamespacedActorID {
	return l.virtualRef.ActorID()
}

func (l actorRef) ModuleID() NamespacedID {
	return l.virtualRef.ModuleID()
}

func (l actorRef) Address() string {
	return l.address
}

func (l actorRef) Generation() uint64 {
	return l.virtualRef.Generation()
}

func (l actorRef) MarshalJSON() ([]byte, error) {
	// This is terrible, I'm sorry.
	return json.Marshal(&serializableActorRef{
		Namespace:     l.Namespace(),
		ModuleID:      l.virtualRef.ModuleID().ID,
		ActorID:       l.virtualRef.ActorID().ID,
		Generation:    l.virtualRef.Generation(),
		IDType:        l.virtualRef.idType,
		ServerID:      l.serverID,
		ServerVersion: l.serverVersion,
		Address:       l.address,
	})
}

type serializableActorRef struct {
	Namespace  string `json:"namespace"`
	ModuleID   string `json:"moduleID"`
	ActorID    string `json:"actorID"`
	Generation uint64 `json:"generation"`
	// idType allows us to ensure that an actor and a worker with the
	// same tuple of <namespace, moduleID, "actorID"> are still
	// namespaced away from each other in any in-memory datastructures.
	IDType        string `json:"idType"`
	ServerID      string `json:"server_id"`
	ServerVersion int64  `json:"server_version"`
	Address       string `json:"address"`
}
