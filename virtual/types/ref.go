package types

import (
	"errors"
	"fmt"
)

type actorRef struct {
	virtualRef ActorReferenceVirtual
	serverID   string
	address    string
}

// NewActorReferences creates an ActorReference.
func NewActorReferences(
	serverID,
	address,
	namespace,
	moduleID,
	actorID string,
	generation uint64,
) (ActorReference, error) {
	virtual, err := NewVirtualActorReference(namespace, moduleID, actorID, generation)
	if err != nil {
		return nil, fmt.Errorf("NewActorReferences: error creating new virtual reference: %w", err)
	}

	if serverID == "" {
		return nil, errors.New("serverID cannot be empty")
	}
	if address == "" {
		return nil, errors.New("address cannot be empty")
	}

	return actorRef{
		virtualRef: virtual,
		serverID:   serverID,
		address:    address,
	}, nil
}

func (l actorRef) Type() ReferenceType {
	return ReferenceTypeLocal
}

func (l actorRef) ServerID() string {
	return l.serverID
}

func (l actorRef) Namespace() string {
	return l.virtualRef.Namespace()
}

func (l actorRef) ActorID() NamespacedID {
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
