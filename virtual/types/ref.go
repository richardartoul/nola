package types

import (
	"errors"
	"fmt"
)

type actorRef struct {
	virtualRef    ActorReferenceVirtual
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

	return actorRef{
		virtualRef:    virtual,
		serverID:      serverID,
		serverVersion: serverVersion,
		address:       address,
	}, nil
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
