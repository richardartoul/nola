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

	serverState ServerState
}

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
		return nil, fmt.Errorf("NewActorReference: error creating new virtual reference: %w", err)
	}

	if serverID == "" {
		return nil, errors.New("serverID cannot be empty")
	}
	if serverState.Address() == "" {
		return nil, errors.New("address cannot be empty")
	}

	vr := virtual.(virtualRef)
	return &actorRef{
		virtualRef:    &vr,
		serverID:      serverID,
		serverVersion: serverVersion,
		serverState:   serverState,
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
		serializable.Namespace,
		serializable.ModuleID,
		serializable.ActorID,
		serializable.Generation,
		serializable.ServerState,
	)
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

func (l actorRef) ServerState() ServerState {
	return l.serverState
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
		ServerState: serializableServerState{
			SNumActivatedActors: l.serverState.NumActivatedActors(),
			SUsedMemory:         l.serverState.UsedMemory(),
			SAddress:            l.serverState.Address(),
		},
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
	IDType        string                  `json:"idType"`
	ServerID      string                  `json:"server_id"`
	ServerVersion int64                   `json:"server_version"`
	ServerState   serializableServerState `json:"server_state"`
}

// ServerState contains information that accompanies a server's heartbeat. It contains
// various information about the current state of the server that might be useful to the
// registry. For example, the number of currently activated actors on the server is useful
// to the registry so it can load-balance future actor activations around the cluster to
// achieve uniformity.
//
// TODO: This should include things like how many CPU seconds and memory the actors are
// using, etc for hotspot detection.
type serializableServerState struct {
	// NumActivatedActors is the number of actors currently activated on the server.
	SNumActivatedActors int `json:"num_activated_actors"`
	// UsedMemory is the amount of memory currently being used by actors on the server.
	SUsedMemory int `json:"used_memory"`
	// Address is the address at which the server can be reached.
	SAddress string `json:"address"`
}

func (s serializableServerState) NumActivatedActors() int {
	return s.SNumActivatedActors
}

func (s serializableServerState) UsedMemory() int {
	return s.SUsedMemory
}

func (s serializableServerState) Address() string {
	return s.SAddress
}
