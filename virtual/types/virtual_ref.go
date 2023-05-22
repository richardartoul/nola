package types

import "errors"

const (
	// IDTypeActor is the idType that indicates a reference is for an actor
	// as opposed to a worker.
	IDTypeActor = "actor"
	// IDTypeWorker is the opposite of IDTypeActor.
	IDTypeWorker = "worker"
)

// func NewVirtualWorkerReference creates a new VirtualActorReference
// for a given worker.
func NewVirtualWorkerReference(
	namespace string,
	moduleID string,
	actorID string,
) (ActorReferenceVirtual, error) {
	return newVirtualActorReference(
		// Hard-code 1 for the generation because workers don't require
		// any communication with the Registry, therefore they have no
		// concept of a generation ID.
		namespace, moduleID, actorID, 1, IDTypeWorker)
}

// NewVirtualActorReference creates a new VirtualActorReference for a
// given actor. Should not be used for workers (use NewVirtualWorkerReference
// for that).
func NewVirtualActorReference(
	namespace string,
	moduleID string,
	actorID string,
	generation uint64,
) (ActorReferenceVirtual, error) {
	return newVirtualActorReference(
		namespace, moduleID, actorID, generation, IDTypeActor)
}

func newVirtualActorReference(
	namespace string,
	moduleID string,
	actorID string,
	generation uint64,
	idType string,
) (ActorReferenceVirtual, error) {
	if namespace == "" {
		return ActorReferenceVirtual{}, errors.New("namespace cannot be empty")
	}
	if moduleID == "" {
		return ActorReferenceVirtual{}, errors.New("moduleID cannot be empty")
	}
	if actorID == "" {
		return ActorReferenceVirtual{}, errors.New("moduleID cannot be empty")
	}
	if generation <= 0 {
		return ActorReferenceVirtual{}, errors.New("generation must be >0")
	}

	return ActorReferenceVirtual{
		Namespace:  namespace,
		ModuleID:   moduleID,
		ActorID:    actorID,
		Generation: generation,
		IDType:     idType,
	}, nil
}
