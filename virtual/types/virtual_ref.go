package types

import "errors"

const (
	// IDTypeActor is the idType that indicates a reference is for an actor
	// as opposed to a worker.
	IDTypeActor = "actor"
	// IDTypeWorker is the opposite of IDTypeActor.
	IDTypeWorker = "worker"
)

type virtualRef struct {
	namespace  string
	moduleID   string
	actorID    string
	generation uint64
	// idType allows us to ensure that an actor and a worker with the
	// same tuple of <namespace, moduleID, "actorID"> are still
	// namespaced away from each other in any in-memory datastructures.
	idType string
}

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
		return nil, errors.New("namespace cannot be empty")
	}
	if moduleID == "" {
		return nil, errors.New("moduleID cannot be empty")
	}
	if actorID == "" {
		return nil, errors.New("moduleID cannot be empty")
	}
	if generation <= 0 {
		return nil, errors.New("generation must be >0")
	}

	return virtualRef{
		namespace:  namespace,
		moduleID:   moduleID,
		actorID:    actorID,
		generation: generation,
		idType:     idType,
	}, nil
}

func (l virtualRef) Namespace() string {
	return l.namespace
}

func (l virtualRef) ActorID() NamespacedActorID {
	return NewNamespacedActorID(l.namespace, l.actorID, l.moduleID, l.idType)
}

func (l virtualRef) ModuleID() NamespacedID {
	return NewNamespacedID(l.namespace, l.moduleID, l.idType)
}

func (l virtualRef) Generation() uint64 {
	return l.generation
}
