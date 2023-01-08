package types

import "errors"

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
		namespace, moduleID, actorID, 1, "worker")
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
		namespace, moduleID, actorID, generation, "actor")
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
	}, nil
}

func (l virtualRef) Namespace() string {
	return l.namespace
}

func (l virtualRef) ActorID() NamespacedID {
	return NewNamespacedID(l.namespace, l.actorID, l.idType)
}

func (l virtualRef) ModuleID() NamespacedID {
	return NewNamespacedID(l.namespace, l.moduleID, l.idType)
}

func (l virtualRef) Generation() uint64 {
	return l.generation
}
