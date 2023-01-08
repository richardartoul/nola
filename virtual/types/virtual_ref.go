package types

import "errors"

type virtualRef struct {
	namespace  string
	moduleID   string
	actorID    string
	generation uint64
}

// NewVirtualActorReference creates a new VirtualActorReference.
func NewVirtualActorReference(
	namespace string,
	moduleID string,
	actorID string,
	generation uint64,
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
	return NewNamespacedID(l.namespace, l.actorID)
}

func (l virtualRef) ModuleID() NamespacedID {
	return NewNamespacedID(l.namespace, l.moduleID)
}

func (l virtualRef) Generation() uint64 {
	return l.generation
}
